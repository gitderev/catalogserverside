import React, { useState, useRef, useCallback, useEffect, useMemo } from 'react';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Checkbox } from '@/components/ui/checkbox';
import { toast } from '@/hooks/use-toast';
import { 
  Pagination,
  PaginationContent,
  PaginationEllipsis,
  PaginationItem,
  PaginationLink,
  PaginationNext,
  PaginationPrevious,
} from '@/components/ui/pagination';
import { Upload, Download, FileText, CheckCircle, XCircle, AlertCircle, Clock, Activity, Info, ChevronLeft, ChevronRight } from 'lucide-react';
import { filterAndNormalizeForEAN, type EANStats, type DiscardedRow } from '@/utils/ean';
import { exportWorkbook, sanitizeCell } from '@/utils/excelFormatter';
import { toComma99Cents, validateEnding99, validateEnding99Cents, computeFinalEan, computeFromListPrice, toCents, formatCents } from '@/utils/pricing';
import * as XLSX from 'xlsx';
import Papa from 'papaparse';

// Constants
const DEFAULT_SHIPPING = 6.00;
const DEFAULT_IVA_PERC = 22;
const PAGE_SIZE = 50;

interface FileData {
  name: string;
  data: any[];
  headers: string[];
  raw: File;
  isValid?: boolean;
}

interface FileUploadState {
  material: { file: FileData | null; status: 'empty' | 'valid' | 'error' | 'warning'; error?: string; warning?: string; diagnostics?: any };
  stock: { file: FileData | null; status: 'empty' | 'valid' | 'error' | 'warning'; error?: string; warning?: string; diagnostics?: any };
  price: { file: FileData | null; status: 'empty' | 'valid' | 'error' | 'warning'; error?: string; warning?: string; diagnostics?: any };
}

interface ProcessedRecord {
  Matnr: string;
  ManufPartNr: string;
  EAN: string;
  ShortDescription: string;
  ExistingStock: number;
  ListPrice: number;
  CustBestPrice: number;
  'Costo di Spedizione': number;
  'Prezzo con spediz e IVA': number;
  FeeDeRev: number;
  'Fee Marketplace': number;
  'Subtotale post-fee': number;
  'Prezzo Finale': number | string; // String display for EAN (e.g. "34,99"), number for MPN
  'ListPrice con Fee': number | string; // Can be empty string for invalid ListPrice
}

interface FeeConfig {
  feeDrev: number;   // e.g. 3 (for 3%)
  feeMkt: number;    // e.g. 15 (for 15%)
}

interface LogEntry {
  source_file: string;
  line: number;
  Matnr: string;
  ManufPartNr: string;
  EAN: string;
  reason: string;
  details: string;
}

interface ProcessingStats {
  totalRecords: number;
  validRecordsEAN: number;
  validRecordsManufPartNr: number;
  filteredRecordsEAN: number;
  filteredRecordsManufPartNr: number;
  stockDuplicates: number;
  priceDuplicates: number;
}

const REQUIRED_HEADERS = {
  material: ['Matnr', 'ManufPartNr', 'EAN', 'ShortDescription'],
  stock: ['Matnr', 'ExistingStock'],
  price: ['Matnr', 'ListPrice', 'CustBestPrice']
};

const OPTIONAL_HEADERS = {
  material: [],
  stock: ['ManufPartNr'],
  price: ['ManufPartNr']
};

const DEFAULT_FEES: FeeConfig = { feeDrev: 3, feeMkt: 15 };

function loadFees(): FeeConfig {
  try {
    const raw = localStorage.getItem('catalog_fees_v2');
    if (!raw) return DEFAULT_FEES;
    const obj = JSON.parse(raw);
    return {
      feeDrev: Number(obj.feeDrev) || DEFAULT_FEES.feeDrev,
      feeMkt: Number(obj.feeMkt) || DEFAULT_FEES.feeMkt,
    };
  } catch { 
    return DEFAULT_FEES; 
  }
}

function saveFees(cfg: FeeConfig) {
  localStorage.setItem('catalog_fees_v2', JSON.stringify(cfg));
}

function computeFinalPrice({
  CustBestPrice, ListPrice, feeDrev, feeMkt
}: { CustBestPrice?: number; ListPrice?: number; feeDrev: number; feeMkt: number; }): {
  base: number, shipping: number, iva: number, subtotConIva: number,
  postFee: number, prezzoFinaleEAN: number, prezzoFinaleMPN: number, listPriceConFee: number | string,
  eanResult: { finalCents: number; finalDisplay: string; route: string; debug: any }
} {
  const hasBest = Number.isFinite(CustBestPrice) && CustBestPrice! > 0;
  const hasListPrice = Number.isFinite(ListPrice) && ListPrice! > 0;
  
  let base = 0;
  let baseRoute = '';
  
  // Select base price with route tracking
  if (hasBest) {
    base = CustBestPrice!;
    baseRoute = 'cbp';
  } else if (hasListPrice) {
    base = Math.ceil(ListPrice!); // ONLY ceil allowed in EAN pipeline - on ListPrice as base
    baseRoute = 'listprice_ceiled';
  } else {
    // No valid price
    const emptyEanResult = { finalCents: 0, finalDisplay: '0,00', route: 'none', debug: {} };
    return { 
      base: 0, shipping: DEFAULT_SHIPPING, iva: 0, subtotConIva: 0, 
      postFee: 0, prezzoFinaleEAN: 0, prezzoFinaleMPN: 0, listPriceConFee: '', eanResult: emptyEanResult 
    };
  }
  
  // Calculate for display/compatibility (old pipeline values)
  const subtot_base_sped = base + DEFAULT_SHIPPING;
  const iva = subtot_base_sped * (DEFAULT_IVA_PERC / 100);
  const subtotConIva = subtot_base_sped + iva;
  const postFee = subtotConIva * (1 + feeDrev/100) * (1 + feeMkt/100);
  
  // EAN final price: use new computeFinalEan function (cent-precise with ending ,99)
  const eanResult = computeFinalEan(
    { listPrice: ListPrice || 0, custBestPrice: CustBestPrice && CustBestPrice > 0 ? CustBestPrice : undefined },
    { feeDeRev: feeDrev, feeMarketplace: feeMkt },
    DEFAULT_SHIPPING,
    DEFAULT_IVA_PERC
  );
  const prezzoFinaleEAN = eanResult.finalCents / 100;
  
  // MPN final price: use old logic (ceil to integer)
  const prezzoFinaleMPN = Math.ceil(postFee);
  
  // Calculate ListPrice con Fee - SEPARATE pipeline, independent from main calculation
  let listPriceConFee: number | string = '';
  if (hasListPrice) {
    const listPriceResult = computeFromListPrice(
      ListPrice!,
      { feeDeRev: feeDrev, feeMarketplace: feeMkt },
      DEFAULT_SHIPPING,
      DEFAULT_IVA_PERC
    );
    listPriceConFee = parseInt(listPriceResult.finalDisplayInt);
  }

  return { base, shipping: DEFAULT_SHIPPING, iva, subtotConIva, postFee, prezzoFinaleEAN, prezzoFinaleMPN, listPriceConFee, eanResult };
}

const AltersideCatalogGenerator: React.FC = () => {
  const [files, setFiles] = useState<FileUploadState>({
    material: { file: null, status: 'empty' },
    stock: { file: null, status: 'empty' },
    price: { file: null, status: 'empty' }
  });

  // Fee configuration
  const [feeConfig, setFeeConfig] = useState<FeeConfig>(loadFees());
  const [rememberFees, setRememberFees] = useState(false);

  // Save fees when rememberFees is checked
  useEffect(() => {
    if (rememberFees) {
      saveFees(feeConfig);
    }
  }, [feeConfig, rememberFees]);

  // Pagination state
  const [currentPage, setCurrentPage] = useState(0);

  const [processingState, setProcessingState] = useState<'idle' | 'validating' | 'ready' | 'running' | 'completed' | 'failed'>('idle');
  const [currentPipeline, setCurrentPipeline] = useState<'EAN' | 'MPN' | null>(null);
  
  // Progress states (based on rows read, not valid rows)
  const [total, setTotal] = useState(0); // prescan estimate
  const [finalTotal, setFinalTotal] = useState<number | null>(null); // actual rows read in join
  const [processed, setProcessed] = useState(0);
  const processedRef = useRef(0);
  const [progressPct, setProgressPct] = useState(0);
  
  const [startTime, setStartTime] = useState<number | null>(null);
  const [elapsedTime, setElapsedTime] = useState(0);
  const [estimatedTime, setEstimatedTime] = useState<number | null>(null);
  
  // Completion gating flags
  const [joinDone, setJoinDone] = useState(false);
  const [excelDone, setExcelDone] = useState(false);
  const [logDone, setLogDone] = useState(false);
  const [eanStats, setEanStats] = useState<EANStats | null>(null);
  const [discardedRows, setDiscardedRows] = useState<DiscardedRow[]>([]);
  
  // Debug events
  const [debugEvents, setDebugEvents] = useState<string[]>([]);
  const [debugState, setDebugState] = useState({
    materialValid: false,
    stockValid: false,
    priceValid: false,
    stockReady: false,
    priceReady: false,
    materialPreScanDone: false,
    joinStarted: false
  });

  // Current pipeline results
  const [currentProcessedData, setCurrentProcessedData] = useState<ProcessedRecord[]>([]);
  const [currentLogEntries, setCurrentLogEntries] = useState<LogEntry[]>([]);
  const [currentStats, setCurrentStats] = useState<ProcessingStats | null>(null);
  
  // Stats state for UI
  const [stats, setStats] = useState({
    totalRows: 0,
    validEAN: 0,
    validMPN: 0,
    discardedEAN: 0,
    discardedMPN: 0,
    duplicates: 0
  });
  
  // Downloaded files state for buttons
  const [downloadReady, setDownloadReady] = useState({
    ean_excel: false,
    ean_log: false,
    mpn_excel: false,
    mpn_log: false
  });

  // Export state for preventing double clicks
  const [isExportingEAN, setIsExportingEAN] = useState(false);
  const [isExportingMPN, setIsExportingMPN] = useState(false);

  const workerRef = useRef<Worker | null>(null);

  const isProcessing = processingState === 'running';
  const isCompleted = processingState === 'completed';
  const canProcess = processingState === 'ready';

  // Reset page when dataset changes
  useEffect(() => {
    setCurrentPage(0);
  }, [currentProcessedData]);

  // Memoized paged rows
  const pagedRows = useMemo(() => {
    const startIndex = currentPage * PAGE_SIZE;
    const endIndex = startIndex + PAGE_SIZE;
    return currentProcessedData.slice(startIndex, endIndex);
  }, [currentProcessedData, currentPage]);

  const totalPages = Math.ceil(currentProcessedData.length / PAGE_SIZE);
  
  // Audit function for critical debugging
  const audit = useCallback((msg: string, data?: any) => {
    const logEntry = `AUDIT: ${msg}${data ? ' | ' + JSON.stringify(data) : ''}`;
    console.warn(logEntry);
    dbg(logEntry);
  }, []);

  // Global debug function
  const dbg = useCallback((event: string, data?: any) => {
    const timestamp = new Date().toLocaleTimeString('it-IT', { hour12: false });
    const message = `[${timestamp}] ${event}${data ? ' | ' + JSON.stringify(data) : ''}`;
    setDebugEvents(prev => [...prev, message]);
  }, []);
  
  // Make dbg available globally for worker
  useEffect(() => {
    (window as any).dbg = dbg;
  }, [dbg]);

  // Consistency check functions with ±1 tolerance
  const sumForPipeline = useCallback(() => {
    if (currentPipeline === 'EAN') {
      return (stats.validEAN ?? 0) + (stats.discardedEAN ?? 0) + (stats.duplicates ?? 0);
    }
    if (currentPipeline === 'MPN') {
      return (stats.validMPN ?? 0) + (stats.discardedMPN ?? 0) + (stats.duplicates ?? 0);
    }
    return NaN;
  }, [currentPipeline, stats]);

  const consistencyOk = useCallback(() => {
    const baseline = (finalTotal ?? processedRef.current ?? processed ?? total ?? 0);
    const sum = sumForPipeline();
    return Number.isFinite(baseline) && Number.isFinite(sum) && Math.abs(sum - baseline) <= 1;
  }, [finalTotal, processed, total, sumForPipeline]);

  const getConsistencySnapshot = useCallback(() => {
    return {
      pipeline: currentPipeline,
      totalPrescan: total,
      finalTotal: finalTotal ?? processedRef.current,
      processedUI: processed,
      stats
    };
  }, [currentPipeline, total, finalTotal, processed, stats]);

  // Completion gating effect with consistency check
  useEffect(() => {
    if (joinDone && excelDone && logDone) {
      if (!consistencyOk()) {
        audit('consistency-failed', getConsistencySnapshot());
        // fallback: accept baseline from join and continue anyway
        // or relaunch join with worker:false, but DON'T block infinitely
      }
      setProgressPct(100);
      setProcessingState('completed');
      dbg('pipeline:completed', {
        pipeline: currentPipeline,
        totalPrescan: total,
        finalTotal: finalTotal ?? processedRef.current
      });
    }
  }, [joinDone, excelDone, logDone, consistencyOk, getConsistencySnapshot, currentPipeline, total, finalTotal, audit, dbg]);

  // Log state changes
  useEffect(() => {
    dbg('state:change', { state: processingState, ...debugState });
  }, [processingState, debugState, dbg]);

  const validateHeaders = (headers: string[], requiredHeaders: string[], optionalHeaders: string[] = []): { 
    valid: boolean; 
    missing: string[]; 
    missingOptional: string[];
    hasWarning: boolean;
  } => {
    const normalizedHeaders = headers.map(h => h.trim().replace(/^\uFEFF/, '')); // Remove BOM
    const missing = requiredHeaders.filter(req => !normalizedHeaders.includes(req));
    const missingOptional = optionalHeaders.filter(opt => !normalizedHeaders.includes(opt));
    
    return { 
      valid: missing.length === 0, 
      missing, 
      missingOptional,
      hasWarning: missingOptional.length > 0
    };
  };

  const parseCSV = async (file: File): Promise<{ data: any[]; headers: string[] }> => {
    return new Promise((resolve, reject) => {
      Papa.parse(file, {
        header: true,
        delimiter: ';',
        encoding: 'UTF-8',
        skipEmptyLines: true,
        complete: (results) => {
          if (results.errors.length > 0) {
            reject(new Error(`Errore parsing: ${results.errors[0].message}`));
            return;
          }
          
          const headers = results.meta.fields || [];
          resolve({
            data: results.data,
            headers
          });
        },
        error: (error) => {
          reject(new Error(`Errore lettura file: ${error.message}`));
        }
      });
    });
  };

  const handleFileUpload = async (file: File, type: keyof FileUploadState) => {
    try {
      const parsed = await parseCSV(file);
      const validation = validateHeaders(parsed.headers, REQUIRED_HEADERS[type], OPTIONAL_HEADERS[type]);
      
      if (!validation.valid) {
        const error = `Header mancanti: ${validation.missing.join(', ')}`;
        setFiles(prev => ({
          ...prev,
          [type]: { file: null, status: 'error', error }
        }));
        setProcessingState('idle');
        
        toast({
          title: "Errore validazione header",
          description: error,
          variant: "destructive"
        });
        return;
      }

      // Handle warnings for optional headers
      let warning = '';
      let status: 'valid' | 'warning' = 'valid';
      
      if (validation.hasWarning && (type === 'stock' || type === 'price')) {
        status = 'warning';
        if (type === 'stock') {
          warning = 'Header opzionale assente: ManufPartNr (continuerò usando il valore dal Material).';
        } else if (type === 'price') {
          warning = 'Header opzionale assente: ManufPartNr (continuerò usando il valore dal Material).';
        }
      }

      // Create file state with diagnostics
      const headerLine = parsed.headers.join(', ');
      const firstDataLine = parsed.data.length > 0 ? Object.values(parsed.data[0]).slice(0, 3).join(', ') + '...' : '';

      const fileState = {
        name: file.name,
        data: parsed.data,
        headers: parsed.headers,
        raw: file,
        isValid: validation.valid
      };

      // Update processing state - ready when all files have valid required headers
      const newFiles = {
        ...files,
        [type]: {
          ...files[type],
          file: fileState,
          status: fileState.isValid ? 'valid' : 'error',
          diagnostics: {
            headerFound: headerLine,
            firstDataRow: firstDataLine,
            validation
          }
        }
      };
      
      // Check if all files are loaded with valid required headers (warnings don't block)
      const allRequiredHeadersValid = Object.entries(newFiles).every(([fileType, fileState]) => {
        if (!fileState.file) return false;
        const requiredHeaders = REQUIRED_HEADERS[fileType as keyof typeof REQUIRED_HEADERS];
        const validation = validateHeaders(fileState.file.headers, requiredHeaders);
        return validation.valid; // Only check required headers
      });
      
      // Update debug state
      const newDebugState = {
        materialValid: newFiles.material.file ? validateHeaders(newFiles.material.file.headers, REQUIRED_HEADERS.material).valid : false,
        stockValid: newFiles.stock.file ? validateHeaders(newFiles.stock.file.headers, REQUIRED_HEADERS.stock).valid : false,
        priceValid: newFiles.price.file ? validateHeaders(newFiles.price.file.headers, REQUIRED_HEADERS.price).valid : false,
        stockReady: newFiles.stock.file !== null,
        priceReady: newFiles.price.file !== null,
        materialPreScanDone: false,
        joinStarted: false
      };

      setDebugState(newDebugState);
      setFiles(newFiles);
      
      if (allRequiredHeadersValid) {
        setProcessingState('ready');
        dbg('validation:ready', { allFiles: Object.keys(newFiles).filter(k => newFiles[k as keyof typeof newFiles].file) });
      } else {
        setProcessingState('validating');
      }

      if (warning) {
        toast({
          title: "Avvertimento",
          description: warning,
          variant: "default"
        });
      } else {
        toast({
          title: "File caricato con successo",
          description: `${file.name} è stato caricato e validato.`,
          variant: "default"
        });
      }

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Errore sconosciuto';
      setFiles(prev => ({
        ...prev,
        [type]: { file: null, status: 'error', error: errorMessage }
      }));
      setProcessingState('idle');
      
      toast({
        title: "Errore caricamento file",
        description: errorMessage,
        variant: "destructive"
      });
    }
  };

  // Process EAN pipeline
  const handleProcessEAN = useCallback(async () => {
    if (!canProcess || isProcessing) return;
    
    try {
      setProcessingState('running');
      setCurrentPipeline('EAN');
      setStartTime(Date.now());
      
      // Reset states
      setJoinDone(false);
      setExcelDone(false);
      setLogDone(false);
      setProcessed(0);
      processedRef.current = 0;
      setProgressPct(0);
      
      dbg('process:ean:start');
      
      // Simulate processing with actual data
      const materialData = files.material.file?.data || [];
      const stockData = files.stock.file?.data || [];
      const priceData = files.price.file?.data || [];
      
      // Join data
      const joinedData: ProcessedRecord[] = [];
      
      for (const material of materialData) {
        const stock = stockData.find(s => s.Matnr === material.Matnr);
        const price = priceData.find(p => p.Matnr === material.Matnr);
        
        if (stock && price) {
          const pricing = computeFinalPrice({
            CustBestPrice: parseFloat(price.CustBestPrice) || 0,
            ListPrice: parseFloat(price.ListPrice) || 0,
            feeDrev: feeConfig.feeDrev,
            feeMkt: feeConfig.feeMkt
          });
          
          joinedData.push({
            Matnr: material.Matnr,
            ManufPartNr: material.ManufPartNr,
            EAN: material.EAN,
            ShortDescription: material.ShortDescription,
            ExistingStock: parseFloat(stock.ExistingStock) || 0,
            ListPrice: parseFloat(price.ListPrice) || 0,
            CustBestPrice: parseFloat(price.CustBestPrice) || 0,
            'Costo di Spedizione': DEFAULT_SHIPPING,
            'Prezzo con spediz e IVA': pricing.subtotConIva,
            FeeDeRev: feeConfig.feeDrev,
            'Fee Marketplace': feeConfig.feeMkt,
            'Subtotale post-fee': pricing.postFee,
            'Prezzo Finale': pricing.eanResult.finalDisplay, // String with ,99 for EAN
            'ListPrice con Fee': pricing.listPriceConFee
          });
        }
        
        processedRef.current++;
        setProcessed(processedRef.current);
        setProgressPct((processedRef.current / materialData.length) * 80);
      }
      
      // Filter for EAN with computeFinalPrice function
      const computePriceFn = (row: any) => {
        const result = computeFinalPrice({
          CustBestPrice: row.CustBestPrice,
          ListPrice: row.ListPrice,
          feeDrev: feeConfig.feeDrev,
          feeMkt: feeConfig.feeMkt
        });
        return result.prezzoFinaleEAN;
      };
      
      const { kept: eanValidRecords, discarded: eanDiscarded, stats: eanFilterStats } = filterAndNormalizeForEAN(joinedData, computePriceFn);
      
      setCurrentProcessedData(eanValidRecords);
      setDiscardedRows(eanDiscarded);
      setEanStats(eanFilterStats);
      setFinalTotal(joinedData.length);
      
      // Update stats
      setCurrentStats({
        totalRecords: joinedData.length,
        validRecordsEAN: eanValidRecords.length,
        validRecordsManufPartNr: 0,
        filteredRecordsEAN: eanValidRecords.length,
        filteredRecordsManufPartNr: 0,
        stockDuplicates: 0,
        priceDuplicates: 0
      });
      
      setStats({
        totalRows: joinedData.length,
        validEAN: eanValidRecords.length,
        validMPN: 0,
        discardedEAN: eanDiscarded.length,
        discardedMPN: 0,
        duplicates: 0
      });
      
      setJoinDone(true);
      setExcelDone(true);
      setLogDone(true);
      
      dbg('process:ean:complete', { valid: eanValidRecords.length, discarded: eanDiscarded.length });
      
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Errore elaborazione EAN';
      dbg('process:ean:error', { error: errorMessage });
      setProcessingState('failed');
      
      toast({
        title: "Errore elaborazione EAN",
        description: errorMessage,
        variant: "destructive"
      });
    }
  }, [canProcess, isProcessing, files, feeConfig, dbg]);

  // Process MPN pipeline
  const handleProcessMPN = useCallback(async () => {
    if (!canProcess || isProcessing) return;
    
    try {
      setProcessingState('running');
      setCurrentPipeline('MPN');
      setStartTime(Date.now());
      
      // Reset states
      setJoinDone(false);
      setExcelDone(false);
      setLogDone(false);
      setProcessed(0);
      processedRef.current = 0;
      setProgressPct(0);
      
      dbg('process:mpn:start');
      
      // Simulate processing with actual data
      const materialData = files.material.file?.data || [];
      const stockData = files.stock.file?.data || [];
      const priceData = files.price.file?.data || [];
      
      // Join data
      const joinedData: ProcessedRecord[] = [];
      
      for (const material of materialData) {
        const stock = stockData.find(s => s.Matnr === material.Matnr);
        const price = priceData.find(p => p.Matnr === material.Matnr);
        
        if (stock && price && material.ManufPartNr) {
          const pricing = computeFinalPrice({
            CustBestPrice: parseFloat(price.CustBestPrice) || 0,
            ListPrice: parseFloat(price.ListPrice) || 0,
            feeDrev: feeConfig.feeDrev,
            feeMkt: feeConfig.feeMkt
          });
          
          joinedData.push({
            Matnr: material.Matnr,
            ManufPartNr: material.ManufPartNr,
            EAN: material.EAN,
            ShortDescription: material.ShortDescription,
            ExistingStock: parseFloat(stock.ExistingStock) || 0,
            ListPrice: parseFloat(price.ListPrice) || 0,
            CustBestPrice: parseFloat(price.CustBestPrice) || 0,
            'Costo di Spedizione': DEFAULT_SHIPPING,
            'Prezzo con spediz e IVA': pricing.subtotConIva,
            FeeDeRev: feeConfig.feeDrev,
            'Fee Marketplace': feeConfig.feeMkt,
            'Subtotale post-fee': pricing.postFee,
            'Prezzo Finale': pricing.prezzoFinaleMPN, // Integer for MPN
            'ListPrice con Fee': pricing.listPriceConFee
          });
        }
        
        processedRef.current++;
        setProcessed(processedRef.current);
        setProgressPct((processedRef.current / materialData.length) * 80);
      }
      
      setCurrentProcessedData(joinedData);
      setFinalTotal(joinedData.length);
      
      // Update stats
      setCurrentStats({
        totalRecords: joinedData.length,
        validRecordsEAN: 0,
        validRecordsManufPartNr: joinedData.length,
        filteredRecordsEAN: 0,
        filteredRecordsManufPartNr: joinedData.length,
        stockDuplicates: 0,
        priceDuplicates: 0
      });
      
      setStats({
        totalRows: joinedData.length,
        validEAN: 0,
        validMPN: joinedData.length,
        discardedEAN: 0,
        discardedMPN: 0,
        duplicates: 0
      });
      
      setJoinDone(true);
      setExcelDone(true);
      setLogDone(true);
      
      dbg('process:mpn:complete', { valid: joinedData.length });
      
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Errore elaborazione MPN';
      dbg('process:mpn:error', { error: errorMessage });
      setProcessingState('failed');
      
      toast({
        title: "Errore elaborazione MPN",
        description: errorMessage,
        variant: "destructive"
      });
    }
  }, [canProcess, isProcessing, files, feeConfig, dbg]);

  // Export EAN Excel
  const handleExportEANExcel = useCallback(async () => {
    if (isExportingEAN) return;
    
    try {
      setIsExportingEAN(true);
      dbg('export:ean:start');

      if (!currentProcessedData || currentProcessedData.length === 0) {
        throw new Error('Nessun dato EAN da esportare');
      }

      // Validate all prices end with ,99
      const invalidPrices = currentProcessedData.filter(row => {
        const finalPrice = row['Prezzo Finale'];
        if (typeof finalPrice === 'string') {
          const priceValue = parseFloat(finalPrice.replace(',', '.'));
          return !validateEnding99(priceValue);
        }
        return false;
      });

      if (invalidPrices.length > 0) {
        console.warn(`Found ${invalidPrices.length} rows with invalid ,99 ending`);
      }

      // Prepare export data with renamed columns
      const exportData = currentProcessedData.map(row => ({
        ...row,
        'Prezzo con spediz e IVA': row['Prezzo con spediz e IVA'], // Keep renamed column
      }));

      exportWorkbook(exportData, 'catalogo_EAN.xlsx', ['EAN']);
      
      dbg('export:ean:success', { rows: exportData.length });
      toast({
        title: "Export completato",
        description: `File Excel EAN esportato con ${exportData.length} righe.`,
        variant: "default"
      });

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Errore export EAN';
      dbg('export:ean:error', { error: errorMessage });
      toast({
        title: "Errore export EAN",
        description: errorMessage,
        variant: "destructive"
      });
    } finally {
      setIsExportingEAN(false);
    }
  }, [currentProcessedData, isExportingEAN, dbg]);

  // Export MPN Excel
  const handleExportMPNExcel = useCallback(async () => {
    if (isExportingMPN) return;
    
    try {
      setIsExportingMPN(true);
      dbg('export:mpn:start');

      if (!currentProcessedData || currentProcessedData.length === 0) {
        throw new Error('Nessun dato MPN da esportare');
      }

      // Convert data for MPN export with proper pricing
      const mpnExportData = currentProcessedData.map(row => {
        const { prezzoFinaleMPN } = computeFinalPrice({
          CustBestPrice: row.CustBestPrice,
          ListPrice: row.ListPrice,
          feeDrev: feeConfig.feeDrev,
          feeMkt: feeConfig.feeMkt
        });

        return {
          ...row,
          'Prezzo Finale': prezzoFinaleMPN, // Integer for MPN
          'Prezzo con spediz e IVA': row['Prezzo con spediz e IVA'], // Keep renamed column
        };
      });

      exportWorkbook(mpnExportData, 'catalogo_MPN.xlsx', ['ManufPartNr']);
      
      dbg('export:mpn:success', { rows: mpnExportData.length });
      toast({
        title: "Export completato",
        description: `File Excel MPN esportato con ${mpnExportData.length} righe.`,
        variant: "default"
      });

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Errore export MPN';
      dbg('export:mpn:error', { error: errorMessage });
      toast({
        title: "Errore export MPN",
        description: errorMessage,
        variant: "destructive"
      });
    } finally {
      setIsExportingMPN(false);
    }
  }, [currentProcessedData, feeConfig, isExportingMPN, dbg]);

  return (
    <div className="container mx-auto p-6 space-y-6">
      <Card className="p-6">
        <h1 className="text-2xl font-bold mb-4">Generatore Catalogo Alterside</h1>
        
        {/* Fee Configuration */}
        <div className="mb-6 p-4 border border-input rounded-lg">
          <h3 className="text-lg font-semibold mb-3">Configurazione Fee</h3>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div>
              <Label htmlFor="feeDrev">Fee De Rev (%)</Label>
              <Input
                id="feeDrev"
                type="number"
                step="0.1"
                value={feeConfig.feeDrev}
                onChange={(e) => setFeeConfig(prev => ({ ...prev, feeDrev: parseFloat(e.target.value) || 0 }))}
                className="border-input focus-visible:ring-ring"
              />
            </div>
            <div>
              <Label htmlFor="feeMkt">Fee Marketplace (%)</Label>
              <Input
                id="feeMkt"
                type="number"
                step="0.1"
                value={feeConfig.feeMkt}
                onChange={(e) => setFeeConfig(prev => ({ ...prev, feeMkt: parseFloat(e.target.value) || 0 }))}
                className="border-input focus-visible:ring-ring"
              />
            </div>
            <div className="flex items-end">
              <div className="flex items-center space-x-2">
                <Checkbox
                  id="rememberFees"
                  checked={rememberFees}
                  onCheckedChange={(checked) => setRememberFees(checked as boolean)}
                />
                <Label htmlFor="rememberFees">Ricorda fee</Label>
              </div>
            </div>
          </div>
        </div>

        {/* File Upload Section */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
          {/* Material File */}
          <Card className="p-4">
            <div className="flex items-center gap-2 mb-2">
              <FileText className="h-5 w-5" />
              <h3 className="font-medium">File Material</h3>
              {files.material.status === 'valid' && <CheckCircle className="h-4 w-4 text-green-500" />}
              {files.material.status === 'error' && <XCircle className="h-4 w-4 text-red-500" />}
              {files.material.status === 'warning' && <AlertCircle className="h-4 w-4 text-yellow-500" />}
            </div>
            <input
              type="file"
              accept=".csv"
              onChange={(e) => {
                const file = e.target.files?.[0];
                if (file) handleFileUpload(file, 'material');
              }}
              className="w-full text-sm"
            />
            {files.material.file && (
              <p className="text-sm text-muted-foreground mt-1">{files.material.file.name}</p>
            )}
            {files.material.error && (
              <p className="text-sm text-red-500 mt-1">{files.material.error}</p>
            )}
            {files.material.warning && (
              <p className="text-sm text-yellow-600 mt-1">{files.material.warning}</p>
            )}
          </Card>

          {/* Stock File */}
          <Card className="p-4">
            <div className="flex items-center gap-2 mb-2">
              <FileText className="h-5 w-5" />
              <h3 className="font-medium">File Stock</h3>
              {files.stock.status === 'valid' && <CheckCircle className="h-4 w-4 text-green-500" />}
              {files.stock.status === 'error' && <XCircle className="h-4 w-4 text-red-500" />}
              {files.stock.status === 'warning' && <AlertCircle className="h-4 w-4 text-yellow-500" />}
            </div>
            <input
              type="file"
              accept=".csv"
              onChange={(e) => {
                const file = e.target.files?.[0];
                if (file) handleFileUpload(file, 'stock');
              }}
              className="w-full text-sm"
            />
            {files.stock.file && (
              <p className="text-sm text-muted-foreground mt-1">{files.stock.file.name}</p>
            )}
            {files.stock.error && (
              <p className="text-sm text-red-500 mt-1">{files.stock.error}</p>
            )}
            {files.stock.warning && (
              <p className="text-sm text-yellow-600 mt-1">{files.stock.warning}</p>
            )}
          </Card>

          {/* Price File */}
          <Card className="p-4">
            <div className="flex items-center gap-2 mb-2">
              <FileText className="h-5 w-5" />
              <h3 className="font-medium">File Price</h3>
              {files.price.status === 'valid' && <CheckCircle className="h-4 w-4 text-green-500" />}
              {files.price.status === 'error' && <XCircle className="h-4 w-4 text-red-500" />}
              {files.price.status === 'warning' && <AlertCircle className="h-4 w-4 text-yellow-500" />}
            </div>
            <input
              type="file"
              accept=".csv"
              onChange={(e) => {
                const file = e.target.files?.[0];
                if (file) handleFileUpload(file, 'price');
              }}
              className="w-full text-sm"
            />
            {files.price.file && (
              <p className="text-sm text-muted-foreground mt-1">{files.price.file.name}</p>
            )}
            {files.price.error && (
              <p className="text-sm text-red-500 mt-1">{files.price.error}</p>
            )}
            {files.price.warning && (
              <p className="text-sm text-yellow-600 mt-1">{files.price.warning}</p>
            )}
          </Card>
        </div>

        {/* Processing Controls */}
        <div className="flex gap-4 mb-6">
          <Button
            onClick={handleProcessEAN}
            disabled={!canProcess || isProcessing}
            className="flex items-center gap-2"
          >
            <Activity className="h-4 w-4" />
            Genera Catalogo EAN
          </Button>
          <Button
            onClick={handleProcessMPN}
            disabled={!canProcess || isProcessing}
            className="flex items-center gap-2"
          >
            <Activity className="h-4 w-4" />
            Genera Catalogo MPN
          </Button>
        </div>

        {/* Actions Section - Always Visible */}
        <Card className="p-4 mb-6">
          <h3 className="text-lg font-semibold mb-3">Azioni</h3>
          <div className="flex gap-4">
            <Button
              data-id="btn-ean"
              onClick={handleExportEANExcel}
              disabled={!isCompleted || currentPipeline !== 'EAN' || isExportingEAN}
              variant="default"
            >
              <Download className="h-4 w-4 mr-2" />
              {isExportingEAN ? 'Esportando...' : 'Download Excel EAN'}
            </Button>
            <Button
              data-id="btn-sku"
              onClick={handleExportMPNExcel}
              disabled={!isCompleted || currentPipeline !== 'MPN' || isExportingMPN}
              variant="default"
            >
              <Download className="h-4 w-4 mr-2" />
              {isExportingMPN ? 'Esportando...' : 'Download Excel MPN'}
            </Button>
          </div>
        </Card>

        {/* Progress Section */}
        {isProcessing && (
          <Card className="p-4 mb-6">
            <div className="flex items-center gap-2 mb-2">
              <Clock className="h-4 w-4" />
              <span className="font-medium">Elaborazione in corso...</span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-2 mb-2">
              <div 
                className="bg-primary h-2 rounded-full transition-all duration-300" 
                style={{ width: `${progressPct}%` }}
              ></div>
            </div>
            <div className="text-sm text-muted-foreground">
              {processed} / {finalTotal || total} righe elaborate ({progressPct.toFixed(1)}%)
            </div>
          </Card>
        )}

        {/* Data Table with Pagination */}
        {currentProcessedData.length > 0 && (
          <Card className="p-4">
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-lg font-semibold">
                Anteprima Dati ({currentProcessedData.length} righe)
              </h3>
              {totalPages > 1 && (
                <div className="text-sm text-muted-foreground">
                  Pagina {currentPage + 1} di {totalPages}
                </div>
              )}
            </div>
            
            <div className="overflow-x-auto">
              <table className="w-full border-collapse border border-input">
                <thead>
                  <tr className="bg-muted">
                    <th className="border border-input p-2 text-left">Matnr</th>
                    <th className="border border-input p-2 text-left">ManufPartNr</th>
                    <th className="border border-input p-2 text-left">EAN</th>
                    <th className="border border-input p-2 text-left">Descrizione</th>
                    <th className="border border-input p-2 text-left">Stock</th>
                    <th className="border border-input p-2 text-left">ListPrice</th>
                    <th className="border border-input p-2 text-left">CustBestPrice</th>
                    <th className="border border-input p-2 text-left">Prezzo con spediz e IVA</th>
                    <th className="border border-input p-2 text-left">ListPrice con Fee</th>
                    <th className="border border-input p-2 text-left">Prezzo Finale</th>
                  </tr>
                </thead>
                <tbody>
                  {pagedRows.map((row, index) => (
                    <tr key={index} className="hover:bg-muted/50">
                      <td className="border border-input p-2">{row.Matnr}</td>
                      <td className="border border-input p-2">{row.ManufPartNr}</td>
                      <td className="border border-input p-2">{row.EAN}</td>
                      <td className="border border-input p-2">{row.ShortDescription}</td>
                      <td className="border border-input p-2">{row.ExistingStock}</td>
                      <td className="border border-input p-2">{row.ListPrice}</td>
                      <td className="border border-input p-2">{row.CustBestPrice}</td>
                      <td className="border border-input p-2">{row['Prezzo con spediz e IVA']}</td>
                      <td className="border border-input p-2">{row['ListPrice con Fee']}</td>
                      <td className="border border-input p-2">{row['Prezzo Finale']}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Pagination Controls */}
            {totalPages > 1 && (
              <div className="mt-4 flex justify-center">
                <Pagination>
                  <PaginationContent>
                    <PaginationItem>
                      <PaginationPrevious 
                        onClick={() => setCurrentPage(Math.max(0, currentPage - 1))}
                        className={currentPage === 0 ? 'pointer-events-none opacity-50' : 'cursor-pointer'}
                      />
                    </PaginationItem>
                    
                    {Array.from({ length: Math.min(totalPages, 5) }, (_, i) => {
                      let pageNum;
                      if (totalPages <= 5) {
                        pageNum = i;
                      } else if (currentPage < 3) {
                        pageNum = i;
                      } else if (currentPage > totalPages - 4) {
                        pageNum = totalPages - 5 + i;
                      } else {
                        pageNum = currentPage - 2 + i;
                      }
                      
                      return (
                        <PaginationItem key={pageNum}>
                          <PaginationLink
                            onClick={() => setCurrentPage(pageNum)}
                            isActive={currentPage === pageNum}
                            className="cursor-pointer"
                          >
                            {pageNum + 1}
                          </PaginationLink>
                        </PaginationItem>
                      );
                    })}
                    
                    {totalPages > 5 && currentPage < totalPages - 3 && (
                      <PaginationItem>
                        <PaginationEllipsis />
                      </PaginationItem>
                    )}
                    
                    <PaginationItem>
                      <PaginationNext 
                        onClick={() => setCurrentPage(Math.min(totalPages - 1, currentPage + 1))}
                        className={currentPage === totalPages - 1 ? 'pointer-events-none opacity-50' : 'cursor-pointer'}
                      />
                    </PaginationItem>
                  </PaginationContent>
                </Pagination>
              </div>
            )}
          </Card>
        )}

        {/* Statistics */}
        {isCompleted && currentStats && (
          <Card className="p-4">
            <h3 className="text-lg font-semibold mb-3">Statistiche Elaborazione</h3>
            <div className="grid grid-cols-2 md:grid-cols-3 gap-4 text-sm">
              <div>
                <span className="font-medium">Righe Totali:</span> {currentStats.totalRecords}
              </div>
              <div>
                <span className="font-medium">EAN Validi:</span> {currentStats.validRecordsEAN}
              </div>
              <div>
                <span className="font-medium">MPN Validi:</span> {currentStats.validRecordsManufPartNr}
              </div>
              <div>
                <span className="font-medium">EAN Filtrati:</span> {currentStats.filteredRecordsEAN}
              </div>
              <div>
                <span className="font-medium">MPN Filtrati:</span> {currentStats.filteredRecordsManufPartNr}
              </div>
              <div>
                <span className="font-medium">Duplicati:</span> {currentStats.stockDuplicates + currentStats.priceDuplicates}
              </div>
            </div>
          </Card>
        )}

        {/* Debug Events */}
        {debugEvents.length > 0 && (
          <Card className="p-4">
            <h3 className="text-lg font-semibold mb-3">Eventi Debug</h3>
            <div className="max-h-40 overflow-y-auto text-xs font-mono">
              {debugEvents.slice(-20).map((event, index) => (
                <div key={index} className="py-1">{event}</div>
              ))}
            </div>
          </Card>
        )}
      </Card>
    </div>
  );
};

export default AltersideCatalogGenerator;
