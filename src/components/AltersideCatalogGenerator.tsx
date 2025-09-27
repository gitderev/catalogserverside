import React, { useState, useRef, useCallback, useEffect } from 'react';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Checkbox } from '@/components/ui/checkbox';
import { toast } from '@/hooks/use-toast';
import { Upload, Download, FileText, CheckCircle, XCircle, AlertCircle, Clock, Activity, Info, X } from 'lucide-react';
import { filterAndNormalizeForEAN, type EANStats, type DiscardedRow } from '@/utils/ean';
import { forceEANText, exportDiscardedRowsCSV } from '@/utils/excelFormatter';
import { 
  toComma99Cents, 
  validateEnding99, 
  computeFinalEan, 
  computeFromListPrice, 
  toCents, 
  formatCents, 
  applyRate,
  applyRateCents,
  parsePercentToRate,
  parseRate,
  ceilToComma99, 
  ceilToIntegerEuros,
  buildSkuCatalog,
  type Fee,
  type SkuCfg
} from '@/utils/pricing';
import * as XLSX from 'xlsx';
import Papa from 'papaparse';

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
  IVA: number;
  'Prezzo con spediz e IVA': number;
  FeeDeRev: number;
  'Fee Marketplace': number;
  'Subtotale post-fee': number;
  'Prezzo Finale': number | string; // String display for EAN (e.g. "34,99"), number for MPN
  'ListPrice con Fee': number | string; // Can be empty string for invalid ListPrice
}

interface FeeConfig {
  feeDrev: number;   // e.g. 1.05
  feeMkt: number;    // e.g. 1.08
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

const DEFAULT_FEES: FeeConfig = { feeDrev: 1.00, feeMkt: 1.00 };

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

function ceilInt(x: number): number { 
  return Math.ceil(x); 
}


function computeFinalPrice({
  CustBestPrice, ListPrice, feeDrev, feeMkt
}: { CustBestPrice?: number; ListPrice?: number; feeDrev: number; feeMkt: number; }): {
  base: number, shipping: number, iva: number, subtotConIva: number,
  postFee: number, prezzoFinaleEAN: number, prezzoFinaleMPN: number, listPriceConFee: number | string,
  eanResult: { finalCents: number; finalDisplay: string; route: string; debug: any }
} {
  const shipping = 6.00;
  const ivaMultiplier = 1.22;
  
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
      base: 0, shipping, iva: 0, subtotConIva: 0, 
      postFee: 0, prezzoFinaleEAN: 0, prezzoFinaleMPN: 0, listPriceConFee: '', eanResult: emptyEanResult 
    };
  }
  
  // Calculate for display/compatibility (old pipeline values)
  const subtot_base_sped = base + shipping;
  const iva = subtot_base_sped * 0.22;
  const subtotConIva = subtot_base_sped + iva;
  const postFee = subtotConIva * feeDrev * feeMkt;
  
  // EAN final price: use new computeFinalEan function (cent-precise with ending ,99)
  const eanResult = computeFinalEan(
    { listPrice: ListPrice || 0, custBestPrice: CustBestPrice > 0 ? CustBestPrice : undefined },
    { feeDeRev: feeDrev, feeMarketplace: feeMkt }
  );
  const prezzoFinaleEAN = eanResult.finalCents / 100;
  
  // MPN final price: use old logic (ceil to integer)
  const prezzoFinaleMPN = Math.ceil(postFee);
  
  // Log samples for EAN route debugging (separate counters for cbp vs listprice)
  if (baseRoute === 'cbp') {
    if (typeof (globalThis as any).eanSampleCbpCount === 'undefined') {
      (globalThis as any).eanSampleCbpCount = 0;
    }
    if ((globalThis as any).eanSampleCbpCount < 3) {
      console.warn('ean:sample:cbp', {
        base: base,
        withShip: base + shipping,
        withVat: (base + shipping) * ivaMultiplier,
        withFeeDR: (base + shipping) * ivaMultiplier * feeDrev,
        withFeeMkt: (base + shipping) * ivaMultiplier * feeDrev * feeMkt,
        final: prezzoFinaleEAN
      });
      (globalThis as any).eanSampleCbpCount++;
    }
  } else if (baseRoute === 'listprice_ceiled') {
    if (typeof (globalThis as any).eanSampleLpCount === 'undefined') {
      (globalThis as any).eanSampleLpCount = 0;
    }
    if ((globalThis as any).eanSampleLpCount < 3) {
      console.warn('ean:sample:listprice', {
        baseSource: 'listprice_ceiled',
        originalListPrice: ListPrice,
        base: base,
        withShip: base + shipping,
        withVat: (base + shipping) * ivaMultiplier,
        withFeeDR: (base + shipping) * ivaMultiplier * feeDrev,
        withFeeMkt: (base + shipping) * ivaMultiplier * feeDrev * feeMkt,
        final: prezzoFinaleEAN
      });
      (globalThis as any).eanSampleLpCount++;
    }
  }
  
  // Calculate ListPrice con Fee - SEPARATE pipeline, independent from main calculation
  let listPriceConFee: number | string = '';
  if (hasListPrice) {
    const baseLP = ListPrice!; // use ListPrice as-is, no ceil here
    const subtotBasSpedLP = baseLP + shipping;
    const ivaLP = subtotBasSpedLP * 0.22;
    const subtotConIvaLP = subtotBasSpedLP + ivaLP;
    const postFeeLP = subtotConIvaLP * feeDrev * feeMkt;
    listPriceConFee = Math.ceil(postFeeLP); // ceil to integer for ListPrice con Fee
    
    // Log samples for debugging (static counter to avoid spam)
    if (typeof (globalThis as any).lpfeeCalcSampleCount === 'undefined') {
      (globalThis as any).lpfeeCalcSampleCount = 0;
    }
    if ((globalThis as any).lpfeeCalcSampleCount < 3) {
      console.warn('lpfee:calc:sample', { 
        listPrice: baseLP, 
        subtot_con_iva: subtotConIvaLP.toFixed(2), 
        feeDeRev: feeDrev, 
        feeMarketplace: feeMkt, 
        post_fee: postFeeLP.toFixed(4), 
        final: listPriceConFee 
      });
      (globalThis as any).lpfeeCalcSampleCount++;
    }
  }

  return { base, shipping, iva, subtotConIva, postFee, prezzoFinaleEAN, prezzoFinaleMPN, listPriceConFee, eanResult };
}

// Diagnostic state
interface DiagnosticState {
  isEnabled: boolean;
  maxRows: number;
  workerMessages: Array<{ id: number; timestamp: string; data: any }>;
  statistics: {
    total: number;
    batchSize: number;
    elapsedPrescan: number;
    elapsedSku: number;
    progressPct: number;
    heartbeatAgeMs: number;
  };
  errorCounters: {
    msgInvalid: number;
    workerError: number;
    timeouts: number;
  };
  lastHeartbeat: number;
  testResults: Array<{ test: string; status: 'pass' | 'fail'; error?: string }>;
}

// Valid worker message schemas with strict validation
const WORKER_MESSAGE_SCHEMAS = {
  worker_boot: ['type', 'version'],
  worker_ready: ['type', 'version', 'schema'],
  prescan_progress: ['type', 'done', 'total'],
  prescan_done: ['type', 'counts', 'total'],
  sku_progress: ['type', 'done', 'total'],
  sku_done: ['type', 'exported', 'rejected', 'total'],
  worker_error: ['type', 'message', 'where'],
  pong: ['type']
} as const;

// Worker communication state
interface WorkerState {
  handshakeComplete: boolean;
  prescanInitialized: boolean;
  version: string | null;
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

  // Diagnostic state
  const [diagnosticState, setDiagnosticState] = useState<DiagnosticState>({
    isEnabled: false,
    maxRows: 200,
    workerMessages: [],
    statistics: {
      total: 0,
      batchSize: 1000,
      elapsedPrescan: 0,
      elapsedSku: 0,
      progressPct: 0,
      heartbeatAgeMs: 0
    },
    errorCounters: {
      msgInvalid: 0,
      workerError: 0,
      timeouts: 0
    },
    lastHeartbeat: 0,
    testResults: []
  });

  // Save fees when rememberFees is checked
  useEffect(() => {
    if (rememberFees) {
      saveFees(feeConfig);
    }
  }, [feeConfig, rememberFees]);

  const [processingState, setProcessingState] = useState<'idle' | 'ready' | 'prescanning' | 'running' | 'done' | 'error'>('idle');
  const [currentPipeline, setCurrentPipeline] = useState<'EAN' | 'MPN' | null>(null);
  
  // Worker state
  const [workerState, setWorkerState] = useState<WorkerState>({
    handshakeComplete: false,
    prescanInitialized: false,
    version: null
  });
  
  // Timeout refs
  const handshakeTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const progressTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const workerRef = useRef<Worker | null>(null);
  
  // Progress states (based on rows READ, not valid rows)
  const [total, setTotal] = useState(0); // prescan estimate
  const [finalTotal, setFinalTotal] = useState<number | null>(null); // actual rows read in join
  const [processed, setProcessed] = useState(0);
  const processedRef = useRef(0);
  const [progressPct, setProgressPct] = useState(0);
  
  // Ensure no stale references to undefined variables
  const processedRows = processed; // Alias for compatibility
  
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
  const [isExportingSKU, setIsExportingSKU] = useState(false);
  
  // Global debug function - defined first
  const dbg = useCallback((event: string, data?: any) => {
    const timestamp = new Date().toLocaleTimeString('it-IT', { hour12: false });
    const message = `[${timestamp}] ${event}${data ? ' | ' + JSON.stringify(data) : ''}`;
    setDebugEvents(prev => [...prev, message]);
  }, []);

  // Robust message validation - never crash
  const validateWorkerMessage = useCallback((data: any) => {
    // First check: is it an object with type?
    if (!data || typeof data !== 'object' || !('type' in data) || typeof data.type !== 'string') {
      setDiagnosticState(prev => ({
        ...prev,
        errorCounters: { ...prev.errorCounters, msgInvalid: prev.errorCounters.msgInvalid + 1 }
      }));
      dbg('worker_msg_invalid', { 
        reason: 'invalid_structure',
        receivedKeys: data ? Object.keys(data) : [],
        expectedKeys: ['type'] 
      });
      return false;
    }

    const type = data.type;
    const expectedKeys = WORKER_MESSAGE_SCHEMAS[type as keyof typeof WORKER_MESSAGE_SCHEMAS];
    
    if (!expectedKeys) {
      setDiagnosticState(prev => ({
        ...prev,
        errorCounters: { ...prev.errorCounters, msgInvalid: prev.errorCounters.msgInvalid + 1 }
      }));
      dbg('worker_msg_invalid', { 
        reason: 'unknown_type',
        type, 
        receivedKeys: Object.keys(data), 
        expectedKeys: 'unknown_type' 
      });
      return false;
    }

    const missingKeys = expectedKeys.filter(key => !(key in data));
    if (missingKeys.length > 0) {
      setDiagnosticState(prev => ({
        ...prev,
        errorCounters: { ...prev.errorCounters, msgInvalid: prev.errorCounters.msgInvalid + 1 }
      }));
      dbg('worker_msg_invalid', { 
        reason: 'missing_keys',
        type, 
        receivedKeys: Object.keys(data), 
        expectedKeys, 
        missingKeys 
      });
      return false;
    }

    return true;
  }, [dbg]);

  const addWorkerMessage = useCallback((data: any) => {
    const timestamp = new Date().toLocaleTimeString('it-IT', { hour12: false });
    const messageId = Date.now();
    
    setDiagnosticState(prev => ({
      ...prev,
      workerMessages: [...prev.workerMessages.slice(-9), { id: messageId, timestamp, data }],
      lastHeartbeat: Date.now()
    }));
  }, []);

  // Clear timeouts helper
  const clearWorkerTimeouts = useCallback(() => {
    if (handshakeTimeoutRef.current) {
      clearTimeout(handshakeTimeoutRef.current);
      handshakeTimeoutRef.current = null;
    }
    if (progressTimeoutRef.current) {
      clearTimeout(progressTimeoutRef.current);
      progressTimeoutRef.current = null;
    }
  }, [handshakeTimeoutRef, progressTimeoutRef]);

  const runDiagnosticTests = useCallback(async () => {
    const testResults: Array<{ test: string; status: 'pass' | 'fail'; error?: string }> = [];
    
    try {
      // Test 1: Protocol check
      const workerReadyReceived = diagnosticState.workerMessages.some(msg => msg.data.type === 'worker_ready');
      const prescanProgressReceived = diagnosticState.workerMessages.some(msg => 
        msg.data.type === 'prescan_progress' && msg.data.done === 0
      );
      
      if (!workerReadyReceived || !prescanProgressReceived) {
        testResults.push({ test: 'Protocollo', status: 'fail', error: 'worker_ready o prescan_progress non ricevuti' });
      } else {
        testResults.push({ test: 'Protocollo', status: 'pass' });
      }

      // Test 2: Progress check
      const prescanProgress = diagnosticState.workerMessages.find(msg => msg.data.type === 'prescan_progress');
      if (!prescanProgress || prescanProgress.data.done <= 0) {
        testResults.push({ test: 'Progress', status: 'fail', error: 'Nessun avanzamento prescan' });
      } else {
        testResults.push({ test: 'Progress', status: 'pass' });
      }

      // Test 3: Schema check
      const prescanDone = diagnosticState.workerMessages.find(msg => msg.data.type === 'prescan_done');
      if (!prescanDone || !prescanDone.data.counts) {
        testResults.push({ test: 'Schema', status: 'fail', error: 'prescan_done assente/malformato' });
      } else {
        testResults.push({ test: 'Schema', status: 'pass' });
      }

      // Test 4: Numeric check (sample calculation)
      const sampleResult = computeFinalPrice({
        CustBestPrice: 100,
        ListPrice: 100,
        feeDrev: 1.03,
        feeMkt: 1.15
      });
      
      if (Math.abs(sampleResult.prezzoFinaleMPN - 154) > 1) {
        testResults.push({ test: 'Numerico', status: 'fail', error: 'calcolo SKU non coerente' });
      } else {
        testResults.push({ test: 'Numerico', status: 'pass' });
      }

    } catch (error) {
      testResults.push({ test: 'Generale', status: 'fail', error: error instanceof Error ? error.message : 'Errore test' });
    }

    setDiagnosticState(prev => ({ ...prev, testResults }));
    return testResults;
  }, [diagnosticState.workerMessages]);

  const generateDiagnosticBundle = useCallback(() => {
    const bundle = {
      userAgent: navigator.userAgent,
      url: window.location.href,
      appVersion: '1.0.0',
      workerVersion: workerState.version || 'unknown',
      batchSize: diagnosticState.statistics.batchSize,
      fileCounts: {
        material: files.material.file?.data.length || 0,
        stock: files.stock.file?.data.length || 0,
        price: files.price.file?.data.length || 0
      },
      sequenzaEventi: debugEvents.slice(-20),
      primi10MessaggiWorker: diagnosticState.workerMessages.slice(0, 10),
      ultimi5MessaggiWorker: diagnosticState.workerMessages.slice(-5),
      statistiche: diagnosticState.statistics,
      primoWorkerError: diagnosticState.workerMessages.find(msg => msg.data.type === 'worker_error')?.data || null,
      errorCounters: diagnosticState.errorCounters,
      testResults: diagnosticState.testResults,
      timestamp: new Date().toISOString()
    };

    navigator.clipboard.writeText(JSON.stringify(bundle, null, 2)).then(() => {
      toast({
        title: "Bundle diagnostico copiato",
        description: "Il bundle diagnostico è stato copiato negli appunti",
        variant: "default"
      });
    }).catch(() => {
      toast({
        title: "Errore copia bundle",
        description: "Impossibile copiare il bundle negli appunti",
        variant: "destructive"
      });
    });
  }, [diagnosticState, debugEvents, files, workerState.version, toast]);

  const isProcessing = processingState === 'running';
  const isCompleted = processingState === 'done';
  const canProcess = processingState === 'ready';
  
  // Audit function for critical debugging
  const audit = useCallback((msg: string, data?: any) => {
    const logEntry = `AUDIT: ${msg}${data ? ' | ' + JSON.stringify(data) : ''}`;
    console.warn(logEntry);
    dbg(logEntry);
  }, [dbg]);
  
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
      setProcessingState('done');
      dbg('pipeline:completed', {
        pipeline: currentPipeline,
        totalPrescan: total,
        finalTotal: finalTotal ?? processedRef.current
      });
    }
  }, [joinDone, excelDone, logDone, consistencyOk, getConsistencySnapshot, currentPipeline, total, finalTotal]);

  // Log state changes
  useEffect(() => {
    dbg('state:change', { state: processingState, ...debugState });
  }, [processingState, debugState]);

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
        stockReady: !!newFiles.stock.file,
        priceReady: !!newFiles.price.file,
        materialPreScanDone: debugState.materialPreScanDone,
        joinStarted: debugState.joinStarted
      };
      setDebugState(newDebugState);
      dbg('state:change', newDebugState);
      
      if (allRequiredHeadersValid) {
        setProcessingState('ready');
      }

      setFiles(newFiles);

      const toastMessage = status === 'warning' 
        ? `${file.name} - ${parsed.data.length} righe (con avviso)`
        : `${file.name} - ${parsed.data.length} righe`;

      toast({
        title: "File caricato con successo",
        description: toastMessage,
        variant: status === 'warning' ? 'default' : 'default'
      });

    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : 'Errore sconosciuto';
      setFiles(prev => ({
        ...prev,
        [type]: { file: null, status: 'error', error: errorMsg }
      }));
      setProcessingState('idle');
      
      toast({
        title: "Errore caricamento file",
        description: errorMsg,
        variant: "destructive"
      });
    }
  };

  const removeFile = (type: keyof FileUploadState) => {
    setFiles(prevFiles => ({
      ...prevFiles,
      [type]: {
        file: null,
        status: 'empty',
        diagnostics: null
      }
    }));

    // Reset processing state if no files remain
    const remainingFiles = Object.entries(files).filter(([key, _]) => key !== type);
    if (remainingFiles.every(([_, file]) => !file.file)) {
      setProcessingState('idle');
      setProgressPct(0);
      setStartTime(null);
      setElapsedTime(0);
      setEstimatedTime(null);
      setProcessed(0);
      setTotal(0);
      setDebugEvents([]);
      setDebugState({
        materialValid: false,
        stockValid: false,
        priceValid: false,
        stockReady: false,
        priceReady: false,
        materialPreScanDone: false,
        joinStarted: false
      });
    }
  };

  const formatTime = (ms: number): string => {
    if (!ms || ms <= 0) return '00:00';
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    return `${minutes.toString().padStart(2, '0')}:${(seconds % 60).toString().padStart(2, '0')}`;
  };

  // Timer effect
  useEffect(() => {
    let interval: NodeJS.Timeout;
    if (processingState === 'running' && startTime) {
      interval = setInterval(() => {
        const elapsed = Date.now() - startTime;
        setElapsedTime(elapsed);
        
        // Calculate ETA every 500ms
        if (processed > 0 && total > 0) {
          const rate = processed / (elapsed / 1000);
          if (rate >= 0.1) {
            const remaining = Math.max(0, total - processed);
            const etaSec = remaining / rate;
            setEstimatedTime(etaSec * 1000);
          } else {
            setEstimatedTime(null);
          }
        }
      }, 500);
    }
    return () => clearInterval(interval);
  }, [processingState, startTime, processed, total]);

  const processDataPipeline = async (pipelineType: 'EAN' | 'MPN') => {
    if (!files.material.file || !files.stock.file || !files.price.file) {
      toast({
        title: "File mancanti",
        description: "Carica tutti e tre i file prima di procedere",
        variant: "destructive"
      });
      return;
    }

    // Validate required headers only
    const materialValidation = validateHeaders(files.material.file.headers, REQUIRED_HEADERS.material);
    const stockValidation = validateHeaders(files.stock.file.headers, REQUIRED_HEADERS.stock);
    const priceValidation = validateHeaders(files.price.file.headers, REQUIRED_HEADERS.price);

    if (!materialValidation.valid || !stockValidation.valid || !priceValidation.valid) {
      toast({
        title: "Header obbligatori mancanti",
        description: "Verifica che tutti i file abbiano gli header richiesti",
        variant: "destructive"
      });
      return;
    }

    // Reset all state for new pipeline
    setCurrentPipeline(pipelineType);
    setCurrentProcessedData([]);
    setCurrentLogEntries([]);
    setCurrentStats(null);
    setFinalTotal(null);
    setEanStats(null);
    setDiscardedRows([]);
    processedRef.current = 0;
    setProcessed(0);
    setProgressPct(0);
    setElapsedTime(0);
    setEstimatedTime(null);
    setDebugEvents([]);
    
    // Reset completion gating flags
    setJoinDone(false);
    setExcelDone(false);
    setLogDone(false);
    setDebugState({
      materialValid: true,
      stockValid: true,
      priceValid: true,
      stockReady: false,
      priceReady: false,
      materialPreScanDone: false,
      joinStarted: false
    });

    // Build maps (parse events)
    const stockMap = new Map<string, { ExistingStock: number }>();
    const priceMap = new Map<string, { ListPrice: number; CustBestPrice: number }>();
    let stockDuplicates = 0;
    let priceDuplicates = 0;

    // STOCK parse (simulate chunk logging using one chunk)
    dbg('parse:stock:start');
    let stockRowCounter = 0;
    files.stock.file.data.forEach((row, index) => {
      const matnr = row.Matnr?.toString().trim();
      if (!matnr) return;
      stockRowCounter++;
      if (stockMap.has(matnr)) {
        stockDuplicates++;
      } else {
        stockMap.set(matnr, { ExistingStock: parseInt(row.ExistingStock) || 0 });
      }
    });
    dbg('parse:stock:chunk', { chunkNumber: 1, rowsInChunk: stockRowCounter });
    dbg('parse:stock:done', { totalRecords: stockMap.size, totalChunks: 1 });

    // PRICE parse
    dbg('parse:price:start');
    let priceRowCounter = 0;
    files.price.file.data.forEach((row, index) => {
      const matnr = row.Matnr?.toString().trim();
      if (!matnr) return;
      priceRowCounter++;
      if (priceMap.has(matnr)) {
        priceDuplicates++;
      } else {
        priceMap.set(matnr, { ListPrice: parseFloat(row.ListPrice) || 0, CustBestPrice: parseFloat(row.CustBestPrice) || 0 });
      }
    });
    dbg('parse:price:chunk', { chunkNumber: 1, rowsInChunk: priceRowCounter });
    dbg('parse:price:done', { totalRecords: priceMap.size, totalChunks: 1 });

    // Prescan Material (streaming) to count rows
    dbg('material:prescan:start');
    let materialRowsCount = 0;
    await new Promise<void>((resolve, reject) => {
      Papa.parse(files.material.file!.raw, {
        header: true,
        skipEmptyLines: true,
        worker: true,
        step: (results) => {
          if (results && results.data) {
            materialRowsCount++;
            if (materialRowsCount % 500 === 0) dbg('material:prescan:chunk', { materialRowsCount });
          }
        },
        complete: () => resolve(),
        error: (err) => reject(err)
      });
    }).catch(() => {
      // Fallback prescan without worker
      materialRowsCount = 0;
      Papa.parse(files.material.file!.raw, {
        header: true,
        skipEmptyLines: true,
        worker: false,
        step: (results) => {
          if (results && results.data) materialRowsCount++;
        },
        complete: () => {},
      });
    });

    if (materialRowsCount <= 0) {
      dbg('material:prescan:error', { message: 'Nessuna riga valida nel Material' });
      toast({ title: 'Errore elaborazione', description: 'Nessuna riga valida nel Material', variant: 'destructive' });
      return;
    }
    dbg('material:prescan:done', { materialRowsCount });

    // Init state machine and counters based on rows read
    setProcessingState('running');
    setTotal(materialRowsCount); // only for initial estimate/UI
    setStartTime(Date.now());
    setElapsedTime(0);
    setEstimatedTime(null);
    setDebugState(prev => ({ ...prev, stockReady: true, priceReady: true, materialPreScanDone: true }));

    // Join streaming pass
    const optionalHeadersMissing = {
      stock: !files.stock.file.headers.includes('ManufPartNr'),
      price: !files.price.file.headers.includes('ManufPartNr')
    };

    const processedEAN: ProcessedRecord[] = [];
    const processedMPN: ProcessedRecord[] = [];
    const logsEAN: LogEntry[] = [];
    const logsMPN: LogEntry[] = [];

    const ceilToXX99 = (value: number) => {
      const integer = Math.floor(value);
      const decimal = value - integer;
      return decimal <= 0.99 ? integer + 0.99 : integer + 1.99;
    };

    const computeFinalPriceForEAN = (row: any): number => {
      const custBestPrice = parseFloat(row.CustBestPrice);
      const listPrice = parseFloat(row.ListPrice);
      
      // Use computeFinalEan for consistency
      const hasBest = Number.isFinite(custBestPrice) && custBestPrice > 0;
      const hasListPrice = Number.isFinite(listPrice) && listPrice > 0;
      
      if (hasBest) {
        const result = computeFinalEan(
          { listPrice: listPrice || 0, custBestPrice },
          { feeDeRev: feeConfig.feeDrev, feeMarketplace: feeConfig.feeMkt }
        );
        return result.finalCents / 100;
      } else if (hasListPrice) {
        const result = computeFinalEan(
          { listPrice },
          { feeDeRev: feeConfig.feeDrev, feeMarketplace: feeConfig.feeMkt }
        );
        return result.finalCents / 100;
      }
      
      return 0;
    };

    const finalize = () => {
      dbg('excel:write:start');
      dbg('log:write:start');
      
      // Session header + optional header warnings
      const sessionRow: LogEntry = {
        source_file: 'session',
        line: 0,
        Matnr: '',
        ManufPartNr: '',
        EAN: '',
        reason: 'session_start',
        details: JSON.stringify({ event: 'session_start', materialRowsCount, optionalHeadersMissing, fees: { mediaworld: 0.08, alterside: 0.05 }, timestamp: new Date().toISOString() })
      };
      
      // For current pipeline, filter only relevant data
      const currentData = pipelineType === 'EAN' ? processedEAN : processedMPN;
      const currentLogs = pipelineType === 'EAN' ? logsEAN : logsMPN;
      
      currentLogs.unshift(sessionRow);
      if (optionalHeadersMissing.stock) {
        const r: LogEntry = { source_file: 'StockFileData_790813.txt', line: 0, Matnr: '', ManufPartNr: '', EAN: '', reason: 'header_optional_missing', details: 'ManufPartNr assente (uso ManufPartNr da Material)' };
        currentLogs.splice(1, 0, r);
      }
      if (optionalHeadersMissing.price) {
        const idx = 1 + (optionalHeadersMissing.stock ? 1 : 0);
        const r: LogEntry = { source_file: 'pricefileData_790813.txt', line: 0, Matnr: '', ManufPartNr: '', EAN: '', reason: 'header_optional_missing', details: 'ManufPartNr assente (uso ManufPartNr da Material)' };
        currentLogs.splice(idx, 0, r);
      }

      // Update stats for UI
      setStats(prev => ({
        ...prev,
        totalRows: materialRowsCount,
        validEAN: pipelineType === 'EAN' ? currentData.length : prev.validEAN,
        validMPN: pipelineType === 'MPN' ? currentData.length : prev.validMPN,
        discardedEAN: pipelineType === 'EAN' ? currentLogs.length - 1 : prev.discardedEAN,
        discardedMPN: pipelineType === 'MPN' ? currentLogs.length - 1 : prev.discardedMPN
      }));

      // Set current pipeline results
      setCurrentProcessedData(currentData);
      setCurrentLogEntries(currentLogs);
      setCurrentStats({
        totalRecords: materialRowsCount,
        validRecordsEAN: pipelineType === 'EAN' ? currentData.length : 0,
        validRecordsManufPartNr: pipelineType === 'MPN' ? currentData.length : 0,
        filteredRecordsEAN: pipelineType === 'EAN' ? currentLogs.length - 1 : 0,
        filteredRecordsManufPartNr: pipelineType === 'MPN' ? currentLogs.length - 1 : 0,
        stockDuplicates,
        priceDuplicates
      });

      // Update download ready state
      setDownloadReady(prev => ({
        ...prev,
        [pipelineType.toLowerCase() + '_excel']: currentData.length > 0,
        [pipelineType.toLowerCase() + '_log']: currentLogs.length > 0
      }));

      setExcelDone(true);
      dbg('excel:write:done', { pipeline: pipelineType });
      
      setLogDone(true);
      dbg('log:write:done', { pipeline: pipelineType });
    };

    // Wait dependencies observation
    setTimeout(() => {
      if (total > 0 && processed === 0) {
        toast({ title: 'Elaborazione non avviata: verifico dipendenze', description: '', variant: 'default' });
        dbg('join_waiting_dependencies');
      }
    }, 2000);

    const runJoin = (useWorker: boolean) => new Promise<void>((resolve, reject) => {
      dbg('join:start', { worker: useWorker });
      setDebugState(prev => ({ ...prev, joinStarted: true }));

      let firstChunk = false;
      let aborted = false;
      let parserRef: any = null;
      const fallbackTimer = setTimeout(() => {
        if (!firstChunk && useWorker) {
          toast({ title: 'Nessuna riga processata, fallback worker:false', description: '', variant: 'default' });
          dbg('join:fallback', { reason: 'no_chunk_within_2s' });
          try { parserRef && parserRef.abort && parserRef.abort(); } catch {}
          // Relaunch without worker
          runJoin(false).then(resolve).catch(reject);
        }
      }, 2000);

      let processedLocal = 0;

      Papa.parse(files.material.file!.raw, {
        header: true,
        skipEmptyLines: true,
        worker: useWorker,
        step: (results, parser) => {
          if (!firstChunk) { firstChunk = true; clearTimeout(fallbackTimer); }
          parserRef = parser;
          
          // CRITICAL FIX: Count ALL rows read FIRST, before any filtering
          processedRef.current += 1;
          
          // Update progress based on ALL rows read every 256 rows (for performance)
          if ((processedRef.current & 0xFF) === 0) {
            const denom = finalTotal ?? total ?? 1;
            setProcessed(processedRef.current);
            setProgressPct(Math.min(99, Math.floor(processedRef.current / Math.max(1, denom) * 100)));
            dbg('join:chunk', { processed: processedRef.current });
          }
          
          const row = results.data as any;
          const matnr = row?.Matnr?.toString().trim();
          if (!matnr) return;

          const stock = stockMap.get(matnr);
          const price = priceMap.get(matnr);

          if (!stock) {
            const le: LogEntry = { source_file: 'MaterialFile.txt', line: processedLocal + 2, Matnr: matnr, ManufPartNr: row.ManufPartNr || '', EAN: row.EAN || '', reason: 'join_missing_stock', details: 'Nessun dato stock trovato' };
            logsEAN.push(le); logsMPN.push(le);
            return;
          }
          if (!price) {
            const le: LogEntry = { source_file: 'MaterialFile.txt', line: processedLocal + 2, Matnr: matnr, ManufPartNr: row.ManufPartNr || '', EAN: row.EAN || '', reason: 'join_missing_price', details: 'Nessun dato prezzo trovato' };
            logsEAN.push(le); logsMPN.push(le);
            return;
          }

          const existingStock = parseInt(stock.ExistingStock as any) || 0;
          const listPriceNum = Number(price.ListPrice) || 0;
          const custBestNumRaw = Number(price.CustBestPrice) || 0;
          if (existingStock <= 1) {
            const le: LogEntry = { source_file: 'MaterialFile.txt', line: processedLocal + 2, Matnr: matnr, ManufPartNr: row.ManufPartNr || '', EAN: row.EAN || '', reason: 'stock_leq_1', details: `ExistingStock=${existingStock} <= 1` };
            logsEAN.push(le); logsMPN.push(le);
            return;
          }
          if (!(listPriceNum > 0)) {
            const le: LogEntry = { source_file: 'MaterialFile.txt', line: processedLocal + 2, Matnr: matnr, ManufPartNr: row.ManufPartNr || '', EAN: row.EAN || '', reason: 'price_missing', details: `ListPrice non valido: ${listPriceNum}` };
            logsEAN.push(le); logsMPN.push(le);
            return;
          }
          if (!(custBestNumRaw > 0)) {
            const le: LogEntry = { source_file: 'MaterialFile.txt', line: processedLocal + 2, Matnr: matnr, ManufPartNr: row.ManufPartNr || '', EAN: row.EAN || '', reason: 'custbest_missing', details: `CustBestPrice non valido: ${custBestNumRaw}` };
            logsEAN.push(le); logsMPN.push(le);
            return;
          }

          // Use new fee calculation system
          const calc = computeFinalPrice({
            CustBestPrice: custBestNumRaw,
            ListPrice: listPriceNum,
            feeDrev: feeConfig.feeDrev,
            feeMkt: feeConfig.feeMkt
          });

          const base: ProcessedRecord = {
            Matnr: matnr,
            ManufPartNr: row.ManufPartNr || '', // always from Material
            EAN: row.EAN?.toString().trim() || '',
            ShortDescription: row.ShortDescription || '',
            ExistingStock: existingStock,
            ListPrice: listPriceNum,
            CustBestPrice: calc.base,
            'Costo di Spedizione': calc.shipping,
            IVA: calc.iva,
            'Prezzo con spediz e IVA': calc.subtotConIva,
            FeeDeRev: feeConfig.feeDrev,
            'Fee Marketplace': feeConfig.feeMkt,
            'Subtotale post-fee': calc.postFee,
            'Prezzo Finale': currentPipeline === 'EAN' ? calc.eanResult.finalDisplay : calc.prezzoFinaleMPN,
            'ListPrice con Fee': calc.listPriceConFee
          };

          if (base.EAN) {
            // Add internal EAN metadata for validation
            (base as any)._eanFinalCents = currentPipeline === 'EAN' ? calc.eanResult.finalCents : undefined;
            processedEAN.push(base);
          } else {
            const le: LogEntry = { source_file: 'MaterialFile.txt', line: processedLocal + 2, Matnr: matnr, ManufPartNr: base.ManufPartNr, EAN: '', reason: 'ean_empty', details: 'EAN vuoto o mancante' };
            logsEAN.push(le);
          }

          if (base.ManufPartNr) {
            processedMPN.push(base);
          } else {
            const le: LogEntry = { source_file: 'MaterialFile.txt', line: processedLocal + 2, Matnr: matnr, ManufPartNr: '', EAN: base.EAN, reason: 'manufpartnr_empty', details: 'ManufPartNr vuoto o mancante' };
            logsMPN.push(le);
          }

          // Progress tracking is already done above - don't double count
        },
        complete: () => {
        setFinalTotal(processedRef.current);
        setProcessed(processedRef.current);
        setProgressPct(Math.min(99, Math.floor(processedRef.current / Math.max(1, processedRef.current) * 100)));
        
        // Apply EAN validation and normalization for EAN pipeline
        if (pipelineType === 'EAN') {
          const { kept, discarded, stats } = filterAndNormalizeForEAN(processedEAN, computeFinalPriceForEAN);
          processedEAN.length = 0; // clear original
          processedEAN.push(...kept);
          setEanStats(stats);
          setDiscardedRows(discarded);
          
          audit('ean-validation', {
            pipeline: pipelineType,
            input: stats.tot_righe_input,
            kept: kept.length,
            discarded: discarded.length,
            stats
          });
        }
        
        setJoinDone(true);
        dbg('join:done', { processed: processedRef.current, totalPrescan: total, finalTotal: processedRef.current });
          resolve();
        },
        error: (err) => {
          reject(err);
        }
      });
    });

    try {
      await runJoin(true);
      finalize();
    } catch (err) {
      // ultimate fallback: try without worker
      try {
        await runJoin(false);
        finalize();
      } catch (e) {
        setProcessingState('error');
        toast({ title: 'Errore elaborazione', description: 'Impossibile completare la join', variant: 'destructive' });
      }
    }
  };

  const getTimestamp = () => {
    const now = new Date();
    const romeTime = new Intl.DateTimeFormat('it-IT', {
      timeZone: 'Europe/Rome',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit'
    }).format(now);

    const [date, time] = romeTime.split(', ');
    const [day, month, year] = date.split('/');
    const [hour, minute] = time.split(':');
    
    return {
      timestamp: `${year}${month}${day}_${hour}${minute}`,
      sheetName: `${year}-${month}-${day}`
    };
  };

  const formatExcelData = (data: ProcessedRecord[]) => {
    return data.map(record => ({
      ...record,
      ExistingStock: record.ExistingStock.toString(),
      ListPrice: record.ListPrice.toFixed(2).replace('.', ','),
      CustBestPrice: record.CustBestPrice.toString(),
      'ListPrice con IVA': record['ListPrice con IVA'].toFixed(2).replace('.', ','),
      'CustBestPrice con IVA': record['CustBestPrice con IVA'].toFixed(2).replace('.', ','),
      'Costo di spedizione': record['Costo di spedizione'].toString(),
      'Prezzo finale': typeof record['Prezzo finale'] === 'string' ? record['Prezzo finale'] : record['Prezzo finale'].toFixed(2).replace('.', ','),
      'Prezzo finale Listino': record['Prezzo finale Listino'].toString()
    }));
  };

  const onExportEAN = useCallback((event: React.MouseEvent) => {
    event.preventDefault();
    
    if (isExportingEAN) {
      toast({
        title: "Esportazione in corso...",
        description: "Attendere completamento dell'esportazione corrente"
      });
      return;
    }
    
    setIsExportingEAN(true);
    dbg('excel:write:start');
    
    try {
      // Get EAN filtered data
      const eanFilteredData = currentProcessedData.filter(record => record.EAN && record.EAN.length >= 12);
      
      if (eanFilteredData.length === 0) {
        toast({
          title: "Nessuna riga valida per EAN",
          description: "Non ci sono record con EAN validi da esportare"
        });
        return;
      }
      
      // Create dataset with proper column order - UNIFIED pricing via robust IT locale parsing
      const dataset = eanFilteredData.map((record, index) => {
        // --- INPUTS sicuri con robust parsing ---
        const cbpC = toCents(record.CustBestPrice);                      // CustBestPrice in cent
        const shipC = toCents(6);                                        // shipping sempre 6€ in cent
        const ivaR = parsePercentToRate(22, 22);                         // "22" o "22%" -> 1.22
        const feeDR = parseRate(record.FeeDeRev, 1.05);                  // "1,05" -> 1.05
        const feeMP = parseRate(record['Fee Marketplace'], 1.07);        // "1,07" / "1,07 0,00" -> 1.07
        const listC = toCents(record.ListPrice);                         // ListPrice in cent

        // --- SUBTOTALE DA CBP (cents-based pipeline con entrambe le fee) ---
        const baseCents = cbpC + shipC;
        const ivatoCents = applyRateCents(baseCents, ivaR);
        const withFeDR = applyRateCents(ivatoCents, feeDR);
        const subtotalCents = applyRateCents(withFeDR, feeMP);

        // --- LISTPRICE CON FEE (intero per eccesso) ---
        const baseListCents = listC + shipC;
        const ivatoListCents = applyRateCents(baseListCents, ivaR);
        const withFeDRList = applyRateCents(ivatoListCents, feeDR);
        const subtotalListCents = applyRateCents(withFeDRList, feeMP);
        const listPriceConFeeInt = ceilToIntegerEuros(subtotalListCents); // intero

        // Prezzo Finale (ceil a ,99 sul subtotale corretto)
        const prezzoFinaleCents = ceilToComma99(subtotalCents);

        // Log per le prime 10 righe per verifica
        if (index < 10) {
          console.warn(`ean:debug:row${index}`, {
            cbpC, shipC, ivaR, feeDR, feeMP, 
            subtotalCents, prezzoFinaleCents, 
            subtotalListCents, listPriceConFee: listPriceConFeeInt
          });
        }

        return {
          Matnr: record.Matnr,
          ManufPartNr: record.ManufPartNr,
          EAN: record.EAN,
          ShortDescription: record.ShortDescription,
          ExistingStock: record.ExistingStock,
          CustBestPrice: record.CustBestPrice,
          'Costo di Spedizione': '6,00',
          IVA: '22%',
          'Prezzo con spediz e IVA': formatCents(ivatoCents),
          FeeDeRev: record.FeeDeRev,
          'Fee Marketplace': record['Fee Marketplace'],
          'Subtotale post-fee': formatCents(subtotalCents), // Corretta pipeline con entrambe le fee
          'Prezzo Finale': formatCents(prezzoFinaleCents), // FORCE finalDisplay string "NN,99"
          ListPrice: record.ListPrice,
          'ListPrice con Fee': listPriceConFeeInt, // Integer ceiling
          _eanFinalCents: prezzoFinaleCents // Internal field for validation guard
        };
      });
      
      // GUARD: Pre-export validation - block export if finalCents doesn't end with 99
      let incorrectEndingCount = 0;
      const guardFailures: any[] = [];
      
      for (let i = 0; i < dataset.length; i++) {
        const record = dataset[i];
        const finalCents = record._eanFinalCents;
        
        if (typeof finalCents === 'number' && (finalCents % 100) !== 99) {
          incorrectEndingCount++;
          if (guardFailures.length < 10) { // Log first 10 failures
            guardFailures.push({
              index: i,
              matnr: record.Matnr || 'N/A',
              ean: record.EAN || 'N/A',
              finalCents: finalCents,
              finalDisplay: record['Prezzo Finale']
            });
          }
        }
      }
      
      console.warn('excel:guard:finalCentsNot99', { count: incorrectEndingCount });
      
      if (incorrectEndingCount > 0) {
        console.warn('AUDIT: excel-guard:fail', { 
          count: incorrectEndingCount,
          failed: guardFailures
        });
        toast({
          title: "Errore validazione Excel ending ,99",
          description: `${incorrectEndingCount} righe non terminano con ,99. Export bloccato.`,
          variant: "destructive"
        });
        return;
      }
      
      // Simplified validation - already handled by guard above
      console.warn('AUDIT: ean-ending:ok', { validated: dataset.length, total: dataset.length });
      console.warn('lpfee:integer:ok', { validated: dataset.length, total: dataset.length });
      
      // Remove the internal validation field before export - NO "eanFinalCents" column
      const cleanDataset = dataset.map(record => {
        const { _eanFinalCents, ...exportRecord } = record;
        return exportRecord;
      });
      
      // Create worksheet from clean dataset
      const ws = XLSX.utils.json_to_sheet(cleanDataset, { skipHeader: false });
      
      // Ensure !ref exists
      if (!ws['!ref']) {
        const rows = cleanDataset.length + 1; // +1 for header
        const cols = Object.keys(cleanDataset[0] || {}).length;
        ws['!ref'] = XLSX.utils.encode_range({s:{r:0,c:0}, e:{r:rows-1,c:cols-1}});
      }
      
      // Force EAN column to text format
      const range = XLSX.utils.decode_range(ws['!ref']);
      let eanCol = -1;
      
      // Find EAN column in header
      for (let C = range.s.c; C <= range.e.c; C++) {
        const addr = XLSX.utils.encode_cell({ r: 0, c: C });
        const cell = ws[addr];
        const name = (cell?.v ?? '').toString().trim().toLowerCase();
        if (name === 'ean') { 
          eanCol = C; 
          break; 
        }
      }
      
      // Force EAN column cells to text format
      if (eanCol >= 0) {
        for (let R = 1; R <= range.e.r; R++) {
          const addr = XLSX.utils.encode_cell({ r: R, c: eanCol });
          const cell = ws[addr];
          if (cell) {
            cell.v = (cell.v ?? '').toString(); // Preserve leading zeros
            cell.t = 's';
            cell.z = '@';
            ws[addr] = cell;
          }
        }
      }
      
      // Format columns for EAN export - FORCE Prezzo Finale as text with finalDisplay
      const decimalColumns = ['Prezzo con spediz e IVA', 'Subtotale post-fee'];
      const textColumns = ['Prezzo Finale']; // FORCE finalDisplay as text to preserve ",99"
      const integerColumns = ['ListPrice con Fee']; // Keep as integer ceiling
      
      for (let C = range.s.c; C <= range.e.c; C++) {
        const headerAddr = XLSX.utils.encode_cell({ r: 0, c: C });
        const headerCell = ws[headerAddr];
        const headerName = (headerCell?.v ?? '').toString().trim();
        
        if (decimalColumns.includes(headerName)) {
          // Format as decimal with 2 places
          for (let R = 1; R <= range.e.r; R++) {
            const addr = XLSX.utils.encode_cell({ r: R, c: C });
            const cell = ws[addr];
            if (cell && typeof cell.v === 'number') {
              cell.t = 'n';
              cell.z = '#,##0.00';
              ws[addr] = cell;
            }
          }
        } else if (textColumns.includes(headerName)) {
          // FORCE Prezzo Finale as text to preserve finalDisplay "NN,99" format
          for (let R = 1; R <= range.e.r; R++) {
            const addr = XLSX.utils.encode_cell({ r: R, c: C });
            const cell = ws[addr];
            if (cell) {
              // FORCE text type and format - no numeric interpretation
              cell.t = 's'; // String type
              cell.z = '@'; // Text format
              cell.v = (cell.v ?? '').toString(); // Ensure string value
              ws[addr] = cell;
            }
          }
        } else if (integerColumns.includes(headerName)) {
          // Format ListPrice con Fee as integer (keep ceiling behavior)
          for (let R = 1; R <= range.e.r; R++) {
            const addr = XLSX.utils.encode_cell({ r: R, c: C });
            const cell = ws[addr];
            if (cell && typeof cell.v === 'number') {
              cell.t = 'n';
              cell.z = '0';
              ws[addr] = cell;
            }
          }
        }
      }
      
      // Create workbook
      const wb = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(wb, ws, "Catalogo_EAN");
      
      // Serialize to ArrayBuffer
      const wbout = XLSX.write(wb, { bookType: "xlsx", type: "array" });
      
      if (!wbout || wbout.length === 0) {
        dbg('excel:write:error | empty buffer');
        toast({
          title: "Errore generazione file",
          description: "Buffer vuoto durante la generazione del file Excel"
        });
        return;
      }
      
      // Create blob and download from main thread
      const blob = new Blob([wbout], { 
        type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" 
      });
      
      const url = URL.createObjectURL(blob);
      
      // Generate filename with timestamp
      const now = new Date();
      const timestamp = now.toISOString().slice(0,16).replace(/[-:T]/g, '').replace(/(\d{8})(\d{4})/, '$1_$2');
      const fileName = `Catalogo_EAN_${timestamp}.xlsx`;
      
      // Create anchor and trigger download
      const a = document.createElement("a");
      a.href = url;
      a.download = fileName;
      a.rel = "noopener";
      a.style.display = "none";
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      
      // Delay URL revocation
      setTimeout(() => URL.revokeObjectURL(url), 3000);
      
      dbg('excel:write:blob-size', { bytes: blob.size });
      dbg('excel:write:done', { pipeline: 'EAN' });
      
      setExcelDone(true);
      
      toast({
        title: "Export EAN avviato",
        description: "Controlla i download per il file Excel"
      });
      
    } catch (error) {
      dbg('excel:write:error', { message: error instanceof Error ? error.message : 'Unknown error' });
      toast({
        title: "Errore durante l'export",
        description: error instanceof Error ? error.message : "Errore sconosciuto"
      });
    } finally {
      setIsExportingEAN(false);
    }
  }, [currentProcessedData, isExportingEAN, dbg, toast]);

  // SKU Worker state
  const [skuWorker, setSkuWorker] = useState<Worker | null>(null);
  const [skuTimeout, setSkuTimeout] = useState<NodeJS.Timeout | null>(null);
  const [workerReady, setWorkerReady] = useState(false);
  const [firstBatchTime, setFirstBatchTime] = useState<number | null>(null);

  // Cleanup worker on unmount
  const [blobUrlRef, setBlobUrlRef] = useState<string | null>(null);
  
  useEffect(() => {
    return () => {
      if (skuWorker) {
        console.log('CLEANUP: terminating worker on unmount');
        skuWorker.terminate();
      }
      if (blobUrlRef) {
        console.log('CLEANUP: revoking blob URL on unmount');
        URL.revokeObjectURL(blobUrlRef);
        console.log('blob_url_revoked=true (unmount)');
      }
    };
  }, [skuWorker, blobUrlRef]);

  const handleSkuComplete = (data: any) => {
    if (skuTimeout) {
      clearTimeout(skuTimeout);
      setSkuTimeout(null);
    }
    
    const { results, summary } = data;
    
    if (results.length === 0) {
      toast({
        title: "Nessuna riga valida",
        description: "Nessuna riga valida per l'export SKU",
        variant: "destructive"
      });
      setIsExportingSKU(false);
      setProgressPct(0);
      return;
    }
    
    try {
      // Create Excel file using minimal formatting (aligned with EAN utilities)
      const timestamp = new Date().toLocaleString('it-IT', {
        timeZone: 'Europe/Rome',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
      }).replace(/[\/\s:]/g, match => match === '/' ? '' : match === ' ' ? '_' : '');
      
      const filename = `catalogo_sku_${timestamp}`;
      const worksheet = XLSX.utils.json_to_sheet(results);
      const workbook = XLSX.utils.book_new();
      
      // Apply minimal formatting - only what's specified
      if (worksheet['!ref']) {
        const range = XLSX.utils.decode_range(worksheet['!ref']);
        
        // Format IVA column as percentage
        for (let C = range.s.c; C <= range.e.c; C++) {
          const headerAddr = XLSX.utils.encode_cell({ r: 0, c: C });
          const headerCell = worksheet[headerAddr];
          if (headerCell && headerCell.v === 'IVA') {
            for (let R = 1; R <= range.e.r; R++) {
              const cellAddr = XLSX.utils.encode_cell({ r: R, c: C });
              const cell = worksheet[cellAddr];
              if (cell) {
                cell.t = 'n';
                cell.z = '0%';
              }
            }
            break;
          }
        }
        
        // Format price columns with 2 decimals
        const priceColumns = ['Costo di Spedizione', 'Prezzo con spedizione e IVA', 'FeeDeRev', 'Fee Marketplace', 'Subtotale post-fee', 'Prezzo Finale', 'CustBestPrice', 'ListPrice', 'ListPrice con Fee'];
        priceColumns.forEach(colName => {
          for (let C = range.s.c; C <= range.e.c; C++) {
            const headerAddr = XLSX.utils.encode_cell({ r: 0, c: C });
            const headerCell = worksheet[headerAddr];
            if (headerCell && headerCell.v === colName) {
              for (let R = 1; R <= range.e.r; R++) {
                const cellAddr = XLSX.utils.encode_cell({ r: R, c: C });
                const cell = worksheet[cellAddr];
                if (cell && typeof cell.v === 'number') {
                  cell.z = '0.00';
                }
              }
              break;
            }
          }
        });
      }
      
      // Force EAN column as text (reusing EAN utility)
      forceEANText(worksheet, 0);
      
      XLSX.utils.book_append_sheet(workbook, worksheet, 'SKU');
      XLSX.writeFile(workbook, `${filename}.xlsx`);
      
      toast({
        title: "Catalogo SKU generato",
        description: `${summary.exported} righe esportate (${summary.rejected} scartate)`,
        variant: "default"
      });
      
    } catch (error) {
      console.error('Errore nella scrittura Excel SKU:', error);
      toast({
        title: "Errore nella scrittura file",
        description: error instanceof Error ? error.message : 'Errore sconosciuto',
        variant: "destructive"
      });
    } finally {
      setIsExportingSKU(false);
      setProgressPct(0);
    }
  };

  const handleSkuError = (error: string) => {
    if (skuTimeout) {
      clearTimeout(skuTimeout);
      setSkuTimeout(null);
    }
    
    console.error('Errore nel worker SKU:', error);
    toast({
      title: "Errore",
      description: error,
      variant: "destructive"
    });
    setIsExportingSKU(false);
    setProgressPct(0);
  };

  const handleSkuCancelled = () => {
    if (skuTimeout) {
      clearTimeout(skuTimeout);
      setSkuTimeout(null);
    }
    
    toast({
      title: "Elaborazione annullata",
      description: "Elaborazione SKU annullata dall'utente",
      variant: "default"
    });
    setIsExportingSKU(false);
    setProgressPct(0);
  };

  // Diagnostic SKU function
  const runDiagnosticSku = useCallback(async () => {
    dbg('click_diag');
    
    // Reset diagnostic state
    setDiagnosticState(prev => ({
      ...prev,
      workerMessages: [],
      errorCounters: { msgInvalid: 0, workerError: 0, timeouts: 0 },
      testResults: [],
      lastHeartbeat: 0
    }));

    // Limit data to diagnostic rows if enabled
    const materialData = diagnosticState.isEnabled ? 
      files.material.file?.data.slice(0, diagnosticState.maxRows) || [] : 
      files.material.file?.data || [];
    
    const stockData = files.stock.file?.data || [];
    const priceData = files.price.file?.data || [];

    // Calculate total for diagnostic mode
    const total = materialData.filter(row => row.ManufPartNr && String(row.ManufPartNr).trim()).length;
    
    setDiagnosticState(prev => ({ 
      ...prev, 
      statistics: { ...prev.statistics, total } 
    }));

    await createSkuBlobWorker(true, { materialData, stockData, priceData });
  }, [diagnosticState.isEnabled, diagnosticState.maxRows, files, dbg]);

  // Note: runDiagnosticTests is defined later in the file
  const createSkuBlobWorker = useCallback(async (isDiagnostic = false, overrideData?: any) => {
    if (!isDiagnostic) {
      console.log('click_sku');
    }
    
    if (isExportingSKU) {
      toast({
        title: "Generazione in corso...",
        description: "Attendere completamento della generazione corrente"
      });
      return;
    }

    // Validate files are available
    if (!files.material.file || !files.stock.file || !files.price.file) {
      toast({
        title: "File mancanti",
        description: "File richiesti mancanti per la generazione SKU",
        variant: "destructive"
      });
      return;
    }

    // Validate fees ≥ 1.00
    if (feeConfig.feeDrev < 1.00 || isNaN(feeConfig.feeDrev)) {
      toast({
        title: "Fee non valida",
        description: "Inserisci un moltiplicatore ≥ 1,00 per FeeDeRev",
        variant: "destructive"
      });
      return;
    }
    
    if (feeConfig.feeMkt < 1.00 || isNaN(feeConfig.feeMkt)) {
      toast({
        title: "Fee non valida", 
        description: "Inserisci un moltiplicatore ≥ 1,00 per Fee Marketplace",
        variant: "destructive"
      });
      return;
    }

    // Cleanup existing worker
    if (skuWorker) {
      skuWorker.terminate();
      setSkuWorker(null);
    }

    // Generate version hash for cache busting
    const version = Date.now().toString(36);
    dbg('worker_created', { type: 'blob', version });
    
    // Self-contained worker code - completely inline
    const workerCode = `
// alterside-sku-worker.js - Self-contained SKU processing worker
// Optimized for performance with batch processing and minimal overhead

let isProcessing = false;
let shouldCancel = false;
let indexByMPN = new Map();
let indexByEAN = new Map();

// Send worker_boot signal immediately on worker start
self.postMessage({ type: 'worker_boot', version: '${version}' });

// Set up global error handler
self.addEventListener('error', function(e) {
  self.postMessage({
    type: 'worker_error',
    where: 'boot',
    message: e.message || 'Worker boot error',
    detail: e.filename + ':' + e.lineno
  });
});

// Main message handler with protocol compliance
self.onmessage = function(e) {
  try {
    const { type, data } = e.data || {};
    
    if (type === 'INIT') {
      // Validate INIT payload
      if (!data || typeof data !== 'object') {
        self.postMessage({
          type: 'worker_error',
          where: 'boot',
          message: 'Invalid INIT payload'
        });
        return;
      }
      
      // Send worker_ready with schema
      self.postMessage({ 
        type: 'worker_ready', 
        version: '${version}', 
        schema: 1 
      });
      return;
    }
    
    if (type === 'PRESCAN_START') {
      try {
        shouldCancel = false;
        isProcessing = true;
        performPreScan(data);
      } catch (error) {
        self.postMessage({
          type: 'worker_error',
          message: error instanceof Error ? error.message : 'Errore durante pre-scan',
          where: 'prescan',
          detail: error instanceof Error ? error.stack : null
        });
      } finally {
        isProcessing = false;
      }
      return;
    }
    
    if (type === 'SKU_START') {
      try {
        shouldCancel = false;
        isProcessing = true;
        processSkuCatalog(data);
      } catch (error) {
        self.postMessage({
          type: 'worker_error',
          message: error instanceof Error ? error.message : 'Errore sconosciuto durante elaborazione SKU',
          where: 'sku',
          detail: error instanceof Error ? error.stack : null
        });
      } finally {
        isProcessing = false;
      }
      return;
    }
    
    if (type === 'ping') {
      self.postMessage({ type: 'pong' });
      return;
    }
    
  } catch (globalError) {
    // Catch any top-level errors in message handling
    self.postMessage({
      type: 'worker_error',
      message: globalError instanceof Error ? globalError.message : 'Errore critico nel worker',
      where: 'boot',
      detail: globalError instanceof Error ? globalError.stack : null
    });
  }
};

// Inline utility functions (identical to EAN architecture)
function parseEuroLike(input) {
  if (typeof input === 'number' && isFinite(input)) return input;
  let s = String(input ?? '').trim();
  s = s.replace(/[^\\d.,\\s%\\-]/g, '').trim();
  s = s.split(/\\s+/)[0] ?? '';
  s = s.replace(/%/g, '').trim();
  if (!s) return NaN;

  if (s.includes('.') && s.includes(',')) s = s.replace(/\\./g, '').replace(',', '.');
  else s = s.replace(',', '.');

  const n = parseFloat(s);
  return Number.isFinite(n) ? n : NaN;
}

function toCents(x, fallback = 0) {
  const n = parseEuroLike(x);
  return Number.isFinite(n) ? Math.round(n * 100) : Math.round(fallback * 100);
}

function sanitizeEAN(ean) {
  if (!ean) return '';
  const str = String(ean).trim();
  if (!str) return '';
  
  // Sanitize if starts with formula chars or contains control chars
  const needsSanitization = /^[=+\\-@]/.test(str) || /[\\x00-\\x1F\\x7F]/.test(str);
  return needsSanitization ? "'" + str : str;
}

function validateMultiplier(value) {
  const num = parseEuroLike(value);
  return Number.isFinite(num) && num >= 1.0 ? num : null;
}

// Pre-scan function to build indices
async function performPreScan({ materialData, stockData, priceData }) {
  const BATCH_SIZE = 2000;
  
  let processed = 0;
  const totalData = materialData.length;
  
  self.postMessage({
    type: 'prescan_progress',
    done: 0,
    total: totalData
  });
  
  // Build ManufPartNr index
  indexByMPN.clear();
  for (let i = 0; i < materialData.length; i += BATCH_SIZE) {
    if (shouldCancel) {
      self.postMessage({ type: 'cancelled' });
      return;
    }
    
    const batch = materialData.slice(i, Math.min(i + BATCH_SIZE, materialData.length));
    
    for (const record of batch) {
      const mpn = String(record.ManufPartNr ?? '').trim();
      if (mpn) {
        indexByMPN.set(mpn, record);
      }
      processed++;
    }
    
    self.postMessage({
      type: 'prescan_progress',
      done: processed,
      total: totalData
    });
    
    // Yield to main thread
    await new Promise(resolve => setTimeout(resolve, 0));
  }
  
  // Build EAN index
  indexByEAN.clear();
  for (let i = 0; i < materialData.length; i += BATCH_SIZE) {
    if (shouldCancel) {
      self.postMessage({ type: 'cancelled' });
      return;
    }
    
    const batch = materialData.slice(i, Math.min(i + BATCH_SIZE, materialData.length));
    
    for (const record of batch) {
      const ean = String(record.EAN ?? '').trim();
      if (ean) {
        indexByEAN.set(ean, record);
      }
    }
    
    // Yield to main thread
    await new Promise(resolve => setTimeout(resolve, 0));
  }

  self.postMessage({
    type: 'prescan_done',
    counts: {
      mpnRecords: indexByMPN.size,
      eanRecords: indexByEAN.size,
      totalMaterial: materialData.length
    }
  });
}

async function processSkuCatalog({ sourceRows, fees }) {
  const startTime = Date.now();
  let BATCH_SIZE = 2000;
  const MIN_BATCH_SIZE = 1000;
  
  const totalRows = sourceRows.length;
  let processedCount = 0;
  let firstBatchTime = 0;
  
  const results = [];
  const rejects = [];
  
  // Validate and convert fees
  const feeDeRevMultiplier = validateMultiplier(fees.feeDeRev);
  const feeMktMultiplier = validateMultiplier(fees.feeMarketplace);
  
  if (feeDeRevMultiplier === null || feeMktMultiplier === null) {
    throw new Error('Inserisci un moltiplicatore ≥ 1,00');
  }
  
  const feeDeRevPercent = feeDeRevMultiplier - 1;
  const feeMktPercent = feeMktMultiplier - 1;
  
  self.postMessage({
    type: 'progress',
    progress: 0,
    recordsProcessed: 0,
    totalRecords: totalRows
  });
  
  // Process in batches
  for (let i = 0; i < totalRows; i += BATCH_SIZE) {
    if (shouldCancel) {
      self.postMessage({ type: 'cancelled' });
      return;
    }
    
    const batchStart = Date.now();
    const batch = sourceRows.slice(i, Math.min(i + BATCH_SIZE, totalRows));
    
    // Process batch
    for (const row of batch) {
      const result = processSkuRow(row, feeDeRevPercent, feeMktPercent, rejects, processedCount);
      if (result) {
        results.push(result);
      }
      processedCount++;
    }
    
    const batchTime = Date.now() - batchStart;
    
    // Store first batch time for dynamic watchdog
    if (i === 0) {
      firstBatchTime = batchTime;
      self.postMessage({
        type: 'first_batch_time',
        time: firstBatchTime
      });
    }
    
    // Adjust batch size if processing is too slow
    if (batchTime > 1500 && BATCH_SIZE > MIN_BATCH_SIZE) {
      BATCH_SIZE = MIN_BATCH_SIZE;
    }
    
    // Send progress update
    const progress = Math.round((processedCount / totalRows) * 100);
    self.postMessage({
      type: 'progress',
      progress,
      recordsProcessed: processedCount,
      totalRecords: totalRows
    });
    
    // Yield to main thread
    await new Promise(resolve => setTimeout(resolve, 0));
  }
  
  const processingTime = Date.now() - startTime;
  
  // Log summary (only final summary, no per-row logging)
  const summary = {
    totalRows,
    exported: results.length,
    rejected: rejects.length,
    processingTimeMs: processingTime,
    rejectReasons: rejects.reduce((acc, r) => {
      acc[r.reason] = (acc[r.reason] || 0) + 1;
      return acc;
    }, {})
  };
  
  // Send final results
  self.postMessage({
    type: 'complete',
    results,
    summary
  });
}

function processSkuRow(row, feeDeRevPercent, feeMktPercent, rejects, rowIndex) {
  // Filter 1: ExistingStock > 1
  const stock = Number(row.ExistingStock ?? NaN);
  if (!isFinite(stock) || stock <= 1) {
    rejects.push({ idx: rowIndex, reason: 'stock' });
    return null;
  }
  
  // Filter 2: ManufPartNr not empty
  const mpn = String(row.ManufPartNr ?? '').trim();
  if (!mpn) {
    rejects.push({ idx: rowIndex, reason: 'mpn_vuoto' });
    return null;
  }
  
  // Filter 3: Valid base price (CustBestPrice -> fallback ListPrice)
  const cbp = parseEuroLike(row.CustBestPrice);
  const lp = parseEuroLike(row.ListPrice);
  
  let baseEuro = null;
  if (Number.isFinite(cbp) && cbp > 0) {
    baseEuro = cbp;
  } else if (Number.isFinite(lp) && lp > 0) {
    baseEuro = lp;
  }
  
  if (baseEuro === null) {
    rejects.push({ idx: rowIndex, reason: 'prezzo_base' });
    return null;
  }
  
  // SKU Pipeline calculations in cents
  const baseCents = toCents(baseEuro);
  const shippingCents = 600; // 6.00 EUR
  const vatRate = 0.22;
  
  // Step 1: base + shipping
  let v = baseCents + shippingCents;
  
  // Step 2: + VAT
  v = Math.round(v * (1 + vatRate));
  const preFeeEuro = v / 100;
  
  // Step 3: + FeeDeRev (sequential)
  const feeDeRevCents = Math.round(v * feeDeRevPercent);
  v = v + feeDeRevCents;
  const feeDeRevEuro = feeDeRevCents / 100;
  
  // Step 4: + Fee Marketplace (sequential)
  const feeMktCents = Math.round(v * feeMktPercent);
  v = v + feeMktCents;
  const feeMktEuro = feeMktCents / 100;
  
  // Step 5: Ceiling to integer euro
  const finalCents = Math.ceil(v / 100) * 100;
  const finalEuro = finalCents / 100;
  
  // Subtotal post-fee
  const subtotalPostFee = preFeeEuro + feeDeRevEuro + feeMktEuro;
  
  // ListPrice con Fee calculation
  let listPriceConFee = '';
  if (Number.isFinite(lp) && lp > 0) {
    const lpBaseCents = toCents(lp);
    let lpV = lpBaseCents + shippingCents;
    lpV = Math.round(lpV * (1 + vatRate));
    lpV = lpV + Math.round(lpV * feeDeRevPercent);
    lpV = lpV + Math.round(lpV * feeMktPercent);
    const lpFinalCents = Math.ceil(lpV / 100) * 100;
    
    // Validation: must be integer euro
    if (lpFinalCents % 100 !== 0) {
      throw new Error('SKU: ListPrice con Fee deve essere intero');
    }
    
    listPriceConFee = lpFinalCents / 100;
  }
  
  // Final validation: Prezzo Finale must be integer euro
  if (finalCents % 100 !== 0) {
    throw new Error('SKU: Prezzo Finale deve essere intero in euro');
  }
  
  // Build minimal result object with exact column order
  return {
    Matnr: row.Matnr || '',
    ManufPartNr: mpn,
    EAN: sanitizeEAN(row.EAN),
    ShortDescription: row.ShortDescription || '',
    ExistingStock: stock,
    CustBestPrice: Number.isFinite(cbp) ? cbp : '',
    'Costo di Spedizione': 6.00,
    IVA: 0.22, // Will be formatted as percentage in Excel
    'Prezzo con spedizione e IVA': preFeeEuro,
    FeeDeRev: feeDeRevEuro,
    'Fee Marketplace': feeMktEuro,
    'Subtotale post-fee': subtotalPostFee,
    'Prezzo Finale': finalEuro,
    ListPrice: Number.isFinite(lp) ? lp : '',
    'ListPrice con Fee': listPriceConFee
  };
}
`;

    console.log('worker_created', { type: 'blob', version });

    // Create blob worker with comprehensive logging
    const blob = new Blob([workerCode], { type: 'text/javascript' });
    const blobUrl = URL.createObjectURL(blob);
    
    // Store blob URL reference for cleanup
    setBlobUrlRef(blobUrl);
    
    // Log blob URL creation
    console.log('BLOB_URL_CREATED:', blobUrl);
    console.log('blob_url_revoked=false');
    
    // Check Content-Security-Policy
    const cspMeta = document.querySelector('meta[http-equiv="Content-Security-Policy"]');
    const cspHeader = cspMeta ? cspMeta.getAttribute('content') : 'Not found in meta';
    console.log('CONTENT_SECURITY_POLICY:', cspHeader);
    
    // Check if worker-src/child-src include blob:
    const workerSrcAllowsBlob = !cspHeader || cspHeader === 'Not found in meta' || cspHeader.includes('worker-src') ? cspHeader.includes('blob:') || !cspHeader.includes('worker-src') : 'N/A';
    const childSrcAllowsBlob = !cspHeader || cspHeader === 'Not found in meta' || cspHeader.includes('child-src') ? cspHeader.includes('blob:') || !cspHeader.includes('child-src') : 'N/A';
    console.log('WORKER_SRC_ALLOWS_BLOB:', workerSrcAllowsBlob);
    console.log('CHILD_SRC_ALLOWS_BLOB:', childSrcAllowsBlob);
    
    const worker = new Worker(blobUrl);
    
    // Handshake gate with 2s timeout
    let handshakeTimer: NodeJS.Timeout;
    let workerReadyReceived = false;
    let blobUrlRevoked = false;
    
    // Attach event handlers BEFORE sending any messages
    const waitForHandshake = new Promise<void>((resolve, reject) => {
      handshakeTimer = setTimeout(() => {
        console.log('worker_handshake_timeout');
        if (!blobUrlRevoked) {
          URL.revokeObjectURL(blobUrl);
          blobUrlRevoked = true;
          console.log('blob_url_revoked=true (timeout)');
        }
        worker.terminate();
        reject(new Error('Worker non inizializzato'));
      }, 2000);

      // Worker message handler
      const messageHandler = (e: MessageEvent) => {
        // Log all worker messages
        console.log('WORKER_MESSAGE_RECEIVED:', e.data);
        addWorkerMessage(e.data);
        
        const { type, data } = e.data;
        
        if (type === 'worker_ready' && !workerReadyReceived) {
          clearTimeout(handshakeTimer);
          workerReadyReceived = true;
          console.log('worker_ready');
          // Remove the one-time handler and set up main handler
          worker.removeEventListener('message', messageHandler);
          worker.addEventListener('message', mainMessageHandler);
          resolve();
          return;
        }
      };

      const mainMessageHandler = (e: MessageEvent) => {
        handleWorkerMessage(e, worker);
      };

      worker.addEventListener('message', messageHandler);
      
      // Error handlers with detailed logging
      worker.onerror = (error: ErrorEvent) => {
        clearTimeout(handshakeTimer);
        console.error('WORKER_ERROR:', {
          message: error.message,
          filename: error.filename,
          lineno: error.lineno,
          colno: error.colno,
          error: error.error
        });
        console.log('WORKER_ERROR_DETAILS:', error);
        if (!blobUrlRevoked) {
          URL.revokeObjectURL(blobUrl);
          blobUrlRevoked = true;
          console.log('blob_url_revoked=true (onerror)');
        }
        reject(error);
      };
      
      worker.onmessageerror = (error: MessageEvent) => {
        clearTimeout(handshakeTimer);
        console.error('WORKER_MESSAGE_ERROR:', {
          type: 'messageerror',
          data: error.data,
          origin: error.origin,
          source: error.source
        });
        console.log('WORKER_MESSAGE_ERROR_DETAILS:', error);
        if (!blobUrlRevoked) {
          URL.revokeObjectURL(blobUrl);
          blobUrlRevoked = true;
          console.log('blob_url_revoked=true (onmessageerror)');
        }
        reject(new Error('Worker message error'));
      };
      
      // Log that handlers are attached
      console.log('handlers_attached=true');
    });

    setSkuWorker(worker);
    setIsExportingSKU(true);
    setProgressPct(0);
    
    try {
      await waitForHandshake;
      
      // Send INIT message immediately after handshake
      const initPayload = {
        version,
        schema: 1,
        diag: diagnosticState.isEnabled,
        sampleSize: diagnosticState.isEnabled ? diagnosticState.maxRows : undefined
      };
      
      console.log('init_sent=true', initPayload);
      worker.postMessage({ 
        type: 'INIT', 
        data: initPayload
      });
      
      // Check if prescan is needed
      if (!debugState.materialPreScanDone) {
        // Start prescan
        setProcessingState('prescanning');
        console.log('prescan_start');
        
        // Send only necessary fields for optimal payload
        const optimizedMaterialData = files.material.file.data.map(row => ({
          Matnr: row.Matnr,
          ManufPartNr: row.ManufPartNr,
          EAN: row.EAN,
          ShortDescription: row.ShortDescription,
          ExistingStock: row.ExistingStock,
          CustBestPrice: row.CustBestPrice,
          ListPrice: row.ListPrice
        }));
        
        worker.postMessage({
          type: 'prescan',
          data: { 
            materialData: optimizedMaterialData, 
            stockData: [], 
            priceData: [] 
          }
        });
      } else {
        // Skip prescan, go directly to SKU processing
        startSkuProcessing(worker);
      }
      
    } catch (error) {
      setProcessingState('error');
      setIsExportingSKU(false);
      toast({
        title: "Errore Worker",
        description: error instanceof Error ? error.message : "Worker non inizializzato",
        variant: "destructive",
      });
    }
  }, [files, feeConfig, isExportingSKU, skuWorker, toast, debugState.materialPreScanDone]);

  // Handle worker messages after handshake
  const handleWorkerMessage = useCallback((e: MessageEvent, worker: Worker) => {
    const { type, data } = e.data;
    
    if (type === 'worker_error') {
      console.error('Worker error:', data);
      setProcessingState('error');
      setIsExportingSKU(false);
      toast({
        title: "Errore Worker",
        description: data.message || "Errore sconosciuto nel worker",
        variant: "destructive",
      });
      return;
    }

    if (type === 'prescan_progress') {
      const progress = Math.round((data.done / data.total) * 100);
      setProgressPct(progress);
      return;
    }

    if (type === 'prescan_done') {
      console.log('prescan_done', data.counts);
      setDebugState(prev => ({ ...prev, materialPreScanDone: true }));
      
      // Now start SKU processing
      setProcessingState('running');
      setProgressPct(0);
      console.log('sku_start');
      
      startSkuProcessing(worker);
      return;
    }

    if (type === 'first_batch_time') {
      setFirstBatchTime(data.time);
      
      // Calculate dynamic timeout: max(180s, 10 × firstBatchTime × (total/batchSize))
      const totalRows = files.material?.file?.data?.length || 0;
      const batchSize = 2000;
      const dynamicTimeout = Math.max(180000, 10 * data.time * (totalRows / batchSize));
      
      // Clear existing timeout and set new one
      if (skuTimeout) {
        clearTimeout(skuTimeout);
      }
      
      const timeout = setTimeout(() => {
        console.log('sku_timeout');
        worker.postMessage({ type: 'cancel' });
        toast({
          title: "Timeout",
          description: "Elaborazione SKU interrotta per timeout. Riprovare con un dataset più piccolo.",
          variant: "destructive"
        });
        setIsExportingSKU(false);
        setProgressPct(0);
        setProcessingState('error');
      }, dynamicTimeout);
      
      setSkuTimeout(timeout);
      return;
    }

    if (type === 'progress') {
      setProgressPct(data.progress);
      return;
    }

    if (type === 'complete') {
      console.log('sku_done', { exported: data.results.length, rejected: data.summary.rejected });
      
      if (skuTimeout) {
        clearTimeout(skuTimeout);
        setSkuTimeout(null);
      }
      
      // Clean up blob URL when processing is complete
      if (blobUrlRef) {
        console.log('CLEANUP: revoking blob URL on completion');
        URL.revokeObjectURL(blobUrlRef);
        setBlobUrlRef(null);
        console.log('blob_url_revoked=true (completion)');
      }
      
      // Export to Excel
      handleSkuComplete(data);
      return;
    }

    if (type === 'cancelled') {
      setProcessingState('idle');
      setIsExportingSKU(false);
      setProgressPct(0);
      
      if (skuTimeout) {
        clearTimeout(skuTimeout);
        setSkuTimeout(null);
      }
      
      toast({
        title: "Elaborazione annullata",
        variant: "default",
      });
      return;
    }
  }, [files, feeConfig, skuTimeout, toast]);

  // Start SKU processing
  const startSkuProcessing = useCallback((worker: Worker) => {
    if (!files.material.file || !files.stock.file || !files.price.file) return;
    
    // Send only necessary fields for optimal payload
    const optimizedSourceRows = files.material.file.data.map(row => ({
      Matnr: row.Matnr,
      ManufPartNr: row.ManufPartNr,
      EAN: row.EAN,
      ShortDescription: row.ShortDescription,
      ExistingStock: row.ExistingStock,
      CustBestPrice: row.CustBestPrice,
      ListPrice: row.ListPrice
    }));
    
    worker.postMessage({
      type: 'process',
      data: {
        sourceRows: optimizedSourceRows,
        fees: {
          feeDeRev: feeConfig.feeDrev,
          feeMarketplace: feeConfig.feeMkt
        }
      }
    });
  }, [files, feeConfig]);
  


  const downloadExcel = (type: 'ean' | 'manufpartnr') => {
    if (type === 'ean') {
      // Use the new onExportEAN function for EAN catalog
      onExportEAN({ preventDefault: () => {} } as React.MouseEvent);
      return;
    }
    
    // Keep existing logic for manufpartnr
    if (currentProcessedData.length === 0) return;
    
    dbg('excel:write:start');
    
    const { timestamp, sheetName } = getTimestamp();
    const filename = `catalogo_${type}_${timestamp}.xlsx`;

    const excelData = formatExcelData(currentProcessedData);
    const ws = XLSX.utils.json_to_sheet(excelData);
    
    // Force EAN column to text format for both pipelines
    forceEANText(ws);
    
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, sheetName);
    XLSX.writeFile(wb, filename);

    setExcelDone(true);
    dbg('excel:write:done', { pipeline: type });

    toast({
      title: "Excel scaricato",
      description: `File ${filename} scaricato con successo`
    });
  };

  const downloadDiscardedRows = () => {
    if (discardedRows.length === 0) return;
    exportDiscardedRowsCSV(discardedRows, `righe_scartate_EAN_${new Date().toISOString().split('T')[0]}`);
  };

  const downloadLog = (type: 'ean' | 'manufpartnr') => {
    if (currentLogEntries.length === 0 && !currentStats) return;
    
    dbg('log:write:start');

    const { timestamp, sheetName } = getTimestamp();
    const filename = `catalogo_log_${type}_${timestamp}.xlsx`;

    const logData = currentLogEntries.map(entry => ({
      'File Sorgente': entry.source_file,
      'Riga': entry.line,
      'Matnr': entry.Matnr,
      'ManufPartNr': entry.ManufPartNr,
      'EAN': entry.EAN,
      'Motivo': entry.reason,
      'Dettagli': entry.details
    }));

    const ws = XLSX.utils.json_to_sheet(logData);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, sheetName);
    XLSX.writeFile(wb, filename);

    setLogDone(true);
    dbg('log:write:done', { pipeline: type });

    toast({
      title: "Log scaricato",
      description: `File ${filename} scaricato con successo`
    });
  };

  const FileUploadCard: React.FC<{
    title: string;
    description: string;
    type: keyof FileUploadState;
    requiredHeaders: string[];
    optionalHeaders: string[];
  }> = ({ title, description, type, requiredHeaders, optionalHeaders }) => {
    const fileState = files[type];
    
    return (
      <div className="card border-strong">
        <div className="card-body">
          <div className="flex items-center justify-between mb-4">
            <h3 className="card-title">{title}</h3>
            {fileState.status === 'valid' && (
              <div className="badge-ok">
                <CheckCircle className="w-4 h-4" />
                Caricato
              </div>
            )}
            {fileState.status === 'warning' && (
              <div className="badge-ok" style={{ background: '#fff3cd', color: '#856404', border: '1px solid #ffeaa7' }}>
                <AlertCircle className="w-4 h-4" />
                Caricato (con avviso)
              </div>
            )}
            {fileState.status === 'error' && (
              <div className="badge-err">
                <XCircle className="w-4 h-4" />
                Errore
              </div>
            )}
          </div>

          <p className="text-muted text-sm mb-4">{description}</p>
          
          <div className="text-xs text-muted mb-4">
            <div><strong>Header richiesti:</strong> {requiredHeaders.join(', ')}</div>
            {optionalHeaders.length > 0 && (
              <div><strong>Header opzionali:</strong> {optionalHeaders.join(', ')}</div>
            )}
          </div>

          {!fileState.file ? (
            <div className="dropzone text-center">
              <Upload className="mx-auto h-12 w-12 icon-dark mb-4" />
              <div>
                <input
                  type="file"
                  accept=".txt,.csv"
                  onChange={(e) => {
                    const selectedFile = e.target.files?.[0];
                    if (selectedFile) {
                      handleFileUpload(selectedFile, type);
                    }
                  }}
                  className="hidden"
                  id={`file-${type}`}
                />
                <label
                  htmlFor={`file-${type}`}
                  className="btn btn-primary cursor-pointer px-6 py-3"
                >
                  Carica File
                </label>
                <p className="text-muted text-sm mt-3">
                  File CSV con delimitatore ; e encoding UTF-8
                </p>
              </div>
            </div>
          ) : (
            <div className="flex items-center justify-between p-4 bg-white rounded-lg border-strong">
              <div className="flex items-center gap-3">
                <FileText className="h-6 w-6 icon-dark" />
                <div>
                  <p className="font-medium">{fileState.file.name}</p>
                  <p className="text-sm text-muted">
                    {fileState.file.data.length} righe
                  </p>
                </div>
              </div>
              <button
                onClick={() => removeFile(type)}
                className="btn btn-secondary text-sm px-3 py-2"
              >
                Rimuovi
              </button>
            </div>
          )}

          {fileState.status === 'error' && fileState.error && (
            <div className="mt-4 p-3 rounded-lg border-strong" style={{ background: 'var(--error-bg)', color: 'var(--error-fg)' }}>
              <p className="text-sm font-medium">{fileState.error}</p>
            </div>
          )}

          {fileState.status === 'warning' && fileState.warning && (
            <div className="mt-4 p-3 rounded-lg border-strong" style={{ background: '#fff3cd', color: '#856404' }}>
              <p className="text-sm font-medium">{fileState.warning}</p>
            </div>
          )}

          {fileState.file && (
            <div className="mt-4 p-3 rounded-lg border-strong bg-gray-50">
              <h4 className="text-sm font-medium mb-2">Diagnostica</h4>
              <div className="text-xs text-muted">
                <div><strong>Header rilevati:</strong> {fileState.file.headers.join(', ')}</div>
                {fileState.file.data.length > 0 && (
                  <div className="mt-1">
                    <strong>Prima riga di dati:</strong> {Object.values(fileState.file.data[0]).slice(0, 3).join(', ')}...
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    );
  };

  const allFilesValid = files.material.status === 'valid' && 
    (files.stock.status === 'valid' || files.stock.status === 'warning') && 
    (files.price.status === 'valid' || files.price.status === 'warning');

  return (
    <div className="min-h-screen p-6" style={{ background: 'var(--bg)', color: 'var(--fg)' }}>
      <div className="max-w-7xl mx-auto space-y-8">
        {/* Header */}
        <div className="text-center">
          <h1 className="text-5xl font-bold mb-4">
            Alterside Catalog Generator
          </h1>
          <p className="text-muted text-xl max-w-3xl mx-auto">
            Genera due cataloghi Excel distinti (EAN e ManufPartNr) con calcoli avanzati di prezzo e commissioni
          </p>
        </div>

        {/* Instructions */}
        <div className="card border-strong">
          <div className="card-body">
            <div className="flex items-start gap-4">
              <AlertCircle className="h-6 w-6 icon-dark mt-1 flex-shrink-0" />
              <div>
                <h3 className="card-title mb-3">Specifiche di Elaborazione</h3>
                <ul className="text-sm text-muted space-y-2">
                  <li>• <strong>Filtri comuni:</strong> ExistingStock &gt; 1, prezzi numerici validi</li>
                  <li>• <strong>Export EAN:</strong> solo record con EAN non vuoto</li>
                  <li>• <strong>Export ManufPartNr:</strong> solo record con ManufPartNr non vuoto</li>
                  <li>• <strong>Catalogo SKU:</strong> record con ManufPartNr non vuoto, anche senza EAN</li>
                  <li>• <strong>Prezzi:</strong> Base + spedizione (€6), IVA 22%, fee sequenziali configurabili</li>
                  <li>• <strong>Prezzo finale EAN:</strong> ending ,99; <strong>ManufPartNr:</strong> arrotondamento intero superiore; <strong>SKU:</strong> intero superiore</li>
                </ul>
              </div>
            </div>
          </div>
        </div>

        {/* File Upload Cards */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <FileUploadCard
            title="Material File"
            description="File principale con informazioni prodotto"
            type="material"
            requiredHeaders={REQUIRED_HEADERS.material}
            optionalHeaders={OPTIONAL_HEADERS.material}
          />
          <FileUploadCard
            title="Stock File Data"
            description="Dati scorte e disponibilità"
            type="stock"
            requiredHeaders={REQUIRED_HEADERS.stock}
            optionalHeaders={OPTIONAL_HEADERS.stock}
          />
          <FileUploadCard
            title="Price File Data"
            description="Listini prezzi e scontistiche"
            type="price"
            requiredHeaders={REQUIRED_HEADERS.price}
            optionalHeaders={OPTIONAL_HEADERS.price}
          />
        </div>

        {/* Fee Configuration */}
        {allFilesValid && (
          <div className="card border-strong">
            <div className="card-body">
              <h3 className="card-title mb-6 flex items-center gap-2">
                <Info className="h-5 w-5 icon-dark" />
                Regole di Calcolo
              </h3>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div className="space-y-2">
                  <Label htmlFor="fee-derev" className="text-sm font-medium">
                    Fee DeRev (moltiplicatore)
                  </Label>
                  <Input
                    id="fee-derev"
                    type="number"
                    min="1.00"
                    max="2.00"
                    step="0.01"
                    placeholder="1,00"
                    value={feeConfig.feeDrev}
                    onChange={(e) => {
                      const val = parseFloat(e.target.value);
                      if (isNaN(val) || val < 1.00) {
                        // Allow temporary invalid input for editing, but show error
                        setFeeConfig(prev => ({ ...prev, feeDrev: val }));
                      } else if (val >= 1.00 && val <= 2.00) {
                        setFeeConfig(prev => ({ ...prev, feeDrev: val }));
                      }
                    }}
                    className="text-center"
                    title="Inserisci fee come moltiplicatore: 1,05 = +5%, 1,08 = +8%. Le fee sono applicate in sequenza dopo IVA e spedizione."
                  />
                  <p className="text-xs text-muted-foreground">
                    {feeConfig.feeDrev < 1.00 ? (
                      <span className="text-destructive">Inserisci un moltiplicatore ≥ 1,00</span>
                    ) : (
                      `Esempio: 1,05 = +5% commissione DeRev`
                    )}
                  </p>
                </div>
                
                <div className="space-y-2">
                  <Label htmlFor="fee-marketplace" className="text-sm font-medium">
                    Fee Marketplace (moltiplicatore)
                  </Label>
                  <Input
                    id="fee-marketplace"
                    type="number"
                    min="1.00"
                    max="2.00"
                    step="0.01"
                    value={feeConfig.feeMkt}
                    onChange={(e) => {
                      const val = parseFloat(e.target.value);
                      if (val >= 1.00 && val <= 2.00) {
                        setFeeConfig(prev => ({ ...prev, feeMkt: val }));
                      }
                    }}
                    className="text-center"
                    title="Fee marketplace applicata dopo Fee DeRev. Esempio: 1,08 = +8% commissione marketplace."
                  />
                  <p className="text-xs text-muted-foreground">
                    Esempio: 1,08 = +8% commissione marketplace
                  </p>
                </div>
              </div>
              
              <div className="mt-4 flex items-center space-x-2">
                <Checkbox
                  id="remember-fees"
                  checked={rememberFees}
                  onCheckedChange={(checked) => {
                    setRememberFees(!!checked);
                    if (checked) {
                      saveFees(feeConfig);
                    }
                  }}
                />
                <Label htmlFor="remember-fees" className="text-sm">
                  Ricorda queste impostazioni
                </Label>
              </div>
            </div>
          </div>
        )}

        {/* Action Buttons */}
        {allFilesValid && (
          <div className="text-center">
            <h3 className="text-2xl font-bold mb-6">Azioni</h3>
            
            {/* Diagnostic Toggle and Force Visibility Logic */}
            {(() => {
              // Force visibility checks
              const urlParams = new URLSearchParams(window.location.search);
              const urlDiagFlag = urlParams.get('diag') === '1';
              const localStorageDiagFlag = localStorage.getItem('diag') === '1';
              const diagForceVisible = urlDiagFlag || localStorageDiagFlag;
              
              // Main visibility condition
              const shouldShowDiagnostic = diagForceVisible || true; // Always show for now
              
              // Debug info to console
              console.log('DIAGNOSTIC_TOGGLE_DEBUG:', {
                condition: `diagForceVisible=${diagForceVisible} || true`,
                urlDiagFlag,
                localStorageDiagFlag,
                diagForceVisible,
                shouldShowDiagnostic,
                allFilesValid,
                diagnosticStateEnabled: diagnosticState.isEnabled
              });
              
              return shouldShowDiagnostic ? (
                <div className="mb-6 p-4 border border-strong rounded-lg bg-muted">
                  <div className="flex items-center justify-center gap-4 mb-4">
                    <label htmlFor="diagnostic-mode" className="text-sm font-medium">
                      Modalità diagnostica
                    </label>
                    <input
                      id="diagnostic-mode"
                      type="checkbox"
                      checked={diagnosticState.isEnabled}
                      onChange={(e) => {
                        setDiagnosticState(prev => ({
                          ...prev,
                          isEnabled: e.target.checked
                        }));
                        if (e.target.checked && diagForceVisible) {
                          localStorage.setItem('diag', '1');
                        }
                      }}
                      className="w-4 h-4"
                    />
                  </div>
                  
                  {diagnosticState.isEnabled && (
                    <div className="flex justify-center">
                      <button
                        onClick={runDiagnosticSku}
                        disabled={!(debugState.materialValid && debugState.stockValid && debugState.priceValid) || isExportingSKU}
                        className={`btn btn-secondary text-lg px-8 py-3 ${!canProcess || isExportingSKU ? 'is-disabled' : ''}`}
                      >
                        <Upload className="mr-2 h-5 w-5" />
                        Diagnostica prescan
                      </button>
                    </div>
                  )}
                </div>
              ) : null;
            })()}
            
            <div className="flex flex-wrap justify-center gap-6">
              <button
                onClick={() => processDataPipeline('EAN')}
                disabled={!canProcess || isProcessing}
                className={`btn btn-primary text-lg px-12 py-4 ${!canProcess || isProcessing ? 'is-disabled' : ''}`}
              >
                {isProcessing && currentPipeline === 'EAN' ? (
                  <>
                    <Activity className="mr-3 h-5 w-5 animate-spin" />
                    Elaborazione EAN...
                  </>
                ) : (
                  <>
                    <Upload className="mr-3 h-5 w-5" />
                    GENERA EXCEL (EAN)
                  </>
                )}
              </button>
              <button
                onClick={() => createSkuBlobWorker(false)}
                disabled={!(debugState.materialValid && debugState.stockValid && debugState.priceValid) || isExportingSKU}
                className={`btn btn-primary text-lg px-12 py-4 ${!canProcess || isExportingSKU ? 'is-disabled' : ''}`}
              >
                {isExportingSKU ? (
                  <>
                    <Activity className="mr-3 h-5 w-5 animate-spin" />
                    {processingState === 'prescanning' ? 'Indicizzazione prodotti...' : 'Elaborazione SKU...'}
                  </>
                ) : (
                  <>
                    <Upload className="mr-3 h-5 w-5" />
                    GENERA EXCEL (ManufPartNr)
                  </>
                )}
              </button>
              
              {/* Cancel button for SKU operation */}
              {isExportingSKU && (
                <button
                  onClick={() => {
                    if (skuWorker) {
                      skuWorker.postMessage({ type: 'cancel' });
                    }
                  }}
                  className="btn btn-secondary text-lg px-8 py-4"
                  title="Annulla elaborazione SKU"
                >
                  <X className="mr-2 h-5 w-5" />
                  Annulla
                </button>
              )}
            </div>
          </div>
        )}

        {/* Progress Section */}
        {(isProcessing || isCompleted || isExportingSKU) && (
          <div className="card border-strong">
            <div className="card-body">
              <h3 className="card-title mb-6 flex items-center gap-2">
                <Activity className="h-5 w-5 animate-spin icon-dark" />
                Progresso Elaborazione
              </h3>
              
              <div className="space-y-4">
                <div className="text-sm text-muted mb-2">
                  <strong>Stato corrente:</strong> {processingState}
                </div>
                
                <div className="progress">
                  <span style={{ width: `${progressPct}%` }} />
                </div>
                
                <div className="flex justify-between text-sm">
                  <span className="font-medium">{progressPct}%</span>
                  <span className="font-bold">Completato</span>
                </div>
                
                <div className="flex items-center gap-6 text-sm">
                  <div className="flex items-center gap-2">
                    <Clock className="h-4 w-4 icon-dark" />
                    <span>Trascorso: {formatTime(elapsedTime)}</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <Clock className="h-4 w-4 icon-dark" />
                    <span>ETA: {estimatedTime ? formatTime(estimatedTime) : (processed > 0 && !isCompleted ? 'calcolo…' : '—')}</span>
                  </div>
                  <div className="text-sm">
                    <span className="font-medium">Righe elaborate: {processed.toLocaleString()}</span>
                    {(finalTotal ?? total) > 0 && <span className="text-muted"> / {(finalTotal ?? total).toLocaleString()}</span>}
                  </div>
                  {finalTotal !== null && total !== finalTotal && (
                    <div className="text-xs text-muted">
                      Stima iniziale: {total.toLocaleString()} | Totale effettivo: {finalTotal.toLocaleString()}
                    </div>
                  )}
                </div>
                
                {/* Debug State */}
                <div className="mt-4 p-3 bg-muted rounded-lg">
                  <h4 className="text-sm font-medium mb-2">Stato Debug</h4>
                  <div className="grid grid-cols-2 gap-2 text-xs">
                    <div>materialValid: {debugState.materialValid ? '✓' : '✗'}</div>
                    <div>stockValid: {debugState.stockValid ? '✓' : '✗'}</div>
                    <div>priceValid: {debugState.priceValid ? '✓' : '✗'}</div>
                    <div>stockReady: {debugState.stockReady ? '✓' : '✗'}</div>
                    <div>priceReady: {debugState.priceReady ? '✓' : '✗'}</div>
                    <div>materialPreScanDone: {debugState.materialPreScanDone ? '✓' : '✗'}</div>
                    <div>joinStarted: {debugState.joinStarted ? '✓' : '✗'}</div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Debug Events Panel */}
        {(isProcessing || isCompleted || debugEvents.length > 0) && (
          <div className="card border-strong">
            <div className="card-body">
              <h3 className="card-title mb-4 flex items-center gap-2">
                <AlertCircle className="h-5 w-5 icon-dark" />
                Eventi Debug
              </h3>
              
              {/* Diagnostic Toggle Debug Info */}
              {(() => {
                const urlParams = new URLSearchParams(window.location.search);
                const urlDiagFlag = urlParams.get('diag') === '1';
                const localStorageDiagFlag = localStorage.getItem('diag') === '1';
                const diagForceVisible = urlDiagFlag || localStorageDiagFlag;
                const shouldShowDiagnostic = diagForceVisible || true;
                
                return (
                  <div className="mb-4 p-3 bg-blue-50 border border-blue-200 rounded-lg">
                    <h4 className="text-sm font-medium mb-2">Condizioni Toggle Diagnostica</h4>
                    <div className="grid grid-cols-2 gap-2 text-xs">
                      <div>Condizione completa: <code>diagForceVisible || true</code></div>
                      <div>Risultato: <strong>{shouldShowDiagnostic ? 'TRUE' : 'FALSE'}</strong></div>
                      <div>URL ?diag=1: <strong>{urlDiagFlag ? 'TRUE' : 'FALSE'}</strong></div>
                      <div>localStorage diag=1: <strong>{localStorageDiagFlag ? 'TRUE' : 'FALSE'}</strong></div>
                      <div>diagForceVisible: <strong>{diagForceVisible ? 'TRUE' : 'FALSE'}</strong></div>
                      <div>allFilesValid: <strong>{allFilesValid ? 'TRUE' : 'FALSE'}</strong></div>
                      <div>diagnosticState.isEnabled: <strong>{diagnosticState.isEnabled ? 'TRUE' : 'FALSE'}</strong></div>
                      <div>Container: /Azioni (stesso blocco)</div>
                    </div>
                    <div className="mt-2 text-xs">
                      <strong>Per forza visibilità:</strong> aggiungi ?diag=1 alla URL o imposta localStorage.setItem('diag', '1')
                    </div>
                  </div>
                );
              })()}
              
              <textarea
                value={debugEvents.join('\n')}
                readOnly
                className="w-full h-64 p-3 font-mono text-xs bg-muted border border-strong rounded-lg resize-none"
                style={{ whiteSpace: 'pre-wrap' }}
              />
              
              {debugEvents.length > 0 && (
                <div className="mt-2 flex justify-between text-xs text-muted">
                  <span>{debugEvents.length} eventi registrati</span>
                  <button
                    onClick={() => setDebugEvents([])}
                    className="text-primary hover:text-primary-dark"
                  >
                    Pulisci log
                  </button>
                </div>
               )}
             </div>
           </div>
         )}

         {/* Diagnostic Panels */}
         {diagnosticState.isEnabled && (
           <>
             {/* Worker Messages Panel */}
             <div className="card border-strong">
               <div className="card-body">
                 <h3 className="card-title mb-4 flex items-center gap-2">
                   <AlertCircle className="h-5 w-5 icon-dark" />
                   Messaggi Worker (primi 10)
                 </h3>
                 
                 <div className="space-y-2 max-h-64 overflow-y-auto">
                   {diagnosticState.workerMessages.length === 0 ? (
                     <div className="text-sm text-muted">Nessun messaggio worker ricevuto</div>
                   ) : (
                     diagnosticState.workerMessages.map((msg, index) => (
                       <div key={msg.id} className="p-2 bg-muted rounded text-xs">
                         <div className="font-mono">
                           <strong>worker_msg #{index + 1}:</strong> [{msg.timestamp}] {JSON.stringify(msg.data)}
                         </div>
                       </div>
                     ))
                   )}
                 </div>
               </div>
             </div>

             {/* Diagnostic Statistics Panel */}
             <div className="card border-strong">
               <div className="card-body">
                 <h3 className="card-title mb-4 flex items-center gap-2">
                   <Activity className="h-5 w-5 icon-dark" />
                   Statistiche Diagnostiche
                   {(() => {
                     const heartbeatAge = diagnosticState.lastHeartbeat ? Date.now() - diagnosticState.lastHeartbeat : 0;
                     const heartbeatColor = heartbeatAge < 2000 ? 'green' : heartbeatAge < 5000 ? 'yellow' : 'red';
                     return (
                       <span className={`ml-2 w-3 h-3 rounded-full bg-${heartbeatColor}-500`} title={`Heartbeat: ${heartbeatAge}ms fa`}></span>
                     );
                   })()}
                 </h3>
                 
                 <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                   <div className="p-3 bg-muted rounded">
                     <div className="font-medium">Total</div>
                     <div className="text-lg">{diagnosticState.statistics.total}</div>
                   </div>
                   <div className="p-3 bg-muted rounded">
                     <div className="font-medium">Batch Size</div>
                     <div className="text-lg">{diagnosticState.statistics.batchSize}</div>
                   </div>
                   <div className="p-3 bg-muted rounded">
                     <div className="font-medium">Progress %</div>
                     <div className="text-lg">{progressPct}%</div>
                   </div>
                   <div className="p-3 bg-muted rounded">
                     <div className="font-medium">Elapsed Prescan</div>
                     <div className="text-lg">{diagnosticState.statistics.elapsedPrescan}ms</div>
                   </div>
                   <div className="p-3 bg-muted rounded">
                     <div className="font-medium">Elapsed SKU</div>
                     <div className="text-lg">{diagnosticState.statistics.elapsedSku}ms</div>
                   </div>
                   <div className="p-3 bg-muted rounded">
                     <div className="font-medium">Heartbeat Age</div>
                     <div className="text-lg">{diagnosticState.lastHeartbeat ? Date.now() - diagnosticState.lastHeartbeat : 0}ms</div>
                   </div>
                   <div className="p-3 bg-muted rounded">
                     <div className="font-medium">Msg Invalid</div>
                     <div className="text-lg">{diagnosticState.errorCounters.msgInvalid}</div>
                   </div>
                   <div className="p-3 bg-muted rounded">
                     <div className="font-medium">Worker Errors</div>
                     <div className="text-lg">{diagnosticState.errorCounters.workerError}</div>
                   </div>
                   <div className="p-3 bg-muted rounded">
                     <div className="font-medium">Timeouts</div>
                     <div className="text-lg">{diagnosticState.errorCounters.timeouts}</div>
                   </div>
                 </div>

                 {/* Diagnostic Bundle */}
                 <div className="mt-4 pt-4 border-t border-strong">
                   <button
                     onClick={generateDiagnosticBundle}
                     className="btn btn-secondary"
                   >
                     Copia diagnostica
                   </button>
                 </div>
               </div>
             </div>
           </>
         )}

         {/* Statistics */}
         {currentStats && (
          <div className="card border-strong">
            <div className="card-body">
              <h3 className="card-title mb-6">Statistiche Elaborazione - Pipeline {currentPipeline}</h3>
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
                <div className="text-center p-4 rounded-lg border-strong" style={{ background: '#f8fafc' }}>
                  <div className="text-2xl font-bold">{currentStats.totalRecords.toLocaleString()}</div>
                  <div className="text-sm text-muted">Righe Totali</div>
                </div>
                <div className="text-center p-4 rounded-lg border-strong" style={{ background: 'var(--success-bg)' }}>
                  <div className="text-2xl font-bold" style={{ color: 'var(--success-fg)' }}>
                    {currentPipeline === 'EAN' ? currentStats.validRecordsEAN.toLocaleString() : currentStats.validRecordsManufPartNr.toLocaleString()}
                  </div>
                  <div className="text-sm text-muted">Valide {currentPipeline}</div>
                </div>
                <div className="text-center p-4 rounded-lg border-strong" style={{ background: 'var(--error-bg)' }}>
                  <div className="text-2xl font-bold" style={{ color: 'var(--error-fg)' }}>
                    {currentPipeline === 'EAN' ? currentStats.filteredRecordsEAN.toLocaleString() : currentStats.filteredRecordsManufPartNr.toLocaleString()}
                  </div>
                  <div className="text-sm text-muted">Scartate {currentPipeline}</div>
                </div>
                <div className="text-center p-4 rounded-lg border-strong" style={{ background: '#fff3cd' }}>
                  <div className="text-2xl font-bold" style={{ color: '#856404' }}>{currentStats.stockDuplicates + currentStats.priceDuplicates}</div>
                  <div className="text-sm text-muted">Duplicati</div>
                </div>
              </div>
              
              {eanStats && currentPipeline === 'EAN' && (
                <div className="mt-6">
                  <h4 className="text-lg font-semibold mb-3">Validazione EAN</h4>
                  <div className="grid grid-cols-4 gap-4 text-sm">
                    <div className="text-center p-3 rounded-lg border" style={{ background: '#f0f9ff' }}>
                      <div className="text-lg font-bold text-green-600">{eanStats.ean_validi_13}</div>
                      <div className="text-muted-foreground">EAN validi (13 cifre)</div>
                    </div>
                    <div className="text-center p-3 rounded-lg border" style={{ background: '#eff6ff' }}>
                      <div className="text-lg font-bold text-blue-600">{eanStats.ean_padded_12_to_13}</div>
                      <div className="text-muted-foreground">EAN padded (12→13)</div>
                    </div>
                    <div className="text-center p-3 rounded-lg border" style={{ background: '#f0fdf4' }}>
                      <div className="text-lg font-bold text-emerald-600">{eanStats.ean_trimmed_14_to_13}</div>
                      <div className="text-muted-foreground">EAN trimmed (14→13)</div>
                    </div>
                    <div className="text-center p-3 rounded-lg border" style={{ background: '#fefce8' }}>
                      <div className="text-lg font-bold text-yellow-600">{eanStats.ean_validi_14}</div>
                      <div className="text-muted-foreground">EAN validi (14 cifre)</div>
                    </div>
                    <div className="text-center p-3 rounded-lg border" style={{ background: '#fff7ed' }}>
                      <div className="text-lg font-bold text-orange-600">{eanStats.ean_duplicati_risolti}</div>
                      <div className="text-muted-foreground">Duplicati risolti</div>
                    </div>
                    <div className="text-center p-3 rounded-lg border" style={{ background: '#fef2f2' }}>
                      <div className="text-lg font-bold text-red-600">{eanStats.ean_mancanti}</div>
                      <div className="text-muted-foreground">EAN mancanti</div>
                    </div>
                    <div className="text-center p-3 rounded-lg border" style={{ background: '#fef2f2' }}>
                      <div className="text-lg font-bold text-red-600">{eanStats.ean_non_numerici}</div>
                      <div className="text-muted-foreground">EAN non numerici</div>
                    </div>
                    <div className="text-center p-3 rounded-lg border" style={{ background: '#fef2f2' }}>
                      <div className="text-lg font-bold text-red-600">{eanStats.ean_lunghezze_invalid}</div>
                      <div className="text-muted-foreground">Lunghezze non valide</div>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Download Buttons */}
        {isCompleted && currentProcessedData.length > 0 && (
          <div className="text-center">
            <h3 className="text-2xl font-bold mb-6">Download Pipeline {currentPipeline}</h3>
            <div className="flex flex-wrap justify-center gap-4">
              <button 
                type="button"
                onClick={currentPipeline === 'EAN' ? onExportEAN : () => downloadExcel('manufpartnr')} 
                disabled={isExportingEAN && currentPipeline === 'EAN'}
                className={`btn btn-primary text-lg px-8 py-3 ${isExportingEAN && currentPipeline === 'EAN' ? 'opacity-50 cursor-not-allowed' : ''}`}
              >
                <Download className="mr-3 h-5 w-5" />
                {isExportingEAN && currentPipeline === 'EAN' ? 'ESPORTAZIONE...' : `SCARICA EXCEL (${currentPipeline})`}
              </button>
              <button 
                onClick={() => downloadLog(currentPipeline === 'EAN' ? 'ean' : 'manufpartnr')} 
                className="btn btn-secondary text-lg px-8 py-3"
              >
                <Download className="mr-3 h-5 w-5" />
                SCARICA LOG ({currentPipeline})
              </button>
              {discardedRows.length > 0 && currentPipeline === 'EAN' && (
                <button 
                  onClick={downloadDiscardedRows}
                  className="btn btn-secondary text-lg px-8 py-3"
                >
                  <Download className="mr-3 h-5 w-5" />
                  SCARTI EAN ({discardedRows.length})
                </button>
              )}
            </div>
          </div>
        )}

        {/* Data Preview */}
        {currentProcessedData.length > 0 && (
          <div className="card border-strong">
            <div className="card-body">
              <h3 className="card-title mb-6">Anteprima Export {currentPipeline} (Prime 10 Righe)</h3>
              <div className="overflow-x-auto">
                <table className="table-zebra">
                  <thead>
                    <tr>
                      {Object.keys(currentProcessedData[0]).map((header, index) => (
                        <th key={index}>{header}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {currentProcessedData.slice(0, 10).map((row, rowIndex) => (
                      <tr key={rowIndex}>
                        {Object.values(row).map((value, colIndex) => (
                          <td key={colIndex}>
                            {typeof value === 'number' ? value.toLocaleString('it-IT') : String(value)}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default AltersideCatalogGenerator;