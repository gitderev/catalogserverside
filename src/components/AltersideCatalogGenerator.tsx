import React, { useState, useRef, useCallback, useEffect } from 'react';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { toast } from '@/hooks/use-toast';
import { Upload, Download, FileText, CheckCircle, XCircle, AlertCircle, Clock, Activity } from 'lucide-react';
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
  IVA: string;
  'ListPrice con IVA': number;
  'CustBestPrice con IVA': number;
  'Costo di spedizione': number;
  'Fee Mediaworld': string;
  'Fee Alterside': string;
  'Prezzo finale': number;
  'Prezzo finale Listino': number;
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

const AltersideCatalogGenerator: React.FC = () => {
  const [files, setFiles] = useState<FileUploadState>({
    material: { file: null, status: 'empty' },
    stock: { file: null, status: 'empty' },
    price: { file: null, status: 'empty' }
  });

  const [processingState, setProcessingState] = useState<'idle' | 'validating' | 'ready' | 'running' | 'completed' | 'failed'>('idle');
  const [currentPipeline, setCurrentPipeline] = useState<'EAN' | 'MPN' | null>(null);
  
  // Progress states (based on rows READ, not valid rows)
  const [total, setTotal] = useState(0);
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

  const workerRef = useRef<Worker | null>(null);

  const isProcessing = processingState === 'running';
  const isCompleted = processingState === 'completed';
  const canProcess = processingState === 'ready';
  
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

  // Consistency check functions
  const getConsistencySnapshot = useCallback(() => {
    const sumEAN = stats.validEAN + stats.discardedEAN + stats.duplicates;
    const sumMPN = stats.validMPN + stats.discardedMPN + stats.duplicates;
    return { total, processed, sumEAN, sumMPN, stats, pipeline: currentPipeline };
  }, [total, processed, stats, currentPipeline]);

  const consistencyCheck = useCallback(() => {
    if (currentPipeline === 'EAN') {
      return (stats.validEAN + stats.discardedEAN + stats.duplicates) === total;
    } else if (currentPipeline === 'MPN') {
      return (stats.validMPN + stats.discardedMPN + stats.duplicates) === total;
    }
    return false;
  }, [currentPipeline, stats, total]);

  // Completion gating effect
  useEffect(() => {
    if (joinDone && excelDone && logDone) {
      // Check consistency before allowing completion
      if (!consistencyCheck()) {
        audit('consistency-failed', getConsistencySnapshot());
        setProcessingState('failed');
        toast({
          title: `Incoerenza conteggi (pipeline ${currentPipeline})`,
          description: `Ho letto ${total} righe ma Valid+Scartati+Duplicati = ${currentPipeline === 'EAN' ? 
            stats.validEAN + stats.discardedEAN + stats.duplicates : 
            stats.validMPN + stats.discardedMPN + stats.duplicates}. Riprovo in modalità compatibilità…`,
          variant: "destructive"
        });
        return;
      }
      setProgressPct(100);
      setProcessingState('completed');
      audit('pipeline:completed', { pipeline: currentPipeline, processed, total });
    }
  }, [joinDone, excelDone, logDone, consistencyCheck, getConsistencySnapshot, currentPipeline, stats, total, processed]);

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
    setProgressPct(0);
    setProcessed(0);
    processedRef.current = 0;
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

    // Init state machine and counters based on rows READ
    setProcessingState('running');
    setTotal(materialRowsCount);
    setProcessed(0);
    processedRef.current = 0;
    setProgressPct(0);
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
      setLogDone(true);
      audit('excel:write:done', { pipeline: pipelineType });
      audit('log:write:done', { pipeline: pipelineType });
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
            setProcessed(processedRef.current);
            setProgressPct(Math.min(99, Math.floor(processedRef.current / Math.max(1, total) * 100)));
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

          const custBestPriceCeil = Math.ceil(custBestNumRaw);
          const listPriceWithIVA = listPriceNum * 1.22;
          const custBestWithIVA = custBestPriceCeil * 1.22;
          const finalPriceBest = ceilToXX99(((custBestWithIVA + 5) * 1.08) * 1.05);
          const finalPriceListino = Math.ceil(((listPriceWithIVA + 5) * 1.08) * 1.05);

          const base: ProcessedRecord = {
            Matnr: matnr,
            ManufPartNr: row.ManufPartNr || '', // always from Material
            EAN: row.EAN?.toString().trim() || '',
            ShortDescription: row.ShortDescription || '',
            ExistingStock: existingStock,
            ListPrice: listPriceNum,
            CustBestPrice: custBestPriceCeil,
            IVA: '22%',
            'ListPrice con IVA': listPriceWithIVA,
            'CustBestPrice con IVA': custBestWithIVA,
            'Costo di spedizione': 5,
            'Fee Mediaworld': '8%',
            'Fee Alterside': '5%',
            'Prezzo finale': finalPriceBest,
            'Prezzo finale Listino': finalPriceListino
          };

          if (base.EAN) {
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
          dbg('join:done');
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
        setProcessingState('failed');
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
      'Prezzo finale': record['Prezzo finale'].toFixed(2).replace('.', ','),
      'Prezzo finale Listino': record['Prezzo finale Listino'].toString()
    }));
  };

  const downloadExcel = (type: 'ean' | 'manufpartnr') => {
    if (currentProcessedData.length === 0) return;
    
    dbg('excel:write:start');
    
    const { timestamp, sheetName } = getTimestamp();
    const filename = `catalogo_${type}_${timestamp}.xlsx`;

    const excelData = formatExcelData(currentProcessedData);
    const ws = XLSX.utils.json_to_sheet(excelData);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, sheetName);
    XLSX.writeFile(wb, filename);

    dbg('excel:write:done');

    toast({
      title: "Excel scaricato",
      description: `File ${filename} scaricato con successo`
    });
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

    dbg('log:write:done');

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
                  <li>• <strong>Prezzi:</strong> CustBestPrice arrotondato per eccesso, IVA 22%, commissioni 8% + 5%</li>
                  <li>• <strong>Prezzo finale:</strong> arrotondamento a ,99 (da Best) o intero superiore (da Listino)</li>
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

        {/* Action Buttons */}
        {allFilesValid && (
          <div className="text-center">
            <h3 className="text-2xl font-bold mb-6">Azioni</h3>
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
                onClick={() => processDataPipeline('MPN')}
                disabled={!canProcess || isProcessing}
                className={`btn btn-primary text-lg px-12 py-4 ${!canProcess || isProcessing ? 'is-disabled' : ''}`}
              >
                {isProcessing && currentPipeline === 'MPN' ? (
                  <>
                    <Activity className="mr-3 h-5 w-5 animate-spin" />
                    Elaborazione ManufPartNr...
                  </>
                ) : (
                  <>
                    <Upload className="mr-3 h-5 w-5" />
                    GENERA EXCEL (ManufPartNr)
                  </>
                )}
              </button>
            </div>
          </div>
        )}

        {/* Progress Section */}
        {(isProcessing || isCompleted) && (
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
                    {total > 0 && <span className="text-muted"> / {total.toLocaleString()}</span>}
                  </div>
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
            </div>
          </div>
        )}

        {/* Download Buttons */}
        {isCompleted && currentProcessedData.length > 0 && (
          <div className="text-center">
            <h3 className="text-2xl font-bold mb-6">Download Pipeline {currentPipeline}</h3>
            <div className="flex flex-wrap justify-center gap-4">
              <button 
                onClick={() => downloadExcel(currentPipeline === 'EAN' ? 'ean' : 'manufpartnr')} 
                className="btn btn-primary text-lg px-8 py-3"
              >
                <Download className="mr-3 h-5 w-5" />
                SCARICA EXCEL ({currentPipeline})
              </button>
              <button 
                onClick={() => downloadLog(currentPipeline === 'EAN' ? 'ean' : 'manufpartnr')} 
                className="btn btn-secondary text-lg px-8 py-3"
              >
                <Download className="mr-3 h-5 w-5" />
                SCARICA LOG ({currentPipeline})
              </button>
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