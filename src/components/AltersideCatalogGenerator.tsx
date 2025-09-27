import React, { useState, useRef, useCallback, useEffect } from 'react';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { Checkbox } from '@/components/ui/checkbox';
import { toast } from '@/hooks/use-toast';
import { Download, Activity, CheckCircle, XCircle, AlertCircle } from 'lucide-react';
import { MaterialUpload } from './MaterialUpload';
import { StockUpload } from './StockUpload';
import { PriceUpload } from './PriceUpload';
import { CalculationRules } from './CalculationRules';

// Processing states
type ProcessingState = 'idle' | 'creating' | 'prescanning' | 'running' | 'done' | 'error';

const AltersideCatalogGenerator = () => {
  const [processingState, setProcessingState] = useState<ProcessingState>('idle');
  const [progressPct, setProgressPct] = useState(0);
  const [lastClickTime, setLastClickTime] = useState(0);
  
  // File states
  const [materialFile, setMaterialFile] = useState<File | null>(null);
  const [stockFile, setStockFile] = useState<File | null>(null);
  const [priceFile, setPriceFile] = useState<File | null>(null);
  
  // Data states
  const [materialData, setMaterialData] = useState<any[]>([]);
  const [stockData, setStockData] = useState<any[]>([]);
  const [priceData, setPriceData] = useState<any[]>([]);
  
  // Validation states
  const [materialValid, setMaterialValid] = useState(false);
  const [stockValid, setStockValid] = useState(false);
  const [priceValid, setPriceValid] = useState(false);
  const [stockReady, setStockReady] = useState(false);
  const [priceReady, setPriceReady] = useState(false);
  
  // Calculation rules
  const [feeDeRev, setFeeDeRev] = useState(1.00);
  const [feeMarketplace, setFeeMarketplace] = useState(1.00);
  
  // Diagnostic mode
  const [isDiagnosticMode, setIsDiagnosticMode] = useState(false);
  
  // Worker refs for SKU processing
  const workerRef = useRef<Worker | null>(null);
  const blobUrlRef = useRef<string | null>(null);
  const progressTimerRef = useRef<NodeJS.Timeout | null>(null);
  const pingTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  
  const [diagnosticState, setDiagnosticState] = useState({
    workerMessages: [],
    statistics: {
      total: 0,
      batchSize: 1000,
      elapsedPrescan: 0,
      elapsedSku: 0
    },
    errorCounters: {
      msgInvalid: 0,
      workerError: 0,
      timeouts: 0
    },
    lastHeartbeat: 0
  });

  const [workerStrategy, setWorkerStrategy] = useState<'blob' | 'module'>('blob');
  const [workerState, setWorkerState] = useState({
    created: false,
    handlersAttached: false,
    initSent: false,
    bootReceived: false,
    readyReceived: false,
    version: ''
  });

  const [debugEvents, setDebugEvents] = useState<string[]>([]);

  // Update ready states when data changes
  useEffect(() => {
    setStockReady(stockValid && stockData.length > 0);
  }, [stockValid, stockData]);

  useEffect(() => {
    setPriceReady(priceValid && priceData.length > 0);
  }, [priceValid, priceData]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (workerRef.current) {
        workerRef.current.terminate();
      }
      if (blobUrlRef.current) {
        URL.revokeObjectURL(blobUrlRef.current);
        console.log('blob_url_revoked=true (unmount)');
      }
      if (progressTimerRef.current) {
        clearTimeout(progressTimerRef.current);
      }
      if (pingTimeoutRef.current) {
        clearTimeout(pingTimeoutRef.current);
      }
    };
  }, []);

  const dbg = useCallback((event: string, data?: any) => {
    const timestamp = new Date().toLocaleTimeString('it-IT', { hour12: false });
    const logEntry = `[${timestamp}] ${event}${data ? ': ' + JSON.stringify(data) : ''}`;
    console.log(logEntry);
    setDebugEvents(prev => [...prev.slice(-19), logEntry]);
  }, []);

  const addWorkerMessage = useCallback((data: any) => {
    const timestamp = new Date().toLocaleTimeString('it-IT', { hour12: false });
    const messageId = Date.now();
    
    setDiagnosticState(prev => ({
      ...prev,
      workerMessages: [...prev.workerMessages.slice(-9), { id: messageId, timestamp, data }],
      lastHeartbeat: Date.now()
    }));
  }, []);

  // Debounced click handler
  const handleDebouncedClick = useCallback((callback: () => void) => {
    const now = Date.now();
    if (now - lastClickTime < 1000) {
      dbg('click_ignored', { reason: 'debounce', timeSinceLastClick: now - lastClickTime });
      return;
    }
    
    if (processingState === 'creating' || processingState === 'prescanning' || processingState === 'running') {
      dbg('click_ignored', { reason: 'busy', state: processingState });
      return;
    }

    setLastClickTime(now);
    callback();
  }, [lastClickTime, processingState, dbg]);

  // Create hardened worker with proper lifecycle
  const createWorker = useCallback(async (strategy: 'blob' | 'module' = 'blob') => {
    dbg('worker_creating', { strategy });
    
    try {
      let worker: Worker;
      
      if (strategy === 'blob') {
        // Create blob worker
        const workerCode = await fetch('/workers/alterside-sku-worker.js').then(r => r.text());
        const blob = new Blob([workerCode], { type: 'application/javascript' });
        const blobUrl = URL.createObjectURL(blob);
        blobUrlRef.current = blobUrl;
        worker = new Worker(blobUrl, { type: 'module' });
      } else {
        // Create module worker
        worker = new Worker('/workers/alterside-sku-worker.js', { type: 'module' });
      }
      
      workerRef.current = worker;
      
      dbg('worker_created', { strategy, url: strategy === 'blob' ? 'blob' : '/workers/alterside-sku-worker.js' });
      
      // Attach handlers immediately
      worker.onmessage = (e) => {
        const { type, ...data } = e.data;
        addWorkerMessage(e.data);
        
        switch (type) {
          case 'worker_boot':
            dbg('worker_boot', data);
            setWorkerState(prev => ({ ...prev, bootReceived: true, version: data.version || '' }));
            break;
            
          case 'worker_ready':
            dbg('worker_ready_received', { version: data.version, schema: data.schema });
            setWorkerState(prev => ({ ...prev, readyReceived: true, version: data.version || '' }));
            
            // Start prescan
            worker.postMessage({ type: 'PRESCAN_START' });
            dbg('prescan_start');
            setProcessingState('prescanning');
            
            // Start progress timer
            progressTimerRef.current = setTimeout(() => {
              dbg('progress_timeout', { note: 'no prescan_progress(0) received' });
              worker.postMessage({ type: 'ping' });
              
              pingTimeoutRef.current = setTimeout(() => {
                dbg('ping_timeout', { note: 'no pong or progress received' });
                worker.terminate();
                setProcessingState('error');
                toast({
                  title: "Errore Prescan",
                  description: "Prescan non inizializzato",
                  variant: "destructive",
                });
              }, 2000);
            }, 1000);
            break;
            
          case 'prescan_progress':
            if (pingTimeoutRef.current) {
              clearTimeout(pingTimeoutRef.current);
              pingTimeoutRef.current = null;
            }
            if (progressTimerRef.current) {
              clearTimeout(progressTimerRef.current);
              progressTimerRef.current = null;
            }
            
            dbg('prescan_progress', { done: data.done, total: data.total });
            const prescanPct = data.total > 0 ? Math.round((data.done / data.total) * 100) : 0;
            setProgressPct(prescanPct);
            break;
            
          case 'prescan_done':
            dbg('prescan_done', data);
            setProcessingState('running');
            break;
            
          case 'sku_start':
            dbg('sku_start', data);
            break;
            
          case 'sku_progress':
            dbg('sku_progress', { done: data.done, total: data.total });
            const skuPct = data.total > 0 ? Math.round((data.done / data.total) * 100) : 0;
            setProgressPct(skuPct);
            break;
            
          case 'sku_done':
            dbg('sku_done', data);
            setProcessingState('done');
            setProgressPct(100);
            
            // Download the file
            if (data.blob) {
              const url = URL.createObjectURL(data.blob);
              const link = document.createElement('a');
              link.href = url;
              link.download = data.filename || 'catalogo_sku.xlsx';
              link.click();
              URL.revokeObjectURL(url);
            }
            
            toast({
              title: "Generazione completata",
              description: `File ${data.filename || 'catalogo_sku.xlsx'} generato con successo`,
            });
            break;
            
          case 'worker_error':
            dbg('worker_error', { where: data.where, message: data.message, detail: data.detail });
            setProcessingState('error');
            toast({
              title: "Errore Worker",
              description: `${data.where}: ${data.message}`,
              variant: "destructive",
            });
            break;
            
          case 'pong':
            if (pingTimeoutRef.current) {
              clearTimeout(pingTimeoutRef.current);
              pingTimeoutRef.current = null;
            }
            dbg('pong_received');
            break;
            
          case 'worker_ready_hint':
            dbg('worker_ready_hint', data);
            break;
            
          default:
            dbg('unknown_message_type', { type, data });
        }
      };
      
      worker.onerror = (error) => {
        dbg('worker_error_event', { message: error.message, filename: error.filename, lineno: error.lineno });
        setProcessingState('error');
        toast({
          title: "Errore Worker",
          description: error.message,
          variant: "destructive",
        });
      };
      
      dbg('handlers_attached', true);
      setWorkerState(prev => ({ ...prev, handlersAttached: true }));
      
      // Send INIT message
      const initPayload = {
        type: 'INIT',
        schema: 1,
        diag: isDiagnosticMode,
        sampleSize: isDiagnosticMode ? 200 : undefined,
        version: '1.0.0'
      };
      
      worker.postMessage(initPayload);
      dbg('init_sent', initPayload);
      setWorkerState(prev => ({ ...prev, initSent: true }));
      
      setWorkerStrategy(strategy);
      
    } catch (error) {
      dbg('worker_creation_failed', { strategy, error: error.message });
      setProcessingState('error');
      toast({
        title: "Errore creazione worker",
        description: error.message,
        variant: "destructive",
      });
    }
  }, [dbg, addWorkerMessage, isDiagnosticMode]);

  // Generate current timestamp for filename
  const getCurrentTimestamp = () => {
    const now = new Date();
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');
    const hour = String(now.getHours()).padStart(2, '0');
    const minute = String(now.getMinutes()).padStart(2, '0');
    return `${year}${month}${day}_${hour}${minute}`;
  };

  // Handle SKU generation
  const handleSkuGeneration = useCallback(() => {
    handleDebouncedClick(() => {
      dbg('click_sku');
      setProcessingState('creating');
      setProgressPct(0);
      setWorkerState({
        created: false,
        handlersAttached: false,
        initSent: false,
        bootReceived: false,
        readyReceived: false,
        version: ''
      });
      createWorker(workerStrategy);
    });
  }, [handleDebouncedClick, dbg, createWorker, workerStrategy]);

  // Handle EAN generation (existing functionality)
  const handleEanGeneration = useCallback(() => {
    handleDebouncedClick(() => {
      dbg('click_ean');
      // Existing EAN generation logic would go here
      toast({
        title: "Generazione EAN",
        description: "Funzionalità EAN non modificata",
      });
    });
  }, [handleDebouncedClick, dbg]);

  // Handle diagnostic prescan
  const handleDiagnosticPrescan = useCallback(() => {
    if (!isDiagnosticMode) return;
    
    handleDebouncedClick(() => {
      dbg('click_prescan_diag');
      setProcessingState('creating');
      setProgressPct(0);
      createWorker(workerStrategy);
    });
  }, [isDiagnosticMode, handleDebouncedClick, dbg, createWorker, workerStrategy]);

  // Copy diagnostic data to clipboard
  const copyDiagnosticData = useCallback(() => {
    const diagnosticText = [
      '=== DIAGNOSTIC DATA ===',
      `Timestamp: ${new Date().toISOString()}`,
      `Worker Strategy: ${workerStrategy}`,
      `Processing State: ${processingState}`,
      `Progress: ${progressPct}%`,
      '',
      '=== DEBUG EVENTS ===',
      ...debugEvents,
      '',
      '=== WORKER MESSAGES ===',
      ...diagnosticState.workerMessages.map((msg: any) => 
        `[${msg.timestamp}] ${JSON.stringify(msg.data)}`
      )
    ].join('\n');
    
    navigator.clipboard.writeText(diagnosticText).then(() => {
      toast({
        title: "Diagnostica copiata",
        description: "Dati diagnostici copiati negli appunti",
      });
    });
  }, [workerStrategy, processingState, progressPct, debugEvents, diagnosticState.workerMessages]);

  const isProcessing = processingState === 'creating' || processingState === 'prescanning' || processingState === 'running';
  const allFilesValid = materialValid && stockValid && priceValid;

  return (
    <div className="min-h-screen bg-background">
      <div className="container mx-auto p-6">
        <div className="mb-8">
          <h1 className="text-3xl font-bold mb-2">Generatore Catalogo Alterside</h1>
          <p className="text-muted-foreground">
            Carica i file Material, Stock e Price per generare il catalogo Excel
          </p>
        </div>

        {/* Upload Sections */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-6">
          <MaterialUpload
            onFileLoaded={(data, headers, isValid) => {
              setMaterialData(data);
              setMaterialValid(isValid);
              if (isValid) setMaterialFile(new File([], 'material.csv'));
            }}
            selectedFile={materialFile}
            isValid={materialValid}
          />
          <StockUpload
            onFileLoaded={(data, headers, isValid) => {
              setStockData(data);
              setStockValid(isValid);
              if (isValid) setStockFile(new File([], 'stock.csv'));
            }}
            selectedFile={stockFile}
            isValid={stockValid}
          />
          <PriceUpload
            onFileLoaded={(data, headers, isValid) => {
              setPriceData(data);
              setPriceValid(isValid);
              if (isValid) setPriceFile(new File([], 'price.csv'));
            }}
            selectedFile={priceFile}
            isValid={priceValid}
          />
        </div>

        {/* Calculation Rules */}
        <div className="mb-6">
          <CalculationRules
            feeDeRev={feeDeRev}
            feeMarketplace={feeMarketplace}
            onFeeDeRevChange={setFeeDeRev}
            onFeeMarketplaceChange={setFeeMarketplace}
          />
        </div>

        {/* Diagnostic Mode Toggle */}
        <Card className="mb-6 p-4">
          <div className="flex items-center space-x-2">
            <Checkbox 
              id="diagnostic-mode"
              checked={isDiagnosticMode}
              onCheckedChange={(checked) => setIsDiagnosticMode(checked === true)}
            />
            <label htmlFor="diagnostic-mode" className="text-sm font-medium">
              Modalità diagnostica
            </label>
          </div>
        </Card>

        {/* Actions */}
        <Card className="mb-6 p-6">
          <div className="space-y-4">
            
            {/* Diagnostic Prescan Button - Only visible with diagnostic mode ON */}
            {isDiagnosticMode && (
              <Button
                onClick={handleDiagnosticPrescan}
                disabled={isProcessing}
                variant="outline"
                className="w-full"
              >
                <Activity className="mr-2 h-4 w-4" />
                Diagnostica prescan
              </Button>
            )}

            {/* Main Action Buttons */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <Button
                onClick={handleEanGeneration}
                disabled={isProcessing || !allFilesValid}
                className="h-12"
              >
                <Download className="mr-2 h-4 w-4" />
                GENERA EXCEL (EAN)
              </Button>

              <Button
                onClick={handleSkuGeneration}
                disabled={isProcessing || !allFilesValid}
                className="h-12"
              >
                <Download className="mr-2 h-4 w-4" />
                GENERA EXCEL (ManufPartNr)
              </Button>
            </div>

            {/* Progress Display */}
            {isProcessing && (
              <div className="mt-4">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-sm font-medium">Progresso</span>
                  <span className="text-sm text-muted-foreground">{progressPct}%</span>
                </div>
                <div className="w-full bg-secondary rounded-full h-2">
                  <div 
                    className="bg-primary h-2 rounded-full transition-all duration-300"
                    style={{ width: `${progressPct}%` }}
                  />
                </div>
                <div className="mt-2 text-sm text-muted-foreground">
                  Stato: {processingState === 'creating' && 'Creazione worker...'}
                  {processingState === 'prescanning' && 'Prescanning...'}
                  {processingState === 'running' && 'Generazione in corso...'}
                </div>
              </div>
            )}
          </div>
        </Card>

        {/* Diagnostic Panels - Only visible with diagnostic mode ON */}
        {isDiagnosticMode && (
          <div className="space-y-6">
            {/* Worker Messages */}
            <Card className="p-6">
              <h3 className="text-lg font-semibold mb-4">Messaggi Worker</h3>
              <div className="bg-muted p-3 rounded max-h-32 overflow-y-auto">
                {diagnosticState.workerMessages.length === 0 ? (
                  <div className="text-muted-foreground text-sm">Nessun messaggio ricevuto</div>
                ) : (
                  diagnosticState.workerMessages.map((msg: any) => (
                    <div key={msg.id} className="text-sm font-mono mb-1">
                      [{msg.timestamp}] {JSON.stringify(msg.data)}
                    </div>
                  ))
                )}
              </div>
            </Card>

            {/* Statistics */}
            <Card className="p-6">
              <h3 className="text-lg font-semibold mb-4">Statistiche Diagnostiche</h3>
              <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                <div className="p-3 bg-muted rounded">
                  <div className="font-medium">Worker Strategy</div>
                  <div className="text-lg">{workerStrategy}</div>
                </div>
                <div className="p-3 bg-muted rounded">
                  <div className="font-medium">Stato</div>
                  <div className="text-lg">{processingState}</div>
                </div>
                <div className="p-3 bg-muted rounded">
                  <div className="font-medium">Progresso</div>
                  <div className="text-lg">{progressPct}%</div>
                </div>
                <div className="p-3 bg-muted rounded">
                  <div className="font-medium">Messaggi</div>
                  <div className="text-lg">{diagnosticState.workerMessages.length}</div>
                </div>
              </div>

              {/* Debug Events */}
              <div className="mt-4">
                <h4 className="font-medium mb-2">Eventi Debug</h4>
                <div className="bg-muted p-3 rounded max-h-32 overflow-y-auto">
                  {debugEvents.length === 0 ? (
                    <div className="text-muted-foreground text-sm">Nessun evento registrato</div>
                  ) : (
                    debugEvents.map((event, index) => (
                      <div key={index} className="text-sm font-mono">{event}</div>
                    ))
                  )}
                </div>
              </div>
            </Card>

            {/* Copy Diagnostic Data */}
            <Card className="p-6">
              <h3 className="text-lg font-semibold mb-4">Copia diagnostica</h3>
              <Button
                onClick={copyDiagnosticData}
                variant="outline"
                className="w-full"
              >
                Copia dati diagnostici negli appunti
              </Button>
            </Card>
          </div>
        )}
      </div>
    </div>
  );
};

export default AltersideCatalogGenerator;
