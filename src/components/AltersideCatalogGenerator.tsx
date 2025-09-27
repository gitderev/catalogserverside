import React, { useState, useRef, useCallback, useEffect, useReducer } from 'react';
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
  const hardTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const progressWatchdogRef = useRef<NodeJS.Timeout | null>(null);
  const lastProgressRef = useRef<number>(0);
  
  const [diagnosticState, setDiagnosticState] = useState({
    isEnabled: false,
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

  // Debug events with reducer and throttling
  const debugEventsRef = useRef<string[]>([]);
  const [debugEvents, dispatchDebugEvent] = useReducer(
    (state: string[], action: { type: 'ADD'; event: string } | { type: 'FLUSH'; events: string[] }) => {
      switch (action.type) {
        case 'ADD':
          const timestamp = new Date().toLocaleTimeString();
          const eventWithTime = `[${timestamp}] ${action.event}`;
          const updated = [...debugEventsRef.current, eventWithTime];
          debugEventsRef.current = updated.length > 200 ? updated.slice(-200) : updated;
          return debugEventsRef.current;
        case 'FLUSH':
          debugEventsRef.current = action.events;
          return action.events;
        default:
          return state;
      }
    },
    []
  );

  // Throttled debug event utility
  const flushTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const addDebugEvent = useCallback((event: string) => {
    dispatchDebugEvent({ type: 'ADD', event });
    
    // Throttle UI updates to max 10fps
    if (flushTimeoutRef.current) return;
    flushTimeoutRef.current = setTimeout(() => {
      flushTimeoutRef.current = null;
      dispatchDebugEvent({ type: 'FLUSH', events: debugEventsRef.current });
    }, 100);
  }, []);

  // One-time gate logging and CSS validation on mount only
  useEffect(() => {
    const allFilesValid = materialValid && stockValid && priceValid;
    
    // Gate visibility logging
    console.log(`[GATE] materialValid=${materialValid}, stockValid=${stockValid}, priceValid=${priceValid}, stockReady=${stockReady}, priceReady=${priceReady}, allFilesValid=${allFilesValid}, diagnosticState.isEnabled=${diagnosticState.isEnabled}`);
    
    // CSS Variables validation  
    const vars = {
      background: getComputedStyle(document.documentElement).getPropertyValue('--background').trim(),
      card: getComputedStyle(document.documentElement).getPropertyValue('--card').trim(),
      muted: getComputedStyle(document.documentElement).getPropertyValue('--muted').trim(),
      primary: getComputedStyle(document.documentElement).getPropertyValue('--primary').trim(),
      ring: getComputedStyle(document.documentElement).getPropertyValue('--ring').trim(),
      input: getComputedStyle(document.documentElement).getPropertyValue('--input').trim()
    };
    const bodyBg = getComputedStyle(document.body).backgroundColor;
    
    addDebugEvent(`css_vars: --background="${vars.background}", --card="${vars.card}", --muted="${vars.muted}", --primary="${vars.primary}", --ring="${vars.ring}", --input="${vars.input}"`);
    addDebugEvent(`body_bg: ${bodyBg}`);
    
    // Button and input probes after mount
    const probeElements = () => {
      // Button probes
      ['btn-ean', 'btn-sku', 'btn-diag'].forEach(btnId => {
        const btn = document.querySelector(`[data-id="${btnId}"]`) as HTMLElement;
        if (btn) {
          const styles = getComputedStyle(btn);
          console.log(`[PROBE] btn_${btnId}: exists=true, display="${styles.display}", visibility="${styles.visibility}", opacity="${styles.opacity}", pointerEvents="${styles.pointerEvents}", color="${styles.color}", backgroundColor="${styles.backgroundColor}"`);
        } else {
          console.log(`[PROBE] btn_${btnId}: missing_in_dom`);
        }
      });
      
      // Input Fee probes
      ['fee-derev', 'fee-marketplace'].forEach(inputId => {
        const input = document.querySelector(`#${inputId}`) as HTMLElement;
        if (input) {
          const styles = getComputedStyle(input);
          console.log(`[PROBE] input_${inputId}: background="${styles.backgroundColor}", borderColor="${styles.borderColor}"`);
        }
      });
    };
    
    setTimeout(probeElements, 100);
  }, []); // Empty dependency array - run once on mount only

  // Update ready states when data changes - with guards to prevent unnecessary re-renders
  useEffect(() => {
    const newStockReady = stockValid && stockData.length > 0;
    if (stockReady !== newStockReady) {
      setStockReady(newStockReady);
    }
  }, [stockValid, stockData.length, stockReady]);

  useEffect(() => {
    const newPriceReady = priceValid && priceData.length > 0;
    if (priceReady !== newPriceReady) {
      setPriceReady(newPriceReady);
    }
  }, [priceValid, priceData.length, priceReady]);

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
      if (flushTimeoutRef.current) {
        clearTimeout(flushTimeoutRef.current);
      }
      if (hardTimeoutRef.current) {
        clearTimeout(hardTimeoutRef.current);
      }
      if (progressWatchdogRef.current) {
        clearTimeout(progressWatchdogRef.current);
      }
    };
  }, []);

  const dbg = useCallback((event: string, data?: any) => {
    const timestamp = new Date().toLocaleTimeString('it-IT', { hour12: false });
    const logEntry = `[${timestamp}] ${event}${data ? ': ' + JSON.stringify(data) : ''}`;
    console.log(logEntry);
    addDebugEvent(logEntry);
  }, [addDebugEvent]);

  const addWorkerMessage = useCallback((data: any) => {
    const timestamp = new Date().toLocaleTimeString('it-IT', { hour12: false });
    const messageId = Date.now();
    
    setDiagnosticState(prev => {
      const newMessages = [...prev.workerMessages.slice(-9), { id: messageId, timestamp, data }];
      const newHeartbeat = Date.now();
      
      // Only update if something actually changed
      if (prev.workerMessages.length !== newMessages.length || 
          prev.lastHeartbeat !== newHeartbeat) {
        return {
          ...prev,
          workerMessages: newMessages,
          lastHeartbeat: newHeartbeat
        };
      }
      return prev;
    });
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
            
            // Start diagnostic protocol timers
            // 1. Prescan must start within 1s
            progressTimerRef.current = setTimeout(() => {
              dbg('progress_timeout', { note: 'no prescan_progress(0) received within 1s' });
              worker.terminate();
              setProcessingState('error');
              setDiagnosticState(prev => ({
                ...prev,
                errorCounters: {
                  ...prev.errorCounters,
                  timeouts: prev.errorCounters.timeouts + 1
                }
              }));
              toast({
                title: "Errore Prescan",
                description: "Prescan non inizializzato",
                variant: "destructive",
              });
            }, 1000);
            
            // 2. Hard timeout after 20s
            hardTimeoutRef.current = setTimeout(() => {
              dbg('hard_timeout', { note: 'diagnostic prescan timeout after 20s' });
              worker.terminate();
              setProcessingState('error');
              setDiagnosticState(prev => ({
                ...prev,
                errorCounters: {
                  ...prev.errorCounters,
                  timeouts: prev.errorCounters.timeouts + 1
                }
              }));
              toast({
                title: "Timeout Diagnostica",
                description: "Diagnostica terminata per timeout (20s)",
                variant: "destructive",
              });
            }, 20000);
            break;
            
          case 'prescan_progress':
            // Clear initial timeout on first progress
            if (progressTimerRef.current) {
              clearTimeout(progressTimerRef.current);
              progressTimerRef.current = null;
            }
            
            // Clear any existing watchdog
            if (progressWatchdogRef.current) {
              clearTimeout(progressWatchdogRef.current);
              progressWatchdogRef.current = null;
            }
            
            dbg('prescan_progress', { done: data.done, total: data.total });
            const prescanPct = data.total > 0 ? Math.round((data.done / data.total) * 100) : 0;
            setProgressPct(prescanPct);
            
            // Start progress watchdog - progress must continue within 5s
            if (data.done < data.total) {
              lastProgressRef.current = data.done;
              progressWatchdogRef.current = setTimeout(() => {
                dbg('progress_stalled', { note: 'no progress for 5s', lastProgress: lastProgressRef.current });
                worker.terminate();
                setProcessingState('error');
                setDiagnosticState(prev => ({
                  ...prev,
                  errorCounters: {
                    ...prev.errorCounters,
                    timeouts: prev.errorCounters.timeouts + 1
                  }
                }));
                toast({
                  title: "Prescan Bloccato",
                  description: "Nessun progresso per 5 secondi",
                  variant: "destructive",
                });
              }, 5000);
            }
            break;
            
          case 'prescan_done':
            // Clear all timers on completion
            if (progressWatchdogRef.current) {
              clearTimeout(progressWatchdogRef.current);
              progressWatchdogRef.current = null;
            }
            if (hardTimeoutRef.current) {
              clearTimeout(hardTimeoutRef.current);
              hardTimeoutRef.current = null;
            }
            
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
    if (!diagnosticState.isEnabled) return;
    
    handleDebouncedClick(() => {
      addDebugEvent('click_diag');
      setProcessingState('creating');
      setProgressPct(0);
      lastProgressRef.current = 0;
      createWorker(workerStrategy);
    });
  }, [diagnosticState.isEnabled, handleDebouncedClick, addDebugEvent, createWorker, workerStrategy]);

  // Cancel diagnostic processing
  const handleCancelDiagnostic = useCallback(() => {
    if (workerRef.current) {
      workerRef.current.terminate();
      workerRef.current = null;
    }
    
    // Clear all timers
    [progressTimerRef, pingTimeoutRef, hardTimeoutRef, progressWatchdogRef].forEach(ref => {
      if (ref.current) {
        clearTimeout(ref.current);
        ref.current = null;
      }
    });
    
    setProcessingState('idle');
    setProgressPct(0);
    addDebugEvent('diagnostic_cancelled');
    
    toast({
      title: "Diagnostica Annullata",
      description: "Processo diagnostico interrotto dall'utente",
    });
  }, [addDebugEvent]);

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

        {/* Actions - ALWAYS RENDERED (no gating) */}
        <Card className="mb-6 p-6">
          <div className="space-y-4">
            
            {/* Diagnostic Mode Toggle - Positioned immediately above action buttons */}
            <div className="flex items-center space-x-2 pb-4 border-b">
              <Checkbox 
                id="diagnostic-mode"
                checked={diagnosticState.isEnabled}
                onCheckedChange={(checked) => {
                  const isEnabled = checked === true;
                  if (diagnosticState.isEnabled !== isEnabled) {
                    setDiagnosticState(prev => ({ ...prev, isEnabled }));
                    addDebugEvent(`state:change diagnostic_mode=${isEnabled}`);
                  }
                }}
              />
              <label htmlFor="diagnostic-mode" className="text-sm font-medium">
                Modalità diagnostica
              </label>
            </div>
            
            {/* Diagnostic Prescan Button - Only exists in DOM when diagnostic mode ON */}
            {diagnosticState.isEnabled && (
              <div className="space-y-2">
                <Button
                  onClick={handleDiagnosticPrescan}
                  disabled={isProcessing}
                  variant="secondary"
                  className="w-full"
                  data-id="btn-diag"
                >
                  <Activity className="mr-2 h-4 w-4" />
                  Diagnostica prescan
                </Button>
                
                {/* Cancel button during processing */}
                {isProcessing && (
                  <Button
                    onClick={handleCancelDiagnostic}
                    variant="destructive"
                    className="w-full"
                  >
                    <XCircle className="mr-2 h-4 w-4" />
                    Annulla
                  </Button>
                )}
              </div>
            )}

            {/* Main Action Buttons */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <Button
                onClick={() => {
                  addDebugEvent('click_ean');
                  handleEanGeneration();
                }}
                disabled={isProcessing || !allFilesValid}
                variant="default"
                className="h-12 disabled:opacity-50 disabled:pointer-events-none"
                data-id="btn-ean"
              >
                <Download className="mr-2 h-4 w-4" />
                GENERA EXCEL (EAN)
              </Button>

              <Button
                onClick={() => {
                  addDebugEvent('click_sku');
                  handleSkuGeneration();
                }}
                disabled={isProcessing || !allFilesValid}
                variant="default"
                className="h-12 disabled:opacity-50 disabled:pointer-events-none"
                data-id="btn-sku"
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

        {/* Debug/Diagnostica - Only visible when diagnostic mode ON */}
        {diagnosticState.isEnabled && (
          <div className="space-y-6">
            {/* Eventi Debug */}
            <Card className="p-6">
              <h3 className="text-lg font-semibold mb-4">Eventi Debug</h3>
              
              {/* Runtime CSS Variables Validation */}
              <div className="mb-4 p-3 bg-muted rounded border">
                <h4 className="font-medium text-sm mb-2 text-foreground">Validazione CSS Variables</h4>
                <div className="text-xs font-mono space-y-1">
                  {(() => {
                    const bg = getComputedStyle(document.documentElement).getPropertyValue('--background').trim();
                    const card = getComputedStyle(document.documentElement).getPropertyValue('--card').trim();
                    const muted = getComputedStyle(document.documentElement).getPropertyValue('--muted').trim();
                    const primary = getComputedStyle(document.documentElement).getPropertyValue('--primary').trim();
                    const ring = getComputedStyle(document.documentElement).getPropertyValue('--ring').trim();
                    const input = getComputedStyle(document.documentElement).getPropertyValue('--input').trim();
                    
                    // Get button styles
                    const btnSku = document.querySelector('[data-button="sku"]') as HTMLElement;
                    const btnEan = document.querySelector('[data-button="ean"]') as HTMLElement;
                    
                    return (
                      <>
                        <div className={bg === '0 0% 100%' ? 'text-foreground' : 'text-destructive'}>
                          --background: "{bg}" {bg === '0 0% 100%' ? '✓' : '✗ Expected: "0 0% 100%"'}
                        </div>
                        <div className={card === '0 0% 100%' ? 'text-foreground' : 'text-destructive'}>
                          --card: "{card}" {card === '0 0% 100%' ? '✓' : '✗ Expected: "0 0% 100%"'}
                        </div>
                        <div className={muted === '210 40% 96%' ? 'text-foreground' : 'text-destructive'}>
                          --muted: "{muted}" {muted === '210 40% 96%' ? '✓' : '✗ Expected: "210 40% 96%"'}
                        </div>
                        <div className={primary === '221.2 83.2% 53.3%' ? 'text-foreground' : 'text-destructive'}>
                          --primary: "{primary}" {primary === '221.2 83.2% 53.3%' ? '✓' : '✗ Expected: "221.2 83.2% 53.3%"'}
                        </div>
                        <div className={ring === '221.2 83.2% 53.3%' ? 'text-foreground' : 'text-destructive'}>
                          --ring: "{ring}" {ring === '221.2 83.2% 53.3%' ? '✓' : '✗ Expected: "221.2 83.2% 53.3%"'}
                        </div>
                        <div className={input === '214.3 31.8% 91.4%' ? 'text-foreground' : 'text-destructive'}>
                          --input: "{input}" {input === '214.3 31.8% 91.4%' ? '✓' : '✗ Expected: "214.3 31.8% 91.4%"'}
                        </div>
                        <div className="mt-2 text-muted-foreground">
                          body.backgroundColor: {getComputedStyle(document.body).backgroundColor}
                        </div>
                        {btnSku && (
                          <div className="mt-1 text-muted-foreground">
                            btnSku.color: {getComputedStyle(btnSku).color} | bg: {getComputedStyle(btnSku).backgroundColor}
                          </div>
                        )}
                        {btnEan && (
                          <div className="mt-1 text-muted-foreground">
                            btnEan.color: {getComputedStyle(btnEan).color} | bg: {getComputedStyle(btnEan).backgroundColor}
                          </div>
                        )}
                      </>
                    );
                  })()}
                </div>
              </div>

              <div className="bg-muted p-3 rounded max-h-32 overflow-y-auto">
                {debugEvents.length === 0 ? (
                  <div className="text-muted-foreground text-sm">Nessun evento registrato</div>
                ) : (
                  debugEvents.map((event, index) => (
                    <div key={index} className="text-sm font-mono text-foreground">{event}</div>
                  ))
                )}
              </div>
            </Card>

            {/* Messaggi Worker (primi 10) */}
            <Card className="p-6">
              <h3 className="text-lg font-semibold mb-4 text-foreground">Messaggi Worker (primi 10)</h3>
              <div className="bg-muted p-3 rounded max-h-32 overflow-y-auto">
                {diagnosticState.workerMessages.length === 0 ? (
                  <div className="text-muted-foreground text-sm">Nessun messaggio ricevuto</div>
                ) : (
                  diagnosticState.workerMessages.slice(0, 10).map((msg: any) => (
                    <div key={msg.id} className="text-sm font-mono mb-1 text-foreground">
                      [{msg.timestamp}] {JSON.stringify(msg.data)}
                    </div>
                  ))
                )}
              </div>
            </Card>

            {/* Statistiche Diagnostiche */}
            <Card className="p-6">
              <h3 className="text-lg font-semibold mb-4 text-foreground">Statistiche Diagnostiche</h3>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <div className="p-3 bg-muted rounded">
                  <div className="font-medium text-foreground">Total</div>
                  <div className="text-lg text-foreground">{diagnosticState.statistics.total}</div>
                </div>
                <div className="p-3 bg-muted rounded">
                  <div className="font-medium text-foreground">BatchSize</div>
                  <div className="text-lg text-foreground">{diagnosticState.statistics.batchSize}</div>
                </div>
                <div className="p-3 bg-muted rounded">
                  <div className="font-medium text-foreground">Elapsed Prescan</div>
                  <div className="text-lg text-foreground">{diagnosticState.statistics.elapsedPrescan}ms</div>
                </div>
                <div className="p-3 bg-muted rounded">
                  <div className="font-medium text-foreground">Elapsed SKU</div>
                  <div className="text-lg text-foreground">{diagnosticState.statistics.elapsedSku}ms</div>
                </div>
                <div className="p-3 bg-muted rounded">
                  <div className="font-medium text-foreground">Progress %</div>
                  <div className="text-lg text-foreground">{diagnosticState.statistics.progressPct}%</div>
                </div>
                <div className="p-3 bg-muted rounded">
                  <div className="font-medium text-foreground">Heartbeat Age</div>
                  <div className="text-lg text-foreground">{diagnosticState.statistics.heartbeatAgeMs}ms</div>
                </div>
                <div className="p-3 bg-muted rounded">
                  <div className="font-medium text-foreground">Msg Invalid</div>
                  <div className="text-lg text-foreground">{diagnosticState.errorCounters.msgInvalid}</div>
                </div>
                <div className="p-3 bg-muted rounded">
                  <div className="font-medium text-foreground">Worker Errors</div>
                  <div className="text-lg text-foreground">{diagnosticState.errorCounters.workerError}</div>
                </div>
                <div className="p-3 bg-muted rounded">
                  <div className="font-medium text-foreground">Timeouts</div>
                  <div className="text-lg text-foreground">{diagnosticState.errorCounters.timeouts}</div>
                </div>
              </div>
            </Card>

            {/* Copia Diagnostica */}
            <Card className="p-6">
              <h3 className="text-lg font-semibold mb-4 text-foreground">Copia diagnostica</h3>
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
