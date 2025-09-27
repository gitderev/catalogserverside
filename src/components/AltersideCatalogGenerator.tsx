import React, { useState, useRef, useCallback, useEffect } from 'react';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Checkbox } from '@/components/ui/checkbox';
import { toast } from '@/hooks/use-toast';
import { Upload, Download, FileText, CheckCircle, XCircle, AlertCircle, Clock, Activity, Info, X } from 'lucide-react';

// Processing states
type ProcessingState = 'idle' | 'creating' | 'prescanning' | 'running' | 'done' | 'error';

// Hardened worker test component
const AltersideCatalogGenerator = () => {
  const [processingState, setProcessingState] = useState<ProcessingState>('idle');
  const [progressPct, setProgressPct] = useState(0);
  const [lastClickTime, setLastClickTime] = useState(0);
  
  // Worker refs
  const workerRef = useRef<Worker | null>(null);
  const blobUrlRef = useRef<string | null>(null);
  const progressTimerRef = useRef<NodeJS.Timeout | null>(null);
  const pingTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  
  const [diagnosticState, setDiagnosticState] = useState({
    isEnabled: false,
    maxRows: 200,
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
    testResults: [],
    lastHeartbeat: 0
  });

  const [workerStrategy, setWorkerStrategy] = useState<'blob' | 'module'>('blob');
  const [echoTestResult, setEchoTestResult] = useState<string>('');
  const [workerState, setWorkerState] = useState({
    created: false,
    handlersAttached: false,
    initSent: false,
    bootReceived: false,
    readyReceived: false,
    version: ''
  });

  const [debugEvents, setDebugEvents] = useState<string[]>([]);

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
  const handleDiagnosticClick = useCallback(() => {
    const now = Date.now();
    if (now - lastClickTime < 1000) {
      dbg('click_ignored', { reason: 'debounce', timeSinceLastClick: now - lastClickTime });
      return;
    }
    setLastClickTime(now);

    // Check if already processing
    if (processingState !== 'idle') {
      dbg('click_ignored', { reason: 'state', currentState: processingState });
      return;
    }

    dbg('click_diag');
    createHardenedWorker();
  }, [lastClickTime, processingState]);

  // Create hardened worker with proper lifecycle
  const createHardenedWorker = useCallback(async () => {
    setProcessingState('creating');
    
    try {
      // Cleanup existing worker
      if (workerRef.current) {
        workerRef.current.terminate();
        workerRef.current = null;
      }
      if (blobUrlRef.current) {
        URL.revokeObjectURL(blobUrlRef.current);
        blobUrlRef.current = null;
      }

      // Generate version
      const appVersion = '1.0.0';
      const workerVersion = Date.now().toString(36);
      
      let worker: Worker;
      
      if (workerStrategy === 'blob') {
        // Create hardened blob worker with proper string concatenation
        const workerCodeParts = [
          'let hasReceivedInit = false;',
          'const workerVersion = "' + workerVersion + '";',
          '',
          '// Worker boot signal - FIRST LINE',
          'self.postMessage({ type: "worker_boot", version: workerVersion });',
          '',
          '// Test channel after brief delay if no INIT received',
          'setTimeout(() => {',
          '  if (!hasReceivedInit) {',
          '    self.postMessage({',
          '      type: "worker_ready_hint",',
          '      note: "waiting_INIT",',
          '      version: workerVersion',
          '    });',
          '  }',
          '}, 0);',
          '',
          'self.addEventListener("error", function(e) {',
          '  self.postMessage({',
          '    type: "worker_error",',
          '    where: "boot",',
          '    message: e.message || "Worker error",',
          '    detail: { filename: e.filename, lineno: e.lineno }',
          '  });',
          '});',
          '',
          'self.onmessage = function(e) {',
          '  try {',
          '    const { type, data } = e.data || {};',
          '',
          '    if (type === "INIT") {',
          '      hasReceivedInit = true;',
          '      self.postMessage({ type: "worker_log", msg: "init_received" });',
          '      self.postMessage({',
          '        type: "worker_ready",',
          '        version: workerVersion,',
          '        schema: 1',
          '      });',
          '      console.log("ready_emitted=true");',
          '      return;',
          '    }',
          '',
          '    if (type === "PRESCAN_START") {',
          '      const materialData = data?.materialData || [];',
          '      const total = materialData.filter(row =>',
          '        row.ManufPartNr && String(row.ManufPartNr).trim()',
          '      ).length;',
          '',
          '      self.postMessage({',
          '        type: "prescan_progress",',
          '        done: 0,',
          '        total: total',
          '      });',
          '',
          '      let done = 0;',
          '      const batchSize = Math.max(1, Math.floor(total / 5));',
          '',
          '      const processBatch = () => {',
          '        done = Math.min(done + batchSize, total);',
          '',
          '        self.postMessage({',
          '          type: "prescan_progress",',
          '          done: done,',
          '          total: total',
          '        });',
          '',
          '        if (done >= total) {',
          '          self.postMessage({',
          '            type: "prescan_done",',
          '            counts: {',
          '              mpnIndexed: total,',
          '              eanIndexed: materialData.filter(row => row.EAN).length',
          '            },',
          '            total: total',
          '          });',
          '        } else {',
          '          setTimeout(processBatch, 200);',
          '        }',
          '      };',
          '',
          '      setTimeout(processBatch, 100);',
          '      return;',
          '    }',
          '',
          '    if (type === "ping") {',
          '      self.postMessage({ type: "pong" });',
          '      return;',
          '    }',
          '',
          '    // Unknown command',
          '    self.postMessage({',
          '      type: "worker_error",',
          '      where: "boot",',
          '      message: "unknown_command",',
          '      detail: type',
          '    });',
          '',
          '  } catch (error) {',
          '    self.postMessage({',
          '      type: "worker_error",',
          '      where: "runtime",',
          '      message: error.message,',
          '      stack: error.stack',
          '    });',
          '  }',
          '};'
        ];
        
        const workerCode = workerCodeParts.join('\n');
        const blob = new Blob([workerCode], { type: 'text/javascript' });
        const blobUrl = URL.createObjectURL(blob);
        blobUrlRef.current = blobUrl;
        worker = new Worker(blobUrl);
        
        dbg('worker_created', { strategy: 'blob', version: workerVersion });
        
      } else {
        // Module worker fallback
        worker = new Worker(new URL('/workers/alterside-sku-worker.js', location.origin), { type: 'module' });
        dbg('worker_created', { strategy: 'module', version: workerVersion });
      }

      workerRef.current = worker;

      // UI dopo worker_created: Aggancia subito worker.onmessage e worker.onerror
      worker.onmessage = (e: MessageEvent) => {
        const { type } = e.data;
        addWorkerMessage(e.data);
        
        if (type === 'worker_boot') {
          dbg('worker_boot', e.data);
          // Non cambiare stato, attendi worker_ready
          return;
        }
        
        if (type === 'worker_ready_hint') {
          dbg('worker_ready_hint', e.data);
          // Channel test received but still waiting for proper worker_ready
          return;
        }
        
        if (type === 'worker_ready') {
          dbg('worker_ready_received=true', e.data);
          
          // Invia PRESCAN_START dopo worker_ready
          const materialData = Array.from({ length: 100 }, (_, i) => ({
            ManufPartNr: `MPN${i + 1}`,
            EAN: `12345678901${i.toString().padStart(2, '0')}`,
            ShortDescription: `Product ${i + 1}`
          }));
          
          worker.postMessage({
            type: 'PRESCAN_START',
            data: { materialData }
          });
          
          dbg('prescan_start');
          setProcessingState('prescanning');
          
          // Avvia progressTimer 1s
          progressTimerRef.current = setTimeout(() => {
            dbg('progress_timer_expired');
            // Invia ping se non arriva prescan_progress(0)
            worker.postMessage({ type: 'ping' });
            
            // Se entro 2s non arriva pong o progress(0), chiudi worker
            pingTimeoutRef.current = setTimeout(() => {
              dbg('prescan_timeout', { reason: 'no_progress_or_pong' });
              worker.terminate();
              setProcessingState('error');
              toast({
                title: "Errore Prescan",
                description: "Prescan non inizializzato",
                variant: "destructive"
              });
            }, 2000);
          }, 1000);
          
          return;
        }
        
        if (type === 'worker_log') {
          dbg('worker_log', e.data);
          return;
        }
        
        if (type === 'prescan_progress') {
          // Clear progress timer on first progress
          if (progressTimerRef.current) {
            clearTimeout(progressTimerRef.current);
            progressTimerRef.current = null;
          }
          if (pingTimeoutRef.current) {
            clearTimeout(pingTimeoutRef.current);
            pingTimeoutRef.current = null;
          }
          
          const progress = e.data.total > 0 ? Math.round((e.data.done / e.data.total) * 100) : 0;
          setProgressPct(progress);
          
          if (e.data.done === 0) {
            dbg('prescan_progress(0,total)', e.data);
          } else {
            dbg('prescan_progress', e.data);
          }
          return;
        }
        
        if (type === 'prescan_done') {
          dbg('prescan_done', e.data);
          setProcessingState('done');
          return;
        }
        
        if (type === 'pong') {
          dbg('pong_received');
          if (pingTimeoutRef.current) {
            clearTimeout(pingTimeoutRef.current);
            pingTimeoutRef.current = null;
          }
          return;
        }
        
        if (type === 'worker_error') {
          dbg('worker_error', e.data);
          setProcessingState('error');
          toast({
            title: "Errore Worker",
            description: e.data.message || "Errore sconosciuto",
            variant: "destructive"
          });
          return;
        }
      };
      
      worker.onerror = (error: ErrorEvent) => {
        dbg('worker_onerror', { message: error.message, filename: error.filename });
        setProcessingState('error');
        toast({
          title: "Errore Worker",
          description: error.message || "Errore worker",
          variant: "destructive"
        });
      };
      
      // Log handlers attached
      console.log('handlers_attached=true');
      dbg('handlers_attached=true');
      
      // Invia subito INIT
      const initPayload = {
        type: 'INIT',
        schema: 1,
        diag: diagnosticState.isEnabled,
        sampleSize: diagnosticState.isEnabled ? diagnosticState.maxRows : undefined,
        version: appVersion
      };
      
      worker.postMessage(initPayload);
      console.log('init_sent=', initPayload);
      dbg('init_sent', initPayload);
      
    } catch (error) {
      dbg('worker_creation_error', { error: error instanceof Error ? error.message : error });
      setProcessingState('error');
      toast({
        title: "Errore Creazione Worker",
        description: error instanceof Error ? error.message : "Errore sconosciuto",
        variant: "destructive"
      });
    }
  }, [workerStrategy, diagnosticState.isEnabled, diagnosticState.maxRows, dbg, toast]);

  const generateDiagnosticBundle = useCallback(() => {
    const bundle = {
      userAgent: navigator.userAgent,
      url: window.location.href,
      appVersion: '1.0.0',
      workerVersion: workerState.version || 'unknown',
      batchSize: diagnosticState.statistics.batchSize,
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
        title: "Diagnostica copiata",
        description: "Bundle diagnostico copiato negli appunti",
      });
    }).catch(() => {
      toast({
        title: "Errore copia",
        description: "Impossibile copiare negli appunti",
        variant: "destructive"
      });
    });
  }, [diagnosticState, debugEvents, workerState.version, toast]);

  // Button disabled state
  const isButtonDisabled = processingState !== 'idle';

  return (
    <div className="container mx-auto p-6 space-y-8">
      <header className="text-center space-y-4">
        <h1 className="text-4xl font-bold text-foreground">
          Alterside Catalog Generator - Hardened Worker
        </h1>
        <p className="text-lg text-muted">
          Test hardened worker lifecycle con protocollo rigido
        </p>
      </header>

      {/* Processing State Display */}
      <div className="card border-strong">
        <div className="card-body">
          <h3 className="card-title mb-4">Stato Processamento</h3>
          <div className="flex items-center gap-4">
            <div className="text-lg font-medium">
              Stato: <span className="text-primary">{processingState}</span>
            </div>
            {processingState === 'prescanning' && (
              <div className="flex items-center gap-2">
                <div className="text-sm">Progresso: {progressPct}%</div>
                <div className="w-32 h-2 bg-muted rounded-full">
                  <div 
                    className="h-full bg-primary rounded-full transition-all"
                    style={{ width: `${progressPct}%` }}
                  />
                </div>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Diagnostic Toggle */}
      <div className="card border-strong">
        <div className="card-body">
          <h3 className="card-title mb-4">Modalità Diagnostica</h3>
          
          <div className="flex items-center gap-4 mb-4">
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
              }}
              className="w-4 h-4"
            />
          </div>
          
          <div className="space-y-3">
            <div className="flex gap-3">
              <button
                onClick={handleDiagnosticClick}
                disabled={isButtonDisabled}
                className={`btn btn-primary ${isButtonDisabled ? 'opacity-50 cursor-not-allowed' : ''}`}
              >
                <Activity className={`mr-2 h-4 w-4 ${processingState === 'creating' || processingState === 'prescanning' ? 'animate-spin' : ''}`} />
                {processingState === 'creating' ? 'Creando...' : 
                 processingState === 'prescanning' ? 'Prescanning...' :
                 'Test Worker Hardened'}
              </button>
              
              <button
                onClick={generateDiagnosticBundle}
                className="btn btn-secondary"
              >
                Copia diagnostica
              </button>
            </div>
            
            {echoTestResult && (
              <div className="p-3 bg-muted rounded border">
                <strong>Test Result:</strong> {echoTestResult}
              </div>
            )}
          </div>
        </div>
      </div>

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

      {/* Debug Events Panel */}
      <div className="card border-strong">
        <div className="card-body">
          <h3 className="card-title mb-4 flex items-center gap-2">
            <AlertCircle className="h-5 w-5 icon-dark" />
            Eventi Debug - Telemetria
          </h3>
          
          <div className="mb-4 p-3 bg-blue-50 border border-blue-200 rounded-lg">
            <h4 className="text-sm font-medium mb-2">Sequenza Attesa</h4>
            <div className="text-xs font-mono">
              click_diag → worker_created → worker_boot → init_sent → worker_ready → prescan_start → prescan_progress(0/total) → prescan_done
            </div>
          </div>
          
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

      {/* Diagnostic Statistics Panel */}
      <div className="card border-strong">
        <div className="card-body">
          <h3 className="card-title mb-4 flex items-center gap-2">
            <Activity className="h-5 w-5 icon-dark" />
            Statistiche Diagnostiche
          </h3>
          
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
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
              <div className="font-medium">Messaggi Worker</div>
              <div className="text-lg">{diagnosticState.workerMessages.length}</div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default AltersideCatalogGenerator;