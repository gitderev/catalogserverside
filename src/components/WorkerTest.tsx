import React, { useState, useRef, useCallback, useEffect } from 'react';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { toast } from '@/hooks/use-toast';
import { Activity, CheckCircle, XCircle, AlertCircle } from 'lucide-react';

// Processing states
type ProcessingState = 'idle' | 'creating' | 'prescanning' | 'running' | 'done' | 'error';

const WorkerTest = () => {
  const [processingState, setProcessingState] = useState<ProcessingState>('idle');
  const [progressPct, setProgressPct] = useState(0);
  const [lastClickTime, setLastClickTime] = useState(0);
  
  // Worker refs
  const workerRef = useRef<Worker | null>(null);
  const blobUrlRef = useRef<string | null>(null);
  const progressTimerRef = useRef<NodeJS.Timeout | null>(null);
  const pingTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  
  const [diagnosticState, setDiagnosticState] = useState({
    isEnabled: true,
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

  // Echo test for worker environment
  const runEchoTest = useCallback(async (strategy: 'blob' | 'module' = 'blob') => {
    dbg('echo_test_start', { strategy });
    setEchoTestResult('Testing...');
    
    try {
      let worker: Worker;
      
      if (strategy === 'blob') {
        // Create minimal echo worker as blob
        const echoWorkerCode = `
          // Echo worker boot
          postMessage({type:'worker_boot', version:'echo-1.0.0'});
          
          // Echo worker message handler
          onmessage = function(e) {
            const { type, ...data } = e.data;
            
            switch(type) {
              case 'ping':
                postMessage({type:'pong', timestamp: Date.now()});
                break;
              case 'crash':
                throw new Error('Intentional crash for testing');
              default:
                postMessage({type:'echo', original: e.data});
            }
          };
        `;
        
        const blob = new Blob([echoWorkerCode], { type: 'application/javascript' });
        const blobUrl = URL.createObjectURL(blob);
        worker = new Worker(blobUrl);
        
        setTimeout(() => URL.revokeObjectURL(blobUrl), 5000);
      } else {
        // Use module worker from public folder
        worker = new Worker('/workers/alterside-sku-worker.js', { type: 'module' });
      }
      
      let bootReceived = false;
      let pongReceived = false;
      
      worker.onmessage = (e) => {
        const { type } = e.data;
        
        if (type === 'worker_boot') {
          dbg('echo_boot', e.data);
          bootReceived = true;
        } else if (type === 'pong') {
          dbg('echo_pong', e.data);
          pongReceived = true;
        }
      };
      
      worker.onerror = (error) => {
        dbg('echo_error', { message: error.message });
      };
      
      // Send ping after short delay
      setTimeout(() => {
        worker.postMessage({ type: 'ping' });
      }, 100);
      
      // Check results after 2 seconds
      setTimeout(() => {
        worker.terminate();
        
        if (bootReceived && pongReceived) {
          setEchoTestResult(`✅ ${strategy.toUpperCase()} OK - echo_boot, echo_pong`);
          dbg('echo_test_success', { strategy, bootReceived, pongReceived });
        } else {
          setEchoTestResult(`❌ ${strategy.toUpperCase()} FAIL - boot:${bootReceived}, pong:${pongReceived}`);
          dbg('echo_test_failed', { strategy, bootReceived, pongReceived });
          
          // If blob failed, try module
          if (strategy === 'blob') {
            setTimeout(() => runEchoTest('module'), 1000);
          }
        }
      }, 2000);
      
    } catch (error) {
      setEchoTestResult(`❌ ${strategy.toUpperCase()} ERROR - ${error.message}`);
      dbg('echo_test_error', { strategy, error: error.message });
      
      // If blob failed, try module
      if (strategy === 'blob') {
        setTimeout(() => runEchoTest('module'), 1000);
      }
    }
  }, [dbg]);

  // Debounced click handler
  const handleDiagnosticClick = useCallback(() => {
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
    dbg('click_diag');
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
  }, [lastClickTime, processingState, dbg, workerStrategy]);

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
            setProcessingState('done');
            setProgressPct(100);
            
            toast({
              title: "Test completato",
              description: "Worker test eseguito con successo",
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
        diag: true,
        sampleSize: 200,
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
  }, [dbg, addWorkerMessage]);

  const isProcessing = processingState === 'creating' || processingState === 'prescanning' || processingState === 'running';

  return (
    <div className="min-h-screen bg-background">
      <div className="container mx-auto p-6">
        <div className="mb-8">
          <h1 className="text-3xl font-bold mb-2">Worker Test - Hardened</h1>
          <p className="text-muted-foreground">
            Test diagnostico per worker environment e lifecycle
          </p>
        </div>

        {/* Test Controls */}
        <Card className="mb-6 p-6">
          <div className="space-y-4">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <Button
                onClick={() => runEchoTest('blob')}
                variant="outline"
                className="h-12"
              >
                <Activity className="mr-2 h-4 w-4" />
                Echo Worker Test (Blob)
              </Button>

              <Button
                onClick={() => runEchoTest('module')}
                variant="outline"
                className="h-12"
              >
                <Activity className="mr-2 h-4 w-4" />
                Echo Worker Test (Module)
              </Button>
            </div>

            <Button
              onClick={handleDiagnosticClick}
              disabled={isProcessing}
              className="w-full h-12"
            >
              <Activity className="mr-2 h-4 w-4" />
              Hardened Worker Test
            </Button>

            {/* Echo Test Result */}
            {echoTestResult && (
              <div className="p-3 bg-muted rounded">
                <div className="font-medium">Echo Test Result</div>
                <div className="text-lg font-mono">{echoTestResult}</div>
              </div>
            )}

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
                  {processingState === 'running' && 'Test in corso...'}
                </div>
              </div>
            )}
          </div>
        </Card>

        {/* Worker State */}
        <Card className="mb-6 p-6">
          <h3 className="text-lg font-semibold mb-4">Worker State</h3>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
            <div className="flex items-center space-x-2">
              {workerState.created ? <CheckCircle className="h-4 w-4 text-green-500" /> : <XCircle className="h-4 w-4 text-red-500" />}
              <span className="text-sm">Created</span>
            </div>
            <div className="flex items-center space-x-2">
              {workerState.handlersAttached ? <CheckCircle className="h-4 w-4 text-green-500" /> : <XCircle className="h-4 w-4 text-red-500" />}
              <span className="text-sm">Handlers</span>
            </div>
            <div className="flex items-center space-x-2">
              {workerState.initSent ? <CheckCircle className="h-4 w-4 text-green-500" /> : <XCircle className="h-4 w-4 text-red-500" />}
              <span className="text-sm">INIT Sent</span>
            </div>
            <div className="flex items-center space-x-2">
              {workerState.bootReceived ? <CheckCircle className="h-4 w-4 text-green-500" /> : <XCircle className="h-4 w-4 text-red-500" />}
              <span className="text-sm">Boot Received</span>
            </div>
            <div className="flex items-center space-x-2">
              {workerState.readyReceived ? <CheckCircle className="h-4 w-4 text-green-500" /> : <XCircle className="h-4 w-4 text-red-500" />}
              <span className="text-sm">Ready Received</span>
            </div>
            <div className="flex items-center space-x-2">
              <AlertCircle className="h-4 w-4 text-blue-500" />
              <span className="text-sm">Version: {workerState.version || 'N/A'}</span>
            </div>
          </div>
        </Card>

        {/* Diagnostic Information */}
        <Card className="p-6">
          <h3 className="text-lg font-semibold mb-4">Informazioni Diagnostiche</h3>
          
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-4">
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

          {/* Debug Events */}
          <div className="mb-4">
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

          {/* Worker Messages */}
          <div>
            <h4 className="font-medium mb-2">Messaggi Worker</h4>
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
          </div>
        </Card>
      </div>
    </div>
  );
};

export default WorkerTest;