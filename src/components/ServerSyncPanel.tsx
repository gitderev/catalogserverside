import React, { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { Progress } from '@/components/ui/progress';
import { 
  Activity, 
  Download, 
  CheckCircle, 
  XCircle, 
  Cloud, 
  FileText,
  Clock,
  AlertTriangle,
  Copy,
  ChevronDown,
  ChevronUp,
  Bug
} from 'lucide-react';
import { useSyncProgress, EXPORT_FILES, ALL_PIPELINE_STEPS, type SyncRunState } from '@/hooks/useSyncProgress';
import { toast } from '@/hooks/use-toast';

interface ServerSyncPanelProps {
  disabled?: boolean;
  onSyncStarted?: () => void;
  onSyncComplete?: () => void;
}

export const ServerSyncPanel: React.FC<ServerSyncPanelProps> = ({
  disabled = false,
  onSyncStarted,
  onSyncComplete,
}) => {
  const {
    state,
    isRunning,
    isComplete,
    isFailed,
    startSync,
    downloadExport,
    reset,
    getProgress,
    getStepDisplayName,
  } = useSyncProgress();

  const [debugExpanded, setDebugExpanded] = useState(false);

  const handleStartSync = async () => {
    onSyncStarted?.();
    const success = await startSync();
    if (success) {
      // Wait for completion via polling
    }
  };

  React.useEffect(() => {
    if (isComplete) {
      onSyncComplete?.();
    }
  }, [isComplete, onSyncComplete]);

  const progress = getProgress();
  const stepName = getStepDisplayName(state.currentStep);

  // Get step status with fallback to pending
  const getStepStatus = (stepKey: string): string => {
    const stepData = state.steps[stepKey];
    if (!stepData) return 'pending';
    return stepData.status || 'pending';
  };

  // Get step icon based on status
  const getStepIcon = (status: string) => {
    switch (status) {
      case 'completed':
      case 'success':
        return <CheckCircle className="h-3 w-3 text-emerald-600" />;
      case 'failed':
        return <XCircle className="h-3 w-3 text-rose-600" />;
      case 'in_progress':
      case 'running':
      case 'building_stock_index':
      case 'building_price_index':
      case 'preparing_material':
        return <Activity className="h-3 w-3 text-blue-600 animate-spin" />;
      default:
        return <Clock className="h-3 w-3 text-slate-400" />;
    }
  };

  // Copy debug info to clipboard
  const copyDebugInfo = () => {
    const debugData = {
      runId: state.runId,
      status: state.status,
      currentStep: state.currentStep,
      errorMessage: state.errorMessage,
      errorDetails: state.errorDetails,
      steps: state.steps,
      exportFiles: state.exportFiles,
      locationWarnings: state.locationWarnings,
      runtimeMs: state.runtimeMs,
    };
    navigator.clipboard.writeText(JSON.stringify(debugData, null, 2));
    toast({ title: 'Debug copiato', description: 'Informazioni debug copiate negli appunti' });
  };

  // Copy last error to clipboard
  const copyLastError = () => {
    const errorData = {
      errorMessage: state.errorMessage,
      errorDetails: state.errorDetails,
      currentStep: state.currentStep,
      stepError: state.steps[state.currentStep]?.error,
    };
    navigator.clipboard.writeText(JSON.stringify(errorData, null, 2));
    toast({ title: 'Errore copiato', description: 'Dettagli errore copiati negli appunti' });
  };

  return (
    <Card className="p-6 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-lg shadow-sm">
      <div className="space-y-4">
        {/* Header */}
        <div className="flex items-center justify-between">
          <h3 className="text-lg font-semibold flex items-center gap-2 text-slate-900 dark:text-slate-100">
            <Cloud className="h-5 w-5 text-blue-600 dark:text-blue-400" />
            Pipeline Server-Side
          </h3>
          {state.runId && (
            <span className="text-xs text-slate-500 dark:text-slate-400 font-mono">
              Run: {state.runId.substring(0, 8)}...
            </span>
          )}
        </div>

        <p className="text-sm text-slate-600 dark:text-slate-400">
          Genera gli export (Catalogo EAN, ePrice, Mediaworld) sul server e caricali su SFTP.
          I file scaricati saranno identici a quelli inviati via SFTP.
        </p>

        {/* Progress Display */}
        {isRunning && (
          <div className="space-y-3 p-4 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg">
            <div className="flex items-center gap-2">
              <Activity className="h-4 w-4 animate-spin text-blue-600 dark:text-blue-400" />
              <span className="font-medium text-slate-900 dark:text-slate-100">In esecuzione: {stepName}</span>
            </div>
            <Progress value={progress} className="h-2" />
            <div className="flex justify-between text-xs text-slate-500 dark:text-slate-400">
              <span>Progresso: {progress}%</span>
              {state.startedAt && (
                <span className="flex items-center gap-1">
                  <Clock className="h-3 w-3" />
                  Avviato: {state.startedAt.toLocaleTimeString('it-IT')}
                </span>
              )}
            </div>
          </div>
        )}

        {isComplete && (
          <div className="p-4 bg-emerald-50 dark:bg-emerald-950/30 border border-emerald-200 dark:border-emerald-800 rounded-lg">
            <div className="flex items-center gap-2 text-emerald-800 dark:text-emerald-300 mb-2">
              <CheckCircle className="h-5 w-5" />
              <span className="font-semibold">Pipeline completata con successo</span>
            </div>
            {state.runtimeMs && (
              <p className="text-sm text-emerald-700 dark:text-emerald-400">
                Tempo di esecuzione: {Math.round(state.runtimeMs / 1000)}s
              </p>
            )}
          </div>
        )}

        {isFailed && (
          <div className="p-4 bg-rose-50 dark:bg-rose-950/30 border border-rose-200 dark:border-rose-800 rounded-lg">
            <div className="flex items-center gap-2 text-rose-800 dark:text-rose-300 mb-2">
              <XCircle className="h-5 w-5" />
              <span className="font-semibold">Pipeline fallita</span>
              {state.currentStep && (
                <span className="text-xs font-mono bg-rose-100 dark:bg-rose-900 px-2 py-0.5 rounded">
                  step: {state.currentStep}
                </span>
              )}
            </div>
            {/* Show step-specific error if available */}
            {state.steps[state.currentStep]?.error ? (
              <div className="space-y-2">
                <p className="text-sm text-rose-700 dark:text-rose-400 break-words font-medium">
                  Errore in {state.currentStep}:
                </p>
                <pre className="text-xs text-rose-600 dark:text-rose-400 bg-rose-100 dark:bg-rose-900/50 p-2 rounded overflow-x-auto whitespace-pre-wrap break-all">
                  {state.steps[state.currentStep].error}
                </pre>
              </div>
            ) : state.errorMessage ? (
              <p className="text-sm text-rose-700 dark:text-rose-400 break-words">
                {state.errorMessage}
              </p>
            ) : (
              <p className="text-sm text-rose-700 dark:text-rose-400">
                Errore sconosciuto. Controlla i log del server.
              </p>
            )}
            <Button
              variant="outline"
              size="sm"
              onClick={reset}
              className="mt-3 border-slate-300 dark:border-slate-600 text-slate-900 dark:text-slate-100"
            >
              Riprova
            </Button>
          </div>
        )}

        {/* Location Warnings */}
        {Object.keys(state.locationWarnings).length > 0 && Object.values(state.locationWarnings).some(v => v > 0) && (
          <div className="p-3 bg-amber-50 dark:bg-amber-950/20 border border-amber-200 dark:border-amber-800 rounded-lg">
            <div className="flex items-center gap-2 text-amber-700 dark:text-amber-400 mb-2">
              <AlertTriangle className="h-4 w-4" />
              <span className="font-medium text-sm">Warning Stock Location</span>
            </div>
            <ul className="text-xs text-amber-600 dark:text-amber-500 space-y-1">
              {Object.entries(state.locationWarnings).map(([key, count]) => (
                count > 0 && (
                  <li key={key}>
                    {key.replace(/_/g, ' ')}: {count}
                  </li>
                )
              ))}
            </ul>
          </div>
        )}

        {/* Action Buttons */}
        <div className="flex flex-wrap gap-3">
          <Button
            onClick={handleStartSync}
            disabled={disabled || isRunning}
            className="flex-1 min-w-[200px] bg-blue-600 hover:bg-blue-700 text-white"
            size="lg"
          >
            {isRunning ? (
              <>
                <Activity className="mr-2 h-5 w-5 animate-spin" />
                Pipeline in corso...
              </>
            ) : (
              <>
                <Cloud className="mr-2 h-5 w-5" />
                AVVIA PIPELINE SERVER
              </>
            )}
          </Button>
        </div>

        {/* Download Buttons - Only shown after success */}
        {isComplete && (
          <div className="border-t pt-4 mt-4">
            <h4 className="text-sm font-medium mb-3 flex items-center gap-2">
              <Download className="h-4 w-4" />
              Scarica Export (file server-side)
            </h4>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-2">
              {Object.entries(EXPORT_FILES).map(([key, file]) => (
                <Button
                  key={key}
                  variant="outline"
                  size="sm"
                  onClick={() => downloadExport(key as keyof typeof EXPORT_FILES)}
                  disabled={isRunning}
                  className="justify-start bg-white dark:bg-slate-800 border-slate-300 dark:border-slate-600 text-slate-900 dark:text-slate-100 hover:bg-slate-50 dark:hover:bg-slate-700"
                >
                  <FileText className="mr-2 h-4 w-4" />
                  {file.displayName}
                </Button>
              ))}
            </div>
            <p className="text-xs text-muted-foreground mt-2">
              I file scaricati sono identici a quelli caricati su SFTP dalla pipeline server-side.
            </p>
          </div>
        )}

        {/* ALL Steps - Always visible */}
        <div className="border-t border-slate-200 dark:border-slate-700 pt-4 mt-4">
          <h4 className="text-sm font-medium mb-3 text-slate-900 dark:text-slate-100">
            Stato Step Pipeline
          </h4>
          <div className="space-y-1">
            {ALL_PIPELINE_STEPS.map((stepKey) => {
              const status = getStepStatus(stepKey);
              const stepData = state.steps[stepKey];
              const isCurrent = state.currentStep === stepKey;
              
              return (
                <div 
                  key={stepKey} 
                  className={`flex items-center gap-2 p-2 rounded text-xs ${
                    isCurrent 
                      ? 'bg-blue-50 dark:bg-blue-950/30 border border-blue-200 dark:border-blue-800' 
                      : 'bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700'
                  }`}
                >
                  {getStepIcon(status)}
                  <span className="font-mono text-slate-900 dark:text-slate-100 flex-1">
                    {getStepDisplayName(stepKey)}
                  </span>
                  <span className={`text-xs px-2 py-0.5 rounded ${
                    status === 'completed' || status === 'success' 
                      ? 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/50 dark:text-emerald-400'
                      : status === 'failed' 
                        ? 'bg-rose-100 text-rose-700 dark:bg-rose-900/50 dark:text-rose-400'
                        : status === 'pending' 
                          ? 'bg-slate-100 text-slate-500 dark:bg-slate-700 dark:text-slate-400'
                          : 'bg-blue-100 text-blue-700 dark:bg-blue-900/50 dark:text-blue-400'
                  }`}>
                    {status}
                  </span>
                  {stepData?.rows !== undefined && (
                    <span className="text-slate-500 dark:text-slate-400">
                      ({stepData.rows} righe)
                    </span>
                  )}
                  {stepData?.duration_ms !== undefined && (
                    <span className="text-slate-500 dark:text-slate-400">
                      {stepData.duration_ms}ms
                    </span>
                  )}
                </div>
              );
            })}
          </div>
        </div>

        {/* Debug Panel - Always visible when run exists */}
        {state.runId && (
          <div className="border-t border-slate-200 dark:border-slate-700 pt-4 mt-4">
            <button
              onClick={() => setDebugExpanded(!debugExpanded)}
              className="flex items-center gap-2 text-sm font-medium text-slate-700 dark:text-slate-300 hover:text-slate-900 dark:hover:text-slate-100 w-full"
            >
              <Bug className="h-4 w-4" />
              Debug Info
              {debugExpanded ? <ChevronUp className="h-4 w-4 ml-auto" /> : <ChevronDown className="h-4 w-4 ml-auto" />}
            </button>
            
            {debugExpanded && (
              <div className="mt-3 space-y-3">
                {/* Quick actions */}
                <div className="flex gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={copyDebugInfo}
                    className="text-xs"
                  >
                    <Copy className="h-3 w-3 mr-1" />
                    Copia debug
                  </Button>
                  {(state.errorMessage || state.errorDetails) && (
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={copyLastError}
                      className="text-xs text-rose-600 border-rose-300 hover:bg-rose-50"
                    >
                      <Copy className="h-3 w-3 mr-1" />
                      Copia ultimo errore
                    </Button>
                  )}
                </div>

                {/* Debug fields */}
                <div className="space-y-2 text-xs font-mono bg-slate-100 dark:bg-slate-800 p-3 rounded-lg overflow-x-auto">
                  <div className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1">
                    <span className="text-slate-500">runId:</span>
                    <span className="text-slate-900 dark:text-slate-100">{state.runId}</span>
                    
                    <span className="text-slate-500">status:</span>
                    <span className="text-slate-900 dark:text-slate-100">{state.status}</span>
                    
                    <span className="text-slate-500">current_step:</span>
                    <span className="text-slate-900 dark:text-slate-100">{state.currentStep || 'N/A'}</span>
                    
                    <span className="text-slate-500">runtime_ms:</span>
                    <span className="text-slate-900 dark:text-slate-100">{state.runtimeMs ?? 'N/A'}</span>
                  </div>

                  {state.errorMessage && (
                    <div className="mt-2 pt-2 border-t border-slate-300 dark:border-slate-600">
                      <span className="text-rose-600 dark:text-rose-400 font-semibold">error_message:</span>
                      <pre className="mt-1 text-rose-700 dark:text-rose-300 whitespace-pre-wrap break-all">
                        {state.errorMessage}
                      </pre>
                    </div>
                  )}

                  {state.errorDetails && (
                    <div className="mt-2 pt-2 border-t border-slate-300 dark:border-slate-600">
                      <span className="text-rose-600 dark:text-rose-400 font-semibold">error_details:</span>
                      <pre className="mt-1 text-rose-700 dark:text-rose-300 whitespace-pre-wrap break-all max-h-40 overflow-y-auto">
                        {JSON.stringify(state.errorDetails, null, 2)}
                      </pre>
                    </div>
                  )}

                  {state.exportFiles && Object.keys(state.exportFiles).length > 0 && (
                    <div className="mt-2 pt-2 border-t border-slate-300 dark:border-slate-600">
                      <span className="text-emerald-600 dark:text-emerald-400 font-semibold">exports.files:</span>
                      <pre className="mt-1 text-slate-700 dark:text-slate-300 whitespace-pre-wrap">
                        {JSON.stringify(state.exportFiles, null, 2)}
                      </pre>
                    </div>
                  )}

                  <details className="mt-2 pt-2 border-t border-slate-300 dark:border-slate-600">
                    <summary className="cursor-pointer text-slate-500 hover:text-slate-700 dark:hover:text-slate-300">
                      steps (raw JSON)
                    </summary>
                    <pre className="mt-1 text-slate-700 dark:text-slate-300 whitespace-pre-wrap break-all max-h-60 overflow-y-auto">
                      {JSON.stringify(state.steps, null, 2)}
                    </pre>
                  </details>
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </Card>
  );
};

export default ServerSyncPanel;
