import { useState, useCallback, useRef, useEffect } from 'react';
import { supabase } from '@/integrations/supabase/client';
import { toast } from '@/hooks/use-toast';

export type SyncStatus = 'idle' | 'starting' | 'running' | 'success' | 'failed' | 'cancelled' | 'waiting_retry';

export interface SyncStep {
  status: string;
  error?: string;
  details?: Record<string, any>;
  rows?: number;
  duration_ms?: number;
  // parse_merge specific fields
  phase?: string;
  offsetBytes?: number;
  productCount?: number;
  chunkIndex?: number;
  retry_count?: number;
  next_retry_at?: string;
  last_error?: string;
  productsPath?: string;
  finalize?: {
    segmentIndex: number;
    nextChunkToPack: number;
    totalChunks: number;
  };
}

export interface ExportFiles {
  ean?: string;
  eprice?: string;
  mediaworld?: string;
}

export interface SyncRunState {
  runId: string | null;
  status: SyncStatus;
  currentStep: string;
  steps: Record<string, SyncStep>;
  errorMessage: string | null;
  errorDetails: Record<string, any> | null;
  runtimeMs: number | null;
  locationWarnings: Record<string, number>;
  startedAt: Date | null;
  finishedAt: Date | null;
  exportFiles: ExportFiles;
}

const INITIAL_STATE: SyncRunState = {
  runId: null,
  status: 'idle',
  currentStep: '',
  steps: {},
  errorMessage: null,
  errorDetails: null,
  runtimeMs: null,
  locationWarnings: {},
  startedAt: null,
  finishedAt: null,
  exportFiles: {},
};

const POLL_INTERVAL_RUNNING_MS = 2500;
const POLL_INTERVAL_IDLE_MS = 12000;

export const ALL_PIPELINE_STEPS = ['import_ftp', 'parse_merge', 'ean_mapping', 'pricing', 'export_ean', 'export_mediaworld', 'export_eprice', 'upload_sftp'];

export const EXPORT_FILES = {
  ean: { path: 'Catalogo EAN.xlsx', displayName: 'Catalogo EAN' },
  eprice: { path: 'Export ePrice.xlsx', displayName: 'Export ePrice' },
  mediaworld: { path: 'Export Mediaworld.xlsx', displayName: 'Export Mediaworld' },
};

export function useSyncProgress() {
  const [state, setState] = useState<SyncRunState>(INITIAL_STATE);
  const [isPolling, setIsPolling] = useState(false);
  const pollIntervalRef = useRef<NodeJS.Timeout | null>(null);

  useEffect(() => {
    return () => {
      if (pollIntervalRef.current) {
        clearInterval(pollIntervalRef.current);
        pollIntervalRef.current = null;
      }
    };
  }, []);

  const pollStatus = useCallback(async (runId: string) => {
    try {
      const { data, error } = await supabase
        .from('sync_runs')
        .select('status, steps, error_message, error_details, runtime_ms, location_warnings, started_at, finished_at')
        .eq('id', runId)
        .single();

      if (error || !data) return;

      const steps = (data.steps || {}) as Record<string, any>;
      const currentStep = steps.current_step || '';
      const exportFiles: ExportFiles = steps.exports?.files || {};
      const isWaitingRetry = steps[currentStep]?.phase === 'waiting_retry';

      setState(prev => ({
        ...prev,
        status: isWaitingRetry ? 'waiting_retry' : data.status as SyncStatus,
        currentStep,
        steps: steps as Record<string, SyncStep>,
        errorMessage: data.error_message,
        errorDetails: (data.error_details || null) as Record<string, any> | null,
        runtimeMs: data.runtime_ms,
        locationWarnings: (data.location_warnings || {}) as Record<string, number>,
        finishedAt: data.finished_at ? new Date(data.finished_at) : null,
        exportFiles,
      }));

      if (['success', 'failed', 'cancelled'].includes(data.status)) {
        if (pollIntervalRef.current) {
          clearInterval(pollIntervalRef.current);
          pollIntervalRef.current = null;
        }
        setIsPolling(false);

        if (data.status === 'success') {
          toast({ title: 'Pipeline completata', description: `Export server-side completato in ${Math.round((data.runtime_ms || 0) / 1000)}s` });
        } else if (data.status === 'failed') {
          toast({ title: 'Pipeline fallita', description: data.error_message || 'Errore sconosciuto', variant: 'destructive' });
        }
      }
    } catch (err) {
      console.error('[useSyncProgress] Poll exception:', err);
    }
  }, []);

  const startPolling = useCallback((runId: string, isWaiting: boolean = false) => {
    if (pollIntervalRef.current) clearInterval(pollIntervalRef.current);
    const interval = isWaiting ? POLL_INTERVAL_IDLE_MS : POLL_INTERVAL_RUNNING_MS;
    pollIntervalRef.current = setInterval(() => pollStatus(runId), interval);
    setIsPolling(true);
    pollStatus(runId);
  }, [pollStatus]);

  const startSync = useCallback(async (): Promise<boolean> => {
    if (state.status === 'running' || state.status === 'starting') {
      toast({ title: 'Sync già in corso', description: 'Attendere il completamento della pipeline corrente' });
      return false;
    }

    setState(prev => ({ ...prev, status: 'starting', errorMessage: null }));

    try {
      const { data, error } = await supabase.functions.invoke('run-full-sync', { body: { trigger: 'manual' } });

      if (error) {
        setState(prev => ({ ...prev, status: 'failed', errorMessage: error.message || 'Errore chiamata run-full-sync' }));
        toast({ title: 'Errore avvio pipeline', description: error.message, variant: 'destructive' });
        return false;
      }

      if (data?.status === 'busy' && data?.run_id) {
        setState(prev => ({ ...prev, runId: data.run_id, status: 'running', startedAt: new Date() }));
        startPolling(data.run_id, false);
        return true;
      }

      if (data?.status === 'waiting_retry' && data?.run_id) {
        setState(prev => ({ ...prev, runId: data.run_id, status: 'waiting_retry' }));
        startPolling(data.run_id, true);
        toast({ title: 'Pipeline in attesa', description: `Retry schedulato` });
        return true;
      }

      if (data?.status === 'error') {
        setState(prev => ({ ...prev, status: 'failed', errorMessage: data.message || data.error }));
        toast({ title: 'Errore avvio pipeline', description: data.message || data.error, variant: 'destructive' });
        return false;
      }

      const runId = data?.run_id || data?.runId || data?.id;
      if (!runId) {
        if (data?.status === 'success') {
          setState(prev => ({ ...prev, status: 'success', errorMessage: null }));
          toast({ title: 'Pipeline completata', description: 'Export generati con successo' });
          return true;
        }
        setState(prev => ({ ...prev, status: 'failed', errorMessage: 'Risposta run-full-sync non valida' }));
        return false;
      }

      setState(prev => ({
        ...prev, runId, status: 'running', currentStep: 'import_ftp', steps: {}, errorMessage: null, startedAt: new Date(), finishedAt: null
      }));
      startPolling(runId, false);
      toast({ title: 'Pipeline avviata', description: 'Generazione export server-side in corso...' });
      return true;
    } catch (err) {
      setState(prev => ({ ...prev, status: 'failed', errorMessage: err instanceof Error ? err.message : 'Errore sconosciuto' }));
      toast({ title: 'Errore', description: err instanceof Error ? err.message : 'Errore sconosciuto', variant: 'destructive' });
      return false;
    }
  }, [state.status, startPolling]);

  const downloadExport = useCallback(async (fileKey: keyof typeof EXPORT_FILES): Promise<void> => {
    const fileInfo = EXPORT_FILES[fileKey];
    const exportFilePath = state.exportFiles[fileKey];
    const downloadPath = exportFilePath ? exportFilePath.replace(/^exports\//, '') : fileInfo.path;
    
    try {
      const { data, error } = await supabase.storage.from('exports').download(downloadPath);

      if (error || !data) {
        toast({ title: 'Errore download', description: `Impossibile scaricare ${fileInfo.displayName}: ${error?.message || 'File non trovato'}`, variant: 'destructive' });
        return;
      }

      const url = URL.createObjectURL(data);
      const a = document.createElement('a');
      a.href = url;
      a.download = downloadPath.split('/').pop() || fileInfo.path;
      a.style.display = 'none';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      setTimeout(() => URL.revokeObjectURL(url), 3000);

      toast({ title: 'Download completato', description: `${fileInfo.displayName} scaricato` });
    } catch (err) {
      toast({ title: 'Errore download', description: err instanceof Error ? err.message : 'Errore sconosciuto', variant: 'destructive' });
    }
  }, [state.exportFiles]);

  const reset = useCallback(() => {
    if (pollIntervalRef.current) { clearInterval(pollIntervalRef.current); pollIntervalRef.current = null; }
    setIsPolling(false);
    setState(INITIAL_STATE);
  }, []);

  const getProgress = useCallback((): number => {
    const currentIdx = ALL_PIPELINE_STEPS.indexOf(state.currentStep);
    if (currentIdx < 0) return 0;
    if (state.status === 'success') return 100;
    
    if (state.currentStep === 'parse_merge' && state.steps.parse_merge) {
      const pm = state.steps.parse_merge;
      const baseProgress = (currentIdx / ALL_PIPELINE_STEPS.length) * 100;
      const stepProgress = 100 / ALL_PIPELINE_STEPS.length;
      
      // Progress based on phase
      if (pm.phase === 'preparing_material' || pm.phase === 'material_ready') {
        return Math.round(baseProgress + stepProgress * 0.1);
      }
      if (pm.phase === 'processing_chunk' && pm.chunkIndex !== undefined) {
        // Estimate based on chunk count (assume ~100 chunks for large files)
        const fraction = Math.min((pm.chunkIndex || 0) / 100, 0.85);
        return Math.round(baseProgress + stepProgress * (0.1 + 0.8 * fraction));
      }
      if (pm.phase === 'finalizing') {
        return Math.round(baseProgress + stepProgress * 0.95);
      }
    }
    
    return Math.round(((currentIdx + 1) / ALL_PIPELINE_STEPS.length) * 100);
  }, [state.currentStep, state.status, state.steps]);

  const getStepDisplayName = useCallback((step: string): string => {
    const names: Record<string, string> = {
      'import_ftp': 'Import da FTP',
      'parse_merge': 'Parsing e merge',
      'ean_mapping': 'Mapping EAN',
      'pricing': 'Calcolo prezzi',
      'export_ean': 'Export Catalogo EAN',
      'export_mediaworld': 'Export Mediaworld',
      'export_eprice': 'Export ePrice',
      'upload_sftp': 'Upload SFTP',
    };
    return names[step] || step;
  }, []);

  return {
    state,
    isPolling,
    startSync,
    downloadExport,
    reset,
    getProgress,
    getStepDisplayName,
    isRunning: state.status === 'running' || state.status === 'starting' || state.status === 'waiting_retry',
    isComplete: state.status === 'success',
    isFailed: state.status === 'failed',
    isWaitingRetry: state.status === 'waiting_retry',
  };
}
