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
  offset?: number;
  productCount?: number;
  chunkIndex?: number;
  retry_count?: number;
  next_retry_at?: string;
  last_error?: string;
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

// Adaptive polling intervals
const POLL_INTERVAL_RUNNING_MS = 2500; // 2.5s while running
const POLL_INTERVAL_IDLE_MS = 12000;   // 12s when idle/waiting

// All pipeline steps in order for UI display
export const ALL_PIPELINE_STEPS = ['import_ftp', 'parse_merge', 'ean_mapping', 'pricing', 'export_ean', 'export_mediaworld', 'export_eprice', 'upload_sftp'];

// Server-generated export files - paths retrieved from sync_runs.steps.exports.files
export const EXPORT_FILES = {
  ean: { path: 'Catalogo EAN.xlsx', displayName: 'Catalogo EAN' },
  eprice: { path: 'Export ePrice.xlsx', displayName: 'Export ePrice' },
  mediaworld: { path: 'Export Mediaworld.xlsx', displayName: 'Export Mediaworld' },
};

export function useSyncProgress() {
  const [state, setState] = useState<SyncRunState>(INITIAL_STATE);
  const [isPolling, setIsPolling] = useState(false);
  const pollIntervalRef = useRef<NodeJS.Timeout | null>(null);
  const lastPollTimeRef = useRef<number>(0);

  // Cleanup polling on unmount
  useEffect(() => {
    return () => {
      if (pollIntervalRef.current) {
        clearInterval(pollIntervalRef.current);
        pollIntervalRef.current = null;
      }
    };
  }, []);

  // Poll sync_runs for status updates
  const pollStatus = useCallback(async (runId: string) => {
    try {
      const { data, error } = await supabase
        .from('sync_runs')
        .select('status, steps, error_message, error_details, runtime_ms, location_warnings, started_at, finished_at')
        .eq('id', runId)
        .single();

      if (error || !data) {
        console.error('[useSyncProgress] Poll error:', error);
        return;
      }

      const steps = (data.steps || {}) as Record<string, any>;
      const currentStep = steps.current_step || '';
      
      // Extract export file paths from steps.exports.files
      const exportFiles: ExportFiles = steps.exports?.files || {};

      // Determine if waiting for retry
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

      // Adjust polling interval based on status
      const isActive = data.status === 'running' && !isWaitingRetry;
      const desiredInterval = isActive ? POLL_INTERVAL_RUNNING_MS : POLL_INTERVAL_IDLE_MS;
      
      // Stop polling when run is complete
      if (['success', 'failed', 'cancelled'].includes(data.status)) {
        if (pollIntervalRef.current) {
          clearInterval(pollIntervalRef.current);
          pollIntervalRef.current = null;
        }
        setIsPolling(false);

        // Show completion toast
        if (data.status === 'success') {
          toast({
            title: 'Pipeline completata',
            description: `Export server-side completato in ${Math.round((data.runtime_ms || 0) / 1000)}s`,
          });
        } else if (data.status === 'failed') {
          toast({
            title: 'Pipeline fallita',
            description: data.error_message || 'Errore sconosciuto',
            variant: 'destructive',
          });
        }
      }
    } catch (err) {
      console.error('[useSyncProgress] Poll exception:', err);
    }
  }, []);

  // Start adaptive polling
  const startPolling = useCallback((runId: string, isWaiting: boolean = false) => {
    if (pollIntervalRef.current) {
      clearInterval(pollIntervalRef.current);
    }
    
    const interval = isWaiting ? POLL_INTERVAL_IDLE_MS : POLL_INTERVAL_RUNNING_MS;
    pollIntervalRef.current = setInterval(() => pollStatus(runId), interval);
    setIsPolling(true);
    
    // Initial poll
    pollStatus(runId);
  }, [pollStatus]);

  // Start server-side sync pipeline
  const startSync = useCallback(async (): Promise<boolean> => {
    // Prevent multiple concurrent syncs
    if (state.status === 'running' || state.status === 'starting') {
      toast({
        title: 'Sync già in corso',
        description: 'Attendere il completamento della pipeline corrente',
      });
      return false;
    }

    setState(prev => ({ ...prev, status: 'starting', errorMessage: null }));

    try {
      console.log('[useSyncProgress] Calling run-full-sync with trigger=manual...');
      
      const { data, error } = await supabase.functions.invoke('run-full-sync', {
        body: { trigger: 'manual' },
      });

      if (error) {
        console.error('[useSyncProgress] Edge function error:', error);
        setState(prev => ({
          ...prev,
          status: 'failed',
          errorMessage: error.message || 'Errore chiamata run-full-sync',
        }));
        toast({
          title: 'Errore avvio pipeline',
          description: error.message,
          variant: 'destructive',
        });
        return false;
      }

      // Handle various response statuses
      if (data?.status === 'error' && data?.message?.includes('già in corso')) {
        setState(prev => ({
          ...prev,
          status: 'idle',
          errorMessage: 'Sync già in corso',
        }));
        toast({
          title: 'Sync già in corso',
          description: 'Attendere il completamento della pipeline corrente',
        });
        return false;
      }

      if (data?.status === 'busy' && data?.run_id) {
        // Resume monitoring existing run
        console.log('[useSyncProgress] Resuming existing run:', data.run_id);
        setState(prev => ({
          ...prev,
          runId: data.run_id,
          status: 'running',
          startedAt: new Date(),
        }));
        startPolling(data.run_id, false);
        return true;
      }

      if (data?.status === 'waiting_retry' && data?.run_id) {
        console.log('[useSyncProgress] Run waiting for retry:', data.run_id);
        setState(prev => ({
          ...prev,
          runId: data.run_id,
          status: 'waiting_retry',
        }));
        startPolling(data.run_id, true);
        toast({
          title: 'Pipeline in attesa',
          description: `Retry schedulato per ${data.next_retry_at || 'presto'}`,
        });
        return true;
      }

      if (data?.status === 'error') {
        setState(prev => ({
          ...prev,
          status: 'failed',
          errorMessage: data.message || data.error,
        }));
        toast({
          title: 'Errore avvio pipeline',
          description: data.message || data.error,
          variant: 'destructive',
        });
        return false;
      }

      // Accept run_id, runId, or id for compatibility
      const runId = data?.run_id || data?.runId || data?.id;
      if (!runId) {
        // If status is success but no run_id, pipeline completed instantly
        if (data?.status === 'success') {
          setState(prev => ({
            ...prev,
            status: 'success',
            errorMessage: null,
          }));
          toast({
            title: 'Pipeline completata',
            description: 'Export generati con successo',
          });
          return true;
        }
        
        // Show detailed debug info
        const debugInfo = JSON.stringify(data, null, 2);
        console.error('[useSyncProgress] No run_id in response:', data);
        setState(prev => ({
          ...prev,
          status: 'failed',
          errorMessage: `Risposta run-full-sync non valida. Dettagli: ${debugInfo?.substring(0, 200) || 'vuoto'}`,
        }));
        return false;
      }

      console.log('[useSyncProgress] Pipeline started, run_id:', runId);
      
      setState(prev => ({
        ...prev,
        runId,
        status: 'running',
        currentStep: 'import_ftp',
        steps: {},
        errorMessage: null,
        startedAt: new Date(),
        finishedAt: null,
      }));

      // Start adaptive polling
      startPolling(runId, false);

      toast({
        title: 'Pipeline avviata',
        description: 'Generazione export server-side in corso...',
      });

      return true;
    } catch (err) {
      console.error('[useSyncProgress] Exception:', err);
      setState(prev => ({
        ...prev,
        status: 'failed',
        errorMessage: err instanceof Error ? err.message : 'Errore sconosciuto',
      }));
      toast({
        title: 'Errore',
        description: err instanceof Error ? err.message : 'Errore sconosciuto',
        variant: 'destructive',
      });
      return false;
    }
  }, [state.status, startPolling]);

  // Download server-generated export file using paths from sync_runs.steps.exports.files
  const downloadExport = useCallback(async (fileKey: keyof typeof EXPORT_FILES): Promise<void> => {
    const fileInfo = EXPORT_FILES[fileKey];
    
    // Use path from sync_runs.steps.exports.files if available, otherwise fallback to default
    const exportFilePath = state.exportFiles[fileKey];
    // Remove 'exports/' prefix if present since we're downloading from 'exports' bucket
    const downloadPath = exportFilePath 
      ? exportFilePath.replace(/^exports\//, '')
      : fileInfo.path;
    
    try {
      console.log(`[useSyncProgress] Downloading ${downloadPath} from exports bucket...`);
      
      const { data, error } = await supabase.storage
        .from('exports')
        .download(downloadPath);

      if (error || !data) {
        console.error(`[useSyncProgress] Download error for ${downloadPath}:`, error);
        toast({
          title: 'Errore download',
          description: `Impossibile scaricare ${fileInfo.displayName}: ${error?.message || 'File non trovato'}`,
          variant: 'destructive',
        });
        return;
      }

      // Create download link
      const url = URL.createObjectURL(data);
      const a = document.createElement('a');
      a.href = url;
      // Use just the filename for download
      const fileName = downloadPath.split('/').pop() || fileInfo.path;
      a.download = fileName;
      a.rel = 'noopener';
      a.style.display = 'none';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      
      setTimeout(() => URL.revokeObjectURL(url), 3000);

      toast({
        title: 'Download completato',
        description: `${fileInfo.displayName} scaricato`,
      });
    } catch (err) {
      console.error(`[useSyncProgress] Download exception:`, err);
      toast({
        title: 'Errore download',
        description: err instanceof Error ? err.message : 'Errore sconosciuto',
        variant: 'destructive',
      });
    }
  }, [state.exportFiles]);

  // Reset state
  const reset = useCallback(() => {
    if (pollIntervalRef.current) {
      clearInterval(pollIntervalRef.current);
      pollIntervalRef.current = null;
    }
    setIsPolling(false);
    setState(INITIAL_STATE);
  }, []);

  // Get progress percentage based on step
  const getProgress = useCallback((): number => {
    const currentIdx = ALL_PIPELINE_STEPS.indexOf(state.currentStep);
    if (currentIdx < 0) return 0;
    if (state.status === 'success') return 100;
    
    // For parse_merge, add fractional progress based on offset
    if (state.currentStep === 'parse_merge' && state.steps.parse_merge) {
      const pm = state.steps.parse_merge;
      const baseProgress = (currentIdx / ALL_PIPELINE_STEPS.length) * 100;
      const stepProgress = 100 / ALL_PIPELINE_STEPS.length;
      
      // Estimate progress within parse_merge
      if (pm.productCount && pm.offset) {
        // Rough estimate - assume ~50k total rows typical
        const estimatedTotal = 50000;
        const fraction = Math.min(pm.offset / estimatedTotal, 0.95);
        return Math.round(baseProgress + stepProgress * fraction);
      }
    }
    
    return Math.round(((currentIdx + 1) / ALL_PIPELINE_STEPS.length) * 100);
  }, [state.currentStep, state.status, state.steps]);

  // Get step display name
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
