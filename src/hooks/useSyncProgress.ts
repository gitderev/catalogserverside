import { useState, useCallback, useRef, useEffect } from 'react';
import { supabase } from '@/integrations/supabase/client';
import { toast } from '@/hooks/use-toast';

export type SyncStatus = 'idle' | 'starting' | 'running' | 'success' | 'failed' | 'cancelled';

export interface SyncStep {
  status: string;
  error?: string;
  rows?: number;
  duration_ms?: number;
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
  runtimeMs: null,
  locationWarnings: {},
  startedAt: null,
  finishedAt: null,
  exportFiles: {},
};

const POLL_INTERVAL_MS = 2000;

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
        .select('status, steps, error_message, runtime_ms, location_warnings, started_at, finished_at')
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

      setState(prev => ({
        ...prev,
        status: data.status as SyncStatus,
        currentStep,
        steps: steps as Record<string, SyncStep>,
        errorMessage: data.error_message,
        runtimeMs: data.runtime_ms,
        locationWarnings: (data.location_warnings || {}) as Record<string, number>,
        finishedAt: data.finished_at ? new Date(data.finished_at) : null,
        exportFiles,
      }));

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

      // Handle 409 Conflict (sync already running)
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

      const runId = data?.run_id;
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
        
        console.error('[useSyncProgress] No run_id in response:', data);
        setState(prev => ({
          ...prev,
          status: 'failed',
          errorMessage: 'Risposta run-full-sync non valida',
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

      // Start polling
      setIsPolling(true);
      pollIntervalRef.current = setInterval(() => pollStatus(runId), POLL_INTERVAL_MS);
      
      // Initial poll
      await pollStatus(runId);

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
  }, [state.status, pollStatus]);

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
      a.download = downloadPath;
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
    const stepOrder = ['import_ftp', 'parse_merge', 'ean_mapping', 'pricing', 'export_ean', 'export_mediaworld', 'export_eprice', 'upload_sftp'];
    const currentIdx = stepOrder.indexOf(state.currentStep);
    if (currentIdx < 0) return 0;
    if (state.status === 'success') return 100;
    return Math.round(((currentIdx + 1) / stepOrder.length) * 100);
  }, [state.currentStep, state.status]);

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
    isRunning: state.status === 'running' || state.status === 'starting',
    isComplete: state.status === 'success',
    isFailed: state.status === 'failed',
  };
}
