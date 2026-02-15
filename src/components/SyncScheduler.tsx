import React, { useState, useEffect, useCallback } from 'react';
import { supabase } from '@/integrations/supabase/client';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Switch } from '@/components/ui/switch';
import { Label } from '@/components/ui/label';
import { Input } from '@/components/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Badge } from '@/components/ui/badge';
import { Separator } from '@/components/ui/separator';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '@/components/ui/dialog';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import { toast } from '@/hooks/use-toast';
import { 
  Clock, 
  Play, 
  Square, 
  RefreshCw, 
  CheckCircle, 
  XCircle, 
  AlertTriangle, 
  Loader2,
  Calendar,
  Activity,
  FileText,
  Upload,
  ChevronDown,
  ChevronRight,
  Server,
  Zap,
  Copy
} from 'lucide-react';

interface SyncConfig {
  id: number;
  enabled: boolean;
  frequency_minutes: number;
  daily_time: string | null;
  max_retries: number;
  retry_delay_minutes: number;
  updated_at: string;
  schedule_type: 'hours' | 'daily';
  notification_mode: 'never' | 'always' | 'only_on_problem';
  notify_on_warning: boolean;
  run_timeout_minutes: number;
  max_attempts: number;
  last_disabled_reason: string | null;
}

/**
 * Valori di stato possibili nella tabella sync_runs:
 * - running: sincronizzazione in corso
 * - success: sincronizzazione completata con successo
 * - failed: sincronizzazione fallita (per errore, timeout, cancel, o reset)
 * - timeout: sincronizzazione interrotta per superamento tempo massimo
 * - skipped: sincronizzazione saltata (es. frequenza non ancora raggiunta)
 */
interface SyncRun {
  id: string;
  started_at: string;
  finished_at: string | null;
  status: 'running' | 'success' | 'success_with_warning' | 'failed' | 'timeout' | 'skipped';
  trigger_type: 'cron' | 'manual';
  attempt: number;
  runtime_ms: number | null;
  error_message: string | null;
  error_details: unknown;
  steps: Record<string, StepResult>;
  metrics: PipelineMetrics;
  cancel_requested: boolean;
  cancelled_by_user: boolean;
  warning_count: number;
  location_warnings: Record<string, number>;
}

// Stale run threshold is dynamic: uses config.run_timeout_minutes (default 60min)

interface StepResult {
  status: 'success' | 'failed' | 'skipped';
  duration_ms: number;
  [key: string]: unknown;
}

interface PipelineMetrics {
  products_total: number;
  products_processed: number;
  products_ean_mapped: number;
  products_ean_missing: number;
  products_ean_invalid: number;
  products_after_override: number;
  mediaworld_export_rows: number;
  mediaworld_export_skipped: number;
  eprice_export_rows: number;
  eprice_export_skipped: number;
  exported_files_count: number;
  sftp_uploaded_files: number;
  warnings: string[];
}

const FREQUENCY_OPTIONS = [
  { value: '60', label: 'Ogni 1 ora' },
  { value: '120', label: 'Ogni 2 ore' },
  { value: '180', label: 'Ogni 3 ore' },
  { value: '360', label: 'Ogni 6 ore' },
  { value: '720', label: 'Ogni 12 ore' },
  { value: '1440', label: 'Una volta al giorno' }
];

const STATUS_LABELS: Record<string, string> = {
  running: 'In esecuzione',
  success: 'Successo',
  success_with_warning: 'Successo con avvisi',
  failed: 'Fallita',
  timeout: 'Timeout',
  skipped: 'Saltata'
};

const NOTIFICATION_MODE_LABELS: Record<string, string> = {
  never: 'Mai',
  always: 'Sempre',
  only_on_problem: 'Solo in caso di problemi'
};

const STEP_LABELS: Record<string, string> = {
  import_ftp: 'Import FTP',
  parse_merge: 'Parsing e Merge',
  ean_mapping: 'Mapping EAN',
  pricing: 'Calcolo Prezzi',
  override: 'Override Prodotti',
  export_ean: 'Export Catalogo EAN',
  export_ean_xlsx: 'Export Catalogo EAN (XLSX)',
  export_amazon: 'Export Amazon',
  export_mediaworld: 'Export Mediaworld',
  export_eprice: 'Export ePrice',
  upload_sftp: 'Upload SFTP'
};

// Helper function to get user-friendly error messages in Italian
const getFriendlyErrorMessage = (errorMessage: string | null, errorDetails: unknown): string => {
  if (!errorMessage) return 'Errore sconosciuto';
  
  const msg = errorMessage.toLowerCase();
  const details = errorDetails ? JSON.stringify(errorDetails).toLowerCase() : '';
  
  // FTP authentication errors
  if (msg.includes('ftp') || msg.includes('authentication') || msg.includes('login') || details.includes('530')) {
    if (msg.includes('credential') || msg.includes('password') || msg.includes('auth') || details.includes('530') || details.includes('login')) {
      return 'Errore import FTP: credenziali non valide (verifica host, utente e password FTP)';
    }
    if (msg.includes('connection') || msg.includes('connect') || msg.includes('timeout') || msg.includes('refused')) {
      return 'Errore import FTP: impossibile connettersi al server (verifica host e porta FTP)';
    }
    if (msg.includes('host') || msg.includes('resolve') || msg.includes('dns')) {
      return 'Errore import FTP: server non raggiungibile (verifica l\'indirizzo host)';
    }
  }
  
  // SFTP errors
  if (msg.includes('sftp') || msg.includes('upload')) {
    if (msg.includes('credential') || msg.includes('password') || msg.includes('auth')) {
      return 'Errore upload SFTP: credenziali non valide (verifica utente e password SFTP)';
    }
    if (msg.includes('connection') || msg.includes('connect')) {
      return 'Errore upload SFTP: impossibile connettersi al server';
    }
    return 'Errore upload SFTP: caricamento file non riuscito';
  }
  
  // Pricing errors
  if (msg.includes('pricing') || msg.includes('prezzo') || msg.includes('price')) {
    return 'Errore calcolo prezzi: verifica la configurazione delle fee';
  }
  
  // Override errors
  if (msg.includes('override') && msg.includes('.99')) {
    return 'Errore override: il prezzo finale deve terminare con ,99';
  }
  
  // User cancelled
  if (msg.includes('interrotta') || msg.includes('cancel')) {
    return 'Sincronizzazione interrotta manualmente dall\'utente';
  }
  
  // Timeout
  if (msg.includes('timeout') || msg.includes('30 minuti')) {
    return 'Sincronizzazione interrotta per timeout (superati 30 minuti)';
  }
  
  // IMPORTANT: Do NOT mask file/parse errors with a generic message.
  // Pass through the original error so the user sees the real cause.
  return errorMessage.length > 200 ? errorMessage.substring(0, 200) + '...' : errorMessage;
};

// Status badge with proper colors
const getStatusBadge = (status: string, size: 'sm' | 'md' = 'sm') => {
  const baseClasses = size === 'md' ? 'px-3 py-1.5 text-sm font-semibold' : 'px-2 py-1 text-xs font-medium';
  
  const statusStyles: Record<string, string> = {
    running: 'alt-sync-badge alt-sync-badge--running',
    success: 'alt-sync-badge alt-sync-badge--success',
    success_with_warning: 'alt-sync-badge alt-sync-badge--timeout',
    failed: 'alt-sync-badge alt-sync-badge--failed',
    timeout: 'alt-sync-badge alt-sync-badge--timeout',
    skipped: 'alt-sync-badge alt-sync-badge--skipped'
  };

  const icons: Record<string, React.ReactNode> = {
    running: <Loader2 className={`${size === 'md' ? 'h-4 w-4' : 'h-3 w-3'} animate-spin`} />,
    success: <CheckCircle className={size === 'md' ? 'h-4 w-4' : 'h-3 w-3'} />,
    success_with_warning: <AlertTriangle className={size === 'md' ? 'h-4 w-4' : 'h-3 w-3'} />,
    failed: <XCircle className={size === 'md' ? 'h-4 w-4' : 'h-3 w-3'} />,
    timeout: <Clock className={size === 'md' ? 'h-4 w-4' : 'h-3 w-3'} />,
    skipped: <AlertTriangle className={size === 'md' ? 'h-4 w-4' : 'h-3 w-3'} />
  };

  return (
    <span className={`inline-flex items-center gap-1.5 rounded-full border ${baseClasses} ${statusStyles[status] || statusStyles.skipped}`}>
      {icons[status]}
      {STATUS_LABELS[status] || status}
    </span>
  );
};

export const SyncScheduler: React.FC = () => {
  const [config, setConfig] = useState<SyncConfig | null>(null);
  const [runs, setRuns] = useState<SyncRun[]>([]);
  const [currentRun, setCurrentRun] = useState<SyncRun | null>(null);
  const [isStaleRun, setIsStaleRun] = useState(false);
  const [selectedRun, setSelectedRun] = useState<SyncRun | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [isStarting, setIsStarting] = useState(false);
  const [isStopping, setIsStopping] = useState(false);
  const [isResetting, setIsResetting] = useState(false);
  const [showLogs, setShowLogs] = useState(false);
  const [expandedLogDetails, setExpandedLogDetails] = useState<string | null>(null);
  const [runEvents, setRunEvents] = useState<Array<{ id: string; level: string; message: string; details: unknown; created_at: string; step: string | null }>>([]);
  const [isLoadingEvents, setIsLoadingEvents] = useState(false);

  // Load config and runs
  const loadData = useCallback(async () => {
    try {
      const { data: configData, error: configError } = await supabase
        .from('sync_config')
        .select('*')
        .eq('id', 1)
        .maybeSingle();

      if (configError) throw configError;
      
      if (configData) {
        setConfig(configData as SyncConfig);
      }

      const { data: runsData, error: runsError } = await supabase
        .from('sync_runs')
        .select('*')
        .order('started_at', { ascending: false })
        .limit(20);

      if (runsError) throw runsError;
      
      setRuns((runsData || []) as unknown as SyncRun[]);

      // Trova run in stato running
      const running = (runsData || []).find((r: Record<string, unknown>) => r.status === 'running');
      
      if (running) {
        // Use config.run_timeout_minutes for stale detection (default 60)
        const staleThresholdMs = ((configData as SyncConfig)?.run_timeout_minutes || 60) * 60 * 1000;
        const startedAt = new Date(running.started_at).getTime();
        const elapsed = Date.now() - startedAt;
        const isStale = elapsed > staleThresholdMs;
        
        setCurrentRun(running as unknown as SyncRun);
        setIsStaleRun(isStale);
        
        if (isStale) {
          console.log(`[SyncScheduler] Detected stale run: ${running.id}, running for ${Math.round(elapsed / 60000)} minutes`);
        }
      } else {
        setCurrentRun(null);
        setIsStaleRun(false);
      }

    } catch (error: unknown) {
      console.error('Error loading sync data:', error);
      toast({
        title: 'Errore',
        description: 'Impossibile caricare i dati di sincronizzazione',
        variant: 'destructive'
      });
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    loadData();
    const interval = setInterval(loadData, 10000);
    return () => clearInterval(interval);
  }, [loadData]);

  // Load sync_events when a run is selected
  useEffect(() => {
    if (!selectedRun) {
      setRunEvents([]);
      return;
    }
    const loadEvents = async () => {
      setIsLoadingEvents(true);
      try {
        const { data, error } = await supabase
          .from('sync_events')
          .select('id, level, message, details, created_at, step')
          .eq('run_id', selectedRun.id)
          .order('created_at', { ascending: false })
          .limit(50);
        if (!error && data) {
          setRunEvents(data as typeof runEvents);
        }
      } catch (e) {
        console.error('Error loading sync events:', e);
      } finally {
        setIsLoadingEvents(false);
      }
    };
    loadEvents();
  }, [selectedRun?.id]);

  const saveConfig = async (updates: Partial<SyncConfig>) => {
    if (!config) {
      // Config not loaded yet — try to create singleton
      toast({ title: 'Errore', description: 'Configurazione non ancora caricata', variant: 'destructive' });
      return;
    }

    if (import.meta.env.DEV) {
      console.log('[SyncScheduler] saveConfig called:', JSON.stringify(updates), new Date().toISOString());
    }

    // Optimistic update
    const previousConfig = { ...config };
    setConfig({ ...config, ...updates });
    setIsSaving(true);

    try {
      const { error, count } = await supabase
        .from('sync_config')
        .update(updates)
        .eq('id', 1);

      if (error) throw error;

      toast({
        title: 'Salvato',
        description: 'Configurazione aggiornata'
      });
    } catch (error: unknown) {
      // Rollback on failure
      setConfig(previousConfig);
      const supaError = error as { code?: string; message?: string };
      const errorCode = supaError?.code || 'UNKNOWN';
      const errorMessage = supaError?.message || 'Errore sconosciuto';
      console.error('Error saving config:', errorCode, errorMessage);
      toast({
        title: 'Errore salvataggio',
        description: `Impossibile salvare (${errorCode}): ${errorMessage}`,
        variant: 'destructive'
      });
    } finally {
      setIsSaving(false);
    }
  };

  const startSync = async () => {
    if (currentRun) {
      toast({
        title: 'Sync in corso',
        description: 'Attendi il completamento della sincronizzazione corrente',
        variant: 'destructive'
      });
      return;
    }

    setIsStarting(true);
    try {
      const { data: { session } } = await supabase.auth.getSession();
      if (!session?.access_token) {
        throw new Error('Sessione non valida');
      }

      // Initial call
      const response = await supabase.functions.invoke('run-full-sync', {
        body: { trigger: 'manual' }
      });

      if (response.error) {
        throw new Error(response.error.message);
      }

      let data = response.data;

      if (data.status === 'error') {
        throw new Error(data.message);
      }

      toast({
        title: 'Sincronizzazione avviata',
        description: 'La pipeline è in esecuzione'
      });

      await loadData();

      // Resume loop: if orchestrator yields, re-invoke until complete
      let resumeCount = 0;
      const MAX_RESUME_CALLS = 200; // safety limit
      
      while (data?.needs_resume && data?.run_id && resumeCount < MAX_RESUME_CALLS) {
        resumeCount++;
        console.log(`[SyncScheduler] Resume #${resumeCount} for run ${data.run_id}, step: ${data.current_step}`);
        
        // Small delay to avoid hammering
        await new Promise(resolve => setTimeout(resolve, 1000));
        
        // Refresh data display
        await loadData();
        
        const resumeResp = await supabase.functions.invoke('run-full-sync', {
          body: { trigger: 'manual', resume_run_id: data.run_id }
        });
        
        if (resumeResp.error) {
          console.error('[SyncScheduler] Resume error:', resumeResp.error);
          break;
        }
        
        data = resumeResp.data;
        
        if (data?.status === 'error') {
          console.error('[SyncScheduler] Resume returned error:', data.message);
          toast({
            title: 'Errore durante sync',
            description: data.message || 'Errore imprevisto durante il resume',
            variant: 'destructive'
          });
          break;
        }
        
        if (data?.status === 'already_finished') {
          console.log('[SyncScheduler] Run already finished:', data.run_status);
          break;
        }
      }
      
      if (resumeCount >= MAX_RESUME_CALLS) {
        console.warn('[SyncScheduler] Max resume calls reached');
        toast({
          title: 'Attenzione',
          description: 'La pipeline ha superato il limite massimo di resume. Controlla lo stato nella UI.',
          variant: 'destructive'
        });
      }

      // Final refresh
      await loadData();

    } catch (error: unknown) {
      console.error('Error starting sync:', error);
      toast({
        title: 'Errore',
        description: (error instanceof Error ? error.message : null) || 'Impossibile avviare la sincronizzazione',
        variant: 'destructive'
      });
    } finally {
      setIsStarting(false);
    }
  };

  const stopSync = async (force = false) => {
    if (!currentRun) {
      toast({
        title: 'Nessuna sync in corso',
        description: 'Non c\'è alcuna sincronizzazione da interrompere'
      });
      return;
    }

    setIsStopping(true);
    try {
      const response = await supabase.functions.invoke('stop-sync', {
        body: { run_id: currentRun.id, force }
      });

      if (response.error) {
        throw new Error(response.error.message);
      }

      const data = response.data;

      if (data.status === 'error') {
        throw new Error(data.message);
      }

      toast({
        title: force ? 'Sincronizzazione interrotta' : 'Richiesta inviata',
        description: force 
          ? 'La sincronizzazione è stata interrotta immediatamente' 
          : 'La sincronizzazione verrà interrotta al prossimo step'
      });

      await loadData();

    } catch (error: unknown) {
      console.error('Error stopping sync:', error);
      toast({
        title: 'Errore',
        description: (error instanceof Error ? error.message : null) || 'Impossibile interrompere la sincronizzazione',
        variant: 'destructive'
      });
    } finally {
      setIsStopping(false);
    }
  };

  // Funzione per forzare il reset di run zombie/bloccate
  const forceResetSync = async () => {
    if (!currentRun) {
      toast({
        title: 'Nessuna sync da resettare',
        description: 'Non c\'è alcuna sincronizzazione bloccata'
      });
      return;
    }

    setIsResetting(true);
    try {
      const response = await supabase.functions.invoke('force-reset-sync', {
        body: { run_id: currentRun.id }
      });

      if (response.error) {
        throw new Error(response.error.message);
      }

      const data = response.data;

      if (data.status === 'error') {
        throw new Error(data.message);
      }

      toast({
        title: 'Reset completato',
        description: 'La sincronizzazione bloccata è stata resettata'
      });

      await loadData();

    } catch (error: unknown) {
      console.error('Error resetting sync:', error);
      toast({
        title: 'Errore',
        description: (error instanceof Error ? error.message : null) || 'Impossibile resettare la sincronizzazione',
        variant: 'destructive'
      });
    } finally {
      setIsResetting(false);
    }
  };

  const getNextSyncTime = (): string | null => {
    if (!config?.enabled || runs.length === 0) return null;

    if (config.schedule_type === 'daily') {
      const dailyTime = config.daily_time || '03:00';
      const [hours, minutes] = dailyTime.split(':').map(Number);
      const now = new Date();
      // Display in Europe/Rome timezone
      const romeNow = new Date(now.toLocaleString('en-US', { timeZone: 'Europe/Rome' }));
      const target = new Date(romeNow);
      target.setHours(hours, minutes, 0, 0);
      
      if (target <= romeNow) {
        target.setDate(target.getDate() + 1);
      }
      
      return `${target.toLocaleDateString('it-IT')} alle ${dailyTime} (Europe/Rome)`;
    }

    // Hours mode
    const lastCronRun = runs.find(r => r.trigger_type === 'cron' && r.attempt === 1);
    if (!lastCronRun) return 'Prossima esecuzione programmata';

    const lastStarted = new Date(lastCronRun.started_at);
    const nextRun = new Date(lastStarted.getTime() + config.frequency_minutes * 60 * 1000);

    return nextRun.toLocaleString('it-IT', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  const hasMaxRetriesWarning = (): boolean => {
    const maxAttempts = config?.max_attempts || 3;
    const lastCronRun = runs.find(r => r.trigger_type === 'cron');
    return lastCronRun?.attempt === maxAttempts && ['failed', 'timeout'].includes(lastCronRun.status);
  };

  const formatDuration = (ms: number | null): string => {
    if (!ms) return '-';
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
    return `${(ms / 60000).toFixed(1)}min`;
  };

  if (isLoading) {
    return (
      <Card className="mb-6 alt-card">
        <CardContent className="flex items-center justify-center py-12">
          <Loader2 className="h-8 w-8 animate-spin text-primary" />
        </CardContent>
      </Card>
    );
  }

  return (
    <>
      {/* Main Scheduling Card */}
      <Card className="mb-6 alt-card">
        <CardHeader className="pb-4">
          <CardTitle className="flex items-center gap-2 text-xl font-bold">
            <RefreshCw className="h-5 w-5 text-primary" />
            Pianificazione della sincronizzazione
          </CardTitle>
          <CardDescription className="alt-text-muted">
            Gestisci la sincronizzazione automatica dei cataloghi verso i marketplace
          </CardDescription>
        </CardHeader>

        <CardContent className="space-y-6">
          {/* Warning for max retries */}
          {hasMaxRetriesWarning() && (
            <div className="alt-alert alt-alert-error">
              <AlertTriangle className="h-5 w-5 text-error flex-shrink-0 mt-0.5" />
              <div>
                <p className="font-semibold text-error">
                  Attenzione: la sincronizzazione automatica ha fallito {config?.max_attempts || 3} volte consecutive.
                </p>
                <p className="text-sm alt-text-muted mt-1">
                  Controlla i log per verificare il problema e correggerlo.
                </p>
                <Button
                  variant="link"
                  className="p-0 h-auto text-error hover:text-error/80 font-medium"
                  onClick={() => {
                    const failedRun = runs.find(r => r.trigger_type === 'cron' && r.attempt === (config?.max_attempts || 3));
                    if (failedRun) setSelectedRun(failedRun);
                  }}
                >
                  Visualizza ultimo job fallito -&gt;
                </Button>
              </div>
            </div>
          )}

          {/* Configuration Section */}
          <div className="alt-panel rounded-lg p-5">
            <h3 className="text-sm font-semibold alt-text-muted uppercase tracking-wide mb-4">Configurazione</h3>
            
            <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
              {/* Enable/Disable Toggle */}
              <div className="space-y-3">
                <div className="flex items-center gap-3">
                  <Switch
                    id="sync-enabled"
                    checked={config?.enabled || false}
                    onCheckedChange={(checked: boolean) => {
                      if (import.meta.env.DEV) {
                        console.log('[SyncScheduler] toggle_click', { checked, ts: Date.now() });
                      }
                      saveConfig({ enabled: checked });
                    }}
                    disabled={isSaving || !config}
                    className="data-[state=checked]:bg-primary"
                  />
                  <div>
                    <Label htmlFor="sync-enabled" className="font-semibold cursor-pointer">
                      Sincronizzazione automatica
                    </Label>
                    <span className={`ml-2 inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${
                      config?.enabled 
                        ? 'bg-success/20 text-success' 
                        : 'alt-badge alt-badge-idle'
                    }`}>
                      {config?.enabled ? 'Attivata' : 'Disattivata'}
                    </span>
                  </div>
                </div>
                <p className="text-xs alt-text-muted leading-relaxed">
                  Quando attiva, la pipeline viene eseguita automaticamente in base alla frequenza scelta.
                </p>
                {config?.last_disabled_reason && !config?.enabled && (
                  <p className="text-xs text-warning">
                    {config.last_disabled_reason}
                  </p>
                )}
                {!config && !isLoading && (
                  <p className="text-xs text-destructive font-medium mt-1">
                    ⚠ Configurazione non trovata. La riga sync_config (id=1) potrebbe mancare dal database.
                  </p>
                )}
              </div>

              {/* Schedule Type */}
              <div className="space-y-2">
                <Label className="font-semibold">Tipo di pianificazione</Label>
                <Select
                  value={config?.schedule_type || 'hours'}
                  onValueChange={(value) => saveConfig({ schedule_type: value as 'hours' | 'daily' })}
                  disabled={isSaving}
                >
                  <SelectTrigger className="alt-input">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="hours">Ogni N ore</SelectItem>
                    <SelectItem value="daily">Giornaliero</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              {/* Frequency (only for hours mode) */}
              {(config?.schedule_type || 'hours') === 'hours' && (
                <div className="space-y-2">
                  <Label className="font-semibold">Frequenza</Label>
                  <Select
                    value={String(config?.frequency_minutes || 60)}
                    onValueChange={(value) => saveConfig({ frequency_minutes: parseInt(value) })}
                    disabled={isSaving}
                  >
                    <SelectTrigger className="alt-input">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      {FREQUENCY_OPTIONS.map(opt => (
                        <SelectItem key={opt.value} value={opt.value}>
                          {opt.label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              )}

              {/* Daily time (only for daily mode) */}
              {(config?.schedule_type || 'hours') === 'daily' && (
                <div className="space-y-2">
                  <Label className="font-semibold">Orario giornaliero (Europe/Rome)</Label>
                  <Input
                    type="time"
                    value={config?.daily_time || '03:00'}
                    onChange={(e) => saveConfig({ daily_time: e.target.value })}
                    disabled={isSaving}
                    className="alt-input"
                  />
                </div>
              )}
            </div>

            {/* Notification Settings */}
            <Separator className="my-4 bg-slate-200" />
            <h3 className="text-sm font-semibold alt-text-muted uppercase tracking-wide mb-4">Notifiche Email</h3>
            <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
              <div className="space-y-2">
                <Label className="font-semibold">Modalità notifica</Label>
                <Select
                  value={config?.notification_mode || 'never'}
                  onValueChange={(value) => saveConfig({ notification_mode: value as 'never' | 'always' | 'only_on_problem' })}
                  disabled={isSaving}
                >
                  <SelectTrigger className="alt-input">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {Object.entries(NOTIFICATION_MODE_LABELS).map(([key, label]) => (
                      <SelectItem key={key} value={key}>{label}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              {config?.notification_mode === 'only_on_problem' && (
                <div className="space-y-3">
                  <div className="flex items-center gap-3">
                    <Switch
                      id="notify-warning"
                      checked={config?.notify_on_warning ?? true}
                      onCheckedChange={(checked) => saveConfig({ notify_on_warning: checked })}
                      disabled={isSaving}
                      className="data-[state=checked]:bg-primary"
                    />
                    <Label htmlFor="notify-warning" className="font-semibold cursor-pointer">
                      Notifica anche per avvisi
                    </Label>
                  </div>
                  <p className="text-xs alt-text-muted">
                    Invia email anche quando la sync completa con avvisi (non solo errori).
                  </p>
                </div>
              )}
            </div>
          </div>

          {/* Current step indicator for running sync */}
          {currentRun && currentRun.status === 'running' && (
            <div className="alt-alert alt-alert-info">
              <Loader2 className="h-5 w-5 animate-spin text-info" />
              <div>
                <p className="font-semibold text-info">Sincronizzazione in corso</p>
                <p className="text-sm alt-text-muted">
                  Step corrente: {STEP_LABELS[String((currentRun.steps as Record<string, unknown>)?.current_step ?? '')] || String((currentRun.steps as Record<string, unknown>)?.current_step ?? '') || 'Avvio...'}
                </p>
              </div>
            </div>
          )}

          {/* Status Summary Section */}
          <div className="grid gap-4 md:grid-cols-3">
            {/* Last sync */}
            <div className="alt-panel rounded-lg p-4">
              <div className="flex items-center gap-2 mb-2">
                <Activity className="h-4 w-4 alt-text-muted" />
                <span className="text-xs font-semibold alt-text-muted uppercase tracking-wide">Ultima sincronizzazione</span>
              </div>
              {runs.length > 0 ? (
                <div className="space-y-2">
                  <div className="flex items-center gap-2">
                    {getStatusBadge(runs[0].status, 'md')}
                  </div>
                  <p className="text-sm alt-text-muted">
                    {new Date(runs[0].started_at).toLocaleString('it-IT', {
                      day: '2-digit',
                      month: '2-digit',
                      hour: '2-digit',
                      minute: '2-digit'
                    })}
                  </p>
                  {runs[0].error_message && runs[0].status === 'failed' && (
                    <p className="text-xs text-error alt-info-box px-2 py-1 rounded">
                      {getFriendlyErrorMessage(runs[0].error_message, runs[0].error_details)}
                    </p>
                  )}
                </div>
              ) : (
                <p className="text-sm alt-text-muted">Nessuna esecuzione</p>
              )}
            </div>

            {/* Metrics summary */}
            <div className="alt-panel rounded-lg p-4">
              <div className="flex items-center gap-2 mb-2">
                <Server className="h-4 w-4 alt-text-muted" />
                <span className="text-xs font-semibold alt-text-muted uppercase tracking-wide">Riepilogo</span>
              </div>
              {runs.length > 0 && runs[0].metrics ? (
                <div className="space-y-1">
                  <div className="flex justify-between">
                    <span className="text-sm alt-text-muted">Prodotti</span>
                    <span className="text-sm font-semibold">{runs[0].metrics.products_processed || 0}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-sm alt-text-muted">File caricati</span>
                    <span className="text-sm font-semibold">{runs[0].metrics.sftp_uploaded_files || 0}</span>
                  </div>
                </div>
              ) : (
                <p className="text-sm alt-text-muted">-</p>
              )}
            </div>

            {/* Next sync */}
            <div className="alt-panel rounded-lg p-4">
              <div className="flex items-center gap-2 mb-2">
                <Calendar className="h-4 w-4 alt-text-muted" />
                <span className="text-xs font-semibold alt-text-muted uppercase tracking-wide">Prossima sincronizzazione</span>
              </div>
              {config?.enabled ? (
                <p className="text-sm font-medium">{getNextSyncTime() || '-'}</p>
              ) : (
                <span className="alt-badge alt-badge-idle">
                  Disattivata
                </span>
              )}
            </div>
          </div>

          <Separator className="bg-slate-200" />

          {/* Action Buttons */}
          <div className="flex flex-wrap gap-3">
            <Button
              onClick={startSync}
              disabled={isStarting || !!currentRun}
              className="bg-emerald-600 hover:bg-emerald-700 text-white font-semibold shadow-sm disabled:bg-slate-300 disabled:text-slate-500"
            >
              {isStarting ? (
                <Loader2 className="h-4 w-4 animate-spin mr-2" />
              ) : (
                <Play className="h-4 w-4 mr-2" />
              )}
              Esegui sincronizzazione ora
            </Button>
            
            {/* Mostra pulsante diverso se la run è bloccata/zombie */}
            {isStaleRun && currentRun ? (
              <Button
                variant="outline"
                onClick={forceResetSync}
                disabled={isResetting}
                className="border-warning text-warning hover:bg-warning/10 hover:border-warning font-semibold"
              >
                {isResetting ? (
                  <Loader2 className="h-4 w-4 animate-spin mr-2" />
                ) : (
                  <AlertTriangle className="h-4 w-4 mr-2" />
                )}
                Sblocca sincronizzazione
              </Button>
            ) : (
              <Button
                variant="outline"
                onClick={() => stopSync(false)}
                disabled={isStopping || !currentRun}
                className="border-error/50 text-error hover:bg-error/10 hover:border-error font-semibold disabled:border-muted disabled:text-muted disabled:bg-transparent"
              >
                {isStopping ? (
                  <Loader2 className="h-4 w-4 animate-spin mr-2" />
                ) : (
                  <Square className="h-4 w-4 mr-2" />
                )}
                Ferma sincronizzazione
              </Button>
            )}

            {/* Pulsante per forzare l'interruzione (sempre visibile se c'è una run) */}
            {currentRun && !isStaleRun && (
              <Button
                variant="ghost"
                onClick={() => stopSync(true)}
                disabled={isStopping}
                className="text-warning hover:text-warning hover:bg-warning/10 font-medium"
                title="Forza l'interruzione immediata senza attendere il prossimo step"
              >
                <AlertTriangle className="h-4 w-4 mr-2" />
                Forza stop
              </Button>
            )}

            <Button
              variant="ghost"
              onClick={() => setShowLogs(!showLogs)}
              className="text-slate-600 hover:text-slate-900 hover:bg-slate-100 font-medium"
            >
              <FileText className="h-4 w-4 mr-2" />
              {showLogs ? 'Nascondi log' : 'Mostra log'}
              {showLogs ? <ChevronDown className="h-4 w-4 ml-1" /> : <ChevronRight className="h-4 w-4 ml-1" />}
            </Button>
          </div>

          {/* Warning per run bloccata */}
          {isStaleRun && currentRun && (
            <div className="alt-alert alt-alert-warning">
              <AlertTriangle className="h-5 w-5 text-warning flex-shrink-0 mt-0.5" />
              <div>
                <p className="font-semibold text-warning">
                  Sincronizzazione bloccata
                </p>
                <p className="text-sm alt-text-muted mt-1">
                   La sincronizzazione è in esecuzione da più di {config?.run_timeout_minutes || 60} minuti e potrebbe essere bloccata.
                   Usa il pulsante "Sblocca sincronizzazione" per resettare lo stato e poter avviare una nuova run.
                </p>
              </div>
            </div>
          )}

          {/* Sync Logs */}
          {showLogs && (
            <div className="bg-white rounded-lg border border-slate-200 overflow-hidden">
              <div className="px-4 py-3 border-b border-border alt-panel">
                <h4 className="font-semibold">Log sincronizzazioni</h4>
              </div>
              <ScrollArea className="h-[350px]">
                <div className="divide-y divide-border">
                  {runs.map(run => (
                    <div key={run.id} className="hover:bg-muted/10 transition-colors">
                      <div
                        className="p-4 cursor-pointer"
                        onClick={() => setSelectedRun(run)}
                      >
                        <div className="flex flex-wrap items-center justify-between gap-2">
                          <div className="flex flex-wrap items-center gap-2">
                            {getStatusBadge(run.status)}
                            <span className="text-sm font-medium">
                              {new Date(run.started_at).toLocaleString('it-IT', {
                                day: '2-digit',
                                month: '2-digit',
                                year: 'numeric',
                                hour: '2-digit',
                                minute: '2-digit'
                              })}
                            </span>
                            <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium ${
                              run.trigger_type === 'cron' 
                                ? 'alt-badge alt-badge-info' 
                                : 'alt-badge alt-badge-success'
                            }`}>
                              {run.trigger_type === 'cron' ? (
                                <><Zap className="h-3 w-3" /> Cron</>
                              ) : (
                                <><Play className="h-3 w-3" /> Manuale</>
                              )}
                            </span>
                            {run.attempt > 1 && (
                              <span className="alt-badge alt-badge-warning">
                                Tentativo {run.attempt}
                              </span>
                            )}
                          </div>
                          <span className="text-sm font-medium alt-text-muted">
                            {formatDuration(run.runtime_ms)}
                          </span>
                        </div>
                        
                        {/* Error message with friendly text */}
                        {run.error_message && (
                          <div className="mt-2">
                            <p className="text-sm text-error font-medium">
                              {getFriendlyErrorMessage(run.error_message, run.error_details)}
                            </p>
                            
                            {/* Expandable technical details */}
                            {run.error_details && (
                              <Collapsible 
                                open={expandedLogDetails === run.id}
                                onOpenChange={(open) => setExpandedLogDetails(open ? run.id : null)}
                              >
                                <CollapsibleTrigger 
                                  className="text-xs alt-text-muted hover:text-foreground mt-1 flex items-center gap-1"
                                  onClick={(e) => e.stopPropagation()}
                                >
                                  {expandedLogDetails === run.id ? (
                                    <><ChevronDown className="h-3 w-3" /> Nascondi dettagli tecnici</>
                                  ) : (
                                    <><ChevronRight className="h-3 w-3" /> Mostra dettagli tecnici</>
                                  )}
                                </CollapsibleTrigger>
                                <CollapsibleContent onClick={(e) => e.stopPropagation()}>
                                  <pre className="mt-2 p-2 alt-info-box rounded text-xs overflow-x-auto max-h-32">
                                    {JSON.stringify(run.error_details, null, 2)}
                                  </pre>
                                </CollapsibleContent>
                              </Collapsible>
                            )}
                          </div>
                        )}
                      </div>
                    </div>
                  ))}
                  {runs.length === 0 && (
                    <div className="p-12 text-center alt-text-muted">
                      <FileText className="h-8 w-8 mx-auto mb-2 opacity-50" />
                      Nessuna sincronizzazione eseguita
                    </div>
                  )}
                </div>
              </ScrollArea>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Run Detail Dialog */}
      <Dialog open={!!selectedRun} onOpenChange={() => setSelectedRun(null)}>
        <DialogContent className="max-w-2xl max-h-[85vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-3 text-lg">
              Dettaglio sincronizzazione
              {selectedRun && getStatusBadge(selectedRun.status, 'md')}
            </DialogTitle>
            <DialogDescription className="text-slate-600">
              {selectedRun && new Date(selectedRun.started_at).toLocaleString('it-IT')}
              {selectedRun?.trigger_type === 'cron' ? ' (Automatica)' : ' (Manuale)'}
              {selectedRun?.attempt && selectedRun.attempt > 1 && ` - Tentativo ${selectedRun.attempt}`}
            </DialogDescription>
          </DialogHeader>

          {selectedRun && (
            <div className="space-y-6 mt-4">
              {/* Friendly error message */}
              {selectedRun.error_message && (
                <div className="bg-red-50 border border-red-200 rounded-lg p-4">
                  <p className="font-semibold text-red-800">
                    {getFriendlyErrorMessage(selectedRun.error_message, selectedRun.error_details)}
                  </p>
                </div>
              )}

              {/* Steps timeline */}
              <div>
                <h4 className="font-semibold text-slate-800 mb-3">Timeline degli step</h4>
                <div className="space-y-2">
                  {Object.entries(STEP_LABELS).map(([key, label]) => {
                    const step = selectedRun.steps?.[key];
                    return (
                      <div key={key} className={`flex items-center justify-between p-3 rounded-lg border ${
                        step?.status === 'success' ? 'bg-emerald-50 border-emerald-200' :
                        step?.status === 'failed' ? 'bg-red-50 border-red-200' :
                        step?.status === 'skipped' ? 'bg-amber-50 border-amber-200' :
                        'bg-slate-50 border-slate-200'
                      }`}>
                        <div className="flex items-center gap-3">
                          {step?.status === 'success' && <CheckCircle className="h-5 w-5 text-emerald-600" />}
                          {step?.status === 'failed' && <XCircle className="h-5 w-5 text-red-600" />}
                          {step?.status === 'skipped' && <AlertTriangle className="h-5 w-5 text-amber-600" />}
                          {!step && <div className="h-5 w-5 rounded-full bg-slate-200" />}
                          <span className={`font-medium ${!step ? 'text-slate-400' : 'text-slate-700'}`}>{label}</span>
                        </div>
                        <span className="text-sm font-medium text-slate-500">
                          {step ? formatDuration(step.duration_ms) : '-'}
                        </span>
                      </div>
                    );
                  })}
                </div>
              </div>

              {/* Metrics */}
              {selectedRun.metrics && (
                <div>
                  <h4 className="font-semibold text-slate-800 mb-3">Metriche</h4>
                  <div className="grid grid-cols-2 gap-2">
                    {[
                      { label: 'Prodotti totali', value: selectedRun.metrics.products_total },
                      { label: 'Prodotti elaborati', value: selectedRun.metrics.products_processed },
                      { label: 'EAN mappati', value: selectedRun.metrics.products_ean_mapped },
                      { label: 'EAN mancanti', value: selectedRun.metrics.products_ean_missing },
                      { label: 'Export Mediaworld', value: selectedRun.metrics.mediaworld_export_rows },
                      { label: 'Export ePrice', value: selectedRun.metrics.eprice_export_rows },
                      { label: 'File SFTP caricati', value: selectedRun.metrics.sftp_uploaded_files },
                    ].map(({ label, value }) => (
                      <div key={label} className="flex justify-between p-3 bg-slate-50 rounded-lg">
                        <span className="text-sm text-slate-600">{label}</span>
                        <span className="text-sm font-semibold text-slate-800">{value ?? 0}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Technical details - collapsible */}
              {selectedRun.error_details && (
                <Collapsible>
                  <CollapsibleTrigger className="flex items-center gap-2 text-sm font-medium text-slate-600 hover:text-slate-800">
                    <ChevronRight className="h-4 w-4" />
                    Dettagli tecnici
                  </CollapsibleTrigger>
                  <CollapsibleContent>
                    <pre className="mt-2 bg-slate-100 p-4 rounded-lg text-xs text-slate-600 overflow-x-auto">
                      {JSON.stringify(selectedRun.error_details, null, 2)}
                    </pre>
                  </CollapsibleContent>
                </Collapsible>
              )}

              {/* Steps details - collapsible */}
              {selectedRun.steps && Object.keys(selectedRun.steps).length > 0 && (
                <Collapsible>
                  <CollapsibleTrigger className="flex items-center gap-2 text-sm font-medium text-slate-600 hover:text-slate-800">
                    <ChevronRight className="h-4 w-4" />
                    Dettagli step (JSON)
                  </CollapsibleTrigger>
                  <CollapsibleContent>
                    <pre className="mt-2 bg-slate-100 p-4 rounded-lg text-xs text-slate-600 overflow-x-auto max-h-48">
                      {JSON.stringify(selectedRun.steps, null, 2)}
                    </pre>
                  </CollapsibleContent>
                </Collapsible>
              )}

              {/* Sync Events (ERROR/WARN) */}
              <div>
                <div className="flex items-center justify-between mb-3">
                  <h4 className="font-semibold text-slate-800">Eventi diagnostici</h4>
                  {runEvents.length > 0 && (
                    <Button
                      variant="ghost"
                      size="sm"
                      className="text-xs"
                      onClick={() => {
                        const safeEvents = runEvents.map(e => ({
                          level: e.level,
                          step: e.step,
                          message: e.message,
                          details: e.details,
                          time: e.created_at
                        }));
                        navigator.clipboard.writeText(JSON.stringify(safeEvents, null, 2));
                        toast({ title: 'Copiato', description: 'Dettagli eventi copiati negli appunti' });
                      }}
                    >
                      <Copy className="h-3 w-3 mr-1" />
                      Copia dettagli
                    </Button>
                  )}
                </div>
                {isLoadingEvents ? (
                  <div className="flex items-center gap-2 text-sm text-slate-500">
                    <Loader2 className="h-4 w-4 animate-spin" /> Caricamento eventi...
                  </div>
                ) : runEvents.length === 0 ? (
                  <p className="text-sm text-slate-500">Nessun evento registrato per questa run.</p>
                ) : (
                  <ScrollArea className="max-h-[200px]">
                    <div className="space-y-2">
                      {runEvents.map(event => (
                        <div key={event.id} className={`p-3 rounded-lg border text-sm ${
                          event.level === 'ERROR' ? 'bg-red-50 border-red-200' :
                          event.level === 'WARN' ? 'bg-amber-50 border-amber-200' :
                          'bg-slate-50 border-slate-200'
                        }`}>
                          <div className="flex items-center gap-2 mb-1">
                            <span className={`text-xs font-bold ${
                              event.level === 'ERROR' ? 'text-red-700' :
                              event.level === 'WARN' ? 'text-amber-700' :
                              'text-slate-600'
                            }`}>{event.level}</span>
                            {event.step && <span className="text-xs text-slate-500">[{STEP_LABELS[event.step] || event.step}]</span>}
                            <span className="text-xs text-slate-400 ml-auto">
                              {new Date(event.created_at).toLocaleTimeString('it-IT')}
                            </span>
                          </div>
                          <p className="text-slate-700">{event.message}</p>
                          {event.details && typeof event.details === 'object' && Object.keys(event.details as object).length > 0 && (
                            <pre className="mt-1 text-xs text-slate-500 overflow-x-auto">
                              {JSON.stringify(event.details, null, 2)}
                            </pre>
                          )}
                        </div>
                      ))}
                    </div>
                  </ScrollArea>
                )}
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </>
  );
};

export default SyncScheduler;
