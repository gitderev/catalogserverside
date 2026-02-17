import React, { useState, useEffect, useCallback, useMemo } from 'react';
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
  Clock, Play, Square, RefreshCw, CheckCircle, XCircle, AlertTriangle, Loader2,
  Calendar, Activity, FileText, Upload, ChevronDown, ChevronRight, Server, Zap, Copy
} from 'lucide-react';
import { SyncHealthPanel } from '@/components/SyncHealthPanel';
import { STEP_LABELS, EXPECTED_STEPS, assertExpectedSteps } from '@/shared/expectedSteps';
import { formatTimestamp, formatTimestampShort } from '@/shared/formatTimestamp';
import {
  SyncRunRecord, classifyRun, getLastRunAll, getLastRunByTrigger,
  deriveAutoDisableInfo, sortRunsDeterministically
} from '@/shared/syncDerivations';

// Runtime assert on mount (non-blocking)
assertExpectedSteps();

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

// Helper function to get user-friendly error messages in Italian
const getFriendlyErrorMessage = (run: SyncRunRecord): string => {
  const classification = classifyRun(run);
  
  // Use classifyRun's deterministic classification
  if (classification.isCancelled) {
    return 'Sincronizzazione interrotta manualmente dall\'utente';
  }
  
  return classification.displayReason || run.error_message || 'Errore sconosciuto';
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

const TriggerBadge: React.FC<{ trigger: string }> = ({ trigger }) => (
  <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium ${
    trigger === 'cron' ? 'alt-badge alt-badge-info' : 'alt-badge alt-badge-success'
  }`}>
    {trigger === 'cron' ? <><Zap className="h-3 w-3" /> Automatica</> : <><Play className="h-3 w-3" /> Manuale</>}
  </span>
);

export const SyncScheduler: React.FC = () => {
  const [config, setConfig] = useState<SyncConfig | null>(null);
  const [runs, setRuns] = useState<SyncRunRecord[]>([]);
  const [currentRun, setCurrentRun] = useState<SyncRunRecord | null>(null);
  const [isStaleRun, setIsStaleRun] = useState(false);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [isStarting, setIsStarting] = useState(false);
  const [isStopping, setIsStopping] = useState(false);
  const [isResetting, setIsResetting] = useState(false);
  const [showLogs, setShowLogs] = useState(false);
  const [expandedLogDetails, setExpandedLogDetails] = useState<string | null>(null);
  const [runEvents, setRunEvents] = useState<Array<{ id: string; level: string; message: string; details: unknown; created_at: string; step: string | null }>>([]);
  const [isLoadingEvents, setIsLoadingEvents] = useState(false);

  // Derive selectedRun from runs by ID (always fresh)
  const selectedRun = useMemo(() => {
    if (!selectedRunId) return null;
    return runs.find(r => r.id === selectedRunId) || null;
  }, [selectedRunId, runs]);

  // Derive last run (any trigger) and last cron run using deterministic sort
  const lastRunAll = useMemo(() => getLastRunAll(runs), [runs]);
  const lastCronRun = useMemo(() => getLastRunByTrigger(runs, 'cron'), [runs]);

  // Derive auto-disable banner info deterministically
  const autoDisableInfo = useMemo(() => {
    if (!config) return { shouldShowBanner: false, streak: 0, maxAttempts: 3, targetRunId: null, reason: null };
    const cronRuns = runs.filter(r => r.trigger_type === 'cron');
    return deriveAutoDisableInfo(config, cronRuns);
  }, [config, runs]);

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
        .limit(50);

      if (runsError) throw runsError;
      
      setRuns((runsData || []) as unknown as SyncRunRecord[]);

      // Find running run
      const running = (runsData || []).find((r: Record<string, unknown>) => r.status === 'running');
      
      if (running) {
        const staleThresholdMs = ((configData as SyncConfig)?.run_timeout_minutes || 60) * 60 * 1000;
        const startedAt = new Date(running.started_at as string).getTime();
        const elapsed = Date.now() - startedAt;
        const isStale = elapsed > staleThresholdMs;
        
        setCurrentRun(running as unknown as SyncRunRecord);
        setIsStaleRun(isStale);
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

  // Adaptive polling: 10s if running, 60s if idle
  useEffect(() => {
    loadData();
    const intervalMs = currentRun ? 10_000 : 60_000;
    const interval = setInterval(loadData, intervalMs);
    return () => clearInterval(interval);
  }, [loadData, !!currentRun]);

  // Load sync_events when a run is selected (by ID)
  useEffect(() => {
    if (!selectedRunId) {
      setRunEvents([]);
      return;
    }
    const loadEvents = async () => {
      setIsLoadingEvents(true);
      try {
        const { data, error } = await supabase
          .from('sync_events')
          .select('id, level, message, details, created_at, step')
          .eq('run_id', selectedRunId)
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
  }, [selectedRunId]);

  const saveConfig = async (updates: Partial<SyncConfig>) => {
    if (!config) {
      toast({ title: 'Errore', description: 'Configurazione non ancora caricata', variant: 'destructive' });
      return;
    }

    // Validate max_attempts <= 5
    if (updates.max_attempts !== undefined && updates.max_attempts > 5) {
      updates.max_attempts = 5;
    }

    const previousConfig = { ...config };
    setConfig({ ...config, ...updates });
    setIsSaving(true);

    try {
      const { error } = await supabase
        .from('sync_config')
        .update(updates)
        .eq('id', 1);

      if (error) throw error;

      toast({ title: 'Salvato', description: 'Configurazione aggiornata' });
      // Refetch after config change
      await loadData();
    } catch (error: unknown) {
      setConfig(previousConfig);
      const supaError = error as { code?: string; message?: string };
      console.error('Error saving config:', supaError?.code, supaError?.message);
      toast({
        title: 'Errore salvataggio',
        description: `Impossibile salvare: ${supaError?.message || 'Errore sconosciuto'}`,
        variant: 'destructive'
      });
    } finally {
      setIsSaving(false);
    }
  };

  const startSync = async () => {
    setIsStarting(true);
    try {
      const { data: { session } } = await supabase.auth.getSession();
      if (!session?.access_token) {
        throw new Error('Sessione non valida');
      }

      const response = await supabase.functions.invoke('run-full-sync', {
        body: { trigger: 'manual' }
      });

      if (response.error) throw new Error(response.error.message);

      const data = response.data;
      if (data.status === 'error') throw new Error(data.message);

      if (data.status === 'yielded' && data.reason === 'locked') {
        toast({
          title: 'Sincronizzazione in corso',
          description: data.run_id 
            ? `Run ${data.run_id.substring(0, 8)}… già in esecuzione` 
            : 'Attendi il completamento della sincronizzazione corrente',
        });
      } else if (data.status === 'resumed' || data.status === 'yielded') {
        toast({
          title: data.status === 'resumed' ? 'Ripresa sincronizzazione' : 'Sincronizzazione in pausa',
          description: data.status === 'resumed'
            ? `Ripresa run esistente ${(data.run_id || '').substring(0, 8)}…`
            : `Run ${(data.run_id || '').substring(0, 8)}… in pausa, verrà ripresa dal cron`,
        });
      } else {
        toast({
          title: 'Sincronizzazione avviata',
          description: 'La pipeline è in esecuzione'
        });
      }

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
      toast({ title: 'Nessuna sync in corso', description: 'Non c\'è alcuna sincronizzazione da interrompere' });
      return;
    }

    setIsStopping(true);
    try {
      const response = await supabase.functions.invoke('stop-sync', {
        body: { run_id: currentRun.id, force }
      });
      if (response.error) throw new Error(response.error.message);
      const data = response.data;
      if (data.status === 'error') throw new Error(data.message);

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

  const forceResetSync = async () => {
    if (!currentRun) {
      toast({ title: 'Nessuna sync da resettare', description: 'Non c\'è alcuna sincronizzazione bloccata' });
      return;
    }

    setIsResetting(true);
    try {
      const response = await supabase.functions.invoke('force-reset-sync', {
        body: { run_id: currentRun.id }
      });
      if (response.error) throw new Error(response.error.message);
      const data = response.data;
      if (data.status === 'error') throw new Error(data.message);

      toast({ title: 'Reset completato', description: 'La sincronizzazione bloccata è stata resettata' });
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
      const romeNow = new Date(now.toLocaleString('en-US', { timeZone: 'Europe/Rome' }));
      const target = new Date(romeNow);
      target.setHours(hours, minutes, 0, 0);
      if (target <= romeNow) target.setDate(target.getDate() + 1);
      return `${target.toLocaleDateString('it-IT')} alle ${dailyTime} (Europe/Rome)`;
    }

    // Hours mode
    const lastCron = runs.find(r => r.trigger_type === 'cron' && r.attempt === 1);
    if (!lastCron) return 'Prossima esecuzione programmata';

    const lastStarted = new Date(lastCron.started_at);
    const nextRun = new Date(lastStarted.getTime() + config.frequency_minutes * 60 * 1000);
    return formatTimestampShort(nextRun.toISOString());
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
          {/* Auto-disable banner: deterministic, only for auto-disable, points to correct run */}
          {autoDisableInfo.shouldShowBanner && (
            <div className="alt-alert alt-alert-error">
              <AlertTriangle className="h-5 w-5 text-error flex-shrink-0 mt-0.5" />
              <div>
                <p className="font-semibold text-error">
                  Sincronizzazione automatica disabilitata: {autoDisableInfo.streak} fallimenti cron consecutivi (soglia: {autoDisableInfo.maxAttempts}).
                </p>
                <p className="text-sm alt-text-muted mt-1">
                  Controlla i log per verificare il problema e correggerlo, poi riabilita lo scheduler.
                </p>
                {autoDisableInfo.targetRunId && (
                  <Button
                    variant="link"
                    className="p-0 h-auto text-error hover:text-error/80 font-medium"
                    onClick={() => setSelectedRunId(autoDisableInfo.targetRunId)}
                  >
                    Visualizza ultimo job fallito →
                  </Button>
                )}
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
                    onCheckedChange={(checked: boolean) => saveConfig({ enabled: checked })}
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
                        <SelectItem key={opt.value} value={opt.value}>{opt.label}</SelectItem>
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
            <div className="space-y-3">
              <div className="alt-alert alt-alert-info">
                <Loader2 className="h-5 w-5 animate-spin text-info" />
                <div>
                  <p className="font-semibold text-info">Sincronizzazione in corso</p>
                  <p className="text-sm alt-text-muted">
                    Step corrente: {STEP_LABELS[String((currentRun.steps as Record<string, unknown>)?.current_step ?? '')] || String((currentRun.steps as Record<string, unknown>)?.current_step ?? '') || 'Avvio...'}
                  </p>
                </div>
              </div>
              <SyncHealthPanel
                runId={currentRun.id}
                runStartedAt={currentRun.started_at}
                runStatus={currentRun.status}
                steps={currentRun.steps as Record<string, unknown>}
              />
            </div>
          )}

          {/* Status Summary Section */}
          <div className="grid gap-4 md:grid-cols-3">
            {/* Last sync (any trigger) */}
            <div className="alt-panel rounded-lg p-4">
              <div className="flex items-center gap-2 mb-2">
                <Activity className="h-4 w-4 alt-text-muted" />
                <span className="text-xs font-semibold alt-text-muted uppercase tracking-wide">Ultima sincronizzazione</span>
              </div>
              {lastRunAll ? (
                <div className="space-y-2">
                  <div className="flex items-center gap-2 flex-wrap">
                    {getStatusBadge(lastRunAll.status, 'md')}
                    <TriggerBadge trigger={lastRunAll.trigger_type} />
                  </div>
                  <p className="text-sm alt-text-muted">
                    {formatTimestampShort(lastRunAll.started_at)}
                  </p>
                  {lastRunAll.status === 'failed' && (
                    <p className="text-xs text-error alt-info-box px-2 py-1 rounded">
                      {getFriendlyErrorMessage(lastRunAll)}
                    </p>
                  )}
                  {/* Show last cron run if different from last run overall */}
                  {lastCronRun && lastCronRun.id !== lastRunAll.id && (
                    <div className="mt-2 pt-2 border-t border-border">
                      <p className="text-xs alt-text-muted mb-1">Ultima automatica:</p>
                      <div className="flex items-center gap-2 flex-wrap">
                        {getStatusBadge(lastCronRun.status)}
                        <span className="text-xs alt-text-muted">{formatTimestampShort(lastCronRun.started_at)}</span>
                      </div>
                    </div>
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
              {lastRunAll && lastRunAll.metrics ? (
                <div className="space-y-1">
                  <div className="flex justify-between">
                    <span className="text-sm alt-text-muted">Prodotti</span>
                    <span className="text-sm font-semibold">{(lastRunAll.metrics as Record<string, unknown>).products_processed as number || 0}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-sm alt-text-muted">File caricati</span>
                    <span className="text-sm font-semibold">{(lastRunAll.metrics as Record<string, unknown>).sftp_uploaded_files as number || 0}</span>
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
                <span className="alt-badge alt-badge-idle">Disattivata</span>
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
              {isStarting ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Play className="h-4 w-4 mr-2" />}
              Esegui sincronizzazione ora
            </Button>
            
            {isStaleRun && currentRun ? (
              <Button
                variant="outline"
                onClick={forceResetSync}
                disabled={isResetting}
                className="border-warning text-warning hover:bg-warning/10 hover:border-warning font-semibold"
              >
                {isResetting ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <AlertTriangle className="h-4 w-4 mr-2" />}
                Sblocca sincronizzazione
              </Button>
            ) : (
              <Button
                variant="outline"
                onClick={() => stopSync(false)}
                disabled={isStopping || !currentRun}
                className="border-error/50 text-error hover:bg-error/10 hover:border-error font-semibold disabled:border-muted disabled:text-muted disabled:bg-transparent"
              >
                {isStopping ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : <Square className="h-4 w-4 mr-2" />}
                Ferma sincronizzazione
              </Button>
            )}

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
                <p className="font-semibold text-warning">Sincronizzazione bloccata</p>
                <p className="text-sm alt-text-muted mt-1">
                   La sincronizzazione è in esecuzione da più di {config?.run_timeout_minutes || 60} minuti e potrebbe essere bloccata.
                   Usa il pulsante "Sblocca sincronizzazione" per resettare lo stato.
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
                  {runs.map(run => {
                    const classification = classifyRun(run);
                    return (
                      <div key={run.id} className="hover:bg-muted/10 transition-colors">
                        <div
                          className="p-4 cursor-pointer"
                          onClick={() => setSelectedRunId(run.id)}
                        >
                          <div className="flex flex-wrap items-center justify-between gap-2">
                            <div className="flex flex-wrap items-center gap-2">
                              {getStatusBadge(run.status)}
                              <span className="text-sm font-medium">
                                {formatTimestampShort(run.started_at)}
                              </span>
                              <TriggerBadge trigger={run.trigger_type} />
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
                          
                          {/* Error message with deterministic classification */}
                          {run.status !== 'running' && run.status !== 'success' && run.error_message && (
                            <div className="mt-2">
                              <p className="text-sm text-error font-medium">
                                {getFriendlyErrorMessage(run)}
                              </p>
                              
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
                    );
                  })}
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

      {/* Run Detail Dialog - always opens by explicit run_id */}
      <Dialog open={!!selectedRun} onOpenChange={() => setSelectedRunId(null)}>
        <DialogContent className="max-w-2xl max-h-[85vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-3 text-lg">
              Dettaglio sincronizzazione
              {selectedRun && getStatusBadge(selectedRun.status, 'md')}
            </DialogTitle>
            <DialogDescription className="text-slate-600">
              {selectedRun && (
                <>
                  {formatTimestamp(selectedRun.started_at)}
                  {' — '}
                  <TriggerBadge trigger={selectedRun.trigger_type} />
                  {selectedRun.attempt > 1 && ` — Tentativo ${selectedRun.attempt}`}
                </>
              )}
            </DialogDescription>
          </DialogHeader>

          {selectedRun && (
            <div className="space-y-6 mt-4">
              {/* Error message using deterministic classification */}
              {selectedRun.error_message && (() => {
                const classification = classifyRun(selectedRun);
                return (
                  <div className="bg-red-50 border border-red-200 rounded-lg p-4">
                    <p className="font-semibold text-red-800">
                      {classification.isCancelled
                        ? 'Sincronizzazione interrotta manualmente dall\'utente'
                        : classification.displayReason || selectedRun.error_message}
                    </p>
                  </div>
                );
              })()}

              {/* Steps timeline - always 13 steps */}
              <div>
                <h4 className="font-semibold text-slate-800 mb-3">Timeline degli step</h4>
                <div className="space-y-2">
                  {EXPECTED_STEPS.map((key) => {
                    const step = selectedRun.steps?.[key] as Record<string, unknown> | undefined;
                    const st = String(step?.status || '');
                    return (
                      <div key={key} className={`flex items-center justify-between p-3 rounded-lg border ${
                        (st === 'success' || st === 'completed') ? 'bg-emerald-50 border-emerald-200' :
                        st === 'failed' ? 'bg-red-50 border-red-200' :
                        st === 'skipped' ? 'bg-amber-50 border-amber-200' :
                        (st === 'in_progress' || st === 'retry_delay') ? 'bg-blue-50 border-blue-200' :
                        'bg-slate-50 border-slate-200'
                      }`}>
                        <div className="flex items-center gap-3">
                          {(st === 'success' || st === 'completed') && <CheckCircle className="h-5 w-5 text-emerald-600" />}
                          {st === 'failed' && <XCircle className="h-5 w-5 text-red-600" />}
                          {st === 'skipped' && <AlertTriangle className="h-5 w-5 text-amber-600" />}
                          {(st === 'in_progress' || st === 'retry_delay') && <div className="h-5 w-5 rounded-full bg-blue-400 animate-pulse" />}
                          {!step && <div className="h-5 w-5 rounded-full bg-slate-200" />}
                          <span className={`font-medium ${!step ? 'text-slate-400' : 'text-slate-700'}`}>
                            {STEP_LABELS[key]}
                            {!step && <span className="text-xs text-slate-400 ml-2">(Non eseguito)</span>}
                          </span>
                        </div>
                        <span className="text-sm font-medium text-slate-500">
                          {step ? formatDuration(step.duration_ms as number) : '-'}
                        </span>
                      </div>
                    );
                  })}
                </div>
              </div>

              {/* Metrics */}
              {selectedRun.metrics && Object.keys(selectedRun.metrics).length > 0 && (
                <div>
                  <h4 className="font-semibold text-slate-800 mb-3">Metriche</h4>
                  <div className="grid grid-cols-2 gap-2">
                    {[
                      { label: 'Prodotti totali', value: (selectedRun.metrics as Record<string, unknown>).products_total },
                      { label: 'Prodotti elaborati', value: (selectedRun.metrics as Record<string, unknown>).products_processed },
                      { label: 'EAN mappati', value: (selectedRun.metrics as Record<string, unknown>).products_ean_mapped },
                      { label: 'EAN mancanti', value: (selectedRun.metrics as Record<string, unknown>).products_ean_missing },
                      { label: 'Export Mediaworld', value: (selectedRun.metrics as Record<string, unknown>).mediaworld_export_rows },
                      { label: 'Export ePrice', value: (selectedRun.metrics as Record<string, unknown>).eprice_export_rows },
                      { label: 'File SFTP caricati', value: (selectedRun.metrics as Record<string, unknown>).sftp_uploaded_files },
                    ].map(({ label, value }) => (
                      <div key={label} className="flex justify-between p-3 bg-slate-50 rounded-lg">
                        <span className="text-sm text-slate-600">{label}</span>
                        <span className="text-sm font-semibold text-slate-800">{(value as number) ?? 0}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Technical details */}
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

              {/* Steps JSON */}
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

              {/* Sync Events */}
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
                          level: e.level, step: e.step, message: e.message,
                          details: e.details, time: e.created_at
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
                              {formatTimestamp(event.created_at)}
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
