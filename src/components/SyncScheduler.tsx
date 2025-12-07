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
  Zap
} from 'lucide-react';

interface SyncConfig {
  id: number;
  enabled: boolean;
  frequency_minutes: number;
  daily_time: string | null;
  max_retries: number;
  retry_delay_minutes: number;
  updated_at: string;
}

interface SyncRun {
  id: string;
  started_at: string;
  finished_at: string | null;
  status: 'running' | 'success' | 'failed' | 'timeout' | 'skipped';
  trigger_type: 'cron' | 'manual';
  attempt: number;
  runtime_ms: number | null;
  error_message: string | null;
  error_details: any;
  steps: Record<string, StepResult>;
  metrics: PipelineMetrics;
  cancel_requested: boolean;
  cancelled_by_user: boolean;
}

interface StepResult {
  status: 'success' | 'failed' | 'skipped';
  duration_ms: number;
  [key: string]: any;
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
  failed: 'Fallita',
  timeout: 'Timeout',
  skipped: 'Saltata'
};

const STEP_LABELS: Record<string, string> = {
  import_ftp: 'Import FTP',
  parse_merge: 'Parsing e Merge',
  ean_mapping: 'Mapping EAN',
  pricing: 'Calcolo Prezzi',
  override: 'Override Prodotti',
  export_ean: 'Export Catalogo EAN',
  export_mediaworld: 'Export Mediaworld',
  export_eprice: 'Export ePrice',
  upload_sftp: 'Upload SFTP'
};

// Helper function to get user-friendly error messages in Italian
const getFriendlyErrorMessage = (errorMessage: string | null, errorDetails: any): string => {
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
  
  // File/parsing errors
  if (msg.includes('parse') || msg.includes('parsing') || msg.includes('file')) {
    return 'Errore elaborazione file: formato non valido o file corrotto';
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
  
  // Return original message if no match (but trim it if too long)
  return errorMessage.length > 100 ? errorMessage.substring(0, 100) + '...' : errorMessage;
};

// Status badge with proper colors
const getStatusBadge = (status: string, size: 'sm' | 'md' = 'sm') => {
  const baseClasses = size === 'md' ? 'px-3 py-1.5 text-sm font-semibold' : 'px-2 py-1 text-xs font-medium';
  
  const statusStyles: Record<string, string> = {
    running: 'bg-blue-100 text-blue-800 border-blue-200',
    success: 'bg-emerald-100 text-emerald-800 border-emerald-200',
    failed: 'bg-red-100 text-red-800 border-red-200',
    timeout: 'bg-amber-100 text-amber-800 border-amber-200',
    skipped: 'bg-slate-100 text-slate-600 border-slate-200'
  };

  const icons: Record<string, React.ReactNode> = {
    running: <Loader2 className={`${size === 'md' ? 'h-4 w-4' : 'h-3 w-3'} animate-spin`} />,
    success: <CheckCircle className={size === 'md' ? 'h-4 w-4' : 'h-3 w-3'} />,
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
  const [selectedRun, setSelectedRun] = useState<SyncRun | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isSaving, setIsSaving] = useState(false);
  const [isStarting, setIsStarting] = useState(false);
  const [isStopping, setIsStopping] = useState(false);
  const [showLogs, setShowLogs] = useState(false);
  const [expandedLogDetails, setExpandedLogDetails] = useState<string | null>(null);

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

      const running = (runsData || []).find((r: any) => r.status === 'running');
      setCurrentRun(running as unknown as SyncRun || null);

    } catch (error: any) {
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

  const saveConfig = async (updates: Partial<SyncConfig>) => {
    if (!config) return;

    setIsSaving(true);
    try {
      const { error } = await supabase
        .from('sync_config')
        .update(updates)
        .eq('id', 1);

      if (error) throw error;

      setConfig({ ...config, ...updates });
      toast({
        title: 'Salvato',
        description: 'Configurazione aggiornata'
      });
    } catch (error: any) {
      console.error('Error saving config:', error);
      toast({
        title: 'Errore',
        description: 'Impossibile salvare la configurazione',
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

      const response = await supabase.functions.invoke('run-full-sync', {
        body: { trigger: 'manual' }
      });

      if (response.error) {
        throw new Error(response.error.message);
      }

      const data = response.data;

      if (data.status === 'error') {
        throw new Error(data.message);
      }

      toast({
        title: 'Sincronizzazione avviata',
        description: 'La pipeline è in esecuzione'
      });

      await loadData();

    } catch (error: any) {
      console.error('Error starting sync:', error);
      toast({
        title: 'Errore',
        description: error.message || 'Impossibile avviare la sincronizzazione',
        variant: 'destructive'
      });
    } finally {
      setIsStarting(false);
    }
  };

  const stopSync = async () => {
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
        title: 'Richiesta inviata',
        description: 'La sincronizzazione verrà interrotta al prossimo step'
      });

      await loadData();

    } catch (error: any) {
      console.error('Error stopping sync:', error);
      toast({
        title: 'Errore',
        description: error.message || 'Impossibile interrompere la sincronizzazione',
        variant: 'destructive'
      });
    } finally {
      setIsStopping(false);
    }
  };

  const getNextSyncTime = (): string | null => {
    if (!config?.enabled || runs.length === 0) return null;

    const lastCronRun = runs.find(r => r.trigger_type === 'cron' && r.attempt === 1);
    if (!lastCronRun) return 'Prossima esecuzione programmata';

    const lastStarted = new Date(lastCronRun.started_at);
    const nextRun = new Date(lastStarted.getTime() + config.frequency_minutes * 60 * 1000);

    if (config.frequency_minutes === 1440 && config.daily_time) {
      const [hours, minutes] = config.daily_time.split(':').map(Number);
      const today = new Date();
      today.setHours(hours, minutes, 0, 0);
      
      if (today <= new Date()) {
        today.setDate(today.getDate() + 1);
      }
      
      return today.toLocaleString('it-IT', {
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
      });
    }

    return nextRun.toLocaleString('it-IT', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  const hasMaxRetriesWarning = (): boolean => {
    const lastCronRun = runs.find(r => r.trigger_type === 'cron');
    return lastCronRun?.attempt === 5 && ['failed', 'timeout'].includes(lastCronRun.status);
  };

  const formatDuration = (ms: number | null): string => {
    if (!ms) return '-';
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
    return `${(ms / 60000).toFixed(1)}min`;
  };

  if (isLoading) {
    return (
      <Card className="mb-6 bg-slate-50 border-slate-200">
        <CardContent className="flex items-center justify-center py-12">
          <Loader2 className="h-8 w-8 animate-spin text-primary" />
        </CardContent>
      </Card>
    );
  }

  return (
    <>
      {/* Main Scheduling Card */}
      <Card className="mb-6 bg-slate-50/80 border-slate-200 shadow-sm">
        <CardHeader className="pb-4">
          <CardTitle className="flex items-center gap-2 text-xl font-bold text-slate-900">
            <RefreshCw className="h-5 w-5 text-primary" />
            Pianificazione della sincronizzazione
          </CardTitle>
          <CardDescription className="text-slate-600">
            Gestisci la sincronizzazione automatica dei cataloghi verso i marketplace
          </CardDescription>
        </CardHeader>

        <CardContent className="space-y-6">
          {/* Warning for max retries */}
          {hasMaxRetriesWarning() && (
            <div className="bg-red-50 border-2 border-red-200 rounded-lg p-4 flex items-start gap-3">
              <AlertTriangle className="h-5 w-5 text-red-600 flex-shrink-0 mt-0.5" />
              <div>
                <p className="font-semibold text-red-800">
                  Attenzione: la sincronizzazione automatica ha fallito 5 volte consecutive.
                </p>
                <p className="text-sm text-red-600 mt-1">
                  Controlla i log per verificare il problema e correggerlo.
                </p>
                <Button
                  variant="link"
                  className="p-0 h-auto text-red-700 hover:text-red-900 font-medium"
                  onClick={() => {
                    const failedRun = runs.find(r => r.trigger_type === 'cron' && r.attempt === 5);
                    if (failedRun) setSelectedRun(failedRun);
                  }}
                >
                  Visualizza ultimo job fallito →
                </Button>
              </div>
            </div>
          )}

          {/* Configuration Section */}
          <div className="bg-white rounded-lg border border-slate-200 p-5">
            <h3 className="text-sm font-semibold text-slate-700 uppercase tracking-wide mb-4">Configurazione</h3>
            
            <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
              {/* Enable/Disable Toggle */}
              <div className="space-y-3">
                <div className="flex items-center gap-3">
                  <Switch
                    id="sync-enabled"
                    checked={config?.enabled || false}
                    onCheckedChange={(checked) => saveConfig({ enabled: checked })}
                    disabled={isSaving}
                    className="data-[state=checked]:bg-emerald-500"
                  />
                  <div>
                    <Label htmlFor="sync-enabled" className="font-semibold text-slate-800 cursor-pointer">
                      Sincronizzazione automatica
                    </Label>
                    <span className={`ml-2 inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${
                      config?.enabled 
                        ? 'bg-emerald-100 text-emerald-700' 
                        : 'bg-slate-100 text-slate-600'
                    }`}>
                      {config?.enabled ? 'Attivata' : 'Disattivata'}
                    </span>
                  </div>
                </div>
                <p className="text-xs text-slate-500 leading-relaxed">
                  Quando attiva, la pipeline viene eseguita automaticamente in base alla frequenza scelta.
                </p>
              </div>

              {/* Frequency */}
              <div className="space-y-2">
                <Label className="font-semibold text-slate-700">Frequenza</Label>
                <Select
                  value={String(config?.frequency_minutes || 60)}
                  onValueChange={(value) => saveConfig({ frequency_minutes: parseInt(value) })}
                  disabled={isSaving}
                >
                  <SelectTrigger className="bg-white border-slate-300 focus:border-primary focus:ring-primary">
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

              {/* Daily time (only for daily frequency) */}
              {config?.frequency_minutes === 1440 && (
                <div className="space-y-2">
                  <Label className="font-semibold text-slate-700">Orario giornaliero</Label>
                  <Input
                    type="time"
                    value={config?.daily_time || '08:00'}
                    onChange={(e) => saveConfig({ daily_time: e.target.value })}
                    disabled={isSaving}
                    className="bg-white border-slate-300 focus:border-primary focus:ring-primary"
                  />
                </div>
              )}
            </div>
          </div>

          {/* Status Summary Section */}
          <div className="grid gap-4 md:grid-cols-3">
            {/* Last sync */}
            <div className="bg-white rounded-lg border border-slate-200 p-4">
              <div className="flex items-center gap-2 mb-2">
                <Activity className="h-4 w-4 text-slate-500" />
                <span className="text-xs font-semibold text-slate-500 uppercase tracking-wide">Ultima sincronizzazione</span>
              </div>
              {runs.length > 0 ? (
                <div className="space-y-2">
                  <div className="flex items-center gap-2">
                    {getStatusBadge(runs[0].status, 'md')}
                  </div>
                  <p className="text-sm text-slate-600">
                    {new Date(runs[0].started_at).toLocaleString('it-IT', {
                      day: '2-digit',
                      month: '2-digit',
                      hour: '2-digit',
                      minute: '2-digit'
                    })}
                  </p>
                  {runs[0].error_message && runs[0].status === 'failed' && (
                    <p className="text-xs text-red-600 bg-red-50 px-2 py-1 rounded">
                      {getFriendlyErrorMessage(runs[0].error_message, runs[0].error_details)}
                    </p>
                  )}
                </div>
              ) : (
                <p className="text-sm text-slate-400">Nessuna esecuzione</p>
              )}
            </div>

            {/* Metrics summary */}
            <div className="bg-white rounded-lg border border-slate-200 p-4">
              <div className="flex items-center gap-2 mb-2">
                <Server className="h-4 w-4 text-slate-500" />
                <span className="text-xs font-semibold text-slate-500 uppercase tracking-wide">Riepilogo</span>
              </div>
              {runs.length > 0 && runs[0].metrics ? (
                <div className="space-y-1">
                  <div className="flex justify-between">
                    <span className="text-sm text-slate-500">Prodotti</span>
                    <span className="text-sm font-semibold text-slate-800">{runs[0].metrics.products_processed || 0}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-sm text-slate-500">File caricati</span>
                    <span className="text-sm font-semibold text-slate-800">{runs[0].metrics.sftp_uploaded_files || 0}</span>
                  </div>
                </div>
              ) : (
                <p className="text-sm text-slate-400">-</p>
              )}
            </div>

            {/* Next sync */}
            <div className="bg-white rounded-lg border border-slate-200 p-4">
              <div className="flex items-center gap-2 mb-2">
                <Calendar className="h-4 w-4 text-slate-500" />
                <span className="text-xs font-semibold text-slate-500 uppercase tracking-wide">Prossima sincronizzazione</span>
              </div>
              {config?.enabled ? (
                <p className="text-sm font-medium text-slate-700">{getNextSyncTime() || '-'}</p>
              ) : (
                <span className="inline-flex items-center px-2 py-1 rounded text-xs font-medium bg-slate-100 text-slate-500">
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
            
            <Button
              variant="outline"
              onClick={stopSync}
              disabled={isStopping || !currentRun}
              className="border-red-300 text-red-600 hover:bg-red-50 hover:border-red-400 font-semibold disabled:border-slate-200 disabled:text-slate-400 disabled:bg-transparent"
            >
              {isStopping ? (
                <Loader2 className="h-4 w-4 animate-spin mr-2" />
              ) : (
                <Square className="h-4 w-4 mr-2" />
              )}
              Ferma sincronizzazione
            </Button>

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

          {/* Sync Logs */}
          {showLogs && (
            <div className="bg-white rounded-lg border border-slate-200 overflow-hidden">
              <div className="px-4 py-3 border-b border-slate-200 bg-slate-50">
                <h4 className="font-semibold text-slate-800">Log sincronizzazioni</h4>
              </div>
              <ScrollArea className="h-[350px]">
                <div className="divide-y divide-slate-100">
                  {runs.map(run => (
                    <div key={run.id} className="hover:bg-slate-50 transition-colors">
                      <div
                        className="p-4 cursor-pointer"
                        onClick={() => setSelectedRun(run)}
                      >
                        <div className="flex flex-wrap items-center justify-between gap-2">
                          <div className="flex flex-wrap items-center gap-2">
                            {getStatusBadge(run.status)}
                            <span className="text-sm font-medium text-slate-700">
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
                                ? 'bg-purple-100 text-purple-700' 
                                : 'bg-blue-100 text-blue-700'
                            }`}>
                              {run.trigger_type === 'cron' ? (
                                <><Zap className="h-3 w-3" /> Cron</>
                              ) : (
                                <><Play className="h-3 w-3" /> Manuale</>
                              )}
                            </span>
                            {run.attempt > 1 && (
                              <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-amber-100 text-amber-700">
                                Tentativo {run.attempt}
                              </span>
                            )}
                          </div>
                          <span className="text-sm font-medium text-slate-500">
                            {formatDuration(run.runtime_ms)}
                          </span>
                        </div>
                        
                        {/* Error message with friendly text */}
                        {run.error_message && (
                          <div className="mt-2">
                            <p className="text-sm text-red-600 font-medium">
                              {getFriendlyErrorMessage(run.error_message, run.error_details)}
                            </p>
                            
                            {/* Expandable technical details */}
                            {run.error_details && (
                              <Collapsible 
                                open={expandedLogDetails === run.id}
                                onOpenChange={(open) => setExpandedLogDetails(open ? run.id : null)}
                              >
                                <CollapsibleTrigger 
                                  className="text-xs text-slate-500 hover:text-slate-700 mt-1 flex items-center gap-1"
                                  onClick={(e) => e.stopPropagation()}
                                >
                                  {expandedLogDetails === run.id ? (
                                    <><ChevronDown className="h-3 w-3" /> Nascondi dettagli tecnici</>
                                  ) : (
                                    <><ChevronRight className="h-3 w-3" /> Mostra dettagli tecnici</>
                                  )}
                                </CollapsibleTrigger>
                                <CollapsibleContent onClick={(e) => e.stopPropagation()}>
                                  <pre className="mt-2 p-2 bg-slate-100 rounded text-xs text-slate-600 overflow-x-auto max-h-32">
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
                    <div className="p-12 text-center text-slate-400">
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
            </div>
          )}
        </DialogContent>
      </Dialog>
    </>
  );
};

export default SyncScheduler;
