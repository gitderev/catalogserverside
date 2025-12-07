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
  ChevronRight
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

  // Load config and runs
  const loadData = useCallback(async () => {
    try {
      // Load config
      const { data: configData, error: configError } = await supabase
        .from('sync_config')
        .select('*')
        .eq('id', 1)
        .maybeSingle();

      if (configError) throw configError;
      
      if (configData) {
        setConfig(configData as SyncConfig);
      }

      // Load recent runs
      const { data: runsData, error: runsError } = await supabase
        .from('sync_runs')
        .select('*')
        .order('started_at', { ascending: false })
        .limit(20);

      if (runsError) throw runsError;
      
      setRuns((runsData || []) as unknown as SyncRun[]);

      // Find current running job
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
    
    // Poll for updates every 10 seconds
    const interval = setInterval(loadData, 10000);
    return () => clearInterval(interval);
  }, [loadData]);

  // Save config
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

  // Start manual sync
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

      // Reload data to show the new run
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

  // Stop running sync
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

      // Reload data
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

  // Calculate next sync time
  const getNextSyncTime = (): string | null => {
    if (!config?.enabled || runs.length === 0) return null;

    const lastCronRun = runs.find(r => r.trigger_type === 'cron' && r.attempt === 1);
    if (!lastCronRun) return 'Prossima esecuzione programmata';

    const lastStarted = new Date(lastCronRun.started_at);
    const nextRun = new Date(lastStarted.getTime() + config.frequency_minutes * 60 * 1000);

    // For daily runs, adjust to daily_time
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

  // Check for max retries warning
  const hasMaxRetriesWarning = (): boolean => {
    const lastCronRun = runs.find(r => r.trigger_type === 'cron');
    return lastCronRun?.attempt === 5 && ['failed', 'timeout'].includes(lastCronRun.status);
  };

  // Format duration
  const formatDuration = (ms: number | null): string => {
    if (!ms) return '-';
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
    return `${(ms / 60000).toFixed(1)}min`;
  };

  // Get status badge
  const getStatusBadge = (status: string) => {
    const variants: Record<string, 'default' | 'secondary' | 'destructive' | 'outline'> = {
      running: 'default',
      success: 'secondary',
      failed: 'destructive',
      timeout: 'destructive',
      skipped: 'outline'
    };

    const icons: Record<string, React.ReactNode> = {
      running: <Loader2 className="h-3 w-3 animate-spin mr-1" />,
      success: <CheckCircle className="h-3 w-3 mr-1" />,
      failed: <XCircle className="h-3 w-3 mr-1" />,
      timeout: <Clock className="h-3 w-3 mr-1" />,
      skipped: <AlertTriangle className="h-3 w-3 mr-1" />
    };

    return (
      <Badge variant={variants[status] || 'outline'} className="flex items-center">
        {icons[status]}
        {STATUS_LABELS[status] || status}
      </Badge>
    );
  };

  if (isLoading) {
    return (
      <Card className="mb-6">
        <CardContent className="flex items-center justify-center py-8">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        </CardContent>
      </Card>
    );
  }

  return (
    <>
      <Card className="mb-6">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <RefreshCw className="h-5 w-5" />
            Pianificazione della sincronizzazione
          </CardTitle>
          <CardDescription>
            Gestisci la sincronizzazione automatica dei cataloghi
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-6">
          {/* Warning for max retries */}
          {hasMaxRetriesWarning() && (
            <div className="bg-destructive/10 border border-destructive/20 rounded-lg p-4 flex items-start gap-3">
              <AlertTriangle className="h-5 w-5 text-destructive flex-shrink-0 mt-0.5" />
              <div>
                <p className="font-medium text-destructive">
                  Attenzione: la sincronizzazione automatica ha fallito 5 volte consecutive.
                </p>
                <p className="text-sm text-muted-foreground mt-1">
                  Controlla i log per maggiori dettagli.
                </p>
                <Button
                  variant="link"
                  className="p-0 h-auto text-destructive"
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

          {/* Configuration */}
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
            {/* Enable/Disable */}
            <div className="flex items-center space-x-2">
              <Switch
                id="sync-enabled"
                checked={config?.enabled || false}
                onCheckedChange={(checked) => saveConfig({ enabled: checked })}
                disabled={isSaving}
              />
              <Label htmlFor="sync-enabled" className="font-medium">
                Sincronizzazione automatica
              </Label>
            </div>

            {/* Frequency */}
            <div className="space-y-2">
              <Label>Frequenza</Label>
              <Select
                value={String(config?.frequency_minutes || 60)}
                onValueChange={(value) => saveConfig({ frequency_minutes: parseInt(value) })}
                disabled={isSaving}
              >
                <SelectTrigger>
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
                <Label>Orario giornaliero</Label>
                <Input
                  type="time"
                  value={config?.daily_time || '08:00'}
                  onChange={(e) => saveConfig({ daily_time: e.target.value })}
                  disabled={isSaving}
                />
              </div>
            )}
          </div>

          <Separator />

          {/* Status Box */}
          <div className="grid gap-4 md:grid-cols-3">
            {/* Last sync */}
            <div className="space-y-1">
              <p className="text-sm font-medium text-muted-foreground">Ultima sincronizzazione</p>
              {runs.length > 0 ? (
                <div className="flex items-center gap-2">
                  {getStatusBadge(runs[0].status)}
                  <span className="text-sm">
                    {new Date(runs[0].started_at).toLocaleString('it-IT', {
                      day: '2-digit',
                      month: '2-digit',
                      hour: '2-digit',
                      minute: '2-digit'
                    })}
                  </span>
                </div>
              ) : (
                <p className="text-sm text-muted-foreground">Nessuna esecuzione</p>
              )}
            </div>

            {/* Metrics summary */}
            <div className="space-y-1">
              <p className="text-sm font-medium text-muted-foreground">Riepilogo</p>
              {runs.length > 0 && runs[0].metrics ? (
                <div className="text-sm space-y-0.5">
                  <p>Prodotti: {runs[0].metrics.products_processed || 0}</p>
                  <p>File caricati: {runs[0].metrics.sftp_uploaded_files || 0}</p>
                </div>
              ) : (
                <p className="text-sm text-muted-foreground">-</p>
              )}
            </div>

            {/* Next sync */}
            <div className="space-y-1">
              <p className="text-sm font-medium text-muted-foreground">Prossima sincronizzazione</p>
              <p className="text-sm">
                {config?.enabled ? getNextSyncTime() || '-' : 'Disattivata'}
              </p>
            </div>
          </div>

          <Separator />

          {/* Action Buttons */}
          <div className="flex gap-3">
            <Button
              onClick={startSync}
              disabled={isStarting || !!currentRun}
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
            >
              <FileText className="h-4 w-4 mr-2" />
              {showLogs ? 'Nascondi log' : 'Mostra log'}
              {showLogs ? <ChevronDown className="h-4 w-4 ml-1" /> : <ChevronRight className="h-4 w-4 ml-1" />}
            </Button>
          </div>

          {/* Sync Logs */}
          {showLogs && (
            <div className="border rounded-lg">
              <div className="p-3 border-b bg-muted/30">
                <h4 className="font-medium">Log sincronizzazioni</h4>
              </div>
              <ScrollArea className="h-[300px]">
                <div className="divide-y">
                  {runs.map(run => (
                    <div
                      key={run.id}
                      className="p-3 hover:bg-muted/50 cursor-pointer transition-colors"
                      onClick={() => setSelectedRun(run)}
                    >
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-3">
                          {getStatusBadge(run.status)}
                          <span className="text-sm">
                            {new Date(run.started_at).toLocaleString('it-IT', {
                              day: '2-digit',
                              month: '2-digit',
                              year: 'numeric',
                              hour: '2-digit',
                              minute: '2-digit'
                            })}
                          </span>
                          <Badge variant="outline" className="text-xs">
                            {run.trigger_type === 'cron' ? 'Cron' : 'Manuale'}
                          </Badge>
                          {run.attempt > 1 && (
                            <Badge variant="secondary" className="text-xs">
                              Tentativo {run.attempt}
                            </Badge>
                          )}
                        </div>
                        <span className="text-sm text-muted-foreground">
                          {formatDuration(run.runtime_ms)}
                        </span>
                      </div>
                      {run.error_message && (
                        <p className="text-sm text-destructive mt-1 truncate">
                          {run.error_message}
                        </p>
                      )}
                    </div>
                  ))}
                  {runs.length === 0 && (
                    <div className="p-8 text-center text-muted-foreground">
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
        <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              Dettaglio sincronizzazione
              {selectedRun && getStatusBadge(selectedRun.status)}
            </DialogTitle>
            <DialogDescription>
              {selectedRun && new Date(selectedRun.started_at).toLocaleString('it-IT')}
              {selectedRun?.trigger_type === 'cron' ? ' (Automatica)' : ' (Manuale)'}
              {selectedRun?.attempt && selectedRun.attempt > 1 && ` - Tentativo ${selectedRun.attempt}`}
            </DialogDescription>
          </DialogHeader>

          {selectedRun && (
            <div className="space-y-6">
              {/* Error message */}
              {selectedRun.error_message && (
                <div className="bg-destructive/10 border border-destructive/20 rounded-lg p-4">
                  <p className="font-medium text-destructive">{selectedRun.error_message}</p>
                </div>
              )}

              {/* Steps timeline */}
              <div>
                <h4 className="font-medium mb-3">Timeline degli step</h4>
                <div className="space-y-2">
                  {Object.entries(STEP_LABELS).map(([key, label]) => {
                    const step = selectedRun.steps?.[key];
                    return (
                      <div key={key} className="flex items-center justify-between p-2 rounded border">
                        <div className="flex items-center gap-2">
                          {step?.status === 'success' && <CheckCircle className="h-4 w-4 text-green-500" />}
                          {step?.status === 'failed' && <XCircle className="h-4 w-4 text-destructive" />}
                          {step?.status === 'skipped' && <AlertTriangle className="h-4 w-4 text-yellow-500" />}
                          {!step && <div className="h-4 w-4 rounded-full bg-muted" />}
                          <span className={!step ? 'text-muted-foreground' : ''}>{label}</span>
                        </div>
                        <span className="text-sm text-muted-foreground">
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
                  <h4 className="font-medium mb-3">Metriche</h4>
                  <div className="grid grid-cols-2 gap-2 text-sm">
                    <div className="flex justify-between p-2 bg-muted/30 rounded">
                      <span>Prodotti totali</span>
                      <span className="font-medium">{selectedRun.metrics.products_total}</span>
                    </div>
                    <div className="flex justify-between p-2 bg-muted/30 rounded">
                      <span>Prodotti elaborati</span>
                      <span className="font-medium">{selectedRun.metrics.products_processed}</span>
                    </div>
                    <div className="flex justify-between p-2 bg-muted/30 rounded">
                      <span>EAN mappati</span>
                      <span className="font-medium">{selectedRun.metrics.products_ean_mapped}</span>
                    </div>
                    <div className="flex justify-between p-2 bg-muted/30 rounded">
                      <span>EAN mancanti</span>
                      <span className="font-medium">{selectedRun.metrics.products_ean_missing}</span>
                    </div>
                    <div className="flex justify-between p-2 bg-muted/30 rounded">
                      <span>Export Mediaworld</span>
                      <span className="font-medium">{selectedRun.metrics.mediaworld_export_rows}</span>
                    </div>
                    <div className="flex justify-between p-2 bg-muted/30 rounded">
                      <span>Export ePrice</span>
                      <span className="font-medium">{selectedRun.metrics.eprice_export_rows}</span>
                    </div>
                    <div className="flex justify-between p-2 bg-muted/30 rounded">
                      <span>File SFTP caricati</span>
                      <span className="font-medium">{selectedRun.metrics.sftp_uploaded_files}</span>
                    </div>
                  </div>
                </div>
              )}

              {/* Technical details */}
              {selectedRun.error_details && (
                <div>
                  <h4 className="font-medium mb-3">Dettagli tecnici</h4>
                  <pre className="bg-muted p-3 rounded text-xs overflow-x-auto">
                    {JSON.stringify(selectedRun.error_details, null, 2)}
                  </pre>
                </div>
              )}

              {/* Steps details */}
              {selectedRun.steps && Object.keys(selectedRun.steps).length > 0 && (
                <div>
                  <h4 className="font-medium mb-3">Dettagli step (JSON)</h4>
                  <pre className="bg-muted p-3 rounded text-xs overflow-x-auto max-h-48">
                    {JSON.stringify(selectedRun.steps, null, 2)}
                  </pre>
                </div>
              )}
            </div>
          )}
        </DialogContent>
      </Dialog>
    </>
  );
};

export default SyncScheduler;