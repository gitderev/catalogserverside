import React, { useState, useEffect, useCallback } from 'react';
import { supabase } from '@/integrations/supabase/client';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '@/components/ui/dialog';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import {
  Clock, CheckCircle, XCircle, AlertTriangle, Loader2,
  Activity, ChevronDown, ChevronRight, Zap, Copy, Filter
} from 'lucide-react';
import { toast } from '@/hooks/use-toast';
import { STEP_LABELS, EXPECTED_STEPS } from '@/shared/expectedSteps';
import { formatTimestamp } from '@/shared/formatTimestamp';

interface CronRun {
  id: string;
  started_at: string;
  finished_at: string | null;
  status: string;
  trigger_type: string;
  attempt: number;
  runtime_ms: number | null;
  error_message: string | null;
  steps: Record<string, any>;
  metrics: Record<string, any>;
  warning_count: number;
}

interface SyncEvent {
  id: string;
  created_at: string;
  level: string;
  message: string;
  step: string | null;
  details: Record<string, any> | null;
}

const CANONICAL_STEPS = [...EXPECTED_STEPS];

const STATUS_LABELS: Record<string, string> = {
  running: 'In esecuzione',
  success: 'Successo',
  success_with_warning: 'Con avvisi',
  failed: 'Fallita',
  timeout: 'Timeout',
};

function formatDuration(ms: number | null): string {
  if (!ms) return '-';
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${(ms / 60000).toFixed(1)}min`;
}

function formatTime(iso: string): string {
  return formatTimestamp(iso);
}

function deriveFailingStep(steps: Record<string, any>): string | null {
  for (const s of CANONICAL_STEPS) {
    const st = steps[s];
    if (st && st.status === 'failed') return s;
  }
  return null;
}

function deriveSyntheticError(run: CronRun): string | null {
  const failStep = deriveFailingStep(run.steps || {});
  if (!failStep) return run.error_message?.substring(0, 140) || null;
  const st = run.steps[failStep];
  const parts: string[] = [];
  if (st.retry?.last_http_status) parts.push(`HTTP ${st.retry.last_http_status}`);
  if (st.code) parts.push(st.code);
  if (st.error) parts.push(String(st.error).substring(0, 100));
  return parts.length ? `[${STEP_LABELS[failStep] || failStep}] ${parts.join(' — ')}` : null;
}

const StatusIcon: React.FC<{ status: string; size?: string }> = ({ status, size = 'h-4 w-4' }) => {
  switch (status) {
    case 'running': return <Loader2 className={`${size} animate-spin text-blue-500`} />;
    case 'success': return <CheckCircle className={`${size} text-emerald-600`} />;
    case 'success_with_warning': return <AlertTriangle className={`${size} text-amber-500`} />;
    case 'failed': return <XCircle className={`${size} text-red-600`} />;
    case 'timeout': return <Clock className={`${size} text-amber-600`} />;
    default: return <Activity className={`${size} text-slate-400`} />;
  }
};

export const SyncCronHistory: React.FC = () => {
  const [cronRuns, setCronRuns] = useState<CronRun[]>([]);
  const [selectedRun, setSelectedRun] = useState<CronRun | null>(null);
  const [events, setEvents] = useState<SyncEvent[]>([]);
  const [isLoadingEvents, setIsLoadingEvents] = useState(false);
  const [eventFilter, setEventFilter] = useState<'all' | 'warn_error' | 'stage'>('all');
  const [isLoading, setIsLoading] = useState(true);

  const loadCronRuns = useCallback(async () => {
    try {
      const { data, error } = await supabase
        .from('sync_runs')
        .select('id, started_at, finished_at, status, trigger_type, attempt, runtime_ms, error_message, steps, metrics, warning_count')
        .eq('trigger_type', 'cron')
        .order('started_at', { ascending: false })
        .limit(10);
      if (!error && data) setCronRuns(data as unknown as CronRun[]);
    } catch { /* ignore */ }
    setIsLoading(false);
  }, []);

  // Initial load + conditional polling
  useEffect(() => {
    loadCronRuns();
  }, [loadCronRuns]);

  useEffect(() => {
    const hasRunning = cronRuns.some(r => r.status === 'running');
    if (!hasRunning) return;
    const interval = setInterval(loadCronRuns, 60_000);
    return () => clearInterval(interval);
  }, [cronRuns, loadCronRuns]);

  // Load events when a run is selected
  useEffect(() => {
    if (!selectedRun) { setEvents([]); return; }
    const load = async () => {
      setIsLoadingEvents(true);
      try {
        const { data, error } = await supabase
          .from('sync_events')
          .select('id, created_at, level, message, step, details')
          .eq('run_id', selectedRun.id)
          .order('created_at', { ascending: false })
          .limit(200);
        if (!error && data) setEvents(data as unknown as SyncEvent[]);
      } catch { /* ignore */ }
      setIsLoadingEvents(false);
    };
    load();
  }, [selectedRun?.id]);

  const filteredEvents = events.filter(e => {
    if (eventFilter === 'warn_error') return e.level === 'WARN' || e.level === 'ERROR';
    if (eventFilter === 'stage') return e.message.includes('_stage');
    return true;
  });

  if (isLoading) {
    return (
      <Card className="mb-6 alt-card">
        <CardContent className="flex items-center justify-center py-8">
          <Loader2 className="h-6 w-6 animate-spin text-primary" />
        </CardContent>
      </Card>
    );
  }

  if (cronRuns.length === 0) return null;

  return (
    <>
      <Card className="mb-6 alt-card">
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-lg font-bold">
            <Zap className="h-5 w-5 text-primary" />
            Sincronizzazioni automatiche
          </CardTitle>
        </CardHeader>
        <CardContent>
          <ScrollArea className="max-h-[420px]">
            <div className="divide-y divide-border">
              {cronRuns.map(run => {
                const currentStep = run.steps?.current_step;
                const failingStep = deriveFailingStep(run.steps || {});
                const syntheticErr = deriveSyntheticError(run);
                const metrics = run.metrics || {};
                return (
                  <div
                    key={run.id}
                    className="p-3 hover:bg-muted/10 cursor-pointer transition-colors"
                    onClick={() => setSelectedRun(run)}
                  >
                    <div className="flex items-center justify-between gap-2 flex-wrap">
                      <div className="flex items-center gap-2 flex-wrap">
                        <StatusIcon status={run.status} />
                        <span className="text-sm font-semibold">
                          {STATUS_LABELS[run.status] || run.status}
                        </span>
                        <span className="text-xs text-muted-foreground">{formatTime(run.started_at)}</span>
                        {run.attempt > 1 && (
                          <span className="text-xs px-1.5 py-0.5 rounded bg-amber-100 text-amber-700 font-medium">
                            Tentativo {run.attempt}
                          </span>
                        )}
                      </div>
                      <div className="flex items-center gap-3 text-xs text-muted-foreground">
                        {currentStep && <span>Step: {STEP_LABELS[currentStep] || currentStep}</span>}
                        <span>{formatDuration(run.runtime_ms)}</span>
                        {run.warning_count > 0 && (
                          <span className="text-amber-600 font-medium">{run.warning_count} avvisi</span>
                        )}
                      </div>
                    </div>

                    {/* Metrics summary */}
                    {(metrics.products_processed || metrics.amazon_export_rows || metrics.mediaworld_export_rows) && (
                      <div className="flex gap-3 mt-1 text-xs text-muted-foreground">
                        {metrics.products_processed > 0 && <span>{metrics.products_processed} prodotti</span>}
                        {metrics.amazon_export_rows > 0 && <span>Amazon: {metrics.amazon_export_rows}</span>}
                        {metrics.mediaworld_export_rows > 0 && <span>MW: {metrics.mediaworld_export_rows}</span>}
                        {metrics.eprice_export_rows > 0 && <span>ePrice: {metrics.eprice_export_rows}</span>}
                      </div>
                    )}

                    {/* Synthetic error */}
                    {syntheticErr && run.status !== 'running' && run.status !== 'success' && (
                      <p className="mt-1 text-xs text-red-600 truncate">{syntheticErr}</p>
                    )}
                  </div>
                );
              })}
            </div>
          </ScrollArea>
        </CardContent>
      </Card>

      {/* Run Detail Dialog */}
      <Dialog open={!!selectedRun} onOpenChange={() => setSelectedRun(null)}>
        <DialogContent className="max-w-3xl max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <StatusIcon status={selectedRun?.status || ''} size="h-5 w-5" />
              Dettaglio run cron
            </DialogTitle>
            <DialogDescription>
              {selectedRun && (
                <span className="text-xs font-mono">{selectedRun.id}</span>
              )}
            </DialogDescription>
          </DialogHeader>

          {selectedRun && (
            <Tabs defaultValue="overview" className="mt-2">
              <TabsList className="grid w-full grid-cols-3">
                <TabsTrigger value="overview">Riepilogo</TabsTrigger>
                <TabsTrigger value="timeline">Timeline Step</TabsTrigger>
                <TabsTrigger value="events">
                  Eventi ({events.length})
                </TabsTrigger>
              </TabsList>

              {/* Overview Tab */}
              <TabsContent value="overview" className="space-y-4 mt-4">
                <div className="grid grid-cols-2 gap-3 text-sm">
                  {[
                    ['Stato', STATUS_LABELS[selectedRun.status] || selectedRun.status],
                    ['Trigger', selectedRun.trigger_type],
                    ['Tentativo', String(selectedRun.attempt)],
                    ['Avvio', formatTime(selectedRun.started_at)],
                    ['Fine', selectedRun.finished_at ? formatTime(selectedRun.finished_at) : 'In corso'],
                    ['Durata', formatDuration(selectedRun.runtime_ms)],
                    ['Avvisi', String(selectedRun.warning_count)],
                    ['Step corrente', STEP_LABELS[selectedRun.steps?.current_step] || selectedRun.steps?.current_step || '-'],
                  ].map(([label, value]) => (
                    <div key={label} className="flex justify-between p-2 bg-muted/30 rounded">
                      <span className="text-muted-foreground">{label}</span>
                      <span className="font-medium">{value}</span>
                    </div>
                  ))}
                </div>
                {selectedRun.error_message && (
                  <div className="p-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-800">
                    {selectedRun.error_message}
                  </div>
                )}
                {/* Metrics */}
                {selectedRun.metrics && Object.keys(selectedRun.metrics).length > 0 && (
                  <Collapsible>
                    <CollapsibleTrigger className="flex items-center gap-1 text-sm font-medium text-muted-foreground hover:text-foreground">
                      <ChevronRight className="h-4 w-4" /> Metriche
                    </CollapsibleTrigger>
                    <CollapsibleContent>
                      <pre className="mt-2 p-3 bg-muted/30 rounded text-xs overflow-x-auto max-h-40">
                        {JSON.stringify(selectedRun.metrics, null, 2)}
                      </pre>
                    </CollapsibleContent>
                  </Collapsible>
                )}
              </TabsContent>

              {/* Timeline Tab */}
              <TabsContent value="timeline" className="mt-4">
                <div className="space-y-1.5">
                  {CANONICAL_STEPS.map(stepKey => {
                    const st = selectedRun.steps?.[stepKey];
                    const status = st?.status || '';
                    const isCompleted = status === 'completed' || status === 'success';
                    const isFailed = status === 'failed';
                    const isActive = status === 'in_progress' || status === 'retry_delay';
                    return (
                      <div key={stepKey} className={`flex items-center justify-between p-2.5 rounded-lg border text-sm ${
                        isCompleted ? 'bg-emerald-50 border-emerald-200' :
                        isFailed ? 'bg-red-50 border-red-200' :
                        isActive ? 'bg-blue-50 border-blue-200' :
                        'bg-muted/20 border-border'
                      }`}>
                        <div className="flex items-center gap-2">
                          {isCompleted && <CheckCircle className="h-4 w-4 text-emerald-600" />}
                          {isFailed && <XCircle className="h-4 w-4 text-red-600" />}
                          {isActive && <Loader2 className="h-4 w-4 text-blue-500 animate-spin" />}
                          {!st && <div className="h-4 w-4 rounded-full bg-muted" />}
                          <span className={!st ? 'text-muted-foreground' : ''}>{STEP_LABELS[stepKey]}</span>
                        </div>
                        <div className="flex items-center gap-2 text-xs text-muted-foreground">
                          {st?.duration_ms != null && <span>{formatDuration(st.duration_ms)}</span>}
                          {st?.rows_written != null && <span>{st.rows_written} righe</span>}
                          {st?.rows != null && !st?.rows_written && <span>{st.rows} righe</span>}
                          {isFailed && st?.error && (
                            <span className="text-red-600 max-w-[200px] truncate" title={st.error}>
                              {st.error}
                            </span>
                          )}
                          {st?.retry?.retry_attempt > 0 && (
                            <span className="text-amber-600">retry {st.retry.retry_attempt}</span>
                          )}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </TabsContent>

              {/* Events Tab */}
              <TabsContent value="events" className="mt-4 space-y-3">
                <div className="flex items-center justify-between">
                  <div className="flex gap-1">
                    {(['all', 'warn_error', 'stage'] as const).map(f => (
                      <Button
                        key={f}
                        variant={eventFilter === f ? 'default' : 'outline'}
                        size="sm"
                        className="text-xs h-7"
                        onClick={() => setEventFilter(f)}
                      >
                        {f === 'all' ? 'Tutti' : f === 'warn_error' ? 'WARN/ERROR' : 'Stage'}
                      </Button>
                    ))}
                  </div>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="text-xs h-7"
                    onClick={() => {
                      const safe = filteredEvents.map(e => ({
                        time: e.created_at, level: e.level, message: e.message, step: e.step, details: e.details,
                      }));
                      navigator.clipboard.writeText(JSON.stringify(safe, null, 2));
                      toast({ title: 'Copiato', description: `${safe.length} eventi copiati` });
                    }}
                  >
                    <Copy className="h-3 w-3 mr-1" /> Copia
                  </Button>
                </div>

                {isLoadingEvents ? (
                  <div className="flex items-center gap-2 text-sm text-muted-foreground py-4">
                    <Loader2 className="h-4 w-4 animate-spin" /> Caricamento eventi...
                  </div>
                ) : filteredEvents.length === 0 ? (
                  <p className="text-sm text-muted-foreground py-4">Nessun evento trovato.</p>
                ) : (
                  <ScrollArea className="max-h-[400px]">
                    <div className="space-y-1.5">
                      {filteredEvents.map(event => (
                        <div key={event.id} className={`p-2.5 rounded-lg border text-sm ${
                          event.level === 'ERROR' ? 'bg-red-50 border-red-200' :
                          event.level === 'WARN' ? 'bg-amber-50 border-amber-200' :
                          'bg-muted/20 border-border'
                        }`}>
                          <div className="flex items-center gap-2 flex-wrap">
                            <span className={`text-xs font-bold ${
                              event.level === 'ERROR' ? 'text-red-700' :
                              event.level === 'WARN' ? 'text-amber-700' :
                              'text-muted-foreground'
                            }`}>{event.level}</span>
                            <span className="text-xs font-medium">{event.message}</span>
                            {event.step && (
                              <span className="text-xs text-muted-foreground">[{STEP_LABELS[event.step] || event.step}]</span>
                            )}
                            <span className="text-xs text-muted-foreground ml-auto">
                              {new Date(event.created_at).toLocaleTimeString('it-IT', { hour: '2-digit', minute: '2-digit', second: '2-digit' })}
                            </span>
                          </div>
                          {event.details && typeof event.details === 'object' && Object.keys(event.details).length > 0 && (
                            <pre className="mt-1 text-xs text-muted-foreground overflow-x-auto max-h-20">
                              {JSON.stringify(event.details, null, 2)}
                            </pre>
                          )}
                        </div>
                      ))}
                    </div>
                  </ScrollArea>
                )}
              </TabsContent>
            </Tabs>
          )}
        </DialogContent>
      </Dialog>
    </>
  );
};
