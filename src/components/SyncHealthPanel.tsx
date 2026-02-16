import React, { useState, useEffect } from 'react';
import { supabase } from '@/integrations/supabase/client';
import { Activity, Clock, Loader2, CheckCircle, AlertTriangle, XCircle, Timer, RotateCcw } from 'lucide-react';

type HealthDecision = 'healthy_progress' | 'waiting_retry_delay' | 'stalled' | 'failed' | 'unknown';

interface HealthSnapshot {
  run_id: string;
  now_ts: string;
  run_age_s: number;
  current_step: string;
  decision: HealthDecision;
  last_event_age_s: number | null;
  retry: {
    step: string | null;
    status: string | null;
    retry_attempt: number;
    next_retry_at: string | null;
    wait_seconds: number | null;
    reason: string | null;
  } | null;
  progress: {
    cursor_pos: number | null;
    chunk_index: number | null;
    total_products: number | null;
    file_total_size: number | null;
  };
}

const STEP_LABELS: Record<string, string> = {
  import_ftp: 'Import FTP',
  parse_merge: 'Parsing e Merge',
  ean_mapping: 'Mapping EAN',
  pricing: 'Calcolo Prezzi',
  override_products: 'Override Prodotti',
  export_ean: 'Export Catalogo EAN',
  export_ean_xlsx: 'Export Catalogo EAN (XLSX)',
  export_amazon: 'Export Amazon',
  export_mediaworld: 'Export Mediaworld',
  export_eprice: 'Export ePrice',
  upload_sftp: 'Upload SFTP',
  versioning: 'Versioning',
  notification: 'Notifica'
};

const DECISION_CONFIG: Record<HealthDecision, { label: string; icon: React.ReactNode; className: string }> = {
  healthy_progress: { label: 'Progresso attivo', icon: <CheckCircle className="h-4 w-4" />, className: 'text-success bg-success/10 border-success/30' },
  waiting_retry_delay: { label: 'In attesa retry', icon: <Timer className="h-4 w-4" />, className: 'text-warning bg-warning/10 border-warning/30' },
  stalled: { label: 'Possibile stallo', icon: <AlertTriangle className="h-4 w-4" />, className: 'text-error bg-error/10 border-error/30' },
  failed: { label: 'Fallita', icon: <XCircle className="h-4 w-4" />, className: 'text-error bg-error/10 border-error/30' },
  unknown: { label: 'Stato sconosciuto', icon: <Activity className="h-4 w-4" />, className: 'alt-text-muted bg-muted/10 border-muted/30' }
};

function classifyHealth(
  runAgeS: number,
  lastEventAgeS: number | null,
  currentStep: string,
  steps: Record<string, unknown>,
  runStatus: string
): HealthDecision {
  if (runStatus === 'failed') return 'failed';

  // Check retry_delay on export_ean_xlsx
  const xlsxState = steps?.export_ean_xlsx as Record<string, unknown> | undefined;
  const retry = xlsxState?.retry as Record<string, unknown> | undefined;
  if (currentStep === 'export_ean_xlsx' && retry?.status === 'retry_delay' && retry?.next_retry_at) {
    const nextAt = new Date(retry.next_retry_at as string).getTime();
    if (Date.now() < nextAt) return 'waiting_retry_delay';
  }

  // Stalled: no event in >300s and not in retry_delay
  if (lastEventAgeS !== null && lastEventAgeS > 300) return 'stalled';

  // Healthy: recent event
  if (lastEventAgeS !== null && lastEventAgeS <= 120) return 'healthy_progress';

  // Between 120-300s: still healthy if run is young
  if (lastEventAgeS !== null && lastEventAgeS <= 300) return 'healthy_progress';

  return 'unknown';
}

interface SyncHealthPanelProps {
  runId: string;
  runStartedAt: string;
  runStatus: string;
  steps: Record<string, unknown>;
}

export const SyncHealthPanel: React.FC<SyncHealthPanelProps> = ({ runId, runStartedAt, runStatus, steps }) => {
  const [snapshot, setSnapshot] = useState<HealthSnapshot | null>(null);
  const [tick, setTick] = useState(0);

  useEffect(() => {
    const interval = setInterval(() => setTick(t => t + 1), 5000);
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    const buildSnapshot = async () => {
      const now = Date.now();
      const runAgeS = Math.round((now - new Date(runStartedAt).getTime()) / 1000);
      const currentStep = (steps?.current_step as string) || 'unknown';

      // Get last event age
      let lastEventAgeS: number | null = null;
      try {
        const { data: events } = await supabase
          .from('sync_events')
          .select('created_at')
          .eq('run_id', runId)
          .order('created_at', { ascending: false })
          .limit(1);
        if (events?.length) {
          lastEventAgeS = Math.round((now - new Date(events[0].created_at).getTime()) / 1000);
        }
      } catch { /* non-blocking */ }

      // Extract retry info
      const xlsxState = steps?.export_ean_xlsx as Record<string, unknown> | undefined;
      const retryState = xlsxState?.retry as Record<string, unknown> | undefined;
      let retryInfo: HealthSnapshot['retry'] = null;
      if (retryState?.status === 'retry_delay') {
        const nextRetryAt = retryState.next_retry_at as string | null;
        const waitSeconds = nextRetryAt ? Math.max(0, Math.ceil((new Date(nextRetryAt).getTime() - now) / 1000)) : null;
        retryInfo = {
          step: 'export_ean_xlsx',
          status: retryState.status as string,
          retry_attempt: (retryState.retry_attempt as number) || 0,
          next_retry_at: nextRetryAt,
          wait_seconds: waitSeconds,
          reason: (retryState.last_error as string) || 'WORKER_LIMIT'
        };
      }

      // Extract progress
      const parseState = steps?.parse_merge as Record<string, unknown> | undefined;
      const progress = {
        cursor_pos: (parseState?.cursor_pos as number) || null,
        chunk_index: (parseState?.chunk_index as number) || null,
        total_products: (parseState?.productCount as number) || null,
        file_total_size: (parseState?.materialBytes as number) || null,
      };

      const decision = classifyHealth(runAgeS, lastEventAgeS, currentStep, steps, runStatus);

      const snap: HealthSnapshot = {
        run_id: runId,
        now_ts: new Date().toISOString(),
        run_age_s: runAgeS,
        current_step: currentStep,
        decision,
        last_event_age_s: lastEventAgeS,
        retry: retryInfo,
        progress,
      };

      setSnapshot(snap);
    };

    buildSnapshot();
  }, [runId, runStartedAt, runStatus, steps, tick]);

  if (!snapshot) return null;

  const cfg = DECISION_CONFIG[snapshot.decision];
  const progressPct = snapshot.progress.file_total_size && snapshot.progress.cursor_pos
    ? Math.round((snapshot.progress.cursor_pos / snapshot.progress.file_total_size) * 100)
    : null;

  const formatAge = (s: number) => {
    if (s < 60) return `${s}s fa`;
    if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s fa`;
    return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m fa`;
  };

  return (
    <div className="alt-panel rounded-lg p-4 space-y-3">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Activity className="h-4 w-4 alt-text-muted" />
          <span className="text-xs font-semibold alt-text-muted uppercase tracking-wide">Stato attuale</span>
        </div>
        <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full border text-xs font-semibold ${cfg.className}`}>
          {cfg.icon}
          {cfg.label}
        </span>
      </div>

      <div className="grid grid-cols-2 gap-2 text-sm">
        <div className="flex justify-between">
          <span className="alt-text-muted">Step corrente</span>
          <span className="font-medium">{STEP_LABELS[snapshot.current_step] || snapshot.current_step}</span>
        </div>
        <div className="flex justify-between">
          <span className="alt-text-muted">Durata run</span>
          <span className="font-medium">{formatAge(snapshot.run_age_s)}</span>
        </div>
        {snapshot.last_event_age_s !== null && (
          <div className="flex justify-between">
            <span className="alt-text-muted">Ultimo evento</span>
            <span className={`font-medium ${snapshot.last_event_age_s > 120 ? 'text-warning' : ''}`}>
              {formatAge(snapshot.last_event_age_s)}
            </span>
          </div>
        )}
        {progressPct !== null && (
          <div className="flex justify-between">
            <span className="alt-text-muted">Progresso file</span>
            <span className="font-medium">{progressPct}%</span>
          </div>
        )}
        {snapshot.progress.chunk_index !== null && (
          <div className="flex justify-between">
            <span className="alt-text-muted">Chunk</span>
            <span className="font-medium">#{snapshot.progress.chunk_index}</span>
          </div>
        )}
        {snapshot.progress.total_products !== null && (
          <div className="flex justify-between">
            <span className="alt-text-muted">Prodotti</span>
            <span className="font-medium">{snapshot.progress.total_products.toLocaleString('it-IT')}</span>
          </div>
        )}
      </div>

      {/* Retry delay info */}
      {snapshot.retry && (
        <div className="bg-warning/5 border border-warning/20 rounded-lg p-3 space-y-1">
          <div className="flex items-center gap-2 text-warning text-xs font-semibold">
            <RotateCcw className="h-3.5 w-3.5" />
            Retry in corso — {snapshot.retry.reason}
          </div>
          <div className="grid grid-cols-2 gap-1 text-xs">
            <div>
              <span className="alt-text-muted">Tentativo: </span>
              <span className="font-medium">{snapshot.retry.retry_attempt}/8</span>
            </div>
            {snapshot.retry.wait_seconds !== null && snapshot.retry.wait_seconds > 0 && (
              <div>
                <span className="alt-text-muted">Prossimo retry: </span>
                <span className="font-medium">{snapshot.retry.wait_seconds}s</span>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};
