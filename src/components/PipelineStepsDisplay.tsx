import React from 'react';
import { CheckCircle, XCircle, AlertTriangle, Loader2, Circle, ChevronDown, ChevronRight } from 'lucide-react';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';

export type StepStatus = 'idle' | 'running' | 'done' | 'warning' | 'error';

export interface PipelineStep {
  id: string;
  label: string;
  status: StepStatus;
  summary?: string;
  details?: {
    warnings?: string[];
    errors?: string[];
    counters?: Record<string, number>;
    info?: string[];  // Informative messages (not warnings)
  };
}

interface PipelineStepsDisplayProps {
  steps: PipelineStep[];
  isRunning: boolean;
}

const getStatusIcon = (status: StepStatus) => {
  switch (status) {
    case 'running':
      return <Loader2 className="h-5 w-5 text-info animate-spin" />;
    case 'done':
      return <CheckCircle className="h-5 w-5 text-success" />;
    case 'warning':
      return <AlertTriangle className="h-5 w-5 text-warning" />;
    case 'error':
      return <XCircle className="h-5 w-5 text-error" />;
    default:
      return <Circle className="h-5 w-5 alt-text-muted" />;
  }
};

const getStatusBadge = (status: StepStatus) => {
  const statusLabels: Record<StepStatus, string> = {
    idle: 'In attesa',
    running: 'In corso',
    done: 'Completato',
    warning: 'Avvisi',
    error: 'Errore'
  };
  
  const badgeClass: Record<StepStatus, string> = {
    idle: 'alt-badge alt-badge-idle',
    running: 'alt-badge alt-badge-info',
    done: 'alt-badge alt-badge-success',
    warning: 'alt-badge alt-badge-warning',
    error: 'alt-badge alt-badge-error'
  };
  
  return (
    <span className={badgeClass[status]}>
      {statusLabels[status]}
    </span>
  );
};

const StepDetails: React.FC<{ details: PipelineStep['details'] }> = ({ details }) => {
  if (!details) return null;
  
  const { warnings = [], errors = [], counters = {}, info = [] } = details;
  const hasContent = warnings.length > 0 || errors.length > 0 || Object.keys(counters).length > 0 || info.length > 0;
  
  if (!hasContent) return null;
  
  return (
    <div className="mt-2 ml-8 space-y-2">
      {/* Counters */}
      {Object.keys(counters).length > 0 && (
        <div className="flex flex-wrap gap-2">
          {Object.entries(counters).map(([key, value]) => (
            <span key={key} className="alt-step-counter">
              <span className="font-medium">{key}:</span> {value}
            </span>
          ))}
        </div>
      )}
      
      {/* Errors */}
      {errors.length > 0 && (
        <div className="text-xs space-y-1">
          <div className="font-medium text-error">Errori ({errors.length}):</div>
          <ul className="list-disc list-inside text-error max-h-24 overflow-y-auto">
            {errors.slice(0, 5).map((err, i) => (
              <li key={i}>{err}</li>
            ))}
            {errors.length > 5 && (
              <li className="alt-text-muted">...e altri {errors.length - 5}</li>
            )}
          </ul>
        </div>
      )}
      
      {/* Warnings */}
      {warnings.length > 0 && (
        <div className="text-xs space-y-1">
          <div className="font-medium text-warning">Avvisi ({warnings.length}):</div>
          <ul className="list-disc list-inside text-warning max-h-24 overflow-y-auto">
            {warnings.slice(0, 5).map((warn, i) => (
              <li key={i}>{warn}</li>
            ))}
            {warnings.length > 5 && (
              <li className="alt-text-muted">...e altri {warnings.length - 5}</li>
            )}
          </ul>
        </div>
      )}
      
      {/* Info (informative, not warnings) */}
      {info.length > 0 && (
        <div className="text-xs space-y-1">
          <div className="font-medium text-info">Info:</div>
          <ul className="list-disc list-inside text-info max-h-24 overflow-y-auto">
            {info.map((msg, i) => (
              <li key={i}>{msg}</li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
};

export const PipelineStepsDisplay: React.FC<PipelineStepsDisplayProps> = ({ steps, isRunning }) => {
  const [expandedSteps, setExpandedSteps] = React.useState<Set<string>>(new Set());
  
  const toggleStep = (stepId: string) => {
    setExpandedSteps(prev => {
      const next = new Set(prev);
      if (next.has(stepId)) {
        next.delete(stepId);
      } else {
        next.add(stepId);
      }
      return next;
    });
  };
  
  const hasDetails = (step: PipelineStep): boolean => {
    if (!step.details) return false;
    const { warnings = [], errors = [], counters = {}, info = [] } = step.details;
    return warnings.length > 0 || errors.length > 0 || Object.keys(counters).length > 0 || info.length > 0;
  };
  
  return (
    <div className="alt-card">
      <h3 className="alt-section-title">
        {isRunning ? (
          <Loader2 className="h-5 w-5 animate-spin text-info" />
        ) : (
          <CheckCircle className="h-5 w-5 alt-text-muted" />
        )}
        Pipeline Steps
      </h3>
      
      <div className="space-y-3">
        {steps.map((step) => {
          const hasInfoContent = step.details?.info && step.details.info.length > 0;
          const canExpand = hasDetails(step) && (step.status === 'warning' || step.status === 'error' || hasInfoContent);
          const isExpanded = expandedSteps.has(step.id);
          
          const stepClass = 
            step.status === 'running' ? 'alt-step alt-step--progress' :
            step.status === 'done' ? 'alt-step alt-step--done' :
            step.status === 'warning' ? 'alt-step alt-step--warning' :
            step.status === 'error' ? 'alt-step alt-step--error' :
            'alt-step alt-step--idle';
          
          return (
            <div key={step.id} className={stepClass}>
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  {getStatusIcon(step.status)}
                  <div>
                    <div className="flex items-center gap-2">
                      <span className="font-medium text-sm">{step.label}</span>
                      {getStatusBadge(step.status)}
                    </div>
                    {step.summary && (
                      <p className="text-xs alt-text-muted mt-0.5 line-clamp-2">
                        {step.summary}
                      </p>
                    )}
                  </div>
                </div>
                
                {canExpand && (
                  <button
                    onClick={() => toggleStep(step.id)}
                    className="p-1 alt-step-expand-btn rounded transition-colors"
                  >
                    {isExpanded ? (
                      <ChevronDown className="h-4 w-4 alt-text-muted" />
                    ) : (
                      <ChevronRight className="h-4 w-4 alt-text-muted" />
                    )}
                  </button>
                )}
              </div>
              
              {canExpand && isExpanded && <StepDetails details={step.details} />}
            </div>
          );
        })}
      </div>
    </div>
  );
};

export default PipelineStepsDisplay;
