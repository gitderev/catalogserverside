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
  };
}

interface PipelineStepsDisplayProps {
  steps: PipelineStep[];
  isRunning: boolean;
}

const getStatusIcon = (status: StepStatus) => {
  switch (status) {
    case 'running':
      return <Loader2 className="h-5 w-5 text-blue-600 animate-spin" />;
    case 'done':
      return <CheckCircle className="h-5 w-5 text-green-600" />;
    case 'warning':
      return <AlertTriangle className="h-5 w-5 text-amber-500" />;
    case 'error':
      return <XCircle className="h-5 w-5 text-red-600" />;
    default:
      return <Circle className="h-5 w-5 text-gray-300" />;
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
  
  const statusColors: Record<StepStatus, string> = {
    idle: 'bg-gray-100 text-gray-600',
    running: 'bg-blue-100 text-blue-700',
    done: 'bg-green-100 text-green-700',
    warning: 'bg-amber-100 text-amber-700',
    error: 'bg-red-100 text-red-700'
  };
  
  return (
    <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${statusColors[status]}`}>
      {statusLabels[status]}
    </span>
  );
};

const StepDetails: React.FC<{ details: PipelineStep['details'] }> = ({ details }) => {
  if (!details) return null;
  
  const { warnings = [], errors = [], counters = {} } = details;
  const hasContent = warnings.length > 0 || errors.length > 0 || Object.keys(counters).length > 0;
  
  if (!hasContent) return null;
  
  return (
    <div className="mt-2 ml-8 space-y-2">
      {/* Counters */}
      {Object.keys(counters).length > 0 && (
        <div className="flex flex-wrap gap-2">
          {Object.entries(counters).map(([key, value]) => (
            <span key={key} className="text-xs px-2 py-1 bg-gray-50 rounded border">
              <span className="font-medium">{key}:</span> {value}
            </span>
          ))}
        </div>
      )}
      
      {/* Errors */}
      {errors.length > 0 && (
        <div className="text-xs space-y-1">
          <div className="font-medium text-red-700">Errori ({errors.length}):</div>
          <ul className="list-disc list-inside text-red-600 max-h-24 overflow-y-auto">
            {errors.slice(0, 5).map((err, i) => (
              <li key={i}>{err}</li>
            ))}
            {errors.length > 5 && (
              <li className="text-gray-500">...e altri {errors.length - 5}</li>
            )}
          </ul>
        </div>
      )}
      
      {/* Warnings */}
      {warnings.length > 0 && (
        <div className="text-xs space-y-1">
          <div className="font-medium text-amber-700">Avvisi ({warnings.length}):</div>
          <ul className="list-disc list-inside text-amber-600 max-h-24 overflow-y-auto">
            {warnings.slice(0, 5).map((warn, i) => (
              <li key={i}>{warn}</li>
            ))}
            {warnings.length > 5 && (
              <li className="text-gray-500">...e altri {warnings.length - 5}</li>
            )}
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
    const { warnings = [], errors = [], counters = {} } = step.details;
    return warnings.length > 0 || errors.length > 0 || Object.keys(counters).length > 0;
  };
  
  return (
    <div className="card border-strong">
      <div className="card-body">
        <h3 className="card-title mb-4 flex items-center gap-2">
          {isRunning ? (
            <Loader2 className="h-5 w-5 animate-spin text-blue-600" />
          ) : (
            <CheckCircle className="h-5 w-5 text-gray-400" />
          )}
          Pipeline Steps
        </h3>
        
        <div className="space-y-3">
          {steps.map((step) => {
            const canExpand = hasDetails(step) && (step.status === 'warning' || step.status === 'error');
            const isExpanded = expandedSteps.has(step.id);
            
            return (
              <div 
                key={step.id} 
                className={`p-3 rounded-lg border transition-colors ${
                  step.status === 'running' ? 'bg-blue-50 border-blue-200' :
                  step.status === 'done' ? 'bg-green-50 border-green-200' :
                  step.status === 'warning' ? 'bg-amber-50 border-amber-200' :
                  step.status === 'error' ? 'bg-red-50 border-red-200' :
                  'bg-gray-50 border-gray-200'
                }`}
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    {getStatusIcon(step.status)}
                    <div>
                      <div className="flex items-center gap-2">
                        <span className="font-medium text-sm">{step.label}</span>
                        {getStatusBadge(step.status)}
                      </div>
                      {step.summary && (
                        <p className="text-xs text-muted-foreground mt-0.5 line-clamp-2">
                          {step.summary}
                        </p>
                      )}
                    </div>
                  </div>
                  
                  {canExpand && (
                    <button
                      onClick={() => toggleStep(step.id)}
                      className="p-1 hover:bg-white/50 rounded transition-colors"
                    >
                      {isExpanded ? (
                        <ChevronDown className="h-4 w-4 text-gray-500" />
                      ) : (
                        <ChevronRight className="h-4 w-4 text-gray-500" />
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
    </div>
  );
};

export default PipelineStepsDisplay;
