import React, { useState, useEffect } from 'react';
import { AlertTriangle, Server, HardDrive } from 'lucide-react';
import { supabase } from '@/integrations/supabase/client';
import type { StockLocationWarningsState } from '@/types/feeConfig';

interface StockLocationWarningsProps {
  warnings: StockLocationWarningsState;
  showServerWarnings?: boolean;
}

interface ServerRunWarnings {
  runId: string;
  finishedAt: string;
  warnings: StockLocationWarningsState;
}

/**
 * Stock Location Warnings Display Component
 * 
 * Shows warning counts from the stock location parsing and validation.
 * Displays both client-side (manual run) and server-side (last sync run) warnings.
 */
const StockLocationWarnings: React.FC<StockLocationWarningsProps> = ({ 
  warnings, 
  showServerWarnings = true 
}) => {
  const [serverWarnings, setServerWarnings] = useState<ServerRunWarnings | null>(null);
  const [serverLoading, setServerLoading] = useState(false);

  // Load server-side warnings from last completed sync run
  useEffect(() => {
    if (!showServerWarnings) return;
    
    const loadServerWarnings = async () => {
      setServerLoading(true);
      try {
        // Query sync_runs for the latest completed run with location_warnings
        const { data, error } = await supabase
          .from('sync_runs')
          .select('id, finished_at, location_warnings')
          .eq('status', 'completed')
          .order('finished_at', { ascending: false })
          .limit(1);
        
        if (error) {
          console.error('[StockLocationWarnings] Error loading server warnings:', error);
          return;
        }
        
        if (data && data.length > 0) {
          const run = data[0];
          const locationWarnings = run.location_warnings as unknown as StockLocationWarningsState;
          
          // Only show if there are actual warnings
          if (locationWarnings && typeof locationWarnings === 'object') {
            const hasAny = Object.values(locationWarnings).some(v => typeof v === 'number' && v > 0);
            if (hasAny) {
              setServerWarnings({
                runId: run.id,
                finishedAt: run.finished_at || '',
                warnings: locationWarnings
              });
            }
          }
        }
      } catch (err) {
        console.error('[StockLocationWarnings] Error:', err);
      } finally {
        setServerLoading(false);
      }
    };
    
    loadServerWarnings();
  }, [showServerWarnings]);

  const hasManualWarnings = Object.values(warnings).some(v => v > 0);
  const hasServerWarnings = serverWarnings && Object.values(serverWarnings.warnings).some(v => v > 0);

  if (!hasManualWarnings && !hasServerWarnings) {
    return null;
  }

  const warningLabels: Record<keyof StockLocationWarningsState, string> = {
    missing_location_file: 'File location mancante',
    invalid_location_parse: 'Errore parsing location',
    missing_location_data: 'Prodotti senza dati location',
    split_mismatch: 'Mismatch stock totale vs IT+EU',
    multi_mpn_per_matnr: 'MPN multipli per Matnr',
    orphan_4255: 'LocationID 4255 orfani',
    decode_fallback_used: 'Fallback encoding usato',
    invalid_stock_value: 'Valori stock non validi',
  };

  const renderWarningGrid = (warningsData: StockLocationWarningsState) => {
    const items = Object.entries(warningsData)
      .filter(([_, value]) => value > 0)
      .map(([key, value]) => ({
        key,
        label: warningLabels[key as keyof StockLocationWarningsState] || key,
        value
      }));

    if (items.length === 0) return null;

    return (
      <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
        {items.map(item => (
          <div 
            key={item.key} 
            className="text-center p-2 rounded alt-warning-stat"
          >
            <div className="text-lg font-bold text-warning">{item.value}</div>
            <div className="text-xs alt-text-muted">{item.label}</div>
          </div>
        ))}
      </div>
    );
  };

  return (
    <div className="mt-4 space-y-4">
      {/* Manual Run Warnings */}
      {hasManualWarnings && (
        <div className="p-4 rounded-lg alt-alert alt-alert-warning">
          <h4 className="text-sm font-semibold mb-3 flex items-center gap-2 text-warning">
            <HardDrive className="h-4 w-4" />
            Avvisi Stock Location (Run Manuale)
          </h4>
          {renderWarningGrid(warnings)}
        </div>
      )}

      {/* Server Run Warnings */}
      {hasServerWarnings && serverWarnings && (
        <div className="p-4 rounded-lg alt-alert alt-alert-info">
          <h4 className="text-sm font-semibold mb-3 flex items-center gap-2 text-info">
            <Server className="h-4 w-4" />
            Avvisi Stock Location (Ultima Run Server)
            {serverWarnings.finishedAt && (
              <span className="text-xs font-normal alt-text-muted ml-2">
                {new Date(serverWarnings.finishedAt).toLocaleString('it-IT')}
              </span>
            )}
          </h4>
          {renderWarningGrid(serverWarnings.warnings)}
        </div>
      )}

      {serverLoading && (
        <div className="text-xs text-muted-foreground">
          Caricamento avvisi server...
        </div>
      )}
    </div>
  );
};

export default StockLocationWarnings;