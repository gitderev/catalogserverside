import React from 'react';
import { AlertTriangle } from 'lucide-react';
import type { StockLocationWarningsState } from '@/types/feeConfig';

interface StockLocationWarningsProps {
  warnings: StockLocationWarningsState;
}

/**
 * Stock Location Warnings Display Component
 * 
 * Shows warning counts from the stock location parsing and validation.
 */
const StockLocationWarnings: React.FC<StockLocationWarningsProps> = ({ warnings }) => {
  const hasWarnings = Object.values(warnings).some(v => v > 0);

  if (!hasWarnings) {
    return null;
  }

  const warningItems = [
    { key: 'missing_location_file', label: 'File location mancante', value: warnings.missing_location_file },
    { key: 'invalid_location_parse', label: 'Errore parsing location', value: warnings.invalid_location_parse },
    { key: 'missing_location_data', label: 'Prodotti senza dati location', value: warnings.missing_location_data },
    { key: 'split_mismatch', label: 'Mismatch stock totale vs IT+EU', value: warnings.split_mismatch },
    { key: 'multi_mpn_per_matnr', label: 'MPN multipli per Matnr', value: warnings.multi_mpn_per_matnr },
    { key: 'orphan_4255', label: 'LocationID 4255 orfani', value: warnings.orphan_4255 },
    { key: 'decode_fallback_used', label: 'Fallback encoding usato', value: warnings.decode_fallback_used },
    { key: 'invalid_stock_value', label: 'Valori stock non validi', value: warnings.invalid_stock_value },
  ].filter(item => item.value > 0);

  return (
    <div className="mt-4 p-4 rounded-lg border" style={{ background: '#fef3c7', borderColor: '#f59e0b' }}>
      <h4 className="text-sm font-semibold mb-3 flex items-center gap-2 text-amber-800">
        <AlertTriangle className="h-4 w-4" />
        Avvisi Stock Location
      </h4>
      
      <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
        {warningItems.map(item => (
          <div 
            key={item.key} 
            className="text-center p-2 rounded bg-white/50"
          >
            <div className="text-lg font-bold text-amber-700">{item.value}</div>
            <div className="text-xs text-amber-600">{item.label}</div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default StockLocationWarnings;
