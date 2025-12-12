import React from 'react';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';
import { Card } from '@/components/ui/card';
import type { FeeConfigState } from '@/types/feeConfig';

interface StockLocationConfigProps {
  config: FeeConfigState;
  onConfigChange: (updates: Partial<FeeConfigState>) => void;
  disabled?: boolean;
}

/**
 * Stock Location Configuration Component
 * 
 * Provides controls for IT/EU stock split configuration:
 * - Toggle for including EU stock (per marketplace)
 * - Preparation days for IT and EU (per marketplace)
 */
const StockLocationConfig: React.FC<StockLocationConfigProps> = ({
  config,
  onConfigChange,
  disabled = false
}) => {
  return (
    <div className="space-y-6">
      {/* Mediaworld Configuration */}
      <Card className="p-4">
        <h4 className="text-lg font-semibold mb-4 text-amber-800">Mediaworld - Configurazione Stock IT/EU</h4>
        
        <div className="space-y-4">
          {/* Include EU Toggle */}
          <div className="flex items-center justify-between">
            <div>
              <Label htmlFor="mediaworld-include-eu" className="text-sm font-medium">
                Includi stock EU come fallback
              </Label>
              <p className="text-xs text-muted-foreground mt-1">
                Se attivo, usa stock EU quando IT &lt; 2
              </p>
            </div>
            <Switch
              id="mediaworld-include-eu"
              checked={config.mediaworldIncludeEu}
              onCheckedChange={(checked) => onConfigChange({ mediaworldIncludeEu: checked })}
              disabled={disabled}
            />
          </div>
          
          {/* IT/EU Preparation Days */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <Label htmlFor="mediaworld-it-days" className="text-sm font-medium">
                Giorni preparazione IT
              </Label>
              <Input
                id="mediaworld-it-days"
                type="number"
                min={1}
                max={45}
                value={config.mediaworldItPreparationDays}
                onChange={(e) => {
                  const val = parseInt(e.target.value, 10);
                  if (!isNaN(val) && val >= 1 && val <= 45) {
                    onConfigChange({ mediaworldItPreparationDays: val });
                  }
                }}
                className="w-full mt-1"
                disabled={disabled}
              />
            </div>
            <div>
              <Label htmlFor="mediaworld-eu-days" className="text-sm font-medium">
                Giorni preparazione EU
              </Label>
              <Input
                id="mediaworld-eu-days"
                type="number"
                min={1}
                max={45}
                value={config.mediaworldEuPreparationDays}
                onChange={(e) => {
                  const val = parseInt(e.target.value, 10);
                  if (!isNaN(val) && val >= 1 && val <= 45) {
                    onConfigChange({ mediaworldEuPreparationDays: val });
                  }
                }}
                className="w-full mt-1"
                disabled={disabled}
              />
            </div>
          </div>
        </div>
      </Card>

      {/* ePrice Configuration */}
      <Card className="p-4">
        <h4 className="text-lg font-semibold mb-4 text-blue-800">ePrice - Configurazione Stock IT/EU</h4>
        
        <div className="space-y-4">
          {/* Include EU Toggle */}
          <div className="flex items-center justify-between">
            <div>
              <Label htmlFor="eprice-include-eu" className="text-sm font-medium">
                Includi stock EU come fallback
              </Label>
              <p className="text-xs text-muted-foreground mt-1">
                Se attivo, usa stock EU quando IT &lt; 2
              </p>
            </div>
            <Switch
              id="eprice-include-eu"
              checked={config.epriceIncludeEu}
              onCheckedChange={(checked) => onConfigChange({ epriceIncludeEu: checked })}
              disabled={disabled}
            />
          </div>
          
          {/* IT/EU Preparation Days */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <Label htmlFor="eprice-it-days" className="text-sm font-medium">
                Giorni preparazione IT
              </Label>
              <Input
                id="eprice-it-days"
                type="number"
                min={1}
                max={30}
                value={config.epriceItPreparationDays}
                onChange={(e) => {
                  const val = parseInt(e.target.value, 10);
                  if (!isNaN(val) && val >= 1 && val <= 30) {
                    onConfigChange({ epriceItPreparationDays: val });
                  }
                }}
                className="w-full mt-1"
                disabled={disabled}
              />
            </div>
            <div>
              <Label htmlFor="eprice-eu-days" className="text-sm font-medium">
                Giorni preparazione EU
              </Label>
              <Input
                id="eprice-eu-days"
                type="number"
                min={1}
                max={30}
                value={config.epriceEuPreparationDays}
                onChange={(e) => {
                  const val = parseInt(e.target.value, 10);
                  if (!isNaN(val) && val >= 1 && val <= 30) {
                    onConfigChange({ epriceEuPreparationDays: val });
                  }
                }}
                className="w-full mt-1"
                disabled={disabled}
              />
            </div>
          </div>
        </div>
      </Card>
    </div>
  );
};

export default StockLocationConfig;
