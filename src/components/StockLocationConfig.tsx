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
 * Parse fee/multiplier input - accepts comma or dot as decimal separator
 * Returns the parsed number or null if invalid
 */
function parseFeeInput(value: string): number | null {
  const trimmed = value.trim();
  if (!trimmed) return null;
  // Replace comma with dot for parsing
  const normalized = trimmed.replace(',', '.');
  const parsed = parseFloat(normalized);
  if (isNaN(parsed) || !isFinite(parsed)) return null;
  return parsed;
}

/**
 * Parse shipping cost input - accepts IT euro format (comma as decimal)
 * Returns the parsed number or null if invalid
 */
function parseShippingInput(value: string): number | null {
  const trimmed = value.trim();
  if (!trimmed) return null;
  // Remove € symbol and spaces
  let normalized = trimmed.replace(/[€\s]/g, '');
  // Replace comma with dot for parsing
  normalized = normalized.replace(',', '.');
  const parsed = parseFloat(normalized);
  if (isNaN(parsed) || !isFinite(parsed)) return null;
  return parsed;
}

/**
 * Format number for display in input (uses comma as decimal separator)
 */
function formatForDisplay(value: number | null, fallback: number): string {
  const num = value ?? fallback;
  return num.toFixed(2).replace('.', ',');
}

/**
 * Stock Location and Per-Export Pricing Configuration Component
 * 
 * Provides controls for:
 * - IT/EU stock split configuration per marketplace
 * - Per-export pricing: Fee DeRev, Fee Marketplace, Shipping Cost
 * 
 * Per-export pricing fallback: if per-export is null, uses global values
 */
const StockLocationConfig: React.FC<StockLocationConfigProps> = ({
  config,
  onConfigChange,
  disabled = false
}) => {
  // Helper to get effective value with fallback
  const getEffectiveValue = (perExport: number | null, global: number) => perExport ?? global;

  return (
    <div className="space-y-6">
      {/* EAN Export Configuration */}
      <Card className="p-4 border-green-200">
        <h4 className="text-lg font-semibold mb-4 text-green-800">Catalogo EAN - Configurazione Pricing</h4>
        
        <div className="space-y-4">
          {/* Per-export pricing fields */}
          <div className="grid grid-cols-3 gap-4">
            <div>
              <Label htmlFor="ean-fee-drev" className="text-sm font-medium">
                Fee DeRev (moltiplicatore)
              </Label>
              <Input
                id="ean-fee-drev"
                type="text"
                inputMode="decimal"
                value={formatForDisplay(config.eanFeeDrev, config.feeDrev)}
                onChange={(e) => {
                  const parsed = parseFeeInput(e.target.value);
                  if (parsed !== null && parsed > 0) {
                    onConfigChange({ eanFeeDrev: parsed });
                  }
                }}
                onBlur={(e) => {
                  // If empty or invalid, set to current effective value
                  const parsed = parseFeeInput(e.target.value);
                  if (parsed === null || parsed <= 0) {
                    onConfigChange({ eanFeeDrev: config.feeDrev });
                  }
                }}
                className="w-full mt-1"
                disabled={disabled}
                placeholder={`${config.feeDrev}`}
              />
              <p className="text-xs text-muted-foreground mt-1">
                Globale: {config.feeDrev}
              </p>
            </div>
            <div>
              <Label htmlFor="ean-fee-mkt" className="text-sm font-medium">
                Fee Marketplace (moltiplicatore)
              </Label>
              <Input
                id="ean-fee-mkt"
                type="text"
                inputMode="decimal"
                value={formatForDisplay(config.eanFeeMkt, config.feeMkt)}
                onChange={(e) => {
                  const parsed = parseFeeInput(e.target.value);
                  if (parsed !== null && parsed > 0) {
                    onConfigChange({ eanFeeMkt: parsed });
                  }
                }}
                onBlur={(e) => {
                  const parsed = parseFeeInput(e.target.value);
                  if (parsed === null || parsed <= 0) {
                    onConfigChange({ eanFeeMkt: config.feeMkt });
                  }
                }}
                className="w-full mt-1"
                disabled={disabled}
                placeholder={`${config.feeMkt}`}
              />
              <p className="text-xs text-muted-foreground mt-1">
                Globale: {config.feeMkt}
              </p>
            </div>
            <div>
              <Label htmlFor="ean-shipping" className="text-sm font-medium">
                Costo spedizione (€ netti IVA)
              </Label>
              <Input
                id="ean-shipping"
                type="text"
                inputMode="decimal"
                value={formatForDisplay(config.eanShippingCost, config.shippingCost)}
                onChange={(e) => {
                  const parsed = parseShippingInput(e.target.value);
                  if (parsed !== null && parsed >= 0) {
                    onConfigChange({ eanShippingCost: parsed });
                  }
                }}
                onBlur={(e) => {
                  const parsed = parseShippingInput(e.target.value);
                  if (parsed === null || parsed < 0) {
                    onConfigChange({ eanShippingCost: config.shippingCost });
                  }
                }}
                className="w-full mt-1"
                disabled={disabled}
                placeholder={`€ ${config.shippingCost}`}
              />
              <p className="text-xs text-muted-foreground mt-1">
                Globale: € {config.shippingCost.toFixed(2)}
              </p>
            </div>
          </div>
          
          <p className="text-xs text-green-600 bg-green-50 p-2 rounded">
            Questi valori determinano "Prezzo Finale" e "ListPrice con Fee" nel Catalogo EAN
          </p>
        </div>
      </Card>

      {/* Mediaworld Configuration */}
      <Card className="p-4 border-amber-200">
        <h4 className="text-lg font-semibold mb-4 text-amber-800">Mediaworld - Configurazione Stock IT/EU e Pricing</h4>
        
        <div className="space-y-4">
          {/* Per-export pricing fields */}
          <div className="grid grid-cols-3 gap-4">
            <div>
              <Label htmlFor="mediaworld-fee-drev" className="text-sm font-medium">
                Fee DeRev (moltiplicatore)
              </Label>
              <Input
                id="mediaworld-fee-drev"
                type="text"
                inputMode="decimal"
                value={formatForDisplay(config.mediaworldFeeDrev, config.feeDrev)}
                onChange={(e) => {
                  const parsed = parseFeeInput(e.target.value);
                  if (parsed !== null && parsed > 0) {
                    onConfigChange({ mediaworldFeeDrev: parsed });
                  }
                }}
                onBlur={(e) => {
                  const parsed = parseFeeInput(e.target.value);
                  if (parsed === null || parsed <= 0) {
                    onConfigChange({ mediaworldFeeDrev: config.feeDrev });
                  }
                }}
                className="w-full mt-1"
                disabled={disabled}
                placeholder={`${config.feeDrev}`}
              />
              <p className="text-xs text-muted-foreground mt-1">
                Globale: {config.feeDrev}
              </p>
            </div>
            <div>
              <Label htmlFor="mediaworld-fee-mkt" className="text-sm font-medium">
                Fee Marketplace (moltiplicatore)
              </Label>
              <Input
                id="mediaworld-fee-mkt"
                type="text"
                inputMode="decimal"
                value={formatForDisplay(config.mediaworldFeeMkt, config.feeMkt)}
                onChange={(e) => {
                  const parsed = parseFeeInput(e.target.value);
                  if (parsed !== null && parsed > 0) {
                    onConfigChange({ mediaworldFeeMkt: parsed });
                  }
                }}
                onBlur={(e) => {
                  const parsed = parseFeeInput(e.target.value);
                  if (parsed === null || parsed <= 0) {
                    onConfigChange({ mediaworldFeeMkt: config.feeMkt });
                  }
                }}
                className="w-full mt-1"
                disabled={disabled}
                placeholder={`${config.feeMkt}`}
              />
              <p className="text-xs text-muted-foreground mt-1">
                Globale: {config.feeMkt}
              </p>
            </div>
            <div>
              <Label htmlFor="mediaworld-shipping" className="text-sm font-medium">
                Costo spedizione (€ netti IVA)
              </Label>
              <Input
                id="mediaworld-shipping"
                type="text"
                inputMode="decimal"
                value={formatForDisplay(config.mediaworldShippingCost, config.shippingCost)}
                onChange={(e) => {
                  const parsed = parseShippingInput(e.target.value);
                  if (parsed !== null && parsed >= 0) {
                    onConfigChange({ mediaworldShippingCost: parsed });
                  }
                }}
                onBlur={(e) => {
                  const parsed = parseShippingInput(e.target.value);
                  if (parsed === null || parsed < 0) {
                    onConfigChange({ mediaworldShippingCost: config.shippingCost });
                  }
                }}
                className="w-full mt-1"
                disabled={disabled}
                placeholder={`€ ${config.shippingCost}`}
              />
              <p className="text-xs text-muted-foreground mt-1">
                Globale: € {config.shippingCost.toFixed(2)}
              </p>
            </div>
          </div>

          {/* Include EU Toggle */}
          <div className="flex items-center justify-between p-3 rounded-lg bg-amber-50">
            <div>
              <Label htmlFor="mediaworld-include-eu" className="text-sm font-medium">
                Includi magazzino EU per Mediaworld
              </Label>
              <p className="text-xs text-muted-foreground mt-1">
                {config.mediaworldIncludeEu 
                  ? '✅ Stock EU abilitato: se IT < 2 usa IT+EU combinato'
                  : '⚠️ Solo stock IT: prodotti EU-only saranno esclusi'}
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
              <p className="text-xs text-green-600 mt-1">
                Lead time export: <strong>{config.mediaworldItPreparationDays}</strong> giorni
              </p>
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
              <p className="text-xs text-blue-600 mt-1">
                Lead time export: <strong>{config.mediaworldEuPreparationDays}</strong> giorni
              </p>
            </div>
          </div>
        </div>
      </Card>

      {/* ePrice Configuration */}
      <Card className="p-4 border-blue-200">
        <h4 className="text-lg font-semibold mb-4 text-blue-800">ePrice - Configurazione Stock IT/EU e Pricing</h4>
        
        <div className="space-y-4">
          {/* Per-export pricing fields */}
          <div className="grid grid-cols-3 gap-4">
            <div>
              <Label htmlFor="eprice-fee-drev" className="text-sm font-medium">
                Fee DeRev (moltiplicatore)
              </Label>
              <Input
                id="eprice-fee-drev"
                type="text"
                inputMode="decimal"
                value={formatForDisplay(config.epriceFeeDrev, config.feeDrev)}
                onChange={(e) => {
                  const parsed = parseFeeInput(e.target.value);
                  if (parsed !== null && parsed > 0) {
                    onConfigChange({ epriceFeeDrev: parsed });
                  }
                }}
                onBlur={(e) => {
                  const parsed = parseFeeInput(e.target.value);
                  if (parsed === null || parsed <= 0) {
                    onConfigChange({ epriceFeeDrev: config.feeDrev });
                  }
                }}
                className="w-full mt-1"
                disabled={disabled}
                placeholder={`${config.feeDrev}`}
              />
              <p className="text-xs text-muted-foreground mt-1">
                Globale: {config.feeDrev}
              </p>
            </div>
            <div>
              <Label htmlFor="eprice-fee-mkt" className="text-sm font-medium">
                Fee Marketplace (moltiplicatore)
              </Label>
              <Input
                id="eprice-fee-mkt"
                type="text"
                inputMode="decimal"
                value={formatForDisplay(config.epriceFeeMkt, config.feeMkt)}
                onChange={(e) => {
                  const parsed = parseFeeInput(e.target.value);
                  if (parsed !== null && parsed > 0) {
                    onConfigChange({ epriceFeeMkt: parsed });
                  }
                }}
                onBlur={(e) => {
                  const parsed = parseFeeInput(e.target.value);
                  if (parsed === null || parsed <= 0) {
                    onConfigChange({ epriceFeeMkt: config.feeMkt });
                  }
                }}
                className="w-full mt-1"
                disabled={disabled}
                placeholder={`${config.feeMkt}`}
              />
              <p className="text-xs text-muted-foreground mt-1">
                Globale: {config.feeMkt}
              </p>
            </div>
            <div>
              <Label htmlFor="eprice-shipping" className="text-sm font-medium">
                Costo spedizione (€ netti IVA)
              </Label>
              <Input
                id="eprice-shipping"
                type="text"
                inputMode="decimal"
                value={formatForDisplay(config.epriceShippingCost, config.shippingCost)}
                onChange={(e) => {
                  const parsed = parseShippingInput(e.target.value);
                  if (parsed !== null && parsed >= 0) {
                    onConfigChange({ epriceShippingCost: parsed });
                  }
                }}
                onBlur={(e) => {
                  const parsed = parseShippingInput(e.target.value);
                  if (parsed === null || parsed < 0) {
                    onConfigChange({ epriceShippingCost: config.shippingCost });
                  }
                }}
                className="w-full mt-1"
                disabled={disabled}
                placeholder={`€ ${config.shippingCost}`}
              />
              <p className="text-xs text-muted-foreground mt-1">
                Globale: € {config.shippingCost.toFixed(2)}
              </p>
            </div>
          </div>

          {/* Include EU Toggle */}
          <div className="flex items-center justify-between p-3 rounded-lg bg-blue-50">
            <div>
              <Label htmlFor="eprice-include-eu" className="text-sm font-medium">
                Includi magazzino EU per ePrice
              </Label>
              <p className="text-xs text-muted-foreground mt-1">
                {config.epriceIncludeEu 
                  ? '✅ Stock EU abilitato: se IT < 2 usa IT+EU combinato'
                  : '⚠️ Solo stock IT: prodotti EU-only saranno esclusi'}
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
              <p className="text-xs text-green-600 mt-1">
                Lead time export: <strong>{config.epriceItPreparationDays}</strong> giorni (no offset)
              </p>
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
              <p className="text-xs text-blue-600 mt-1">
                Lead time export: <strong>{config.epriceEuPreparationDays}</strong> giorni (no offset)
              </p>
            </div>
          </div>
        </div>
      </Card>

      {/* Legend */}
      <div className="text-xs text-muted-foreground p-3 bg-gray-50 rounded-lg">
        <p><strong>Pricing per-export:</strong> Ogni export usa i suoi Fee DeRev, Fee Marketplace e Costo Spedizione per calcolare i prezzi finali</p>
        <p className="mt-1"><strong>Logica stock:</strong> Se IT ≥ 2 → usa IT; altrimenti se EU abilitato e IT+EU ≥ 2 → usa combinato con lead time EU</p>
        <p className="mt-1"><strong>Lead time:</strong> export = giorni preparazione configurati (nessun offset)</p>
      </div>
    </div>
  );
};

export default StockLocationConfig;