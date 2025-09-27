import React from 'react';
import { Card } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Calculator, Euro } from 'lucide-react';

interface CalculationRulesProps {
  feeDeRev: number;
  feeMarketplace: number;
  onFeeDeRevChange: (value: number) => void;
  onFeeMarketplaceChange: (value: number) => void;
}

export const CalculationRules: React.FC<CalculationRulesProps> = ({
  feeDeRev,
  feeMarketplace,
  onFeeDeRevChange,
  onFeeMarketplaceChange
}) => {
  return (
    <Card className="p-6">
      <div className="flex items-center gap-3 mb-4">
        <Calculator className="h-6 w-6 text-primary" />
        <h3 className="text-lg font-semibold">Regole di Calcolo</h3>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div className="space-y-2">
          <Label htmlFor="fee-derev">FeeDeRev (moltiplicatore ≥1,00)</Label>
          <div className="relative">
            <Euro className="absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
            <Input
              id="fee-derev"
              type="number"
              step="0.01"
              min="1.00"
              value={feeDeRev}
              onChange={(e) => onFeeDeRevChange(Math.max(1.00, parseFloat(e.target.value) || 1.00))}
              className="pl-8"
              placeholder="1.00"
            />
          </div>
        </div>

        <div className="space-y-2">
          <Label htmlFor="fee-marketplace">Fee Marketplace (moltiplicatore ≥1,00)</Label>
          <div className="relative">
            <Euro className="absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
            <Input
              id="fee-marketplace"
              type="number"
              step="0.01"
              min="1.00"
              value={feeMarketplace}
              onChange={(e) => onFeeMarketplaceChange(Math.max(1.00, parseFloat(e.target.value) || 1.00))}
              className="pl-8"
              placeholder="1.00"
            />
          </div>
        </div>
      </div>

      <div className="mt-6 p-4 bg-muted rounded-lg">
        <h4 className="font-medium mb-2">Regole Applicate:</h4>
        <ul className="text-sm text-muted-foreground space-y-1">
          <li>• SKU: righe con ManufPartNr non vuoto, anche con EAN vuoto</li>
          <li>• Filtro: ExistingStock &gt; 1</li>
          <li>• Prezzo base: CustBestPrice, fallback ListPrice</li>
          <li>• Spedizione: 6€ fissi</li>
          <li>• IVA: 22% percentuale fissa</li>
          <li>• Ordine fee: prima DeRev, poi Marketplace</li>
          <li>• Prezzo finale: arrotondato all'euro superiore</li>
        </ul>
      </div>
    </Card>
  );
};