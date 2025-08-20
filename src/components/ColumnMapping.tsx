import React from 'react';
import { Card } from '@/components/ui/card';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Label } from '@/components/ui/label';

interface ColumnMappingProps {
  file1Headers: string[];
  file2Headers: string[];
  skuColumn1: string;
  skuColumn2: string;
  onSkuColumn1Change: (value: string) => void;
  onSkuColumn2Change: (value: string) => void;
}

export const ColumnMapping: React.FC<ColumnMappingProps> = ({
  file1Headers,
  file2Headers,
  skuColumn1,
  skuColumn2,
  onSkuColumn1Change,
  onSkuColumn2Change
}) => {
  return (
    <Card className="p-6">
      <h3 className="text-lg font-semibold text-card-foreground mb-4">
        Configurazione Colonne SKU
      </h3>
      <p className="text-muted-foreground mb-6">
        Seleziona le colonne che contengono i codici SKU per ciascun file
      </p>
      
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div className="space-y-2">
          <Label htmlFor="sku-column-1" className="text-sm font-medium">
            Colonna SKU - File 1
          </Label>
          <Select value={skuColumn1} onValueChange={onSkuColumn1Change}>
            <SelectTrigger>
              <SelectValue placeholder="Seleziona colonna SKU" />
            </SelectTrigger>
            <SelectContent>
              {file1Headers.map((header) => (
                <SelectItem key={header} value={header}>
                  {header}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        
        <div className="space-y-2">
          <Label htmlFor="sku-column-2" className="text-sm font-medium">
            Colonna SKU - File 2
          </Label>
          <Select value={skuColumn2} onValueChange={onSkuColumn2Change}>
            <SelectTrigger>
              <SelectValue placeholder="Seleziona colonna SKU" />
            </SelectTrigger>
            <SelectContent>
              {file2Headers.map((header) => (
                <SelectItem key={header} value={header}>
                  {header}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>
    </Card>
  );
};