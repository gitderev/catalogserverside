import React from 'react';
import { Card } from '@/components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { ScrollArea } from '@/components/ui/scroll-area';

interface DataPreviewProps {
  data: any[];
  title: string;
  maxRows?: number;
}

export const DataPreview: React.FC<DataPreviewProps> = ({ 
  data, 
  title, 
  maxRows = 5 
}) => {
  if (!data || data.length === 0) {
    return null;
  }

  const headers = Object.keys(data[0] || {});
  const previewData = data.slice(0, maxRows);

  return (
    <Card className="p-6">
      <h3 className="text-lg font-semibold text-card-foreground mb-4">{title}</h3>
      <div className="text-sm text-muted-foreground mb-4">
        Mostrando {previewData.length} di {data.length} righe
      </div>
      
      <ScrollArea className="h-[300px] w-full rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              {headers.map((header, index) => (
                <TableHead key={index} className="font-semibold">
                  {header}
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {previewData.map((row, rowIndex) => (
              <TableRow key={rowIndex}>
                {headers.map((header, colIndex) => (
                  <TableCell key={colIndex} className="text-sm">
                    {row[header] || '-'}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </ScrollArea>
    </Card>
  );
};