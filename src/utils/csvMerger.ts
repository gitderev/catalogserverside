import Papa from 'papaparse';
import * as XLSX from 'xlsx';

export interface ParsedCSV {
  data: any[];
  headers: string[];
}

export const parseCSV = (file: File): Promise<ParsedCSV> => {
  return new Promise((resolve, reject) => {
    Papa.parse(file, {
      header: true,
      skipEmptyLines: true,
      // CRITICAL: Disable dynamic typing to prevent MPN/SKU numeric coercion
      dynamicTyping: false,
      transform: (value: string) => String(value ?? '').trim(),
      complete: (results) => {
        if (results.errors.length > 0) {
          reject(new Error('Errore nel parsing del CSV: ' + results.errors[0].message));
          return;
        }
        
        const data = results.data as any[];
        const headers = Object.keys(data[0] || {});
        
        // Post-parse integrity check
        const identifierFields = ['ManufPartNr', 'Matnr', 'ManufacturerPartNo', 'EAN', 'SKU', 'mpn', 'ean'];
        data.forEach((row) => {
          for (const field of identifierFields) {
            if (field in row && typeof row[field] === 'number') {
              row[field] = String(row[field]);
            }
          }
        });
        
        resolve({ data, headers });
      },
      error: (error) => {
        reject(new Error('Errore nel parsing del CSV: ' + error.message));
      }
    });
  });
};

export const mergeCSVData = (
  data1: any[],
  data2: any[],
  skuColumn1: string,
  skuColumn2: string
): any[] => {
  const mergedData: any[] = [];
  const data2Map = new Map();
  
  // Crea una mappa dei dati del secondo file usando il SKU come chiave
  data2.forEach(row => {
    const sku = row[skuColumn2];
    if (sku) {
      data2Map.set(sku, row);
    }
  });
  
  // Processa i dati del primo file
  data1.forEach(row1 => {
    const sku = row1[skuColumn1];
    if (!sku) return;
    
    const row2 = data2Map.get(sku);
    
    if (row2) {
      // Merge delle righe - rinomina le colonne duplicate
      const mergedRow: any = {};
      
      // Aggiungi colonne dal primo file
      Object.keys(row1).forEach(key => {
        mergedRow[`File1_${key}`] = row1[key];
      });
      
      // Aggiungi colonne dal secondo file
      Object.keys(row2).forEach(key => {
        const newKey = Object.keys(row1).includes(key) && key !== skuColumn1 
          ? `File2_${key}` 
          : key;
        mergedRow[newKey] = row2[key];
      });
      
      // Aggiungi una colonna SKU unificata
      mergedRow['SKU'] = sku;
      
      mergedData.push(mergedRow);
      data2Map.delete(sku); // Rimuovi per evitare duplicati
    } else {
      // SKU presente solo nel primo file
      const mergedRow: any = { SKU: sku };
      Object.keys(row1).forEach(key => {
        mergedRow[`File1_${key}`] = row1[key];
      });
      mergedData.push(mergedRow);
    }
  });
  
  // Aggiungi le righe del secondo file che non hanno match
  data2Map.forEach((row2, sku) => {
    const mergedRow: any = { SKU: sku };
    Object.keys(row2).forEach(key => {
      mergedRow[`File2_${key}`] = row2[key];
    });
    mergedData.push(mergedRow);
  });
  
  return mergedData;
};

export const exportToExcel = (data: any[], filename: string = 'merged_data'): void => {
  const worksheet = XLSX.utils.json_to_sheet(data);
  const workbook = XLSX.utils.book_new();
  
  // Imposta la larghezza delle colonne
  const maxWidth = 30;
  const wscols = Object.keys(data[0] || {}).map(() => ({ width: maxWidth }));
  worksheet['!cols'] = wscols;
  
  XLSX.utils.book_append_sheet(workbook, worksheet, 'Dati Uniti');
  
  // Scarica il file
  XLSX.writeFile(workbook, `${filename}.xlsx`);
};

export const autoDetectSKUColumn = (headers: string[]): string => {
  const skuPatterns = [
    /^sku$/i,
    /^product_sku$/i,
    /^productsku$/i,
    /^product.*sku$/i,
    /^sku.*code$/i,
    /^codice$/i,
    /^code$/i,
    /^id$/i,
    /^product.*id$/i,
    /^item.*code$/i
  ];
  
  for (const pattern of skuPatterns) {
    const match = headers.find(header => pattern.test(header));
    if (match) return match;
  }
  
  return headers[0] || '';
};