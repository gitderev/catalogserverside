import Papa from 'papaparse';
import * as XLSX from 'xlsx';

export interface ParsedTXT {
  data: any[];
  headers: string[];
}

// Detect delimiter in TXT file
const detectDelimiter = (text: string): string => {
  const sample = text.substring(0, 1000); // Use first 1000 chars
  const delimiters = ['\t', ',', ';', '|'];
  let maxCount = 0;
  let bestDelimiter = ',';

  delimiters.forEach(delimiter => {
    const count = (sample.match(new RegExp(`\\${delimiter}`, 'g')) || []).length;
    if (count > maxCount) {
      maxCount = count;
      bestDelimiter = delimiter;
    }
  });

  return bestDelimiter;
};

export const parseTXT = (file: File): Promise<ParsedTXT> => {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    
    reader.onload = (event) => {
      const text = event.target?.result as string;
      const delimiter = detectDelimiter(text);
      
      Papa.parse(text, {
        delimiter: delimiter,
        header: true,
        skipEmptyLines: true,
        // CRITICAL: Disable dynamic typing to prevent MPN/SKU numeric coercion
        dynamicTyping: false,
        transformHeader: (header: string) => header.trim(),
        transform: (value: string) => String(value ?? '').trim(),
        complete: (results) => {
          if (results.errors.length > 0) {
            reject(new Error('Errore nel parsing del TXT: ' + results.errors[0].message));
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
          reject(new Error('Errore nel parsing del TXT: ' + error.message));
        }
      });
    };
    
    reader.onerror = () => reject(new Error('Errore nella lettura del file'));
    reader.readAsText(file);
  });
};

export const mergeMultipleTXTData = (
  materialData: any[],
  additionalFiles: { data: any[]; skuColumn: string; fileName: string }[]
): any[] => {
  const mergedData: any[] = [];
  
  // Create maps for each additional file
  const fileMaps = additionalFiles.map(file => {
    const map = new Map();
    file.data.forEach(row => {
      const sku = row[file.skuColumn];
      if (sku) {
        map.set(sku.toString().trim(), row);
      }
    });
    return { map, fileName: file.fileName };
  });
  
  // Process each product in the material file
  materialData.forEach(materialRow => {
    const materialSku = materialRow['SKU'] || materialRow['sku'] || materialRow['Sku'] || Object.values(materialRow)[0];
    if (!materialSku) return;
    
    const skuKey = materialSku.toString().trim();
    const mergedRow: any = { ...materialRow };
    
    // Add data from each additional file
    fileMaps.forEach(({ map, fileName }, index) => {
      const additionalRow = map.get(skuKey);
      if (additionalRow) {
        Object.keys(additionalRow).forEach(key => {
          // Avoid overwriting existing columns, prefix with file identifier
          const newKey = mergedRow.hasOwnProperty(key) && key !== additionalFiles[index].skuColumn
            ? `${fileName}_${key}`
            : key;
          mergedRow[newKey] = additionalRow[key];
        });
      }
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