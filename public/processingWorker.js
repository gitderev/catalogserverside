// processingWorker.ts
import Papa from 'papaparse';

interface FileData {
  name: string;
  data: any[];
  headers: string[];
}

interface ProcessedRecord {
  Matnr: string;
  ManufPartNr: string;
  EAN: string;
  ShortDescription: string;
  ExistingStock: number;
  ListPrice: number;
  CustBestPrice: number;
  IVA: string;
  'ListPrice con IVA': number;
  'CustBestPrice con IVA': number;
}

interface LogEntry {
  source_file: string;
  line: number;
  Matnr: string;
  ManufPartNr: string;
  EAN: string;
  reason: string;
  details: string;
}

interface ProcessingProgress {
  stage: string;
  progress: number;
  currentFile?: string;
  recordsProcessed?: number;
  totalRecords?: number;
}

// Half-up rounding to 2 decimals
function roundHalfUp(num: number): number {
  return Math.round((num + Number.EPSILON) * 100) / 100;
}

function isNumeric(value: any): boolean {
  return !isNaN(parseFloat(value)) && isFinite(value);
}

self.onmessage = function(e) {
  const { files } = e.data;
  
  try {
    processFiles(files);
  } catch (error) {
    self.postMessage({
      type: 'error',
      error: error instanceof Error ? error.message : 'Errore sconosciuto durante l\'elaborazione'
    });
  }
};

function processFiles(files: { material: FileData; stock: FileData; price: FileData }) {
  const logs: LogEntry[] = [];
  const processed: ProcessedRecord[] = [];
  
  // Progress tracking
  let currentProgress = 0;
  const updateProgress = (stage: string, progress: number, extra?: any) => {
    self.postMessage({
      type: 'progress',
      stage,
      progress,
      ...extra
    });
  };

  updateProgress('Inizializzazione', 5);

  // Create lookup maps for performance
  const stockMap = new Map<string, any>();
  const priceMap = new Map<string, any>();
  
  // Track duplicates
  const stockDuplicates = new Set<string>();
  const priceDuplicates = new Set<string>();

  updateProgress('Creazione indici stock', 15);
  
  // Build stock map and detect duplicates
  files.stock.data.forEach((record, index) => {
    const matnr = record.Matnr?.toString().trim();
    if (matnr) {
      if (stockMap.has(matnr)) {
        stockDuplicates.add(matnr);
        logs.push({
          source_file: 'StockFileData',
          line: index + 2, // +2 because header is line 1, data starts at line 2
          Matnr: matnr,
          ManufPartNr: record.ManufPartNr || '',
          EAN: record.EAN || '',
          reason: 'duplicato',
          details: 'Matnr duplicato nel file stock - mantenuto il primo'
        });
      } else {
        stockMap.set(matnr, record);
      }
    }
  });

  updateProgress('Creazione indici prezzi', 25);
  
  // Build price map and detect duplicates
  files.price.data.forEach((record, index) => {
    const matnr = record.Matnr?.toString().trim();
    if (matnr) {
      if (priceMap.has(matnr)) {
        priceDuplicates.add(matnr);
        logs.push({
          source_file: 'pricefileData',
          line: index + 2,
          Matnr: matnr,
          ManufPartNr: record.ManufPartNr || '',
          EAN: record.EAN || '',
          reason: 'duplicato',
          details: 'Matnr duplicato nel file prezzi - mantenuto il primo'
        });
      } else {
        priceMap.set(matnr, record);
      }
    }
  });

  updateProgress('Elaborazione record principali', 35);

  const totalMaterialRecords = files.material.data.length;
  
  // Process material records (base for left join)
  files.material.data.forEach((materialRecord, index) => {
    const progress = 35 + Math.floor((index / totalMaterialRecords) * 50);
    if (index % 100 === 0) {
      updateProgress('Elaborazione record', progress, {
        recordsProcessed: index,
        totalRecords: totalMaterialRecords
      });
    }

    const matnr = materialRecord.Matnr?.toString().trim();
    if (!matnr) {
      logs.push({
        source_file: 'MaterialFile',
        line: index + 2,
        Matnr: '',
        ManufPartNr: materialRecord.ManufPartNr || '',
        EAN: materialRecord.EAN || '',
        reason: 'Matnr vuoto',
        details: 'Codice materiale mancante'
      });
      return;
    }

    // Get related records
    const stockRecord = stockMap.get(matnr);
    const priceRecord = priceMap.get(matnr);

    // Combine data
    const combined = {
      ...materialRecord,
      ...stockRecord,
      ...priceRecord
    };

    // Apply filters
    const ean = combined.EAN?.toString().trim();
    if (!ean) {
      logs.push({
        source_file: 'MaterialFile',
        line: index + 2,
        Matnr: matnr,
        ManufPartNr: combined.ManufPartNr || '',
        EAN: '',
        reason: 'EAN vuoto',
        details: 'Campo EAN mancante o vuoto'
      });
      return;
    }

    const existingStock = combined.ExistingStock;
    if (!isNumeric(existingStock) || parseFloat(existingStock) <= 0) {
      logs.push({
        source_file: 'MaterialFile',
        line: index + 2,
        Matnr: matnr,
        ManufPartNr: combined.ManufPartNr || '',
        EAN: ean,
        reason: 'ExistingStock non valido',
        details: `ExistingStock: ${existingStock} (deve essere > 0)`
      });
      return;
    }

    const listPrice = combined.ListPrice;
    if (!isNumeric(listPrice)) {
      logs.push({
        source_file: 'MaterialFile',
        line: index + 2,
        Matnr: matnr,
        ManufPartNr: combined.ManufPartNr || '',
        EAN: ean,
        reason: 'ListPrice non valido',
        details: `ListPrice: ${listPrice} (deve essere numerico)`
      });
      return;
    }

    const custBestPrice = combined.CustBestPrice;
    if (!custBestPrice || custBestPrice.toString().trim() === '' || !isNumeric(custBestPrice)) {
      logs.push({
        source_file: 'MaterialFile',
        line: index + 2,
        Matnr: matnr,
        ManufPartNr: combined.ManufPartNr || '',
        EAN: ean,
        reason: 'CustBestPrice non valido',
        details: `CustBestPrice: ${custBestPrice} (deve essere presente e numerico)`
      });
      return;
    }

    // Calculate prices with IVA (22%)
    const listPriceNum = parseFloat(listPrice);
    const custBestPriceNum = parseFloat(custBestPrice);
    const listPriceWithIVA = roundHalfUp(listPriceNum * 1.22);
    const custBestPriceWithIVA = roundHalfUp(custBestPriceNum * 1.22);

    // Create processed record
    processed.push({
      Matnr: matnr,
      ManufPartNr: combined.ManufPartNr || '',
      EAN: ean,
      ShortDescription: combined.ShortDescription || '',
      ExistingStock: parseInt(existingStock),
      ListPrice: listPriceNum,
      CustBestPrice: custBestPriceNum,
      IVA: '22%',
      'ListPrice con IVA': listPriceWithIVA,
      'CustBestPrice con IVA': custBestPriceWithIVA
    });
  });

  updateProgress('Finalizzazione', 95);

  // Calculate statistics
  const stats = {
    totalRecords: files.material.data.length,
    validRecords: processed.length,
    filteredRecords: logs.length,
    stockDuplicates: stockDuplicates.size,
    priceDuplicates: priceDuplicates.size
  };

  updateProgress('Completato', 100);

  // Send results
  self.postMessage({
    type: 'complete',
    processedData: processed,
    logEntries: logs,
    stats
  });
};