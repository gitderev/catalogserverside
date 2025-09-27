// alterside-sku-worker.js - Self-contained SKU processing worker
// Optimized for performance with batch processing and minimal overhead

let isProcessing = false;
let shouldCancel = false;
let indexByMPN = new Map();
let indexByEAN = new Map();

// Send ready signal immediately on worker start
self.postMessage({ type: 'worker_ready' });

// Wrap all message handling in try/catch for error safety
self.onmessage = function(e) {
  try {
    const { type, data } = e.data;
    
    if (type === 'cancel') {
      shouldCancel = true;
      return;
    }
    
    if (type === 'prescan') {
      try {
        shouldCancel = false;
        isProcessing = true;
        performPreScan(data);
      } catch (error) {
        self.postMessage({
          type: 'worker_error',
          message: error instanceof Error ? error.message : 'Errore durante pre-scan',
          stack: error instanceof Error ? error.stack : null
        });
      } finally {
        isProcessing = false;
      }
    }
    
    if (type === 'process') {
      try {
        shouldCancel = false;
        isProcessing = true;
        processSkuCatalog(data);
      } catch (error) {
        self.postMessage({
          type: 'worker_error',
          message: error instanceof Error ? error.message : 'Errore sconosciuto durante l\'elaborazione SKU',
          stack: error instanceof Error ? error.stack : null
        });
      } finally {
        isProcessing = false;
      }
    }
  } catch (globalError) {
    // Catch any top-level errors in message handling
    self.postMessage({
      type: 'worker_error',
      message: globalError instanceof Error ? globalError.message : 'Errore critico nel worker',
      stack: globalError instanceof Error ? globalError.stack : null
    });
  }
};

// Pre-scan function to build indices
async function performPreScan({ materialData, stockData, priceData }) {
  const startTime = Date.now();
  const BATCH_SIZE = 2000;
  
  let processed = 0;
  const totalData = materialData.length + stockData.length + priceData.length;
  
  self.postMessage({
    type: 'prescan_progress',
    progress: 0
  });
  
  // Build ManufPartNr index
  indexByMPN.clear();
  for (let i = 0; i < materialData.length; i += BATCH_SIZE) {
    if (shouldCancel) {
      self.postMessage({ type: 'cancelled' });
      return;
    }
    
    const batch = materialData.slice(i, Math.min(i + BATCH_SIZE, materialData.length));
    
    for (const record of batch) {
      const mpn = String(record.ManufPartNr ?? '').trim();
      if (mpn) {
        indexByMPN.set(mpn, record);
      }
      processed++;
    }
    
    const progress = Math.round((processed / totalData) * 100);
    self.postMessage({
      type: 'prescan_progress',
      progress: Math.min(progress, 33)
    });
    
    // Yield to main thread
    await new Promise(resolve => setTimeout(resolve, 0));
  }
  
  // Build EAN index
  indexByEAN.clear();
  for (let i = 0; i < materialData.length; i += BATCH_SIZE) {
    if (shouldCancel) {
      self.postMessage({ type: 'cancelled' });
      return;
    }
    
    const batch = materialData.slice(i, Math.min(i + BATCH_SIZE, materialData.length));
    
    for (const record of batch) {
      const ean = String(record.EAN ?? '').trim();
      if (ean) {
        indexByEAN.set(ean, record);
      }
      processed++;
    }
    
    const progress = Math.round((processed / totalData) * 100);
    self.postMessage({
      type: 'prescan_progress',
      progress: Math.min(progress, 66)
    });
    
    // Yield to main thread
    await new Promise(resolve => setTimeout(resolve, 0));
  }
  
  // Index stock and price data
  for (let i = 0; i < stockData.length + priceData.length; i++) {
    processed++;
  }
  
  const processingTime = Date.now() - startTime;
  
  self.postMessage({
    type: 'prescan_done',
    counts: {
      mpnRecords: indexByMPN.size,
      eanRecords: indexByEAN.size,
      totalMaterial: materialData.length,
      processingTimeMs: processingTime
    }
  });
}

// Utility functions aligned with EAN architecture
function parseEuroLike(input) {
  if (typeof input === 'number' && isFinite(input)) return input;
  let s = String(input ?? '').trim();
  s = s.replace(/[^\d.,\s%\-]/g, '').trim();
  s = s.split(/\s+/)[0] ?? '';
  s = s.replace(/%/g, '').trim();
  if (!s) return NaN;

  if (s.includes('.') && s.includes(',')) s = s.replace(/\./g, '').replace(',', '.');
  else s = s.replace(',', '.');

  const n = parseFloat(s);
  return Number.isFinite(n) ? n : NaN;
}

function toCents(x, fallback = 0) {
  const n = parseEuroLike(x);
  return Number.isFinite(n) ? Math.round(n * 100) : Math.round(fallback * 100);
}

function sanitizeEAN(ean) {
  if (!ean) return '';
  const str = String(ean).trim();
  if (!str) return '';
  
  // Sanitize if starts with formula chars or contains control chars
  const needsSanitization = /^[=+\-@]/.test(str) || /[\x00-\x1F\x7F]/.test(str);
  return needsSanitization ? `'${str}` : str;
}

function validateMultiplier(value) {
  const num = parseEuroLike(value);
  return Number.isFinite(num) && num >= 1.0 ? num : null;
}

async function processSkuCatalog({ sourceRows, fees }) {
  const startTime = Date.now();
  const INITIAL_BATCH_SIZE = 2000;
  const MIN_BATCH_SIZE = 1000;
  const MAX_BATCH_TIME_MS = 1500;
  
  let batchSize = INITIAL_BATCH_SIZE;
  const totalRows = sourceRows.length;
  let processedCount = 0;
  
  const results = [];
  const rejects = [];
  
  // Validate and convert fees
  const feeDeRevMultiplier = validateMultiplier(fees.feeDeRev);
  const feeMktMultiplier = validateMultiplier(fees.feeMarketplace);
  
  if (feeDeRevMultiplier === null || feeMktMultiplier === null) {
    throw new Error('Inserisci un moltiplicatore ≥ 1,00');
  }
  
  const feeDeRevPercent = feeDeRevMultiplier - 1;
  const feeMktPercent = feeMktMultiplier - 1;
  
  self.postMessage({
    type: 'progress',
    stage: 'Elaborazione SKU',
    progress: 0,
    recordsProcessed: 0,
    totalRecords: totalRows
  });
  
  // Process in batches
  for (let i = 0; i < totalRows; i += batchSize) {
    if (shouldCancel) {
      self.postMessage({ type: 'cancelled' });
      return;
    }
    
    const batchStart = Date.now();
    const batch = sourceRows.slice(i, Math.min(i + batchSize, totalRows));
    
    // Process batch
    for (const row of batch) {
      const result = processSkuRow(row, feeDeRevPercent, feeMktPercent, rejects, processedCount);
      if (result) {
        results.push(result);
      }
      processedCount++;
    }
    
    const batchTime = Date.now() - batchStart;
    
    // Adjust batch size if processing is too slow
    if (batchTime > MAX_BATCH_TIME_MS && batchSize > MIN_BATCH_SIZE) {
      batchSize = MIN_BATCH_SIZE;
    }
    
    // Send progress update
    const progress = Math.round((processedCount / totalRows) * 100);
    self.postMessage({
      type: 'progress',
      stage: 'Elaborazione SKU',
      progress,
      recordsProcessed: processedCount,
      totalRecords: totalRows
    });
    
    // Yield to main thread
    await new Promise(resolve => setTimeout(resolve, 0));
  }
  
  const processingTime = Date.now() - startTime;
  
  // Log summary (only final summary, no per-row logging)
  const summary = {
    totalRows,
    exported: results.length,
    rejected: rejects.length,
    processingTimeMs: processingTime,
    rejectReasons: rejects.reduce((acc, r) => {
      acc[r.reason] = (acc[r.reason] || 0) + 1;
      return acc;
    }, {})
  };
  
  // Log first 5 sample rows for QA
  const samples = results.slice(0, 5).map(row => ({
    matnr: row.Matnr,
    base: row.CustBestPrice,
    preFee: row['Prezzo con spedizione e IVA'],
    feeDeRev: row.FeeDeRev,
    feeMkt: row['Fee Marketplace'],
    final: row['Prezzo Finale']
  }));
  
  console.log('SKU Processing Summary:', summary);
  console.log('SKU Sample Results:', samples);
  
  // Send final results
  self.postMessage({
    type: 'complete',
    results,
    summary,
    samples
  });
}

function processSkuRow(row, feeDeRevPercent, feeMktPercent, rejects, rowIndex) {
  // Filter 1: ExistingStock > 1
  const stock = Number(row.ExistingStock ?? NaN);
  if (!isFinite(stock) || stock <= 1) {
    rejects.push({ idx: rowIndex, reason: 'stock' });
    return null;
  }
  
  // Filter 2: ManufPartNr not empty
  const mpn = String(row.ManufPartNr ?? '').trim();
  if (!mpn) {
    rejects.push({ idx: rowIndex, reason: 'mpn_vuoto' });
    return null;
  }
  
  // Filter 3: Valid base price (CustBestPrice -> fallback ListPrice)
  const cbp = parseEuroLike(row.CustBestPrice);
  const lp = parseEuroLike(row.ListPrice);
  
  let baseEuro = null;
  if (Number.isFinite(cbp) && cbp > 0) {
    baseEuro = cbp;
  } else if (Number.isFinite(lp) && lp > 0) {
    baseEuro = lp;
  }
  
  if (baseEuro === null) {
    rejects.push({ idx: rowIndex, reason: 'prezzo_base' });
    return null;
  }
  
  // SKU Pipeline calculations in cents
  const baseCents = toCents(baseEuro);
  const shippingCents = 600; // 6.00 EUR
  const vatRate = 0.22;
  
  // Step 1: base + shipping
  let v = baseCents + shippingCents;
  
  // Step 2: + VAT
  v = Math.round(v * (1 + vatRate));
  const preFeeEuro = v / 100;
  
  // Step 3: + FeeDeRev (sequential)
  const feeDeRevCents = Math.round(v * feeDeRevPercent);
  v = v + feeDeRevCents;
  const feeDeRevEuro = feeDeRevCents / 100;
  
  // Step 4: + Fee Marketplace (sequential)
  const feeMktCents = Math.round(v * feeMktPercent);
  v = v + feeMktCents;
  const feeMktEuro = feeMktCents / 100;
  
  // Step 5: Ceiling to integer euro
  const finalCents = Math.ceil(v / 100) * 100;
  const finalEuro = finalCents / 100;
  
  // Subtotal post-fee
  const subtotalPostFee = preFeeEuro + feeDeRevEuro + feeMktEuro;
  
  // ListPrice con Fee calculation
  let listPriceConFee = '';
  if (Number.isFinite(lp) && lp > 0) {
    const lpBaseCents = toCents(lp);
    let lpV = lpBaseCents + shippingCents;
    lpV = Math.round(lpV * (1 + vatRate));
    lpV = lpV + Math.round(lpV * feeDeRevPercent);
    lpV = lpV + Math.round(lpV * feeMktPercent);
    const lpFinalCents = Math.ceil(lpV / 100) * 100;
    
    // Validation: must be integer euro
    if (lpFinalCents % 100 !== 0) {
      throw new Error('SKU: ListPrice con Fee deve essere intero');
    }
    
    listPriceConFee = lpFinalCents / 100;
  }
  
  // Final validation: Prezzo Finale must be integer euro
  if (finalCents % 100 !== 0) {
    throw new Error('SKU: Prezzo Finale deve essere intero in euro');
  }
  
  // Build minimal result object with exact column order
  return {
    Matnr: row.Matnr || '',
    ManufPartNr: mpn,
    EAN: sanitizeEAN(row.EAN),
    ShortDescription: row.ShortDescription || '',
    ExistingStock: stock,
    CustBestPrice: Number.isFinite(cbp) ? cbp : '',
    'Costo di Spedizione': 6.00,
    IVA: 0.22, // Will be formatted as percentage in Excel
    'Prezzo con spedizione e IVA': preFeeEuro,
    FeeDeRev: feeDeRevEuro,
    'Fee Marketplace': feeMktEuro,
    'Subtotale post-fee': subtotalPostFee,
    'Prezzo Finale': finalEuro,
    ListPrice: Number.isFinite(lp) ? lp : '',
    'ListPrice con Fee': listPriceConFee
  };
}