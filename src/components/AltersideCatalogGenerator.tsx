import React, { useState, useRef, useCallback, useEffect } from 'react';
import { supabase } from '@/integrations/supabase/client';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Checkbox } from '@/components/ui/checkbox';
import { toast } from '@/hooks/use-toast';
import { Upload, Download, FileText, CheckCircle, XCircle, AlertCircle, Clock, Activity, Info, LogOut, Cloud, Loader2 } from 'lucide-react';
import { useAuth } from '@/hooks/useAuth';
import { filterAndNormalizeForEAN, type EANStats, type DiscardedRow } from '@/utils/ean';
import { forceEANText, exportDiscardedRowsCSV } from '@/utils/excelFormatter';
import { 
  parseOverrideFile, 
  applyOverrideToCatalog, 
  validateEnding99Guard,
  type OverrideIndex,
  type OverrideError,
  type OverrideStats
} from '@/utils/override';
import { exportMediaworldCatalog } from '@/utils/mediaworldExport';
import { 
  toComma99Cents, 
  validateEnding99, 
  computeFromListPrice, 
  toCents, 
  formatCents, 
  applyRate,
  applyRateCents,
  parsePercentToRate,
  parseRate,
  ceilToComma99, 
  ceilToIntegerEuros 
} from '@/utils/pricing';
import * as XLSX from 'xlsx';
import Papa from 'papaparse';
import SyncScheduler from '@/components/SyncScheduler';

// Helper functions for MPN calculations
function asNum(v: any): number {
  if (v == null || v === '') return 0;
  if (typeof v === 'number') return v;
  const s = String(v).replace(/\./g, '').replace(',', '.');
  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}

// Dedicated parsing function for distributor price file (ListPrice, CustBestPrice, Surcharge)
function parseDistributorPrice(raw: any): number {
  if (raw === null || raw === undefined) return 0;
  
  let value = String(raw).trim();
  if (value === '') return 0;
  
  // Normalize decimal separator: comma → dot
  value = value.replace(',', '.');
  
  // Handle case ".00" → "0.00"
  if (value.startsWith('.')) {
    value = '0' + value;
  }
  
  const num = parseFloat(value);
  if (isNaN(num)) return 0;
  
  return num;
}

function ceil2(v: any): number {
  return Math.ceil(asNum(v) * 100) / 100;
}

function ceilInt(v: any): number {
  return Math.ceil(asNum(v));
}

function toEnding99(v: any): number {
  const n = asNum(v);
  const c = Math.round(n * 100) % 100;
  if (c === 99) return Math.floor(n) + 0.99;
  const f = Math.floor(n);
  return n <= f + 0.99 ? f + 0.99 : f + 1.99;
}

function toComma(n: any): string {
  return asNum(n).toFixed(2).replace('.', ',');
}

function toExcelText(s: any): string {
  const str = String(s ?? '');
  return /^[=+\-@]/.test(str) ? `'${str}` : str;
}

interface FileData {
  name: string;
  data: any[];
  headers: string[];
  raw: File;
  isValid?: boolean;
}

interface EANPrefillCounters {
  already_populated: number;
  filled_now: number;
  skipped_due_to_conflict: number;
  duplicate_mpn_rows: number;
  mpn_not_in_material: number;
  empty_ean_rows: number;
  missing_mapping_in_new_file: number;
  errori_formali: number;
}

interface EANPrefillReports {
  updated: Array<{ ManufPartNr: string; EAN_old: string; EAN_new: string }>;
  already_populated: Array<{ ManufPartNr: string; EAN_existing: string }>;
  skipped_due_to_conflict: Array<{ ManufPartNr: string; EAN_material: string; EAN_mapping_first: string }>;
  duplicate_mpn_rows: Array<{ mpn: string; ean_seen_first: string; ean_conflicting: string; row_index: number }>;
  mpn_not_in_material: Array<{ mpn: string; ean: string; row_index: number }>;
  empty_ean_rows: Array<{ mpn: string; row_index: number }>;
  missing_mapping_in_new_file: Array<{ ManufPartNr: string }>;
  errori_formali: Array<{ raw_line: string; reason: string; row_index: number }>;
}

interface PrefillState {
  status: 'idle' | 'running' | 'done' | 'skipped';
  counters: EANPrefillCounters | null;
  reports: EANPrefillReports | null;
}

// Override Prodotti State
interface OverrideState {
  file: File | null;
  filename: string | null;
  uploadedAt: Date | null;
  index: OverrideIndex | null;
  errors: OverrideError[];
  disabled: boolean;
  validCount: number;
  invalidCount: number;
  lastApplyStats: OverrideStats | null;
}

interface FileUploadState {
  material: { file: FileData | null; status: 'empty' | 'valid' | 'error' | 'warning'; error?: string; warning?: string; diagnostics?: any };
  stock: { file: FileData | null; status: 'empty' | 'valid' | 'error' | 'warning'; error?: string; warning?: string; diagnostics?: any };
  price: { file: FileData | null; status: 'empty' | 'valid' | 'error' | 'warning'; error?: string; warning?: string; diagnostics?: any };
  eanMapping: { file: File | null; status: 'empty' | 'ready' | 'processing' | 'completed' | 'error'; error?: string };
}

interface ProcessedRecord {
  Matnr: string;
  ManufPartNr: string;
  EAN: string;
  ShortDescription: string;
  ExistingStock: number;
  ListPrice: number;
  CustBestPrice: number;
  Surcharge: number;
  basePriceCents: number;
  'Costo di Spedizione': number;
  IVA: number;
  'Prezzo con spediz e IVA': number;
  FeeDeRev: number;
  'Fee Marketplace': number;
  'Subtotale post-fee': number;
  'Prezzo Finale': number | string; // String display for EAN (e.g. "34,99"), number for MPN
  'ListPrice con Fee': number | string; // Can be empty string for invalid ListPrice
}

interface FeeConfig {
  feeDrev: number;      // e.g. 1.05
  feeMkt: number;       // e.g. 1.08
  shippingCost: number; // e.g. 6.00
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

interface ProcessingStats {
  totalRecords: number;
  validRecordsEAN: number;
  validRecordsManufPartNr: number;
  filteredRecordsEAN: number;
  filteredRecordsManufPartNr: number;
  stockDuplicates: number;
  priceDuplicates: number;
}

const REQUIRED_HEADERS = {
  material: ['Matnr', 'ManufPartNr', 'EAN', 'ShortDescription'],
  stock: ['Matnr', 'ExistingStock'],
  price: ['Matnr', 'ListPrice', 'CustBestPrice']
};

const OPTIONAL_HEADERS = {
  material: [],
  stock: ['ManufPartNr'],
  price: ['ManufPartNr']
};

const DEFAULT_FEES: FeeConfig = { feeDrev: 1.05, feeMkt: 1.08, shippingCost: 6.00 };



function computeFinalPrice({
  basePriceCents, base, baseRoute, feeDrev, feeMkt, shippingCost, CustBestPrice, ListPrice
}: { basePriceCents: number; base: number; baseRoute: string; feeDrev: number; feeMkt: number; shippingCost: number; CustBestPrice?: number; ListPrice?: number; }): {
  base: number, shipping: number, iva: number, subtotConIva: number,
  postFee: number, prezzoFinaleEAN: number, prezzoFinaleMPN: number, listPriceConFee: number | string,
  eanResult: { finalCents: number; finalDisplay: string; route: string; debug: any }
} {
  const shipping = shippingCost;
  const ivaMultiplier = 1.22;
  
  // Calculate for display/compatibility (old pipeline values)
  const subtot_base_sped = base + shipping;
  const iva = subtot_base_sped * 0.22;
  const subtotConIva = subtot_base_sped + iva;
  const postFee = subtotConIva * feeDrev * feeMkt;
  
  // EAN final price: Calculate in cents, then apply ending ,99
  const shippingCents = Math.round(shippingCost * 100);
  const afterShippingCents = basePriceCents + shippingCents;
  const afterIvaCents = Math.round(afterShippingCents * ivaMultiplier);
  const afterFeeDeRevCents = Math.round(afterIvaCents * feeDrev);
  const afterFeesCents = Math.round(afterFeeDeRevCents * feeMkt);
  
  // Apply ,99 ending using toComma99Cents
  const finalCents = toComma99Cents(afterFeesCents);
  const finalDisplay = formatCents(finalCents);
  
  const eanResult = {
    finalCents,
    finalDisplay,
    route: baseRoute,
    debug: {
      route: baseRoute,
      basePriceCents,
      afterShippingCents,
      afterIvaCents,
      afterFeeDeRevCents,
      afterFeesCents,
      finalCents
    }
  };
  
  const prezzoFinaleEAN = finalCents / 100;
  
  // MPN final price: use old logic (ceil to integer)
  const prezzoFinaleMPN = Math.ceil(postFee);
  
  // Helper function for robust numeric normalization
  const normalizeNumeric = (value: any): number | null => {
    if (value === null || value === undefined || value === '') return null;
    
    let str = String(value).trim();
    // Remove € symbol and internal spaces
    str = str.replace(/€/g, '').replace(/\s/g, '');
    // Replace comma with dot for decimal separator
    str = str.replace(',', '.');
    
    const parsed = parseFloat(str);
    return isNaN(parsed) ? null : parsed;
  };

  // Calculate ListPrice con Fee - SEPARATE pipeline, independent from main calculation
  let listPriceConFee: number | string = '';
  
  // Normalize all required fields BEFORE any comparison or calculation
  const normCustBestPrice = normalizeNumeric(CustBestPrice);
  const normShipping = normalizeNumeric(shipping);
  const normIvaPerc = 22; // IVA is always 22% in this system
  const normFeeDrev = normalizeNumeric(feeDrev);
  const normFeeMkt = normalizeNumeric(feeMkt);
  const normPrezzoFinale = normalizeNumeric(prezzoFinaleEAN);
  const normListPrice = normalizeNumeric(ListPrice);
  
  // Check if all required inputs are valid for the alternative rule
  const hasValidInputsForAltRule = normCustBestPrice !== null && 
                                     normShipping !== null && 
                                     normIvaPerc !== null && 
                                     normFeeDrev !== null && 
                                     normFeeMkt !== null && 
                                     normPrezzoFinale !== null;
  
  // Check if alternative rule should be activated
  const shouldUseAlternativeRule = normListPrice === null || 
                                     normListPrice === 0 || 
                                     (normCustBestPrice !== null && normListPrice < normCustBestPrice);
  
  if (shouldUseAlternativeRule && hasValidInputsForAltRule) {
    // OVERRIDE RULE: ListPrice is absent, 0, non-numeric, or < CustBestPrice
    // Use CustBestPrice × 1.25 as base
    const base = normCustBestPrice! * 1.25;
    const ivaMultiplier = 1 + (normIvaPerc! / 100);
    const valore_candidato = ((base + normShipping!) * ivaMultiplier) * normFeeDrev! * normFeeMkt!;
    const candidato_ceil = Math.ceil(valore_candidato);
    
    // Calculate minimum constraint: 25% above Prezzo Finale
    const minimo_consentito = Math.ceil(normPrezzoFinale! * 1.25);
    
    // FORCE WRITE: Take the maximum and overwrite ListPrice con Fee
    listPriceConFee = Math.max(candidato_ceil, minimo_consentito);
    
    // Log when rule is activated
    if (typeof (globalThis as any).lpfeeAltRuleCount === 'undefined') {
      (globalThis as any).lpfeeAltRuleCount = 0;
    }
    if ((globalThis as any).lpfeeAltRuleCount < 5) {
      const reason = normListPrice === null ? 'ListPrice assente' : 
                     (normListPrice === 0 ? 'ListPrice zero' : 'ListPrice < CustBestPrice');
      console.warn('lpfee:override', {
        motivo: `override ListPrice con Fee: ${reason}`,
        CustBestPrice: normCustBestPrice,
        ListPrice: normListPrice !== null ? normListPrice : 'N/A',
        PrezzoFinale: normPrezzoFinale,
        base,
        valore_candidato: valore_candidato.toFixed(4),
        candidato_ceil,
        minimo_consentito,
        ListPriceConFee: listPriceConFee
      });
      (globalThis as any).lpfeeAltRuleCount++;
    }
  } else if (!shouldUseAlternativeRule && normListPrice !== null && normListPrice > 0) {
    // STANDARD RULE: ListPrice is valid, numeric, > 0, and >= CustBestPrice (or no CustBestPrice)
    // Keep existing calculation unchanged
    const baseLP = normListPrice;
    const subtotBasSpedLP = baseLP + normShipping!;
    const ivaLP = subtotBasSpedLP * 0.22;
    const subtotConIvaLP = subtotBasSpedLP + ivaLP;
    const postFeeLP = subtotConIvaLP * normFeeDrev! * normFeeMkt!;
    listPriceConFee = Math.ceil(postFeeLP);
    
    // Log samples for debugging
    if (typeof (globalThis as any).lpfeeCalcSampleCount === 'undefined') {
      (globalThis as any).lpfeeCalcSampleCount = 0;
    }
    if ((globalThis as any).lpfeeCalcSampleCount < 3) {
      console.warn('lpfee:calc:sample', { 
        listPrice: baseLP, 
        subtot_con_iva: subtotConIvaLP.toFixed(2), 
        feeDeRev: normFeeDrev, 
        feeMarketplace: normFeeMkt, 
        post_fee: postFeeLP.toFixed(4), 
        final: listPriceConFee 
      });
      (globalThis as any).lpfeeCalcSampleCount++;
    }
  } else if (shouldUseAlternativeRule && !hasValidInputsForAltRule) {
    // ERROR: Alternative rule should activate but inputs are invalid
    console.warn('lpfee:input_non_valido', {
      motivo: 'input non valido per calcolo ListPrice con Fee',
      normCustBestPrice,
      normShipping,
      normIvaPerc,
      normFeeDrev,
      normFeeMkt,
      normPrezzoFinale
    });
    // listPriceConFee remains empty string
  }
  // else: both conditions fail or inputs invalid, listPriceConFee remains empty string

  return { base, shipping, iva, subtotConIva, postFee, prezzoFinaleEAN, prezzoFinaleMPN, listPriceConFee, eanResult };
}

const AltersideCatalogGenerator: React.FC = () => {
  const { logout } = useAuth();
  const [files, setFiles] = useState<FileUploadState>({
    material: { file: null, status: 'empty' },
    stock: { file: null, status: 'empty' },
    price: { file: null, status: 'empty' },
    eanMapping: { file: null, status: 'empty' }
  });

  // State for tracking material rows and version
  const [materialRows, setMaterialRows] = useState<any[]>([]);
  const [materialVersion, setMaterialVersion] = useState(0);
  
  // Pre-fill EAN state
  const [prefillState, setPrefillState] = useState<PrefillState>({
    status: 'idle',
    counters: null,
    reports: null
  });
  const eanPrefillWorkerRef = useRef<Worker | null>(null);
  
  // Legacy compatibility (can be removed after migration)
  const eanPrefillCompleted = prefillState.status === 'done';
  const eanPrefillCounters = prefillState.counters;
  const eanPrefillReports = prefillState.reports;
  const isProcessingPrefill = prefillState.status === 'running';

  // Override Prodotti state
  const [overrideState, setOverrideState] = useState<OverrideState>({
    file: null,
    filename: null,
    uploadedAt: null,
    index: null,
    errors: [],
    disabled: false,
    validCount: 0,
    invalidCount: 0,
    lastApplyStats: null
  });
  
  // Base catalog (before override) and final dataset
  const [eanCatalogBase, setEanCatalogBase] = useState<any[]>([]);

  // Override persistence state
  const overrideLoadedRef = useRef(false);

  // Mapping persistence state
  const [mappingInfo, setMappingInfo] = useState<{ filename: string; uploadedAt: string } | null>(null);
  const mappingLoadedRef = useRef(false);

  // Fee configuration
  const [feeConfig, setFeeConfig] = useState<FeeConfig>(DEFAULT_FEES);
  const [isLoadingFees, setIsLoadingFees] = useState(true);
  const [isSavingFees, setIsSavingFees] = useState(false);

  // Load fee config from Supabase on mount
  useEffect(() => {
    const loadFeesFromSupabase = async () => {
      try {
        const { data, error } = await supabase
          .from('fee_config')
          .select('fee_drev, fee_mkt, shipping_cost, mediaworld_preparation_days, eprice_preparation_days')
          .order('updated_at', { ascending: false })
          .limit(1);
        
        if (error) {
          console.error('Errore caricamento fee config:', error);
          toast({
            title: "Errore caricamento regole",
            description: "Impossibile caricare le regole di calcolo dal server. Utilizzati valori predefiniti.",
            variant: "destructive"
          });
        } else if (data && data.length > 0) {
          const row = data[0];
          setFeeConfig({
            feeDrev: Number(row.fee_drev),
            feeMkt: Number(row.fee_mkt),
            shippingCost: Number(row.shipping_cost)
          });
          // Load preparation days if present
          if (row.mediaworld_preparation_days !== null && row.mediaworld_preparation_days !== undefined) {
            setPrepDaysMediaworld(Number(row.mediaworld_preparation_days));
          }
          if (row.eprice_preparation_days !== null && row.eprice_preparation_days !== undefined) {
            setPrepDays(Number(row.eprice_preparation_days));
          }
          console.log('Fee config caricato da Supabase:', row);
        } else {
          console.log('Nessuna fee config trovata, utilizzo valori predefiniti');
        }
      } catch (err) {
        console.error('Errore loadFeesFromSupabase:', err);
        toast({
          title: "Errore caricamento regole",
          description: "Impossibile caricare le regole di calcolo dal server. Utilizzati valori predefiniti.",
          variant: "destructive"
        });
      } finally {
        setIsLoadingFees(false);
      }
    };

    loadFeesFromSupabase();
  }, []);

  // Save fee config to Supabase
  const handleSaveFees = async () => {
    setIsSavingFees(true);
    try {
      const { error } = await supabase
        .from('fee_config')
        .insert({
          fee_drev: feeConfig.feeDrev,
          fee_mkt: feeConfig.feeMkt,
          shipping_cost: feeConfig.shippingCost
        });
      
      if (error) {
        console.error('Errore salvataggio fee config:', error);
        toast({
          title: "Errore salvataggio",
          description: "Impossibile salvare le regole di calcolo sul server.",
          variant: "destructive"
        });
      } else {
        toast({
          title: "Regole salvate",
          description: "Le regole di calcolo sono state salvate con successo."
        });
        console.log('Fee config salvato su Supabase');
      }
    } catch (err) {
      console.error('Errore handleSaveFees:', err);
      toast({
        title: "Errore salvataggio",
        description: "Impossibile salvare le regole di calcolo sul server.",
        variant: "destructive"
      });
    } finally {
      setIsSavingFees(false);
    }
  };

  // Save preparation days to Supabase (global settings)
  const handleSavePrepDays = async () => {
    // Validate inputs
    if (!Number.isInteger(prepDays) || prepDays < 1) {
      toast({
        title: "Errore validazione",
        description: "I giorni di preparazione ePrice devono essere un numero intero >= 1",
        variant: "destructive"
      });
      return;
    }
    if (!Number.isInteger(prepDaysMediaworld) || prepDaysMediaworld < 1) {
      toast({
        title: "Errore validazione",
        description: "I giorni di preparazione Mediaworld devono essere un numero intero >= 1",
        variant: "destructive"
      });
      return;
    }

    setIsSavingPrepDays(true);
    try {
      // Get existing config to update (or insert new)
      const { data: existingConfig } = await supabase
        .from('fee_config')
        .select('id')
        .order('updated_at', { ascending: false })
        .limit(1);
      
      if (existingConfig && existingConfig.length > 0) {
        // Update existing record
        const { error } = await supabase
          .from('fee_config')
          .update({
            mediaworld_preparation_days: prepDaysMediaworld,
            eprice_preparation_days: prepDays,
            updated_at: new Date().toISOString()
          })
          .eq('id', existingConfig[0].id);
        
        if (error) {
          console.error('Errore salvataggio giorni preparazione:', error);
          toast({
            title: "Errore salvataggio",
            description: "Impossibile salvare i giorni di preparazione sul server.",
            variant: "destructive"
          });
        } else {
          toast({
            title: "Impostazioni salvate",
            description: "I giorni di preparazione sono stati salvati come impostazioni globali."
          });
          console.log('Giorni preparazione salvati su Supabase');
        }
      } else {
        // Insert new record with all defaults plus prep days
        const { error } = await supabase
          .from('fee_config')
          .insert({
            fee_drev: feeConfig.feeDrev,
            fee_mkt: feeConfig.feeMkt,
            shipping_cost: feeConfig.shippingCost,
            mediaworld_preparation_days: prepDaysMediaworld,
            eprice_preparation_days: prepDays
          });
        
        if (error) {
          console.error('Errore salvataggio giorni preparazione:', error);
          toast({
            title: "Errore salvataggio",
            description: "Impossibile salvare i giorni di preparazione sul server.",
            variant: "destructive"
          });
        } else {
          toast({
            title: "Impostazioni salvate",
            description: "I giorni di preparazione sono stati salvati come impostazioni globali."
          });
          console.log('Giorni preparazione salvati su Supabase');
        }
      }
    } catch (err) {
      console.error('Errore handleSavePrepDays:', err);
      toast({
        title: "Errore salvataggio",
        description: "Impossibile salvare i giorni di preparazione sul server.",
        variant: "destructive"
      });
    } finally {
      setIsSavingPrepDays(false);
    }
  };

  // Persist mapping file to Supabase Storage
  const persistMappingFile = async (file: File) => {
    try {
      const { error } = await supabase.storage
        .from('mapping-files')
        .upload('latest/mapping_sku_ean.csv', file, { upsert: true });
      
      if (error) {
        console.error('Errore upload mapping:', error);
        toast({
          title: "Errore salvataggio mapping",
          description: "Impossibile salvare il file di associazione sul server.",
          variant: "destructive"
        });
        return;
      }
      
      setMappingInfo({
        filename: file.name,
        uploadedAt: new Date().toISOString()
      });
    } catch (err) {
      console.error('Errore persistMappingFile:', err);
      toast({
        title: "Errore salvataggio mapping",
        description: "Impossibile salvare il file di associazione sul server.",
        variant: "destructive"
      });
    }
  };

  // Persist override file to Supabase Storage
  const persistOverrideFile = async (file: File) => {
    try {
      const { error } = await supabase.storage
        .from('mapping-files')
        .upload('latest/override_prodotti.xlsx', file, { upsert: true });
      
      if (error) {
        console.error('Errore upload override:', error);
        toast({
          title: "Errore salvataggio override",
          description: "Impossibile salvare il file override sul server.",
          variant: "destructive"
        });
        return;
      }
      
      console.log('File override persistito su Storage');
    } catch (err) {
      console.error('Errore persistOverrideFile:', err);
      toast({
        title: "Errore salvataggio override",
        description: "Impossibile salvare il file override sul server.",
        variant: "destructive"
      });
    }
  };

  // Delete override file from Supabase Storage
  const deleteOverrideFileFromStorage = async () => {
    try {
      const { error } = await supabase.storage
        .from('mapping-files')
        .remove(['latest/override_prodotti.xlsx']);
      
      if (error) {
        console.error('Errore eliminazione override da storage:', error);
      } else {
        console.log('File override eliminato da Storage');
      }
    } catch (err) {
      console.error('Errore deleteOverrideFileFromStorage:', err);
    }
  };

  // Load mapping file from storage on mount
  useEffect(() => {
    if (mappingLoadedRef.current) return;
    mappingLoadedRef.current = true;

    const loadMappingFromStorage = async () => {
      try {
        const { data, error } = await supabase.storage
          .from('mapping-files')
          .download('latest/mapping_sku_ean.csv');
        
        if (error || !data) {
          // File doesn't exist, nothing to do
          console.log('Nessun file mapping salvato trovato');
          return;
        }
        
        const file = new File([data], 'mapping_sku_ean.csv', { type: 'text/plain' });
        
        // Update mapping info
        setMappingInfo({
          filename: 'mapping_sku_ean.csv',
          uploadedAt: new Date().toISOString()
        });
        
        // Load into the eanMapping state
        setFiles(prev => ({
          ...prev,
          eanMapping: { file, status: 'ready' }
        }));
        
        toast({
          title: "Mapping caricato",
          description: "File di associazione SKU↔EAN ripristinato automaticamente"
        });
      } catch (err) {
        console.error('Errore caricamento mapping:', err);
      }
    };

    loadMappingFromStorage();
  }, []);

  // Load override file from storage on mount
  useEffect(() => {
    if (overrideLoadedRef.current) return;
    overrideLoadedRef.current = true;

    const loadOverrideFromStorage = async () => {
      try {
        const { data, error } = await supabase.storage
          .from('mapping-files')
          .download('latest/override_prodotti.xlsx');
        
        if (error || !data) {
          // File doesn't exist, nothing to do
          console.log('Nessun file override salvato trovato');
          return;
        }
        
        const file = new File([data], 'override_prodotti.xlsx', { 
          type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' 
        });
        
        // Parse the file
        const buffer = await file.arrayBuffer();
        const result = parseOverrideFile(buffer);
        
        setOverrideState({
          file,
          filename: 'override_prodotti.xlsx',
          uploadedAt: new Date(),
          index: result.index,
          errors: result.errors,
          disabled: false,
          validCount: result.validCount,
          invalidCount: result.invalidCount,
          lastApplyStats: null
        });
        
        if (result.success && result.index) {
          toast({
            title: "Override caricato",
            description: `File override ripristinato: ${result.validCount} righe valide`
          });
        } else {
          console.warn('File override caricato ma non valido:', result.errors);
        }
      } catch (err) {
        console.error('Errore caricamento override:', err);
      }
    };

    loadOverrideFromStorage();
  }, []);

  const [processingState, setProcessingState] = useState<'idle' | 'validating' | 'ready' | 'running' | 'completed' | 'failed'>('idle');
  const [currentPipeline, setCurrentPipeline] = useState<'EAN' | 'MPN' | null>(null);
  
  // Progress states (based on rows READ, not valid rows)
  const [total, setTotal] = useState(0); // prescan estimate
  const [finalTotal, setFinalTotal] = useState<number | null>(null); // actual rows read in join
  const [processed, setProcessed] = useState(0);
  const processedRef = useRef(0);
  const [progressPct, setProgressPct] = useState(0);
  
  // Ensure no stale references to undefined variables
  const processedRows = processed; // Alias for compatibility
  
  const [startTime, setStartTime] = useState<number | null>(null);
  const [elapsedTime, setElapsedTime] = useState(0);
  const [estimatedTime, setEstimatedTime] = useState<number | null>(null);
  
  // Completion gating flags
  const [joinDone, setJoinDone] = useState(false);
  const [excelDone, setExcelDone] = useState(false);
  const [logDone, setLogDone] = useState(false);
  const [eanStats, setEanStats] = useState<EANStats | null>(null);
  const [discardedRows, setDiscardedRows] = useState<DiscardedRow[]>([]);
  
  // Debug events
  const [debugEvents, setDebugEvents] = useState<string[]>([]);
  const [debugState, setDebugState] = useState({
    materialValid: false,
    stockValid: false,
    priceValid: false,
    stockReady: false,
    priceReady: false,
    materialPreScanDone: false,
    joinStarted: false
  });

  // Current pipeline results
  const [currentProcessedData, setCurrentProcessedData] = useState<ProcessedRecord[]>([]);
  const [currentLogEntries, setCurrentLogEntries] = useState<LogEntry[]>([]);
  const [currentStats, setCurrentStats] = useState<ProcessingStats | null>(null);
  
  // =====================================================================
  // EAN CATALOG DATASET - FONTE UNICA PER TUTTI GLI EXPORT
  // Questo dataset viene generato durante l'export del Catalogo EAN e
  // contiene i prezzi finali formattati ("NN,99"). Viene riutilizzato
  // da ePrice e Mediaworld per garantire coerenza dei prezzi.
  // =====================================================================
  const [eanCatalogDataset, setEanCatalogDataset] = useState<any[]>([]);
  
  // =====================================================================
  // useEffect: LOG quando eanCatalogDataset è REALMENTE pronto (post-render)
  // Questo log viene emesso DOPO che React ha aggiornato lo state,
  // quindi mostra il valore reale e non quello in fase di aggiornamento.
  // =====================================================================
  useEffect(() => {
    if (!eanCatalogDataset || eanCatalogDataset.length === 0) return;

    console.log('%c[eanCatalogDataset:ready-for-exports]', 'color: #FF9800; font-weight: bold;', {
      rows: eanCatalogDataset.length,
      firstRow: {
        EAN: eanCatalogDataset[0]?.EAN,
        PrezzoFinale: eanCatalogDataset[0]?.['Prezzo Finale'],
        PrezzoFinaleType: typeof eanCatalogDataset[0]?.['Prezzo Finale'],
      },
      timestamp: new Date().toISOString()
    });
  }, [eanCatalogDataset]);
  
  // Stats state for UI
  const [stats, setStats] = useState({
    totalRows: 0,
    validEAN: 0,
    validMPN: 0,
    discardedEAN: 0,
    discardedMPN: 0,
    duplicates: 0
  });
  
  // Downloaded files state for buttons
  const [downloadReady, setDownloadReady] = useState({
    ean_excel: false,
    ean_log: false,
    mpn_excel: false,
    mpn_log: false
  });

  // Export state for preventing double clicks
  const [isExportingEAN, setIsExportingEAN] = useState(false);
  const [isExportingEprice, setIsExportingEprice] = useState(false);
  const [isExportingMediaworld, setIsExportingMediaworld] = useState(false);

  // Validation results state for visual summary
  interface ExportValidationResult {
    exportType: 'ePrice' | 'Mediaworld';
    timestamp: Date;
    success: boolean;
    totalRows: number;
    validRows: number;
    skippedRows: number;
    errors: Array<{ row: number; field: string; reason: string; sku?: string }>;
    warnings: string[];
    structureErrors: string[];
  }
  const [lastEpriceValidation, setLastEpriceValidation] = useState<ExportValidationResult | null>(null);
  const [lastMediaworldValidation, setLastMediaworldValidation] = useState<ExportValidationResult | null>(null);
  
  // ePrice export configuration
  const [prepDays, setPrepDays] = useState<number>(1);
  
  // Mediaworld export configuration
  const [prepDaysMediaworld, setPrepDaysMediaworld] = useState<number>(3);
  
  // State for saving preparation days
  const [isSavingPrepDays, setIsSavingPrepDays] = useState(false);

  // Pipeline Master state
  const [pipelineRunning, setPipelineRunning] = useState(false);
  const [pipelineStatus, setPipelineStatus] = useState<string>('');
  const [pipelineProgress, setPipelineProgress] = useState<number>(0);
  const [pipelineStepLabel, setPipelineStepLabel] = useState<string>('In attesa');

  const workerRef = useRef<Worker | null>(null);

  // FTP import loading state
  const [ftpImportLoading, setFtpImportLoading] = useState(false);

  // SFTP Upload state
  interface SftpUploadStatus {
    phase: 'idle' | 'generating' | 'saving' | 'uploading' | 'complete' | 'error';
    message: string;
    results?: Array<{ filename: string; uploaded: boolean; error?: string }>;
  }
  const [sftpUploadStatus, setSftpUploadStatus] = useState<SftpUploadStatus>({ phase: 'idle', message: '' });

  // Refs for pipeline master to track current state in closures
  const filesRef = useRef(files);
  const prefillStateRef = useRef(prefillState);
  const processingStateRef = useRef(processingState);
  const sftpUploadStatusRef = useRef(sftpUploadStatus);
  const eanCatalogDatasetRef = useRef(eanCatalogDataset);
  
  // Keep refs in sync with state
  useEffect(() => { filesRef.current = files; }, [files]);
  useEffect(() => { prefillStateRef.current = prefillState; }, [prefillState]);
  useEffect(() => { processingStateRef.current = processingState; }, [processingState]);
  useEffect(() => { sftpUploadStatusRef.current = sftpUploadStatus; }, [sftpUploadStatus]);
  useEffect(() => { eanCatalogDatasetRef.current = eanCatalogDataset; }, [eanCatalogDataset]);

  const isProcessing = processingState === 'running';
  const isCompleted = processingState === 'completed';
  const canProcess = processingState === 'ready';
  
  // Audit function for critical debugging
  const audit = useCallback((msg: string, data?: any) => {
    const logEntry = `AUDIT: ${msg}${data ? ' | ' + JSON.stringify(data) : ''}`;
    console.warn(logEntry);
    dbg(logEntry);
  }, []);

  // Global debug function
  const dbg = useCallback((event: string, data?: any) => {
    const timestamp = new Date().toLocaleTimeString('it-IT', { hour12: false });
    const message = `[${timestamp}] ${event}${data ? ' | ' + JSON.stringify(data) : ''}`;
    setDebugEvents(prev => [...prev, message]);
  }, []);
  
  // Make dbg available globally for worker
  useEffect(() => {
    (window as any).dbg = dbg;
  }, [dbg]);

  // Cleanup workers on unmount
  useEffect(() => {
    return () => {
      if (workerRef.current) {
        workerRef.current.terminate();
        workerRef.current = null;
      }
      if (eanPrefillWorkerRef.current) {
        eanPrefillWorkerRef.current.terminate();
        eanPrefillWorkerRef.current = null;
      }
    };
  }, []);

  // Consistency check functions with ±1 tolerance
  const sumForPipeline = useCallback(() => {
    if (currentPipeline === 'EAN') {
      return (stats.validEAN ?? 0) + (stats.discardedEAN ?? 0) + (stats.duplicates ?? 0);
    }
    if (currentPipeline === 'MPN') {
      return (stats.validMPN ?? 0) + (stats.discardedMPN ?? 0) + (stats.duplicates ?? 0);
    }
    return NaN;
  }, [currentPipeline, stats]);

  const consistencyOk = useCallback(() => {
    const baseline = (finalTotal ?? processedRef.current ?? processed ?? total ?? 0);
    const sum = sumForPipeline();
    return Number.isFinite(baseline) && Number.isFinite(sum) && Math.abs(sum - baseline) <= 1;
  }, [finalTotal, processed, total, sumForPipeline]);

  const getConsistencySnapshot = useCallback(() => {
    return {
      pipeline: currentPipeline,
      totalPrescan: total,
      finalTotal: finalTotal ?? processedRef.current,
      processedUI: processed,
      stats
    };
  }, [currentPipeline, total, finalTotal, processed, stats]);

  // Completion gating effect with consistency check
  useEffect(() => {
    if (joinDone && excelDone && logDone) {
      if (!consistencyOk()) {
        audit('consistency-failed', getConsistencySnapshot());
        // fallback: accept baseline from join and continue anyway
        // or relaunch join with worker:false, but DON'T block infinitely
      }
      setProgressPct(100);
      setProcessingState('completed');
      dbg('pipeline:completed', {
        pipeline: currentPipeline,
        totalPrescan: total,
        finalTotal: finalTotal ?? processedRef.current
      });
    }
  }, [joinDone, excelDone, logDone, consistencyOk, getConsistencySnapshot, currentPipeline, total, finalTotal]);

  // Log state changes
  useEffect(() => {
    dbg('state:change', { state: processingState, ...debugState });
  }, [processingState, debugState]);

  const validateHeaders = (headers: string[], requiredHeaders: string[], optionalHeaders: string[] = []): { 
    valid: boolean; 
    missing: string[]; 
    missingOptional: string[];
    hasWarning: boolean;
  } => {
    const normalizedHeaders = headers.map(h => h.trim().replace(/^\uFEFF/, '')); // Remove BOM
    const missing = requiredHeaders.filter(req => !normalizedHeaders.includes(req));
    const missingOptional = optionalHeaders.filter(opt => !normalizedHeaders.includes(opt));
    
    return { 
      valid: missing.length === 0, 
      missing, 
      missingOptional,
      hasWarning: missingOptional.length > 0
    };
  };

  const parseCSV = async (file: File): Promise<{ data: any[]; headers: string[] }> => {
    return new Promise((resolve, reject) => {
      Papa.parse(file, {
        header: true,
        delimiter: ';',
        encoding: 'UTF-8',
        skipEmptyLines: true,
        complete: (results) => {
          if (results.errors.length > 0) {
            reject(new Error(`Errore parsing: ${results.errors[0].message}`));
            return;
          }
          
          const headers = results.meta.fields || [];
          resolve({
            data: results.data,
            headers
          });
        },
        error: (error) => {
          reject(new Error(`Errore lettura file: ${error.message}`));
        }
      });
    });
  };

  const handleFileUpload = async (file: File, type: keyof FileUploadState) => {
    try {
      const parsed = await parseCSV(file);
      const validation = validateHeaders(parsed.headers, REQUIRED_HEADERS[type], OPTIONAL_HEADERS[type]);
      
      if (!validation.valid) {
        const error = `Header mancanti: ${validation.missing.join(', ')}`;
        setFiles(prev => ({
          ...prev,
          [type]: { file: null, status: 'error', error }
        }));
        setProcessingState('idle');
        
        toast({
          title: "Errore validazione header",
          description: error,
          variant: "destructive"
        });
        return;
      }

      // Handle warnings for optional headers
      let warning = '';
      let status: 'valid' | 'warning' = 'valid';
      
      if (validation.hasWarning && (type === 'stock' || type === 'price')) {
        status = 'warning';
        if (type === 'stock') {
          warning = 'Header opzionale assente: ManufPartNr (continuerò usando il valore dal Material).';
        } else if (type === 'price') {
          warning = 'Header opzionale assente: ManufPartNr (continuerò usando il valore dal Material).';
        }
      }

      // Create file state with diagnostics
      const headerLine = parsed.headers.join(', ');
      const firstDataLine = parsed.data.length > 0 ? Object.values(parsed.data[0]).slice(0, 3).join(', ') + '...' : '';

      const fileState = {
        name: file.name,
        data: parsed.data,
        headers: parsed.headers,
        raw: file,
        isValid: validation.valid
      };

      // Use functional update to preserve other files' state
      setFiles(prev => {
        const newFiles = {
          ...prev,
          [type]: {
            ...prev[type],
            file: fileState,
            status: fileState.isValid ? 'valid' : 'error',
            diagnostics: {
              headerFound: headerLine,
              firstDataRow: firstDataLine,
              validation
            }
          }
        };
        
        // Check if all files are loaded with valid required headers (warnings don't block)
        const allRequiredHeadersValid = Object.entries(newFiles).every(([fileType, fileState]) => {
          if (fileType === 'eanMapping') return true; // Skip eanMapping check (optional)
          if (!fileState.file) return false;
          const fileData = fileState.file as FileData;
          const requiredHeaders = REQUIRED_HEADERS[fileType as keyof typeof REQUIRED_HEADERS];
          const val = validateHeaders(fileData.headers, requiredHeaders);
          return val.valid; // Only check required headers
        });
        
        // Update debug state
        const newDebugState = {
          materialValid: newFiles.material.file ? validateHeaders(newFiles.material.file.headers, REQUIRED_HEADERS.material).valid : false,
          stockValid: newFiles.stock.file ? validateHeaders(newFiles.stock.file.headers, REQUIRED_HEADERS.stock).valid : false,
          priceValid: newFiles.price.file ? validateHeaders(newFiles.price.file.headers, REQUIRED_HEADERS.price).valid : false,
          stockReady: !!newFiles.stock.file,
          priceReady: !!newFiles.price.file,
          materialPreScanDone: debugState.materialPreScanDone,
          joinStarted: debugState.joinStarted
        };
        setDebugState(newDebugState);
        dbg('state:change', newDebugState);
        
        if (allRequiredHeadersValid) {
          setProcessingState('ready');
        }
        
        return newFiles;
      });

      const toastMessage = status === 'warning' 
        ? `${file.name} - ${parsed.data.length} righe (con avviso)`
        : `${file.name} - ${parsed.data.length} righe`;

      toast({
        title: "File caricato con successo",
        description: toastMessage,
        variant: status === 'warning' ? 'default' : 'default'
      });

    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : 'Errore sconosciuto';
      setFiles(prev => ({
        ...prev,
        [type]: { file: null, status: 'error', error: errorMsg }
      }));
      setProcessingState('idle');
      
      toast({
        title: "Errore caricamento file",
        description: errorMsg,
        variant: "destructive"
      });
    }
  };

  const removeFile = (type: keyof FileUploadState) => {
    setFiles(prevFiles => ({
      ...prevFiles,
      [type]: {
        file: null,
        status: 'empty',
        diagnostics: type === 'eanMapping' ? undefined : null
      }
    }));

    // Reset EAN prefill if removing mapping file
    if (type === 'eanMapping') {
      setPrefillState({ status: 'idle', counters: null, reports: null });
    }
    
    // Reset materialRows if removing material file
    if (type === 'material') {
      setMaterialRows([]);
      setMaterialVersion(0);
      setPrefillState({ status: 'idle', counters: null, reports: null });
    }

    // Reset processing state if no files remain
    const remainingFiles = Object.entries(files).filter(([key, _]) => key !== type);
    if (remainingFiles.every(([_, file]) => !file.file)) {
      setProcessingState('idle');
      setProgressPct(0);
      setStartTime(null);
      setElapsedTime(0);
      setEstimatedTime(null);
      setProcessed(0);
      setTotal(0);
      setDebugEvents([]);
      setDebugState({
        materialValid: false,
        stockValid: false,
        priceValid: false,
        stockReady: false,
        priceReady: false,
        materialPreScanDone: false,
        joinStarted: false
      });
    }
  };

  const formatTime = (ms: number): string => {
    if (!ms || ms <= 0) return '00:00';
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    return `${minutes.toString().padStart(2, '0')}:${(seconds % 60).toString().padStart(2, '0')}`;
  };

  // FTP automatic import handler - calls edge function 3 times sequentially
  const handleFtpImport = async () => {
    setFtpImportLoading(true);
    
    // Check authentication - session should already exist after password gate
    const { data: { session }, error: sessionError } = await supabase.auth.getSession();
    
    if (sessionError || !session || !session.access_token) {
      console.error('FTP Import: No valid session found', { sessionError });
      toast({
        title: "Errore di configurazione",
        description: "Nessuna sessione admin attiva. Ricarica la pagina e reinserisci la password di accesso.",
        variant: "destructive"
      });
      setFtpImportLoading(false);
      return;
    }
    
    const accessToken = session.access_token;
    const edgeFunctionUrl = "https://hdcniibdblgqkhhgbqtz.supabase.co/functions/v1/import-catalog-ftp";
    
    // Helper: download file from URL and convert to File object
    const fetchFileFromUrl = async (fileUrl: string, filename: string): Promise<File> => {
      const response = await fetch(fileUrl);
      if (!response.ok) {
        throw new Error(`Download fallito per ${filename}: ${response.status} ${response.statusText}`);
      }
      const blob = await response.blob();
      return new File([blob], filename, { type: "text/plain" });
    };
    
    // Helper: import a single file type from FTP
    const importSingleFileFromFtp = async (fileType: "material" | "stock" | "price") => {
      // Call edge function with Authorization header
      let res: Response;
      try {
        res = await fetch(edgeFunctionUrl, {
          method: "POST",
          headers: { 
            "Content-Type": "application/json",
            "Authorization": `Bearer ${accessToken}`
          },
          body: JSON.stringify({ fileType }),
        });
      } catch (e) {
        throw new Error(`Errore rete import FTP (${fileType})`);
      }

      const data = await res.json();

      if (!res.ok || data.status !== "ok") {
        throw new Error(data.message || `Import FTP fallito per ${fileType}`);
      }

      // Extract file info based on fileType
      const keyMap = { material: "materialFile", stock: "stockFile", price: "priceFile" } as const;
      const fileInfo = data.files?.[keyMap[fileType]];
      
      const url = fileInfo?.url;
      const filename = fileInfo?.filename || `${fileType}File.txt`;
      
      if (!url) {
        throw new Error(`URL mancante nella risposta per ${fileType}`);
      }

      // Download file from Storage URL
      const file = await fetchFileFromUrl(url, filename);

      // Pass to existing upload handler
      try {
        await handleFileUpload(file, fileType);
      } catch (e) {
        throw new Error(`Errore elaborazione file ${fileType}`);
      }
    };
    
    try {
      await importSingleFileFromFtp("material");
      await importSingleFileFromFtp("stock");
      await importSingleFileFromFtp("price");

      toast({
        title: "Import FTP completato",
        description: "I file Material, Stock e Price sono stati importati dal server FTP."
      });
    } catch (error) {
      toast({
        title: "Errore import FTP",
        description: error instanceof Error ? error.message : "Errore sconosciuto durante l'import FTP.",
        variant: "destructive"
      });
    } finally {
      setFtpImportLoading(false);
    }
  };

  // Timer effect
  useEffect(() => {
    let interval: NodeJS.Timeout;
    if (processingState === 'running' && startTime) {
      interval = setInterval(() => {
        const elapsed = Date.now() - startTime;
        setElapsedTime(elapsed);
        
        // Calculate ETA every 500ms
        if (processed > 0 && total > 0) {
          const rate = processed / (elapsed / 1000);
          if (rate >= 0.1) {
            const remaining = Math.max(0, total - processed);
            const etaSec = remaining / rate;
            setEstimatedTime(etaSec * 1000);
          } else {
            setEstimatedTime(null);
          }
        }
      }, 500);
    }
    return () => clearInterval(interval);
  }, [processingState, startTime, processed, total]);

  const processDataPipeline = async (pipelineType: 'EAN' | 'MPN') => {
    // Usa filesRef.current per avere lo stato aggiornato (stessa fonte di verità della UI)
    const currentFiles = filesRef.current;
    
    // hasAllBaseFiles: true solo se tutti e tre i file base sono presenti e validi
    // (stessa logica usata dalle card per mostrare "Caricato")
    const hasMaterial = !!currentFiles.material.file && 
                        (currentFiles.material.status === 'valid' || currentFiles.material.status === 'warning');
    const hasStock = !!currentFiles.stock.file && 
                     (currentFiles.stock.status === 'valid' || currentFiles.stock.status === 'warning');
    const hasPrice = !!currentFiles.price.file && 
                     (currentFiles.price.status === 'valid' || currentFiles.price.status === 'warning');
    const hasAllBaseFiles = hasMaterial && hasStock && hasPrice;
    
    // Debug: log per diagnosticare lo stato dei file base
    console.log("DEBUG base files presence", { 
      material: { file: !!currentFiles.material.file, status: currentFiles.material.status, hasMaterial },
      stock: { file: !!currentFiles.stock.file, status: currentFiles.stock.status, hasStock },
      price: { file: !!currentFiles.price.file, status: currentFiles.price.status, hasPrice },
      hasAllBaseFiles 
    });
    
    if (!hasAllBaseFiles) {
      toast({
        title: "File mancanti",
        description: "Carica tutti e tre i file prima di procedere",
        variant: "destructive"
      });
      return;
    }
    
    // Block processing if pre-fill is running
    if (prefillState.status === 'running') {
      toast({
        title: "Pre-fill in corso",
        description: "Attendi il completamento del pre-fill EAN prima di generare il catalogo",
        variant: "destructive"
      });
      return;
    }

    // Validate required headers only (using currentFiles from filesRef)
    const materialValidation = validateHeaders(currentFiles.material.file!.headers, REQUIRED_HEADERS.material);
    const stockValidation = validateHeaders(currentFiles.stock.file!.headers, REQUIRED_HEADERS.stock);
    const priceValidation = validateHeaders(currentFiles.price.file!.headers, REQUIRED_HEADERS.price);

    if (!materialValidation.valid || !stockValidation.valid || !priceValidation.valid) {
      toast({
        title: "Header obbligatori mancanti",
        description: "Verifica che tutti i file abbiano gli header richiesti",
        variant: "destructive"
      });
      return;
    }

    // Validate shipping cost
    if (isNaN(feeConfig.shippingCost) || feeConfig.shippingCost < 0) {
      toast({
        title: "Costo di spedizione non valido",
        description: "Il costo di spedizione deve essere un numero positivo",
        variant: "destructive"
      });
      return;
    }
    
    // CRITICAL VALIDATION: Test LP/CBP logic with fundamental cases
    if (typeof (globalThis as any).lpCbpValidationDone === 'undefined') {
      console.warn('=== VALIDATION: Testing LP/CBP logic ===');
      
      // Case 1: CBP with positive surcharge
      const test1_cbp = 100.00;
      const test1_surcharge = 3.50;
      const test1_baseCents = Math.round((test1_cbp + test1_surcharge) * 100);
      const test1_expected = 10350;
      const test1_pass = test1_baseCents === test1_expected;
      console.warn('TEST 1 - CBP with Surcharge 3.50:', {
        CustBestPrice: test1_cbp,
        Surcharge: test1_surcharge,
        basePriceCents: test1_baseCents,
        expected: test1_expected,
        PASS: test1_pass
      });
      
      // Case 2: CBP with ".00" surcharge
      const test2_cbp = 100.00;
      const test2_surcharge_string = ".00";
      const test2_surcharge = parseDistributorPrice(test2_surcharge_string);
      const test2_baseCents = Math.round((test2_cbp + test2_surcharge) * 100);
      const test2_baseCentsZero = Math.round(test2_cbp * 100);
      const test2_pass = test2_baseCents === test2_baseCentsZero;
      console.warn('TEST 2 - CBP with Surcharge ".00":', {
        CustBestPrice: test2_cbp,
        SurchargeString: test2_surcharge_string,
        SurchargeParsed: test2_surcharge,
        basePriceCents: test2_baseCents,
        basePriceCentsWithZero: test2_baseCentsZero,
        PASS: test2_pass
      });
      
      // Case 3: LP vs CBP with equal prices and Surcharge=0
      const test3_price = 150.00;
      const test3_surcharge = 0;
      const test3_lpBaseCents = Math.round(test3_price * 100);
      const test3_cbpBaseCents = Math.round((test3_price + test3_surcharge) * 100);
      const test3_pass = test3_lpBaseCents === test3_cbpBaseCents;
      console.warn('TEST 3 - LP vs CBP equal prices, Surcharge=0:', {
        Price: test3_price,
        LP_basePriceCents: test3_lpBaseCents,
        CBP_basePriceCents: test3_cbpBaseCents,
        PASS: test3_pass
      });
      
      // Case 4: LP with Surcharge (should NOT affect LP)
      const test4_lp = 200.00;
      const test4_surcharge = 10.00;
      const test4_lpBaseCents = Math.round(test4_lp * 100);
      const test4_expected = 20000;
      const test4_pass = test4_lpBaseCents === test4_expected && test4_lpBaseCents === Math.round(test4_lp * 100);
      console.warn('TEST 4 - LP with Surcharge (should NOT affect):', {
        ListPrice: test4_lp,
        Surcharge: test4_surcharge,
        LP_basePriceCents: test4_lpBaseCents,
        expected: test4_expected,
        SurchargeNotUsed: 'LP does NOT use Surcharge',
        PASS: test4_pass
      });
      
      const allTestsPass = test1_pass && test2_pass && test3_pass && test4_pass;
      console.warn('=== VALIDATION RESULT:', allTestsPass ? 'ALL TESTS PASSED ✓' : 'SOME TESTS FAILED ✗', '===');
      
      if (!allTestsPass) {
        toast({
          title: "Validazione logica fallita",
          description: "Alcuni test sulla logica LP/CBP non sono passati. Controlla la console.",
          variant: "destructive"
        });
      }
      
      (globalThis as any).lpCbpValidationDone = true;
    }

    // Reset all state for new pipeline
    setCurrentPipeline(pipelineType);
    setCurrentProcessedData([]);
    setCurrentLogEntries([]);
    setCurrentStats(null);
    setFinalTotal(null);
    setEanStats(null);
    setDiscardedRows([]);
    processedRef.current = 0;
    setProcessed(0);
    setProgressPct(0);
    setElapsedTime(0);
    setEstimatedTime(null);
    setDebugEvents([]);
    
    // Reset completion gating flags
    setJoinDone(false);
    setExcelDone(false);
    setLogDone(false);
    setDebugState({
      materialValid: true,
      stockValid: true,
      priceValid: true,
      stockReady: false,
      priceReady: false,
      materialPreScanDone: false,
      joinStarted: false
    });

    // Build maps (parse events)
    const stockMap = new Map<string, { ExistingStock: number }>();
    const priceMap = new Map<string, { ListPrice: number; CustBestPrice: number; Surcharge: number }>();
    let stockDuplicates = 0;
    let priceDuplicates = 0;

    // STOCK parse (simulate chunk logging using one chunk)
    dbg('parse:stock:start');
    let stockRowCounter = 0;
    currentFiles.stock.file!.data.forEach((row, index) => {
      const matnr = row.Matnr?.toString().trim();
      if (!matnr) return;
      stockRowCounter++;
      if (stockMap.has(matnr)) {
        stockDuplicates++;
      } else {
        stockMap.set(matnr, { ExistingStock: parseInt(row.ExistingStock) || 0 });
      }
    });
    dbg('parse:stock:chunk', { chunkNumber: 1, rowsInChunk: stockRowCounter });
    dbg('parse:stock:done', { totalRecords: stockMap.size, totalChunks: 1 });

    // PRICE parse
    dbg('parse:price:start');
    let priceRowCounter = 0;
    currentFiles.price.file!.data.forEach((row, index) => {
      const matnr = row.Matnr?.toString().trim();
      if (!matnr) return;
      priceRowCounter++;
      if (priceMap.has(matnr)) {
        priceDuplicates++;
      } else {
        priceMap.set(matnr, { 
          ListPrice: parseDistributorPrice(row.ListPrice), 
          CustBestPrice: parseDistributorPrice(row.CustBestPrice),
          Surcharge: parseDistributorPrice(row.Surcharge)
        });
      }
    });
    dbg('parse:price:chunk', { chunkNumber: 1, rowsInChunk: priceRowCounter });
    dbg('parse:price:done', { totalRecords: priceMap.size, totalChunks: 1 });

    // Use updated material data (from pre-fill or original)
    const currentMaterialData = currentFiles.material.file!.data;
    const materialRowsCount = currentMaterialData.length;
    
    // Diagnostic: Log EAN count before processing
    const countEANNonVuoti_before = currentMaterialData.filter((row: any) => {
      const ean = String(row.EAN ?? '').trim();
      return ean.length > 0;
    }).length;
    console.warn('diagnostic:ean_count_before', { total: materialRowsCount, nonEmpty: countEANNonVuoti_before });
    
    if (materialRowsCount <= 0) {
      dbg('material:prescan:error', { message: 'Nessuna riga valida nel Material' });
      toast({ title: 'Errore elaborazione', description: 'Nessuna riga valida nel Material', variant: 'destructive' });
      return;
    }
    dbg('material:prescan:done', { materialRowsCount });

    // Init state machine and counters based on rows read
    setProcessingState('running');
    setTotal(materialRowsCount); // only for initial estimate/UI
    setStartTime(Date.now());
    setElapsedTime(0);
    setEstimatedTime(null);
    setDebugState(prev => ({ ...prev, stockReady: true, priceReady: true, materialPreScanDone: true }));

    // Join streaming pass
    const optionalHeadersMissing = {
      stock: !currentFiles.stock.file!.headers.includes('ManufPartNr'),
      price: !currentFiles.price.file!.headers.includes('ManufPartNr')
    };

    const processedEAN: ProcessedRecord[] = [];
    const processedMPN: ProcessedRecord[] = [];
    const logsEAN: LogEntry[] = [];
    const logsMPN: LogEntry[] = [];

    const ceilToXX99 = (value: number) => {
      const integer = Math.floor(value);
      const decimal = value - integer;
      return decimal <= 0.99 ? integer + 0.99 : integer + 1.99;
    };

    const computeFinalPriceForEAN = (row: any): number => {
      // basePriceCents must be pre-calculated and stored in row
      const basePriceCents = row.basePriceCents || 0;
      if (basePriceCents === 0) return 0;
      
      // Calculate in cents: shipping + IVA + fees
      const shippingCents = Math.round(feeConfig.shippingCost * 100);
      const afterShippingCents = basePriceCents + shippingCents;
      const afterIvaCents = Math.round(afterShippingCents * 1.22);
      const afterFeeDeRevCents = Math.round(afterIvaCents * feeConfig.feeDrev);
      const afterFeesCents = Math.round(afterFeeDeRevCents * feeConfig.feeMkt);
      
      // Apply ,99 ending
      const finalCents = toComma99Cents(afterFeesCents);
      
      return finalCents / 100;
    };

    const finalize = () => {
      dbg('excel:write:start');
      dbg('log:write:start');
      
      // Session header + optional header warnings
      const sessionRow: LogEntry = {
        source_file: 'session',
        line: 0,
        Matnr: '',
        ManufPartNr: '',
        EAN: '',
        reason: 'session_start',
        details: JSON.stringify({ event: 'session_start', materialRowsCount, optionalHeadersMissing, fees: { mediaworld: 0.08, alterside: 0.05 }, timestamp: new Date().toISOString() })
      };
      
      // For current pipeline, filter only relevant data
      const currentData = pipelineType === 'EAN' ? processedEAN : processedMPN;
      const currentLogs = pipelineType === 'EAN' ? logsEAN : logsMPN;
      
      currentLogs.unshift(sessionRow);
      if (optionalHeadersMissing.stock) {
        const r: LogEntry = { source_file: 'StockFileData_790813.txt', line: 0, Matnr: '', ManufPartNr: '', EAN: '', reason: 'header_optional_missing', details: 'ManufPartNr assente (uso ManufPartNr da Material)' };
        currentLogs.splice(1, 0, r);
      }
      if (optionalHeadersMissing.price) {
        const idx = 1 + (optionalHeadersMissing.stock ? 1 : 0);
        const r: LogEntry = { source_file: 'pricefileData_790813.txt', line: 0, Matnr: '', ManufPartNr: '', EAN: '', reason: 'header_optional_missing', details: 'ManufPartNr assente (uso ManufPartNr da Material)' };
        currentLogs.splice(idx, 0, r);
      }

      // Update stats for UI
      setStats(prev => ({
        ...prev,
        totalRows: materialRowsCount,
        validEAN: pipelineType === 'EAN' ? currentData.length : prev.validEAN,
        validMPN: pipelineType === 'MPN' ? currentData.length : prev.validMPN,
        discardedEAN: pipelineType === 'EAN' ? currentLogs.length - 1 : prev.discardedEAN,
        discardedMPN: pipelineType === 'MPN' ? currentLogs.length - 1 : prev.discardedMPN
      }));

      // Set current pipeline results
      setCurrentProcessedData(currentData);
      setCurrentLogEntries(currentLogs);
      setCurrentStats({
        totalRecords: materialRowsCount,
        validRecordsEAN: pipelineType === 'EAN' ? currentData.length : 0,
        validRecordsManufPartNr: pipelineType === 'MPN' ? currentData.length : 0,
        filteredRecordsEAN: pipelineType === 'EAN' ? currentLogs.length - 1 : 0,
        filteredRecordsManufPartNr: pipelineType === 'MPN' ? currentLogs.length - 1 : 0,
        stockDuplicates,
        priceDuplicates
      });
      
      // =====================================================================
      // CRITICAL: Popola eanCatalogDataset DIRETTAMENTE alla fine della pipeline EAN
      // Questo garantisce che i dati siano disponibili per l'export anche quando
      // onExportEAN viene chiamato immediatamente dopo processDataPipeline
      // =====================================================================
      if (pipelineType === 'EAN' && currentData.length > 0) {
        // Costruisce il dataset formattato per l'export EAN
        const formattedDataset = currentData.map((record: any) => {
          // Determina la route e calcola baseCents correttamente (con Surcharge per CBP)
          const hasBest = Number.isFinite(record.CustBestPrice) && record.CustBestPrice > 0;
          const hasListPrice = Number.isFinite(record.ListPrice) && record.ListPrice > 0;
          const surchargeValue = (Number.isFinite(record.Surcharge) && record.Surcharge >= 0) ? record.Surcharge : 0;
          
          let baseCents = 0;
          let route = '';
          
          if (hasBest) {
            baseCents = Math.round((record.CustBestPrice + surchargeValue) * 100);
            route = 'cbp';
          } else if (hasListPrice) {
            baseCents = Math.round(record.ListPrice * 100);
            route = 'listprice';
          } else {
            baseCents = 0;
            route = 'none';
          }
          
          // Calcola prezzi usando la stessa logica dell'export
          const shippingCents = Math.round(feeConfig.shippingCost * 100);
          const afterShippingCents = baseCents + shippingCents;
          const afterIvaCents = Math.round(afterShippingCents * 1.22);
          const afterFeeDeRevCents = Math.round(afterIvaCents * feeConfig.feeDrev);
          const afterFeesCents = Math.round(afterFeeDeRevCents * feeConfig.feeMkt);
          const finalCents = toComma99Cents(afterFeesCents);
          const finalEuros = finalCents / 100;
          const prezzoFinaleString = finalEuros.toFixed(2).replace('.', ',');
          
          // ListPrice con Fee
          let listPriceConFeeString: string | number = '';
          const normListPrice = Number.isFinite(record.ListPrice) ? record.ListPrice : null;
          const normCustBestPrice = Number.isFinite(record.CustBestPrice) ? record.CustBestPrice : null;
          
          const shouldUseAltRule = normListPrice === null || normListPrice === 0 || 
                                   (normCustBestPrice !== null && normListPrice < normCustBestPrice);
          
          if (shouldUseAltRule && normCustBestPrice !== null) {
            const baseLP = normCustBestPrice * 1.25;
            const valoreCandidat = ((baseLP + feeConfig.shippingCost) * 1.22) * feeConfig.feeDrev * feeConfig.feeMkt;
            const candidatoCeil = Math.ceil(valoreCandidat);
            const minimoConsentito = Math.ceil(finalEuros * 1.25);
            listPriceConFeeString = Math.max(candidatoCeil, minimoConsentito);
          } else if (normListPrice !== null && normListPrice > 0) {
            const subtotConIvaLP = (normListPrice + feeConfig.shippingCost) * 1.22;
            const postFeeLP = subtotConIvaLP * feeConfig.feeDrev * feeConfig.feeMkt;
            listPriceConFeeString = Math.ceil(postFeeLP);
          }
          
          return {
            Matnr: record.Matnr,
            ManufPartNr: record.ManufPartNr,
            EAN: record.EAN,
            ShortDescription: record.ShortDescription,
            ExistingStock: record.ExistingStock,
            ListPrice: record.ListPrice,
            CustBestPrice: record.CustBestPrice,
            Surcharge: record.Surcharge,
            basePriceCents: baseCents,
            'Costo di Spedizione': feeConfig.shippingCost,
            IVA: afterIvaCents / 100 - afterShippingCents / 100,
            'Prezzo con spediz e IVA': afterIvaCents / 100,
            FeeDeRev: feeConfig.feeDrev,
            'Fee Marketplace': feeConfig.feeMkt,
            'Subtotale post-fee': afterFeesCents / 100,
            'Prezzo Finale': prezzoFinaleString,
            'ListPrice con Fee': listPriceConFeeString,
            _route: route,
            _finalCents: finalCents
          };
        });
        
        // Salva nel state E nella ref per accesso immediato
        setEanCatalogDataset(formattedDataset);
        eanCatalogDatasetRef.current = formattedDataset;
        
        console.log('%c[processDataPipeline:eanCatalogDataset:set]', 'color: #4CAF50; font-weight: bold;', {
          rows: formattedDataset.length,
          sample_EAN: formattedDataset[0]?.EAN,
          sample_PrezzoFinale: formattedDataset[0]?.['Prezzo Finale'],
          timestamp: new Date().toISOString()
        });
      }

      // Update download ready state
      setDownloadReady(prev => ({
        ...prev,
        [pipelineType.toLowerCase() + '_excel']: currentData.length > 0,
        [pipelineType.toLowerCase() + '_log']: currentLogs.length > 0
      }));

      setExcelDone(true);
      dbg('excel:write:done', { pipeline: pipelineType });
      
      setLogDone(true);
      dbg('log:write:done', { pipeline: pipelineType });
    };

    // Wait dependencies observation
    setTimeout(() => {
      if (total > 0 && processed === 0) {
        toast({ title: 'Elaborazione non avviata: verifico dipendenze', description: '', variant: 'default' });
        dbg('join_waiting_dependencies');
      }
    }, 2000);

    const runJoin = (useWorker: boolean) => new Promise<void>((resolve, reject) => {
      dbg('join:start', { worker: useWorker, usingUpdatedMaterial: true });
      setDebugState(prev => ({ ...prev, joinStarted: true }));

      let processedLocal = 0;
      
      // CRITICAL: Use currentMaterialData (updated by pre-fill) instead of reparsing raw file
      const processRow = (row: any, index: number) => {
        // CRITICAL FIX: Count ALL rows read FIRST, before any filtering
        processedRef.current += 1;
        
        // Update progress based on ALL rows read every 256 rows (for performance)
        if ((processedRef.current & 0xFF) === 0) {
          const denom = finalTotal ?? total ?? 1;
          setProcessed(processedRef.current);
          setProgressPct(Math.min(99, Math.floor(processedRef.current / Math.max(1, denom) * 100)));
        }

        const matnr = String(row.Matnr ?? '').trim();
        if (!matnr) return;

        processedLocal++;
        if ((processedLocal & 0xFF) === 0) {
          dbg('join:chunk', { localProcessed: processedLocal, globalProcessed: processedRef.current });
        }

        const stockData = stockMap.get(matnr);
        const priceData = priceMap.get(matnr);

        if (!stockData || !priceData) {
          logsEAN.push({
            source_file: 'MaterialFile',
            line: processedRef.current,
            Matnr: matnr,
            ManufPartNr: String(row.ManufPartNr ?? ''),
            EAN: String(row.EAN ?? ''),
            reason: 'missing_stock_or_price',
            details: !stockData ? 'Stock mancante' : 'Price mancante'
          });
          logsMPN.push({
            source_file: 'MaterialFile',
            line: processedRef.current,
            Matnr: matnr,
            ManufPartNr: String(row.ManufPartNr ?? ''),
            EAN: String(row.EAN ?? ''),
            reason: 'missing_stock_or_price',
            details: !stockData ? 'Stock mancante' : 'Price mancante'
          });
          return;
        }

        const existingStock = stockData.ExistingStock;
        const listPrice = priceData.ListPrice;
        const custBestPrice = priceData.CustBestPrice;
        const surcharge = priceData.Surcharge;

        if (existingStock < 2) {
          logsEAN.push({
            source_file: 'MaterialFile',
            line: processedRef.current,
            Matnr: matnr,
            ManufPartNr: String(row.ManufPartNr ?? ''),
            EAN: String(row.EAN ?? ''),
            reason: 'stock_lt_2',
            details: `ExistingStock=${existingStock}`
          });
          logsMPN.push({
            source_file: 'MaterialFile',
            line: processedRef.current,
            Matnr: matnr,
            ManufPartNr: String(row.ManufPartNr ?? ''),
            EAN: String(row.EAN ?? ''),
            reason: 'stock_lt_2',
            details: `ExistingStock=${existingStock}`
          });
          return;
        }

        // CALCULATE basePriceCents using LP/CBP logic BEFORE calling computeFinalPrice
        const hasBest = Number.isFinite(custBestPrice) && custBestPrice > 0;
        const hasListPrice = Number.isFinite(listPrice) && listPrice > 0;
        
        if (!hasBest && !hasListPrice) {
          logsEAN.push({
            source_file: 'MaterialFile',
            line: processedRef.current,
            Matnr: matnr,
            ManufPartNr: String(row.ManufPartNr ?? ''),
            EAN: String(row.EAN ?? ''),
            reason: 'invalid_price',
            details: `ListPrice=${listPrice}, CustBestPrice=${custBestPrice}, Surcharge=${surcharge}`
          });
          logsMPN.push({
            source_file: 'MaterialFile',
            line: processedRef.current,
            Matnr: matnr,
            ManufPartNr: String(row.ManufPartNr ?? ''),
            EAN: String(row.EAN ?? ''),
            reason: 'invalid_price',
            details: `ListPrice=${listPrice}, CustBestPrice=${custBestPrice}, Surcharge=${surcharge}`
          });
          return;
        }
        
        // Ensure surcharge is valid and non-negative
        const validSurcharge = (Number.isFinite(surcharge) && surcharge >= 0) ? surcharge : 0;
        
        let basePriceCents = 0;
        let base = 0;
        let baseRoute = '';
        
        if (hasBest) {
          // CBP ROUTE: ALWAYS use CustBestPrice + Surcharge, sum cents separately
          const custBestPriceCents = Math.round(custBestPrice * 100);
          const surchargeCents = Math.round(validSurcharge * 100);
          basePriceCents = custBestPriceCents + surchargeCents;
          base = custBestPrice + validSurcharge;
          baseRoute = 'cbp';
        } else if (hasListPrice) {
          // LP ROUTE: use ListPrice only with Math.round, NO Surcharge
          basePriceCents = Math.round(listPrice * 100);
          base = listPrice;
          baseRoute = 'listprice';
        }
        
        // Log samples for LP/CBP debugging (only first 3 of each)
        if (baseRoute === 'cbp') {
          if (typeof (globalThis as any).eanSampleCbpCount === 'undefined') {
            (globalThis as any).eanSampleCbpCount = 0;
          }
          if ((globalThis as any).eanSampleCbpCount < 3) {
            console.warn('ean:sample:cbp:upstream', {
              custBestPrice,
              surcharge: validSurcharge,
              custBestPriceCents: Math.round(custBestPrice * 100),
              surchargeCents: Math.round(validSurcharge * 100),
              basePriceCents,
              base
            });
            (globalThis as any).eanSampleCbpCount++;
          }
        } else if (baseRoute === 'listprice') {
          if (typeof (globalThis as any).eanSampleLpCount === 'undefined') {
            (globalThis as any).eanSampleLpCount = 0;
          }
          if ((globalThis as any).eanSampleLpCount < 3) {
            console.warn('ean:sample:listprice:upstream', {
              listPrice,
              basePriceCents,
              base,
              Surcharge_NOT_USED: 'LP route does not use Surcharge'
            });
            (globalThis as any).eanSampleLpCount++;
          }
        }

        const calc = computeFinalPrice({
          basePriceCents,
          base,
          baseRoute,
          feeDrev: feeConfig.feeDrev,
          feeMkt: feeConfig.feeMkt,
          shippingCost: feeConfig.shippingCost,
          CustBestPrice: custBestPrice,
          ListPrice: listPrice
        });

        const processedRecord: ProcessedRecord = {
          Matnr: matnr,
          ManufPartNr: String(row.ManufPartNr ?? ''),
          EAN: String(row.EAN ?? ''),
          ShortDescription: String(row.ShortDescription ?? ''),
          ExistingStock: existingStock,
          ListPrice: listPrice,
          CustBestPrice: custBestPrice,
          Surcharge: surcharge,
          basePriceCents: basePriceCents, // Store for computeFinalPriceForEAN
          'Costo di Spedizione': calc.shipping,
          IVA: calc.iva,
          'Prezzo con spediz e IVA': calc.subtotConIva,
          FeeDeRev: feeConfig.feeDrev,
          'Fee Marketplace': feeConfig.feeMkt,
          'Subtotale post-fee': calc.postFee,
          'Prezzo Finale': 0, // Will be set by pipeline
          'ListPrice con Fee': calc.listPriceConFee
        };

        // EAN pipeline
        const eanTrimmed = String(row.EAN ?? '').trim();
        if (eanTrimmed && eanTrimmed.length > 0) {
          processedRecord['Prezzo Finale'] = calc.prezzoFinaleEAN;
          processedEAN.push(processedRecord);
        } else {
          logsEAN.push({
            source_file: 'MaterialFile',
            line: processedRef.current,
            Matnr: matnr,
            ManufPartNr: String(row.ManufPartNr ?? ''),
            EAN: String(row.EAN ?? ''),
            reason: 'ean_empty',
            details: 'EAN vuoto'
          });
        }

        // MPN pipeline
        const mpnTrimmed = String(row.ManufPartNr ?? '').trim();
        if (mpnTrimmed && mpnTrimmed.length > 0) {
          processedRecord['Prezzo Finale'] = calc.prezzoFinaleMPN;
          processedMPN.push(processedRecord);
        } else {
          logsMPN.push({
            source_file: 'MaterialFile',
            line: processedRef.current,
            Matnr: matnr,
            ManufPartNr: String(row.ManufPartNr ?? ''),
            EAN: String(row.EAN ?? ''),
            reason: 'mpn_empty',
            details: 'ManufPartNr vuoto'
          });
        }
      };
      
      // Process all material rows synchronously
      currentMaterialData.forEach((row: any, index: number) => {
        processRow(row, index);
      });
      
      // Finalize after processing all rows
      setFinalTotal(processedRef.current);
      setProcessed(processedRef.current);
      setProgressPct(99);
      dbg('join:done', { totalProcessed: processedRef.current, validEAN: processedEAN.length, validMPN: processedMPN.length });
      
      // Diagnostic: Log EAN count after processing
      const countEANValidi = processedEAN.length;
      console.warn('diagnostic:ean_count_after_join', { validRows: countEANValidi });
      
      // Apply EAN validation and normalization for EAN pipeline
      if (pipelineType === 'EAN') {
        const { kept, discarded, stats } = filterAndNormalizeForEAN(processedEAN, computeFinalPriceForEAN);
        processedEAN.length = 0; // clear original
        processedEAN.push(...kept);
        setEanStats(stats);
        setDiscardedRows(discarded);
        
        audit('ean-validation', {
          pipeline: pipelineType,
          input: stats.tot_righe_input,
          kept: kept.length,
          discarded: discarded.length,
          stats
        });
      }
      
      setJoinDone(true);
      resolve();
    });

    try {
      await runJoin(true);
      finalize();
    } catch (err) {
      // ultimate fallback: try without worker
      try {
        await runJoin(false);
        finalize();
      } catch (e) {
        setProcessingState('failed');
        toast({ title: 'Errore elaborazione', description: 'Impossibile completare la join', variant: 'destructive' });
      }
    }
  };

  const getTimestamp = () => {
    const now = new Date();
    const romeTime = new Intl.DateTimeFormat('it-IT', {
      timeZone: 'Europe/Rome',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit'
    }).format(now);

    const [date, time] = romeTime.split(', ');
    const [day, month, year] = date.split('/');
    const [hour, minute] = time.split(':');
    
    return {
      timestamp: `${year}${month}${day}_${hour}${minute}`,
      sheetName: `${year}-${month}-${day}`
    };
  };

  const formatExcelData = (data: ProcessedRecord[]) => {
    return data.map(record => {
      // Create a new object with string formatting for export
      const formatted: any = { ...record };
      
      // Apply MPN-specific formatting based on current pipeline
      if (currentPipeline === 'MPN') {
        // Validate "Subtotale post-fee" first  
        const subfee = asNum(record['Subtotale post-fee']);
        if (subfee <= 0) {
          // Log error and skip this record
          console.log({
            reason: 'mpn_invalid_subtotale_postfee',
            value: record,
            matnr: record.Matnr,
            mpn: record.ManufPartNr
          });
          return null; // Will be filtered out
        }
        
        // Calculate "Prezzo Finale" using toEnding99 on "Subtotale post-fee"
        const finalNum = toEnding99(subfee);
        const finalText = toComma(finalNum);
        
        // MPN specific formatting
        formatted.EAN = toExcelText(record.EAN);
        formatted['Costo di Spedizione'] = toComma(record['Costo di Spedizione']);
        formatted.IVA = '22%';
        formatted['Subtotale post-fee'] = toComma(ceil2(record['Subtotale post-fee']));
        formatted['ListPrice con Fee'] = String(ceilInt(record['ListPrice con Fee']));
        formatted['Prezzo Finale'] = finalText;
        
        // Validate "Prezzo Finale" ends with ",99"
        if (!finalText.endsWith(',99')) {
          console.warn('MPN: Prezzo Finale non termina con ,99', record.Matnr || 'unknown');
        }
        
        // Remove technical columns for MPN export
        Object.keys(formatted).forEach(key => {
          if (key.startsWith('_') || 
              key.endsWith('_cents') || 
              key.endsWith('_Cents') || 
              key.endsWith('_Debug') ||
              key === '_eanFinalCents') {
            delete formatted[key];
          }
        });
      } else {
        // Original EAN formatting (maintain existing logic)
        const asNumber = (v: any) => { 
          const n = typeof v === 'string' ? parseFloat(v.replace('.', '').replace(',', '.')) : Number(v); 
          return Number.isFinite(n) ? n : 0; 
        };
        const ceil2 = (v: any) => Math.ceil(asNumber(v) * 100) / 100;

        formatted.Matnr = String(record.Matnr ?? '');
        formatted.ManufPartNr = String(record.ManufPartNr ?? '');
        formatted.EAN = String(record.EAN ?? '');
        formatted.ShortDescription = String(record.ShortDescription ?? '');
        formatted.ExistingStock = String(record.ExistingStock ?? '');
        formatted.ListPrice = asNumber(record.ListPrice).toFixed(2).replace('.', ',');
        formatted.CustBestPrice = String(record.CustBestPrice ?? '');
        formatted.Surcharge = asNumber(record.Surcharge).toFixed(2).replace('.', ',');
        formatted['Costo di Spedizione'] = toComma(record['Costo di Spedizione']);
        formatted.IVA = '22%';
        formatted['Prezzo con spediz e IVA'] = asNumber(record['Prezzo con spediz e IVA']).toFixed(2).replace('.', ',');
        formatted.FeeDeRev = String(record.FeeDeRev ?? '');
        formatted['Fee Marketplace'] = String(record['Fee Marketplace'] ?? '');
        formatted['Subtotale post-fee'] = ceil2(record['Subtotale post-fee']).toFixed(2).replace('.', ',');
        formatted['Prezzo Finale'] = (typeof record['Prezzo Finale'] === 'string')
          ? record['Prezzo Finale']
          : asNumber(record['Prezzo Finale']).toFixed(2).replace('.', ',');
        formatted['ListPrice con Fee'] = (typeof record['ListPrice con Fee'] === 'string')
          ? record['ListPrice con Fee']
          : String(Math.ceil(asNumber(record['ListPrice con Fee'])));
      }
      
      return formatted;
    });
  };

  const onExportEAN = useCallback(async (event: React.MouseEvent) => {
    event.preventDefault();
    
    if (isExportingEAN) {
      toast({
        title: "Esportazione in corso...",
        description: "Attendere completamento dell'esportazione corrente"
      });
      return;
    }
    
    setIsExportingEAN(true);
    dbg('excel:write:start');
    
    try {
      // =====================================================================
      // FONTE DATI: Usa eanCatalogDatasetRef per accedere ai dati più recenti
      // (evita closure stale quando chiamato da handleFullPipeline)
      // =====================================================================
      const datasetFromRef = eanCatalogDatasetRef.current;
      const datasetFromState = eanCatalogDataset;
      
      console.log('%c[onExportEAN:source-check]', 'color: #FF5722; font-weight: bold;', {
        eanCatalogDatasetRef_length: datasetFromRef?.length ?? 0,
        eanCatalogDataset_length: datasetFromState?.length ?? 0,
        currentProcessedData_length: currentProcessedData?.length ?? 0,
        timestamp: new Date().toISOString()
      });
      
      // =====================================================================
      // FONTE UNICA: Priorità a ref (più recente), poi state, poi fallback
      // =====================================================================
      let eanFilteredData: any[] = [];
      
      // Prima controlla la ref (contiene i dati più recenti dopo processDataPipeline)
      if (datasetFromRef && datasetFromRef.length > 0) {
        eanFilteredData = datasetFromRef;
        console.log('%c[onExportEAN:using-eanCatalogDatasetRef]', 'color: #4CAF50; font-weight: bold;', {
          source: 'eanCatalogDatasetRef (ref)',
          rows: eanFilteredData.length,
          sample_EAN: eanFilteredData[0]?.EAN,
          sample_PrezzoFinale: eanFilteredData[0]?.['Prezzo Finale']
        });
      } else if (datasetFromState && datasetFromState.length > 0) {
        // Fallback allo state (per chiamate manuali dopo che React ha aggiornato)
        eanFilteredData = datasetFromState;
        console.log('%c[onExportEAN:using-eanCatalogDataset-state]', 'color: #4CAF50; font-weight: bold;', {
          source: 'eanCatalogDataset (state)',
          rows: eanFilteredData.length,
          sample_EAN: eanFilteredData[0]?.EAN,
          sample_PrezzoFinale: eanFilteredData[0]?.['Prezzo Finale']
        });
      } else {
        // Ultimo fallback: usa currentProcessedData se non c'è dataset
        eanFilteredData = currentProcessedData.filter(record => record.EAN && record.EAN.length >= 12);
        console.log('%c[onExportEAN:using-currentProcessedData-fallback]', 'color: #FFC107; font-weight: bold;', {
          source: 'currentProcessedData (fallback)',
          filtered_rows: eanFilteredData.length,
          original_rows: currentProcessedData.length
        });
      }
      
      if (eanFilteredData.length === 0) {
        console.error('%c[onExportEAN:NO-DATA]', 'color: red; font-weight: bold;', {
          eanCatalogDatasetRef_length: datasetFromRef?.length ?? 0,
          eanCatalogDataset_length: datasetFromState?.length ?? 0,
          currentProcessedData_length: currentProcessedData?.length ?? 0,
          message: 'Nessuna riga valida per EAN'
        });
        toast({
          title: "Nessuna riga valida per EAN",
          description: "Non ci sono record con EAN validi da esportare. Verifica che la pipeline EAN sia completata."
        });
        return;
      }
      
      // =====================================================================
      // BRANCH: Se i dati sono già formattati (dalla pipeline), usa quelli
      // direttamente senza ricalcolare. Altrimenti, crea il dataset da zero.
      // =====================================================================
      let finalDataset: any[];
      let shouldApplyOverride = false; // Dichiarato qui per visibilità in entrambi i branch
      
      // Controlla se i dati sono già formattati (provengono dalla ref o dallo state)
      const dataIsAlreadyFormatted = (datasetFromRef && datasetFromRef.length > 0) || 
                                      (datasetFromState && datasetFromState.length > 0);
      
      if (dataIsAlreadyFormatted) {
        // FONTE UNICA: eanCatalogDataset è già filtrato, validato e con override applicato
        finalDataset = eanFilteredData;
        console.log('%c[onExportEAN:SKIP-CREATION] Uso dataset già formattato', 'color: #4CAF50; font-weight: bold;', {
          rows: finalDataset.length,
          sample_EAN: finalDataset[0]?.EAN,
          sample_PrezzoFinale: finalDataset[0]?.['Prezzo Finale']
        });
      } else {
        // FALLBACK: Crea dataset da currentProcessedData (prima esecuzione o stato non sincronizzato)
        console.log('%c[onExportEAN:CREATE-DATASET] Creazione dataset da eanFilteredData', 'color: #FFC107; font-weight: bold;', {
          source_rows: eanFilteredData.length
        });
        
        // Create dataset with proper column order - UNIFIED pricing via robust IT locale parsing
        const dataset = eanFilteredData.map((record, index) => {
          // Determine route and calculate baseCents correctly (with Surcharge for CBP)
        const hasBest = Number.isFinite(record.CustBestPrice) && record.CustBestPrice > 0;
        const hasListPrice = Number.isFinite(record.ListPrice) && record.ListPrice > 0;
        const surchargeValue = (Number.isFinite(record.Surcharge) && record.Surcharge >= 0) ? record.Surcharge : 0;
        
        let baseCents = 0;
        let route = '';
        
        if (hasBest) {
          // CBP ROUTE: ALWAYS use CustBestPrice + Surcharge
          baseCents = Math.round((record.CustBestPrice + surchargeValue) * 100);
          route = 'cbp';
        } else if (hasListPrice) {
          // LP ROUTE: use ListPrice only with Math.round, NO Surcharge
          baseCents = Math.round(record.ListPrice * 100);
          route = 'listprice';
        } else {
          // Invalid price - should not happen in filtered data
          console.warn('Export EAN: record without valid price', record.Matnr);
          baseCents = 0;
          route = 'none';
        }
        
        // --- INPUTS sicuri con robust parsing ---
        const shipC = toCents(feeConfig.shippingCost);                   // shipping from config in cent
        const ivaR = parsePercentToRate(22, 22);                         // "22" o "22%" -> 1.22
        const feeDR = parseRate(record.FeeDeRev, 1.05);                  // "1,05" -> 1.05
        const feeMP = parseRate(record['Fee Marketplace'], 1.07);        // "1,07" / "1,07 0,00" -> 1.07
        const listC = toCents(record.ListPrice);                         // ListPrice in cent

        // --- SUBTOTALE (cents-based pipeline con entrambe le fee) ---
        const afterShippingCents = baseCents + shipC;
        const ivatoCents = applyRateCents(afterShippingCents, ivaR);
        const withFeDR = applyRateCents(ivatoCents, feeDR);
        const subtotalCents = applyRateCents(withFeDR, feeMP);

        // --- LISTPRICE CON FEE (intero per eccesso) - STANDARD CALCULATION ---
        const baseListCents = listC + shipC;
        const ivatoListCents = applyRateCents(baseListCents, ivaR);
        const withFeDRList = applyRateCents(ivatoListCents, feeDR);
        const subtotalListCents = applyRateCents(withFeDRList, feeMP);
        let listPriceConFeeInt = ceilToIntegerEuros(subtotalListCents); // intero (standard)

        // Prezzo Finale (ceil a ,99 sul subtotale corretto)
        const prezzoFinaleCents = ceilToComma99(subtotalCents);

        // --- OVERRIDE RULE FOR "ListPrice con Fee" (ULTIMO STEP) ---
        // Normalize ListPrice for comparison
        const normalizeNumericOverride = (val: any): number | null => {
          if (val == null || val === '') return null;
          const str = String(val).trim().replace(/€/g, '').replace(/\s/g, '').replace(/\u00A0/g, '');
          const normalized = str.replace(',', '.');
          const parsed = parseFloat(normalized);
          return Number.isFinite(parsed) ? parsed : null;
        };

        const normListPrice = normalizeNumericOverride(record.ListPrice);
        const normCustBestPrice = normalizeNumericOverride(record.CustBestPrice);
        const normShipping = normalizeNumericOverride(feeConfig.shippingCost);
        const normIVA = normalizeNumericOverride(22);
        const normFeeDeRev = normalizeNumericOverride(record.FeeDeRev);
        const normFeeMarketplace = normalizeNumericOverride(record['Fee Marketplace']);
        const normPrezzoFinale = prezzoFinaleCents / 100; // Already calculated in cents

        // Check if override rule should activate
        const shouldOverride = normListPrice === null || 
                               normListPrice === 0 || 
                               (normCustBestPrice !== null && normListPrice < normCustBestPrice);

        if (shouldOverride) {
          // Validate all required inputs are available
          if (normCustBestPrice !== null && normShipping !== null && normIVA !== null && 
              normFeeDeRev !== null && normFeeMarketplace !== null && normPrezzoFinale !== null) {
            
            // Calculate override value
            const base = normCustBestPrice * 1.25;
            const candidato = ((base + normShipping) * (1 + normIVA / 100)) * normFeeDeRev * normFeeMarketplace;
            
            // Ceiling to integer (no intermediate rounding)
            const candidato_ceil = Math.ceil(candidato);
            
            // Minimum constraint: 25% above Prezzo Finale, then ceiling
            const minimo_consentito = Math.ceil(normPrezzoFinale * 1.25);
            
            // Final override value
            const overrideValue = Math.max(candidato_ceil, minimo_consentito);
            
            // OVERRIDE: Write the new value
            listPriceConFeeInt = overrideValue;
            
            // Log override activation (first 10 only)
            if (index < 10) {
              const reason = normListPrice === null ? 'ListPrice assente' :
                           normListPrice === 0 ? 'ListPrice zero' :
                           'ListPrice < CustBestPrice';
              
              console.warn(`lpfee:override:row${index}`, {
                OverrideListPriceConFee: true,
                Motivo: reason,
                ManufPartNr: record.ManufPartNr || 'N/A',
                CustBestPrice: normCustBestPrice,
                ListPrice: normListPrice ?? 'N/A',
                PrezzoFinale: normPrezzoFinale.toFixed(2),
                base: base.toFixed(4),
                candidato: candidato.toFixed(4),
                candidato_ceil,
                minimo_consentito,
                ListPriceConFee_FINAL: overrideValue
              });
            }
          } else {
            // Invalid inputs for override calculation
            if (index < 5) {
              console.warn(`lpfee:override:input_non_valido:row${index}`, {
                ManufPartNr: record.ManufPartNr || 'N/A',
                CustBestPrice: normCustBestPrice,
                Shipping: normShipping,
                IVA: normIVA,
                FeeDeRev: normFeeDeRev,
                FeeMarketplace: normFeeMarketplace,
                PrezzoFinale: normPrezzoFinale
              });
            }
          }
        }

        // Log per le prime 10 righe per verifica
        if (index < 10) {
          console.warn(`ean:export:row${index}`, {
            route,
            ...(route === 'cbp' && {
              CustBestPrice: record.CustBestPrice,
              Surcharge: surchargeValue,
              baseCents_CBP_with_Surcharge: baseCents
            }),
            ...(route === 'listprice' && {
              ListPrice: record.ListPrice,
              baseCents_LP_no_Surcharge: baseCents
            }),
            shipC, ivaR, feeDR, feeMP, 
            subtotalCents, prezzoFinaleCents, 
            subtotalListCents, listPriceConFee: listPriceConFeeInt
          });
        }

        // Genera il prezzo finale formattato come stringa "NN,99"
        const prezzoFinaleFormatted = formatCents(prezzoFinaleCents);
        
        // === LOG ESTESO: GENERAZIONE CATALOGO EAN ===
        if (index < 5) {
          console.log(`%c[EAN:generated:row${index}]`, 'color: #4CAF50; font-weight: bold;', {
            indice_riga: index,
            EAN: record.EAN,
            SKU: record.ManufPartNr,
            'Prezzo Finale RAW': prezzoFinaleFormatted,
            'Prezzo Finale RAW type': typeof prezzoFinaleFormatted,
            'prezzoFinaleCents (sorgente)': prezzoFinaleCents,
            'ListPrice con Fee RAW': listPriceConFeeInt,
            'ListPrice con Fee RAW type': typeof listPriceConFeeInt,
            'ExistingStock RAW': record.ExistingStock,
            'termina_con_99': prezzoFinaleFormatted.endsWith(',99')
          });
        }
        
        // Per i primi 2 prodotti, logga struttura completa
        if (index < 2) {
          console.log(`%c[EAN:generated:row${index}:FULL]`, 'color: #2196F3; font-weight: bold;', {
            record_completo: {
              Matnr: record.Matnr,
              ManufPartNr: record.ManufPartNr,
              EAN: record.EAN,
              ShortDescription: record.ShortDescription,
              ExistingStock: record.ExistingStock,
              CustBestPrice: record.CustBestPrice,
              ListPrice: record.ListPrice,
              prezzoFinaleCents: prezzoFinaleCents,
              listPriceConFeeInt: listPriceConFeeInt
            }
          });
        }
        
        return {
          Matnr: record.Matnr,
          ManufPartNr: record.ManufPartNr,
          EAN: record.EAN,
          ShortDescription: record.ShortDescription,
          ExistingStock: record.ExistingStock,
          CustBestPrice: record.CustBestPrice,
          Surcharge: record.Surcharge, // Informational field only (in euros)
          'Costo di Spedizione': formatCents(shipC),
          IVA: '22%',
          'Prezzo con spediz e IVA': formatCents(ivatoCents),
          FeeDeRev: record.FeeDeRev,
          'Fee Marketplace': record['Fee Marketplace'],
          'Subtotale post-fee': formatCents(subtotalCents), // Corretta pipeline con entrambe le fee
          'Prezzo Finale': prezzoFinaleFormatted, // FORCE finalDisplay string "NN,99"
          ListPrice: record.ListPrice,
          'ListPrice con Fee': listPriceConFeeInt, // Integer ceiling
          _eanFinalCents: prezzoFinaleCents // Internal field for validation guard
        };
      });
      
      // GUARD: Pre-export validation - block export if finalCents doesn't end with 99
      let incorrectEndingCount = 0;
      const guardFailures: any[] = [];
      
      for (let i = 0; i < dataset.length; i++) {
        const record = dataset[i];
        const finalCents = record._eanFinalCents;
        
        if (typeof finalCents === 'number' && (finalCents % 100) !== 99) {
          incorrectEndingCount++;
          if (guardFailures.length < 10) { // Log first 10 failures
            guardFailures.push({
              index: i,
              matnr: record.Matnr || 'N/A',
              ean: record.EAN || 'N/A',
              finalCents: finalCents,
              finalDisplay: record['Prezzo Finale']
            });
          }
        }
      }
      
      console.warn('excel:guard:finalCentsNot99', { count: incorrectEndingCount });
      
      if (incorrectEndingCount > 0) {
        console.warn('AUDIT: excel-guard:fail', { 
          count: incorrectEndingCount,
          failed: guardFailures
        });
        toast({
          title: "Errore validazione Excel ending ,99",
          description: `${incorrectEndingCount} righe non terminano con ,99. Export bloccato.`,
          variant: "destructive"
        });
        return;
      }
      
      // Simplified validation - already handled by guard above
      console.warn('AUDIT: ean-ending:ok', { validated: dataset.length, total: dataset.length });
      console.warn('lpfee:integer:ok', { validated: dataset.length, total: dataset.length });
      
      // Remove the internal validation field before export - NO "eanFinalCents" column
      const cleanDataset = dataset.map(record => {
        const { _eanFinalCents, ...exportRecord } = record;
        return exportRecord;
      });
      
      // =====================================================================
      // SALVA IL CATALOGO BASE (prima dell'override)
      // =====================================================================
      setEanCatalogBase(cleanDataset);
      
      // =====================================================================
      // APPLICA OVERRIDE SE PRESENTE E ABILITATO
      // =====================================================================
      finalDataset = cleanDataset;
      
      if (shouldApplyOverride && overrideState.index) {
        console.log('%c[Override:applying] === APPLICAZIONE OVERRIDE ===', 'color: #E91E63; font-weight: bold; font-size: 14px;');
        
        const overrideResult = applyOverrideToCatalog(cleanDataset, overrideState.index);
        
        // Valida che tutti i prezzi finali terminino con ,99
        const guardResult = validateEnding99Guard(overrideResult.catalog);
        
        if (!guardResult.valid) {
          console.error('%c[Override:guard:FAIL]', 'color: red; font-weight: bold;', {
            failures: guardResult.failures.slice(0, 10)
          });
          toast({
            title: "Errore validazione Override",
            description: `${guardResult.failures.length} prezzi override non terminano con ,99. Override non applicato.`,
            variant: "destructive"
          });
          // Usa il dataset base senza override
          finalDataset = cleanDataset;
          setOverrideState(prev => ({
            ...prev,
            errors: [...prev.errors, ...guardResult.failures.map(f => ({
              rowIndex: f.index,
              field: 'Prezzo Finale',
              value: f.price,
              reason: `Prezzo non termina con ,99 per EAN ${f.ean}`
            }))]
          }));
        } else {
          // Override applicato con successo
          finalDataset = overrideResult.catalog;
          
          // Aggiorna stats e errori nello state
          setOverrideState(prev => ({
            ...prev,
            errors: [...prev.errors, ...overrideResult.errors],
            lastApplyStats: overrideResult.stats
          }));
          
          console.log('%c[Override:applied]', 'color: #E91E63; font-weight: bold;', {
            updatedExisting: overrideResult.stats.updatedExisting,
            addedNew: overrideResult.stats.addedNew,
            skippedRows: overrideResult.stats.skippedRows,
            warnings: overrideResult.stats.warnings,
            totalErrors: overrideResult.errors.length
          });
          
          toast({
            title: "Override applicato",
            description: `${overrideResult.stats.updatedExisting} aggiornati, ${overrideResult.stats.addedNew} nuovi prodotti`
          });
        }
      }
      
      // =====================================================================
      // SALVA IL DATASET FINALE PER RIUTILIZZO IN EPRICE E MEDIAWORLD
      // Questo dataset contiene i prezzi formattati "NN,99" e sarà usato
      // come UNICA FONTE DI VERITÀ per tutti gli export.
      // =====================================================================
      setEanCatalogDataset(finalDataset);
      eanCatalogDatasetRef.current = finalDataset;
      
      } // Fine del branch else (creazione dataset da currentProcessedData)
      
      // =====================================================================
      // LOG ESTESO: SALVATAGGIO eanCatalogDataset
      // =====================================================================
      console.log('%c[eanCatalogDataset:saved] === DATASET SALVATO ===', 'color: #FF9800; font-weight: bold; font-size: 14px;');
      console.log(`%c[eanCatalogDataset:saved]`, 'color: #FF9800;', { 
        numero_totale_record: finalDataset.length,
        overrideApplied: shouldApplyOverride,
        timestamp: new Date().toISOString()
      });
      
      // Log primi 5 record per verifica formato prezzi con confronto
      finalDataset.slice(0, 5).forEach((record: any, idx: number) => {
        const prezzoFinale = record['Prezzo Finale'];
        const terminaCon99 = typeof prezzoFinale === 'string' && prezzoFinale.endsWith(',99');
        
        console.log(`%c[eanCatalogDataset:saved:row${idx}]`, 'color: #FF9800;', {
          EAN: record.EAN,
          SKU: record.ManufPartNr,
          'Prezzo Finale RAW': prezzoFinale,
          'Prezzo Finale RAW type': typeof prezzoFinale,
          'termina_con_99': terminaCon99,
          'ListPrice con Fee RAW': record['ListPrice con Fee'],
          'ListPrice con Fee RAW type': typeof record['ListPrice con Fee'],
          'ExistingStock RAW': record.ExistingStock,
          '__override': record.__override || false,
          '__overrideSource': record.__overrideSource || 'none'
        });
        
        // ALERT se il prezzo NON termina con ,99
        if (!terminaCon99) {
          console.error(`%c[eanCatalogDataset:ERROR:row${idx}] PREZZO NON TERMINA CON ,99!`, 'color: red; font-weight: bold;', {
            EAN: record.EAN,
            prezzoFinale: prezzoFinale,
            type: typeof prezzoFinale
          });
        }
      });
      
      // Create worksheet from final dataset (with override applied if applicable)
      const ws = XLSX.utils.json_to_sheet(finalDataset, { skipHeader: false });
      
      // Ensure !ref exists
      if (!ws['!ref']) {
        const rows = finalDataset.length + 1; // +1 for header
        const cols = Object.keys(finalDataset[0] || {}).length;
        ws['!ref'] = XLSX.utils.encode_range({s:{r:0,c:0}, e:{r:rows-1,c:cols-1}});
      }
      
      // Force EAN column to text format
      const range = XLSX.utils.decode_range(ws['!ref']);
      let eanCol = -1;
      
      // Find EAN column in header
      for (let C = range.s.c; C <= range.e.c; C++) {
        const addr = XLSX.utils.encode_cell({ r: 0, c: C });
        const cell = ws[addr];
        const name = (cell?.v ?? '').toString().trim().toLowerCase();
        if (name === 'ean') { 
          eanCol = C; 
          break; 
        }
      }
      
      // Force EAN column cells to text format
      if (eanCol >= 0) {
        for (let R = 1; R <= range.e.r; R++) {
          const addr = XLSX.utils.encode_cell({ r: R, c: eanCol });
          const cell = ws[addr];
          if (cell) {
            cell.v = (cell.v ?? '').toString(); // Preserve leading zeros
            cell.t = 's';
            cell.z = '@';
            ws[addr] = cell;
          }
        }
      }
      
      // Format columns for EAN export - FORCE Prezzo Finale as text with finalDisplay
      const decimalColumns = ['Prezzo con spediz e IVA', 'Subtotale post-fee'];
      const textColumns = ['Prezzo Finale']; // FORCE finalDisplay as text to preserve ",99"
      const integerColumns = ['ListPrice con Fee']; // Keep as integer ceiling
      
      for (let C = range.s.c; C <= range.e.c; C++) {
        const headerAddr = XLSX.utils.encode_cell({ r: 0, c: C });
        const headerCell = ws[headerAddr];
        const headerName = (headerCell?.v ?? '').toString().trim();
        
        if (decimalColumns.includes(headerName)) {
          // Format as decimal with 2 places
          for (let R = 1; R <= range.e.r; R++) {
            const addr = XLSX.utils.encode_cell({ r: R, c: C });
            const cell = ws[addr];
            if (cell && typeof cell.v === 'number') {
              cell.t = 'n';
              cell.z = '#,##0.00';
              ws[addr] = cell;
            }
          }
        } else if (textColumns.includes(headerName)) {
          // FORCE Prezzo Finale as text to preserve finalDisplay "NN,99" format
          for (let R = 1; R <= range.e.r; R++) {
            const addr = XLSX.utils.encode_cell({ r: R, c: C });
            const cell = ws[addr];
            if (cell) {
              // FORCE text type and format - no numeric interpretation
              cell.t = 's'; // String type
              cell.z = '@'; // Text format
              cell.v = (cell.v ?? '').toString(); // Ensure string value
              ws[addr] = cell;
            }
          }
        } else if (integerColumns.includes(headerName)) {
          // Format ListPrice con Fee as integer (keep ceiling behavior)
          for (let R = 1; R <= range.e.r; R++) {
            const addr = XLSX.utils.encode_cell({ r: R, c: C });
            const cell = ws[addr];
            if (cell && typeof cell.v === 'number') {
              cell.t = 'n';
              cell.z = '0';
              ws[addr] = cell;
            }
          }
        }
      }
      
      // Create workbook
      const wb = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(wb, ws, "Catalogo_EAN");
      
      // Serialize to ArrayBuffer
      const wbout = XLSX.write(wb, { bookType: "xlsx", type: "array" });
      
      if (!wbout || wbout.length === 0) {
        dbg('excel:write:error | empty buffer');
        toast({
          title: "Errore generazione file",
          description: "Buffer vuoto durante la generazione del file Excel"
        });
        return;
      }
      
      // Create blob and download from main thread
      const blob = new Blob([wbout], { 
        type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" 
      });
      
      const url = URL.createObjectURL(blob);
      
      // Generate filename with timestamp
      const now = new Date();
      const timestamp = now.toISOString().slice(0,16).replace(/[-:T]/g, '').replace(/(\d{8})(\d{4})/, '$1_$2');
      const fileName = `Catalogo_EAN_${timestamp}.xlsx`;
      
      // Create anchor and trigger download
      const a = document.createElement("a");
      a.href = url;
      a.download = fileName;
      a.rel = "noopener";
      a.style.display = "none";
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      
      // Delay URL revocation
      setTimeout(() => URL.revokeObjectURL(url), 3000);
      
      dbg('excel:write:blob-size', { bytes: blob.size });
      dbg('excel:write:done', { pipeline: 'EAN' });
      
      setExcelDone(true);
      
      // === LOG CORRETTO: RIEPILOGO FINALE CATALOGO EAN ===
      console.log('%c[EAN:generation:complete]', 'color: #4CAF50; font-weight: bold; font-size: 14px;', {
        totale_prodotti_generati: finalDataset.length,
        dataset_salvato_in_eanCatalogDataset: finalDataset.length,
        allineamento: 'OK',
        pronto_per_ePrice: finalDataset.length > 0,
        pronto_per_Mediaworld: finalDataset.length > 0,
        timestamp: new Date().toISOString(),
      });
      
      toast({
        title: "Export EAN completato",
        description: "Download avviato. Salvataggio su bucket in corso..."
      });

      // =====================================================================
      // STEP 2: Save all 3 exports to Supabase bucket "exports"
      // =====================================================================
      setSftpUploadStatus({ phase: 'saving', message: 'Salvataggio file nel bucket...' });
      
      try {
        // Generate ePrice export
        console.log('[EAN:sftp] Generating ePrice export...');
        const ePriceDataset = finalDataset.map((record: any) => {
          const prezzoFinaleRaw = record['Prezzo Finale'];
          let price = 0;
          if (typeof prezzoFinaleRaw === 'string') {
            price = parseFloat(prezzoFinaleRaw.replace(',', '.'));
          } else if (typeof prezzoFinaleRaw === 'number') {
            price = prezzoFinaleRaw;
          }

          return {
            'sku': record.ManufPartNr || '',
            'product-id': record.EAN || '',
            'product-id-type': 'EAN',
            'price': price,
            'quantity': record.ExistingStock || 0,
            'state': 11,
            'fulfillment-latency': prepDays,
            'logistic-class': 'K'
          };
        }).filter((r: any) => r['product-id'] && r['product-id'].length >= 12 && r.quantity > 0);

        const ePriceWs = XLSX.utils.json_to_sheet(ePriceDataset);
        const ePriceWb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(ePriceWb, ePriceWs, "Tracciato_Inserimento_Offerte");
        const ePriceBuffer = XLSX.write(ePriceWb, { bookType: "xlsx", type: "array" });
        const ePriceBlob = new Blob([ePriceBuffer], { type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" });

        // Generate Mediaworld export
        console.log('[EAN:sftp] Generating Mediaworld export...');
        const templateResponse = await fetch('/mediaworld-template.xlsx');
        if (!templateResponse.ok) {
          throw new Error('Template Mediaworld non trovato');
        }
        const templateBuffer = await templateResponse.arrayBuffer();
        const templateWb = XLSX.read(templateBuffer, { type: 'array' });
        const dataSheet = templateWb.Sheets['Data'];
        
        if (!dataSheet) {
          throw new Error('Foglio "Data" non trovato nel template Mediaworld');
        }

        const mediaworldRows: any[][] = [];
        finalDataset.forEach((record: any) => {
          const ean = record.EAN || '';
          const sku = record.ManufPartNr || '';
          const stock = Number(record.ExistingStock) || 0;
          
          if (!ean || ean.length < 12 || stock <= 0 || !sku) return;

          const prezzoFinaleRaw = record['Prezzo Finale'];
          const listPriceConFeeRaw = record['ListPrice con Fee'];
          
          let prezzoFinale = 0;
          if (typeof prezzoFinaleRaw === 'string') {
            prezzoFinale = parseFloat(prezzoFinaleRaw.replace(',', '.'));
          } else if (typeof prezzoFinaleRaw === 'number') {
            prezzoFinale = prezzoFinaleRaw;
          }

          let listPriceConFee = 0;
          if (typeof listPriceConFeeRaw === 'number') {
            listPriceConFee = listPriceConFeeRaw;
          } else if (typeof listPriceConFeeRaw === 'string') {
            listPriceConFee = parseFloat(listPriceConFeeRaw.replace(',', '.'));
          }

          if (prezzoFinale <= 0 || listPriceConFee <= 0) return;

          mediaworldRows.push([
            sku, ean, 'EAN', record.ShortDescription || '', '',
            listPriceConFee, '', stock, '', 'Nuovo', '', '',
            'Consegna gratuita', prezzoFinale, '', '',
            prepDaysMediaworld, '', 'recommended-retail-price', '', '', ''
          ]);
        });

        const startRow = 2;
        mediaworldRows.forEach((row, rowIndex) => {
          row.forEach((value, colIndex) => {
            const addr = XLSX.utils.encode_cell({ r: startRow + rowIndex, c: colIndex });
            dataSheet[addr] = typeof value === 'number' ? { v: value, t: 'n' } : { v: value, t: 's' };
          });
        });

        const endRow = startRow + mediaworldRows.length - 1;
        dataSheet['!ref'] = XLSX.utils.encode_range({ s: { r: 0, c: 0 }, e: { r: endRow, c: 21 } });

        const mediaworldBuffer = XLSX.write(templateWb, { bookType: "xlsx", type: "array" });
        const mediaworldBlob = new Blob([mediaworldBuffer], { type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" });

        // Upload all 3 files to bucket
        const filesToUpload = [
          { name: 'Catalogo EAN.xlsx', blob: blob },
          { name: 'Export ePrice.xlsx', blob: ePriceBlob },
          { name: 'Export Mediaworld.xlsx', blob: mediaworldBlob }
        ];

        for (const file of filesToUpload) {
          const { error } = await supabase.storage.from('exports').upload(file.name, file.blob, { upsert: true });
          if (error) {
            console.error(`[EAN:sftp] Error saving ${file.name}:`, error);
            throw new Error(`Errore salvataggio ${file.name}: ${error.message}`);
          }
          console.log(`[EAN:sftp] Saved ${file.name} to bucket`);
        }

        // =====================================================================
        // STEP 3: Call edge function to upload to SFTP
        // =====================================================================
        setSftpUploadStatus({ phase: 'uploading', message: 'Upload SFTP in corso...' });
        console.log('[EAN:sftp] Calling upload-exports-to-sftp edge function...');

        const { data: sftpResult, error: sftpError } = await supabase.functions.invoke('upload-exports-to-sftp', {
          body: {
            files: [
              { bucket: 'exports', path: 'Catalogo EAN.xlsx', filename: 'Catalogo EAN.xlsx' },
              { bucket: 'exports', path: 'Export ePrice.xlsx', filename: 'Export ePrice.xlsx' },
              { bucket: 'exports', path: 'Export Mediaworld.xlsx', filename: 'Export Mediaworld.xlsx' }
            ]
          }
        });

        if (sftpError) {
          console.error('[EAN:sftp] Edge function error:', sftpError);
          setSftpUploadStatus({ 
            phase: 'error', 
            message: `File salvati nel bucket. Errore SFTP: ${sftpError.message}`,
            results: filesToUpload.map(f => ({ filename: f.name, uploaded: false, error: sftpError.message }))
          });
          toast({
            title: "File salvati nel bucket",
            description: `Upload SFTP fallito: ${sftpError.message}`,
            variant: "destructive"
          });
        } else if (sftpResult?.status === 'ok') {
          setSftpUploadStatus({ 
            phase: 'complete', 
            message: 'Upload SFTP completato con successo!',
            results: sftpResult.results
          });
          toast({
            title: "Export completato",
            description: "File salvati nel bucket e caricati su SFTP"
          });
        } else {
          setSftpUploadStatus({ 
            phase: 'error', 
            message: sftpResult?.message || 'File salvati nel bucket. Upload SFTP non riuscito.',
            results: sftpResult?.results
          });
          toast({
            title: "File salvati nel bucket",
            description: sftpResult?.message || "Upload SFTP non riuscito",
            variant: "destructive"
          });
        }

      } catch (sftpErr: any) {
        console.error('[EAN:sftp] Error:', sftpErr);
        setSftpUploadStatus({ 
          phase: 'error', 
          message: sftpErr.message || 'Errore durante salvataggio/upload'
        });
        // Don't show destructive toast - local download succeeded
        toast({
          title: "Download locale completato",
          description: `Errore bucket/SFTP: ${sftpErr.message}`
        });
      }
      
    } catch (error) {
      dbg('excel:write:error', { message: error instanceof Error ? error.message : 'Unknown error' });
      toast({
        title: "Errore durante l'export",
        description: error instanceof Error ? error.message : "Errore sconosciuto"
      });
    } finally {
      setIsExportingEAN(false);
    }
  }, [eanCatalogDataset, currentProcessedData, isExportingEAN, feeConfig, prepDays, prepDaysMediaworld, dbg, toast]);

  // =====================================================================
  // TEMPLATE EPRICE UFFICIALE: Struttura e validazione
  // Questi valori devono corrispondere esattamente al template ufficiale
  // "Tracciato_Pubblicazione_Offerte.xlsx"
  // =====================================================================
  const EPRICE_TEMPLATE = {
    sheetName: "Tracciato_Inserimento_Offerte",
    headers: ["sku", "product-id", "product-id-type", "price", "quantity", "state", "fulfillment-latency", "logistic-class"],
    columnCount: 8,
    fixedValues: {
      "product-id-type": "EAN",
      "state": 11,
      "logistic-class": "K"
    },
    validations: {
      sku: { type: "string", required: true, minLength: 1 },
      "product-id": { type: "string", required: true, pattern: /^\d{12,14}$/ },
      "product-id-type": { type: "string", required: true, value: "EAN" },
      price: { type: "number", required: true, min: 0.01, max: 100000, decimals: 2 },
      quantity: { type: "integer", required: true, min: 0 },
      state: { type: "integer", required: true, value: 11 },
      "fulfillment-latency": { type: "integer", required: true, min: 0, max: 365 },
      "logistic-class": { type: "string", required: true, value: "K" }
    }
  };

  // Validate ePrice file structure against official template
  const validateEpriceStructure = useCallback((
    ws: XLSX.WorkSheet,
    sheetName: string
  ): { isValid: boolean; errors: string[]; warnings: string[] } => {
    const errors: string[] = [];
    const warnings: string[] = [];

    // 1. Validate sheet name
    if (sheetName !== EPRICE_TEMPLATE.sheetName) {
      warnings.push(`Nome foglio: "${sheetName}" invece di "${EPRICE_TEMPLATE.sheetName}"`);
    }

    // 2. Validate sheet exists and has data
    if (!ws['!ref']) {
      errors.push("Foglio vuoto o non valido");
      return { isValid: false, errors, warnings };
    }

    const range = XLSX.utils.decode_range(ws['!ref']);
    
    // 3. Validate column count
    const actualColumnCount = range.e.c - range.s.c + 1;
    if (actualColumnCount !== EPRICE_TEMPLATE.columnCount) {
      errors.push(`Numero colonne: ${actualColumnCount} invece di ${EPRICE_TEMPLATE.columnCount}`);
    }

    // 4. Validate headers (row 0)
    const actualHeaders: string[] = [];
    for (let C = range.s.c; C <= range.e.c; C++) {
      const addr = XLSX.utils.encode_cell({ r: 0, c: C });
      const cell = ws[addr];
      actualHeaders.push(cell?.v?.toString() || '');
    }

    // Check header order and names
    for (let i = 0; i < EPRICE_TEMPLATE.headers.length; i++) {
      const expected = EPRICE_TEMPLATE.headers[i];
      const actual = actualHeaders[i] || '';
      if (actual !== expected) {
        errors.push(`Header colonna ${i + 1}: "${actual}" invece di "${expected}"`);
      }
    }

    // 5. Validate data rows
    const rowCount = range.e.r - range.s.r; // excluding header
    if (rowCount === 0) {
      errors.push("Nessuna riga dati presente");
      return { isValid: false, errors, warnings };
    }

    // Sample validation of first 5 data rows
    const sampleSize = Math.min(5, rowCount);
    for (let R = 1; R <= sampleSize; R++) {
      const rowNum = R + 1; // Excel row number (1-indexed + header)
      
      // Validate each column
      for (let C = 0; C < EPRICE_TEMPLATE.headers.length; C++) {
        const colName = EPRICE_TEMPLATE.headers[C];
        const addr = XLSX.utils.encode_cell({ r: R, c: C });
        const cell = ws[addr];
        const validation = EPRICE_TEMPLATE.validations[colName as keyof typeof EPRICE_TEMPLATE.validations];
        
        if (!validation) continue;

        const value = cell?.v;
        const cellType = cell?.t;

        // Required check
        if (validation.required && (value === undefined || value === null || value === '')) {
          errors.push(`Riga ${rowNum}, ${colName}: valore mancante`);
          continue;
        }

        // Type validation
        if (validation.type === 'number') {
          if (typeof value !== 'number' || !Number.isFinite(value)) {
            errors.push(`Riga ${rowNum}, ${colName}: deve essere un numero (trovato: ${typeof value})`);
          } else {
            if ('min' in validation && value < validation.min!) {
              errors.push(`Riga ${rowNum}, ${colName}: valore ${value} sotto il minimo ${validation.min}`);
            }
            if ('max' in validation && value > validation.max!) {
              errors.push(`Riga ${rowNum}, ${colName}: valore ${value} sopra il massimo ${validation.max}`);
            }
          }
        } else if (validation.type === 'integer') {
          if (typeof value !== 'number' || !Number.isInteger(value)) {
            errors.push(`Riga ${rowNum}, ${colName}: deve essere un intero (trovato: ${value})`);
          }
        } else if (validation.type === 'string') {
          if ('pattern' in validation && validation.pattern && typeof value === 'string') {
            if (!validation.pattern.test(value)) {
              errors.push(`Riga ${rowNum}, ${colName}: formato non valido (${value})`);
            }
          }
        }

        // Fixed value validation
        if ('value' in validation && validation.value !== undefined) {
          if (value !== validation.value) {
            errors.push(`Riga ${rowNum}, ${colName}: valore "${value}" invece di "${validation.value}"`);
          }
        }
      }
    }

    // 6. Check price column format (should be number with 2 decimals)
    const priceColIndex = EPRICE_TEMPLATE.headers.indexOf('price');
    if (priceColIndex >= 0) {
      for (let R = 1; R <= Math.min(3, rowCount); R++) {
        const addr = XLSX.utils.encode_cell({ r: R, c: priceColIndex });
        const cell = ws[addr];
        if (cell && cell.t !== 'n') {
          warnings.push(`Riga ${R + 1}, price: tipo cella "${cell.t}" invece di "n" (numero)`);
        }
        if (cell && cell.z !== '0.00') {
          warnings.push(`Riga ${R + 1}, price: formato "${cell.z || 'default'}" invece di "0.00"`);
        }
      }
    }

    return {
      isValid: errors.length === 0,
      errors,
      warnings
    };
  }, []);

  // ePrice Export Function - reuses EAN dataset
  const onExportEprice = useCallback((event: React.MouseEvent) => {
    event.preventDefault();
    
    if (isExportingEprice) {
      toast({
        title: "Esportazione in corso...",
        description: "Attendere completamento dell'esportazione ePrice"
      });
      return;
    }
    
    setIsExportingEprice(true);
    
    try {
      // =====================================================================
      // FONTE UNICA: USARE IL DATASET DEL CATALOGO EAN
      // Il dataset eanCatalogDataset contiene i prezzi formattati "NN,99"
      // esattamente come vengono scritti nel file Excel del Catalogo EAN.
      // Se il dataset è vuoto, l'utente deve prima generare il Catalogo EAN.
      // =====================================================================
      
      if (!eanCatalogDataset || eanCatalogDataset.length === 0) {
        toast({
          title: "Catalogo EAN non disponibile",
          description: "Devi prima generare il Catalogo EAN prima di esportare per ePrice",
          variant: "destructive"
        });
        setIsExportingEprice(false);
        return;
      }
      
      // =====================================================================
      // USA DIRETTAMENTE eanCatalogDataset SENZA ULTERIORI FILTRI
      // Questo dataset è già filtrato durante la generazione del Catalogo EAN.
      // NON usare currentProcessedData o altre fonti dati.
      // =====================================================================
      
      // =====================================================================
      // LOG ESTESO: EXPORT ePRICE - CONTEGGI E CONFRONTI
      // =====================================================================
      console.log('%c[ePrice:counts] === INIZIO EXPORT ePRICE ===', 'color: #9C27B0; font-weight: bold; font-size: 14px;');
      console.log(`%c[ePrice:counts]`, 'color: #9C27B0;', { 
        'prodotti in eanCatalogDataset': eanCatalogDataset.length,
        'prodotti da esportare': eanCatalogDataset.length,
        'differenza (dovrebbe essere 0)': 0,
        timestamp: new Date().toISOString()
      });
      
      // Log primi 5 record dalla sorgente
      eanCatalogDataset.slice(0, 5).forEach((record: any, idx: number) => {
        console.log(`%c[ePrice:source:row${idx}]`, 'color: #9C27B0;', {
          EAN: record.EAN,
          SKU: record.ManufPartNr,
          'Prezzo Finale RAW (da Catalogo EAN)': record['Prezzo Finale'],
          'Prezzo Finale RAW type': typeof record['Prezzo Finale'],
          'ExistingStock RAW': record.ExistingStock
        });
      });
      
      // USA DIRETTAMENTE IL DATASET - già filtrato in onExportEAN
      const eanFilteredData = eanCatalogDataset;
      
      // Log di inizio export standardizzato (senza console.warn fuorvianti)
      console.log('%c[ePrice:export:start]', 'color: #9C27B0; font-weight: bold;', {
        rows_da_exportare: eanFilteredData.length,
        timestamp: new Date().toISOString(),
      });
      
      if (eanFilteredData.length === 0) {
        toast({
          title: "Nessuna riga valida per ePrice",
          description: "Non ci sono record con EAN validi da esportare. Genera prima il Catalogo EAN.",
          variant: "destructive"
        });
        setIsExportingEprice(false);
        return;
      }
      
      // Validate prepDays
      if (!Number.isInteger(prepDays) || prepDays < 0 || prepDays > 30) {
        toast({
          title: "Errore validazione",
          description: "I giorni di preparazione devono essere un numero intero tra 0 e 30",
          variant: "destructive"
        });
        setIsExportingEprice(false);
        return;
      }
      
      // Validation arrays for tracking issues
      const validationErrors: string[] = [];
      const validationWarnings: string[] = [];
      let skippedCount = 0;
      
      // Use template headers
      const headers = [...EPRICE_TEMPLATE.headers];
      
      // Build AOA (Array of Arrays) with exact headers
      const aoa: (string | number)[][] = [headers];
      
      // Add data rows with validation
      eanFilteredData.forEach((record, index) => {
        const rowErrors: string[] = [];
        
        // Validate SKU
        const sku = record.ManufPartNr || '';
        if (!sku || sku.trim() === '') {
          rowErrors.push(`Riga ${index + 1}: SKU mancante`);
        }
        
        // Validate EAN (already filtered but double-check format)
        const ean = record.EAN || '';
        if (!/^\d{12,14}$/.test(ean)) {
          rowErrors.push(`Riga ${index + 1}: EAN non valido (${ean})`);
        }
        
        // Validate quantity
        const quantity = record.ExistingStock ?? 0;
        if (!Number.isFinite(quantity) || quantity < 0) {
          rowErrors.push(`Riga ${index + 1}: Quantità non valida (${quantity})`);
        }
        
        // =====================================================================
        // IMPORTANTE: I prezzi ePrice derivano DIRETTAMENTE dal Catalogo EAN.
        // NON ricalcoliamo i prezzi. Usiamo il valore di "Prezzo Finale" già calcolato.
        // La conversione avviene da stringa con virgola (es. "175,99") a numero (175.99).
        // =====================================================================
        
        // =====================================================================
        // PREZZO FINALE DA CATALOGO EAN - SOLA LETTURA, NESSUN RICALCOLO
        // Il valore viene letto ESATTAMENTE come è nel Catalogo EAN.
        // Unica trasformazione: virgola -> punto e conversione a numero.
        // NON usare Math.ceil, Math.floor, Math.round, toFixed, parseInt.
        // =====================================================================
        const prezzoFinaleRaw = record['Prezzo Finale'];
        let prezzoFinaleNumber: number | null = null;
        
        if (prezzoFinaleRaw != null) {
          // Se già numero, usalo direttamente
          if (typeof prezzoFinaleRaw === 'number' && Number.isFinite(prezzoFinaleRaw)) {
            prezzoFinaleNumber = prezzoFinaleRaw;
          } else {
            // Se stringa con virgola (es. "1238,99"), sostituisci e converti
            const s = String(prezzoFinaleRaw).trim().replace(',', '.');
            if (s) {
              const n = parseFloat(s);
              if (Number.isFinite(n)) {
                prezzoFinaleNumber = n;
              }
            }
          }
        }
        
        // LOG ESTESO per i primi 10 record - verifica COMPLETA coerenza prezzi
        if (index < 10) {
          const rawValue = prezzoFinaleRaw;
          const afterReplace = typeof rawValue === 'string' ? rawValue.replace(',', '.') : String(rawValue);
          const terminaCon99 = prezzoFinaleNumber !== null && (Math.round(prezzoFinaleNumber * 100) % 100) === 99;
          
          console.log(`%c[ePrice:export:row${index}]`, 'color: #9C27B0;', {
            EAN: ean,
            SKU: sku,
            'RAW Prezzo Finale (da eanCatalogDataset)': rawValue,
            'RAW type': typeof rawValue,
            'dopo replace virgola': afterReplace,
            'parseFloat result': prezzoFinaleNumber,
            'valore scritto in Excel': prezzoFinaleNumber,
            'termina_con_99': terminaCon99,
            'ExistingStock RAW': record.ExistingStock,
            'quantity esportato': quantity
          });
          
          // ALERT se il prezzo non termina con .99
          if (!terminaCon99 && prezzoFinaleNumber !== null) {
            console.error(`%c[ePrice:warning:price-mismatch:row${index}] PREZZO NON TERMINA CON .99!`, 'color: red; font-weight: bold;', {
              EAN: ean,
              prezzoFinale: prezzoFinaleNumber,
              raw: rawValue
            });
          }
        }
        
        // Validate price exists and is valid
        if (prezzoFinaleNumber === null || prezzoFinaleNumber <= 0) {
          rowErrors.push(`Riga ${index + 1}: Prezzo Finale mancante o non valido (SKU: ${sku}, raw: ${prezzoFinaleRaw})`);
        }
        
        // If there are critical errors for this row, skip it
        if (rowErrors.length > 0) {
          validationErrors.push(...rowErrors);
          skippedCount++;
          return; // Skip this row
        }
        
        // VERIFICA FINALE: il prezzo deve terminare con .99
        if (prezzoFinaleNumber !== null) {
          const cents = Math.round(prezzoFinaleNumber * 100) % 100;
          if (cents !== 99) {
            console.error(`%c[ePrice:warning:not99:row${index}] PREZZO NON TERMINA CON .99!`, 'color: red; font-weight: bold;', {
              EAN: ean,
              prezzoFinale: prezzoFinaleNumber,
              cents: cents,
              raw: prezzoFinaleRaw
            });
          }
        }
        
        // Build row with exact template structure and fixed values
        aoa.push([
          sku,                                        // sku
          ean,                                        // product-id
          EPRICE_TEMPLATE.fixedValues["product-id-type"], // product-id-type = "EAN"
          prezzoFinaleNumber ?? 0,                    // price (NUMBER with dot, e.g., 175.99)
          quantity,                                   // quantity - ESATTO dal Catalogo EAN
          EPRICE_TEMPLATE.fixedValues["state"],       // state = 11
          prepDays,                                   // fulfillment-latency
          EPRICE_TEMPLATE.fixedValues["logistic-class"] // logistic-class = "K"
        ]);
      });
      
      // Check if we have valid rows after validation
      if (aoa.length <= 1) {
        toast({
          title: "Errore validazione",
          description: `Nessuna riga valida dopo la validazione. ${validationErrors.length} errori trovati.`,
          variant: "destructive"
        });
        console.error("Errori validazione ePrice:", validationErrors);
        setIsExportingEprice(false);
        return;
      }
      
      // Show warnings if any (but continue export)
      if (validationWarnings.length > 0) {
        console.warn("Warning validazione ePrice:", validationWarnings);
      }
      
      // Show summary if rows were skipped
      if (skippedCount > 0) {
        toast({
          title: "Attenzione",
          description: `${skippedCount} righe saltate per errori di validazione. Esportate ${aoa.length - 1} righe valide.`,
        });
        console.warn("Righe saltate:", validationErrors);
      }
      
      // Create worksheet from AOA
      const ws = XLSX.utils.aoa_to_sheet(aoa);
      
      // Force product-id (column B) to text format to preserve leading zeros
      if (ws['!ref']) {
        const range = XLSX.utils.decode_range(ws['!ref']);
        const eanCol = 1; // Column B (product-id)
        
        for (let R = 1; R <= range.e.r; R++) {
          const addr = XLSX.utils.encode_cell({ r: R, c: eanCol });
          const cell = ws[addr];
          if (cell) {
            cell.v = (cell.v ?? '').toString();
            cell.t = 's';
            cell.z = '@';
            ws[addr] = cell;
          }
        }
        
        // Format price (column D) as NUMBER with 2 decimal places (0.00)
        // NON forzare a testo - deve essere un numero con punto decimale
        const priceCol = 3; // Column D (price)
        for (let R = 1; R <= range.e.r; R++) {
          const addr = XLSX.utils.encode_cell({ r: R, c: priceCol });
          const cell = ws[addr];
          if (cell && typeof cell.v === 'number') {
            cell.t = 'n'; // number type
            cell.z = '0.00'; // format with 2 decimal places
            ws[addr] = cell;
          }
        }
      }
      
      // Create workbook with official sheet name
      const wb = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(wb, ws, EPRICE_TEMPLATE.sheetName);
      
      // =====================================================================
      // VALIDAZIONE STRUTTURA: Verifica che il file generato sia conforme
      // al template ufficiale prima del download
      // =====================================================================
      const validationResult = validateEpriceStructure(ws, EPRICE_TEMPLATE.sheetName);
      
      // Build validation report for UI - USA eanCatalogDataset NON currentProcessedData
      const validationReport: ExportValidationResult = {
        exportType: 'ePrice',
        timestamp: new Date(),
        success: validationResult.isValid,
        totalRows: eanCatalogDataset.length, // CORRETTO: usa eanCatalogDataset
        validRows: aoa.length - 1,
        skippedRows: skippedCount,
        errors: validationErrors.map(e => ({ row: 0, field: '', reason: e })),
        warnings: [...validationWarnings, ...validationResult.warnings],
        structureErrors: validationResult.errors
      };
      
      // LOG STANDARDIZZATO: riepilogo export ePRICE
      const exportedRows = aoa.length - 1;
      const inputRows = eanCatalogDataset.length;
      
      console.log('%c[ePrice:export:summary]', 'color: #9C27B0; font-weight: bold;', {
        prodotti_input_CatalogoEAN: inputRows,
        prodotti_ePrice_export: exportedRows,
        prodotti_scartati: skippedCount,
        allineamento_set: inputRows === (exportedRows + skippedCount) ? 'OK' : 'MISMATCH'
      });
      
      // === LOG VERIFICA FINALE ePRICE ===
      console.log('%c[check:export-final-ePrice]', 'color: #E91E63; font-weight: bold;', {
        catalogoEAN_rows: inputRows,
        ePrice_export_rows: exportedRows,
        difference: inputRows - exportedRows - skippedCount,
        status: inputRows === (exportedRows + skippedCount) ? 'OK' : 'MISMATCH'
      });
      
      if (!validationResult.isValid) {
        console.error("Errori struttura template ePrice:", validationResult.errors);
        validationReport.success = false;
        setLastEpriceValidation(validationReport);
        toast({
          title: "Errore struttura file",
          description: `Il file generato non è conforme al template: ${validationResult.errors.slice(0, 3).join('; ')}`,
          variant: "destructive"
        });
        setIsExportingEprice(false);
        return;
      }
      
      if (validationResult.warnings.length > 0) {
        console.warn("Warning struttura template ePrice:", validationResult.warnings);
      }
      
      // Serialize to ArrayBuffer
      const wbout = XLSX.write(wb, { bookType: "xlsx", type: "array" });
      
      if (!wbout || wbout.length === 0) {
        toast({
          title: "Errore generazione file",
          description: "Buffer vuoto durante la generazione del file ePrice",
          variant: "destructive"
        });
        return;
      }
      
      // Create blob and download
      const blob = new Blob([wbout], { 
        type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" 
      });
      
      const url = URL.createObjectURL(blob);
      const fileName = "Tracciato_Pubblicazione_Offerte_new.xlsx";
      
      const a = document.createElement("a");
      a.href = url;
      a.download = fileName;
      a.rel = "noopener";
      a.style.display = "none";
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      
      setTimeout(() => URL.revokeObjectURL(url), 3000);
      
      // Save validation report for UI
      setLastEpriceValidation(validationReport);
      
      toast({
        title: "Export ePrice completato",
        description: `File generato con ${aoa.length - 1} righe, conforme al template ufficiale`
      });
      
    } catch (error) {
      console.error('Errore export ePrice:', error);
      toast({
        title: "Errore export ePrice",
        description: error instanceof Error ? error.message : "Errore sconosciuto durante l'export",
        variant: "destructive"
      });
    } finally {
      setIsExportingEprice(false);
    }
  }, [eanCatalogDataset, isExportingEprice, prepDays, toast, validateEpriceStructure]);

  // Mediaworld Export Function - reuses EAN dataset
  const onExportMediaworld = useCallback(async (event: React.MouseEvent) => {
    event.preventDefault();
    
    if (isExportingMediaworld) {
      toast({
        title: "Esportazione in corso...",
        description: "Attendere completamento dell'esportazione Mediaworld"
      });
      return;
    }
    
    // =====================================================================
    // FONTE UNICA: USARE IL DATASET DEL CATALOGO EAN
    // Il dataset eanCatalogDataset contiene i prezzi formattati "NN,99"
    // esattamente come vengono scritti nel file Excel del Catalogo EAN.
    // =====================================================================
    if (!eanCatalogDataset || eanCatalogDataset.length === 0) {
      toast({
        title: "Catalogo EAN non disponibile",
        description: "Devi prima generare il Catalogo EAN prima di esportare per Mediaworld",
        variant: "destructive"
      });
      return;
    }
    
    // === LOG ESTESO: VERIFICA FONTE DATI MEDIAWORLD ===
    console.log('%c[Mediaworld:source:check] === VERIFICA FONTE DATI ===', 'color: #00BCD4; font-weight: bold; font-size: 14px;');
    console.log(`%c[Mediaworld:source:check]`, 'color: #00BCD4;', { 
      'eanCatalogDataset length': eanCatalogDataset.length,
      'exists': !!eanCatalogDataset,
      'isArray': Array.isArray(eanCatalogDataset),
      timestamp: new Date().toISOString()
    });
    
    // Log primi 5 record per debug COMPLETO
    eanCatalogDataset.slice(0, 5).forEach((record: any, idx: number) => {
      const prezzoFinaleRaw = record['Prezzo Finale'];
      const listPriceConFeeRaw = record['ListPrice con Fee'];
      const terminaCon99 = typeof prezzoFinaleRaw === 'string' && prezzoFinaleRaw.endsWith(',99');
      
      console.log(`%c[Mediaworld:source:row${idx}]`, 'color: #00BCD4;', {
        EAN: record.EAN,
        SKU: record.ManufPartNr,
        'Prezzo Finale RAW': prezzoFinaleRaw,
        'Prezzo Finale type': typeof prezzoFinaleRaw,
        'termina_con_99': terminaCon99,
        'ListPrice con Fee RAW': listPriceConFeeRaw,
        'ListPrice con Fee type': typeof listPriceConFeeRaw,
        'ExistingStock': record.ExistingStock
      });
      
      // ALERT se il prezzo NON termina con ,99
      if (!terminaCon99) {
        console.error(`%c[Mediaworld:source:ERROR:row${idx}] PREZZO NON TERMINA CON ,99!`, 'color: red; font-weight: bold;', {
          EAN: record.EAN,
          prezzoFinale: prezzoFinaleRaw,
          type: typeof prezzoFinaleRaw
        });
      }
    });
    
    // Validate prepDaysMediaworld
    if (!Number.isInteger(prepDaysMediaworld) || prepDaysMediaworld < 1 || prepDaysMediaworld > 45) {
      toast({
        title: "Errore validazione",
        description: "I giorni di preparazione devono essere un numero intero tra 1 e 45",
        variant: "destructive"
      });
      return;
    }
    
    setIsExportingMediaworld(true);
    
    try {
      // USA eanCatalogDataset come fonte unica (log standardizzato, no console.warn)
      console.log('%c[Mediaworld:export:start]', 'color: #00BCD4; font-weight: bold;', {
        rows_da_exportare: eanCatalogDataset.length,
        timestamp: new Date().toISOString(),
      });
      
      const result = await exportMediaworldCatalog({
        processedData: eanCatalogDataset,
        feeConfig,
        prepDays: prepDaysMediaworld
      });
      
      // === LOG STANDARDIZZATO: RIEPILOGO EXPORT MEDIAWORLD ===
      const mwExportedRows = result.rowCount ?? 0;
      const mwSkippedRows = result.skippedCount ?? 0;
      const mwInputRows = eanCatalogDataset.length;
      
      console.log('%c[Mediaworld:export:summary]', 'color: #00BCD4; font-weight: bold;', {
        prodotti_input_CatalogoEAN: mwInputRows,
        prodotti_Mediaworld_export: mwExportedRows,
        prodotti_scartati: mwSkippedRows,
        allineamento_set: mwInputRows === (mwExportedRows + mwSkippedRows) ? 'OK' : 'MISMATCH'
      });
      
      // === LOG VERIFICA FINALE MEDIAWORLD ===
      console.log('%c[check:export-final-Mediaworld]', 'color: #E91E63; font-weight: bold;', {
        catalogoEAN_rows: mwInputRows,
        Mediaworld_export_rows: mwExportedRows,
        difference: mwInputRows - mwExportedRows - mwSkippedRows,
        status: mwInputRows === (mwExportedRows + mwSkippedRows) ? 'OK' : 'MISMATCH'
      });
      
      // Build validation report for UI
      const mediaworldValidationReport: ExportValidationResult = {
        exportType: 'Mediaworld',
        timestamp: new Date(),
        success: result.success ?? false,
        totalRows: eanCatalogDataset.length,
        validRows: result.rowCount ?? 0,
        skippedRows: result.skippedCount ?? 0,
        errors: (result.validationErrors ?? []).map(e => ({
          row: e.row,
          field: e.field,
          reason: e.reason,
          sku: e.sku
        })),
        warnings: [],
        structureErrors: []
      };
      
      if (result.success) {
        // Show success with validation summary if there were skipped rows
        if (result.skippedCount && result.skippedCount > 0) {
          toast({
            title: "Export Mediaworld completato con avvisi",
            description: `${result.rowCount} righe esportate, ${result.skippedCount} righe scartate per errori di validazione`
          });
          
          // Log validation errors for debugging
          if (result.validationErrors && result.validationErrors.length > 0) {
            console.warn('Mediaworld validation errors:', result.validationErrors);
          }
        } else {
          toast({
            title: "Export Mediaworld completato",
            description: `File mediaworld-offers-*.xlsx generato con ${result.rowCount} righe`
          });
        }
      } else {
        // Show error with validation details
        let errorDescription = result.error || "Errore sconosciuto";
        
        if (result.validationErrors && result.validationErrors.length > 0) {
          const firstErrors = result.validationErrors.slice(0, 3);
          const errorDetails = firstErrors.map(e => `Riga ${e.row}: ${e.field} - ${e.reason}`).join('; ');
          errorDescription += `. Primi errori: ${errorDetails}`;
          
          console.error('Mediaworld validation errors:', result.validationErrors);
        }
        
        toast({
          title: "Errore export Mediaworld",
          description: errorDescription,
          variant: "destructive"
        });
      }
      
      // Save validation report for UI
      setLastMediaworldValidation(mediaworldValidationReport);
    } catch (error) {
      console.error('Errore export Mediaworld:', error);
      toast({
        title: "Errore export Mediaworld",
        description: error instanceof Error ? error.message : "Errore sconosciuto durante l'export",
        variant: "destructive"
      });
    } finally {
      setIsExportingMediaworld(false);
    }
  }, [eanCatalogDataset, isExportingMediaworld, feeConfig, prepDaysMediaworld, toast]);

  // SFTP upload is now integrated into onExportEAN - no separate function needed

  const downloadExcel = (type: 'ean' | 'manufpartnr') => {
    if (type === 'ean') {
      // Use the new onExportEAN function for EAN catalog
      onExportEAN({ preventDefault: () => {} } as React.MouseEvent);
      return;
    }
    
    // MPN export with validations
    try {
      // Filter MPN rows from current processed data
      const mpnRows = currentProcessedData.filter(record => {
        const hasManufPartNr = record.ManufPartNr && record.ManufPartNr.trim().length > 0;
        
        // Additional validation for negative or null components
        if (hasManufPartNr) {
          const listPrice = asNum(record.ListPrice);
          const shipping = feeConfig.shippingCost;
          const iva = 0.22;
          const feeDeRev = asNum(record.FeeDeRev);
          const feeMarketplace = asNum(record['Fee Marketplace']);
          const postFee = asNum(record['Subtotale post-fee']);
          
          if (listPrice < 0 || shipping < 0 || iva < 0 || feeDeRev <= 0 || feeMarketplace <= 0 || postFee < 0) {
            console.log({
              source_file: 'material',
              line: 0,
              Matnr: record.Matnr,
              ManufPartNr: record.ManufPartNr,
              EAN: record.EAN,
              reason: 'negative_or_null_component',
              details: `Invalid components: listPrice=${listPrice}, feeDeRev=${feeDeRev}, feeMarketplace=${feeMarketplace}, postFee=${postFee}`
            });
            return false; // Exclude this row
          }
        }
        
        return hasManufPartNr;
      });
      
      // Check if dataset is empty after filtering
      if (!Array.isArray(mpnRows) || mpnRows.length === 0) {
        console.warn("MPN: dataset vuoto, export annullato");
        toast({
          title: "Nessuna riga valida per MPN",
          description: "Non ci sono record con ManufPartNr validi da esportare"
        });
        return;
      }
      
      // Format data for export and remove internal columns
      const excelData = formatExcelData(mpnRows)
        .filter(record => record !== null) // Remove null records from validation failures
        .map(record => {
          const cleanRecord = { ...record };
          // Remove any internal columns like _eanFinalCents
          Object.keys(cleanRecord).forEach(key => {
            if (key.startsWith('_')) {
              delete cleanRecord[key];
            }
          });
          return cleanRecord;
        });
      
      // Validate ending 99 cents
      excelData.forEach((record, index) => {
        const prezzoFinale = String(record['Prezzo Finale'] ?? '');
        if (!prezzoFinale.endsWith(',99')) {
          console.warn("mpnEnding99:ko", `row_${index}_${record.Matnr || 'unknown'}`);
        }
      });
      
      dbg('excel:write:start');
      
      const { timestamp, sheetName } = getTimestamp();
      const filename = `Catalogo_MPN.xlsx`;

      const ws = XLSX.utils.json_to_sheet(excelData);
      
      // Force EAN column to text format for both pipelines
      forceEANText(ws);
      
      const wb = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(wb, ws, sheetName);
      
      console.info('mpn:wb', mpnRows.length);
      XLSX.writeFile(wb, filename);

      setExcelDone(true);
      dbg('excel:write:done', { pipeline: type });

      toast({
        title: "Excel scaricato",
        description: `File ${filename} scaricato con successo`
      });
    } catch (e) {
      console.error('mpn:export-fail', e);
      toast({
        title: "Errore durante l'export MPN",
        description: e instanceof Error ? e.message : "Errore sconosciuto",
        variant: "destructive"
      });
    }
  };

  const downloadDiscardedRows = () => {
    if (discardedRows.length === 0) return;
    exportDiscardedRowsCSV(discardedRows, `righe_scartate_EAN_${new Date().toISOString().split('T')[0]}`);
  };

  const handleEANMappingUpload = async (file: File) => {
    // Validate file extension
    const validExtensions = ['.csv', '.txt'];
    const fileExt = file.name.substring(file.name.lastIndexOf('.')).toLowerCase();
    
    if (!validExtensions.includes(fileExt)) {
      toast({
        title: "Formato file non valido",
        description: "Accettati solo file .csv o .txt",
        variant: "destructive"
      });
      return;
    }
    
    setFiles(prev => ({
      ...prev,
      eanMapping: { file, status: 'ready' }
    }));
    
    // Persist to Supabase Storage
    await persistMappingFile(file);
    
    toast({
      title: "File caricato",
      description: `${file.name} pronto per l'elaborazione`
    });
  };

  // =====================================================================
  // OVERRIDE PRODOTTI - Gestione file
  // =====================================================================
  
  const handleOverrideFileUpload = async (file: File) => {
    // Validate file extension
    const fileExt = file.name.substring(file.name.lastIndexOf('.')).toLowerCase();
    
    if (fileExt !== '.xlsx' && fileExt !== '.xls') {
      toast({
        title: "Formato file non valido",
        description: "Accettati solo file Excel (.xlsx, .xls)",
        variant: "destructive"
      });
      return;
    }
    
    try {
      const buffer = await file.arrayBuffer();
      const result = parseOverrideFile(buffer);
      
      setOverrideState({
        file,
        filename: file.name,
        uploadedAt: new Date(),
        index: result.index,
        errors: result.errors,
        disabled: false,
        validCount: result.validCount,
        invalidCount: result.invalidCount,
        lastApplyStats: null
      });
      
      if (result.success && result.index) {
        // Persist to Supabase Storage
        await persistOverrideFile(file);
        
        toast({
          title: "File override caricato",
          description: `${result.validCount} righe valide, ${result.invalidCount} scartate`
        });
      } else {
        toast({
          title: "Errore file override",
          description: result.errors[0]?.reason || "File non valido",
          variant: "destructive"
        });
      }
    } catch (error) {
      toast({
        title: "Errore lettura file",
        description: error instanceof Error ? error.message : "Errore sconosciuto",
        variant: "destructive"
      });
    }
  };
  
  const handleDisableOverride = () => {
    setOverrideState(prev => ({
      ...prev,
      disabled: true
    }));
    toast({
      title: "Override disabilitato",
      description: "L'override non verrà applicato alla prossima generazione"
    });
  };
  
  const handleEnableOverride = () => {
    setOverrideState(prev => ({
      ...prev,
      disabled: false
    }));
    toast({
      title: "Override riabilitato",
      description: "L'override verrà applicato alla prossima generazione"
    });
  };
  
  const handleRemoveOverrideFile = async () => {
    // Delete from Supabase Storage
    await deleteOverrideFileFromStorage();
    
    setOverrideState({
      file: null,
      filename: null,
      uploadedAt: null,
      index: null,
      errors: [],
      disabled: false,
      validCount: 0,
      invalidCount: 0,
      lastApplyStats: null
    });
    toast({
      title: "File override rimosso",
      description: "Il file override è stato eliminato"
    });
  };
  
  // Check if override should be applied
  const shouldApplyOverride = overrideState.file !== null && 
                               overrideState.index !== null && 
                               !overrideState.disabled;

  const processEANPrefill = async () => {
    // Fonte di verità per Material: usa filesRef.current per coerenza con l'UI
    // L'UI mostra "Caricato" quando files.material.status === 'valid'
    // Usiamo filesRef.current per evitare closure stale in contesti async
    const currentFiles = filesRef.current;
    
    // Debug: log per diagnosticare lo stato del Material
    console.log("DEBUG Material presence", { 
      filesMaterialFile: !!currentFiles.material.file, 
      filesMaterialStatus: currentFiles.material.status,
      mappingInfo: !!mappingInfo,
      eanMappingFile: !!currentFiles.eanMapping.file
    });
    
    // hasMaterial: true se file presente E status valido (coerente con badge "Caricato" nella UI)
    const hasMaterial = !!currentFiles.material.file && 
                        (currentFiles.material.status === 'valid' || currentFiles.material.status === 'warning');
    
    // Verifica file Material usando la stessa logica dell'UI
    if (!hasMaterial) {
      toast({
        title: "File Material mancante",
        description: "Carica il file Material prima di eseguire il pre-fill EAN.",
        variant: "destructive"
      });
      return;
    }
    
    // Verifica file mapping: controlla sia file in memoria sia mapping salvato (mappingInfo)
    if (!currentFiles.eanMapping.file) {
      // Se non c'è il file in memoria, controlla se esiste un mapping salvato
      if (mappingInfo) {
        // Mapping già configurato e salvato in precedenza: considera il prefill come già disponibile
        console.log('[processEANPrefill] Mapping già salvato (mappingInfo), prefill considerato disponibile');
        setPrefillState(prev => ({ ...prev, status: 'done' }));
        return;
      } else {
        // Nessun mapping configurato
        toast({
          title: "File di mapping mancante",
          description: "Carica un file SKU↔EAN oppure esegui la pipeline senza pre-fill EAN.",
          variant: "destructive"
        });
        return;
      }
    }
    
    setPrefillState(prev => ({ ...prev, status: 'running' }));
    setFiles(prev => ({
      ...prev,
      eanMapping: { ...prev.eanMapping, status: 'processing' }
    }));
    
    try {
      // Read mapping file as text (usa currentFiles per coerenza)
      const mappingText = await currentFiles.eanMapping.file!.text();
      
      // Validate header (case-insensitive)
      const lines = mappingText.split('\n');
      const header = lines[0]?.trim().toLowerCase();
      
      if (header !== 'mpn;ean') {
        throw new Error('Header richiesto: mpn;ean');
      }
      
      const materialData = currentFiles.material.file!.data;
      
      // Diagnostic: Count EAN before pre-fill
      const countEANNonVuoti_before = materialData.filter((row: any) => {
        const ean = String(row.EAN ?? '').trim();
        return ean.length > 0;
      }).length;
      console.warn('prefill:diagnostic:before', { total: materialData.length, nonEmpty: countEANNonVuoti_before });
      
      // Use worker for large files
      if (lines.length > 100000 || materialData.length > 100000) {
        // Process with Web Worker
        return new Promise<void>((resolve, reject) => {
          const worker = new Worker('/ean-prefill-worker.js');
          eanPrefillWorkerRef.current = worker;
          
          worker.onmessage = (e) => {
            if (e.data.error) {
              reject(new Error(e.data.message));
              return;
            }
            
            if (e.data.success) {
              const updatedMaterial = e.data.updatedMaterial;
              
              // Diagnostic: Verify material was updated
              const countEANNonVuoti_after = updatedMaterial.filter((row: any) => {
                const ean = String(row.EAN ?? '').trim();
                return ean.length > 0;
              }).length;
              console.warn('prefill:diagnostic:after_worker', { total: updatedMaterial.length, nonEmpty: countEANNonVuoti_after });
              
              if (countEANNonVuoti_after < countEANNonVuoti_before) {
                worker.terminate();
                eanPrefillWorkerRef.current = null;
                reject(new Error('Material non aggiornato dopo Pre-fill'));
                return;
              }
              
              // Update material file with filled EANs
              setFiles(prev => ({
                ...prev,
                material: {
                  ...prev.material,
                  file: prev.material.file ? {
                    ...prev.material.file,
                    data: updatedMaterial
                  } : null
                },
                eanMapping: { ...prev.eanMapping, status: 'completed' }
              }));
              
              // Update state with counters and increment version
              setPrefillState({
                status: 'done',
                counters: e.data.counters,
                reports: e.data.reports
              });
              setMaterialRows(updatedMaterial);
              setMaterialVersion(prev => prev + 1);
              
              toast({
                title: "Pre-fill completato",
                description: `${e.data.counters.filled_now} EAN aggiunti`
              });
              
              worker.terminate();
              eanPrefillWorkerRef.current = null;
              resolve();
            }
          };
          
          worker.onerror = (error) => {
            reject(error);
          };
          
          worker.postMessage({
            mappingText,
            materialData,
            counters: {}
          });
        }).catch((error) => {
          setPrefillState({ status: 'idle', counters: null, reports: null });
          setFiles(prev => ({
            ...prev,
            eanMapping: { ...prev.eanMapping, status: 'error', error: error.message }
          }));
          toast({
            title: "Errore elaborazione",
            description: error.message,
            variant: "destructive"
          });
        });
      }
      
      // Process without worker (synchronous for small files)
      const mappingMap = new Map<string, string>();
      const reports: EANPrefillReports = {
        duplicate_mpn_rows: [],
        empty_ean_rows: [],
        errori_formali: [],
        updated: [],
        already_populated: [],
        skipped_due_to_conflict: [],
        mpn_not_in_material: [],
        missing_mapping_in_new_file: []
      };
      
      const counters: EANPrefillCounters = {
        already_populated: 0,
        filled_now: 0,
        skipped_due_to_conflict: 0,
        duplicate_mpn_rows: 0,
        mpn_not_in_material: 0,
        empty_ean_rows: 0,
        missing_mapping_in_new_file: 0,
        errori_formali: 0
      };
      
      // Parse mapping file
      for (let i = 1; i < lines.length; i++) {
        const line = lines[i]?.trim();
        if (!line) continue;
        
        const parts = line.split(';');
        if (parts.length < 2) {
          counters.errori_formali++;
          reports.errori_formali.push({
            raw_line: line,
            reason: 'formato_errato',
            row_index: i + 1
          });
          continue;
        }
        
        const mpn = parts[0]?.trim();
        const ean = parts[1]?.trim();
        
        if (!ean) {
          counters.empty_ean_rows++;
          reports.empty_ean_rows.push({
            mpn: mpn,
            row_index: i + 1
          });
          continue;
        }
        
        if (mappingMap.has(mpn)) {
          const existing = mappingMap.get(mpn)!;
          if (existing !== ean) {
            counters.duplicate_mpn_rows++;
            reports.duplicate_mpn_rows.push({
              mpn: mpn,
              ean_seen_first: existing,
              ean_conflicting: ean,
              row_index: i + 1
            });
          }
        } else {
          mappingMap.set(mpn, ean);
        }
      }
      
      // Create a set of all MPNs in material
      const materialMPNs = new Set(
        materialData.map((row: any) => row.ManufPartNr?.toString().trim()).filter(Boolean)
      );
      
      // Check for MPNs in mapping that don't exist in material
      for (const [mpn, ean] of mappingMap.entries()) {
        if (!materialMPNs.has(mpn)) {
          counters.mpn_not_in_material++;
          reports.mpn_not_in_material.push({
            mpn: mpn,
            ean: ean,
            row_index: 0
          });
        }
      }
      
      // Process material rows - CRITICAL: treat EAN as string, never use Number/parseInt
      const updatedMaterial = materialData.map((row: any) => {
        const newRow = { ...row };
        const mpn = String(row.ManufPartNr ?? '').trim();
        const currentEAN = String(row.EAN ?? '').trim();
        
        if (currentEAN) {
          // EAN already populated
          counters.already_populated++;
          reports.already_populated.push({
            ManufPartNr: mpn,
            EAN_existing: currentEAN
          });
          
          // Check if there's also a mapping for this MPN
          if (mpn && mappingMap.has(mpn)) {
            const mappingEAN = mappingMap.get(mpn)!;
            counters.skipped_due_to_conflict++;
            reports.skipped_due_to_conflict.push({
              ManufPartNr: mpn,
              EAN_material: currentEAN,
              EAN_mapping_first: mappingEAN
            });
          }
        } else if (mpn && mappingMap.has(mpn)) {
          // EAN empty and mapping exists - fill it (as string)
          const mappingEAN = mappingMap.get(mpn)!;
          newRow.EAN = mappingEAN;
          counters.filled_now++;
          reports.updated.push({
            ManufPartNr: mpn,
            EAN_old: currentEAN || '',
            EAN_new: mappingEAN
          });
        } else {
          // EAN empty and no mapping found
          counters.missing_mapping_in_new_file++;
          reports.missing_mapping_in_new_file.push({
            ManufPartNr: mpn || ''
          });
        }
        
        return newRow;
      });
      
      // Diagnostic: Verify material was updated
      const countEANNonVuoti_after = updatedMaterial.filter((row: any) => {
        const ean = String(row.EAN ?? '').trim();
        return ean.length > 0;
      }).length;
      console.warn('prefill:diagnostic:after_sync', { total: updatedMaterial.length, nonEmpty: countEANNonVuoti_after });
      
      if (countEANNonVuoti_after < countEANNonVuoti_before) {
        throw new Error('Material non aggiornato dopo Pre-fill');
      }
      
      // Update material file with filled EANs
      setFiles(prev => ({
        ...prev,
        material: {
          ...prev.material,
          file: prev.material.file ? {
            ...prev.material.file,
            data: updatedMaterial
          } : null
        },
        eanMapping: { ...prev.eanMapping, status: 'completed' }
      }));
      
      // Update state with counters and increment version
      setPrefillState({
        status: 'done',
        counters: counters,
        reports: reports
      });
      setMaterialRows(updatedMaterial);
      setMaterialVersion(prev => prev + 1);
      
      toast({
        title: "Pre-fill completato",
        description: `${counters.filled_now} EAN aggiunti`
      });
      
    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : 'Errore sconosciuto';
      setPrefillState({ status: 'idle', counters: null, reports: null });
      setFiles(prev => ({
        ...prev,
        eanMapping: { ...prev.eanMapping, status: 'error', error: errorMsg }
      }));
      toast({
        title: "Errore elaborazione",
        description: errorMsg,
        variant: "destructive"
      });
    }
  };

  const downloadEANPrefillReport = () => {
    if (!eanPrefillReports) {
      toast({
        title: "Nessun report disponibile",
        description: "Elabora prima le associazioni EAN",
        variant: "destructive"
      });
      return;
    }
    
    try {
      const wb = XLSX.utils.book_new();
      
      // Helper to force columns as text
      const forceColumnsAsText = (ws: XLSX.WorkSheet, columnNames: string[]) => {
        if (!ws['!ref']) return;
        const range = XLSX.utils.decode_range(ws['!ref']);
        
        for (const colName of columnNames) {
          let colIndex = -1;
          // Find column index
          for (let C = range.s.c; C <= range.e.c; C++) {
            const addr = XLSX.utils.encode_cell({ r: 0, c: C });
            const cell = ws[addr];
            const name = (cell?.v ?? '').toString().trim();
            if (name === colName) {
              colIndex = C;
              break;
            }
          }
          
          if (colIndex < 0) continue;
          
          // Force cells as text
          for (let R = 1; R <= range.e.r; R++) {
            const addr = XLSX.utils.encode_cell({ r: R, c: colIndex });
            const cell = ws[addr];
            if (!cell) continue;
            cell.v = (cell.v ?? '').toString();
            cell.t = 's';
            cell.z = '@';
            ws[addr] = cell;
          }
        }
      };
      
      // Sheet 1: Updated
      if (eanPrefillReports.updated.length > 0) {
        const ws1 = XLSX.utils.json_to_sheet(eanPrefillReports.updated);
        XLSX.utils.book_append_sheet(wb, ws1, "updated");
        forceColumnsAsText(ws1, ['EAN_old', 'EAN_new']);
      }
      
      // Sheet 2: Already Populated
      if (eanPrefillReports.already_populated.length > 0) {
        const ws2 = XLSX.utils.json_to_sheet(eanPrefillReports.already_populated);
        XLSX.utils.book_append_sheet(wb, ws2, "already_populated");
        forceColumnsAsText(ws2, ['EAN_existing']);
      }
      
      // Sheet 3: Skipped Due to Conflict
      if (eanPrefillReports.skipped_due_to_conflict.length > 0) {
        const ws3 = XLSX.utils.json_to_sheet(eanPrefillReports.skipped_due_to_conflict);
        XLSX.utils.book_append_sheet(wb, ws3, "skipped_due_to_conflict");
        forceColumnsAsText(ws3, ['EAN_material', 'EAN_mapping_first']);
      }
      
      // Sheet 4: Duplicate MPN Rows
      if (eanPrefillReports.duplicate_mpn_rows.length > 0) {
        const ws4 = XLSX.utils.json_to_sheet(eanPrefillReports.duplicate_mpn_rows);
        XLSX.utils.book_append_sheet(wb, ws4, "duplicate_mpn_rows");
        forceColumnsAsText(ws4, ['ean_seen_first', 'ean_conflicting']);
      }
      
      // Sheet 5: MPN Not in Material
      if (eanPrefillReports.mpn_not_in_material.length > 0) {
        const ws5 = XLSX.utils.json_to_sheet(eanPrefillReports.mpn_not_in_material);
        XLSX.utils.book_append_sheet(wb, ws5, "mpn_not_in_material");
        forceColumnsAsText(ws5, ['ean']);
      }
      
      // Sheet 6: Empty EAN Rows
      if (eanPrefillReports.empty_ean_rows.length > 0) {
        const ws6 = XLSX.utils.json_to_sheet(eanPrefillReports.empty_ean_rows);
        XLSX.utils.book_append_sheet(wb, ws6, "empty_ean_rows");
      }
      
      // Sheet 7: Missing Mapping
      if (eanPrefillReports.missing_mapping_in_new_file.length > 0) {
        const ws7 = XLSX.utils.json_to_sheet(eanPrefillReports.missing_mapping_in_new_file);
        XLSX.utils.book_append_sheet(wb, ws7, "missing_mapping_in_new_file");
      }
      
      // Sheet 8: Errori Formali
      if (eanPrefillReports.errori_formali.length > 0) {
        const ws8 = XLSX.utils.json_to_sheet(eanPrefillReports.errori_formali);
        XLSX.utils.book_append_sheet(wb, ws8, "errori_formali");
      }
      
      const wbout = XLSX.write(wb, { bookType: 'xlsx', type: 'array' });
      const blob = new Blob([wbout], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
      const url = URL.createObjectURL(blob);
      
      const a = document.createElement('a');
      a.href = url;
      a.download = 'ean_prefill_report.xlsx';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
      
      toast({
        title: "Report scaricato",
        description: "File ean_prefill_report.xlsx scaricato con successo"
      });
      
    } catch (error) {
      toast({
        title: "Errore generazione report",
        description: error instanceof Error ? error.message : 'Errore sconosciuto',
        variant: "destructive"
      });
    }
  };

  // =====================================================================
  // PIPELINE MASTER: Esecuzione sequenziale completa
  // Orchestrazione: handleFtpImport → processEANPrefill → processDataPipeline('EAN') → downloadExcel('ean')
  // =====================================================================
  const handleFullPipeline = async () => {
    if (pipelineRunning) return;
    
    setPipelineRunning(true);
    setPipelineStatus('');
    setPipelineProgress(0);
    setPipelineStepLabel('Inizializzazione pipeline…');
    
    try {
      // =====================================================================
      // STEP 1: Import FTP
      // =====================================================================
      setPipelineProgress(0);
      setPipelineStepLabel('Import FTP in corso…');
      setPipelineStatus('Import FTP in corso…');
      console.log('[Pipeline Master] Step 1: handleFtpImport');
      
      // Call handleFtpImport and wait for completion
      await handleFtpImport();
      
      // Wait for files to become valid (using refs to get current state)
      await new Promise<void>((resolve, reject) => {
        let attempts = 0;
        const maxAttempts = 120; // 60 secondi
        const checkInterval = 500;
        
        const waitForCompletion = setInterval(() => {
          attempts++;
          const currentFiles = filesRef.current;
          
          // Check if files are now valid
          if (currentFiles.material.status === 'valid' && 
              (currentFiles.stock.status === 'valid' || currentFiles.stock.status === 'warning') &&
              (currentFiles.price.status === 'valid' || currentFiles.price.status === 'warning')) {
            clearInterval(waitForCompletion);
            resolve();
          } else if (attempts >= maxAttempts) {
            clearInterval(waitForCompletion);
            reject(new Error('Timeout: Import FTP non completato entro 60 secondi'));
          }
        }, checkInterval);
      });
      
      console.log('[Pipeline Master] Step 1 completato: file importati');
      setPipelineProgress(25);
      setPipelineStepLabel('Import FTP completato. Esecuzione EAN Prefill…');
      
      // =====================================================================
      // STEP 2: EAN Prefill (se non già completato)
      // Gestione: file mapping in memoria, mapping salvato (mappingInfo), o nessun mapping
      // =====================================================================
      const currentPrefillState = prefillStateRef.current;
      const currentFilesForPrefill = filesRef.current;
      
      // hasMaterial: coerente con l'UI (badge "Caricato")
      const hasMaterial = !!currentFilesForPrefill.material.file && 
                          (currentFilesForPrefill.material.status === 'valid' || 
                           currentFilesForPrefill.material.status === 'warning');
      
      // Debug: log per diagnosticare lo stato nella pipeline master
      console.log('[Pipeline Master] Step 2 - Material presence check:', {
        hasMaterial,
        materialFile: !!currentFilesForPrefill.material.file,
        materialStatus: currentFilesForPrefill.material.status,
        eanMappingFile: !!currentFilesForPrefill.eanMapping.file,
        mappingInfo: !!mappingInfo,
        prefillStatus: currentPrefillState.status
      });
      
      if (currentPrefillState.status === 'done') {
        // Prefill già completato in questa sessione
        console.log('[Pipeline Master] Step 2 saltato: prefill già completato');
        setPipelineStatus('EAN Prefill già completato, proseguo…');
        setPipelineProgress(50);
        setPipelineStepLabel('EAN Prefill già completato. Elaborazione pipeline EAN…');
      } else if (currentFilesForPrefill.eanMapping.file && hasMaterial) {
        // File mapping presente in memoria e Material valido, eseguo il prefill
        setPipelineStatus('EAN Prefill in corso…');
        setPipelineStepLabel('EAN Prefill in corso…');
        console.log('[Pipeline Master] Step 2: processEANPrefill (file in memoria)');
        
        try {
          await processEANPrefill();
          
          // Attendi che il prefill sia completato
          await new Promise<void>((resolve, reject) => {
            let attempts = 0;
            const maxAttempts = 120; // 60 secondi
            const checkInterval = 500;
            
            const waitForPrefill = setInterval(() => {
              attempts++;
              const currentState = prefillStateRef.current;
              
              if (currentState.status === 'done') {
                clearInterval(waitForPrefill);
                resolve();
              } else if (currentState.status === 'idle' && attempts > 10) {
                // Se torna a idle dopo diversi tentativi, il prefill è fallito
                clearInterval(waitForPrefill);
                reject(new Error('EAN Prefill fallito o interrotto'));
              } else if (attempts >= maxAttempts) {
                clearInterval(waitForPrefill);
                reject(new Error('Timeout: EAN Prefill non completato entro 60 secondi'));
              }
            }, checkInterval);
          });
          
          console.log('[Pipeline Master] Step 2 completato: prefill eseguito');
          setPipelineProgress(50);
          setPipelineStepLabel('EAN Prefill completato. Elaborazione pipeline EAN…');
        } catch (prefillError) {
          // Errore reale durante il prefill con file mapping presente: blocca la pipeline
          const errorMessage = prefillError instanceof Error ? prefillError.message : 'Errore sconosciuto';
          console.error('[Pipeline Master] Errore EAN Prefill:', errorMessage);
          throw new Error(`EAN Prefill fallito o interrotto: ${errorMessage}`);
        }
      } else if (!currentFilesForPrefill.eanMapping.file && mappingInfo) {
        // Mapping già salvato in precedenza (mappingInfo presente) ma non in memoria
        // Considera il prefill come già disponibile e prosegui
        console.log('[Pipeline Master] Step 2: mapping salvato (mappingInfo), prefill considerato già disponibile');
        setPrefillState(prev => ({ ...prev, status: 'done' }));
        setPipelineStatus('Mapping EAN già configurato, proseguo…');
        setPipelineProgress(50);
        setPipelineStepLabel('Mapping EAN già salvato. Elaborazione pipeline EAN…');
      } else {
        // Nessun mapping configurato (né in memoria né salvato)
        console.log('[Pipeline Master] Step 2 saltato: nessun mapping configurato');
        setPipelineStatus('Nessun mapping EAN, proseguo senza prefill…');
        setPipelineProgress(50);
        setPipelineStepLabel('Nessun mapping EAN configurato, prefill saltato. Elaborazione pipeline EAN…');
      }
      
      // =====================================================================
      // STEP 3: processDataPipeline('EAN')
      // =====================================================================
      setPipelineStatus('Elaborazione EAN in corso…');
      setPipelineStepLabel('Elaborazione EAN in corso…');
      console.log('[Pipeline Master] Step 3: processDataPipeline(EAN)');
      
      await processDataPipeline('EAN');
      
      // Attendi che la pipeline sia completata
      await new Promise<void>((resolve, reject) => {
        let attempts = 0;
        const maxAttempts = 240; // 120 secondi
        const checkInterval = 500;
        
        const waitForPipeline = setInterval(() => {
          attempts++;
          const currentState = processingStateRef.current;
          
          if (currentState === 'completed') {
            clearInterval(waitForPipeline);
            resolve();
          } else if (currentState === 'failed') {
            clearInterval(waitForPipeline);
            reject(new Error('Elaborazione pipeline EAN fallita'));
          } else if (attempts >= maxAttempts) {
            clearInterval(waitForPipeline);
            reject(new Error('Timeout: Elaborazione EAN non completata entro 120 secondi'));
          }
        }, checkInterval);
      });
      
      console.log('[Pipeline Master] Step 3 completato: pipeline EAN elaborata');
      setPipelineProgress(75);
      setPipelineStepLabel('Elaborazione EAN completata. Export e upload in corso…');
      
      // =====================================================================
      // STEP 4: downloadExcel('ean') che include salvataggio bucket e SFTP
      // =====================================================================
      setPipelineStatus('Export e upload in corso…');
      setPipelineStepLabel('Export e upload SFTP in corso…');
      console.log('[Pipeline Master] Step 4: downloadExcel(ean)');
      
      // Reset SFTP status before starting export
      setSftpUploadStatus({ phase: 'idle', message: '' });
      
      // downloadExcel('ean') chiama onExportEAN internamente
      downloadExcel('ean');
      
      // Attendi che l'export sia completato (monitorando sftpUploadStatus via ref)
      await new Promise<void>((resolve, reject) => {
        let attempts = 0;
        const maxAttempts = 180; // 90 secondi
        const checkInterval = 500;
        
        const waitForExport = setInterval(() => {
          attempts++;
          const currentStatus = sftpUploadStatusRef.current;
          
          if (currentStatus.phase === 'complete') {
            clearInterval(waitForExport);
            resolve();
          } else if (currentStatus.phase === 'error') {
            clearInterval(waitForExport);
            reject(new Error(currentStatus.message || 'Errore durante export/upload SFTP'));
          } else if (attempts >= maxAttempts) {
            clearInterval(waitForExport);
            reject(new Error('Timeout: Export/Upload non completato entro 90 secondi'));
          }
        }, checkInterval);
      });
      
      console.log('[Pipeline Master] Step 4 completato: export e upload SFTP eseguiti');
      
      // =====================================================================
      // SUCCESSO COMPLETO
      // =====================================================================
      setPipelineProgress(100);
      setPipelineStepLabel('Pipeline completata con successo');
      setPipelineStatus('Pipeline completata con successo!');
      toast({
        title: "Pipeline completata con successo",
        description: "Catalogo EAN ed export Mediaworld/ePrice generati e caricati su SFTP."
      });
      
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Errore sconosciuto';
      console.error('[Pipeline Master] Errore:', errorMessage);
      setPipelineStepLabel(`Errore: ${errorMessage}`);
      setPipelineStatus(`Errore: ${errorMessage}`);
      toast({
        title: "Pipeline interrotta",
        description: errorMessage,
        variant: "destructive"
      });
    } finally {
      setPipelineRunning(false);
    }
  };

  const downloadLog = (type: 'ean' | 'manufpartnr') => {
    if (currentLogEntries.length === 0 && !currentStats) return;
    
    dbg('log:write:start');

    const { timestamp, sheetName } = getTimestamp();
    const filename = `catalogo_log_${type}_${timestamp}.xlsx`;

    const logData = currentLogEntries.map(entry => ({
      'File Sorgente': entry.source_file,
      'Riga': entry.line,
      'Matnr': entry.Matnr,
      'ManufPartNr': entry.ManufPartNr,
      'EAN': entry.EAN,
      'Motivo': entry.reason,
      'Dettagli': entry.details
    }));

    const ws = XLSX.utils.json_to_sheet(logData);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, sheetName);
    XLSX.writeFile(wb, filename);

    setLogDone(true);
    dbg('log:write:done', { pipeline: type });

    toast({
      title: "Log scaricato",
      description: `File ${filename} scaricato con successo`
    });
  };

  const FileUploadCard: React.FC<{
    title: string;
    description: string;
    type: keyof FileUploadState;
    requiredHeaders: string[];
    optionalHeaders: string[];
  }> = ({ title, description, type, requiredHeaders, optionalHeaders }) => {
    const fileState = files[type];
    
    return (
      <div className="card border-strong">
        <div className="card-body">
          <div className="flex items-center justify-between mb-4">
            <h3 className="card-title">{title}</h3>
            {fileState.status === 'valid' && (
              <div className="badge-ok">
                <CheckCircle className="w-4 h-4" />
                Caricato
              </div>
            )}
            {fileState.status === 'warning' && (
              <div className="badge-ok" style={{ background: '#fff3cd', color: '#856404', border: '1px solid #ffeaa7' }}>
                <AlertCircle className="w-4 h-4" />
                Caricato (con avviso)
              </div>
            )}
            {fileState.status === 'error' && (
              <div className="badge-err">
                <XCircle className="w-4 h-4" />
                Errore
              </div>
            )}
          </div>

          <p className="text-muted text-sm mb-4">{description}</p>
          
          <div className="text-xs text-muted mb-4">
            <div><strong>Header richiesti:</strong> {requiredHeaders.join(', ')}</div>
            {optionalHeaders.length > 0 && (
              <div><strong>Header opzionali:</strong> {optionalHeaders.join(', ')}</div>
            )}
          </div>

          {!fileState.file ? (
            <div className="dropzone text-center">
              <Upload className="mx-auto h-12 w-12 icon-dark mb-4" />
              <div>
                <input
                  type="file"
                  accept=".txt,.csv"
                  onChange={(e) => {
                    const selectedFile = e.target.files?.[0];
                    if (selectedFile) {
                      handleFileUpload(selectedFile, type);
                    }
                  }}
                  className="hidden"
                  id={`file-${type}`}
                />
                <label
                  htmlFor={`file-${type}`}
                  className="btn btn-primary cursor-pointer px-6 py-3"
                >
                  Carica File
                </label>
                <p className="text-muted text-sm mt-3">
                  File CSV con delimitatore ; e encoding UTF-8
                </p>
              </div>
            </div>
          ) : (
            <div className="flex items-center justify-between p-4 bg-white rounded-lg border-strong">
              <div className="flex items-center gap-3">
                <FileText className="h-6 w-6 icon-dark" />
                <div>
                  <p className="font-medium">{(fileState.file as FileData).name}</p>
                  <p className="text-sm text-muted">
                    {(fileState.file as FileData).data.length} righe
                  </p>
                </div>
              </div>
              <button
                onClick={() => removeFile(type)}
                className="btn btn-secondary text-sm px-3 py-2"
              >
                Rimuovi
              </button>
            </div>
          )}

          {fileState.status === 'error' && fileState.error && (
            <div className="mt-4 p-3 rounded-lg border-strong" style={{ background: 'var(--error-bg)', color: 'var(--error-fg)' }}>
              <p className="text-sm font-medium">{fileState.error}</p>
            </div>
          )}

          {fileState.status === 'warning' && fileState.warning && (
            <div className="mt-4 p-3 rounded-lg border-strong" style={{ background: '#fff3cd', color: '#856404' }}>
              <p className="text-sm font-medium">{fileState.warning}</p>
            </div>
          )}

          {fileState.file && 'data' in fileState.file && (
            <div className="mt-4 p-3 rounded-lg border-strong bg-gray-50">
              <h4 className="text-sm font-medium mb-2">Diagnostica</h4>
              <div className="text-xs text-muted">
                <div><strong>Header rilevati:</strong> {(fileState.file as FileData).headers.join(', ')}</div>
                {(fileState.file as FileData).data.length > 0 && (
                  <div className="mt-1">
                    <strong>Prima riga di dati:</strong> {Object.values((fileState.file as FileData).data[0]).slice(0, 3).join(', ')}...
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    );
  };

  const allFilesValid = files.material.status === 'valid' && 
    (files.stock.status === 'valid' || files.stock.status === 'warning') && 
    (files.price.status === 'valid' || files.price.status === 'warning');

  return (
    <div className="min-h-screen p-6" style={{ background: 'var(--bg)', color: 'var(--fg)' }}>
      <div className="max-w-7xl mx-auto space-y-8">
        {/* Header */}
        <div className="relative text-center">
          <Button
            variant="outline"
            size="sm"
            onClick={logout}
            className="absolute right-0 top-0"
          >
            <LogOut className="h-4 w-4 mr-2" />
            Esci
          </Button>
          <h1 className="text-5xl font-bold mb-4">
            Alterside Catalog Generator
          </h1>
          <p className="text-muted text-xl max-w-3xl mx-auto">
            Genera due cataloghi Excel distinti (EAN e ManufPartNr) con calcoli avanzati di prezzo e commissioni
          </p>
        </div>

        {/* Sync Scheduler */}
        <SyncScheduler />

        {/* Pipeline Master Button */}
        <div className="card border-strong" style={{ background: '#1e3a5f' }}>
          <div className="card-body">
            <div className="flex flex-col md:flex-row items-center justify-between gap-4">
              <div className="text-white">
                <h3 className="text-xl font-bold mb-1">Pipeline Completa Automatica</h3>
                <p className="text-sm opacity-80">
                  Import FTP → Prefill EAN → Elaborazione → Export e Upload SFTP
                </p>
              </div>
              <div className="flex flex-col items-center gap-2">
                <Button
                  onClick={handleFullPipeline}
                  disabled={pipelineRunning || isProcessing || ftpImportLoading || isExportingEAN}
                  size="lg"
                  className="px-8 py-3 text-lg font-semibold"
                  style={{ 
                    background: pipelineRunning ? '#6b7280' : '#10b981', 
                    color: 'white',
                    opacity: (pipelineRunning || isProcessing || ftpImportLoading || isExportingEAN) ? 0.6 : 1
                  }}
                >
                  {pipelineRunning ? (
                    <>
                      <Loader2 className="mr-2 h-5 w-5 animate-spin" />
                      Pipeline in esecuzione…
                    </>
                  ) : (
                    <>
                      <Cloud className="mr-2 h-5 w-5" />
                      Esegui Pipeline Completa
                    </>
                  )}
                </Button>
                {/* Progress Bar */}
                {(pipelineRunning || pipelineProgress > 0) && (
                  <div className="w-full mt-3">
                    <div className="flex items-center justify-between mb-1">
                      <span className="text-sm text-white font-medium">{pipelineStepLabel}</span>
                      <span className="text-sm text-white font-bold">{pipelineProgress}%</span>
                    </div>
                    <div className="w-full h-3 bg-gray-300 rounded-full overflow-hidden">
                      <div 
                        className="h-full transition-all duration-300 ease-out"
                        style={{ 
                          width: `${pipelineProgress}%`,
                          background: pipelineStepLabel.includes('Errore') 
                            ? '#ef4444' 
                            : pipelineProgress === 100 
                              ? '#22c55e' 
                              : '#3b82f6'
                        }}
                      />
                    </div>
                  </div>
                )}
                {pipelineStatus && !pipelineRunning && pipelineProgress === 0 && (
                  <div className={`text-sm px-3 py-1 rounded ${
                    pipelineStatus.includes('Errore') 
                      ? 'bg-red-100 text-red-700' 
                      : pipelineStatus.includes('successo') 
                        ? 'bg-green-100 text-green-700'
                        : 'bg-blue-100 text-blue-700'
                  }`}>
                    {pipelineStatus}
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>

        {/* Instructions */}
        <div className="card border-strong">
          <div className="card-body">
            <div className="flex items-start gap-4">
              <AlertCircle className="h-6 w-6 icon-dark mt-1 flex-shrink-0" />
              <div>
                <h3 className="card-title mb-3">Specifiche di Elaborazione</h3>
                <ul className="text-sm text-muted space-y-2">
                  <li>• <strong>Filtri comuni:</strong> ExistingStock &gt; 1, prezzi numerici validi</li>
                  <li>• <strong>Export EAN:</strong> solo record con EAN non vuoto</li>
                  <li>• <strong>Export ManufPartNr:</strong> solo record con ManufPartNr non vuoto</li>
                  <li>• <strong>Prezzi:</strong> Base + spedizione (€6), IVA 22%, fee sequenziali configurabili</li>
                  <li>• <strong>Prezzo finale EAN:</strong> ending ,99; <strong>ManufPartNr:</strong> arrotondamento intero superiore</li>
                </ul>
              </div>
            </div>
          </div>
        </div>

        {/* EAN Pre-fill Section (Optional) */}
        <div className="card border-strong" style={{ background: '#f8fafc' }}>
          <div className="card-body">
            <div className="flex items-start gap-4 mb-4">
              <Info className="h-6 w-6 icon-dark mt-1 flex-shrink-0" />
              <div className="flex-1">
                <h3 className="card-title mb-2">Associare EAN da file SKU↔EAN (opzionale)</h3>
                <p className="text-sm text-muted mb-3">
                  Delimitatore <strong>;</strong> — Header richiesto: <strong>mpn;ean</strong> — Elaborato prima degli altri file
                </p>
                
                {/* Always visible mapping info */}
                <div className="mb-4 p-3 rounded-lg" style={{ background: mappingInfo ? '#e8f5e9' : '#fff3e0', color: mappingInfo ? '#2e7d32' : '#e65100' }}>
                  <p className="text-sm">
                    {mappingInfo ? (
                      <>
                        <strong>Ultimo file di associazione salvato:</strong> {mappingInfo.filename} (caricato il {new Date(mappingInfo.uploadedAt).toLocaleDateString('it-IT', { day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit' })})
                      </>
                    ) : (
                      <>
                        <strong>Nessun file di associazione salvato:</strong> il pre-fill EAN sarà saltato finché non carichi un file mapping.
                      </>
                    )}
                  </p>
                </div>
                
                {!files.eanMapping.file ? (
                  <div className="mt-4">
                    <div className="dropzone text-center p-6">
                      <Upload className="mx-auto h-10 w-10 icon-dark mb-3" />
                      <input
                        type="file"
                        accept=".txt,.csv"
                        onChange={(e) => {
                          const selectedFile = e.target.files?.[0];
                          if (selectedFile) {
                            handleEANMappingUpload(selectedFile);
                          }
                        }}
                        className="hidden"
                        id="file-ean-mapping"
                      />
                      <label
                        htmlFor="file-ean-mapping"
                        className="btn btn-primary cursor-pointer px-6 py-3"
                      >
                        Carica File Mapping
                      </label>
                      <p className="text-muted text-xs mt-3">
                        File .csv o .txt con delimitatore ; e encoding UTF-8
                      </p>
                    </div>
                  </div>
                ) : (
                  <div className="mt-4">
                    <div className="flex items-center justify-between p-4 bg-white rounded-lg border-strong mb-3">
                      <div className="flex items-center gap-3">
                        <FileText className="h-6 w-6 icon-dark" />
                        <div>
                          <p className="font-medium">{files.eanMapping.file.name}</p>
                          <p className="text-sm text-muted">
                            {files.eanMapping.status === 'ready' && 'Pronto per elaborazione'}
                            {files.eanMapping.status === 'processing' && 'Elaborazione in corso...'}
                            {files.eanMapping.status === 'completed' && 'Elaborazione completata'}
                            {files.eanMapping.status === 'error' && 'Errore elaborazione'}
                          </p>
                        </div>
                      </div>
                      <button
                        onClick={() => removeFile('eanMapping')}
                        className="btn btn-secondary text-sm px-3 py-2"
                        disabled={isProcessingPrefill}
                      >
                        Rimuovi
                      </button>
                    </div>
                    
                    {files.eanMapping.status === 'error' && files.eanMapping.error && (
                      <div className="p-3 rounded-lg border-strong mb-3" style={{ background: 'var(--error-bg)', color: 'var(--error-fg)' }}>
                        <p className="text-sm font-medium">{files.eanMapping.error}</p>
                      </div>
                    )}
                    
                    <div className="flex gap-3">
                      <button
                        onClick={processEANPrefill}
                        disabled={!files.material.file || prefillState.status === 'running' || prefillState.status === 'done' || pipelineRunning}
                        className={`btn btn-primary px-6 py-2 ${(!files.material.file || prefillState.status === 'running' || prefillState.status === 'done' || pipelineRunning) ? 'is-disabled' : ''}`}
                      >
                        {prefillState.status === 'running' ? (
                          <>
                            <Activity className="mr-2 h-4 w-4 animate-spin" />
                            Elaborazione...
                          </>
                        ) : prefillState.status === 'done' ? (
                          <>
                            <CheckCircle className="mr-2 h-4 w-4" />
                            Completato
                          </>
                        ) : (
                          'Elabora associazioni'
                        )}
                      </button>
                      
                      {prefillState.status === 'done' && (
                        <button
                          onClick={downloadEANPrefillReport}
                          className="btn btn-secondary px-6 py-2"
                        >
                          <Download className="mr-2 h-4 w-4" />
                          Scarica report
                        </button>
                      )}
                    </div>
                  </div>
                )}
                
                {prefillState.status === 'done' && prefillState.counters && (
                  <div className="mt-4 p-4 rounded-lg border-strong" style={{ background: '#e8f5e9' }}>
                    <h4 className="text-sm font-semibold mb-3" style={{ color: '#2e7d32' }}>
                      <CheckCircle className="inline h-4 w-4 mr-1" />
                      EAN pre-fill completato
                    </h4>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs">
                      <div className="p-2 bg-white rounded">
                        <div className="font-bold text-lg text-green-600">{prefillState.counters.filled_now}</div>
                        <div className="text-muted">EAN riempiti ora</div>
                      </div>
                      <div className="p-2 bg-white rounded">
                        <div className="font-bold text-lg">{prefillState.counters.already_populated}</div>
                        <div className="text-muted">Già popolati</div>
                      </div>
                      <div className="p-2 bg-white rounded">
                        <div className="font-bold text-lg text-orange-600">{prefillState.counters.skipped_due_to_conflict}</div>
                        <div className="text-muted">Conflitti</div>
                      </div>
                      <div className="p-2 bg-white rounded">
                        <div className="font-bold text-lg text-blue-600">{prefillState.counters.missing_mapping_in_new_file}</div>
                        <div className="text-muted">Senza mapping</div>
                      </div>
                      <div className="p-2 bg-white rounded">
                        <div className="font-bold text-lg text-red-600">{prefillState.counters.duplicate_mpn_rows}</div>
                        <div className="text-muted">MPN duplicati</div>
                      </div>
                      <div className="p-2 bg-white rounded">
                        <div className="font-bold text-lg text-red-600">{prefillState.counters.mpn_not_in_material}</div>
                        <div className="text-muted">MPN non in Material</div>
                      </div>
                      <div className="p-2 bg-white rounded">
                        <div className="font-bold text-lg text-yellow-600">{prefillState.counters.empty_ean_rows}</div>
                        <div className="text-muted">EAN vuoti nel mapping</div>
                      </div>
                      <div className="p-2 bg-white rounded">
                        <div className="font-bold text-lg text-gray-600">{prefillState.counters.errori_formali}</div>
                        <div className="text-muted">Errori formali</div>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>

        {/* Override Prodotti Section (Optional) */}
        <div className="card border-strong" style={{ background: '#fef3c7' }}>
          <div className="card-body">
            <div className="flex items-start gap-4 mb-4">
              <Info className="h-6 w-6 icon-dark mt-1 flex-shrink-0" />
              <div className="flex-1">
                <h3 className="card-title mb-2">Override Prodotti (opzionale)</h3>
                <p className="text-sm text-muted mb-3">
                  File XLSX con colonne: <strong>SKU, EAN, Quantity, ListPrice, OfferPrice</strong> — Sovrascrive prezzi e quantità nel Catalogo EAN
                </p>
                
                {!overrideState.file ? (
                  <div className="mt-4">
                    <div className="dropzone text-center p-6">
                      <Upload className="mx-auto h-10 w-10 icon-dark mb-3" />
                      <input
                        type="file"
                        accept=".xlsx,.xls"
                        onChange={(e) => {
                          const selectedFile = e.target.files?.[0];
                          if (selectedFile) {
                            handleOverrideFileUpload(selectedFile);
                          }
                        }}
                        className="hidden"
                        id="file-override"
                      />
                      <label
                        htmlFor="file-override"
                        className="btn btn-primary cursor-pointer px-6 py-3"
                      >
                        Carica File Override
                      </label>
                      <p className="text-muted text-xs mt-3">
                        File Excel .xlsx con le 5 colonne richieste
                      </p>
                    </div>
                  </div>
                ) : (
                  <div className="mt-4">
                    <div className="flex items-center justify-between p-4 bg-white rounded-lg border-strong mb-3">
                      <div className="flex items-center gap-3">
                        <FileText className="h-6 w-6 icon-dark" />
                        <div>
                          <p className="font-medium">{overrideState.filename}</p>
                          <p className="text-sm text-muted">
                            {overrideState.uploadedAt && `Caricato: ${overrideState.uploadedAt.toLocaleString('it-IT')}`}
                            {' • '}
                            {overrideState.validCount} righe valide, {overrideState.invalidCount} scartate
                            {overrideState.disabled && <span className="text-orange-600 font-medium"> • DISABILITATO</span>}
                          </p>
                        </div>
                      </div>
                      <div className="flex gap-2">
                        {overrideState.disabled ? (
                          <button
                            onClick={handleEnableOverride}
                            className="btn btn-primary text-sm px-3 py-2"
                          >
                            Riabilita
                          </button>
                        ) : (
                          <button
                            onClick={handleDisableOverride}
                            className="btn btn-secondary text-sm px-3 py-2"
                          >
                            Disabilita
                          </button>
                        )}
                        <button
                          onClick={handleRemoveOverrideFile}
                          className="btn btn-secondary text-sm px-3 py-2"
                          style={{ color: '#dc2626' }}
                        >
                          Elimina
                        </button>
                      </div>
                    </div>
                    
                    {/* Override Errors */}
                    {overrideState.errors.length > 0 && (
                      <div className="p-3 rounded-lg border-strong mb-3" style={{ background: '#fef2f2' }}>
                        <h4 className="text-sm font-semibold mb-2" style={{ color: '#dc2626' }}>
                          Errori/Warning ({overrideState.errors.length})
                        </h4>
                        <div className="max-h-32 overflow-y-auto text-xs space-y-1">
                          {overrideState.errors.slice(0, 20).map((err, idx) => (
                            <div key={idx} className="text-red-700">
                              Riga {err.rowIndex}: {err.field} - {err.reason}
                            </div>
                          ))}
                          {overrideState.errors.length > 20 && (
                            <div className="text-muted">...e altri {overrideState.errors.length - 20} errori</div>
                          )}
                        </div>
                      </div>
                    )}
                    
                    {/* Override Apply Stats */}
                    {overrideState.lastApplyStats && (
                      <div className="p-4 rounded-lg border-strong" style={{ background: '#ecfdf5' }}>
                        <h4 className="text-sm font-semibold mb-3" style={{ color: '#059669' }}>
                          <CheckCircle className="inline h-4 w-4 mr-1" />
                          Override applicato nell'ultima generazione
                        </h4>
                        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-xs">
                          <div className="p-2 bg-white rounded">
                            <div className="font-bold text-lg text-green-600">{overrideState.lastApplyStats.updatedExisting}</div>
                            <div className="text-muted">Prodotti aggiornati</div>
                          </div>
                          <div className="p-2 bg-white rounded">
                            <div className="font-bold text-lg text-blue-600">{overrideState.lastApplyStats.addedNew}</div>
                            <div className="text-muted">Nuovi prodotti</div>
                          </div>
                          <div className="p-2 bg-white rounded">
                            <div className="font-bold text-lg text-orange-600">{overrideState.lastApplyStats.skippedRows}</div>
                            <div className="text-muted">Righe scartate</div>
                          </div>
                          <div className="p-2 bg-white rounded">
                            <div className="font-bold text-lg text-yellow-600">{overrideState.lastApplyStats.warnings}</div>
                            <div className="text-muted">Warning</div>
                          </div>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
        <div className="flex justify-end mb-4">
          <Button
            variant="outline"
            onClick={handleFtpImport}
            disabled={ftpImportLoading || isProcessing || pipelineRunning}
          >
            {ftpImportLoading ? (
              <>
                <Activity className="mr-2 h-4 w-4 animate-spin" />
                Import da FTP in corso…
              </>
            ) : (
              <>
                <Download className="mr-2 h-4 w-4" />
                Importa automaticamente da FTP
              </>
            )}
          </Button>
        </div>

        {/* File Upload Cards */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <FileUploadCard
            title="Material File"
            description="File principale con informazioni prodotto"
            type="material"
            requiredHeaders={REQUIRED_HEADERS.material}
            optionalHeaders={OPTIONAL_HEADERS.material}
          />
          <FileUploadCard
            title="Stock File Data"
            description="Dati scorte e disponibilità"
            type="stock"
            requiredHeaders={REQUIRED_HEADERS.stock}
            optionalHeaders={OPTIONAL_HEADERS.stock}
          />
          <FileUploadCard
            title="Price File Data"
            description="Listini prezzi e scontistiche"
            type="price"
            requiredHeaders={REQUIRED_HEADERS.price}
            optionalHeaders={OPTIONAL_HEADERS.price}
          />
        </div>

        {/* Fee Configuration */}
        {allFilesValid && (
          <div className="card border-strong">
            <div className="card-body">
              <h3 className="card-title mb-6 flex items-center gap-2">
                <Info className="h-5 w-5 icon-dark" />
                Regole di Calcolo
              </h3>
              
              <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <div className="space-y-2">
                  <Label htmlFor="fee-derev" className="text-sm font-medium">
                    Fee DeRev (moltiplicatore)
                  </Label>
                  <Input
                    id="fee-derev"
                    type="number"
                    min="1.00"
                    max="2.00"
                    step="0.01"
                    value={feeConfig.feeDrev}
                    onChange={(e) => {
                      const val = parseFloat(e.target.value);
                      if (val >= 1.00 && val <= 2.00) {
                        setFeeConfig(prev => ({ ...prev, feeDrev: val }));
                      }
                    }}
                    className="text-center"
                    title="Inserisci fee come moltiplicatore: 1,05 = +5%, 1,08 = +8%. Le fee sono applicate in sequenza dopo IVA e spedizione."
                  />
                  <p className="text-xs text-muted-foreground">
                    Esempio: 1,05 = +5% commissione DeRev
                  </p>
                </div>
                
                <div className="space-y-2">
                  <Label htmlFor="fee-marketplace" className="text-sm font-medium">
                    Fee Marketplace (moltiplicatore)
                  </Label>
                  <Input
                    id="fee-marketplace"
                    type="number"
                    min="1.00"
                    max="2.00"
                    step="0.01"
                    value={feeConfig.feeMkt}
                    onChange={(e) => {
                      const val = parseFloat(e.target.value);
                      if (val >= 1.00 && val <= 2.00) {
                        setFeeConfig(prev => ({ ...prev, feeMkt: val }));
                      }
                    }}
                    className="text-center"
                    title="Fee marketplace applicata dopo Fee DeRev. Esempio: 1,08 = +8% commissione marketplace."
                  />
                  <p className="text-xs text-muted-foreground">
                    Esempio: 1,08 = +8% commissione marketplace
                  </p>
                </div>

                <div className="space-y-2">
                  <Label htmlFor="shipping-cost" className="text-sm font-medium">
                    Costo di spedizione fisso (€)
                  </Label>
                  <Input
                    id="shipping-cost"
                    type="number"
                    min="0"
                    step="0.01"
                    value={feeConfig.shippingCost}
                    onChange={(e) => {
                      const val = parseFloat(e.target.value);
                      if (!isNaN(val) && val >= 0) {
                        setFeeConfig(prev => ({ ...prev, shippingCost: val }));
                      }
                    }}
                    className="text-center"
                    title="Costo di spedizione fisso in euro. Esempio: 6,00 = spedizione di 6 euro per ogni prodotto."
                  />
                  <p className="text-xs text-muted-foreground">
                    Esempio: 6,00 = spedizione di 6 euro
                  </p>
                </div>
              </div>
              
              <div className="mt-4">
                <Button
                  onClick={handleSaveFees}
                  disabled={isLoadingFees || isSavingFees}
                  className="w-full"
                >
                  {isSavingFees ? (
                    <>
                      <Activity className="mr-2 h-4 w-4 animate-spin" />
                      Salvataggio...
                    </>
                  ) : (
                    'Salva regole di calcolo'
                  )}
                </Button>
              </div>
            </div>
          </div>
        )}

        {/* Action Buttons */}
        {allFilesValid && (
          <div className="text-center">
            <h3 className="text-2xl font-bold mb-6">Azioni</h3>
            <div className="flex flex-wrap justify-center gap-6">
              <button
                onClick={() => processDataPipeline('EAN')}
                disabled={!canProcess || isProcessing || pipelineRunning}
                className={`btn btn-primary text-lg px-12 py-4 ${!canProcess || isProcessing || pipelineRunning ? 'is-disabled' : ''}`}
              >
                {isProcessing && currentPipeline === 'EAN' ? (
                  <>
                    <Activity className="mr-3 h-5 w-5 animate-spin" />
                    Elaborazione EAN...
                  </>
                ) : (
                  <>
                    <Upload className="mr-3 h-5 w-5" />
                    GENERA EXCEL (EAN)
                  </>
                )}
              </button>
              <button
                onClick={() => processDataPipeline('MPN')}
                disabled={!canProcess || isProcessing || pipelineRunning}
                className={`btn btn-primary text-lg px-12 py-4 ${!canProcess || isProcessing || pipelineRunning ? 'is-disabled' : ''}`}
              >
                {isProcessing && currentPipeline === 'MPN' ? (
                  <>
                    <Activity className="mr-3 h-5 w-5 animate-spin" />
                    Elaborazione ManufPartNr...
                  </>
                ) : (
                  <>
                    <Upload className="mr-3 h-5 w-5" />
                    GENERA EXCEL (ManufPartNr)
                  </>
                )}
              </button>
            </div>
          </div>
        )}

        {/* Progress Section */}
        {(isProcessing || isCompleted) && (
          <div className="card border-strong">
            <div className="card-body">
              <h3 className="card-title mb-6 flex items-center gap-2">
                <Activity className="h-5 w-5 animate-spin icon-dark" />
                Progresso Elaborazione
              </h3>
              
              <div className="space-y-4">
                <div className="text-sm text-muted mb-2">
                  <strong>Stato corrente:</strong> {processingState}
                </div>
                
                <div className="progress">
                  <span style={{ width: `${progressPct}%` }} />
                </div>
                
                <div className="flex justify-between text-sm">
                  <span className="font-medium">{progressPct}%</span>
                  <span className="font-bold">Completato</span>
                </div>
                
                <div className="flex items-center gap-6 text-sm">
                  <div className="flex items-center gap-2">
                    <Clock className="h-4 w-4 icon-dark" />
                    <span>Trascorso: {formatTime(elapsedTime)}</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <Clock className="h-4 w-4 icon-dark" />
                    <span>ETA: {estimatedTime ? formatTime(estimatedTime) : (processed > 0 && !isCompleted ? 'calcolo…' : '—')}</span>
                  </div>
                  <div className="text-sm">
                    <span className="font-medium">Righe elaborate: {processed.toLocaleString()}</span>
                    {(finalTotal ?? total) > 0 && <span className="text-muted"> / {(finalTotal ?? total).toLocaleString()}</span>}
                  </div>
                  {finalTotal !== null && total !== finalTotal && (
                    <div className="text-xs text-muted">
                      Stima iniziale: {total.toLocaleString()} | Totale effettivo: {finalTotal.toLocaleString()}
                    </div>
                  )}
                </div>
                
                {/* Debug State */}
                <div className="mt-4 p-3 bg-muted rounded-lg">
                  <h4 className="text-sm font-medium mb-2">Stato Debug</h4>
                  <div className="grid grid-cols-2 gap-2 text-xs">
                    <div>materialValid: {debugState.materialValid ? '✓' : '✗'}</div>
                    <div>stockValid: {debugState.stockValid ? '✓' : '✗'}</div>
                    <div>priceValid: {debugState.priceValid ? '✓' : '✗'}</div>
                    <div>stockReady: {debugState.stockReady ? '✓' : '✗'}</div>
                    <div>priceReady: {debugState.priceReady ? '✓' : '✗'}</div>
                    <div>materialPreScanDone: {debugState.materialPreScanDone ? '✓' : '✗'}</div>
                    <div>joinStarted: {debugState.joinStarted ? '✓' : '✗'}</div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Debug Events Panel */}
        {(isProcessing || isCompleted || debugEvents.length > 0) && (
          <div className="card border-strong">
            <div className="card-body">
              <h3 className="card-title mb-4 flex items-center gap-2">
                <AlertCircle className="h-5 w-5 icon-dark" />
                Eventi Debug
              </h3>
              
              <textarea
                value={debugEvents.join('\n')}
                readOnly
                className="w-full h-64 p-3 font-mono text-xs bg-muted border border-strong rounded-lg resize-none"
                style={{ whiteSpace: 'pre-wrap' }}
              />
              
              {debugEvents.length > 0 && (
                <div className="mt-2 flex justify-between text-xs text-muted">
                  <span>{debugEvents.length} eventi registrati</span>
                  <button
                    onClick={() => setDebugEvents([])}
                    className="text-primary hover:text-primary-dark"
                  >
                    Pulisci log
                  </button>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Statistics */}
        {currentStats && (
          <div className="card border-strong">
            <div className="card-body">
              <h3 className="card-title mb-6">Statistiche Elaborazione - Pipeline {currentPipeline}</h3>
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
                <div className="text-center p-4 rounded-lg border-strong" style={{ background: '#f8fafc' }}>
                  <div className="text-2xl font-bold">{currentStats.totalRecords.toLocaleString()}</div>
                  <div className="text-sm text-muted">Righe Totali</div>
                </div>
                <div className="text-center p-4 rounded-lg border-strong" style={{ background: 'var(--success-bg)' }}>
                  <div className="text-2xl font-bold" style={{ color: 'var(--success-fg)' }}>
                    {currentPipeline === 'EAN' ? currentStats.validRecordsEAN.toLocaleString() : currentStats.validRecordsManufPartNr.toLocaleString()}
                  </div>
                  <div className="text-sm text-muted">Valide {currentPipeline}</div>
                </div>
                <div className="text-center p-4 rounded-lg border-strong" style={{ background: 'var(--error-bg)' }}>
                  <div className="text-2xl font-bold" style={{ color: 'var(--error-fg)' }}>
                    {currentPipeline === 'EAN' ? currentStats.filteredRecordsEAN.toLocaleString() : currentStats.filteredRecordsManufPartNr.toLocaleString()}
                  </div>
                  <div className="text-sm text-muted">Scartate {currentPipeline}</div>
                </div>
                <div className="text-center p-4 rounded-lg border-strong" style={{ background: '#fff3cd' }}>
                  <div className="text-2xl font-bold" style={{ color: '#856404' }}>{currentStats.stockDuplicates + currentStats.priceDuplicates}</div>
                  <div className="text-sm text-muted">Duplicati</div>
                </div>
              </div>
              
              {eanStats && currentPipeline === 'EAN' && (
                <div className="mt-6">
                  <h4 className="text-lg font-semibold mb-3">Validazione EAN</h4>
                  <div className="grid grid-cols-4 gap-4 text-sm">
                    <div className="text-center p-3 rounded-lg border" style={{ background: '#f0f9ff' }}>
                      <div className="text-lg font-bold text-green-600">{eanStats.ean_validi_13}</div>
                      <div className="text-muted-foreground">EAN validi (13 cifre)</div>
                    </div>
                    <div className="text-center p-3 rounded-lg border" style={{ background: '#eff6ff' }}>
                      <div className="text-lg font-bold text-blue-600">{eanStats.ean_padded_12_to_13}</div>
                      <div className="text-muted-foreground">EAN padded (12→13)</div>
                    </div>
                    <div className="text-center p-3 rounded-lg border" style={{ background: '#f0fdf4' }}>
                      <div className="text-lg font-bold text-emerald-600">{eanStats.ean_trimmed_14_to_13}</div>
                      <div className="text-muted-foreground">EAN trimmed (14→13)</div>
                    </div>
                    <div className="text-center p-3 rounded-lg border" style={{ background: '#fefce8' }}>
                      <div className="text-lg font-bold text-yellow-600">{eanStats.ean_validi_14}</div>
                      <div className="text-muted-foreground">EAN validi (14 cifre)</div>
                    </div>
                    <div className="text-center p-3 rounded-lg border" style={{ background: '#fff7ed' }}>
                      <div className="text-lg font-bold text-orange-600">{eanStats.ean_duplicati_risolti}</div>
                      <div className="text-muted-foreground">Duplicati risolti</div>
                    </div>
                    <div className="text-center p-3 rounded-lg border" style={{ background: '#fef2f2' }}>
                      <div className="text-lg font-bold text-red-600">{eanStats.ean_mancanti}</div>
                      <div className="text-muted-foreground">EAN mancanti</div>
                    </div>
                    <div className="text-center p-3 rounded-lg border" style={{ background: '#fef2f2' }}>
                      <div className="text-lg font-bold text-red-600">{eanStats.ean_non_numerici}</div>
                      <div className="text-muted-foreground">EAN non numerici</div>
                    </div>
                    <div className="text-center p-3 rounded-lg border" style={{ background: '#fef2f2' }}>
                      <div className="text-lg font-bold text-red-600">{eanStats.ean_lunghezze_invalid}</div>
                      <div className="text-muted-foreground">Lunghezze non valide</div>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Download Buttons */}
        {isCompleted && currentProcessedData.length > 0 && (
          <div className="text-center">
            <h3 className="text-2xl font-bold mb-6">Download Pipeline {currentPipeline}</h3>
            <div className="flex flex-wrap justify-center gap-4">
              <button 
                type="button"
                onClick={currentPipeline === 'EAN' ? onExportEAN : () => downloadExcel('manufpartnr')} 
                disabled={(isExportingEAN && currentPipeline === 'EAN') || pipelineRunning}
                className={`btn btn-primary text-lg px-8 py-3 ${(isExportingEAN && currentPipeline === 'EAN') || pipelineRunning ? 'opacity-50 cursor-not-allowed' : ''}`}
              >
                <Download className="mr-3 h-5 w-5" />
                {isExportingEAN && currentPipeline === 'EAN' ? 'ESPORTAZIONE...' : `SCARICA EXCEL (${currentPipeline})`}
              </button>
              <button 
                onClick={() => downloadLog(currentPipeline === 'EAN' ? 'ean' : 'manufpartnr')} 
                className="btn btn-secondary text-lg px-8 py-3"
              >
                <Download className="mr-3 h-5 w-5" />
                SCARICA LOG ({currentPipeline})
              </button>
              {discardedRows.length > 0 && currentPipeline === 'EAN' && (
                <button 
                  onClick={downloadDiscardedRows}
                  className="btn btn-secondary text-lg px-8 py-3"
                >
                  <Download className="mr-3 h-5 w-5" />
                  SCARTI EAN ({discardedRows.length})
                </button>
              )}
            </div>
            
            {/* ePrice Export Section - Only for EAN pipeline */}
            {currentPipeline === 'EAN' && (
              <div className="mt-8 p-6 rounded-lg border" style={{ background: '#f0f9ff' }}>
                <h4 className="text-lg font-semibold mb-4 text-blue-800">Esporta Catalogo ePrice</h4>
                <div className="flex flex-wrap items-center justify-center gap-4">
                  <div className="flex items-center gap-2">
                    <Label htmlFor="prepDays" className="text-sm font-medium whitespace-nowrap">
                      Giorni di preparazione dell'ordine:
                    </Label>
                    <Input
                      id="prepDays"
                      type="number"
                      min={1}
                      max={30}
                      value={prepDays}
                      onChange={(e) => {
                        const val = parseInt(e.target.value, 10);
                        if (!isNaN(val) && val >= 1 && val <= 30) {
                          setPrepDays(val);
                        }
                      }}
                      className="w-20 text-center"
                    />
                  </div>
                  <button 
                    type="button"
                    onClick={onExportEprice}
                    disabled={isExportingEprice || pipelineRunning}
                    className={`btn btn-primary text-lg px-8 py-3 ${isExportingEprice || pipelineRunning ? 'opacity-50 cursor-not-allowed' : ''}`}
                    style={{ background: '#0369a1' }}
                  >
                    <Download className="mr-3 h-5 w-5" />
                    {isExportingEprice ? 'ESPORTAZIONE...' : 'Esporta catalogo ePrice'}
                  </button>
                </div>
                <p className="text-xs text-muted-foreground mt-3">
                  Genera il file "Tracciato_Pubblicazione_Offerte_new.xlsx" per ePrice
                </p>
              </div>
            )}
            
            {/* Mediaworld Export Section - Only for EAN pipeline */}
            {currentPipeline === 'EAN' && (
              <div className="mt-8 p-6 rounded-lg border" style={{ background: '#fef3c7' }}>
                <h4 className="text-lg font-semibold mb-4 text-amber-800">Esporta Catalogo Mediaworld</h4>
                <div className="flex flex-wrap items-center justify-center gap-4">
                  <div className="flex items-center gap-2">
                    <Label htmlFor="prepDaysMediaworld" className="text-sm font-medium whitespace-nowrap">
                      Tempo di preparazione spedizione (giorni):
                    </Label>
                    <Input
                      id="prepDaysMediaworld"
                      type="number"
                      min={1}
                      max={45}
                      value={prepDaysMediaworld}
                      onChange={(e) => {
                        const val = parseInt(e.target.value, 10);
                        if (!isNaN(val) && val >= 1 && val <= 45) {
                          setPrepDaysMediaworld(val);
                        }
                      }}
                      className="w-20 text-center"
                    />
                  </div>
                  <button 
                    type="button"
                    onClick={onExportMediaworld}
                    disabled={isExportingMediaworld || pipelineRunning}
                    className={`btn btn-primary text-lg px-8 py-3 ${isExportingMediaworld || pipelineRunning ? 'opacity-50 cursor-not-allowed' : ''}`}
                    style={{ background: '#d97706' }}
                  >
                    <Download className="mr-3 h-5 w-5" />
                    {isExportingMediaworld ? 'ESPORTAZIONE...' : 'Genera catalogo Mediaworld (.xlsx)'}
                  </button>
                </div>
                <p className="text-xs text-muted-foreground mt-3">
                  Genera il file "mediaworld-offers-YYYYMMDD.xlsx" con formato Mediaworld
                </p>
              </div>
            )}

            {/* Salva impostazioni giorni di preparazione - Only for EAN pipeline */}
            {currentPipeline === 'EAN' && (
              <div className="mt-4 flex justify-center">
                <button
                  type="button"
                  onClick={handleSavePrepDays}
                  disabled={isSavingPrepDays}
                  className={`btn btn-secondary text-sm px-6 py-2 ${isSavingPrepDays ? 'opacity-50 cursor-not-allowed' : ''}`}
                  style={{ background: '#6b7280', color: 'white' }}
                >
                  {isSavingPrepDays ? (
                    <>
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                      Salvataggio...
                    </>
                  ) : (
                    'Salva impostazioni giorni di preparazione'
                  )}
                </button>
              </div>
            )}

            {/* SFTP Upload Status - Only for EAN pipeline */}
            {currentPipeline === 'EAN' && sftpUploadStatus.phase !== 'idle' && (
              <div className="mt-8 p-6 rounded-lg border-2" style={{ 
                background: sftpUploadStatus.phase === 'complete' ? '#f0fdf4' : 
                            sftpUploadStatus.phase === 'error' ? '#fef2f2' : '#f0f9ff', 
                borderColor: sftpUploadStatus.phase === 'complete' ? '#22c55e' : 
                             sftpUploadStatus.phase === 'error' ? '#ef4444' : '#3b82f6'
              }}>
                <h4 className={`text-lg font-semibold mb-4 flex items-center gap-2 ${
                  sftpUploadStatus.phase === 'complete' ? 'text-green-800' :
                  sftpUploadStatus.phase === 'error' ? 'text-red-800' : 'text-blue-800'
                }`}>
                  {sftpUploadStatus.phase === 'complete' && <CheckCircle className="h-5 w-5" />}
                  {sftpUploadStatus.phase === 'error' && <XCircle className="h-5 w-5" />}
                  {['saving', 'uploading'].includes(sftpUploadStatus.phase) && <Loader2 className="h-5 w-5 animate-spin" />}
                  <Cloud className="h-5 w-5" />
                  Stato Upload Bucket/SFTP
                </h4>
                
                <div className="flex items-center gap-2 mb-2">
                  <span className={`font-medium ${
                    sftpUploadStatus.phase === 'complete' ? 'text-green-700' :
                    sftpUploadStatus.phase === 'error' ? 'text-red-700' :
                    'text-blue-700'
                  }`}>
                    {sftpUploadStatus.message}
                  </span>
                </div>
                
                {sftpUploadStatus.results && (
                  <div className="mt-2 space-y-1">
                    {sftpUploadStatus.results.map((result, idx) => (
                      <div key={idx} className="flex items-center gap-2 text-sm">
                        {result.uploaded ? (
                          <CheckCircle className="h-4 w-4 text-green-600" />
                        ) : (
                          <XCircle className="h-4 w-4 text-red-600" />
                        )}
                        <span>{result.filename}</span>
                        {result.error && <span className="text-red-600 text-xs">({result.error})</span>}
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {/* Validation Summary Section */}
        {(lastEpriceValidation || lastMediaworldValidation) && currentPipeline === 'EAN' && (
          <div className="card border-strong">
            <div className="card-body">
              <h3 className="card-title mb-6 flex items-center gap-2">
                <FileText className="h-5 w-5" />
                Riepilogo Validazione Export
              </h3>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                {/* ePrice Validation Summary */}
                {lastEpriceValidation && (
                  <div className={`p-4 rounded-lg border-2 ${lastEpriceValidation.success ? 'border-green-500 bg-green-50' : 'border-red-500 bg-red-50'}`}>
                    <div className="flex items-center justify-between mb-3">
                      <h4 className="font-semibold text-lg flex items-center gap-2">
                        {lastEpriceValidation.success ? (
                          <CheckCircle className="h-5 w-5 text-green-600" />
                        ) : (
                          <XCircle className="h-5 w-5 text-red-600" />
                        )}
                        ePrice
                      </h4>
                      <span className="text-xs text-muted-foreground">
                        {lastEpriceValidation.timestamp.toLocaleTimeString('it-IT')}
                      </span>
                    </div>
                    
                    <div className="grid grid-cols-3 gap-3 mb-3">
                      <div className="text-center p-2 bg-white rounded border">
                        <div className="text-lg font-bold text-blue-600">{lastEpriceValidation.totalRows}</div>
                        <div className="text-xs text-muted-foreground">Totali</div>
                      </div>
                      <div className="text-center p-2 bg-white rounded border">
                        <div className="text-lg font-bold text-green-600">{lastEpriceValidation.validRows}</div>
                        <div className="text-xs text-muted-foreground">Valide</div>
                      </div>
                      <div className="text-center p-2 bg-white rounded border">
                        <div className="text-lg font-bold text-red-600">{lastEpriceValidation.skippedRows}</div>
                        <div className="text-xs text-muted-foreground">Scartate</div>
                      </div>
                    </div>
                    
                    {lastEpriceValidation.structureErrors.length > 0 && (
                      <div className="mb-2">
                        <div className="text-sm font-medium text-red-700 mb-1">Errori struttura:</div>
                        <ul className="text-xs text-red-600 list-disc list-inside max-h-24 overflow-y-auto">
                          {lastEpriceValidation.structureErrors.slice(0, 5).map((err, i) => (
                            <li key={i}>{err}</li>
                          ))}
                          {lastEpriceValidation.structureErrors.length > 5 && (
                            <li className="text-muted-foreground">...e altri {lastEpriceValidation.structureErrors.length - 5} errori</li>
                          )}
                        </ul>
                      </div>
                    )}
                    
                    {lastEpriceValidation.errors.length > 0 && (
                      <div className="mb-2">
                        <div className="text-sm font-medium text-orange-700 mb-1">Righe scartate:</div>
                        <ul className="text-xs text-orange-600 list-disc list-inside max-h-24 overflow-y-auto">
                          {lastEpriceValidation.errors.slice(0, 5).map((err, i) => (
                            <li key={i}>{err.reason}</li>
                          ))}
                          {lastEpriceValidation.errors.length > 5 && (
                            <li className="text-muted-foreground">...e altri {lastEpriceValidation.errors.length - 5} problemi</li>
                          )}
                        </ul>
                      </div>
                    )}
                    
                    {lastEpriceValidation.warnings.length > 0 && (
                      <div>
                        <div className="text-sm font-medium text-yellow-700 mb-1">Avvisi:</div>
                        <ul className="text-xs text-yellow-600 list-disc list-inside max-h-20 overflow-y-auto">
                          {lastEpriceValidation.warnings.slice(0, 3).map((warn, i) => (
                            <li key={i}>{warn}</li>
                          ))}
                          {lastEpriceValidation.warnings.length > 3 && (
                            <li className="text-muted-foreground">...e altri {lastEpriceValidation.warnings.length - 3} avvisi</li>
                          )}
                        </ul>
                      </div>
                    )}
                  </div>
                )}
                
                {/* Mediaworld Validation Summary */}
                {lastMediaworldValidation && (
                  <div className={`p-4 rounded-lg border-2 ${lastMediaworldValidation.success ? 'border-green-500 bg-green-50' : 'border-red-500 bg-red-50'}`}>
                    <div className="flex items-center justify-between mb-3">
                      <h4 className="font-semibold text-lg flex items-center gap-2">
                        {lastMediaworldValidation.success ? (
                          <CheckCircle className="h-5 w-5 text-green-600" />
                        ) : (
                          <XCircle className="h-5 w-5 text-red-600" />
                        )}
                        Mediaworld
                      </h4>
                      <span className="text-xs text-muted-foreground">
                        {lastMediaworldValidation.timestamp.toLocaleTimeString('it-IT')}
                      </span>
                    </div>
                    
                    <div className="grid grid-cols-3 gap-3 mb-3">
                      <div className="text-center p-2 bg-white rounded border">
                        <div className="text-lg font-bold text-blue-600">{lastMediaworldValidation.totalRows}</div>
                        <div className="text-xs text-muted-foreground">Totali</div>
                      </div>
                      <div className="text-center p-2 bg-white rounded border">
                        <div className="text-lg font-bold text-green-600">{lastMediaworldValidation.validRows}</div>
                        <div className="text-xs text-muted-foreground">Valide</div>
                      </div>
                      <div className="text-center p-2 bg-white rounded border">
                        <div className="text-lg font-bold text-red-600">{lastMediaworldValidation.skippedRows}</div>
                        <div className="text-xs text-muted-foreground">Scartate</div>
                      </div>
                    </div>
                    
                    {lastMediaworldValidation.errors.length > 0 && (
                      <div className="mb-2">
                        <div className="text-sm font-medium text-red-700 mb-1">Errori validazione ({lastMediaworldValidation.errors.length}):</div>
                        <ul className="text-xs text-red-600 list-disc list-inside max-h-32 overflow-y-auto">
                          {lastMediaworldValidation.errors.slice(0, 8).map((err, i) => (
                            <li key={i}>
                              Riga {err.row}: <strong>{err.field}</strong> - {err.reason}
                              {err.sku && <span className="text-muted-foreground"> (SKU: {err.sku})</span>}
                            </li>
                          ))}
                          {lastMediaworldValidation.errors.length > 8 && (
                            <li className="text-muted-foreground font-medium">
                              ...e altri {lastMediaworldValidation.errors.length - 8} errori
                            </li>
                          )}
                        </ul>
                      </div>
                    )}
                    
                    {lastMediaworldValidation.success && lastMediaworldValidation.errors.length === 0 && (
                      <div className="text-sm text-green-700 flex items-center gap-2">
                        <CheckCircle className="h-4 w-4" />
                        Tutte le righe sono conformi al template ufficiale
                      </div>
                    )}
                  </div>
                )}
              </div>
              
              {/* Clear validation button */}
              <div className="mt-4 flex justify-end">
                <button
                  type="button"
                  onClick={() => {
                    setLastEpriceValidation(null);
                    setLastMediaworldValidation(null);
                  }}
                  className="text-sm text-muted-foreground hover:text-foreground"
                >
                  Pulisci riepilogo
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Data Preview */}
        {currentProcessedData.length > 0 && (
          <div className="card border-strong">
            <div className="card-body">
              <h3 className="card-title mb-6">Anteprima Export {currentPipeline} (Prime 10 Righe)</h3>
              <div className="overflow-x-auto">
                <table className="table-zebra">
                  <thead>
                    <tr>
                      {Object.keys(currentProcessedData[0]).map((header, index) => (
                        <th key={index}>{header}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {currentProcessedData.slice(0, 10).map((row, rowIndex) => (
                      <tr key={rowIndex}>
                        {Object.values(row).map((value, colIndex) => (
                          <td key={colIndex}>
                            {typeof value === 'number' ? value.toLocaleString('it-IT') : String(value)}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default AltersideCatalogGenerator;