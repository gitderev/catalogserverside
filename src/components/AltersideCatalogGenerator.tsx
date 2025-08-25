import React, { useState, useRef } from 'react';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { toast } from '@/hooks/use-toast';
import { Upload, Download, FileText, CheckCircle, XCircle, AlertCircle, Clock, Activity } from 'lucide-react';
import * as XLSX from 'xlsx';
import Papa from 'papaparse';

interface FileData {
  name: string;
  data: any[];
  headers: string[];
}

interface FileUploadState {
  material: { file: FileData | null; status: 'none' | 'valid' | 'error'; error?: string };
  stock: { file: FileData | null; status: 'none' | 'valid' | 'error'; error?: string };
  price: { file: FileData | null; status: 'none' | 'valid' | 'error'; error?: string };
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
  'Costo di spedizione': number;
  'Fee Mediaworld': string;
  'Fee Alterside': string;
  'Prezzo finale': number;
  'Prezzo finale Listino': number;
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
  stock: ['Matnr', 'ManufPartNr', 'ExistingStock'],
  price: ['Matnr', 'ManufPartNr', 'ListPrice', 'CustBestPrice']
};

const AltersideCatalogGenerator: React.FC = () => {
  const [files, setFiles] = useState<FileUploadState>({
    material: { file: null, status: 'none' },
    stock: { file: null, status: 'none' },
    price: { file: null, status: 'none' }
  });

  const [processing, setProcessing] = useState(false);
  const [progress, setProgress] = useState({ stage: '', progress: 0, recordsProcessed: 0, totalRecords: 0 });
  const [processingTime, setProcessingTime] = useState({ started: 0, elapsed: 0, estimated: 0 });
  const [processedDataEAN, setProcessedDataEAN] = useState<ProcessedRecord[]>([]);
  const [processedDataManufPartNr, setProcessedDataManufPartNr] = useState<ProcessedRecord[]>([]);
  const [logEntriesEAN, setLogEntriesEAN] = useState<LogEntry[]>([]);
  const [logEntriesManufPartNr, setLogEntriesManufPartNr] = useState<LogEntry[]>([]);
  const [stats, setStats] = useState<ProcessingStats | null>(null);

  const workerRef = useRef<Worker | null>(null);
  const timeInterval = useRef<NodeJS.Timeout | null>(null);

  const validateHeaders = (headers: string[], requiredHeaders: string[]): { valid: boolean; missing: string[] } => {
    const normalizedHeaders = headers.map(h => h.trim());
    const missing = requiredHeaders.filter(req => !normalizedHeaders.includes(req));
    return { valid: missing.length === 0, missing };
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
      const validation = validateHeaders(parsed.headers, REQUIRED_HEADERS[type]);
      
      if (!validation.valid) {
        const error = `Header mancanti: ${validation.missing.join(', ')}`;
        setFiles(prev => ({
          ...prev,
          [type]: { file: null, status: 'error', error }
        }));
        
        toast({
          title: "Errore validazione header",
          description: error,
          variant: "destructive"
        });
        return;
      }

      setFiles(prev => ({
        ...prev,
        [type]: {
          file: {
            name: file.name,
            data: parsed.data,
            headers: parsed.headers
          },
          status: 'valid'
        }
      }));

      toast({
        title: "File caricato con successo",
        description: `${file.name} - ${parsed.data.length} righe`
      });

    } catch (error) {
      const errorMsg = error instanceof Error ? error.message : 'Errore sconosciuto';
      setFiles(prev => ({
        ...prev,
        [type]: { file: null, status: 'error', error: errorMsg }
      }));
      
      toast({
        title: "Errore caricamento file",
        description: errorMsg,
        variant: "destructive"
      });
    }
  };

  const formatTime = (ms: number): string => {
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    
    if (hours > 0) {
      return `${hours}h ${minutes % 60}m ${seconds % 60}s`;
    } else if (minutes > 0) {
      return `${minutes}m ${seconds % 60}s`;
    } else {
      return `${seconds}s`;
    }
  };

  const processData = async () => {
    if (!files.material.file || !files.stock.file || !files.price.file) {
      toast({
        title: "File mancanti",
        description: "Carica tutti e tre i file prima di procedere",
        variant: "destructive"
      });
      return;
    }

    setProcessing(true);
    setProcessingTime({ started: Date.now(), elapsed: 0, estimated: 0 });
    
    // Start timing
    timeInterval.current = setInterval(() => {
      const elapsed = Date.now() - processingTime.started;
      const estimated = progress.progress > 0 ? (elapsed / progress.progress) * (100 - progress.progress) : 0;
      setProcessingTime(prev => ({ ...prev, elapsed, estimated }));
    }, 1000);

    try {
      // Create Web Worker
      workerRef.current = new Worker('/alterside-worker.js');
      
      workerRef.current.onmessage = (e) => {
        const { type, ...data } = e.data;
        
        switch (type) {
          case 'progress':
            setProgress({
              stage: data.stage,
              progress: data.progress,
              recordsProcessed: data.recordsProcessed || 0,
              totalRecords: data.totalRecords || 0
            });
            break;
            
          case 'complete':
            setProcessedDataEAN(data.processedDataEAN);
            setProcessedDataManufPartNr(data.processedDataManufPartNr);
            setLogEntriesEAN(data.logEntriesEAN);
            setLogEntriesManufPartNr(data.logEntriesManufPartNr);
            setStats(data.stats);
            
            toast({
              title: "Elaborazione completata",
              description: `EAN: ${data.processedDataEAN.length} record | ManufPartNr: ${data.processedDataManufPartNr.length} record`
            });
            
            setProcessing(false);
            if (timeInterval.current) {
              clearInterval(timeInterval.current);
            }
            break;
            
          case 'error':
            toast({
              title: "Errore elaborazione",
              description: data.error,
              variant: "destructive"
            });
            setProcessing(false);
            if (timeInterval.current) {
              clearInterval(timeInterval.current);
            }
            break;
        }
      };

      // Send data to worker
      workerRef.current.postMessage({
        files: {
          material: files.material.file,
          stock: files.stock.file,
          price: files.price.file
        }
      });

    } catch (error) {
      toast({
        title: "Errore",
        description: "Errore durante l'avvio dell'elaborazione",
        variant: "destructive"
      });
      setProcessing(false);
      if (timeInterval.current) {
        clearInterval(timeInterval.current);
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
    return data.map(record => ({
      ...record,
      ExistingStock: record.ExistingStock.toString(),
      ListPrice: record.ListPrice.toFixed(2).replace('.', ','),
      CustBestPrice: record.CustBestPrice.toString(),
      'ListPrice con IVA': record['ListPrice con IVA'].toFixed(2).replace('.', ','),
      'CustBestPrice con IVA': record['CustBestPrice con IVA'].toFixed(2).replace('.', ','),
      'Costo di spedizione': record['Costo di spedizione'].toString(),
      'Prezzo finale': record['Prezzo finale'].toFixed(2).replace('.', ','),
      'Prezzo finale Listino': record['Prezzo finale Listino'].toString()
    }));
  };

  const downloadExcel = (type: 'ean' | 'manufpartnr') => {
    const data = type === 'ean' ? processedDataEAN : processedDataManufPartNr;
    if (data.length === 0) return;

    const { timestamp, sheetName } = getTimestamp();
    const filename = `catalogo_${type}_${timestamp}.xlsx`;

    const excelData = formatExcelData(data);
    const ws = XLSX.utils.json_to_sheet(excelData);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, sheetName);
    XLSX.writeFile(wb, filename);

    toast({
      title: "Excel scaricato",
      description: `File ${filename} scaricato con successo`
    });
  };

  const downloadLog = (type: 'ean' | 'manufpartnr') => {
    const logs = type === 'ean' ? logEntriesEAN : logEntriesManufPartNr;
    if (logs.length === 0 && !stats) return;

    const { timestamp, sheetName } = getTimestamp();
    const filename = `catalogo_log_${type}_${timestamp}.xlsx`;

    const logData = logs.map(entry => ({
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
  }> = ({ title, description, type, requiredHeaders }) => {
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
            {fileState.status === 'error' && (
              <div className="badge-err">
                <XCircle className="w-4 h-4" />
                Errore
              </div>
            )}
          </div>

          <p className="text-muted text-sm mb-4">{description}</p>
          
          <div className="text-xs text-muted mb-4">
            <strong>Header richiesti:</strong> {requiredHeaders.join(', ')}
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
                  <p className="font-medium">{fileState.file.name}</p>
                  <p className="text-sm text-muted">
                    {fileState.file.data.length} righe
                  </p>
                </div>
              </div>
              <button
                onClick={() => setFiles(prev => ({ ...prev, [type]: { file: null, status: 'none' } }))}
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
        </div>
      </div>
    );
  };

  const allFilesValid = files.material.status === 'valid' && files.stock.status === 'valid' && files.price.status === 'valid';

  return (
    <div className="min-h-screen p-6" style={{ background: 'var(--bg)', color: 'var(--fg)' }}>
      <div className="max-w-7xl mx-auto space-y-8">
        {/* Header */}
        <div className="text-center">
          <h1 className="text-5xl font-bold mb-4">
            Alterside Catalog Generator
          </h1>
          <p className="text-muted text-xl max-w-3xl mx-auto">
            Genera due cataloghi Excel distinti (EAN e ManufPartNr) con calcoli avanzati di prezzo e commissioni
          </p>
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
                  <li>• <strong>Prezzi:</strong> CustBestPrice arrotondato per eccesso, IVA 22%, commissioni 7% + 5%</li>
                  <li>• <strong>Prezzo finale:</strong> arrotondamento a ,99 (da Best) o intero superiore (da Listino)</li>
                </ul>
              </div>
            </div>
          </div>
        </div>

        {/* File Upload Cards */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <FileUploadCard
            title="Material File"
            description="File principale con informazioni prodotto"
            type="material"
            requiredHeaders={REQUIRED_HEADERS.material}
          />
          <FileUploadCard
            title="Stock File Data"
            description="Dati scorte e disponibilità"
            type="stock"
            requiredHeaders={REQUIRED_HEADERS.stock}
          />
          <FileUploadCard
            title="Price File Data"
            description="Listini prezzi e scontistiche"
            type="price"
            requiredHeaders={REQUIRED_HEADERS.price}
          />
        </div>

        {/* Process Button */}
        {allFilesValid && (
          <div className="text-center">
            <button
              onClick={processData}
              disabled={processing}
              className={`btn btn-primary text-lg px-12 py-4 ${processing ? 'is-disabled' : ''}`}
            >
              {processing ? (
                <>
                  <Activity className="mr-3 h-5 w-5 animate-spin" />
                  Elaborazione in corso...
                </>
              ) : (
                <>
                  <Upload className="mr-3 h-5 w-5" />
                  ELABORA DATI
                </>
              )}
            </button>
          </div>
        )}

        {/* Progress Section */}
        {processing && (
          <div className="card border-strong">
            <div className="card-body">
              <h3 className="card-title mb-6 flex items-center gap-2">
                <Activity className="h-5 w-5 animate-spin icon-dark" />
                Progresso Elaborazione
              </h3>
              
              <div className="space-y-4">
                <div className="progress">
                  <span style={{ width: `${progress.progress}%` }} />
                </div>
                
                <div className="flex justify-between text-sm">
                  <span className="font-medium">{progress.stage}</span>
                  <span className="font-bold">{progress.progress}%</span>
                </div>
                
                {progress.totalRecords > 0 && (
                  <div className="text-sm text-muted">
                    Record elaborati: {progress.recordsProcessed.toLocaleString()} / {progress.totalRecords.toLocaleString()}
                  </div>
                )}
                
                <div className="flex items-center gap-6 text-sm">
                  <div className="flex items-center gap-2">
                    <Clock className="h-4 w-4 icon-dark" />
                    <span>Trascorso: {formatTime(processingTime.elapsed)}</span>
                  </div>
                  {processingTime.estimated > 0 && (
                    <div className="flex items-center gap-2">
                      <Clock className="h-4 w-4 icon-dark" />
                      <span>Stimato: {formatTime(processingTime.estimated)}</span>
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Statistics */}
        {stats && (
          <div className="card border-strong">
            <div className="card-body">
              <h3 className="card-title mb-6">Statistiche Elaborazione</h3>
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
                <div className="text-center p-4 rounded-lg border-strong" style={{ background: '#f8fafc' }}>
                  <div className="text-2xl font-bold">{stats.totalRecords.toLocaleString()}</div>
                  <div className="text-sm text-muted">Righe Totali</div>
                </div>
                <div className="text-center p-4 rounded-lg border-strong" style={{ background: 'var(--success-bg)' }}>
                  <div className="text-2xl font-bold" style={{ color: 'var(--success-fg)' }}>{stats.validRecordsEAN.toLocaleString()}</div>
                  <div className="text-sm text-muted">Valide EAN</div>
                </div>
                <div className="text-center p-4 rounded-lg border-strong" style={{ background: 'var(--success-bg)' }}>
                  <div className="text-2xl font-bold" style={{ color: 'var(--success-fg)' }}>{stats.validRecordsManufPartNr.toLocaleString()}</div>
                  <div className="text-sm text-muted">Valide ManufPartNr</div>
                </div>
                <div className="text-center p-4 rounded-lg border-strong" style={{ background: 'var(--error-bg)' }}>
                  <div className="text-2xl font-bold" style={{ color: 'var(--error-fg)' }}>{stats.filteredRecordsEAN.toLocaleString()}</div>
                  <div className="text-sm text-muted">Scartate EAN</div>
                </div>
                <div className="text-center p-4 rounded-lg border-strong" style={{ background: 'var(--error-bg)' }}>
                  <div className="text-2xl font-bold" style={{ color: 'var(--error-fg)' }}>{stats.filteredRecordsManufPartNr.toLocaleString()}</div>
                  <div className="text-sm text-muted">Scartate ManufPartNr</div>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Download Buttons */}
        {(processedDataEAN.length > 0 || processedDataManufPartNr.length > 0) && (
          <div className="flex flex-wrap justify-center gap-4">
            <button 
              onClick={() => downloadExcel('ean')} 
              className={`btn btn-primary text-lg px-8 py-3 ${processedDataEAN.length === 0 ? 'is-disabled' : ''}`}
              disabled={processedDataEAN.length === 0}
            >
              <Download className="mr-3 h-5 w-5" />
              SCARICA EXCEL (EAN)
            </button>
            <button 
              onClick={() => downloadExcel('manufpartnr')} 
              className={`btn btn-primary text-lg px-8 py-3 ${processedDataManufPartNr.length === 0 ? 'is-disabled' : ''}`}
              disabled={processedDataManufPartNr.length === 0}
            >
              <Download className="mr-3 h-5 w-5" />
              SCARICA EXCEL (ManufPartNr)
            </button>
            <button 
              onClick={() => downloadLog('ean')} 
              className={`btn btn-secondary text-lg px-8 py-3 ${logEntriesEAN.length === 0 ? 'is-disabled' : ''}`}
              disabled={logEntriesEAN.length === 0}
            >
              <Download className="mr-3 h-5 w-5" />
              SCARICA LOG (EAN)
            </button>
            <button 
              onClick={() => downloadLog('manufpartnr')} 
              className={`btn btn-secondary text-lg px-8 py-3 ${logEntriesManufPartNr.length === 0 ? 'is-disabled' : ''}`}
              disabled={logEntriesManufPartNr.length === 0}
            >
              <Download className="mr-3 h-5 w-5" />
              SCARICA LOG (ManufPartNr)
            </button>
          </div>
        )}

        {/* Data Previews */}
        {processedDataEAN.length > 0 && (
          <div className="card border-strong">
            <div className="card-body">
              <h3 className="card-title mb-6">Anteprima Export EAN (Prime 10 Righe)</h3>
              <div className="overflow-x-auto">
                <table className="table-zebra">
                  <thead>
                    <tr>
                      {Object.keys(processedDataEAN[0]).map((header, index) => (
                        <th key={index}>{header}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {processedDataEAN.slice(0, 10).map((row, rowIndex) => (
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

        {processedDataManufPartNr.length > 0 && (
          <div className="card border-strong">
            <div className="card-body">
              <h3 className="card-title mb-6">Anteprima Export ManufPartNr (Prime 10 Righe)</h3>
              <div className="overflow-x-auto">
                <table className="table-zebra">
                  <thead>
                    <tr>
                      {Object.keys(processedDataManufPartNr[0]).map((header, index) => (
                        <th key={index}>{header}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {processedDataManufPartNr.slice(0, 10).map((row, rowIndex) => (
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