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
  validRecords: number;
  filteredRecords: number;
  stockDuplicates: number;
  priceDuplicates: number;
}

const REQUIRED_HEADERS = {
  material: ['Matnr', 'ManufPartNr', 'EAN', 'ShortDescription'],
  stock: ['Matnr', 'ManufPartNr', 'ExistingStock'],
  price: ['Matnr', 'ManufPartNr', 'ListPrice', 'CustBestPrice']
};

const ElaboratoreCatalogo: React.FC = () => {
  const [files, setFiles] = useState<FileUploadState>({
    material: { file: null, status: 'none' },
    stock: { file: null, status: 'none' },
    price: { file: null, status: 'none' }
  });

  const [processing, setProcessing] = useState(false);
  const [progress, setProgress] = useState({ stage: '', progress: 0, recordsProcessed: 0, totalRecords: 0 });
  const [processingTime, setProcessingTime] = useState({ started: 0, elapsed: 0, estimated: 0 });
  const [processedData, setProcessedData] = useState<ProcessedRecord[]>([]);
  const [logEntries, setLogEntries] = useState<LogEntry[]>([]);
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
      workerRef.current = new Worker('/processingWorker.js');
      
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
            setProcessedData(data.processedData);
            setLogEntries(data.logEntries);
            setStats(data.stats);
            
            toast({
              title: "Elaborazione completata",
              description: `${data.processedData.length} record validi elaborati`
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

  const downloadExcel = () => {
    if (processedData.length === 0) return;

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
    
    const timestamp = `${year}${month}${day}_${hour}${minute}`;
    const sheetName = `${year}-${month}-${day}`;
    const filename = `catalogo_${timestamp}.xlsx`;

    // Format data for Excel with proper number formatting
    const excelData = processedData.map(record => ({
      ...record,
      ExistingStock: record.ExistingStock.toString(),
      ListPrice: record.ListPrice.toFixed(2).replace('.', ','),
      CustBestPrice: record.CustBestPrice.toFixed(2).replace('.', ','),
      'ListPrice con IVA': record['ListPrice con IVA'].toFixed(2).replace('.', ','),
      'CustBestPrice con IVA': record['CustBestPrice con IVA'].toFixed(2).replace('.', ',')
    }));

    const ws = XLSX.utils.json_to_sheet(excelData);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, sheetName);
    XLSX.writeFile(wb, filename);

    toast({
      title: "Excel scaricato",
      description: `File ${filename} scaricato con successo`
    });
  };

  const downloadLog = () => {
    if (logEntries.length === 0 && !stats) return;

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
    
    const timestamp = `${year}${month}${day}_${hour}${minute}`;
    const filename = `catalogo_log_${timestamp}.csv`;

    // Create summary
    const summary = [
      'RIEPILOGO ELABORAZIONE',
      `Timestamp: ${romeTime}`,
      `Righe totali lette: ${stats?.totalRecords || 0}`,
      `Righe esportate: ${stats?.validRecords || 0}`,
      `Righe scartate: ${stats?.filteredRecords || 0}`,
      `Duplicati stock: ${stats?.stockDuplicates || 0}`,
      `Duplicati prezzi: ${stats?.priceDuplicates || 0}`,
      '',
      'DETTAGLIO SCARTI',
      'source_file;line;Matnr;ManufPartNr;EAN;reason;details'
    ];

    const csvContent = [
      ...summary,
      ...logEntries.map(entry => 
        `${entry.source_file};${entry.line};${entry.Matnr};${entry.ManufPartNr};${entry.EAN};${entry.reason};${entry.details}`
      )
    ].join('\n');

    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = filename;
    link.click();

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
      <div className="flat-card p-6">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-xl font-semibold text-card-foreground">{title}</h3>
          {fileState.status === 'valid' && (
            <div className="flat-badge-success">
              <CheckCircle className="w-4 h-4" />
              Caricato
            </div>
          )}
          {fileState.status === 'error' && (
            <div className="flat-badge-error">
              <XCircle className="w-4 h-4" />
              Errore
            </div>
          )}
        </div>

        <p className="text-muted-foreground text-sm mb-4">{description}</p>
        
        <div className="text-xs text-muted-foreground mb-4">
          <strong>Header richiesti:</strong> {requiredHeaders.join(', ')}
        </div>

        {!fileState.file ? (
          <div className="flat-upload-box text-center">
            <Upload className="mx-auto h-12 w-12 text-muted-foreground mb-4" />
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
                className="flat-button cursor-pointer inline-block"
              >
                Carica File
              </label>
              <p className="text-muted-foreground text-sm mt-3">
                File CSV con delimitatore ; e encoding UTF-8
              </p>
            </div>
          </div>
        ) : (
          <div className="flex items-center justify-between p-4 bg-accent rounded-lg">
            <div className="flex items-center gap-3">
              <FileText className="h-6 w-6 text-primary" />
              <div>
                <p className="font-medium text-card-foreground">{fileState.file.name}</p>
                <p className="text-sm text-muted-foreground">
                  {fileState.file.data.length} righe
                </p>
              </div>
            </div>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setFiles(prev => ({ ...prev, [type]: { file: null, status: 'none' } }))}
              className="text-destructive hover:text-destructive"
            >
              Rimuovi
            </Button>
          </div>
        )}

        {fileState.status === 'error' && fileState.error && (
          <div className="mt-4 p-3 bg-destructive/10 border border-destructive/20 rounded-lg">
            <p className="text-destructive text-sm font-medium">{fileState.error}</p>
          </div>
        )}
      </div>
    );
  };

  const allFilesValid = files.material.status === 'valid' && files.stock.status === 'valid' && files.price.status === 'valid';

  return (
    <div className="min-h-screen bg-background p-6 font-inter">
      <div className="max-w-7xl mx-auto space-y-8">
        {/* Header */}
        <div className="text-center">
          <h1 className="text-5xl font-bold text-foreground mb-4">
            Elaboratore Catalogo
          </h1>
          <p className="text-muted-foreground text-xl max-w-3xl mx-auto">
            Carica i file TXT, elabora i dati con regole precise e scarica il catalogo Excel pronto per l'uso
          </p>
        </div>

        {/* Instructions */}
        <Card className="p-6 bg-accent/30 border-2 border-accent">
          <div className="flex items-start gap-4">
            <AlertCircle className="h-6 w-6 text-primary mt-1 flex-shrink-0" />
            <div>
              <h3 className="text-lg font-semibold text-card-foreground mb-3">Istruzioni di Caricamento</h3>
              <ul className="text-sm text-muted-foreground space-y-2">
                <li>• <strong>File CSV:</strong> Delimitatore punto e virgola (;), encoding UTF-8, header obbligatorio</li>
                <li>• <strong>Unione dati:</strong> Left join su Matnr partendo dal MaterialFile</li>
                <li>• <strong>Filtri applicati:</strong> EAN non vuoto, ExistingStock &gt; 0, prezzi numerici validi</li>
                <li>• <strong>Calcoli:</strong> IVA al 22% sui prezzi, arrotondamento a 2 decimali</li>
                <li>• <strong>Output:</strong> Excel con formato data italiana e log CSV dettagliato</li>
              </ul>
            </div>
          </div>
        </Card>

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
            <Button
              onClick={processData}
              disabled={processing}
              size="lg"
              className="flat-button-success text-lg px-12 py-4"
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
            </Button>
          </div>
        )}

        {/* Progress Section */}
        {processing && (
          <Card className="p-6">
            <h3 className="text-xl font-semibold mb-6 flex items-center gap-2">
              <Activity className="h-5 w-5 animate-spin" />
              Progresso Elaborazione
            </h3>
            
            <div className="space-y-4">
              <div className="flat-progress">
                <div 
                  className="flat-progress-fill"
                  style={{ width: `${progress.progress}%` }}
                />
              </div>
              
              <div className="flex justify-between text-sm">
                <span className="font-medium">{progress.stage}</span>
                <span className="font-bold">{progress.progress}%</span>
              </div>
              
              {progress.totalRecords > 0 && (
                <div className="text-sm text-muted-foreground">
                  Record elaborati: {progress.recordsProcessed.toLocaleString()} / {progress.totalRecords.toLocaleString()}
                </div>
              )}
              
              <div className="flex items-center gap-6 text-sm">
                <div className="flex items-center gap-2">
                  <Clock className="h-4 w-4" />
                  <span>Trascorso: {formatTime(processingTime.elapsed)}</span>
                </div>
                {processingTime.estimated > 0 && (
                  <div className="flex items-center gap-2">
                    <Clock className="h-4 w-4" />
                    <span>Stimato: {formatTime(processingTime.estimated)}</span>
                  </div>
                )}
              </div>
            </div>
          </Card>
        )}

        {/* Statistics */}
        {stats && (
          <Card className="p-6">
            <h3 className="text-xl font-semibold text-card-foreground mb-6">Statistiche Elaborazione</h3>
            <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
              <div className="text-center p-4 bg-accent rounded-lg">
                <div className="text-3xl font-bold text-primary">{stats.totalRecords.toLocaleString()}</div>
                <div className="text-sm text-muted-foreground">Righe Totali</div>
              </div>
              <div className="text-center p-4 bg-success/10 rounded-lg">
                <div className="text-3xl font-bold text-success">{stats.validRecords.toLocaleString()}</div>
                <div className="text-sm text-muted-foreground">Righe Valide</div>
              </div>
              <div className="text-center p-4 bg-destructive/10 rounded-lg">
                <div className="text-3xl font-bold text-destructive">{stats.filteredRecords.toLocaleString()}</div>
                <div className="text-sm text-muted-foreground">Righe Scartate</div>
              </div>
              <div className="text-center p-4 bg-orange-100 rounded-lg">
                <div className="text-3xl font-bold text-orange-600">{stats.stockDuplicates.toLocaleString()}</div>
                <div className="text-sm text-muted-foreground">Duplicati Stock</div>
              </div>
              <div className="text-center p-4 bg-orange-100 rounded-lg">
                <div className="text-3xl font-bold text-orange-600">{stats.priceDuplicates.toLocaleString()}</div>
                <div className="text-sm text-muted-foreground">Duplicati Prezzi</div>
              </div>
            </div>
          </Card>
        )}

        {/* Download Buttons */}
        {processedData.length > 0 && (
          <div className="flex justify-center gap-6">
            <Button onClick={downloadExcel} className="flat-button-success text-lg px-8 py-3">
              <Download className="mr-3 h-5 w-5" />
              SCARICA EXCEL
            </Button>
            <Button onClick={downloadLog} variant="outline" className="border-2 text-lg px-8 py-3">
              <Download className="mr-3 h-5 w-5" />
              SCARICA LOG
            </Button>
          </div>
        )}

        {/* Data Preview */}
        {processedData.length > 0 && (
          <Card className="p-6">
            <h3 className="text-xl font-semibold text-card-foreground mb-6">Anteprima Dati (Prime 10 Righe)</h3>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b-2 border-border">
                    {Object.keys(processedData[0]).map((header, index) => (
                      <th key={index} className="text-left font-semibold p-3 bg-accent">
                        {header}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {processedData.slice(0, 10).map((row, rowIndex) => (
                    <tr key={rowIndex} className={rowIndex % 2 === 0 ? 'bg-background' : 'bg-accent/30'}>
                      {Object.values(row).map((value, colIndex) => (
                        <td key={colIndex} className="p-3 border-b border-border">
                          {typeof value === 'number' ? value.toLocaleString('it-IT') : String(value)}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>
        )}
      </div>
    </div>
  );
};

export default ElaboratoreCatalogo;