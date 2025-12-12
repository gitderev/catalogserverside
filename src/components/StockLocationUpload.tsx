import React, { useState, useCallback } from 'react';
import { supabase } from '@/integrations/supabase/client';
import { Upload, FileText, CheckCircle, XCircle, AlertCircle } from 'lucide-react';
import { toast } from '@/hooks/use-toast';
import { 
  STOCK_LOCATION_LATEST_KEY, 
  STOCK_LOCATION_MANUAL_PREFIX,
  STOCK_LOCATION_REGEX,
  parseStockLocationFile,
  createEmptyWarnings
} from '@/utils/stockLocation';

interface StockLocationUploadProps {
  disabled?: boolean;
  onFileLoaded?: (content: string, warnings: ReturnType<typeof createEmptyWarnings>) => void;
}

interface UploadState {
  status: 'empty' | 'uploading' | 'valid' | 'error';
  filename: string | null;
  rowCount: number;
  error: string | null;
}

/**
 * Stock Location Upload Card Component
 * 
 * Fourth upload card for the stock location file (790813_StockFile_YYYYMMDD.txt).
 * Uploads to bucket 'ftp-import' with keys:
 * - stock-location/latest.txt (always overwritten)
 * - stock-location/manual/<timestamp>_<filename>.txt (versioned copy)
 */
const StockLocationUpload: React.FC<StockLocationUploadProps> = ({ 
  disabled = false,
  onFileLoaded 
}) => {
  const [state, setState] = useState<UploadState>({
    status: 'empty',
    filename: null,
    rowCount: 0,
    error: null
  });

  const handleFileUpload = useCallback(async (file: File) => {
    if (disabled) return;

    // Validate filename format
    const filenameMatch = file.name.match(STOCK_LOCATION_REGEX);
    if (!filenameMatch) {
      setState({
        status: 'error',
        filename: file.name,
        rowCount: 0,
        error: `Nome file non valido. Atteso formato: 790813_StockFile_YYYYMMDD.txt`
      });
      toast({
        title: "File non valido",
        description: "Il nome del file deve essere nel formato 790813_StockFile_YYYYMMDD.txt",
        variant: "destructive"
      });
      return;
    }

    setState({
      status: 'uploading',
      filename: file.name,
      rowCount: 0,
      error: null
    });

    try {
      // Read file content
      const content = await file.text();
      
      // Parse to validate and count rows
      const warnings = createEmptyWarnings();
      const index = parseStockLocationFile(content, warnings);
      const rowCount = Object.keys(index).length;

      if (rowCount === 0) {
        setState({
          status: 'error',
          filename: file.name,
          rowCount: 0,
          error: 'File vuoto o formato non valido. Verificare header: Matnr;Stock;...;LocationID;...'
        });
        toast({
          title: "File non valido",
          description: "Il file non contiene dati validi o l'header è mancante",
          variant: "destructive"
        });
        return;
      }

      // Upload to latest.txt (overwrite)
      const { error: latestError } = await supabase.storage
        .from('ftp-import')
        .upload(STOCK_LOCATION_LATEST_KEY, file, { 
          upsert: true,
          contentType: 'text/plain'
        });

      if (latestError) {
        throw new Error(`Errore upload latest: ${latestError.message}`);
      }

      // Upload versioned copy
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      const versionedKey = `${STOCK_LOCATION_MANUAL_PREFIX}${timestamp}_${file.name}`;
      
      await supabase.storage
        .from('ftp-import')
        .upload(versionedKey, file, { 
          upsert: true,
          contentType: 'text/plain'
        });

      setState({
        status: 'valid',
        filename: file.name,
        rowCount,
        error: null
      });

      toast({
        title: "File caricato",
        description: `Stock Location caricato: ${rowCount} prodotti univoci`
      });

      // Notify parent with parsed content and warnings
      if (onFileLoaded) {
        onFileLoaded(content, warnings);
      }

    } catch (error) {
      const errMsg = error instanceof Error ? error.message : 'Errore sconosciuto';
      setState({
        status: 'error',
        filename: file.name,
        rowCount: 0,
        error: errMsg
      });
      toast({
        title: "Errore upload",
        description: errMsg,
        variant: "destructive"
      });
    }
  }, [disabled, onFileLoaded]);

  const removeFile = useCallback(() => {
    setState({
      status: 'empty',
      filename: null,
      rowCount: 0,
      error: null
    });
  }, []);

  return (
    <div className="card border-strong">
      <div className="card-body">
        <div className="flex items-center justify-between mb-4">
          <h3 className="card-title">Stock Location (IT/EU)</h3>
          {state.status === 'valid' && (
            <div className="badge-ok">
              <CheckCircle className="w-4 h-4" />
              Caricato
            </div>
          )}
          {state.status === 'uploading' && (
            <div className="badge-ok" style={{ background: '#e0f2fe', color: '#0369a1', border: '1px solid #7dd3fc' }}>
              <AlertCircle className="w-4 h-4 animate-pulse" />
              Caricamento...
            </div>
          )}
          {state.status === 'error' && (
            <div className="badge-err">
              <XCircle className="w-4 h-4" />
              Errore
            </div>
          )}
        </div>

        <p className="text-muted text-sm mb-4">
          File split stock per magazzino IT (4242) e EU (4254).
          Formato: 790813_StockFile_YYYYMMDD.txt
        </p>

        <div className="text-xs text-muted mb-4">
          <div><strong>Header richiesti:</strong> Matnr, Stock, LocationID</div>
          <div><strong>Opzionali:</strong> ManufPartNo, NextDelDate, Category</div>
        </div>

        {state.status === 'empty' ? (
          <div className={`dropzone text-center ${disabled ? 'opacity-50 cursor-not-allowed' : ''}`}>
            <Upload className="mx-auto h-12 w-12 icon-dark mb-4" />
            <div>
              <input
                type="file"
                accept=".txt,.csv"
                onChange={(e) => {
                  const selectedFile = e.target.files?.[0];
                  if (selectedFile) {
                    handleFileUpload(selectedFile);
                  }
                }}
                className="hidden"
                id="file-stock-location"
                disabled={disabled}
              />
              <label
                htmlFor="file-stock-location"
                className={`btn btn-primary cursor-pointer px-6 py-3 ${disabled ? 'pointer-events-none opacity-50' : ''}`}
              >
                Carica File
              </label>
              <p className="text-muted text-sm mt-3">
                CSV con delimitatore ; e encoding UTF-8
              </p>
            </div>
          </div>
        ) : (
          <div className="flex items-center justify-between p-4 bg-white rounded-lg border-strong">
            <div className="flex items-center gap-3">
              <FileText className="h-6 w-6 icon-dark" />
              <div>
                <p className="font-medium">{state.filename}</p>
                <p className="text-sm text-muted">
                  {state.rowCount > 0 ? `${state.rowCount} prodotti univoci` : 'Caricamento...'}
                </p>
              </div>
            </div>
            <button
              onClick={removeFile}
              className="btn btn-secondary text-sm px-3 py-2"
              disabled={disabled}
            >
              Rimuovi
            </button>
          </div>
        )}

        {state.status === 'error' && state.error && (
          <div className="mt-4 p-3 rounded-lg border-strong" style={{ background: 'var(--error-bg)', color: 'var(--error-fg)' }}>
            <p className="text-sm font-medium">{state.error}</p>
          </div>
        )}

        {state.status === 'valid' && (
          <div className="mt-4 p-3 rounded-lg border-strong bg-gray-50">
            <h4 className="text-sm font-medium mb-2">Informazioni</h4>
            <div className="text-xs text-muted">
              <div>Lo stock IT proviene da LocationID 4242</div>
              <div>Lo stock EU proviene da LocationID 4254</div>
              <div className="mt-1 text-amber-600">LocationID 4255 viene ignorato (duplicato EU)</div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default StockLocationUpload;
