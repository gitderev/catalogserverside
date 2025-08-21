import React, { useState, useCallback } from 'react';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { Progress } from '@/components/ui/progress';
import { Separator } from '@/components/ui/separator';
import { FileUpload } from '@/components/FileUpload';
import { DataPreview } from '@/components/DataPreview';
import { ColumnMapping } from '@/components/ColumnMapping';
import { parseTXT, mergeTXTData, exportToExcel, autoDetectSKUColumn, ParsedTXT } from '@/utils/txtMerger';
import { useToast } from '@/hooks/use-toast';
import { Merge, Download, FileSpreadsheet, CheckCircle } from 'lucide-react';

const Index = () => {
  const [file1, setFile1] = useState<File | null>(null);
  const [file2, setFile2] = useState<File | null>(null);
  const [parsedData1, setParsedData1] = useState<ParsedTXT | null>(null);
  const [parsedData2, setParsedData2] = useState<ParsedTXT | null>(null);
  const [skuColumn1, setSkuColumn1] = useState<string>('');
  const [skuColumn2, setSkuColumn2] = useState<string>('');
  const [mergedData, setMergedData] = useState<any[] | null>(null);
  const [isProcessing, setIsProcessing] = useState(false);
  const [progress, setProgress] = useState(0);
  
  const { toast } = useToast();

  const handleFileSelect = useCallback((fileNumber: 1 | 2) => {
    return async (file: File) => {
      if (fileNumber === 1) {
        setFile1(file);
        setParsedData1(null);
        setSkuColumn1('');
      } else {
        setFile2(file);
        setParsedData2(null);
        setSkuColumn2('');
      }
      
      if (file) {
        try {
          setIsProcessing(true);
          setProgress(25);
          
          const parsed = await parseTXT(file);
          const detectedSKU = autoDetectSKUColumn(parsed.headers);
          
          if (fileNumber === 1) {
            setParsedData1(parsed);
            setSkuColumn1(detectedSKU);
          } else {
            setParsedData2(parsed);
            setSkuColumn2(detectedSKU);
          }
          
          setProgress(100);
          
          toast({
            title: "File caricato con successo",
            description: `File ${fileNumber} elaborato correttamente. ${detectedSKU ? `Colonna SKU rilevata: ${detectedSKU}` : ''}`,
          });
        } catch (error) {
          toast({
            title: "Errore nel caricamento",
            description: error instanceof Error ? error.message : "Errore sconosciuto",
            variant: "destructive",
          });
        } finally {
          setIsProcessing(false);
          setProgress(0);
        }
      }
    };
  }, [toast]);

  const handleMerge = useCallback(async () => {
    if (!parsedData1 || !parsedData2 || !skuColumn1 || !skuColumn2) {
      toast({
        title: "Dati mancanti",
        description: "Assicurati di aver caricato entrambi i file e selezionato le colonne SKU",
        variant: "destructive",
      });
      return;
    }

    try {
      setIsProcessing(true);
      setProgress(20);

      const merged = mergeTXTData(
        parsedData1.data,
        parsedData2.data,
        skuColumn1,
        skuColumn2
      );

      setProgress(100);
      setMergedData(merged);

      toast({
        title: "Unione completata",
        description: `${merged.length} prodotti elaborati con successo`,
      });
    } catch (error) {
      toast({
        title: "Errore nell'unione",
        description: error instanceof Error ? error.message : "Errore sconosciuto",
        variant: "destructive",
      });
    } finally {
      setIsProcessing(false);
      setProgress(0);
    }
  }, [parsedData1, parsedData2, skuColumn1, skuColumn2, toast]);

  const handleExport = useCallback(() => {
    if (!mergedData) return;
    
    try {
      const timestamp = new Date().toISOString().split('T')[0];
      exportToExcel(mergedData, `txt_merged_${timestamp}`);
      
      toast({
        title: "Export completato",
        description: `File Excel scaricato con ${mergedData.length} righe`,
      });
    } catch (error) {
      toast({
        title: "Errore nell'export",
        description: error instanceof Error ? error.message : "Errore sconosciuto",
        variant: "destructive",
      });
    }
  }, [mergedData, toast]);

  const canMerge = parsedData1 && parsedData2 && skuColumn1 && skuColumn2;
  const canExport = mergedData && mergedData.length > 0;

  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <div className="bg-gradient-hero border-b shadow-elevation">
        <div className="container mx-auto px-4 py-8">
          <div className="text-center">
            <div className="flex items-center justify-center gap-3 mb-4">
              <FileSpreadsheet className="h-10 w-10 text-primary" />
              <h1 className="text-4xl font-bold text-primary">TXT Merger Pro</h1>
            </div>
            <p className="text-xl text-primary/80 max-w-2xl mx-auto">
              Unisci facilmente due file TXT basandoti sui codici SKU per creare un database completo dei tuoi prodotti
            </p>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="container mx-auto px-4 py-8">
        {/* Progress Bar */}
        {isProcessing && (
          <Card className="p-4 mb-6">
            <div className="flex items-center gap-4">
              <div className="flex-1">
                <Progress value={progress} className="h-2" />
              </div>
              <span className="text-sm text-muted-foreground">{progress}%</span>
            </div>
          </Card>
        )}

        {/* File Upload Section */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
          <FileUpload
            title="File TXT 1"
            onFileSelect={handleFileSelect(1)}
            selectedFile={file1}
          />
          <FileUpload
            title="File TXT 2"
            onFileSelect={handleFileSelect(2)}
            selectedFile={file2}
          />
        </div>

        {/* Column Mapping */}
        {parsedData1 && parsedData2 && (
          <>
            <ColumnMapping
              file1Headers={parsedData1.headers}
              file2Headers={parsedData2.headers}
              skuColumn1={skuColumn1}
              skuColumn2={skuColumn2}
              onSkuColumn1Change={setSkuColumn1}
              onSkuColumn2Change={setSkuColumn2}
            />
            
            <div className="my-8">
              <Separator />
            </div>
          </>
        )}

        {/* Data Preview Section */}
        {(parsedData1 || parsedData2) && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
            {parsedData1 && (
              <DataPreview
                data={parsedData1.data}
                title="Anteprima File 1"
              />
            )}
            {parsedData2 && (
              <DataPreview
                data={parsedData2.data}
                title="Anteprima File 2"
              />
            )}
          </div>
        )}

        {/* Action Buttons */}
        <div className="flex flex-col sm:flex-row gap-4 justify-center mb-8">
          <Button
            onClick={handleMerge}
            disabled={!canMerge || isProcessing}
            variant="hero"
            size="lg"
            className="min-w-[200px]"
          >
            <Merge className="mr-2 h-5 w-5" />
            Unisci File TXT
          </Button>
          
          <Button
            onClick={handleExport}
            disabled={!canExport}
            variant="success"
            size="lg"
            className="min-w-[200px]"
          >
            <Download className="mr-2 h-5 w-5" />
            Scarica Excel
          </Button>
        </div>

        {/* Merged Data Preview */}
        {mergedData && (
          <div className="mb-8">
            <div className="flex items-center gap-3 mb-4">
              <CheckCircle className="h-6 w-6 text-green-500" />
              <h2 className="text-2xl font-semibold text-card-foreground">
                Risultato Unione
              </h2>
            </div>
            <DataPreview
              data={mergedData}
              title={`Dati Uniti (${mergedData.length} prodotti)`}
              maxRows={10}
            />
          </div>
        )}

        {/* Instructions */}
        <Card className="p-6 bg-accent/30">
          <h3 className="text-lg font-semibold text-card-foreground mb-4">
            Come utilizzare TXT Merger Pro
          </h3>
          <ol className="list-decimal list-inside space-y-2 text-muted-foreground">
            <li>Carica i tuoi due file TXT utilizzando i riquadri sopra</li>
            <li>Il sistema rileva automaticamente il delimitatore (virgola, tab, punto e virgola)</li>
            <li>Verifica che le colonne SKU siano state rilevate automaticamente</li>
            <li>Se necessario, seleziona manualmente le colonne contenenti i codici SKU</li>
            <li>Clicca su "Unisci File TXT" per elaborare i dati</li>
            <li>Scarica il file Excel con tutti i dati uniti per codice SKU</li>
          </ol>
        </Card>
      </div>
    </div>
  );
};

export default Index;