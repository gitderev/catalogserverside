import React, { useCallback } from 'react';
import { useDropzone } from 'react-dropzone';
import { Upload, FileText, X, CheckCircle, XCircle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { toast } from '@/hooks/use-toast';
import Papa from 'papaparse';

interface MaterialUploadProps {
  onFileLoaded: (data: any[], headers: string[], isValid: boolean) => void;
  selectedFile?: File | null;
  isValid?: boolean;
}

export const MaterialUpload: React.FC<MaterialUploadProps> = ({
  onFileLoaded,
  selectedFile,
  isValid = false
}) => {
  const onDrop = useCallback((acceptedFiles: File[]) => {
    if (acceptedFiles.length > 0) {
      const file = acceptedFiles[0];
      
      const reader = new FileReader();
      reader.onload = (e) => {
        const text = e.target?.result as string;
        
        Papa.parse(text, {
          header: true,
          skipEmptyLines: true,
          complete: (results) => {
            const headers = results.meta.fields || [];
            const requiredHeaders = ['Matnr', 'ManufPartNr', 'EAN', 'ShortDescription'];
            
            const hasAllRequired = requiredHeaders.every(header => 
              headers.some(h => h.toLowerCase() === header.toLowerCase())
            );
            
            if (!hasAllRequired) {
              toast({
                title: "Errore validazione Material",
                description: `Header richiesti: ${requiredHeaders.join(', ')}`,
                variant: "destructive",
              });
              onFileLoaded([], [], false);
            } else {
              toast({
                title: "Material caricato",
                description: `${results.data.length} righe caricate con successo`,
              });
              onFileLoaded(results.data, headers, true);
            }
          },
          error: (error) => {
            toast({
              title: "Errore parsing Material",
              description: error.message,
              variant: "destructive",
            });
            onFileLoaded([], [], false);
          }
        });
      };
      
      reader.readAsText(file);
    }
  }, [onFileLoaded]);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'text/csv': ['.csv'],
      'text/plain': ['.txt']
    },
    multiple: false
  });

  const clearFile = () => {
    onFileLoaded([], [], false);
  };

  return (
    <Card className="p-6">
      <div className="flex items-center gap-3 mb-4">
        <Upload className="h-6 w-6 text-primary" />
        <h3 className="text-lg font-semibold">Upload Material</h3>
        {selectedFile && (
          <div className="ml-auto flex items-center gap-2">
            {isValid ? (
              <CheckCircle className="h-5 w-5 text-green-500" />
            ) : (
              <XCircle className="h-5 w-5 text-red-500" />
            )}
          </div>
        )}
      </div>
      
      <div className="text-sm text-muted-foreground mb-4">
        Header richiesti: Matnr, ManufPartNr, EAN, ShortDescription
      </div>

      {!selectedFile ? (
        <div
          {...getRootProps()}
          className={`border-2 border-dashed rounded-lg p-6 text-center cursor-pointer transition-colors ${
            isDragActive
              ? 'border-primary bg-primary/5'
              : 'border-muted-foreground/25 hover:border-primary/50'
          }`}
        >
          <input {...getInputProps()} />
          <Upload className="mx-auto h-12 w-12 text-muted-foreground mb-4" />
          <p className="text-lg font-medium mb-2">
            {isDragActive ? 'Rilascia il file qui' : 'Carica file Material'}
          </p>
          <p className="text-sm text-muted-foreground">
            Trascina e rilascia o clicca per selezionare un file CSV/TXT
          </p>
        </div>
      ) : (
        <div className="border rounded-lg p-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <FileText className="h-8 w-8 text-blue-500" />
              <div>
                <p className="font-medium">{selectedFile.name}</p>
                <p className="text-sm text-muted-foreground">
                  {(selectedFile.size / 1024).toFixed(1)} KB
                </p>
              </div>
            </div>
            <Button
              variant="outline"
              size="sm"
              onClick={clearFile}
            >
              <X className="h-4 w-4" />
            </Button>
          </div>
        </div>
      )}
    </Card>
  );
};