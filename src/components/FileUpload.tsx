import React, { useCallback } from 'react';
import { useDropzone } from 'react-dropzone';
import { Upload, FileText, X } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';

interface FileUploadProps {
  onFileSelect: (file: File) => void;
  selectedFile?: File | null;
  title: string;
  accept?: string;
}

export const FileUpload: React.FC<FileUploadProps> = ({
  onFileSelect,
  selectedFile,
  title,
  accept = '.csv'
}) => {
  const onDrop = useCallback((acceptedFiles: File[]) => {
    if (acceptedFiles.length > 0) {
      onFileSelect(acceptedFiles[0]);
    }
  }, [onFileSelect]);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'text/csv': ['.csv'],
      'application/vnd.ms-excel': ['.csv']
    },
    multiple: false
  });

  const removeFile = () => {
    onFileSelect(null as any);
  };

  return (
    <Card className="p-6">
      <h3 className="text-lg font-semibold text-card-foreground mb-4">{title}</h3>
      
      {!selectedFile ? (
        <div
          {...getRootProps()}
          className={`border-2 border-dashed rounded-lg p-8 text-center cursor-pointer transition-all duration-300 ${
            isDragActive 
              ? 'border-primary bg-accent/50 shadow-glow' 
              : 'border-border hover:border-primary hover:bg-accent/30'
          }`}
        >
          <input {...getInputProps()} />
          <Upload className="mx-auto h-12 w-12 text-muted-foreground mb-4" />
          {isDragActive ? (
            <p className="text-primary font-medium">Rilascia il file CSV qui...</p>
          ) : (
            <div>
              <p className="text-foreground font-medium mb-2">
                Trascina il file CSV qui o clicca per selezionare
              </p>
              <p className="text-muted-foreground text-sm">
                Supporta solo file .csv
              </p>
            </div>
          )}
        </div>
      ) : (
        <div className="flex items-center justify-between p-4 bg-accent rounded-lg">
          <div className="flex items-center gap-3">
            <FileText className="h-6 w-6 text-primary" />
            <div>
              <p className="font-medium text-card-foreground">{selectedFile.name}</p>
              <p className="text-sm text-muted-foreground">
                {(selectedFile.size / 1024).toFixed(2)} KB
              </p>
            </div>
          </div>
          <Button
            variant="ghost"
            size="sm"
            onClick={removeFile}
            className="text-destructive hover:text-destructive"
          >
            <X className="h-4 w-4" />
          </Button>
        </div>
      )}
    </Card>
  );
};