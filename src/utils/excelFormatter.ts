import * as XLSX from 'xlsx';

export function forceEANText(ws: XLSX.WorkSheet, headerRowIndex = 0): void {
  if (!ws['!ref']) return;
  const range = XLSX.utils.decode_range(ws['!ref']);

  // trova la colonna 'EAN' nell'header
  let eanCol = -1;
  for (let C = range.s.c; C <= range.e.c; C++) {
    const addr = XLSX.utils.encode_cell({ r: headerRowIndex, c: C });
    const cell = ws[addr];
    const name = (cell?.v ?? '').toString().trim().toLowerCase();
    if (name === 'ean') { 
      eanCol = C; 
      break; 
    }
  }
  if (eanCol < 0) return;

  // forza tutte le celle dati della colonna a stringa + formato testo
  for (let R = headerRowIndex + 1; R <= range.e.r; R++) {
    const addr = XLSX.utils.encode_cell({ r: R, c: eanCol });
    const cell = ws[addr];
    if (!cell) continue;

    cell.v = (cell.v ?? '').toString(); // preserva 0 iniziali
    cell.t = 's';
    cell.z = '@';
    ws[addr] = cell;
  }
}

export function exportDiscardedRowsCSV(discarded: any[], filename: string = 'righe_scartate_EAN'): void {
  if (discarded.length === 0) return;
  
  const worksheet = XLSX.utils.json_to_sheet(discarded);
  const workbook = XLSX.utils.book_new();
  
  XLSX.utils.book_append_sheet(workbook, worksheet, 'Righe Scartate');
  XLSX.writeFile(workbook, `${filename}.csv`);
}