import { Comment } from '@/lib/db/schema';

export function exportToCSV(data: Comment[], filename: string = 'export.csv'): void {
  if (data.length === 0) return;

  // Get all unique keys from the data, excluding object/array types for basic CSV
  const headerSet = new Set<string>();
  
  data.forEach((item) => {
    Object.keys(item).forEach((key) => {
      const value = item[key as keyof Comment];
      // Only include non-object/array types or nulls for simple CSV export
      if (typeof value !== 'object' || value === null) {
        headerSet.add(key);
      }
    });
  });

  const headers = Array.from(headerSet);
  if (headers.length === 0) return;

  // Build CSV content
  let csv = headers.map(h => `"${h}"`).join(',') + '\n';
  
  data.forEach((item) => {
    const row = headers.map((header) => {
      const value = item[header as keyof Comment];
      
      if (value == null) return ''; // handle null or undefined by returning empty string
      if (typeof value === 'string') {
        // Escape double quotes inside cell text
        return `"${value.replace(/"/g, '""')}"`;
      }
      // For other primitive types, convert to string
      return String(value);
    });
    
    csv += row.join(',') + '\n';
  });

  // Trigger download
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  link.style.display = 'none';
  
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
} 