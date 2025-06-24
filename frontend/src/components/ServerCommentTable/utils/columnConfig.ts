import { Field } from '@/lib/config';
import { ColumnConfig } from '../types';

const COLUMN_WIDTHS: Record<string, string> = {
  comment: 'w-1/2 max-w-3xl',
  keyQuote: 'w-1/6',
  rationale: 'w-1/6',
  themes: 'w-1/8',
  title: 'w-1/8',
  stance: 'w-24',
  category: 'w-32',
  postedDate: 'w-28',
  receivedDate: 'w-28',
  link: 'w-20'
};

export function getColumnConfig(field: Field): ColumnConfig {
  return {
    key: field.key,
    className: COLUMN_WIDTHS[field.key] || '',
    sortable: true // Defaulting to true as per useTableColumns logic
  };
} 