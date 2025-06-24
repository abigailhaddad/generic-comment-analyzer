// frontend/src/components/FilterModal/utils/filterHelpers.ts
import { Field } from '@/lib/config';

export function getUniqueValues(field: Field): string[] {
  // This would usually come from the database
  // For now, we'll use hardcoded values based on field key
  
  if (field.key === 'stance') {
    return ['For', 'Against', 'Neutral/Unclear'];
  }
  
  if (field.badges && Object.keys(field.badges).length) {
    return Object.keys(field.badges);
  }
  
  if (field.key === 'themes') {
    return [
      'Due process/employee rights',
      'Merit-based system concerns',
      'Politicization concerns',
      'Scientific integrity',
      'Institutional knowledge loss'
    ];
  }
  
  return [];
}

export function normalizeFilterValue(value: unknown, field: Field): unknown {
  // Normalize the filter value based on field type
  if (!value) return null;
  
  if (field.filter === 'select' || field.filter === 'multi-label') {
    if (typeof value === 'object' && 'values' in value) {
      return value;
    }
    if (typeof value === 'string') {
      return { values: [value] };
    }
    if (Array.isArray(value)) {
      return { values: value };
    }
    return { values: [] };
  }
  
  if (field.filter === 'text') {
    if (Array.isArray(value)) {
      return { values: value };
    }
    if (typeof value === 'string') {
      return { values: [value] };
    }
    if (typeof value === 'object' && 'values' in value) {
      return value;
    }
    return { values: [] };
  }
  
  if (field.filter === 'date') {
    if (typeof value === 'object' && 'mode' in value) {
      return value;
    }
    return { mode: 'range', startDate: '', endDate: '' };
  }
  
  return value;
}