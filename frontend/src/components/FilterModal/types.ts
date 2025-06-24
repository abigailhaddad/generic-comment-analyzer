// frontend/src/components/FilterModal/types.ts
import { Field } from '@/lib/config';

export type FilterMode = 'exact' | 'includes' | 'at_least';
export type DateFilterMode = 'exact' | 'range' | 'before' | 'after';

export interface DateFilterValue {
  mode: DateFilterMode;
  startDate?: string;
  endDate?: string;
}

export interface MultiSelectFilterValue {
  values: string[];
  mode?: FilterMode;
}

export interface TextFilterValue {
  values: string[];
}

export type FilterValue = string | string[] | DateFilterValue | MultiSelectFilterValue | TextFilterValue;

export interface BaseFilterProps {
  value: FilterValue | null;
  onChange: (value: FilterValue | null) => void;
  field: Field;
}

export interface FilterModalProps {
  field: Field;
  currentValue: unknown;
  onApply: (value: unknown) => void;
  onClose: () => void;
  isOpen: boolean;
}