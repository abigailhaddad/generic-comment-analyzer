// frontend/src/components/FilterModal/hooks/useFilterValue.ts
import { useState, useEffect } from 'react';
import { Field } from '@/lib/config';
import { FilterValue } from '../types';
import { normalizeFilterValue } from '../utils/filterHelpers';

export function useFilterValue(
  initialValue: unknown,
  field: Field
): [FilterValue | null, (value: FilterValue | null) => void] {
  const [value, setValue] = useState<FilterValue | null>(() => {
    return normalizeFilterValue(initialValue, field) as FilterValue | null;
  });

  useEffect(() => {
    setValue(normalizeFilterValue(initialValue, field) as FilterValue | null);
  }, [initialValue, field]);

  return [value, setValue];
}