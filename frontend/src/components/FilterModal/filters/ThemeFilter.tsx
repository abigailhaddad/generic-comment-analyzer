// frontend/src/components/FilterModal/filters/ThemeFilter.tsx
import React, { useState } from 'react';
import { BaseFilterProps, MultiSelectFilterValue, FilterMode, FilterValue } from '../types';
import { SelectFilter } from './SelectFilter';

const FILTER_MODE_OPTIONS: Array<{ value: FilterMode; label: string; description: string }> = [
  { 
    value: 'includes', 
    label: 'Must Include Any',
    description: 'Find comments that include any of the selected themes'
  },
  { 
    value: 'at_least', 
    label: 'Must Include At Least',
    description: 'Find comments that include at least all of the selected themes'
  },
  { 
    value: 'exact', 
    label: 'Exact Match',
    description: 'Find comments that match exactly these themes (no more, no less)'
  },
];

export function ThemeFilter({ value, onChange, field }: BaseFilterProps) {
  const currentValue = value as MultiSelectFilterValue | null;
  const [mode, setMode] = useState<FilterMode>(currentValue?.mode || 'includes');
  
  const handleChange = (newValue: FilterValue | null) => {
    if (newValue && typeof newValue === 'object' && 'values' in newValue) {
      // We expect newValue to be MultiSelectFilterValue from SelectFilter
      const multiSelectValue = newValue as MultiSelectFilterValue;
      onChange({ ...multiSelectValue, mode });
    } else if (newValue === null) {
      onChange(null);
    } 
    // Optionally, handle other FilterValue types if they could somehow be passed, 
    // or add an error/log if an unexpected type is received.
    // For now, we assume SelectFilter only sends MultiSelectFilterValue or null.
  };
  
  const handleModeChange = (newMode: FilterMode) => {
    setMode(newMode);
    if (currentValue) {
      onChange({ ...currentValue, mode: newMode });
    }
  };
  
  return (
    <div>
      <div className="mb-4">
        <div className="text-sm font-medium text-gray-700 mb-2">Filter Mode:</div>
        <div className="space-y-2">
          {FILTER_MODE_OPTIONS.map(option => (
            <label key={option.value} className="flex items-start cursor-pointer">
              <input
                type="radio"
                className="mt-1 mr-2"
                checked={mode === option.value}
                onChange={() => handleModeChange(option.value)}
              />
              <div className="flex-1">
                <span className="text-sm font-medium">{option.label}</span>
                <p className="text-xs text-gray-500">{option.description}</p>
              </div>
            </label>
          ))}
        </div>
      </div>
      
      <SelectFilter 
        value={currentValue} 
        onChange={handleChange} 
        field={field} 
      />
    </div>
  );
}