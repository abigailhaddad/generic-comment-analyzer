// frontend/src/components/FilterModal/filters/SelectFilter.tsx
import React, { useState, useMemo } from 'react';
import { BaseFilterProps } from '../types';
import { getUniqueValues } from '../utils/filterHelpers';
import Button from '@/components/ui/Button';

export function SelectFilter({ value, onChange, field }: BaseFilterProps) {
  const [searchTerm, setSearchTerm] = useState('');
  
  const options = useMemo(() => getUniqueValues(field), [field]);
  
  // Handle both array format and object format
  const selectedValues = useMemo(() => {
    if (!value) return [];
    
    // If value is already an array (from URL parsing)
    if (Array.isArray(value)) {
      return value;
    }
    
    // If value is an object with values property
    if (typeof value === 'object' && 'values' in value && Array.isArray(value.values)) {
      return value.values;
    }
    
    return [];
  }, [value]);
  
  const filteredOptions = useMemo(() => {
    if (!searchTerm) return options;
    return options.filter(option => 
      option.toLowerCase().includes(searchTerm.toLowerCase())
    );
  }, [options, searchTerm]);
  
  const handleToggle = (option: string) => {
    const newValues = selectedValues.includes(option)
      ? selectedValues.filter(v => v !== option)
      : [...selectedValues, option];
    
    onChange({ values: newValues });
  };
  
  const selectAll = () => {
    onChange({ values: filteredOptions });
  };
  
  const deselectAll = () => {
    onChange({ values: [] });
  };
  
  return (
    <div>
      <div className="mb-4">
        <input 
          type="text" 
          className="w-full px-3 py-2 border border-gray-300 rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          placeholder="Search options..."
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          autoFocus
        />
      </div>
      
      <div className="max-h-[300px] overflow-y-auto border border-gray-200 rounded p-2">
        {filteredOptions.length === 0 ? (
          <div className="text-center py-4 text-gray-500">
            No matching options
          </div>
        ) : (
          <div className="py-2">
            {filteredOptions.map(option => (
              <label 
                key={option} 
                className="flex items-center mb-2 cursor-pointer text-sm py-1 px-2 rounded hover:bg-gray-50"
              >
                <input
                  type="checkbox"
                  className="mr-2"
                  checked={selectedValues.includes(option)}
                  onChange={() => handleToggle(option)}
                />
                {option}
              </label>
            ))}
          </div>
        )}
      </div>
      
      <div className="flex gap-2 mt-2">
        <Button 
          variant="outline" 
          size="sm"
          onClick={selectAll}
        >
          Select All Visible
        </Button>
        <Button 
          variant="outline" 
          size="sm"
          onClick={deselectAll}
        >
          Deselect All
        </Button>
      </div>
    </div>
  );
}