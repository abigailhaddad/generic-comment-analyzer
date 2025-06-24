// frontend/src/components/FilterModal/filters/TextFilter.tsx
import React, { useState, useEffect } from 'react';
import { BaseFilterProps, TextFilterValue } from '../types';
import Button from '@/components/ui/Button';

export function TextFilter({ value, onChange }: BaseFilterProps) {
  const currentValue = value as TextFilterValue | null;
  const [values, setValues] = useState<string[]>(currentValue?.values || []);
  const [inputValue, setInputValue] = useState('');
  
  useEffect(() => {
    onChange(values.length > 0 ? { values } : null);
  }, [values, onChange]);
  
  const addValue = () => {
    const trimmed = inputValue.trim();
    if (trimmed && !values.includes(trimmed)) {
      setValues([...values, trimmed]);
      setInputValue('');
    }
  };
  
  const removeValue = (valueToRemove: string) => {
    setValues(values.filter(v => v !== valueToRemove));
  };
  
  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      addValue();
    }
  };
  
  return (
    <div>
      <div className="mb-4">
        <div className="flex">
          <input
            type="text"
            className="flex-1 px-3 py-2 border border-gray-300 rounded-l text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            placeholder="Type and press Enter"
            value={inputValue}
            onChange={(e) => setInputValue(e.target.value)}
            onKeyDown={handleKeyDown}
            autoFocus
          />
          <Button 
            onClick={addValue}
            className="rounded-l-none"
          >
            Add
          </Button>
        </div>
        <div className="text-xs text-gray-500 mt-1">
          Press Enter or click Add after typing
        </div>
      </div>
      
      <div>
        {values.length === 0 ? (
          <div className="italic text-gray-400">
            No filters added yet
          </div>
        ) : (
          <div className="flex flex-wrap gap-2">
            {values.map((val, index) => (
              <div 
                key={index} 
                className="bg-gray-200 px-3 py-1 rounded-full inline-flex items-center text-sm"
              >
                {val}
                <button
                  className="ml-2 text-gray-500 hover:text-gray-700 font-bold"
                  onClick={() => removeValue(val)}
                  aria-label={`Remove ${val}`}
                >
                  ×
                </button>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}