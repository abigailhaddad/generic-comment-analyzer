import React, { useState, useEffect } from 'react';
import { Field } from '@/lib/config';

interface CountFilterProps {
  field: Field;
  value: string | null;
  onChange: (value: string | null) => void;
}

export function CountFilter({ field, value, onChange }: CountFilterProps) {
  const [selectedValue, setSelectedValue] = useState<string | null>(value);

  useEffect(() => {
    setSelectedValue(value);
  }, [value]);

  const handleChange = (val: string | null) => {
    setSelectedValue(val);
    onChange(val);
  };

  // Count ranges
  const countRanges = ['1', '2-10', '11-50', '50+'];

  return (
    <div className="space-y-4">
      <div>
        <label htmlFor={`${field.key}-select`} className="block text-sm font-medium text-gray-700 mb-2">
          Select {field.title} Range
        </label>
        <select
          id={`${field.key}-select`}
          value={selectedValue || ''}
          onChange={(e) => handleChange(e.target.value || null)}
          className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          <option value="">All Ranges</option>
          {countRanges.map(range => (
            <option key={range} value={range}>
              {range}
            </option>
          ))}
        </select>
      </div>

      {selectedValue && (
        <div className="bg-blue-50 p-3 rounded-md">
          <p className="text-sm text-blue-700">
            Filtering by: <strong>{selectedValue}</strong>
          </p>
        </div>
      )}
    </div>
  );
}