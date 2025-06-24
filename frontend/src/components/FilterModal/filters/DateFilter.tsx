// frontend/src/components/FilterModal/filters/DateFilter.tsx
import React, { useState, useEffect } from 'react';
import { DateFilterMode, DateFilterValue, BaseFilterProps } from '../types';

const DATE_MODE_OPTIONS: Array<{ value: DateFilterMode; label: string }> = [
  { value: 'exact', label: 'Exact Date' },
  { value: 'range', label: 'Date Range' },
  { value: 'before', label: 'Before Date' },
  { value: 'after', label: 'After Date' },
];

const DATE_MODE_DESCRIPTIONS: Record<DateFilterMode, string> = {
  exact: 'Filter for comments on this exact date',
  range: 'Filter for comments between these dates (inclusive)',
  before: 'Filter for comments before this date (exclusive)',
  after: 'Filter for comments after this date (exclusive)',
};

export function DateFilter({ value, onChange }: BaseFilterProps) {
  const currentValue = value as DateFilterValue | null;
  
  const [mode, setMode] = useState<DateFilterMode>(currentValue?.mode || 'range');
  const [startDate, setStartDate] = useState(currentValue?.startDate || '');
  const [endDate, setEndDate] = useState(currentValue?.endDate || '');

  useEffect(() => {
    if (currentValue) {
      setMode(currentValue.mode || 'range');
      setStartDate(currentValue.startDate || '');
      setEndDate(currentValue.endDate || '');
    }
  }, [currentValue]);

  useEffect(() => {
    const dateValue: DateFilterValue = {
      mode,
      startDate: (mode === 'exact' || mode === 'after') ? startDate : 
               mode === 'range' ? startDate : undefined,
      endDate: mode === 'exact' ? startDate :
              mode === 'before' ? endDate :
              mode === 'range' ? endDate : undefined
    };
    
    if (dateValue.startDate || dateValue.endDate) {
      onChange(dateValue);
    } else {
      onChange(null);
    }
  }, [mode, startDate, endDate, onChange]);

  const renderDateInputs = () => {
    switch (mode) {
      case 'exact':
        return (
          <div className="mb-4">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Date
            </label>
            <input
              type="date"
              className="w-full px-3 py-2 border border-gray-300 rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              value={startDate}
              onChange={(e) => setStartDate(e.target.value)}
            />
          </div>
        );
      
      case 'range':
        return (
          <>
            <div className="mb-4">
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Start Date
              </label>
              <input
                type="date"
                className="w-full px-3 py-2 border border-gray-300 rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
              />
            </div>
            <div className="mb-4">
              <label className="block text-sm font-medium text-gray-700 mb-2">
                End Date
              </label>
              <input
                type="date"
                className="w-full px-3 py-2 border border-gray-300 rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
                min={startDate}
              />
            </div>
          </>
        );
      
      case 'before':
        return (
          <div className="mb-4">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Before Date
            </label>
            <input
              type="date"
              className="w-full px-3 py-2 border border-gray-300 rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              value={endDate}
              onChange={(e) => setEndDate(e.target.value)}
            />
          </div>
        );
      
      case 'after':
        return (
          <div className="mb-4">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              After Date
            </label>
            <input
              type="date"
              className="w-full px-3 py-2 border border-gray-300 rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              value={startDate}
              onChange={(e) => setStartDate(e.target.value)}
            />
          </div>
        );
    }
  };

  return (
    <div>
      <div className="mb-4">
        <label className="block text-sm font-medium text-gray-700 mb-2">
          Filter Mode
        </label>
        <select
          className="w-full px-3 py-2 border border-gray-300 rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          value={mode}
          onChange={(e) => setMode(e.target.value as DateFilterMode)}
        >
          {DATE_MODE_OPTIONS.map(option => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
      </div>
      
      {renderDateInputs()}
      
      <div className="text-xs text-gray-500 mt-2">
        {DATE_MODE_DESCRIPTIONS[mode]}
      </div>
    </div>
  );
}