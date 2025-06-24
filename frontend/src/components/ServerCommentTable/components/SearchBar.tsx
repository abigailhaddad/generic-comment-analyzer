import React from 'react';
import SearchInput from '@/components/ui/SearchInput';

interface SearchBarProps {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
}

export function SearchBar({ value, onChange, placeholder = "Search comments..." }: SearchBarProps) {
  return (
    <SearchInput
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder}
      lightTheme={true}
    />
  );
} 