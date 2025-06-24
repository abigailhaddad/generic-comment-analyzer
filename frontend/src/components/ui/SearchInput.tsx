'use client';

import React from 'react';

interface SearchInputProps {
  value: string;
  onChange: (e: React.ChangeEvent<HTMLInputElement>) => void;
  placeholder?: string;
  className?: string;
  lightTheme?: boolean;
  id?: string;
  ariaLabel?: string;
}

export default function SearchInput({
  value,
  onChange,
  placeholder = 'Search...',
  className = '',
  lightTheme = false,
  id = 'search-input',
  ariaLabel = 'Search'
}: SearchInputProps) {
  const baseInputClasses = "placeholder-opacity-70 border rounded py-2 pl-10 pr-4 focus:outline-none focus:ring-2";
  const themeClasses = lightTheme 
    ? "bg-white text-slate-700 placeholder-slate-500 border-slate-300 focus:ring-slate-400 focus:ring-opacity-50 focus:border-slate-400"
    : "bg-white bg-opacity-20 text-white placeholder-white border-transparent focus:ring-white focus:ring-opacity-40 focus:border-transparent";
  
  return (
    <div className={`relative ${className}`}>
      <div className={`absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none ${lightTheme ? 'text-slate-400' : 'text-white'}`}>
        <svg 
          className="h-4 w-4" 
          fill="none" 
          viewBox="0 0 24 24" 
          stroke="currentColor"
          aria-hidden="true"
        >
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
        </svg>
      </div>
      <input
        id={id}
        type="search"
        className={`${baseInputClasses} ${themeClasses}`}
        placeholder={placeholder}
        value={value}
        onChange={onChange}
        aria-label={ariaLabel}
        role="searchbox"
      />
    </div>
  );
} 