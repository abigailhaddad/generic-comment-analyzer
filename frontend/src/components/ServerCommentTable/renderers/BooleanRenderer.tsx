import React from 'react';
import { RendererProps } from '../types';

export function BooleanRenderer({ value }: RendererProps<boolean>) {
  // You might want to customize how booleans are displayed, e.g., Yes/No, True/False, icons
  return <span className={value ? 'text-green-600 font-semibold' : 'text-red-600 font-semibold'}>
    {value ? 'Yes' : 'No'}
  </span>;
} 