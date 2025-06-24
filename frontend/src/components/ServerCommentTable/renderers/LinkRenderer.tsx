import React from 'react';
import { RendererProps } from '../types';

export function LinkRenderer({ value }: RendererProps<string>) {
  // Basic link renderer, assumes value is a valid URL
  // You might want to add validation or display text customization
  return (
    <a 
      href={value} 
      target="_blank" 
      rel="noopener noreferrer" 
      className="text-blue-600 hover:text-blue-800 hover:underline"
    >
      {value} {/* Or some other display text, e.g., 'View Link' */}
    </a>
  );
} 