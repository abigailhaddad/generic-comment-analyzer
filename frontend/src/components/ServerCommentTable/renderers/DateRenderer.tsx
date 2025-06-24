import React from 'react';
import { RendererProps } from '../types';

export function DateRenderer({ value }: RendererProps<string | Date | null | undefined>) {
  if (!value) {
    return <span className="text-gray-400 italic">—</span>;
  }

  let dateString = '';
  try {
    const date = new Date(value);
    if (!isNaN(date.getTime())) {
      dateString = date.toLocaleDateString(); // Or any other format you prefer
    } else {
      dateString = 'Invalid Date';
    }
  } catch (error) {
    dateString = 'Invalid Date';
    console.error(error);
  }
  
  return <span>{dateString}</span>;
} 