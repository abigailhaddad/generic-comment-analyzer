import React from 'react';
import { RendererProps } from '../types';
import Badge from '@/components/ui/Badge';

interface CountRendererProps extends RendererProps<number | null | undefined> {
  value: number | null | undefined;
}

// Helper function to convert count to range
export function getCountRange(value: number): string {
  if (value === 1) {
    return '1';
  } else if (value >= 2 && value <= 10) {
    return '2-10';
  } else if (value >= 11 && value <= 50) {
    return '11-50';
  } else {
    return '50+';
  }
}

export function CountRenderer({ field, value }: CountRendererProps) {
  // Handle null/undefined - default to 1 if not provided
  const numValue = value ?? 1;
  
  const rangeKey = getCountRange(numValue);
  
  // Type-safe way to get the color class
  let colorClass = 'bg-gray-100';
  if (field.badges && rangeKey in field.badges) {
    colorClass = field.badges[rangeKey as keyof typeof field.badges] || 'bg-gray-100';
  }
  
  return (
    <Badge 
      className={colorClass}
      label={rangeKey}
      type="default"
    />
  );
}