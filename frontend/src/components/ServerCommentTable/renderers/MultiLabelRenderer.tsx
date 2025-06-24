import React from 'react';
import Badge from '@/components/ui/Badge'; // Assuming a generic Badge component
import { RendererProps } from '../types';

export function MultiLabelRenderer({ value, field, searchQuery }: RendererProps<string>) {
  // Assuming value is a comma-separated string of labels
  const labels = value.split(',').map(label => label.trim()).filter(Boolean);
  
  return (
    <div className="flex flex-wrap gap-1">
      {labels.map((label, index) => (
        <Badge 
          key={`${field.key}-${label}-${index}`}
          type="primary" // Changed from "neutral" to "primary"
          label={label}
          highlight={searchQuery} // Pass searchQuery for potential highlighting in Badge
          filterType={field.key} // Pass field key for context if Badge handles filtering
        />
      ))}
    </div>
  );
} 