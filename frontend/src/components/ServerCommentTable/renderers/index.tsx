// frontend/src/components/ServerCommentTable/renderers/index.ts
import React from 'react';
import { RendererProps } from '../types';
import { TitleRenderer } from './TitleRenderer';
import { BadgeRenderer } from './BadgeRenderer';
import { BooleanRenderer } from './BooleanRenderer';
import { DateRenderer } from './DateRenderer';
import { LinkRenderer } from './LinkRenderer';
import { MultiLabelRenderer } from './MultiLabelRenderer';
import { TextRenderer } from './TextRenderer';
import { CountRenderer } from './CountRenderer';

export function createFieldRenderer(props: RendererProps): React.ReactElement {
  const { field, value } = props;
  
  // Special case for title
  if (field.key === 'title') {
    return (
      <TitleRenderer {...props} value={value as string} />
    );
  }
  
  // Handle null/undefined/empty values (except for count fields which may have 0)
  if (field.format !== 'count' && (value === null || value === undefined || value === '')) {
    return <span className="text-gray-400 italic">—</span>;
  }
  
  // Handle different field types
  if (typeof value === 'boolean') {
    return <BooleanRenderer {...props} value={value} />;
  }
  
  if (field.format === 'multi-label' && typeof value === 'string') {
    return <MultiLabelRenderer {...props} value={value} />;
  }
  
  if (field.format === 'date') {
    return <DateRenderer {...props} value={value as string | Date | null | undefined} />;
  }
  
  if (field.format === 'link' && typeof value === 'string') {
    return <LinkRenderer {...props} value={value} />;
  }
  
  if (field.format === 'count') {
    return <CountRenderer {...props} value={value as number} />;
  }
  
  if (field.badges) {
    // Regular badge handling for string values
    if (typeof value === 'string' && field.badges[value as keyof typeof field.badges]) {
      return <BadgeRenderer {...props} value={value} />;
    }
  }
  
  // Default to text renderer
  if (typeof value === 'string') {
    return <TextRenderer {...props} value={value} />;
  }
  
  // Fallback for other types
  return <span>{typeof value === 'object' ? JSON.stringify(value) : String(value)}</span>;
}