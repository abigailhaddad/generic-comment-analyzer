import React, { useState, useEffect } from 'react';
import TextHighlighter from '@/components/ui/TextHighlighter';
import { RendererProps } from '../types';

export function TextRenderer({ value, field, searchQuery }: RendererProps<string>) {
  const [isSmallViewport, setIsSmallViewport] = useState(false);

  useEffect(() => {
    const updateViewportSize = () => {
      setIsSmallViewport(window.innerWidth < 1280);
    };

    updateViewportSize();
    window.addEventListener('resize', updateViewportSize);
    return () => window.removeEventListener('resize', updateViewportSize);
  }, []);

  // Calculate adjusted character limit for smaller viewports
  const getAdjustedCharLimit = () => {
    if (!field.charLimit) return undefined;
    
    // If viewport is less than 1280px, show half the text
    return isSmallViewport ? Math.floor(field.charLimit / 2) : field.charLimit;
  };

  return (
    <TextHighlighter 
      text={value}
      searchTerm={searchQuery}
      highlightType={field.key} // Pass field.key to TextHighlighter for context-specific highlighting
      charLimit={getAdjustedCharLimit()} // Pass adjusted charLimit based on viewport
      smartTruncation={field.key === 'comment'} // Example: enable smart truncation for comment field
      isSmallViewport={isSmallViewport} // Pass viewport info for search-only display logic
    />
  );
} 