'use client';

import React from 'react';

/**
 * Props for the TextHighlighter component
 */
interface TextHighlighterProps {
  /**
   * The text to be displayed and potentially highlighted
   */
  text: string;
  
  /**
   * The search term to highlight within the text
   */
  searchTerm?: string;
  
  /**
   * Optional type to determine highlight color
   */
  highlightType?: string;
  
  /**
   * Character limit for truncating text
   */
  charLimit?: number;
  
  /**
   * Whether to use smart truncation for search matches
   */
  smartTruncation?: boolean;
  
  /**
   * Whether the viewport is small (< 1280px)
   */
  isSmallViewport?: boolean;
}

/**
 * A component for displaying text with highlighted search matches
 * and smart truncation to ensure search matches are visible
 */
export default function TextHighlighter({
  text,
  searchTerm = '',
  highlightType,
  charLimit,
  smartTruncation = true,
  isSmallViewport = false
}: TextHighlighterProps) {
  // If no text or empty text, show placeholder
  if (!text || text === '') {
    return <span className="text-gray-400 italic">—</span>;
  }
  
  // Special handling for small viewport with search term - show only search results
  if (isSmallViewport && searchTerm && searchTerm.trim() !== '') {
    return <span title={text} className="cursor-help">{createSearchOnlyText(text, searchTerm, highlightType)}</span>;
  }
  
  // If no search term or character limit, just return the text
  if ((!searchTerm || searchTerm === '') && (!charLimit || text.length <= charLimit)) {
    return <>{text}</>;
  }
  
  // If we have a character limit but no search term, just truncate
  if ((!searchTerm || searchTerm === '') && charLimit && text.length > charLimit) {
    return <span title={text} className="cursor-help">{text.substring(0, charLimit)}...</span>;
  }
  
  // If we have a search term but no truncation needed, just highlight
  if (searchTerm && (!charLimit || text.length <= charLimit)) {
    return <>{highlightMatches(text, searchTerm, highlightType)}</>;
  }
  
  // If we have both a search term and need truncation
  if (searchTerm && charLimit && text.length > charLimit) {
    return smartTruncation 
      ? <span title={text} className="cursor-help">{createSmartTruncatedText(text, charLimit, searchTerm, highlightType)}</span>
      : <span title={text} className="cursor-help">{highlightMatches(`${text.substring(0, charLimit)}...`, searchTerm, highlightType)}</span>;
  }
  
  // Fallback (shouldn't reach here with proper logic above)
  return <>{text}</>;
}

/**
 * Highlight matches of searchTerm in text
 */
function highlightMatches(text: string, searchTerm: string, highlightType?: string) {
  if (!searchTerm || !text) return text;
  
  // Escape special characters in the search term
  const escapedSearchTerm = searchTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const regex = new RegExp(`(${escapedSearchTerm})`, 'gi');
  
  // Split text by search term matches
  const parts = text.split(regex);
  
  if (parts.length <= 1) return text;
  
  // Get highlight color based on field type
  const highlightClass = getHighlightColor(highlightType);
  
  // Return text with highlighted spans
  return (
    <>
      {parts.map((part, i) => 
        regex.test(part) ? (
          <span key={i} className={`${highlightClass} font-medium px-1 rounded`}>
            {part}
          </span>
        ) : (
          <span key={i}>{part}</span>
        )
      )}
    </>
  );
}

/**
 * Create text that shows only search matches with context for small viewports
 */
function createSearchOnlyText(text: string, searchTerm: string, highlightType?: string) {
  if (!searchTerm || !text) {
    return text;
  }
  
  // Find all matches with their positions
  const escapedSearchTerm = searchTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const regex = new RegExp(`(${escapedSearchTerm})`, 'gi');
  const matches = [];
  let match;
  
  while ((match = regex.exec(text)) !== null) {
    matches.push({
      index: match.index,
      length: match[0].length,
      text: match[0]
    });
    // Prevent infinite loop on zero-length matches
    if (match.index === regex.lastIndex) {
      regex.lastIndex++;
    }
  }
  
  if (matches.length === 0) {
    // No matches found, show truncated text
    return `${text.substring(0, 100)}...`;
  }
  
  // Show only the first few matches with context
  const contextSize = 40; // Characters of context around each match
  const maxMatches = 2; // Maximum number of matches to show
  const segments = [];
  
  for (let i = 0; i < Math.min(matches.length, maxMatches); i++) {
    const matchObj = matches[i];
    const start = Math.max(0, matchObj.index - contextSize);
    const end = Math.min(text.length, matchObj.index + matchObj.length + contextSize);
    
    let segment = text.substring(start, end);
    
    // Add ellipsis if we're not at the beginning/end
    if (start > 0) segment = '...' + segment;
    if (end < text.length) segment = segment + '...';
    
    segments.push(<span key={`match-${i}`}>{highlightMatches(segment, searchTerm, highlightType)}</span>);
    
    // Add separator between segments if there are multiple
    if (i < Math.min(matches.length, maxMatches) - 1) {
      segments.push(<span key={`sep-${i}`} className="text-gray-500 mx-2 font-bold">•••</span>);
    }
  }
  
  return <>{segments}</>;
}

/**
 * Create smart truncated text that ensures search matches are visible
 */
function createSmartTruncatedText(text: string, limit: number, searchTerm: string, highlightType?: string) {
  if (text.length <= limit) {
    return highlightMatches(text, searchTerm, highlightType);
  }
  
  // If no search term or empty search term, just truncate
  if (!searchTerm) {
    return `${text.substring(0, limit)}...`;
  }
  
  // Find the first search match
  const matchIndex = findFirstMatchIndex(text, searchTerm);
  
  // If no match or match is within visible area, show normal truncation with highlighting
  if (matchIndex === -1 || matchIndex < limit) {
    return highlightMatches(`${text.substring(0, limit)}...`, searchTerm, highlightType);
  }
  
  // If match is outside visible area, show context around match
  const contextSize = Math.floor(limit / 2);
  const matchStart = Math.max(0, matchIndex - (contextSize / 2));
  const matchEnd = Math.min(text.length, matchStart + contextSize);
  
  // Create segments
  const firstSegment = text.substring(0, Math.floor(limit / 3));
  const matchSegment = text.substring(matchStart, matchEnd);
  
  return (
    <>
      {highlightMatches(firstSegment, searchTerm, highlightType)}
      <span className="text-gray-500 mx-1 font-bold">...</span>
      {highlightMatches(matchSegment, searchTerm, highlightType)}
      {matchEnd < text.length ? '...' : ''}
    </>
  );
}

/**
 * Find the index of the first search match in text
 */
function findFirstMatchIndex(text: string, searchTerm: string): number {
  if (!searchTerm || !text) return -1;
  
  const escapedSearchTerm = searchTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const regex = new RegExp(escapedSearchTerm, 'gi');
  const match = regex.exec(text);
  
  return match ? match.index : -1;
}

/**
 * Get highlight color based on field type
 */
function getHighlightColor(highlightType?: string): string {
  if (!highlightType) return 'bg-yellow-200 text-gray-900';
  
  switch (highlightType) {
    case 'title':
      return 'bg-blue-200 text-blue-900';
    case 'comment':
      return 'bg-green-200 text-green-900';
    case 'themes':
      return 'bg-purple-200 text-purple-900';
    case 'keyQuote':
      return 'bg-orange-200 text-orange-900';
    case 'rationale':
      return 'bg-pink-200 text-pink-900';
    case 'stance':
      return 'bg-indigo-200 text-indigo-900';
    default:
      return 'bg-yellow-200 text-gray-900';
  }
}