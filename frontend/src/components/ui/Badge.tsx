'use client';

import React from 'react';

export type BadgeType = 'success' | 'danger' | 'warning' | 'primary' | 'default';

// Helper function to determine badge type from a class string
export function getBadgeTypeFromClass(badgeClass?: string): BadgeType {
  if (!badgeClass) return 'default';
  if (badgeClass.includes('success')) return 'success';
  if (badgeClass.includes('danger')) return 'danger';
  if (badgeClass.includes('warning')) return 'warning';
  if (badgeClass.includes('primary')) return 'primary';
  return 'default';
}

interface BadgeProps {
  type?: BadgeType;
  label: string;
  className?: string;
  highlight?: string;
  filterType?: string;
  id?: string;
}

export default function Badge({ 
  type = 'default', 
  label, 
  className = '', 
  highlight = '',
  filterType,
  id
}: BadgeProps) {
  // Get badge color class based on type
  const getBadgeColorClass = (type: BadgeType): string => {
    switch (type) {
      case 'success':
        return 'bg-emerald-100 text-emerald-700';
      case 'danger':
        return 'bg-rose-100 text-rose-700';
      case 'warning':
        return 'bg-amber-100 text-amber-700';
      case 'primary':
        return 'bg-slate-100 text-slate-700';
      default:
        return 'bg-slate-100 text-slate-700';
    }
  };

  // Get highlight color based on filter type
  const getHighlightColor = (filterType?: string): string => {
    if (!filterType) return 'bg-yellow-200 text-gray-900';
    
    switch (filterType) {
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
  };

  // Get semantic meaning of badge for accessibility
  const getBadgeRole = (type: BadgeType): string => {
    switch (type) {
      case 'success':
        return 'Success status';
      case 'danger':
        return 'Error status';
      case 'warning':
        return 'Warning status';
      case 'primary':
        return 'Information';
      default:
        return 'Status';
    }
  };

  // Highlight search match in text if highlight string is provided
  const highlightSearchMatch = (text: string, searchTerm: string) => {
    if (!searchTerm || !text) return text;
    
    // Escape special characters in the search term
    const escapedSearchTerm = searchTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const regex = new RegExp(`(${escapedSearchTerm})`, 'gi');
    
    // Split text by search term matches
    const parts = text.split(regex);
    
    if (parts.length <= 1) return text;
    
    // Get the correct highlight color
    const highlightClass = getHighlightColor(filterType);
    
    // Return text with highlighted spans
    return (
      <>
        {parts.map((part, i) => 
          regex.test(part) ? (
            <span 
              key={i} 
              className={`${highlightClass} font-medium px-1 rounded`}
              data-highlighted="true"
            >
              {part}
            </span>
          ) : (
            part
          )
        )}
      </>
    );
  };

  return (
    <span 
      className={`inline-block px-2 py-1 text-xs font-medium rounded-full ${getBadgeColorClass(type)} ${className}`}
      role="status"
      aria-label={`${getBadgeRole(type)}: ${label}`}
      id={id}
    >
      {highlight ? highlightSearchMatch(label, highlight) : label}
    </span>
  );
} 