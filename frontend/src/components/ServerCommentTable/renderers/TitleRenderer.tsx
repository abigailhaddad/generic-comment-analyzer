// frontend/src/components/ServerCommentTable/renderers/TitleRenderer.tsx
import React from 'react';
import { RendererProps } from '../types';

export function TitleRenderer({ value, comment, onAction }: RendererProps<string>) {
  const handleClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    onAction?.('click', comment);
  };
  
  return (
    <div className="flex items-center">
      <button 
        onClick={handleClick}
        className="font-medium text-blue-600 hover:text-blue-800 hover:underline text-left cursor-pointer flex items-center"
      >
        <svg 
          xmlns="http://www.w3.org/2000/svg" 
          className="h-4 w-4 mr-1 text-blue-500" 
          fill="none" 
          viewBox="0 0 24 24" 
          stroke="currentColor"
        >
          <path 
            strokeLinecap="round" 
            strokeLinejoin="round" 
            strokeWidth={2} 
            d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" 
          />
          <path 
            strokeLinecap="round" 
            strokeLinejoin="round" 
            strokeWidth={2} 
            d="M2.458 12C3.732 7.943 7.523 5 12 5c4.478 0 8.268 2.943 9.542 7-1.274 4.057-5.064 7-9.542 7-4.477 0-8.268-2.943-9.542-7z" 
          />
        </svg>
        <span className="underline">{String(value) || 'Untitled Comment'}</span>
      </button>
    </div>
  );
}