import React from 'react';

interface ExportButtonProps {
  onClick: () => void;
  disabled?: boolean;
}

export function ExportButton({ onClick, disabled }: ExportButtonProps) {
  return (
    <button 
      className="flex items-center px-3 py-2 text-sm font-medium bg-white text-slate-700 rounded hover:bg-slate-50 focus:outline-none focus:ring-2 focus:ring-slate-400 focus:ring-opacity-50 border border-slate-300 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
      onClick={onClick}
      disabled={disabled}
    >
      <span className="mr-1 opacity-70">📥</span>
      Export CSV
    </button>
  );
} 