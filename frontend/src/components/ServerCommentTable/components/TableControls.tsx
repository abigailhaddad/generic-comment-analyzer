import React from 'react';
import { SearchBar } from './SearchBar';
import { ColumnVisibilityMenu } from './ColumnVisibilityMenu';
import { ExportButton } from './ExportButton';

interface TableControlsProps {
  title: string;
  searchValue: string;
  onSearchChange: (value: string) => void;
  onExport: () => void;
  columnVisibility: {
    visibleColumns: Record<string, boolean>;
    toggleColumn: (key: string) => void;
    showMenu: boolean;
    setShowMenu: (show: boolean) => void;
  };
}

export function TableControls({ 
  title, 
  searchValue, 
  onSearchChange, 
  onExport, 
  columnVisibility 
}: TableControlsProps) {
  return (
    <div className="bg-gradient-to-r from-slate-50 to-slate-100 border-b border-slate-200 p-4">
      <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
        <h5 className="text-lg font-semibold text-slate-800 flex items-center">
          <span className="mr-2 opacity-60">📋</span>
          {title}
        </h5>
        
        <div className="flex flex-wrap items-center gap-2">
          <SearchBar value={searchValue} onChange={onSearchChange} />
          <ExportButton onClick={onExport} />
          <ColumnVisibilityMenu 
            visibleColumns={columnVisibility.visibleColumns}
            onToggle={columnVisibility.toggleColumn}
            isOpen={columnVisibility.showMenu}
            onToggleMenu={() => columnVisibility.setShowMenu(!columnVisibility.showMenu)}
          />
        </div>
      </div>
    </div>
  );
} 