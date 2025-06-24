import React, { useState, useEffect } from 'react';
import { Comment } from '@/lib/db/schema';
import { Column, SortingState } from '../types';

interface TableContentProps {
  data: Comment[];
  columns: Column<Comment>[];
  sorting?: SortingState;
  onSort: (column: string) => void;
  onRowClick: (item: Comment) => void;
  loading: boolean;
}

export function TableContent({ 
  data, 
  columns, 
  sorting, 
  onSort, 
  onRowClick, 
  loading 
}: TableContentProps) {
  const [screenSize, setScreenSize] = useState<'mobile' | 'tablet' | 'desktop'>('desktop');

  useEffect(() => {
    const updateScreenSize = () => {
      if (window.innerWidth < 768) {
        setScreenSize('mobile');
      } else if (window.innerWidth < 1024) {
        setScreenSize('tablet');
      } else {
        setScreenSize('desktop');
      }
    };

    updateScreenSize();
    window.addEventListener('resize', updateScreenSize);
    return () => window.removeEventListener('resize', updateScreenSize);
  }, []);

  const getTableConfig = () => {
    switch (screenSize) {
      case 'mobile':
        return { 
          visibleColumns: 3,
          columnWidth: '200px', // Fixed width per column
          tableMinWidth: '600px' // 3 * 200px
        };
      case 'tablet':
        return { 
          visibleColumns: 5,
          columnWidth: '180px', // Fixed width per column
          tableMinWidth: '900px' // 5 * 180px
        };
      default:
        return { 
          visibleColumns: 7,
          columnWidth: '160px', // Fixed width per column
          tableMinWidth: '1360px' // 6 * 160px + 1 * 400px (comment column)
        };
    }
  };

  const getColumnWidth = (column: Column<Comment>) => {
    // Make comment column much wider in desktop mode
    if (screenSize === 'desktop' && column.key === 'comment') {
      return '400px'; // Much wider than base width (160px * 2.5)
    }
    
    // Use the base width for all other columns
    const baseConfig = getTableConfig();
    return baseConfig.columnWidth;
  };

  const tableConfig = getTableConfig();

  return (
    <div className="flex-1 overflow-auto relative" style={{ maxHeight: '60vh', minHeight: '400px' }}>
      {loading && <LoadingOverlay />}
      
      <table 
        className="divide-y divide-gray-200" 
        style={{ minWidth: tableConfig.tableMinWidth }}
      >
        <TableHead 
          columns={columns} 
          sorting={sorting} 
          onSort={onSort}
          getColumnWidth={getColumnWidth}
        />
        <TableBody 
          data={data} 
          columns={columns} 
          onRowClick={onRowClick}
          tableConfig={tableConfig}
          getColumnWidth={getColumnWidth}
        />
      </table>
    </div>
  );
}

function LoadingOverlay() {
  return (
    <div className="absolute inset-0 flex items-center justify-center bg-white bg-opacity-70 z-10">
      <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-blue-500" />
    </div>
  );
}

interface TableHeadProps {
  columns: Column<Comment>[];
  sorting?: SortingState;
  onSort: (column: string) => void;
  getColumnWidth: (column: Column<Comment>) => string;
}

function TableHead({ columns, sorting, onSort, getColumnWidth }: TableHeadProps) {
  return (
    <thead className="bg-gray-50">
      <tr>
        {columns.map((column) => (
          <th
            key={column.key}
            className={`px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider ${
              column.sortable ? 'cursor-pointer hover:bg-blue-50' : ''
            } ${sorting?.column === column.key ? 'text-blue-700 bg-blue-50' : 'text-gray-700'}`}
            style={{ width: getColumnWidth(column) }}
            onClick={column.sortable ? () => onSort(column.key) : undefined}
          >
            <div className="flex items-center justify-between">
              <span>{column.title}</span>
              {column.sortable && sorting?.column === column.key && (
                <SortIcon direction={sorting.direction} />
              )}
            </div>
          </th>
        ))}
      </tr>
    </thead>
  );
}

interface TableBodyProps {
  data: Comment[];
  columns: Column<Comment>[];
  onRowClick: (item: Comment) => void;
  tableConfig: { visibleColumns: number; columnWidth: string; tableMinWidth: string };
  getColumnWidth: (column: Column<Comment>) => string;
}

function TableBody({ data, columns, onRowClick,  getColumnWidth }: TableBodyProps) {
  if (data.length === 0) {
    return (
      <tbody>
        <tr>
          <td colSpan={columns.length} className="px-6 py-10 text-center text-gray-500 bg-gray-50">
            <EmptyState />
          </td>
        </tr>
      </tbody>
    );
  }

  return (
    <tbody className="bg-white divide-y divide-gray-200">
      {data.map((item) => {
        return (
          <tr
            key={item.id}
            className="cursor-pointer hover:bg-blue-50"
            onClick={() => onRowClick(item)}
          >
            {columns.map((column) => (
              <td 
                key={`${item.id}-${column.key}`} 
                className="px-4 py-3 text-sm break-words"
                style={{ width: getColumnWidth(column) }}
              >
                <div className="max-w-full">
                  {column.render(item)}
                </div>
              </td>
            ))}
          </tr>
        );
      })}
    </tbody>
  );
}

function SortIcon({ direction }: { direction: 'asc' | 'desc' }) {
  return (
    <svg className="h-4 w-4 text-blue-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      {direction === 'asc' ? (
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 15l7-7 7 7" />
      ) : (
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
      )}
    </svg>
  );
}

function EmptyState() {
  return (
    <div className="flex flex-col items-center">
      <svg className="w-12 h-12 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} 
          d="M9.172 16.172a4 4 0 015.656 0M9 10h.01M15 10h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
      </svg>
      <p className="mt-2 text-sm font-medium">No matching records found</p>
      <p className="text-xs text-gray-400 mt-1">Try changing your search criteria</p>
    </div>
  );
} 