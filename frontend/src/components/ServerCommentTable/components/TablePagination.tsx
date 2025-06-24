import React from 'react';
import PaginationControls from './PaginationControls'; // Assuming PaginationControls will be moved here

interface TablePaginationProps {
  currentPage: number;
  totalPages: number;
  pageSize: number;
  setPageSize: (size: number) => void;
  totalItems: number;
  visibleItems: number;
  canPreviousPage: boolean;
  canNextPage: boolean;
  goToPage: (page: number) => void;
  previousPage: () => void;
  nextPage: () => void;
  loading?: boolean;
  position: 'top' | 'bottom';
  instanceId: string; // Added instanceId based on expected props for PaginationControls
}

export function TablePagination(props: TablePaginationProps) {
  const { position, visibleItems, ...paginationProps } = props;
  
  // Don't render top pagination if no items and it's the top position
  if (position === 'top' && visibleItems === 0 && props.totalItems > 0) {
    // Still render if there are total items but current page has 0 (e.g. after filtering)
  } else if (position === 'top' && props.totalItems === 0) {
    return null;
  }
  
  return (
    <div className={`${position === 'top' ? 'border-b' : 'border-t'} border-gray-200`}>
      <PaginationControls {...paginationProps} instanceId={`${props.instanceId}-${position}`} visibleItems={visibleItems} />
    </div>
  );
} 