import TablePagination from '@/components/ui/TablePagination';

interface PaginationControlsProps {
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
  position?: 'top' | 'bottom';
  instanceId: string;
}

export default function PaginationControls({
  currentPage,
  totalPages,
  pageSize,
  setPageSize,
  totalItems,
  visibleItems,
  canPreviousPage,
  canNextPage,
  goToPage,
  previousPage,
  nextPage,
  position = 'bottom',
  instanceId
}: PaginationControlsProps) {
  return (
    // <div className="flex flex-wrap gap-4 items-center justify-end w-full flex-1">

      <TablePagination
        currentPage={currentPage}
        totalPages={totalPages}
        pageSize={pageSize}
        setPageSize={setPageSize}
        totalItems={totalItems}
        visibleItems={visibleItems}
        canPreviousPage={canPreviousPage}
        canNextPage={canNextPage}
        goToPage={goToPage}
        previousPage={previousPage}
        nextPage={nextPage}
        position={position}
        instanceId={instanceId}
      />
    // </div>
  );
} 