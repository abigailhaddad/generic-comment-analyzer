import { Comment } from '@/lib/db/schema';
import { Field } from '@/lib/config';
import React from 'react';

export interface TableConfig {
  fields: Field[];
  searchQuery: string;
  onRowClick: (comment: Comment) => void;
}

export interface ColumnConfig {
  key: string;
  width?: string;
  className?: string;
  sortable?: boolean;
}

export interface RendererProps<T = unknown> {
  value: T;
  field: Field;
  comment: Comment;
  searchQuery?: string;
  onAction?: (action: string, data?: Comment | string | number | boolean | null) => void;
}

export interface Column<TItem> {
  key: string;
  title: string;
  sortable?: boolean;
  className?: string;
  render: (item: TItem) => React.ReactNode;
}

export interface SortingState {
  column: string;
  direction: 'asc' | 'desc';
}

export interface TableHeaderProps {
  searchQuery: string;
  onSearchChange: (value: string) => void;
  visibleColumns: Record<string, boolean>;
  onColumnToggle: (key: string) => void;
  onExport: () => void;
} 