import { useState, useCallback, useMemo, useEffect } from 'react';
import { Field } from '@/lib/config';

interface UseColumnVisibilityReturn {
  visibleColumns: Record<string, boolean>;
  visibleFields: Field[];
  toggleColumn: (key: string) => void;
  showMenu: boolean;
  setShowMenu: (show: boolean) => void;
}

export function useColumnVisibility(fields: Field[]): UseColumnVisibilityReturn {
  const [visibleColumns, setVisibleColumns] = useState<Record<string, boolean>>(() => {
    const initial: Record<string, boolean> = {};
    fields.forEach(field => {
      initial[field.key] = field.visible !== false;
    });
    return initial;
  });
  
  const [showMenu, setShowMenu] = useState(false);
  
  const toggleColumn = useCallback((key: string) => {
    setVisibleColumns(prev => ({
      ...prev,
      [key]: !prev[key]
    }));
  }, []);
  
  const visibleFields = useMemo(() => 
    fields.filter(field => visibleColumns[field.key]),
    [fields, visibleColumns]
  );
  
  useEffect(() => {
    if (!showMenu) return;
    
    const handleClickOutside = (e: MouseEvent) => {
      const target = e.target as Element;
      // Ensure the click is not on an element with class 'column-visibility-menu' or its children
      if (target.closest && !target.closest('.column-visibility-menu')) {
        setShowMenu(false);
      }
    };
    
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [showMenu]);
  
  return {
    visibleColumns,
    visibleFields,
    toggleColumn,
    showMenu,
    setShowMenu
  };
} 