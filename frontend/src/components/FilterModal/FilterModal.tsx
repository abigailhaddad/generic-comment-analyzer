// frontend/src/components/FilterModal/FilterModal.tsx
import React from 'react';
import { FilterModalProps } from './types';
import { useFilterValue } from './hooks/useFilterValue';
import Modal from '@/components/ui/Modal';
import Button from '@/components/ui/Button';
import { DateFilter } from './filters/DateFilter';
import { SelectFilter } from './filters/SelectFilter';
import { TextFilter } from './filters/TextFilter';
import { ThemeFilter } from './filters/ThemeFilter';
import { CountFilter } from './filters/CountFilter';

export function FilterModal({ 
  field, 
  currentValue, 
  onApply, 
  onClose, 
  isOpen 
}: FilterModalProps) {
  const [value, setValue] = useFilterValue(currentValue, field);
  
  const renderFilter = () => {
    switch (field.filter) {
      case 'date':
        return <DateFilter value={value} onChange={setValue} field={field} />;
      
      case 'select':
        return <SelectFilter value={value} onChange={setValue} field={field} />;
      
      case 'multi-label':
        if (field.key === 'themes') {
          return <ThemeFilter value={value} onChange={setValue} field={field} />;
        }
        return <SelectFilter value={value} onChange={setValue} field={field} />;
      
      case 'text':
        return <TextFilter value={value} onChange={setValue} field={field} />;
      
      case 'count':
        return <CountFilter value={value as string | null} onChange={setValue} field={field} />;
      
      default:
        return <p>Unsupported filter type: {field.filter}</p>;
    }
  };
  
  const handleApply = () => {
    // Transform the value back to the format expected by the parent
    let finalValue = value;
    
    if (field.filter === 'select' || field.filter === 'multi-label') {
      if (value && typeof value === 'object' && 'values' in value) {
        const filterValue = value as { values: string[]; mode?: string };
        if (field.key === 'themes' && filterValue.mode) {
          // Keep the full object for themes
          finalValue = filterValue;
        } else if (filterValue.values.length === 0) {
          finalValue = null;
        } else {
          // For other select fields, just pass the values array
          finalValue = filterValue.values;
        }
      }
    } else if (field.filter === 'text') {
      if (value && typeof value === 'object' && 'values' in value) {
        const textValue = value as { values: string[] };
        finalValue = textValue.values.length > 0 ? textValue.values : null;
      }
    }
    
    onApply(finalValue);
  };
  
  const modalFooter = (
    <>
      <Button variant="secondary" onClick={onClose}>
        Cancel
      </Button>
      <Button onClick={handleApply}>
        Apply
      </Button>
    </>
  );
  
  return (
    <Modal
      isOpen={isOpen}
      onClose={onClose}
      title={`Filter by ${field.title}`}
      footer={modalFooter}
    >
      {renderFilter()}
    </Modal>
  );
}