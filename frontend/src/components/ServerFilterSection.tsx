// components/ServerFilterSection.tsx
'use client';

import { useState } from 'react';
import { Field, datasetConfig } from '@/lib/config';
import FilterModal from './FilterModal';
import { useServerDataContext } from '@/contexts/ServerDataContext';
import Card from '@/components/ui/Card';
import SearchInput from '@/components/ui/SearchInput';

export default function ServerFilterSection() {
  // Get filters, setFilters, searchQuery, and setSearchQuery from the context
  const { filters, setFilters, searchQuery, setSearchQuery } = useServerDataContext();
  
  const [showFilterModal, setShowFilterModal] = useState(false);
  const [activeField, setActiveField] = useState<Field | null>(null);
  
  // Handle filter change
  const handleFilterChange = (key: string, value: unknown) => {
    const newFilters = { ...filters };
    
    // If value is empty or empty array, remove the filter
    if (!value ||
        (Array.isArray(value) && value.length === 0) ||
        (value && typeof value === 'object' && 'values' in value && Array.isArray(value.values) && value.values.length === 0)) {
      delete newFilters[key];
    } else {
      newFilters[key] = value;
    }
    
    // Update filters in the context
    setFilters(newFilters);
  };
  
  // Clear all filters
  const clearAllFilters = () => {
    setFilters({});
  };
  
  // Open filter modal
  const openFilterModal = (field: Field) => {
    setActiveField(field);
    // Set showFilterModal after activeField is set to ensure proper modal rendering
    setShowFilterModal(true);
  };
  
  // Close filter modal
  const closeFilterModal = () => {
    setShowFilterModal(false);
    setActiveField(null);
  };
  
  // Check if we have any active filters
  const hasActiveFilters = Object.keys(filters).length > 0;
  
  // Helper to render filter tags for active filters
  const renderFilterTags = () => {
    if (!hasActiveFilters) {
      return (
        <div className="text-center py-4 bg-slate-50 rounded-lg border border-slate-200">
          <div className="text-slate-700 font-medium mb-1">No filters applied</div>
          <div className="text-slate-600 text-sm">Use the filter buttons below to refine your data view</div>
        </div>
      );
    }
    
    return Object.entries(filters).map(([key, value]) => {
      // Find matching field
      const field = datasetConfig.fields.find(f => f.key === key);
      
      if (!field) return null;
      
      // Handle different filter types
      // For themes with filter mode object: { values: ["Transparency", "Accountability"], mode: "includes" }
      if (value && typeof value === 'object' && 'values' in value && Array.isArray(value.values)) {
        const filterValue = value as { values: string[], mode: 'exact' | 'includes' | 'at_least' };
        let modeText = '(Any Match)';
        if (filterValue.mode === 'at_least') {
          modeText = '(Must Include All)';
        } else if (filterValue.mode === 'exact') {
          modeText = '(Exact Match)';
        }
        
        return filterValue.values.map((v, i) => (
          <div 
            key={`${key}-${i}`}
            className="bg-slate-100 text-slate-700 rounded-full px-3 py-1.5 mr-2 mb-2 text-sm inline-flex items-center shadow-sm hover:bg-slate-200 transition-colors border border-slate-200"
          >
            <span className="mr-1 font-medium">{field.title} {i === 0 ? modeText : ''}:</span> 
            <span>{v}</span>
            <button 
              className="ml-2 inline-flex items-center justify-center w-5 h-5 bg-slate-700 bg-opacity-10 rounded-full text-xs hover:bg-opacity-20 transition-colors"
              onClick={() => {
                // Create new values array without this value
                const newValues = (filterValue.values).filter(item => item !== v);
                if (newValues.length === 0) {
                  // If removing the last value, remove the filter entirely
                  handleFilterChange(key, null);
                } else {
                  // Otherwise update the values array while keeping the mode
                  handleFilterChange(key, {
                    values: newValues,
                    mode: filterValue.mode
                  });
                }
              }}
              aria-label="Remove filter"
            >
              ×
            </button>
          </div>
        ));
      }
      // For array values like themes: ["Transparency", "Accountability"]
      // This creates multiple filter tags, one for each array value
      else if (Array.isArray(value)) {
        return (value as string[]).map((v, i) => (
          <div 
            key={`${key}-${i}`}
            className="bg-slate-100 text-slate-700 rounded-full px-3 py-1.5 mr-2 mb-2 text-sm inline-flex items-center shadow-sm hover:bg-slate-200 transition-colors border border-slate-200"
          >
            <span className="mr-1 font-medium">{field.title}:</span> 
            <span>{v}</span>
            <button 
              className="ml-2 inline-flex items-center justify-center w-5 h-5 bg-slate-700 bg-opacity-10 rounded-full text-xs hover:bg-opacity-20 transition-colors"
              onClick={() => handleFilterChange(key, (value as string[]).filter(item => item !== v))}
              aria-label="Remove filter"
            >
              ×
            </button>
          </div>
        ));
        //TODO: This date range filter type should be its own interface or type: {mode: 'range', startDate: '2025-05-16', endDate: '2025-05-26'}
        //TODO: It also looks like after date doesn't quite work...
      } else if (typeof value === 'object' && value !== null && ('startDate' in value || 'endDate' in value)) {
        // Handle date filter objects with mode
        const dateFilter = value as { mode?: string, startDate?: string, endDate?: string };
        let dateString = '';
        
        // Helper function to format date without timezone issues
        const formatDateSafe = (dateStr: string) => {
          // Parse as YYYY-MM-DD and create date in local timezone
          const [year, month, day] = dateStr.split('-').map(Number);
          const date = new Date(year, month - 1, day); // month is 0-indexed
          return date.toLocaleDateString();
        };

        if (dateFilter.mode === 'exact' && dateFilter.startDate) {
          dateString = formatDateSafe(dateFilter.startDate);
        } else if (dateFilter.mode === 'range' && dateFilter.startDate && dateFilter.endDate) {
          dateString = `${formatDateSafe(dateFilter.startDate)} - ${formatDateSafe(dateFilter.endDate)}`;
        } else if (dateFilter.mode === 'before' && dateFilter.endDate) {
          dateString = `Before ${formatDateSafe(dateFilter.endDate)}`;
        } else if (dateFilter.mode === 'after' && dateFilter.startDate) {
          dateString = `After ${formatDateSafe(dateFilter.startDate)}`;
        } else {
          // Fallback for legacy format without mode
          if (dateFilter.startDate && dateFilter.endDate) {
            dateString = `${formatDateSafe(dateFilter.startDate)} - ${formatDateSafe(dateFilter.endDate)}`;
          } else if (dateFilter.startDate) {
            dateString = `From ${formatDateSafe(dateFilter.startDate)}`;
          } else if (dateFilter.endDate) {
            dateString = `Until ${formatDateSafe(dateFilter.endDate)}`;
          }
        }

        return (
          <div
            key={key}
            className="bg-slate-100 text-slate-700 rounded-full px-3 py-1.5 mr-2 mb-2 text-sm inline-flex items-center shadow-sm hover:bg-slate-200 transition-colors border border-slate-200"
          >
            <span className="mr-1 font-medium">{field.title}:</span>
            <span>{dateString}</span>
            <button
              className="ml-2 inline-flex items-center justify-center w-5 h-5 bg-slate-700 bg-opacity-10 rounded-full text-xs hover:bg-opacity-20 transition-colors"
              onClick={() => handleFilterChange(key, null)}
              aria-label="Remove filter"
            >
              ×
            </button>
          </div>
        );
      } else {
        // Handle other non-array, non-date-range values
        // For stance: "For", this creates a single filter tag "Stance: For"
        return (
          <div 
            key={key}
            className="bg-slate-100 text-slate-700 rounded-full px-3 py-1.5 mr-2 mb-2 text-sm inline-flex items-center shadow-sm hover:bg-slate-200 transition-colors border border-slate-200"
          >
            <span className="mr-1 font-medium">{field.title}:</span> 
            <span>{String(value)}</span>
            <button 
              className="ml-2 inline-flex items-center justify-center w-5 h-5 bg-slate-700 bg-opacity-10 rounded-full text-xs hover:bg-opacity-20 transition-colors"
              onClick={() => handleFilterChange(key, null)}
              aria-label="Remove filter"
            >
              ×
            </button>
          </div>
        );
      }
    });
  };
  
  // Render available filters as buttons
  const renderFilterButtons = () => {
    const filterableFields = datasetConfig.fields.filter(field => field.filter);
    
    return (
      <div className="flex flex-wrap gap-2 mt-4">
        {filterableFields.map(field => (
          <button 
            key={field.key}
            className={`px-3 py-2 rounded-md text-sm font-medium transition-colors 
              ${filters[field.key] 
                ? 'bg-slate-700 text-white hover:bg-slate-800 border border-slate-700' 
                : 'bg-white text-slate-700 hover:bg-slate-50 border border-slate-300'}`}
            onClick={() => openFilterModal(field)}
          >
            {field.title}
          </button>
        ))}
      </div>
    );
  };
  
  return (
    <Card collapsible={true} initiallyCollapsed={false}>
      <Card.Header className="bg-gradient-to-r from-slate-50 to-slate-100 border-b border-slate-200">
        <div className="flex justify-between items-center w-full">
          <h5 className="text-lg font-semibold text-slate-800 flex items-center">
            <span className="mr-2 opacity-60">{hasActiveFilters ? '🔍' : '⚙️'}</span>
            {hasActiveFilters ? 'Active Filters' : 'Filters'}
          </h5>
          {hasActiveFilters && (
            <button 
              className="bg-white hover:bg-slate-50 text-slate-700 text-sm px-3 py-1 rounded transition-colors border border-slate-200"
              onClick={clearAllFilters}
            >
              Clear All
            </button>
          )}
        </div>
      </Card.Header>
      <Card.Body className="p-4">
        <div className={`flex flex-wrap mb-4 ${hasActiveFilters ? '' : 'hidden'}`}>
          {renderFilterTags()}
        </div>
        
        {!hasActiveFilters && renderFilterTags()}
        
        <div className="mt-4 pt-4 border-t border-gray-200">
          <h6 className="font-medium text-gray-700 mb-2">Filter Options</h6>
          {renderFilterButtons()}
        </div>
        
        <div className="mt-4 pt-4 border-t border-gray-200">
          <h6 className="font-medium text-gray-700 mb-2">Search</h6>
          <SearchInput
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            placeholder="Search comments..."
            lightTheme={true}
          />
          {searchQuery && (
            <div className="mt-2 text-sm text-gray-600">
              Searching for: <span className="font-medium">{searchQuery}</span>
              <button
                className="ml-2 text-slate-600 hover:text-slate-800"
                onClick={() => setSearchQuery('')}
              >
                Clear
              </button>
            </div>
          )}
        </div>
      </Card.Body>
      
      {/* Filter Modal - only render when showFilterModal is true */}
      {activeField && (
        <FilterModal 
          field={activeField}
          currentValue={filters[activeField.key] || null}
          onClose={closeFilterModal}
          onApply={(value) => {
            handleFilterChange(activeField.key, value);
            closeFilterModal();
          }}
          isOpen={showFilterModal}
        />
      )}
    </Card>
  );
}