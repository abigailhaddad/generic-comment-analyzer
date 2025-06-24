import { renderHook, act } from '@testing-library/react';
import { useDataTable } from '@/lib/hooks/useDataTable';
import { useRouter, useSearchParams, usePathname } from 'next/navigation';

// Mock the Next.js navigation hooks
jest.mock('next/navigation', () => ({
  useRouter: jest.fn(),
  useSearchParams: jest.fn(),
  usePathname: jest.fn(),
}));

describe('useDataTable', () => {
  const mockReplace = jest.fn();
  const mockGet = jest.fn();
  const mockPathname = '/test-path';
  
  // Simple test data
  const testData = [
    { id: '1', title: 'Test 1', comment: 'Comment A' },
    { id: '2', title: 'Test 2', comment: 'Comment B' },
    { id: '3', title: 'Test 3', comment: 'Comment C' },
  ];
  
  beforeEach(() => {
    jest.clearAllMocks();
    
    // Setup the router mock
    (useRouter as jest.Mock).mockReturnValue({
      replace: mockReplace,
    });
    
    // Setup the searchParams mock
    (useSearchParams as jest.Mock).mockReturnValue({
      get: mockGet,
      toString: jest.fn().mockReturnValue(''),
    });
    
    // Setup the pathname mock
    (usePathname as jest.Mock).mockReturnValue(mockPathname);
  });
  
  test('search query should update URL parameters', () => {
    const { result } = renderHook(() => useDataTable({
      data: testData,
    }));
    
    act(() => {
      result.current.setSearchQuery('test query');
    });
    
    expect(mockReplace).toHaveBeenCalledWith(
      expect.stringContaining('search=test+query'),
      expect.any(Object)
    );
  });
  
  test('URL parameters should update sorting and search state', () => {
    mockGet.mockImplementation((param) => {
      if (param === 'sort') return 'title';
      if (param === 'sortDirection') return 'desc';
      if (param === 'search') return 'test query';
      return null;
    });
    
    const { result } = renderHook(() => useDataTable({
      data: testData,
    }));
    
    expect(result.current.sorting).toEqual({ column: 'title', direction: 'desc' });
    expect(result.current.searchQuery).toBe('test query');
  });
  
  test('basic sorting functionality should work', () => {
    const { result } = renderHook(() => useDataTable({
      data: testData,
    }));
    
    // Check if data is available
    expect(result.current.filteredData.length).toBe(3);
    
    // Sort by title ascending (default first sort)
    act(() => {
      result.current.handleSort('title');
    });
    
    // Check URL was updated
    expect(mockReplace).toHaveBeenCalledWith(
      expect.stringContaining('sort=title'),
      expect.any(Object)
    );
    expect(mockReplace).toHaveBeenCalledWith(
      expect.stringContaining('sortDirection=asc'),
      expect.any(Object)
    );
    
    // Sort by title descending (second click)
    act(() => {
      result.current.handleSort('title');
    });
    
    // Check URL was updated
    expect(mockReplace).toHaveBeenCalledWith(
      expect.stringContaining('sortDirection=desc'),
      expect.any(Object)
    );
    
    // Check the sorting state
    expect(result.current.sorting).toEqual({
      column: 'title',
      direction: 'desc'
    });
  });
  
  test('sorting by different columns should work', () => {
    const { result } = renderHook(() => useDataTable({
      data: testData,
    }));
    
    // Sort by title
    act(() => {
      result.current.handleSort('title');
    });
    
    // Check sorting state
    expect(result.current.sorting).toEqual({
      column: 'title',
      direction: 'asc'
    });
    
    // Sort by comment
    act(() => {
      result.current.handleSort('comment');
    });
    
    // Check sorting state was updated to new column
    expect(result.current.sorting).toEqual({
      column: 'comment',
      direction: 'asc'
    });
  });
}); 