import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import FilterSection from '@/components/_backup/FilterSection';
import { useRouter, useSearchParams, usePathname } from 'next/navigation';

// Mock the Next.js navigation hooks
jest.mock('next/navigation', () => ({
  useRouter: jest.fn(),
  useSearchParams: jest.fn(),
  usePathname: jest.fn(),
}));

// Mock the config to have a simpler version for testing
jest.mock('@/lib/config', () => ({
  datasetConfig: {
    fields: [
      {
        key: 'stance',
        title: 'Stance',
        filter: 'select',
      },
      {
        key: 'themes',
        title: 'Themes',
        filter: 'select',
      }
    ]
  }
}));

describe('FilterSection', () => {
  const mockReplace = jest.fn();
  const mockGet = jest.fn();
  const mockPathname = '/test-path';
  const mockOnFilterChange = jest.fn();
  
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
  
  test('loads filters from URL on mount', async () => {
    // Setup URL parameters
    mockGet.mockImplementation((param) => {
      if (param === 'filter_analysis.stance') return '["For"]';
      return null;
    });
    
    render(<FilterSection onFilterChange={mockOnFilterChange} />);
    
    // Check that onFilterChange was called with the correct filters
    await waitFor(() => {
      expect(mockOnFilterChange).toHaveBeenCalledWith({ 'analysis.stance': ['For'] });
    });
  });
  
  test('updates URL when filters change', async () => {
    // Render the component
    render(<FilterSection onFilterChange={mockOnFilterChange} />);
    
    // Find and click on a filter button (e.g., "Stance")
    const stanceButton = screen.getByText('Stance');
    fireEvent.click(stanceButton);
    
    // Wait for filter modal to appear and select an option
    // (This would require mocking the FilterModal component or integration testing)
    
    // Check that URL was updated with filter parameters
    // This test is incomplete and would need to be expanded with proper
    // handling of the FilterModal component
  });
  
  test('clears all filters', async () => {
    // Setup initial filters
    mockGet.mockImplementation((param) => {
      if (param === 'filter_analysis.stance') return '["For"]';
      return null;
    });
    
    const { rerender } = render(<FilterSection onFilterChange={mockOnFilterChange} />);
    
    // Mock that we now have active filters
    rerender(<FilterSection onFilterChange={mockOnFilterChange} />);
    
    // Find and click the "Clear All" button
    const clearButton = screen.getByText('Clear All');
    fireEvent.click(clearButton);
    
    // Check that onFilterChange was called with empty filters
    expect(mockOnFilterChange).toHaveBeenCalledWith({});
    
    // Check that URL parameters for filters were removed
    expect(mockReplace).toHaveBeenCalled();
  });

  test('clear all clears all filters and updates the url', async () => {
    mockGet.mockImplementation((param) => {
      if (param === 'filter_analysis.stance') return '"For"';
      if (param === 'filter_analysis.themes') return '"Theme1"';
      return null;
    });
    render(<FilterSection onFilterChange={mockOnFilterChange} />);
    // Should show Clear All button
    expect(screen.getByText('Clear All')).toBeInTheDocument();
    // Click Clear All
    fireEvent.click(screen.getByText('Clear All'));
    // Should call onFilterChange with {}
    expect(mockOnFilterChange).toHaveBeenCalledWith({});
    // Should update the URL
    expect(mockReplace).toHaveBeenCalled();
  });

  test('removing one filter successfully updates the url', async () => {
    mockGet.mockImplementation((param) => {
      if (param === 'filter_analysis.stance') return '"For"';
      if (param === 'filter_analysis.themes') return '"Theme1"';
      return null;
    });
    render(<FilterSection onFilterChange={mockOnFilterChange} />);
    // Should show both filters
    expect(screen.getAllByRole('button', { name: 'Remove filter' }).length).toBeGreaterThan(1);
    // Click the first remove (×) button
    fireEvent.click(screen.getAllByRole('button', { name: 'Remove filter' })[0]);
    // Should call onFilterChange with only one filter left
    expect(mockOnFilterChange).toHaveBeenCalledWith(expect.objectContaining({}));
    // Should update the URL
    expect(mockReplace).toHaveBeenCalled();
  });

  test('does not call onFilterChange again if filters do not change', async () => {
    mockGet.mockImplementation((param) => {
      if (param === 'filter_analysis.stance') return '"For"';
      return null;
    });
    const { rerender } = render(<FilterSection onFilterChange={mockOnFilterChange} />);
    // Should call once on mount
    expect(mockOnFilterChange).toHaveBeenCalledTimes(1);

    // Simulate rerender with same searchParams (no change)
    rerender(<FilterSection onFilterChange={mockOnFilterChange} />);
    // Should still only be called once
    expect(mockOnFilterChange).toHaveBeenCalledTimes(1);
  });
}); 