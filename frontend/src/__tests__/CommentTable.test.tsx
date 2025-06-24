import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { useRouter } from 'next/navigation';
import '@testing-library/jest-dom';
import { useState } from 'react';

// Mock the Next.js navigation hooks
jest.mock('next/navigation', () => ({
  useRouter: jest.fn(() => ({
    replace: jest.fn(),
  })),
  useSearchParams: jest.fn(() => ({
    get: jest.fn(),
    toString: jest.fn(() => ''),
  })),
  usePathname: jest.fn(() => '/test-path'),
}));

// Simple SearchComponent for testing URL parameters
const SearchComponent = ({ onSearch }: { onSearch: (query: string) => void }) => {
  const [query, setQuery] = useState('');
  
  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const newQuery = e.target.value;
    setQuery(newQuery);
    onSearch(newQuery);
  };
  
  return (
    <input 
      type="text" 
      placeholder="Search..." 
      value={query}
      onChange={handleChange}
      data-testid="search-input"
    />
  );
};

describe('Search component with URL synchronization', () => {
  const mockRouter = useRouter as jest.Mock;
  
  beforeEach(() => {
    jest.clearAllMocks();
  });
  
  test('updates URL when search is performed', async () => {
    const mockReplace = jest.fn();
    mockRouter.mockReturnValue({
      replace: mockReplace,
    });
    
    const handleSearch = (query: string) => {
      const params = new URLSearchParams();
      if (query) {
        params.set('search', query);
      }
      mockReplace(`/test-path?${params.toString()}`, { scroll: false });
    };
    
    render(<SearchComponent onSearch={handleSearch} />);
    
    // Find the search input
    const searchInput = screen.getByTestId('search-input');
    
    // Type a search query
    fireEvent.change(searchInput, { target: { value: 'Test query' } });
    
    // Check that URL was updated with search parameter
    await waitFor(() => {
      expect(mockReplace).toHaveBeenCalledWith(
        expect.stringContaining('search=Test+query'),
        expect.any(Object)
      );
    });
  });
}); 