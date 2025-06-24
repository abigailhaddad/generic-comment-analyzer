// Import jest-dom for custom matchers
import '@testing-library/jest-dom';

// Mock Next.js router
jest.mock('next/navigation', () => ({
  useRouter: () => ({
    push: jest.fn(),
    replace: jest.fn(),
    prefetch: jest.fn(),
    back: jest.fn(),
    forward: jest.fn()
  }),
  useSearchParams: () => ({
    get: jest.fn(),
    has: jest.fn(),
    getAll: jest.fn(),
    forEach: jest.fn(),
    entries: jest.fn(() => []),
    toString: jest.fn()
  }),
  usePathname: () => '/test-path',
})); 