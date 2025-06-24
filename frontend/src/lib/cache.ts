/**
 * Improved caching system with proper invalidation
 * Features:
 * - LRU (Least Recently Used) cache mechanism
 * - Configurable cache size and TTL
 * - Cache invalidation by key or pattern
 * - Automatic cache expiration
 */

import { cacheConfig } from './cache-config';

interface CacheEntry<T> {
  value: T;
  timestamp: number;
  expiresAt: number;
}

export class Cache {
  private readonly store: Map<string, CacheEntry<unknown>>;
  private readonly maxSize: number;
  private readonly defaultTTL: number;

  /**
   * Create a new cache instance
   * @param maxSize - Maximum number of items to cache (defaults to 100)
   * @param defaultTTL - Default TTL in seconds (defaults to 1 hour)
   */
  constructor(
    maxSize = 100,
    defaultTTL = 60 * 60
  ) {
    this.store = new Map();
    this.maxSize = maxSize;
    this.defaultTTL = defaultTTL;
  }

  /**
   * Get a value from the cache
   * @param key - Cache key
   * @returns The cached value or undefined if not found or expired
   */
  get<T>(key: string): T | undefined {
    // Skip cache in development if configured
    if (process.env.NODE_ENV === 'development' && cacheConfig.disableCacheInDevelopment) {
      return undefined;
    }

    const entry = this.store.get(key) as CacheEntry<T> | undefined;

    // Return undefined if entry not found
    if (!entry) {
      return undefined;
    }

    // Check if entry has expired
    if (Date.now() > entry.expiresAt) {
      this.delete(key);
      return undefined;
    }

    // Move the entry to the end of the LRU list
    this.store.delete(key);
    this.store.set(key, entry);

    return entry.value;
  }

  /**
   * Set a value in the cache
   * @param key - Cache key
   * @param value - Value to cache
   * @param ttl - Time to live in seconds (optional, uses default if not provided)
   */
  set<T>(key: string, value: T, ttl?: number): void {
    // Skip cache in development if configured
    if (process.env.NODE_ENV === 'development' && cacheConfig.disableCacheInDevelopment) {
      return;
    }

    // Remove oldest entries if we've reached max size
    if (this.store.size >= this.maxSize) {
      const oldestKey = this.store?.keys()?.next()?.value;
      if (oldestKey) {
        this.store.delete(oldestKey);
      }
    }

    const now = Date.now();
    const ttlMs = (ttl ?? this.defaultTTL) * 1000;

    this.store.set(key, {
      value,
      timestamp: now,
      expiresAt: now + ttlMs
    });
  }

  /**
   * Check if a key exists in the cache and is not expired
   * @param key - Cache key
   * @returns True if the key exists and is not expired
   */
  has(key: string): boolean {
    // Skip cache in development if configured
    if (process.env.NODE_ENV === 'development' && cacheConfig.disableCacheInDevelopment) {
      return false;
    }

    const entry = this.store.get(key);
    if (!entry) {
      return false;
    }

    // Check if expired
    if (Date.now() > entry.expiresAt) {
      this.delete(key);
      return false;
    }

    return true;
  }

  /**
   * Delete a key from the cache
   * @param key - Cache key
   */
  delete(key: string): void {
    this.store.delete(key);
  }

  /**
   * Clear all entries from the cache
   */
  clear(): void {
    this.store.clear();
  }

  /**
   * Delete all keys that match a pattern
   * @param pattern - String or RegExp pattern to match against keys
   */
  deletePattern(pattern: string | RegExp): void {
    const regex = typeof pattern === 'string'
      ? new RegExp(pattern.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))
      : pattern;

    for (const key of this.store.keys()) {
      if (regex.test(key)) {
        this.delete(key);
      }
    }
  }

  /**
   * Get or set a value with a callback
   * @param key - Cache key
   * @param fetcher - Function to call if key is not in cache
   * @param ttl - Time to live in seconds (optional)
   * @returns Promise that resolves to the value
   */
  async getOrSet<T>(
    key: string,
    fetcher: () => Promise<T>,
    ttl?: number
  ): Promise<T> {
    // Check if we already have a valid cached value
    const cachedValue = this.get<T>(key);
    if (cachedValue !== undefined) {
      return cachedValue;
    }

    // Otherwise, fetch and cache the value
    const value = await fetcher();
    this.set(key, value, ttl);
    return value;
  }

  /**
   * Get all keys in the cache
   * @returns Array of all keys
   */
  keys(): string[] {
    return Array.from(this.store.keys());
  }

  /**
   * Get the number of items in the cache
   * @returns Number of cached items
   */
  size(): number {
    return this.store.size;
  }
}

// Create and export a singleton instance
export const cache = new Cache(
  100, // Maximum 100 items
  cacheConfig.maxAge // TTL from config
);

/**
 * Helper function to get a value from the cache or fetch it
 * @param key - Cache key
 * @param fetcher - Function to fetch the value if not in cache
 * @param ttl - Optional TTL override
 * @returns Promise that resolves to the value
 */
export async function getCachedData<T>(
  key: string,
  fetcher: () => Promise<T>,
  ttl?: number
): Promise<T> {
  return cache.getOrSet(key, fetcher, ttl);
}