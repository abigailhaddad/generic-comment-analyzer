// src/lib/cache-config.ts
export const cacheConfig = {
    // How long to keep cache valid (in seconds)
    maxAge: 60 * 60 * 24, // 24 hours by default
    
    // Whether to disable caching in development
    disableCacheInDevelopment: true
  };