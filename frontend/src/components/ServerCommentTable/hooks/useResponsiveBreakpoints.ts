import { useState, useEffect } from 'react';

const MOBILE_MAX_WIDTH = 767; // Up to 767px is mobile
const TABLET_MAX_WIDTH = 1279; // 768px to 1279px is tablet (2x2 grid for stats)
// Desktop is >= 1280px (4 columns for stats)

interface ResponsiveBreakpoints {
  isMobile: boolean;
  isTablet: boolean;
  isDesktop: boolean;
  screenWidth: number;
  isHydrated: boolean; // Track if we've hydrated on the client
}

export function useResponsiveBreakpoints(): ResponsiveBreakpoints {
  const [screenWidth, setScreenWidth] = useState(0); // Start with 0 to avoid hydration mismatch
  const [isHydrated, setIsHydrated] = useState(false);

  useEffect(() => {
    // This only runs on the client
    const handleResize = () => {
      setScreenWidth(window.innerWidth);
    };

    // Set initial screen width and mark as hydrated
    setScreenWidth(window.innerWidth);
    setIsHydrated(true);

    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  // Before hydration, assume desktop to avoid layout shift
  // After hydration, use actual screen width
  const effectiveWidth = isHydrated ? screenWidth : 1280; // Default to desktop width

  const isMobile = effectiveWidth <= MOBILE_MAX_WIDTH;
  const isTablet = effectiveWidth > MOBILE_MAX_WIDTH && effectiveWidth <= TABLET_MAX_WIDTH;
  const isDesktop = effectiveWidth > TABLET_MAX_WIDTH;

  return { isMobile, isTablet, isDesktop, screenWidth: effectiveWidth, isHydrated };
} 