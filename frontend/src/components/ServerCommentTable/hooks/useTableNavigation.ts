import { useRouter, usePathname, useSearchParams } from 'next/navigation';
import { useCallback } from 'react';
import { Comment } from '@/lib/db/schema';

export function useTableNavigation() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  
  const handleRowClick = useCallback((comment: Comment) => {
    const currentParams = new URLSearchParams(searchParams.toString());
    const returnUrl = `${pathname}?${currentParams.toString()}`;
    
    router.push(`/comment/${comment.id}?returnUrl=${encodeURIComponent(returnUrl)}`);
  }, [router, pathname, searchParams]);
  
  return { handleRowClick };
} 