// components/ServerCommentDataProvider.tsx
'use client';

import ServerDataState from './ServerDataState';
import { ServerDataContextProvider } from '@/contexts/ServerDataContext';

interface ServerCommentDataProviderProps {
  initialPage?: number;
  initialPageSize?: number;
}

export default function ServerCommentDataProvider({ 
  initialPage, 
  initialPageSize 
}: ServerCommentDataProviderProps = {}) {
  return (
    <ServerDataContextProvider 
      initialPage={initialPage}
      initialPageSize={initialPageSize}
    >
      <ServerDataState />
    </ServerDataContextProvider>
  );
}