import { getCommentById } from '@/lib/actions/comments';
import CommentDetail from '@/components/CommentDetail';
import BackButton from '@/components/BackButton';
import { notFound } from 'next/navigation';

interface PageProps {
  params: Promise<{
    id: string;
  }>;
  searchParams: Promise<{
    returnUrl?: string;
  }>;
}

export default async function CommentPage({ params, searchParams }: PageProps) {
  const { id }  = await params;
  const { returnUrl } = await searchParams;
  const lastUrl = returnUrl ? decodeURIComponent(returnUrl) : '/';
  
  // Server-side data fetching
  const result = await getCommentById(id);
  
  // If comment not found, show 404 page
  if (!result.success || !result.data) {
    notFound();
  }
  
  return (
    <main className="min-h-screen bg-gray-50">
      <div className="pb-12">
        <div className="container mx-auto px-4 md:px-6">
          <BackButton returnUrl={lastUrl} />
          <CommentDetail comment={result.data} />
        </div>
      </div>
    </main>
  );
}