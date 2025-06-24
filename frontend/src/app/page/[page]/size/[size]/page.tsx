import { Suspense } from "react";
import { Metadata } from "next";
import ServerCommentDataProvider from "@/components/ServerCommentDataProvider";
import { datasetConfig } from "@/lib/config";
import { redirect } from "next/navigation";

// Generate metadata
export const metadata: Metadata = {
  title: datasetConfig.title,
  description: 'Public Comments on "Schedule F" Regulation',
};

// Statically generate common page size combinations at build time
// Disable in development to prevent debugger issues
export async function generateStaticParams() {
  // Skip static generation in development to prevent debugger pausing issues
  if (process.env.NODE_ENV === "development") {
    return [];
  }

  const commonPageSizes = [25, 50]; // Reduced from [10, 25, 50, 100]
  const pagesToPreGenerate = [1, 2]; // Reduced from [1, 2, 3]

  const params = [];

  // Generate first few pages for each common size
  for (const size of commonPageSizes) {
    for (const page of pagesToPreGenerate) {
      params.push({
        page: page.toString(),
        size: size.toString(),
      });
    }
  }

  return params;
}

interface PageProps {
  params: Promise<{
    page: string;
    size: string;
  }>;
}

export default async function CommentPage({ params }: PageProps) {
  // Await params as required in Next.js 15
  const { page: pageParam, size: sizeParam } = await params;

  // Validate page and size params
  const page = parseInt(pageParam, 10);
  const size = parseInt(sizeParam, 10);

  // Redirect to valid values if invalid
  if (isNaN(page) || page < 1 || isNaN(size) || size < 1) {
    // redirects to /page/1/size/10
    redirect(`/page/1/size/10`);
  }

  return (
    <main className="min-h-screen bg-gray-50">
      <div className="pb-12">
        <div className="container mx-auto px-4 md:px-6">
          <Suspense
            fallback={<div className="text-center py-12">Loading Dashboard...</div>} >
            <ServerCommentDataProvider
              initialPage={page}
              initialPageSize={size}
            />
          </Suspense>

          {/* Optional: Show development info */}
          {process.env.NODE_ENV === "development" && (
            <div className="text-xs text-gray-500 text-center mt-4">
              Next.js 15 async params - dynamic rendering
              {process.env.LAST_DATA_UPDATE && (
                <>
                  {" "}
                  | Last data update:{" "}
                  {new Date(process.env.LAST_DATA_UPDATE).toLocaleString()}
                </>
              )}
            </div>
          )}
        </div>
      </div>
    </main>
  );
}
