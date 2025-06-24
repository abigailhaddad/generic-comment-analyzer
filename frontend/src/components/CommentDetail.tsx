// components/CommentDetail.tsx
import { Comment } from '@/lib/db/schema';
import Badge from './ui/Badge';
import Link from 'next/link';
import { getRelatedComments } from '@/lib/actions/comments';

interface CommentDetailProps {
  comment: Comment;
  relatedComments?: Comment[];
}

export default async function CommentDetail({ comment }: CommentDetailProps) {
  // Fetch related comments if the comment has a lookupId
  let relatedComments: Comment[] = [];
  if (comment.lookupId) {
    const relatedResult = await getRelatedComments(comment.lookupId);
    if (relatedResult.success && relatedResult.data) {
      // Filter out the current comment from related comments
      relatedComments = relatedResult.data.filter(c => c.id !== comment.id);
    }
  }

  // Helper function to determine badge type
  const getBadgeType = (value: string): 'success' | 'danger' | 'warning' | 'primary' | 'default' => {
    if (value === 'For') return 'success';
    if (value === 'Against') return 'danger';
    if (value === 'Neutral/Unclear') return 'warning';
    return 'primary';
  };

  // Helper function to format date for URL filtering
  const formatDateForFilter = (date: Date | string | null) => {
    if (!date) return null;
    const dateObj = new Date(date);
    return dateObj.toISOString().split('T')[0]; // YYYY-MM-DD format
  };

  // Helper function to create filter URL for dates
  const createDateFilterUrl = (date: Date | string | null, type: 'posted' | 'received') => {
    const formattedDate = formatDateForFilter(date);
    if (!formattedDate) return '/page/1/size/10';
    
    // Create the filter object in the expected format
    const filterObject = {
      mode: 'exact',
      startDate: formattedDate,
      endDate: formattedDate
    };
    
    // Convert to JSON and URL encode
    const filterJson = JSON.stringify(filterObject);
    const encodedFilter = encodeURIComponent(filterJson);
    
    // Use the correct filter parameter name
    const filterParam = type === 'posted' ? 'filter_posted_date' : 'filter_received_date';
    return `/page/1/size/10?${filterParam}=${encodedFilter}`;
  };

  return (
    <article 
      className="bg-white rounded-lg shadow-md p-6"
      aria-labelledby="comment-title"
    >
      <header className="mb-8">
        <h1 id="comment-title" className="text-2xl font-bold mb-4 text-gray-800">
          {comment.title || 'Untitled Comment'}
        </h1>
        
        {/* Stance Badge */}
        {comment.stance && (
          <div className="mb-4">
            <div className="flex items-center gap-2">
              <span className="text-sm font-medium text-gray-600">Stance:</span>
              <Badge 
                type={getBadgeType(comment.stance)}
                label={comment.stance}
                id="comment-stance"
                aria-label={`Stance: ${comment.stance}`}
              />
            </div>
          </div>
        )}

        {/* Related Duplicate Comments */}
        {relatedComments.length > 0 && (
          <div className="mb-4 p-4 bg-yellow-50 border border-yellow-200 rounded-md">
            <h3 className="text-sm font-medium text-yellow-800 mb-2">
              🔗 {comment.commentCount && comment.commentCount > 1 ? `${comment.commentCount - 1} ` : ''}Related Duplicate Comment{relatedComments.length !== 1 ? 's' : ''}
            </h3>
            <div className="flex flex-wrap gap-2">
              {relatedComments.slice(0, 10).map((relatedComment) => (
                <Link
                  key={relatedComment.id}
                  href={`/comment/${relatedComment.id}`}
                  className="text-sm text-blue-600 hover:text-blue-800 hover:underline bg-white px-2 py-1 rounded border border-yellow-300"
                  title={relatedComment.title || 'Untitled Comment'}
                >
                  {relatedComment.id}
                </Link>
              ))}
              {relatedComments.length > 10 && (
                <span className="text-sm text-gray-600 px-2 py-1">
                  +{relatedComments.length - 10} more
                </span>
              )}
            </div>
          </div>
        )}
      </header>

      {/* Comment Text */}
      <section 
        className="mb-8"
        aria-labelledby="comment-heading"
      >
        <h2 id="comment-heading" className="text-lg font-semibold mb-4 text-gray-700 border-b pb-2">Comment</h2>
        <div 
          className="whitespace-pre-wrap text-gray-700 bg-gray-50 p-4 rounded-md border border-gray-200"
          aria-label="Comment text"
        >
          {comment.comment || 'No comment text available'}
        </div>
      </section>

      {/* Analysis Section */}
      <section 
        className="mb-8"
        aria-labelledby="analysis-heading"
      >
        <h2 id="analysis-heading" className="text-lg font-semibold mb-4 text-gray-700 border-b pb-2">Analysis</h2>
        
        {/* Key Quote */}
        {comment.keyQuote && (
          <div 
            className="mb-6"
            aria-labelledby="key-quote-heading"
          >
            <h3 id="key-quote-heading" className="text-md font-medium mb-3 text-gray-600 flex items-center">
              <span className="mr-2">💬</span>Key Quote
            </h3>
            <blockquote className="italic bg-blue-50 p-4 rounded-md border-l-4 border-blue-300 text-gray-700">
              &ldquo;{comment.keyQuote}&rdquo;
            </blockquote>
          </div>
        )}
        
        {/* Themes */}
        {comment.themes && (
          <div 
            className="mb-6"
            aria-labelledby="themes-heading"
          >
            <h3 id="themes-heading" className="text-md font-medium mb-3 text-gray-600 flex items-center">
              <span className="mr-2">🏷️</span>Themes
            </h3>
            <div 
              className="flex flex-wrap gap-2"
              role="list"
              aria-label="Comment themes"
            >
              {comment.themes.split(',').map((theme, index) => (
                <Badge 
                  key={index} 
                  type="primary" 
                  label={theme.trim()} 
                  id={`theme-${index}`}
                />
              ))}
            </div>
          </div>
        )}
        
        {/* Rationale */}
        {comment.rationale && (
          <div 
            className="mb-6"
            aria-labelledby="rationale-heading"
          >
            <h3 id="rationale-heading" className="text-md font-medium mb-3 text-gray-600 flex items-center">
              <span className="mr-2">📝</span>Rationale
            </h3>
            <div 
              className="text-gray-700 bg-gray-50 p-4 rounded-md border border-gray-200"
              aria-label="Comment rationale"
            >
              {comment.rationale}
            </div>
          </div>
        )}
      </section>

      {/* Metadata Section */}
      <section 
        aria-labelledby="metadata-heading"
      >
        <h2 id="metadata-heading" className="text-lg font-semibold mb-4 text-gray-700 border-b pb-2">Metadata</h2>
        <dl className="grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-4">
          {comment.id && (
            <div>
              <dt className="text-sm font-medium text-gray-500">Comment ID:</dt>
              <dd className="text-gray-700 font-mono text-sm">{comment.id}</dd>
            </div>
          )}
          {comment.organization && (
            <div>
              <dt className="text-sm font-medium text-gray-500">Organization:</dt>
              <dd className="text-gray-700">{comment.organization}</dd>
            </div>
          )}
          {comment.documentType && (
            <div>
              <dt className="text-sm font-medium text-gray-500">Document Type:</dt>
              <dd className="text-gray-700">{comment.documentType}</dd>
            </div>
          )}
          {comment.category && (
            <div>
              <dt className="text-sm font-medium text-gray-500">Category:</dt>
              <dd className="text-gray-700">{comment.category}</dd>
            </div>
          )}
          {comment.attachmentCount !== null && comment.attachmentCount > 0 && (
            <div>
              <dt className="text-sm font-medium text-gray-500">Attachment Count:</dt>
              <dd className="text-gray-700">{comment.attachmentCount}</dd>
            </div>
          )}
          {comment.hasAttachments !== null && (
            <div>
              <dt className="text-sm font-medium text-gray-500">Has Attachments:</dt>
              <dd className="text-gray-700">
                <Badge 
                  type={comment.hasAttachments ? 'success' : 'default'}
                  label={comment.hasAttachments ? 'Yes' : 'No'}
                />
              </dd>
            </div>
          )}
          {comment.postedDate && (
            <div>
              <dt className="text-sm font-medium text-gray-500">Posted Date:</dt>
              <dd className="text-gray-700">
                <Link 
                  href={createDateFilterUrl(comment.postedDate, 'posted')}
                  className="text-blue-600 hover:text-blue-800 hover:underline inline-flex items-center"
                  title="View all comments posted on this date"
                >
                  <time dateTime={new Date(comment.postedDate).toISOString()}>
                    {new Date(comment.postedDate).toLocaleDateString()}
                  </time>
                  <span className="ml-1 text-xs">📅</span>
                </Link>
              </dd>
            </div>
          )}
          {comment.receivedDate && (
            <div>
              <dt className="text-sm font-medium text-gray-500">Received Date:</dt>
              <dd className="text-gray-700">
                <Link 
                  href={createDateFilterUrl(comment.receivedDate, 'received')}
                  className="text-blue-600 hover:text-blue-800 hover:underline inline-flex items-center"
                  title="View all comments received on this date"
                >
                  <time dateTime={new Date(comment.receivedDate).toISOString()}>
                    {new Date(comment.receivedDate).toLocaleDateString()}
                  </time>
                  <span className="ml-1 text-xs">📅</span>
                </Link>
              </dd>
            </div>
          )}
          {comment.clusterId && (
            <div>
              <dt className="text-sm font-medium text-gray-500">Cluster ID:</dt>
              <dd className="text-gray-700">{comment.clusterId}</dd>
            </div>
          )}
          {comment.textSource && (
            <div>
              <dt className="text-sm font-medium text-gray-500">Text Source:</dt>
              <dd className="text-gray-700">{comment.textSource}</dd>
            </div>
          )}
          {comment.commentCount && comment.commentCount > 1 && (
            <div>
              <dt className="text-sm font-medium text-gray-500">Total in Group:</dt>
              <dd className="text-gray-700">{comment.commentCount} comments</dd>
            </div>
          )}
          {comment.createdAt && (
            <div>
              <dt className="text-sm font-medium text-gray-500">Created Date:</dt>
              <dd className="text-gray-700">
                <time dateTime={new Date(comment.createdAt).toISOString()}>
                  {new Date(comment.createdAt).toLocaleDateString()} at {new Date(comment.createdAt).toLocaleTimeString()}
                </time>
              </dd>
            </div>
          )}
          {comment.link && (
            <div>
              <dt className="text-sm font-medium text-gray-500">External Link:</dt>
              <dd>
                <a 
                  href={comment.link} 
                  target="_blank" 
                  rel="noopener noreferrer" 
                  className="text-blue-600 hover:text-blue-800 hover:underline inline-flex items-center focus:outline-none focus:ring-2 focus:ring-blue-500 rounded px-1"
                  aria-label="View original comment on external site (opens in new tab)"
                >
                  <span className="mr-1" aria-hidden="true">🔗</span>View Original
                </a>
              </dd>
            </div>
          )}
        </dl>
      </section>

      {/* Attachments Section */}
      {comment.attachmentUrls && comment.attachmentTitles && (
        <section 
          aria-labelledby="attachments-heading"
          className="mt-8"
        >
          <h2 id="attachments-heading" className="text-lg font-semibold mb-4 text-gray-700 border-b pb-2">Attachments</h2>
          <ul className="list-disc pl-5 space-y-2">
            {(() => {
              const urls = comment.attachmentUrls.split('; ');
              const titles = comment.attachmentTitles.split('; ');
              return urls.map((url, index) => {
                const title = titles[index] || `Attachment ${index + 1}`;
                return (
                  <li key={index} className="text-gray-700">
                    <a 
                      href={url} 
                      target="_blank" 
                      rel="noopener noreferrer"
                      className="text-blue-600 hover:text-blue-800 hover:underline inline-flex items-center focus:outline-none focus:ring-2 focus:ring-blue-500 rounded px-1"
                      aria-label={`Download or view attachment: ${title} (opens in new tab)`}
                    >
                      <span className="mr-1" aria-hidden="true">📄</span>{title}
                    </a>
                  </li>
                );
              });
            })()}
          </ul>
        </section>
      )}
    </article>
  );
}