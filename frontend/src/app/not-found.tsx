import Link from 'next/link';

export default function NotFound() {
  return (
    <div className="container text-center py-5">
      <h1 className="display-4">404 - Page Not Found</h1>
      <p className="lead">We couldn&apos;t find the page you were looking for.</p>
      <Link href="/" className="btn btn-primary">
        Return Home
      </Link>
    </div>
  );
} 