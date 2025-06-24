'use client';

import Card from '@/components/ui/Card';
import Image from 'next/image';
import Link from 'next/link';

interface AttributionCardProps {
  name: string;
  role: string;
  bio: string;
  github: string;
  linkedin: string;
  avatarUrl: string;
}

export default function AttributionCard({
  name,
  role,
  bio,
  github,
  linkedin,
  avatarUrl,
}: AttributionCardProps) {
  return (
    <Card collapsible={false} className="overflow-hidden">
      {/* Header */}
      <Card.Header className="flex items-center gap-4 bg-gradient-to-r from-slate-50 to-slate-100 border-b border-slate-200">
        <Image
          src={avatarUrl}
          alt={name}
          width={72}
          height={72}
          className="rounded-full shadow-sm border border-gray-200 shrink-0"
        />
        <div>
          <h2 className="text-xl font-semibold text-slate-800 leading-tight">
            {name}
          </h2>
          <p className="text-sm text-slate-600">{role}</p>
        </div>
      </Card.Header>

      {/* Body */}
      <Card.Body>
        <p className="text-slate-700 mb-4 leading-relaxed whitespace-pre-line">
          {bio.split(' ').map((word, index) => {
            // Check if the word looks like a URL
            if (word.includes('.') && (word.includes('.com') || word.includes('.org') || word.includes('.net') || word.includes('.substack'))) {
              // Remove trailing punctuation
              const punctuation = word.match(/[.,!?;:]$/)?.[0] || '';
              const cleanWord = punctuation ? word.slice(0, -1) : word;
              
              // Add https:// if not present
              const url = cleanWord.startsWith('http://') || cleanWord.startsWith('https://') ? cleanWord : `https://${cleanWord}`;
              
              return (
                <span key={index}>
                  <a
                    href={url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-slate-600 hover:text-slate-800 hover:underline"
                  >
                    {cleanWord}
                  </a>
                  {punctuation}{' '}
                </span>
              );
            }
            return <span key={index}>{word} </span>;
          })}
        </p>

        <div className="flex gap-3">
          <Link
            href={`https://github.com/${github}`}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1 text-sm font-medium text-gray-700 hover:text-gray-900 hover:underline"
            aria-label={`GitHub profile of ${name} (opens in new tab)`}
          >
            <svg
              className="w-4 h-4"
              fill="currentColor"
              viewBox="0 0 24 24"
              aria-hidden="true"
            >
              <path d="M12 0C5.373 0 0 5.373 0 12a12 12 0 008.207 11.387c.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.31.468-2.381 1.236-3.221-.124-.303-.535-1.524.118-3.176 0 0 1.008-.322 3.301 1.23a10.563 10.563 0 013.003-.404c1.02.005 2.047.138 3.006.404 2.292-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576A12.003 12.003 0 0024 12c0-6.627-5.373-12-12-12z" />
            </svg>
            <span className="hidden sm:inline">GitHub</span>
          </Link>

          <Link
            href={linkedin}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1 text-sm font-medium text-gray-700 hover:text-gray-900 hover:underline"
            aria-label={`LinkedIn profile of ${name} (opens in new tab)`}
          >
            <svg
              className="w-4 h-4"
              fill="currentColor"
              viewBox="0 0 24 24"
              aria-hidden="true"
            >
              <path d="M19 0h-14C2.239 0 0 2.239 0 5v14c0 2.761 2.239 5 5 5h14c2.761 0 5-2.239 5-5V5c0-2.761-2.239-5-5-5zm-11 19H5V9h3v10zm-1.5-11.29c-.966 0-1.75-.785-1.75-1.75S5.534 4.21 6.5 4.21c.965 0 1.75.784 1.75 1.75S7.465 7.71 6.5 7.71zM20 19h-3v-5.604c0-1.337-.026-3.059-1.864-3.059-1.865 0-2.152 1.455-2.152 2.96V19h-3V9h2.881v1.367h.041c.401-.758 1.382-1.556 2.846-1.556 3.043 0 3.604 2.001 3.604 4.604V19z" />
            </svg>
            <span className="hidden sm:inline">LinkedIn</span>
          </Link>
        </div>
      </Card.Body>
    </Card>
  );
}