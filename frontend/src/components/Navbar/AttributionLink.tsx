import Link from "next/link";
import React from "react";

type AttributionLinkProps = {
  className?: string;
  onClick?: () => void;
  tabIndex?: number;
};

const AttributionLink: React.FC<AttributionLinkProps> = ({
  className = "",
  onClick,
  tabIndex,
}) => (
  <Link
    href="/attribution"
    className={`flex items-center bg-slate-100 text-slate-700 px-3 py-1.5 rounded hover:bg-slate-200 transition-colors border border-slate-200 ${className}`}
    onClick={onClick}
    tabIndex={tabIndex}
    aria-label="Project attribution page"
  >
    {/* People / information icon */}
    <svg
      className="w-5 h-5 mr-2"
      fill="none"
      stroke="currentColor"
      strokeWidth={2}
      viewBox="0 0 24 24"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden="true"
    >
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M17 20h5v-2a4 4 0 00-3-3.87"
      />
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M9 20H4v-2a4 4 0 013-3.87"
      />
      <path
        strokeLinecap="round"
        strokeLinejoin="round"
        d="M16 7a4 4 0 11-8 0 4 4 0 018 0z"
      />
    </svg>
    Attribution
  </Link>
);

export default AttributionLink; 