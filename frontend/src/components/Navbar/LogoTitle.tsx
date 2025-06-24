import Link from "next/link";
import Image from "next/image";
import React from "react";

type LogoTitleProps = {
  title: string;
  description: string;
  scrolled: boolean;
};

const LogoTitle: React.FC<LogoTitleProps> = ({ title, description, scrolled }) => (
  <div className="flex items-center space-x-3">
    <div
      className={`transition-transform duration-300 ${scrolled ? "scale-90" : "scale-100"}`}
    >
      <Image
        src="/favicon.svg"
        alt="Schedule F Logo"
        width={28}
        height={28}
        className="w-7 h-7"
      />
    </div>
    <div className="flex flex-col">
      <Link
        href="/"
        className={`navbar-brand-text font-semibold text-slate-900 hover:text-slate-700 transition-all duration-300 ${
          scrolled ? "text-lg md:text-xl" : "text-xl md:text-2xl"
        }`}
        aria-label={`${title} - Home`}
      >
        {title}
      </Link>
      <a
        href="https://www.regulations.gov/document/OPM-2025-0004-0001"
        target="_blank"
        rel="noopener noreferrer"
        className={`group text-xs md:text-sm text-slate-600 hover:text-slate-800 max-w-sm md:max-w-xl lg:max-w-2xl transition-all duration-300 flex items-center ${
          scrolled ? "hidden md:flex" : "flex"
        }`}
        aria-label="View official regulations document (opens in new tab)"
      >
        <span>
          {description}
          <svg
            xmlns="http://www.w3.org/2000/svg"
            className="h-3 w-3 ml-0.5 opacity-70 group-hover:opacity-100 transition-opacity inline-flex"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            aria-hidden="true"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"
            />
          </svg>
        </span>
      </a>
    </div>
  </div>
);

export default LogoTitle;