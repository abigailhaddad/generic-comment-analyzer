import React from "react";

type MobileMenuButtonProps = {
  isMenuOpen: boolean;
  toggleMenu: () => void;
  handleKeyDown: (e: React.KeyboardEvent) => void;
};

const MobileMenuButton: React.FC<MobileMenuButtonProps> = ({ isMenuOpen, toggleMenu, handleKeyDown }) => (
  <button
    className="flex items-center"
    onClick={toggleMenu}
    onKeyDown={handleKeyDown}
    aria-expanded={isMenuOpen}
    aria-label="Toggle navigation menu"
    aria-controls="mobile-menu"
  >
    <span className="sr-only">Open menu</span>
    <div className="w-6 flex flex-col gap-1.5">
      <span
        className={`block h-0.5 w-full bg-slate-700 transform transition-transform duration-300 ease-in-out ${
          isMenuOpen ? "rotate-45 translate-y-2" : ""
        }`}
      ></span>
      <span
        className={`block h-0.5 w-full bg-slate-700 transition-opacity duration-300 ease-in-out ${
          isMenuOpen ? "opacity-0" : "opacity-100"
        }`}
      ></span>
      <span
        className={`block h-0.5 w-full bg-slate-700 transform transition-transform duration-300 ease-in-out ${
          isMenuOpen ? "-rotate-45 -translate-y-2" : ""
        }`}
      ></span>
    </div>
  </button>
);

export default MobileMenuButton; 