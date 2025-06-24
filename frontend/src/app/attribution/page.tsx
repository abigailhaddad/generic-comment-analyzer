// src/app/attribution/page.tsx
'use client' 

//import { Metadata } from "next";
import AttributionCard from "@/components/AttributionCard";
import Card from "@/components/ui/Card";
//import Button from "@/components/ui/Button";
import { useState } from "react";

/**
 * Page metadata – this is picked up by the Next.js App Router.
 */
// export const metadata: Metadata = {
//   title: "Attribution & Collaboration – Schedule F Analysis",
//   description:
//     "Meet the team behind the Schedule F Analysis project, learn how to cite our work, and explore collaboration opportunities.",
// };

/**
 * Simple helper to build an avatar URL from a GitHub username.
 */
const githubAvatar = (username: string, size = 240) =>
  `https://github.com/${username}.png?size=${size}`;

/**
 * Data‑driven description of contributors so the component markup stays tidy.
 */
const CONTRIBUTORS = [
  {
    name: "Michael Boyce",
    role: "Developer and website-maker-in-chief",
    bio: `Michael Boyce is a tech and policy expert who recently wrapped up his role as Director of the AI Corps at DHS, where he built the federal government's largest civilian AI team and hired 50 senior AI experts into the Department. Before that, he spent time at the White House working on a range of issues includingAI policy, Western Hemisphere Foreign Assistance, and product/strategy roles at the United States Digital Service. He has over a decade of experience digitizing government processes and building machine learning systems across immigration and national security agencies. Earlier in his career, he worked directly as a Refugee Officer, leading teams and interviewing refugees in places like Ethiopia, Jordan, Kenya, Lebanon, Malaysia, and Turkey, before later leading the team that digitized the  U.S. refugee and asylum  application process. He lives in the Petworth neighborhood of Washington DC, and likes playing chess and finding good soup dumpling restaurants in the DC suburbs.`,
    github: "michaeleboyce",
    linkedin: "https://www.linkedin.com/in/michael-boyce-dhs-ai-corps/",
  },
  {
    name: "Abigail Haddad",
    role: "Creator and Machine Learning Engineer",
    bio: `Abigail Haddad is an Machine Learning Engineer working on text data pipelines and testing frameworks. She has a PhD in Public Policy from RAND and previously worked for the Army and DHS AI Corps. She co-organizes Data Science DC, runs a civic tech happy hour, and blogs at presentofcoding.substack.com, where she writes about practical approaches to LLM evaluation, text processing pipelines, and coding in government. Some of her work has focused on analyzing government personnel data, including FedScope and USAJobs datasets. She's currently looking for her next job.`,
    github: "abigailhaddad",
    linkedin: "https://www.linkedin.com/in/abigail-haddad/",
  },
];

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  
  const handleCopy = () => {
    navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };
  
  return (
    <button
      onClick={handleCopy}
      className="text-slate-600 hover:text-slate-800 text-sm font-medium"
    >
      {copied ? "✓ Copied!" : "📋 Copy to clipboard"}
    </button>
  );
}

export default function AttributionPage() {
  const attributionText = `Schedule F comment analysis by Michael Boyce and Abigail Haddad via schedule-f.vercel.app which analyzed public comments from regulations.gov to determine support and themes.`;
  
  const academicCitation = `Boyce, M. & Haddad, A. (2024). Schedule F Public Comment Analysis Dataset. Retrieved from https://schedule-f.vercel.app`;

  return (
    <main className="min-h-screen bg-slate-50 flex flex-col">
      <div className="container mx-auto px-4 md:px-8 py-10">
        <h1 className="text-3xl md:text-4xl font-semibold text-slate-900 mb-8 flex items-center gap-2">
          <span aria-hidden="true" className="opacity-70">👥</span>
          Attribution & Collaboration
        </h1>

        {/* Quick Attribution Section */}
        <Card className="mb-8" collapsible={false}>
          <Card.Header className="bg-gradient-to-r from-slate-50 to-slate-100 border-b border-slate-200">
            <h2 className="text-lg font-semibold text-slate-800 flex items-center">
              <span className="mr-2 opacity-60">📝</span>
              Quick Attribution
            </h2>
          </Card.Header>
          <Card.Body className="p-6">
            <div className="space-y-4">
              <div className="bg-slate-50 p-4 rounded-lg border border-slate-200">
                <h3 className="font-semibold text-sm text-slate-700 mb-2">For Articles & Reports:</h3>
                <p className="text-sm font-mono bg-white p-3 rounded border border-slate-200 mb-2">
                  {attributionText}
                </p>
                <CopyButton text={attributionText} />
              </div>
              
              <div className="bg-slate-50 p-4 rounded-lg border border-slate-200">
                <h3 className="font-semibold text-sm text-slate-700 mb-2">For Academic Papers:</h3>
                <p className="text-sm font-mono bg-white p-3 rounded border border-slate-200 mb-2">
                  {academicCitation}
                </p>
                <CopyButton text={academicCitation} />
              </div>
            </div>
          </Card.Body>
        </Card>

        {/* Let's Collaborate Section */}
        <Card className="mb-8" collapsible={false}>
          <Card.Header className="bg-gradient-to-r from-slate-100 to-blue-50 border-b border-slate-200">
            <h2 className="text-lg font-semibold text-slate-800 flex items-center">
              <span className="mr-2 opacity-60">🤝</span>
              Let&apos;s Collaborate
            </h2>
          </Card.Header>
          <Card.Body className="p-6">
            <p className="text-slate-700 mb-6">
              We&apos;re passionate about making government data more accessible and analyzable. 
              Whether you&apos;re a journalist, researcher, policy analyst, or civic technologist, 
              we&apos;d love to connect!
            </p>
            
            <div className="flex flex-wrap gap-3 justify-center">
              <a 
                href="mailto:abigail.haddad@gmail.com?subject=Schedule F Analysis Collaboration"
                className="inline-flex items-center px-4 py-2 bg-slate-700 hover:bg-slate-800 text-white rounded-md font-medium transition-colors"
              >
                <span className="mr-2 opacity-80">📧</span> Email Us
              </a>
              <a 
                href="https://github.com/your-repo/schedule-f-analysis"
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center px-4 py-2 bg-white hover:bg-slate-50 text-slate-700 border border-slate-300 rounded-md font-medium transition-colors"
              >
                <span className="mr-2 opacity-80">🐙</span> View on GitHub
              </a>
            </div>
          </Card.Body>
        </Card>

        {/* Meet the Team Section */}
        <h2 className="text-2xl font-semibold text-slate-800 mb-6 flex items-center gap-2">
          <span aria-hidden="true" className="opacity-70">👨‍💻👩‍💻</span>
          Meet the Team
        </h2>
        
        <div className="grid gap-8 md:grid-cols-2">
          {CONTRIBUTORS.map(({ name, role, bio, github, linkedin }) => (
            <AttributionCard
              key={name}
              name={name}
              role={role}
              bio={bio}
              github={github}
              linkedin={linkedin}
              avatarUrl={githubAvatar(github)}
            />
          ))}
        </div>

        {/* Projects Using This Data (Initially empty, ready to showcase) */}
        <Card className="mt-8" collapsible={false}>
          <Card.Header className="bg-gradient-to-r from-amber-50 to-orange-50 border-b border-amber-200">
            <h2 className="text-lg font-semibold text-slate-800 flex items-center">
              <span className="mr-2 opacity-60">🌟</span>
              Projects Using This Analysis
            </h2>
          </Card.Header>
          <Card.Body className="p-6 text-center">
            <p className="text-slate-600 mb-4">
              Has your organization used this data? We&apos;d love to showcase your work!
            </p>
            <a 
              href="mailto:abigail.haddad@gmail.com?subject=We used your Schedule F analysis"
              className="text-slate-600 hover:text-slate-800 hover:underline font-medium"
            >
              Let us know about your project →
            </a>
          </Card.Body>
        </Card>
      </div>
    </main>
  );
}