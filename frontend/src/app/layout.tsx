// app/layout.tsx
import './globals.css'
import { datasetConfig } from '@/lib/config'
import { Metadata } from 'next'
import { Analytics } from "@vercel/analytics/next"
import DatabaseStatus from '@/components/DatabaseStatus'
import Navbar from '@/components/Navbar'

export const metadata: Metadata = {
  title: datasetConfig.title,
  description: 'Public Comments on "Schedule F" Regulation',
  metadataBase: new URL(process.env.NEXT_PUBLIC_BASE_URL || 'http://localhost:3000'),
  other: {
    charset: 'utf-8',
  },
  icons: {
    icon: [
      { url: '/favicon.ico', sizes: '32x32' },
      { url: '/favicon.svg', type: 'image/svg+xml' },
    ],
    apple: {
      url: '/apple-touch-icon.png',
      sizes: '180x180',
    },
  },
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body className="bg-light font-sans">
        <Navbar />
        <div className="h-36 sm:h-36 md:h-36 lg:h-36 xl:h-36" />
        {children}
        {process.env.NODE_ENV === 'development' && <DatabaseStatus />}
        <Analytics />
      </body>
    </html>
  )
}