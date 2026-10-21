import type { Metadata, Viewport } from 'next'
import { Geist, Geist_Mono } from 'next/font/google'
import { Analytics } from '@vercel/analytics/next'
import './globals.css'
import { PwaRegister } from '@/components/layout/pwa-register'

const _geist = Geist({ subsets: ["latin"] });
const _geistMono = Geist_Mono({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: 'La Quillotana ERP',
  description: 'Plataforma de gestión empresarial de Grupo La Quillotana',
  applicationName: 'La Quillotana ERP',
  generator: 'v0.app',
  icons: {
    icon: [{ url: '/icon.svg', type: 'image/svg+xml' }],
    apple: [{ url: '/icons/pwa/apple-touch-icon.png', sizes: '180x180', type: 'image/png' }],
  },
  appleWebApp: {
    capable: true,
    title: 'La Quillotana ERP',
    statusBarStyle: 'black-translucent',
  },
}

export const viewport: Viewport = {
  width: 'device-width',
  initialScale: 1,
  viewportFit: 'cover',
  themeColor: '#17191E',
}

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode
}>) {
  return (
    <html lang="es">
      <body className="font-sans antialiased">
        {children}
        <PwaRegister />
        {/* Solo en Vercel: evita scripts / CSP innecesarios en Cloudflare u otros hosts */}
        {process.env.VERCEL ? <Analytics /> : null}
      </body>
    </html>
  )
}
