import type { Metadata, Viewport } from 'next'
import { Geist, Geist_Mono } from 'next/font/google'
import { Analytics } from '@vercel/analytics/next'
import './globals.css'
import { PwaRegister } from '@/components/layout/pwa-register'

const _geist = Geist({ subsets: ["latin"] });
const _geistMono = Geist_Mono({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: 'Quillotana ERP',
  description: 'Plataforma de gestión empresarial de Grupo La Quillotana',
  applicationName: 'Quillotana ERP',
  generator: 'v0.app',
  icons: {
    icon: [
      { url: '/favicon.ico', sizes: 'any' },
      { url: '/favicon-16x16.png', sizes: '16x16', type: 'image/png' },
      { url: '/favicon-32x32.png', sizes: '32x32', type: 'image/png' },
      { url: '/icons/pwa/icon-192.png', sizes: '192x192', type: 'image/png' },
    ],
    apple: [
      {
        url: '/icons/pwa/apple-touch-icon.png',
        sizes: '180x180',
        type: 'image/png',
      },
    ],
  },
  appleWebApp: {
    capable: true,
    title: 'Quillotana ERP',
    // Barra de estado OPACA: iOS reserva su zona y el contenido web parte
    // debajo (hora/batería/señal jamás tapan controles). Con 'black',
    // env(safe-area-inset-top) vale 0 en la PWA, así que las utilidades
    // .pt-safe existentes se anulan solas: sin doble padding ni franjas.
    // OJO: iOS puede requerir eliminar y reinstalar la PWA para aplicar
    // el nuevo estilo de barra / icono.
    statusBarStyle: 'black',
  },
}

export const viewport: Viewport = {
  width: 'device-width',
  initialScale: 1,
  viewportFit: 'cover',
  themeColor: '#000000',
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
