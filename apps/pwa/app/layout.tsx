import type { Metadata, Viewport } from "next"
import { PwaRegister } from "@/components/pwa-register"
import "./globals.css"

export const metadata: Metadata = {
  title: "Quillotana Operativa",
  description: "Despachos del chofer — Grupo La Quillotana",
  appleWebApp: {
    capable: true,
    title: "Operativa",
    statusBarStyle: "black-translucent",
  },
  icons: {
    icon: "/icons/pwa/icon-192.png",
    apple: "/icons/pwa/apple-touch-icon.png",
  },
}

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  maximumScale: 1,
  themeColor: "#0f1115",
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="es">
      <body>
        <div className="app-shell">{children}</div>
        <PwaRegister />
      </body>
    </html>
  )
}
