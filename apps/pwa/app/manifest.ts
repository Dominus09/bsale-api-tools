import type { MetadataRoute } from "next"

export default function manifest(): MetadataRoute.Manifest {
  return {
    name: "Quillotana Operativa",
    short_name: "Operativa",
    description: "Gestión integral de operaciones — Grupo La Quillotana",
    // Mantener identidad y arranque offline ya instalado hasta validar arranque
    // frío del selector en iOS. Login y menú navegan siempre a /modulos.
    id: "/despachos",
    start_url: "/despachos",
    scope: "/",
    display: "standalone",
    orientation: "portrait-primary",
    background_color: "#000000",
    theme_color: "#000000",
    lang: "es",
    icons: [
      {
        src: "/icons/pwa/icon-192.png",
        sizes: "192x192",
        type: "image/png",
        purpose: "any",
      },
      {
        src: "/icons/pwa/icon-512.png",
        sizes: "512x512",
        type: "image/png",
        purpose: "any",
      },
      {
        src: "/icons/pwa/icon-maskable-512.png",
        sizes: "512x512",
        type: "image/png",
        purpose: "maskable",
      },
    ],
  }
}
