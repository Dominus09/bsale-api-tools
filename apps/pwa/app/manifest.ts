import type { MetadataRoute } from "next"

export default function manifest(): MetadataRoute.Manifest {
  return {
    name: "Quillotana Operativa",
    short_name: "Operativa",
    description: "PWA operativa de Grupo La Quillotana — choferes y terreno",
    id: "/despachos",
    start_url: "/despachos",
    scope: "/",
    display: "standalone",
    orientation: "portrait-primary",
    background_color: "#0f1115",
    theme_color: "#0f1115",
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
