import type { MetadataRoute } from "next"

/**
 * Web App Manifest — Quillotana ERP (PWA instalable).
 *
 * start_url "/" a propósito: la raíz resuelve la sesión existente
 * (login → selector de empresa → inicio según permisos), por lo que la app
 * instalada recupera la sesión al reabrir sin rutas especiales.
 * Íconos: emblema oficial aprobado (PNG cuadrado fondo negro), sin redibujar.
 */
export default function manifest(): MetadataRoute.Manifest {
  return {
    name: "Quillotana ERP",
    short_name: "Quillotana ERP",
    description: "Plataforma de gestión empresarial de Grupo La Quillotana",
    id: "/",
    start_url: "/",
    scope: "/",
    display: "standalone",
    orientation: "any",
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
