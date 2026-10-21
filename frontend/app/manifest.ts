import type { MetadataRoute } from "next"

/**
 * Web App Manifest — La Quillotana ERP (PWA instalable).
 *
 * start_url "/" a propósito: la raíz resuelve la sesión existente
 * (login → selector de empresa → inicio según permisos), por lo que la app
 * instalada recupera la sesión al reabrir sin rutas especiales.
 * Los íconos derivan del emblema oficial (sin redibujar): fondo Graphite.
 */
export default function manifest(): MetadataRoute.Manifest {
  return {
    name: "La Quillotana ERP",
    short_name: "Quillotana",
    description: "Plataforma de gestión empresarial de Grupo La Quillotana",
    id: "/",
    start_url: "/",
    scope: "/",
    display: "standalone",
    orientation: "any",
    background_color: "#17191E",
    theme_color: "#17191E",
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
