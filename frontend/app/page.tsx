import HomeClient from "./home-client"

/**
 * Fuerza render dinámico en la raíz para evitar 404 / páginas estáticas vacías
 * en algunos hosts (p. ej. reglas de caché o export incompleto).
 */
export const dynamic = "force-dynamic"

export default function HomePage() {
  return <HomeClient />
}
