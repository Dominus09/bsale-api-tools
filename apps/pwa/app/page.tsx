import { redirect } from "next/navigation"

/** Entrada web: selector de módulos; la sesión instalada mantiene su ruta offline. */
export default function Home() {
  redirect("/modulos")
}
