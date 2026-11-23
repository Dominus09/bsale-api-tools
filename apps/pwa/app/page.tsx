import { redirect } from "next/navigation"

/** Entrada: selector de módulo (chofer único auto-redirige a /despachos). */
export default function Home() {
  redirect("/modulos")
}
