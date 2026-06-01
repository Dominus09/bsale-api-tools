import { redirect } from "next/navigation"

/** Alias de ruta: misma pantalla que /distribuidora/orders */
export default function PreDespachoOcRedirectPage() {
  redirect("/distribuidora/orders")
}
