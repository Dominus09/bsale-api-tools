import { permanentRedirect } from "next/navigation"

/** Clientes sin georef se gestionan desde Rutero; URL antigua conservada para enlaces guardados. */
export default function SinGeorefPage() {
  permanentRedirect("/distribuidora/rutero")
}
