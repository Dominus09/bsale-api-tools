import { permanentRedirect } from "next/navigation"

/** La gestión de pendientes vive en Rutero; URL antigua conservada para enlaces guardados. */
export default function PendientesPage() {
  permanentRedirect("/distribuidora/rutero")
}
