"use client"

import Link from "next/link"
import { setActiveModuleId } from "@/lib/modules"

export default function BodegaPlaceholderPage() {
  return (
    <div className="module-placeholder">
      <p className="module-picker-kicker">Módulo Bodega</p>
      <h2>Próximamente</h2>
      <p className="muted">
        Tienes el perfil bodega asignado. Este módulo aún no está operativo en la PWA.
      </p>
      <div className="module-placeholder-actions">
        <Link
          className="btn"
          href="/modulos"
          onClick={() => setActiveModuleId(null)}
        >
          Cambiar módulo
        </Link>
      </div>
    </div>
  )
}
