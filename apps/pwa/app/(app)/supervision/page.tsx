"use client"

import Link from "next/link"
import { setActiveModuleId } from "@/lib/modules"

export default function SupervisionPlaceholderPage() {
  return (
    <div className="module-placeholder">
      <p className="module-picker-kicker">Módulo Supervisión</p>
      <h2>Próximamente</h2>
      <p className="muted">
        Tienes el perfil supervisor asignado. El centro de control en PWA se habilitará
        en una etapa posterior (el ERP ya tiene paneles de operaciones).
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
