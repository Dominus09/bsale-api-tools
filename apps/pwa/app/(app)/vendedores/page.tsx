"use client"

import Link from "next/link"
import { setActiveModuleId } from "@/lib/modules"

export default function VendedoresPlaceholderPage() {
  return (
    <div className="module-placeholder">
      <p className="module-picker-kicker">Módulo Vendedores</p>
      <h2>En preparación</h2>
      <p className="muted">
        El acceso está autorizado en tu perfil. La jornada comercial (ruta, visitas y
        recuperación de clientes) se habilitará tras la aprobación de arquitectura.
        No hay pantallas operativas ficticias activas.
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
