import Link from "next/link"

export default function OfflinePage() {
  return (
    <main className="main">
      <div className="card">
        <h2 style={{ marginTop: 0 }}>Sin conexión</h2>
        <p className="muted">
          No hay red en este momento. Si ya abriste tus despachos con internet,
          puedes consultar la información sincronizada (solo lectura).
        </p>
        <p className="muted" style={{ fontSize: "0.85rem" }}>
          Entregas, GPS, fotografías, R2 y WhatsApp requieren conexión. No se
          registra nada en el servidor mientras estés offline.
        </p>
        <div className="btn-row" style={{ marginTop: 12 }}>
          <Link href="/despachos" className="btn btn-primary" style={{ textAlign: "center" }}>
            Ir a Mis despachos
          </Link>
          <Link href="/" className="btn btn-secondary" style={{ textAlign: "center" }}>
            Reintentar
          </Link>
        </div>
      </div>
    </main>
  )
}
