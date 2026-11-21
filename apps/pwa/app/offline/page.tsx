import Link from "next/link"

export default function OfflinePage() {
  return (
    <main className="main">
      <div className="card">
        <h2 style={{ marginTop: 0 }}>Sin conexión</h2>
        <p className="muted">
          Si ya sincronizaste un despacho, puedes consultar fichas y pedidos
          (Offline V1, solo lectura).
        </p>
        <p className="muted" style={{ fontSize: "0.85rem" }}>
          Offline V2 permite capturas locales (entrega / foto) solo con
          autorización previa y despacho descargado. WhatsApp y la confirmación
          en servidor requieren recuperar conexión.
        </p>
        <div className="btn-row" style={{ marginTop: 12 }}>
          <Link href="/despachos" className="btn btn-primary" style={{ textAlign: "center" }}>
            Ir a Mis despachos
          </Link>
          <Link href="/despachos" className="btn btn-secondary" style={{ textAlign: "center" }}>
            Reintentar
          </Link>
        </div>
      </div>
    </main>
  )
}
