export default function OfflinePage() {
  return (
    <main className="main">
      <div className="card">
        <h2>Sin conexión</h2>
        <p className="muted">
          No hay red en este momento. En P1 no se guardan despachos ni documentos
          offline. Vuelve a intentar cuando recuperes señal.
        </p>
      </div>
    </main>
  )
}
