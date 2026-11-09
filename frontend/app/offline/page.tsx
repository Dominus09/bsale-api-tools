import type { Metadata } from "next"

export const metadata: Metadata = {
  title: "Sin conexión — Quillotana ERP",
}

/**
 * Página informativa de falta de conexión (fallback del service worker).
 * No contiene datos comerciales ni simula guardado offline: la ERP requiere
 * internet para operar.
 */
export default function OfflinePage() {
  return (
    <main className="flex min-h-dvh flex-col items-center justify-center gap-5 bg-black px-6 text-center text-white">
      <img
        src="/icons/pwa/icon-192.png"
        alt="Quillotana ERP"
        className="h-20 w-20"
      />
      <div className="space-y-2">
        <h1 className="text-xl font-semibold">Sin conexión a internet</h1>
        <p className="mx-auto max-w-sm text-sm leading-relaxed text-white/65">
          Quillotana ERP necesita conexión para mostrar información
          actualizada. Ningún cambio quedó guardado mientras no haya conexión.
        </p>
      </div>
      <a
        href="/"
        className="inline-flex h-11 items-center rounded-md bg-white px-5 text-sm font-medium text-black"
      >
        Reintentar
      </a>
    </main>
  )
}
