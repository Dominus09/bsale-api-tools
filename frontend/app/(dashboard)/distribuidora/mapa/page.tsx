import dynamic from "next/dynamic"

const MapaRuteroClient = dynamic(() => import("@/components/distribuidora/mapa-rutero-client"), {
  ssr: false,
  loading: () => (
    <div className="p-4">
      <div className="rounded-xl border border-border bg-white p-4 shadow-sm dark:bg-card">
        <div className="mb-3 flex justify-between">
          <div className="space-y-2">
            <div className="h-6 w-40 animate-pulse rounded bg-muted" />
            <div className="h-4 w-32 animate-pulse rounded bg-muted" />
          </div>
          <div className="flex gap-2">
            <div className="h-9 w-40 animate-pulse rounded-md bg-muted" />
            <div className="h-9 w-36 animate-pulse rounded-md bg-muted" />
          </div>
        </div>
        <div className="h-[75vh] animate-pulse rounded-lg bg-muted/50" />
      </div>
    </div>
  ),
})

export default function MapaPage() {
  return <MapaRuteroClient />
}
