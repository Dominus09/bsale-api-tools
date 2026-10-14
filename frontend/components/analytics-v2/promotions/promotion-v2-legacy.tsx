"use client"

import { useCallback, useEffect, useState } from "react"
import { Loader2 } from "lucide-react"

import { Alert, AlertDescription } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { getLegacyPromotions, importLegacyPromotion } from "@/lib/analytics-v2/api"
import { formatDate, formatMoney, formatPct } from "@/lib/analytics-v2/labels"
import { LEGACY_MAPPING_LABELS } from "@/lib/analytics-v2/promotions-labels"
import type { LegacyResponse } from "@/lib/analytics-v2/promotions-types"

const LEGACY_STATUS_LABELS: Record<string, string> = {
  activa: "Activa",
  inactiva: "Inactiva",
  programada: "Programada",
  vencida: "Vencida",
}

export function PromotionLegacyPanel({
  companyId,
  canImport,
  tablesReady,
  onOpenCampaign,
  onImported,
}: {
  companyId: number
  canImport: boolean
  tablesReady: boolean
  onOpenCampaign: (campaignId: number) => void
  onImported: () => void
}) {
  const [data, setData] = useState<LegacyResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [busy, setBusy] = useState<number | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [notice, setNotice] = useState<string | null>(null)

  const load = useCallback(
    (signal?: AbortSignal) => {
      setLoading(true)
      setError(null)
      getLegacyPromotions(companyId, signal)
        .then(setData)
        .catch((e: unknown) => {
          if (!signal?.aborted) setError(e instanceof Error ? e.message : "Error al cargar promociones anteriores")
        })
        .finally(() => {
          if (!signal?.aborted) setLoading(false)
        })
    },
    [companyId],
  )

  useEffect(() => {
    const ac = new AbortController()
    load(ac.signal)
    return () => ac.abort()
  }, [load])

  const runImport = async (promotionId: number) => {
    setBusy(promotionId)
    setError(null)
    setNotice(null)
    try {
      const res = await importLegacyPromotion(companyId, promotionId)
      setNotice(
        `Importada como borrador (campaña #${res.campaign.id}): ${res.imported} producto(s); ${res.skipped.length} omitido(s) por no poder identificarse con certeza.`,
      )
      onImported()
      load()
    } catch (e) {
      setError(e instanceof Error ? e.message : "No se pudo importar")
    } finally {
      setBusy(null)
    }
  }

  return (
    <div className="space-y-3">
      <Alert>
        <AlertDescription>
          Promociones registradas en el módulo anterior. Se muestran en solo lectura y nunca otorgan excepción de margen: no tienen variante ni lista
          identificadas con certeza, aprobación ni usuario. Importarlas crea un borrador V2 que debe revisarse y aprobarse; el registro original no se
          modifica.
        </AlertDescription>
      </Alert>
      {error ? (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}
      {notice ? (
        <Alert>
          <AlertDescription>{notice}</AlertDescription>
        </Alert>
      ) : null}
      {loading && !data ? (
        <div className="flex justify-center py-8">
          <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
        </div>
      ) : null}
      {data && data.promotions.length === 0 ? <p className="text-sm text-muted-foreground">Sin promociones anteriores para esta empresa.</p> : null}
      {data?.promotions.map((p) => (
        <div key={p.promotion_id} className="rounded-md border">
          <div className="flex flex-wrap items-center justify-between gap-2 border-b bg-muted/30 px-3 py-2">
            <div className="text-sm">
              <span className="font-medium">
                Promoción #{p.promotion_id} · {p.tipo ?? "—"} · {p.canal ?? "—"}
              </span>
              <span className="ml-2 text-xs text-muted-foreground">
                {formatDate(p.starts_on)} → {formatDate(p.ends_on)} · {LEGACY_STATUS_LABELS[p.legacy_status] ?? p.legacy_status} · {p.items} ítem(s)
              </span>
            </div>
            {p.imported_campaign_id ? (
              <Button size="sm" variant="ghost" onClick={() => onOpenCampaign(p.imported_campaign_id as number)}>
                Ver campaña V2 #{p.imported_campaign_id}
              </Button>
            ) : canImport && tablesReady ? (
              <Button size="sm" variant="outline" disabled={busy != null} onClick={() => void runImport(p.promotion_id)}>
                {busy === p.promotion_id ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                Importar como borrador
              </Button>
            ) : null}
          </div>
          <div className="overflow-x-auto">
            <table className="w-full min-w-[760px] text-xs">
              <thead className="text-left text-[11px] uppercase tracking-wide text-muted-foreground">
                <tr>
                  <th className="px-3 py-1.5 font-medium">Código de barras</th>
                  <th className="px-3 py-1.5 font-medium">Lista (texto)</th>
                  <th className="px-3 py-1.5 text-right font-medium">Precio normal congelado</th>
                  <th className="px-3 py-1.5 text-right font-medium">Precio oferta congelado</th>
                  <th className="px-3 py-1.5 font-medium">Identificación en RAW</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border/60">
                {p.snapshots.map((s) => (
                  <tr key={s.snapshot_id}>
                    <td className="px-3 py-1.5">{s.barcode ?? "—"}</td>
                    <td className="px-3 py-1.5">{s.price_list_text ?? "—"}</td>
                    <td className="px-3 py-1.5 text-right tabular-nums">{formatMoney(s.regular_price)}</td>
                    <td className="px-3 py-1.5 text-right tabular-nums">
                      {formatMoney(s.sale_price)} {s.discount_pct != null ? <span className="text-muted-foreground">({formatPct(s.discount_pct)})</span> : null}
                    </td>
                    <td className="px-3 py-1.5">
                      {LEGACY_MAPPING_LABELS[s.mapping_status] ?? s.mapping_status}
                      {s.variant_id ? <span className="text-muted-foreground"> · variante {s.variant_id}, lista {s.price_list_id ?? "—"}</span> : null}
                    </td>
                  </tr>
                ))}
                {p.snapshots.length === 0 ? (
                  <tr>
                    <td colSpan={5} className="px-3 py-3 text-center text-muted-foreground">
                      Sin precios congelados.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </div>
      ))}
    </div>
  )
}
