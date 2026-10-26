"use client"

import { useEffect, useState } from "react"
import { Loader2 } from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from "@/components/ui/sheet"
import { getPromotionCampaign, removeCampaignItem, transitionCampaign, updateCampaignItem } from "@/lib/analytics-v2/api"
import { formatDate, formatDateTime, formatMoney, formatPct, formatQty } from "@/lib/analytics-v2/labels"
import { CLEARANCE_LABELS } from "@/lib/analytics-v2/promotions-labels"
import type { CampaignDetail, CampaignItem, WriteWarning } from "@/lib/analytics-v2/promotions-types"

import { ApprovalBadge, ExceptionBadge, KindBadge, TimelineBadge, VerificationBadge } from "./promotion-badges"
import { AddItemDialog, CampaignFormDialog, ReasonDialog, warningText } from "./promotion-v2-dialogs"
import { PromotionHistoryTable } from "./promotion-v2-tables"

type Transition = "submit" | "approve" | "reject" | "cancel"

const TRANSITION_COPY: Record<Transition, { title: string; label: string; description: string; required: boolean }> = {
  submit: {
    title: "Enviar a aprobación",
    label: "Enviar",
    description: "La campaña quedará pendiente de aprobación por gerencia.",
    required: false,
  },
  approve: {
    title: "Aprobar campaña",
    label: "Aprobar",
    description:
      "Aprobar no cambia precios en Bsale. La excepción de margen solo se aplicará cuando el precio en Bsale coincida con el promocional.",
    required: false,
  },
  reject: { title: "Rechazar campaña", label: "Rechazar", description: "Indique el motivo del rechazo.", required: true },
  cancel: {
    title: "Cancelar campaña",
    label: "Cancelar campaña",
    description: "La campaña deja de valer desde hoy. El historial se conserva. Si el precio sigue en Bsale, debe revertirse allí.",
    required: true,
  },
}

type ItemAction = { kind: "price" | "remove"; item: CampaignItem } | null

export function PromotionCampaignSheet({
  companyId,
  campaignId,
  onOpenChange,
  onChanged,
}: {
  companyId: number
  campaignId: number | null
  onOpenChange: (open: boolean) => void
  onChanged: () => void
}) {
  const [campaign, setCampaign] = useState<CampaignDetail | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [warnings, setWarnings] = useState<WriteWarning[]>([])
  const [transition, setTransition] = useState<Transition | null>(null)
  const [itemAction, setItemAction] = useState<ItemAction>(null)
  const [editOpen, setEditOpen] = useState(false)
  const [addOpen, setAddOpen] = useState(false)

  useEffect(() => {
    setWarnings([])
    if (campaignId == null) {
      setCampaign(null)
      return
    }
    const ac = new AbortController()
    setLoading(true)
    setError(null)
    getPromotionCampaign(companyId, campaignId, ac.signal)
      .then(setCampaign)
      .catch((e: unknown) => {
        if (!ac.signal.aborted) setError(e instanceof Error ? e.message : "Error al cargar la campaña")
      })
      .finally(() => {
        if (!ac.signal.aborted) setLoading(false)
      })
    return () => ac.abort()
  }, [companyId, campaignId])

  const applied = (next: CampaignDetail, w: WriteWarning[] = []) => {
    setCampaign(next)
    setWarnings(w)
    onChanged()
  }

  const actions = campaign?.allowed_actions ?? []
  const canEdit = actions.includes("edit")
  const activeItems = campaign?.items.filter((i) => i.item_status === "activo") ?? []
  const removedItems = campaign?.items.filter((i) => i.item_status !== "activo") ?? []

  return (
    <Sheet open={campaignId != null} onOpenChange={onOpenChange}>
      <SheetContent side="right" className="w-full overflow-y-auto sm:max-w-5xl">
        <SheetHeader>
          <SheetTitle>{campaign?.name ?? (loading ? "Cargando…" : "Campaña")}</SheetTitle>
          <SheetDescription>
            {campaign
              ? `${formatDate(campaign.starts_on)} → ${formatDate(campaign.ends_on)} · ${campaign.meta.company_name} · creada por ${campaign.created_by ?? "—"}`
              : ""}
          </SheetDescription>
        </SheetHeader>

        <div className="space-y-4 px-4 pb-6">
          {error ? (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          ) : null}
          {loading && !campaign ? (
            <div className="flex justify-center py-10">
              <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
            </div>
          ) : null}

          {campaign ? (
            <>
              <div className="flex flex-wrap items-center gap-2">
                <KindBadge kind={campaign.kind} />
                <TimelineBadge timeline={campaign.timeline} />
                <ApprovalBadge status={campaign.approval_status} />
                {campaign.responsible ? <span className="text-xs text-muted-foreground">Responsable: {campaign.responsible}</span> : null}
                {campaign.legacy_promotion_id ? (
                  <span className="text-xs text-muted-foreground">Importada de promoción anterior #{campaign.legacy_promotion_id}</span>
                ) : null}
              </div>

              <div className="grid gap-2 text-xs md:grid-cols-3">
                <div className="rounded-md border p-2">
                  <p className="text-[11px] uppercase text-muted-foreground">Envío a aprobación</p>
                  <p>{campaign.submitted_by ? `${campaign.submitted_by} · ${formatDateTime(campaign.submitted_at)}` : "—"}</p>
                </div>
                <div className="rounded-md border p-2">
                  <p className="text-[11px] uppercase text-muted-foreground">Aprobación</p>
                  <p>{campaign.approved_by ? `${campaign.approved_by} · ${formatDateTime(campaign.approved_at)}` : "—"}</p>
                </div>
                <div className="rounded-md border p-2">
                  <p className="text-[11px] uppercase text-muted-foreground">Rechazo / cancelación</p>
                  <p>
                    {campaign.cancelled_by
                      ? `Cancelada por ${campaign.cancelled_by} · ${formatDateTime(campaign.cancelled_at)}`
                      : campaign.rejected_by
                        ? `Rechazada por ${campaign.rejected_by} · ${formatDateTime(campaign.rejected_at)}`
                        : "—"}
                  </p>
                  {campaign.status_reason ? <p className="text-muted-foreground">Motivo: {campaign.status_reason}</p> : null}
                </div>
              </div>
              {campaign.notes ? <p className="whitespace-pre-wrap text-sm text-muted-foreground">{campaign.notes}</p> : null}

              <div className="flex flex-wrap gap-2">
                {canEdit ? (
                  <>
                    <Button size="sm" variant="outline" onClick={() => setEditOpen(true)}>
                      Editar datos
                    </Button>
                    <Button size="sm" variant="outline" onClick={() => setAddOpen(true)}>
                      Agregar producto
                    </Button>
                  </>
                ) : null}
                {(["submit", "approve", "reject", "cancel"] as Transition[])
                  .filter((a) => actions.includes(a))
                  .map((a) => (
                    <Button key={a} size="sm" variant={a === "approve" ? "default" : a === "submit" ? "secondary" : "outline"} onClick={() => setTransition(a)}>
                      {TRANSITION_COPY[a].label}
                    </Button>
                  ))}
              </div>

              {warnings.length ? (
                <Alert>
                  <AlertTitle>Registrado con advertencias</AlertTitle>
                  <AlertDescription>{warnings.map(warningText).join(" ")}</AlertDescription>
                </Alert>
              ) : null}

              <section className="space-y-2">
                <h3 className="text-sm font-semibold">Productos ({activeItems.length})</h3>
                <div className="overflow-x-auto rounded-md border">
                  <table className="w-full min-w-[980px] text-sm">
                    <thead className="bg-muted/40 text-left text-[11px] uppercase tracking-wide text-muted-foreground">
                      <tr>
                        <th className="px-3 py-2 font-medium">Producto</th>
                        <th className="px-3 py-2 font-medium">Lista</th>
                        <th className="px-3 py-2 text-right font-medium">Normal (registro)</th>
                        <th className="px-3 py-2 text-right font-medium">Promo</th>
                        <th className="px-3 py-2 text-right font-medium">Bsale hoy</th>
                        <th className="px-3 py-2 font-medium">Verificación / excepción</th>
                        <th className="px-3 py-2 font-medium" />
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-border/60">
                      {activeItems.map((i) => (
                        <tr key={i.item_id} className="align-top">
                          <td className="max-w-[240px] px-3 py-2">
                            <span className="block truncate font-medium">{i.product_name ?? `Variante ${i.variant_id}`}</span>
                            <span className="block truncate text-[11px] text-muted-foreground">
                              {i.sku_code ?? "—"} · {i.barcode ?? "—"}
                            </span>
                            {campaign.kind === "remate" ? (
                              <span className="block text-[11px] text-orange-800 dark:text-orange-300">
                                {i.clearance_reason ? CLEARANCE_LABELS[i.clearance_reason] : "Sin motivo"} · stock ref. {formatQty(i.reference_stock)} (
                                {formatDate(i.reference_stock_fetched_at)})
                              </span>
                            ) : null}
                            {i.notes ? <span className="block text-[11px] text-muted-foreground">{i.notes}</span> : null}
                          </td>
                          <td className="px-3 py-2 text-xs">
                            {i.price_list.label ?? i.price_list.name}
                          </td>
                          <td className="px-3 py-2 text-right tabular-nums">{formatMoney(i.normal_gross_price)}</td>
                          <td className="px-3 py-2 text-right tabular-nums font-medium">
                            {formatMoney(i.promo_gross_price)}
                            <span className="block text-[11px] text-muted-foreground">{i.discount_pct != null ? formatPct(i.discount_pct) : ""}</span>
                          </td>
                          <td className="px-3 py-2 text-right tabular-nums">
                            {formatMoney(i.bsale_current_gross)}
                            <span className="block text-[11px] text-muted-foreground">{formatDateTime(i.bsale_fetched_at)}</span>
                          </td>
                          <td className="max-w-[240px] space-y-1 px-3 py-2">
                            <VerificationBadge verification={i.verification} />
                            <ExceptionBadge check={i.exception_check} />
                          </td>
                          <td className="whitespace-nowrap px-3 py-2 text-right">
                            {canEdit ? (
                              <>
                                <Button size="sm" variant="ghost" onClick={() => setItemAction({ kind: "price", item: i })}>
                                  Precio
                                </Button>
                                <Button size="sm" variant="ghost" className="text-red-700 dark:text-red-300" onClick={() => setItemAction({ kind: "remove", item: i })}>
                                  Retirar
                                </Button>
                              </>
                            ) : null}
                          </td>
                        </tr>
                      ))}
                      {activeItems.length === 0 ? (
                        <tr>
                          <td colSpan={7} className="px-3 py-6 text-center text-muted-foreground">
                            Sin productos.
                          </td>
                        </tr>
                      ) : null}
                    </tbody>
                  </table>
                </div>
                {removedItems.length ? (
                  <p className="text-xs text-muted-foreground">
                    Retirados: {removedItems.map((i) => `${i.product_name ?? i.variant_id} (${i.removed_by ?? "—"}, ${formatDate(i.removed_at)})`).join(" · ")}
                  </p>
                ) : null}
              </section>

              <section className="space-y-2">
                <h3 className="text-sm font-semibold">Medición comercial</h3>
                <div className="grid gap-2 text-xs md:grid-cols-4">
                  {(
                    [
                      ["Antes", campaign.measurement.before],
                      ["Durante", campaign.measurement.during],
                      ["Después", campaign.measurement.after],
                    ] as const
                  ).map(([label, w]) => (
                    <div key={label} className="rounded-md border p-2">
                      <p className="text-[11px] uppercase text-muted-foreground">{label}</p>
                      <p>{w ? `${formatDate(w.from)} → ${formatDate(w.to)}` : "Aún no ocurre"}</p>
                    </div>
                  ))}
                  <div className="rounded-md border p-2">
                    <p className="text-[11px] uppercase text-muted-foreground">Duración</p>
                    <p>
                      Planificada {campaign.measurement.planned_days} d · real {campaign.measurement.real_days} d
                    </p>
                  </div>
                </div>
                <p className="text-xs text-muted-foreground">
                  Ventanas preparadas para comparar unidades, venta neta, rotación y stock. Las métricas aún no están implementadas: no se muestran cifras.
                </p>
              </section>

              <section className="space-y-2">
                <h3 className="text-sm font-semibold">Historial (no editable)</h3>
                <PromotionHistoryTable events={campaign.history} loading={false} showCampaign={false} />
              </section>
            </>
          ) : null}
        </div>

        {campaign ? (
          <>
            <CampaignFormDialog
              companyId={companyId}
              open={editOpen}
              campaign={campaign}
              defaultKind={campaign.kind}
              onOpenChange={setEditOpen}
              onSaved={(c) => applied(c)}
            />
            <AddItemDialog companyId={companyId} campaign={campaign} open={addOpen} onOpenChange={setAddOpen} onSaved={applied} />
            <ReasonDialog
              open={transition != null}
              title={transition ? TRANSITION_COPY[transition].title : ""}
              description={transition ? TRANSITION_COPY[transition].description : ""}
              required={transition ? TRANSITION_COPY[transition].required : false}
              confirmLabel={transition ? TRANSITION_COPY[transition].label : ""}
              onOpenChange={(open) => {
                if (!open) setTransition(null)
              }}
              onConfirm={async (reason) => {
                if (!transition) return
                applied(await transitionCampaign(companyId, campaign.id, transition, { version: campaign.version, reason: reason || null }))
              }}
            />
            <ReasonDialog
              open={itemAction != null}
              title={itemAction?.kind === "remove" ? "Retirar producto" : "Cambiar precio promocional"}
              description={
                itemAction?.kind === "remove"
                  ? "El producto queda retirado; el registro y su historial se conservan."
                  : "El cambio queda en el historial con el precio anterior. En una campaña aprobada, la devuelve a aprobación."
              }
              required={itemAction?.kind === "remove"}
              confirmLabel={itemAction?.kind === "remove" ? "Retirar" : "Guardar precio"}
              priceField={itemAction?.kind === "price" ? { label: "Nuevo precio bruto", initial: itemAction.item.promo_gross_price ?? "" } : null}
              onOpenChange={(open) => {
                if (!open) setItemAction(null)
              }}
              onConfirm={async (reason, price) => {
                if (!itemAction) return
                const res =
                  itemAction.kind === "remove"
                    ? await removeCampaignItem(companyId, campaign.id, itemAction.item.item_id, { version: campaign.version, reason })
                    : await updateCampaignItem(companyId, campaign.id, itemAction.item.item_id, {
                        version: campaign.version,
                        promo_gross_price: price ?? undefined,
                        reason: reason || null,
                      })
                applied(res.campaign, res.warnings)
              }}
            />
          </>
        ) : null}
      </SheetContent>
    </Sheet>
  )
}
