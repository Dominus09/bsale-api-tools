"use client"

import { useEffect, useState } from "react"
import { Loader2, Search } from "lucide-react"

import { Alert, AlertDescription } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Textarea } from "@/components/ui/textarea"
import { cn } from "@/lib/utils"
import { addCampaignItem, createCampaign, searchPromotionProducts, updateCampaign } from "@/lib/analytics-v2/api"
import { formatDateTime, formatMoney } from "@/lib/analytics-v2/labels"
import { CLEARANCE_LABELS, KIND_LABELS } from "@/lib/analytics-v2/promotions-labels"
import type {
  CampaignDetail,
  CampaignKind,
  ClearanceReason,
  ProductSearchItem,
  WriteWarning,
} from "@/lib/analytics-v2/promotions-types"

const labelClass = "flex flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground"
const selectClass = "h-9 rounded-md border border-input bg-background px-2 text-sm normal-case text-foreground"

function errorText(e: unknown, fallback: string): string {
  return e instanceof Error ? e.message : fallback
}

export function CampaignFormDialog({
  companyId,
  open,
  campaign,
  defaultKind,
  onOpenChange,
  onSaved,
}: {
  companyId: number
  open: boolean
  campaign: CampaignDetail | null
  defaultKind: CampaignKind
  onOpenChange: (open: boolean) => void
  onSaved: (campaign: CampaignDetail) => void
}) {
  const editing = campaign != null
  const [kind, setKind] = useState<CampaignKind>(defaultKind)
  const [name, setName] = useState("")
  const [startsOn, setStartsOn] = useState("")
  const [endsOn, setEndsOn] = useState("")
  const [responsible, setResponsible] = useState("")
  const [notes, setNotes] = useState("")
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!open) return
    setError(null)
    setKind(campaign?.kind ?? defaultKind)
    setName(campaign?.name ?? "")
    setStartsOn(campaign?.starts_on ?? "")
    setEndsOn(campaign?.ends_on ?? "")
    setResponsible(campaign?.responsible ?? "")
    setNotes(campaign?.notes ?? "")
  }, [open, campaign, defaultKind])

  const submit = async () => {
    setSaving(true)
    setError(null)
    try {
      const saved = editing
        ? await updateCampaign(companyId, campaign.id, {
            version: campaign.version,
            name: name.trim(),
            starts_on: startsOn,
            ends_on: endsOn,
            responsible: responsible.trim() || null,
            notes: notes.trim() || null,
          })
        : await createCampaign(companyId, {
            kind,
            name: name.trim(),
            starts_on: startsOn,
            ends_on: endsOn,
            responsible: responsible.trim() || null,
            notes: notes.trim() || null,
          })
      onSaved(saved)
      onOpenChange(false)
    } catch (e) {
      setError(errorText(e, "No se pudo guardar la campaña"))
    } finally {
      setSaving(false)
    }
  }

  const datesChange = editing && (startsOn !== campaign.starts_on || endsOn !== campaign.ends_on)

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>{editing ? "Editar campaña" : "Nueva campaña"}</DialogTitle>
          <DialogDescription>
            Registro administrativo. No modifica precios en Bsale: el precio debe aplicarse allí y luego se verifica contra BSALE RAW.
          </DialogDescription>
        </DialogHeader>
        <form
          className="grid gap-3"
          onSubmit={(e) => {
            e.preventDefault()
            void submit()
          }}
        >
          <div className="grid grid-cols-2 gap-3">
            <label className={labelClass}>
              Tipo
              <select className={selectClass} value={kind} disabled={editing} onChange={(e) => setKind(e.target.value as CampaignKind)}>
                {(Object.keys(KIND_LABELS) as CampaignKind[]).map((k) => (
                  <option key={k} value={k}>
                    {KIND_LABELS[k]}
                  </option>
                ))}
              </select>
            </label>
            <label className={labelClass}>
              Responsable{kind === "remate" ? " *" : ""}
              <Input value={responsible} maxLength={120} onChange={(e) => setResponsible(e.target.value)} />
            </label>
          </div>
          <label className={labelClass}>
            Nombre *
            <Input value={name} maxLength={120} required onChange={(e) => setName(e.target.value)} placeholder="Ej: Ofertas octubre Supermercado" />
          </label>
          <div className="grid grid-cols-2 gap-3">
            <label className={labelClass}>
              Inicio *
              <Input type="date" required value={startsOn} onChange={(e) => setStartsOn(e.target.value)} />
            </label>
            <label className={labelClass}>
              Término *
              <Input type="date" required value={endsOn} onChange={(e) => setEndsOn(e.target.value)} />
            </label>
          </div>
          <label className={labelClass}>
            Observaciones
            <Textarea value={notes} maxLength={1000} rows={3} onChange={(e) => setNotes(e.target.value)} />
          </label>
          {datesChange && campaign?.approval_status === "aprobada" ? (
            <p className="text-xs text-amber-700 dark:text-amber-300">Cambiar fechas de una campaña aprobada la devuelve a “Pendiente de aprobación”.</p>
          ) : null}
          {error ? (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          ) : null}
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)} disabled={saving}>
              Cancelar
            </Button>
            <Button type="submit" disabled={saving}>
              {saving ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
              Guardar
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}

const WARNING_LABELS: Record<string, string> = {
  sin_precio_bsale: "La variante no tiene precio vigente en Bsale para esa lista.",
  precio_promocional_no_menor_al_normal: "El precio promocional no es menor al precio Bsale actual.",
  superposicion_con_otra_campania: "El producto ya está en otra campaña con fechas superpuestas.",
}

export function warningText(w: WriteWarning): string {
  return WARNING_LABELS[w.code] ?? w.code
}

export function AddItemDialog({
  companyId,
  campaign,
  open,
  onOpenChange,
  onSaved,
}: {
  companyId: number
  campaign: CampaignDetail
  open: boolean
  onOpenChange: (open: boolean) => void
  onSaved: (campaign: CampaignDetail, warnings: WriteWarning[]) => void
}) {
  const [q, setQ] = useState("")
  const [results, setResults] = useState<ProductSearchItem[]>([])
  const [searching, setSearching] = useState(false)
  const [selected, setSelected] = useState<ProductSearchItem | null>(null)
  const [priceListId, setPriceListId] = useState("")
  const [promoPrice, setPromoPrice] = useState("")
  const [clearance, setClearance] = useState<ClearanceReason | "">("")
  const [notes, setNotes] = useState("")
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!open) return
    setQ("")
    setResults([])
    setSelected(null)
    setPriceListId("")
    setPromoPrice("")
    setClearance("")
    setNotes("")
    setError(null)
  }, [open])

  const search = async () => {
    if (q.trim().length < 2) return
    setSearching(true)
    setError(null)
    try {
      const res = await searchPromotionProducts(companyId, q.trim())
      setResults(res.items)
    } catch (e) {
      setError(errorText(e, "No se pudo buscar"))
    } finally {
      setSearching(false)
    }
  }

  const listPrice = selected?.prices.find((p) => String(p.price_list.id) === priceListId)
  const evaluated = new Set(campaign.meta.evaluated_price_list_ids)

  const submit = async () => {
    if (!selected || !priceListId) return
    setSaving(true)
    setError(null)
    try {
      const res = await addCampaignItem(companyId, campaign.id, {
        version: campaign.version,
        variant_id: selected.variant_id,
        price_list_id: Number(priceListId),
        promo_gross_price: promoPrice.trim(),
        clearance_reason: campaign.kind === "remate" ? clearance || null : null,
        notes: notes.trim() || null,
      })
      onSaved(res.campaign, res.warnings)
      onOpenChange(false)
    } catch (e) {
      setError(errorText(e, "No se pudo agregar el producto"))
    } finally {
      setSaving(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[85vh] overflow-y-auto sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>Agregar producto a “{campaign.name}”</DialogTitle>
          <DialogDescription>
            El precio normal y el stock de referencia se leen en el servidor desde BSALE RAW al registrar; no se ingresan a mano.
          </DialogDescription>
        </DialogHeader>
        <form
          className="flex gap-2"
          onSubmit={(e) => {
            e.preventDefault()
            void search()
          }}
        >
          <Input value={q} maxLength={60} onChange={(e) => setQ(e.target.value)} placeholder="SKU, código de barras o nombre" />
          <Button type="submit" variant="outline" disabled={searching || q.trim().length < 2}>
            {searching ? <Loader2 className="h-4 w-4 animate-spin" /> : <Search className="h-4 w-4" />}
          </Button>
        </form>
        {results.length ? (
          <ul className="max-h-56 divide-y overflow-y-auto rounded-md border text-sm">
            {results.map((r) => (
              <li key={r.variant_id}>
                <button
                  type="button"
                  className={cn("w-full px-3 py-2 text-left hover:bg-muted/40", selected?.variant_id === r.variant_id && "bg-muted")}
                  onClick={() => {
                    setSelected(r)
                    setPriceListId("")
                  }}
                >
                  <span className="block font-medium">
                    {r.product_name ?? `Variante ${r.variant_id}`}
                    {!r.variant_active ? <span className="ml-1 text-[11px] text-red-700 dark:text-red-300">(inactiva)</span> : null}
                  </span>
                  <span className="block text-[11px] text-muted-foreground">
                    {r.code ?? "—"} · {r.barcode ?? "—"}
                    {r.variant_name ? ` · ${r.variant_name}` : ""}
                  </span>
                </button>
              </li>
            ))}
          </ul>
        ) : null}
        {selected ? (
          <form
            className="grid gap-3"
            onSubmit={(e) => {
              e.preventDefault()
              void submit()
            }}
          >
            <div className="grid grid-cols-2 gap-3">
              <label className={labelClass}>
                Lista *
                <select className={selectClass} value={priceListId} required onChange={(e) => setPriceListId(e.target.value)}>
                  <option value="">Seleccione…</option>
                  {selected.prices
                    .filter((p) => evaluated.has(p.price_list.id))
                    .map((p) => (
                      <option key={p.price_list.id} value={p.price_list.id}>
                        {p.price_list.label ?? p.price_list.name} (#{p.price_list.id}) · {formatMoney(p.gross)}
                      </option>
                    ))}
                </select>
              </label>
              <label className={labelClass}>
                Precio {campaign.kind === "remate" ? "de remate" : "promocional"} bruto *
                <Input inputMode="decimal" required value={promoPrice} maxLength={14} onChange={(e) => setPromoPrice(e.target.value)} />
              </label>
            </div>
            {listPrice ? (
              <p className="text-xs text-muted-foreground">
                Precio Bsale actual: {formatMoney(listPrice.gross)} · leído {formatDateTime(listPrice.fetched_at)}
              </p>
            ) : null}
            {campaign.kind === "remate" ? (
              <label className={labelClass}>
                Motivo del remate *
                <select className={selectClass} value={clearance} required onChange={(e) => setClearance(e.target.value as ClearanceReason)}>
                  <option value="">Seleccione…</option>
                  {(Object.keys(CLEARANCE_LABELS) as ClearanceReason[]).map((c) => (
                    <option key={c} value={c}>
                      {CLEARANCE_LABELS[c]}
                    </option>
                  ))}
                </select>
              </label>
            ) : null}
            <label className={labelClass}>
              Observaciones
              <Textarea value={notes} maxLength={1000} rows={2} onChange={(e) => setNotes(e.target.value)} />
            </label>
            {campaign.approval_status === "aprobada" ? (
              <p className="text-xs text-amber-700 dark:text-amber-300">Agregar productos devuelve la campaña a “Pendiente de aprobación”.</p>
            ) : null}
            {error ? (
              <Alert variant="destructive">
                <AlertDescription>{error}</AlertDescription>
              </Alert>
            ) : null}
            <DialogFooter>
              <Button type="button" variant="outline" onClick={() => onOpenChange(false)} disabled={saving}>
                Cancelar
              </Button>
              <Button type="submit" disabled={saving || !priceListId || !promoPrice.trim()}>
                {saving ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                Agregar
              </Button>
            </DialogFooter>
          </form>
        ) : error ? (
          <Alert variant="destructive">
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        ) : null}
      </DialogContent>
    </Dialog>
  )
}

export function ReasonDialog({
  open,
  title,
  description,
  required,
  confirmLabel,
  priceField,
  onOpenChange,
  onConfirm,
}: {
  open: boolean
  title: string
  description: string
  required: boolean
  confirmLabel: string
  priceField?: { label: string; initial: string } | null
  onOpenChange: (open: boolean) => void
  onConfirm: (reason: string, price: string | null) => Promise<void>
}) {
  const [reason, setReason] = useState("")
  const [price, setPrice] = useState("")
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const initialPrice = priceField?.initial ?? ""

  useEffect(() => {
    if (!open) return
    setReason("")
    setPrice(initialPrice)
    setError(null)
  }, [open, initialPrice])

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
          <DialogDescription>{description}</DialogDescription>
        </DialogHeader>
        <form
          className="grid gap-3"
          onSubmit={async (e) => {
            e.preventDefault()
            setSaving(true)
            setError(null)
            try {
              await onConfirm(reason.trim(), priceField ? price.trim() : null)
              onOpenChange(false)
            } catch (err) {
              setError(errorText(err, "No se pudo completar la acción"))
            } finally {
              setSaving(false)
            }
          }}
        >
          {priceField ? (
            <label className={labelClass}>
              {priceField.label}
              <Input inputMode="decimal" required value={price} maxLength={14} onChange={(e) => setPrice(e.target.value)} />
            </label>
          ) : null}
          <label className={labelClass}>
            Motivo{required ? " *" : ""}
            <Textarea value={reason} required={required} maxLength={500} rows={3} onChange={(e) => setReason(e.target.value)} />
          </label>
          {error ? (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          ) : null}
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => onOpenChange(false)} disabled={saving}>
              Volver
            </Button>
            <Button type="submit" disabled={saving || (required && !reason.trim())}>
              {saving ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
              {confirmLabel}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}
