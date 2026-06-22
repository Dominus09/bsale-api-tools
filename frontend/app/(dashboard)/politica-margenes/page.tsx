"use client"

import Link from "next/link"
import { useCallback, useEffect, useMemo, useState } from "react"
import {
  AlertTriangle,
  FileSpreadsheet,
  Loader2,
  RefreshCw,
  Save,
  Scale,
} from "lucide-react"

import {
  downloadMarginRulesExcel,
  getCompanies,
  getMarginRules,
  getPriceLists,
  patchMarginRule,
  type Company,
  type MarginRuleRow,
  type PriceListRef,
} from "@/lib/api"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Textarea } from "@/components/ui/textarea"

const ALL = "__all__"

type DraftRow = {
  min_margin: string
  max_margin: string
  active: boolean
  notes: string
}

function emptyDraft(row: MarginRuleRow): DraftRow {
  return {
    min_margin: String(row.min_margin ?? 0),
    max_margin: String(row.max_margin ?? 0),
    active: Boolean(row.active),
    notes: row.notes ?? "",
  }
}

function isDirty(row: MarginRuleRow, draft: DraftRow): boolean {
  return (
    String(row.min_margin) !== draft.min_margin ||
    String(row.max_margin) !== draft.max_margin ||
    Boolean(row.active) !== draft.active ||
    (row.notes ?? "") !== draft.notes
  )
}

function parseNum(s: string): number | null {
  const t = s.trim().replace(",", ".")
  if (!t) return null
  const n = Number(t)
  return Number.isFinite(n) ? n : null
}

export default function PoliticaMargenesPage() {
  const [items, setItems] = useState<MarginRuleRow[]>([])
  const [drafts, setDrafts] = useState<Record<number, DraftRow>>({})
  const [loading, setLoading] = useState(true)
  const [busy, setBusy] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [msg, setMsg] = useState<string | null>(null)
  const [rowWarnings, setRowWarnings] = useState<Record<number, string>>({})

  const [companies, setCompanies] = useState<Company[]>([])
  const [priceLists, setPriceLists] = useState<PriceListRef[]>([])
  const [companyFilter, setCompanyFilter] = useState(ALL)
  const [priceListFilter, setPriceListFilter] = useState(ALL)
  const [productTypeFilter, setProductTypeFilter] = useState(ALL)
  const [activeFilter, setActiveFilter] = useState<"all" | "active" | "inactive">("all")

  const productTypeOptions = useMemo(() => {
    const map = new Map<string, number | null>()
    for (const r of items) {
      const name = r.product_type_name ?? "(Todos los tipos)"
      if (!map.has(name)) {
        map.set(name, r.product_type_id ?? null)
      }
    }
    return Array.from(map.entries())
      .map(([name, id]) => ({ name, id }))
      .sort((a, b) => a.name.localeCompare(b.name, "es"))
  }, [items])

  const filteredItems = useMemo(() => {
    return items.filter((r) => {
      if (productTypeFilter !== ALL) {
        const opt = productTypeOptions.find((o) => String(o.id ?? "null") === productTypeFilter)
        if (opt) {
          if (opt.id == null) {
            if (r.product_type_id != null) return false
          } else if (r.product_type_id !== opt.id) return false
        }
      }
      return true
    })
  }, [items, productTypeFilter, productTypeOptions])

  const loadRules = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const params: {
        company_id?: number
        price_list_id?: number
        active?: "all" | "active" | "inactive"
      } = { active: activeFilter }
      if (companyFilter !== ALL) params.company_id = Number(companyFilter)
      if (priceListFilter !== ALL) params.price_list_id = Number(priceListFilter)
      const data = await getMarginRules(params)
      setItems(data.items)
      setDrafts(Object.fromEntries(data.items.map((r) => [r.id, emptyDraft(r)])))
      setRowWarnings({})
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al cargar reglas")
    } finally {
      setLoading(false)
    }
  }, [activeFilter, companyFilter, priceListFilter])

  useEffect(() => {
    void getCompanies()
      .then(setCompanies)
      .catch(() => setCompanies([]))
  }, [])

  useEffect(() => {
    if (companyFilter === ALL) {
      setPriceLists([])
      setPriceListFilter(ALL)
      return
    }
    void getPriceLists(Number(companyFilter))
      .then(setPriceLists)
      .catch(() => setPriceLists([]))
  }, [companyFilter])

  useEffect(() => {
    void loadRules()
  }, [loadRules])

  const updateDraft = (id: number, patch: Partial<DraftRow>) => {
    setDrafts((prev) => ({
      ...prev,
      [id]: { ...prev[id], ...patch },
    }))
  }

  const saveRow = async (row: MarginRuleRow) => {
    const draft = drafts[row.id]
    if (!draft) return
    const minN = parseNum(draft.min_margin)
    const maxN = parseNum(draft.max_margin)
    if (minN === null || maxN === null) {
      setMsg("Los márgenes deben ser numéricos.")
      return
    }
    if (minN > maxN) {
      setMsg("El margen mínimo no puede ser mayor al máximo.")
      return
    }
    setBusy(`save-${row.id}`)
    setMsg(null)
    try {
      const res = await patchMarginRule(row.id, {
        min_margin: minN,
        max_margin: maxN,
        active: draft.active,
        notes: draft.notes.trim() || null,
      })
      setItems((prev) => prev.map((r) => (r.id === row.id ? res.item : r)))
      setDrafts((prev) => ({ ...prev, [row.id]: emptyDraft(res.item) }))
      if (res.warnings?.length) {
        setRowWarnings((prev) => ({ ...prev, [row.id]: res.warnings![0]! }))
      } else {
        setRowWarnings((prev) => {
          const next = { ...prev }
          delete next[row.id]
          return next
        })
      }
      setMsg(`Regla guardada (${row.rule_key}).`)
    } catch (e: unknown) {
      setMsg(e instanceof Error ? e.message : "Error al guardar")
    } finally {
      setBusy(null)
    }
  }

  const exportExcel = async () => {
    setBusy("export")
    try {
      await downloadMarginRulesExcel({
        company_id: companyFilter !== ALL ? Number(companyFilter) : undefined,
        price_list_id: priceListFilter !== ALL ? Number(priceListFilter) : undefined,
        active: activeFilter,
      })
      setMsg("Excel descargado.")
    } catch (e: unknown) {
      setMsg(e instanceof Error ? e.message : "Error al exportar")
    } finally {
      setBusy(null)
    }
  }

  return (
    <div className="space-y-6 p-6">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="mb-1 flex items-center gap-2 text-muted-foreground">
            <Scale className="size-4" aria-hidden />
            <span className="text-xs font-medium uppercase tracking-wide">Analítica</span>
          </div>
          <h1 className="text-2xl font-bold text-foreground">Política de Márgenes</h1>
          <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
            Reglas en{" "}
            <code className="rounded bg-muted px-1 text-xs">bsale.margin_rules</code>. Alimentan{" "}
            <Link href="/margins" className="text-primary hover:underline">
              Analítica → Márgenes
            </Link>{" "}
            y las alertas de productos bajo margen.
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button
            type="button"
            variant="outline"
            size="sm"
            disabled={!!busy || loading}
            onClick={() => void loadRules()}
          >
            <RefreshCw className="mr-1 size-3.5" />
            Actualizar
          </Button>
          <Button
            type="button"
            variant="secondary"
            size="sm"
            disabled={!!busy || loading}
            onClick={() => void exportExcel()}
          >
            <FileSpreadsheet className="mr-1 size-3.5" />
            Exportar Excel
          </Button>
        </div>
      </div>

      <Alert>
        <AlertTitle>Uso operativo</AlertTitle>
        <AlertDescription className="text-sm">
          Los cambios aquí no recalculan precios automáticamente. Afectan cómo se clasifican y
          alertan los márgenes en el módulo de análisis. Revise{" "}
          <Link href="/alerts" className="text-primary hover:underline">
            Alertas
          </Link>{" "}
          tras actualizar reglas críticas.
        </AlertDescription>
      </Alert>

      {msg ? (
        <Alert>
          <AlertDescription>{msg}</AlertDescription>
        </Alert>
      ) : null}

      {error ? (
        <Alert variant="destructive">
          <AlertTriangle className="size-4" />
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      <div className="grid gap-3 rounded-lg border bg-muted/20 p-4 sm:grid-cols-2 lg:grid-cols-4">
        <div className="space-y-1">
          <Label className="text-xs">Empresa</Label>
          <Select value={companyFilter} onValueChange={setCompanyFilter}>
            <SelectTrigger className="h-9">
              <SelectValue placeholder="Todas" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={ALL}>Todas</SelectItem>
              {companies.map((c) => (
                <SelectItem key={c.company_id} value={String(c.company_id)}>
                  {c.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="space-y-1">
          <Label className="text-xs">Lista de precios</Label>
          <Select
            value={priceListFilter}
            onValueChange={setPriceListFilter}
            disabled={companyFilter === ALL}
          >
            <SelectTrigger className="h-9">
              <SelectValue placeholder="Todas" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={ALL}>Todas</SelectItem>
              {priceLists.map((pl) => (
                <SelectItem key={pl.id} value={String(pl.id)}>
                  {pl.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="space-y-1">
          <Label className="text-xs">Tipo producto</Label>
          <Select value={productTypeFilter} onValueChange={setProductTypeFilter}>
            <SelectTrigger className="h-9">
              <SelectValue placeholder="Todos" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={ALL}>Todos</SelectItem>
              {productTypeOptions.map((o) => (
                <SelectItem key={o.name} value={String(o.id ?? "null")}>
                  {o.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="space-y-1">
          <Label className="text-xs">Estado</Label>
          <Select
            value={activeFilter}
            onValueChange={(v) => setActiveFilter(v as "all" | "active" | "inactive")}
          >
            <SelectTrigger className="h-9">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">Todos</SelectItem>
              <SelectItem value="active">Activo</SelectItem>
              <SelectItem value="inactive">Inactivo</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>

      {loading ? (
        <div className="flex items-center justify-center py-16 text-muted-foreground">
          <Loader2 className="mr-2 size-5 animate-spin" />
          Cargando reglas…
        </div>
      ) : (
        <div className="overflow-x-auto rounded-lg border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Empresa</TableHead>
                <TableHead>Lista de precios</TableHead>
                <TableHead>Tipo producto</TableHead>
                <TableHead className="w-24">Mín. %</TableHead>
                <TableHead className="w-24">Máx. %</TableHead>
                <TableHead className="w-16">Activo</TableHead>
                <TableHead className="min-w-[180px]">Notas</TableHead>
                <TableHead className="w-20" />
              </TableRow>
            </TableHeader>
            <TableBody>
              {filteredItems.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={8} className="py-10 text-center text-muted-foreground">
                    Sin reglas para los filtros seleccionados.
                  </TableCell>
                </TableRow>
              ) : (
                filteredItems.map((row) => {
                  const draft = drafts[row.id] ?? emptyDraft(row)
                  const dirty = isDirty(row, draft)
                  const minN = parseNum(draft.min_margin)
                  const maxN = parseNum(draft.max_margin)
                  const localWarn =
                    minN === 0 && maxN === 0
                      ? "Mín. y máx. en 0: sin restricción efectiva."
                      : minN !== null && maxN !== null && minN > maxN
                        ? "Mínimo mayor que máximo."
                        : rowWarnings[row.id]
                  return (
                    <TableRow key={row.id}>
                      <TableCell className="text-xs font-medium">{row.company_name ?? row.company_id}</TableCell>
                      <TableCell className="text-xs">{row.price_list_name ?? row.price_list_id}</TableCell>
                      <TableCell className="text-xs">{row.product_type_name ?? "—"}</TableCell>
                      <TableCell>
                        <Input
                          type="text"
                          inputMode="decimal"
                          className="h-8 text-xs tabular-nums"
                          value={draft.min_margin}
                          onChange={(e) => updateDraft(row.id, { min_margin: e.target.value })}
                        />
                      </TableCell>
                      <TableCell>
                        <Input
                          type="text"
                          inputMode="decimal"
                          className="h-8 text-xs tabular-nums"
                          value={draft.max_margin}
                          onChange={(e) => updateDraft(row.id, { max_margin: e.target.value })}
                        />
                      </TableCell>
                      <TableCell>
                        <Checkbox
                          checked={draft.active}
                          onCheckedChange={(v) => updateDraft(row.id, { active: v === true })}
                        />
                      </TableCell>
                      <TableCell>
                        <Textarea
                          rows={2}
                          className="min-h-8 resize-y text-xs"
                          value={draft.notes}
                          onChange={(e) => updateDraft(row.id, { notes: e.target.value })}
                        />
                        {localWarn ? (
                          <p className="mt-1 text-[10px] text-amber-700 dark:text-amber-400">
                            {localWarn}
                          </p>
                        ) : null}
                      </TableCell>
                      <TableCell>
                        <Button
                          type="button"
                          size="icon"
                          variant={dirty ? "default" : "ghost"}
                          className="size-8"
                          disabled={!!busy || !dirty}
                          title="Guardar fila"
                          onClick={() => void saveRow(row)}
                        >
                          {busy === `save-${row.id}` ? (
                            <Loader2 className="size-3.5 animate-spin" />
                          ) : (
                            <Save className="size-3.5" />
                          )}
                        </Button>
                      </TableCell>
                    </TableRow>
                  )
                })
              )}
            </TableBody>
          </Table>
        </div>
      )}

      {!loading && filteredItems.length > 0 ? (
        <p className="text-xs text-muted-foreground">
          {filteredItems.length} regla(s) · Clave técnica{" "}
          <code className="rounded bg-muted px-1">rule_key</code> no editable (ej.{" "}
          {filteredItems[0]?.rule_key})
        </p>
      ) : null}
    </div>
  )
}
