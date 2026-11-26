"use client"

import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import type { SymbologyCatalog } from "@/lib/warehouse/types"
import { LAYOUT_COLORS, colorsForType, ELEMENT_TYPE_LABELS_ES } from "@/lib/warehouse/style"
import type { WarehouseElementType } from "@/lib/warehouse/types"

type Props = {
  open: boolean
  onOpenChange: (open: boolean) => void
  catalog: SymbologyCatalog | null
}

function Swatch({ fill, stroke, dash }: { fill?: string; stroke?: string; dash?: boolean }) {
  return (
    <span
      className="inline-block h-6 w-10 rounded-sm border"
      style={{
        background: fill || "transparent",
        borderColor: stroke || "#94a3b8",
        borderStyle: dash ? "dashed" : "solid",
        borderWidth: 2,
      }}
    />
  )
}

export function SymbologyDialog({ open, onOpenChange, catalog }: Props) {
  const nomenclature = catalog?.nomenclature || {}
  const examples = catalog?.nomenclature_examples || []
  const symbols = catalog?.symbols || []
  const colors = catalog?.colors_layout_view || {}

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[85vh] max-w-3xl overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Simbología — Layout físico</DialogTitle>
        </DialogHeader>
        <p className="text-sm text-muted-foreground">
          Vista Layout únicamente. No se muestran estados de inventario. Las nomenclaturas PAL, GL,
          CT y niveles de apilamiento figuran como futuras.
        </p>
        <Tabs defaultValue="nomenclature">
          <TabsList className="grid w-full grid-cols-3">
            <TabsTrigger value="nomenclature">Nomenclatura</TabsTrigger>
            <TabsTrigger value="symbols">Símbolos</TabsTrigger>
            <TabsTrigger value="colors">Colores</TabsTrigger>
          </TabsList>

          <TabsContent value="nomenclature" className="space-y-4 pt-3">
            <div className="grid gap-2 sm:grid-cols-2">
              {Object.entries(nomenclature).map(([code, meaning]) => (
                <div
                  key={code}
                  className="flex items-start gap-2 rounded-md border border-border px-3 py-2 text-sm"
                >
                  <code className="shrink-0 rounded bg-muted px-1.5 py-0.5 font-semibold text-foreground">
                    {code}
                  </code>
                  <span className="text-muted-foreground">{meaning}</span>
                </div>
              ))}
            </div>
            {examples.length > 0 ? (
              <div className="space-y-3">
                <h4 className="text-sm font-semibold text-foreground">Ejemplos gráficos</h4>
                {examples.map((ex) => (
                  <div key={ex.code} className="rounded-lg border border-border p-3">
                    <div className="mb-2 flex flex-wrap items-center gap-1">
                      {ex.parts.map((p, i) => (
                        <span key={p.token} className="flex items-center gap-1">
                          {i > 0 ? <span className="text-muted-foreground">-</span> : null}
                          <span className="rounded bg-lq-info/10 px-2 py-1 text-sm font-bold text-lq-info">
                            {p.token}
                          </span>
                        </span>
                      ))}
                      <span className="ml-2 font-mono text-muted-foreground">→ {ex.code}</span>
                    </div>
                    <ul className="space-y-1 text-sm text-muted-foreground">
                      {ex.parts.map((p) => (
                        <li key={p.token}>
                          <strong>{p.token}</strong>: {p.meaning}
                        </li>
                      ))}
                    </ul>
                  </div>
                ))}
              </div>
            ) : null}
          </TabsContent>

          <TabsContent value="symbols" className="space-y-2 pt-3">
            {(symbols.length
              ? symbols
              : Object.keys(ELEMENT_TYPE_LABELS_ES).map((t) => ({
                  type: t,
                  label_es: ELEMENT_TYPE_LABELS_ES[t as WarehouseElementType],
                  shape: "filled_rect",
                }))
            ).map((s) => {
              const c = colorsForType(s.type as WarehouseElementType)
              return (
                <div
                  key={s.type}
                  className="flex items-center gap-3 rounded-md border border-border px-3 py-2 text-sm"
                >
                  <Swatch fill={c.fill} stroke={c.stroke} />
                  <div>
                    <div className="font-medium text-foreground">{s.label_es}</div>
                    <div className="text-xs text-muted-foreground">
                      tipo `{s.type}` · forma {s.shape}
                    </div>
                  </div>
                </div>
              )
            })}
          </TabsContent>

          <TabsContent value="colors" className="space-y-3 pt-3">
            {Object.entries(colors).map(([key, val]) => (
              <div
                key={key}
                className="flex items-center gap-3 rounded-md border border-border px-3 py-2 text-sm"
              >
                <Swatch
                  fill={val.fill || LAYOUT_COLORS.background}
                  stroke={val.stroke || val.badge}
                  dash={Boolean(val.dash)}
                />
                <div>
                  <div className="font-medium text-foreground">{val.label_es}</div>
                  <div className="text-xs text-muted-foreground">{key}</div>
                </div>
              </div>
            ))}
            <p className="text-sm text-muted-foreground">
              {catalog?.colors_note ||
                "Evolución futura: ocupación, productos, vencimientos y FIFO/FEFO en otras vistas."}
            </p>
            {catalog?.colors_future_views?.length ? (
              <p className="text-xs text-muted-foreground">
                Vistas previstas: {catalog.colors_future_views.join(" · ")}
              </p>
            ) : null}
          </TabsContent>
        </Tabs>
      </DialogContent>
    </Dialog>
  )
}
