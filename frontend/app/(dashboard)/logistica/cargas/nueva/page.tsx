"use client"

import { useRef, useState } from "react"
import { useRouter } from "next/navigation"
import { FileUp, Loader2 } from "lucide-react"

import {
  confirmCargaImport,
  previewCargaImport,
  type ImportPreview,
} from "@/lib/cargas-api"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { cn } from "@/lib/utils"

export default function NuevaCargaPage() {
  const router = useRouter()
  const inputRef = useRef<HTMLInputElement>(null)
  const [file, setFile] = useState<File | null>(null)
  const [preview, setPreview] = useState<ImportPreview | null>(null)
  const [pickingOverride, setPickingOverride] = useState("")
  const [loading, setLoading] = useState(false)
  const [importing, setImporting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function onFile(f: File | null) {
    setFile(f)
    setPreview(null)
    setError(null)
    if (!f) return
    setLoading(true)
    try {
      const data = await previewCargaImport(f)
      setPreview(data)
      setPickingOverride(data.picking_number || "")
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al parsear")
    } finally {
      setLoading(false)
    }
  }

  async function onConfirm() {
    if (!file || !preview?.can_import) return
    setImporting(true)
    setError(null)
    try {
      const load = await confirmCargaImport(file, {
        pickingNumber: pickingOverride || undefined,
        expectedFileHash: preview.file_hash || undefined,
      })
      router.push(`/logistica/cargas/${load.id}/certificar`)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al importar")
    } finally {
      setImporting(false)
    }
  }

  return (
    <div className="mx-auto max-w-3xl space-y-4 pb-16">
      <div>
        <h1 className="text-2xl font-bold">Nueva carga</h1>
        <p className="text-sm text-muted-foreground">
          Subir PDF o Excel de picking → preview → confirmar
        </p>
      </div>

      <div
        className="flex cursor-pointer flex-col items-center justify-center gap-2 rounded-xl border border-dashed bg-muted/30 px-4 py-12"
        onClick={() => inputRef.current?.click()}
      >
        <FileUp className="size-10 text-muted-foreground" />
        <p className="font-medium">Seleccionar PDF o Excel</p>
        <p className="text-xs text-muted-foreground">.pdf · .xlsx · .xls</p>
        <input
          ref={inputRef}
          type="file"
          accept=".pdf,.xlsx,.xls,application/pdf,application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
          className="hidden"
          onChange={(e) => void onFile(e.target.files?.[0] ?? null)}
        />
      </div>

      {loading ? (
        <div className="flex items-center justify-center gap-2 py-10 text-muted-foreground">
          <Loader2 className="size-5 animate-spin" />
          Parseando documento…
        </div>
      ) : null}

      {error ? (
        <p className="rounded-md border border-destructive/40 bg-destructive/5 px-3 py-2 text-sm text-destructive">
          {error}
        </p>
      ) : null}

      {preview ? (
        <div className="space-y-4">
          <div className="rounded-xl border p-4">
            <p className="text-xl font-bold">
              Picking #{preview.picking_number || "—"}
            </p>
            <dl className="mt-3 grid grid-cols-2 gap-2 text-sm">
              <div>
                <dt className="text-muted-foreground">Destino</dt>
                <dd className="font-medium">{preview.destination || "—"}</dd>
              </div>
              <div>
                <dt className="text-muted-foreground">Fecha</dt>
                <dd className="font-medium">{preview.picking_date || "—"}</dd>
              </div>
              <div>
                <dt className="text-muted-foreground">Camión</dt>
                <dd className="font-medium">{preview.truck || "—"}</dd>
              </div>
              <div>
                <dt className="text-muted-foreground">Sello</dt>
                <dd className="font-medium">{preview.seal || "—"}</dd>
              </div>
              <div>
                <dt className="text-muted-foreground">SKU</dt>
                <dd className="font-medium">{preview.valid_count}</dd>
              </div>
              <div>
                <dt className="text-muted-foreground">Unidades</dt>
                <dd className="font-medium">
                  {preview.summed_units.toLocaleString("es-CL")}
                </dd>
              </div>
              <div>
                <dt className="text-muted-foreground">Valor</dt>
                <dd className="font-medium">
                  {preview.summed_value != null
                    ? `$${preview.summed_value.toLocaleString("es-CL")}`
                    : "—"}
                </dd>
              </div>
              <div>
                <dt className="text-muted-foreground">Doc. unidades</dt>
                <dd className="font-medium">
                  {preview.document_units_total != null
                    ? preview.document_units_total.toLocaleString("es-CL")
                    : "—"}
                </dd>
              </div>
              <div>
                <dt className="text-muted-foreground">Hash archivo</dt>
                <dd className="font-mono text-xs break-all">
                  {preview.file_hash
                    ? `${preview.file_hash.slice(0, 16)}…`
                    : "—"}
                </dd>
              </div>
              <div>
                <dt className="text-muted-foreground">Doc. unidades</dt>
                <dd className="font-medium">
                  {preview.document_units_total != null
                    ? preview.document_units_total.toLocaleString("es-CL")
                    : "—"}
                </dd>
              </div>
              <div className="col-span-2">
                <dt className="text-muted-foreground">Hash archivo (SHA-256)</dt>
                <dd className="break-all font-mono text-[11px]">
                  {preview.file_hash || "—"}
                </dd>
              </div>
            </dl>

            <div className="mt-4 flex flex-wrap gap-2 text-sm">
              <span className="rounded-full bg-emerald-100 px-3 py-1 text-emerald-800">
                ✅ {preview.valid_count} válidos
              </span>
              <span className="rounded-full bg-amber-100 px-3 py-1 text-amber-900">
                ⚠ {preview.warning_count} advertencias
              </span>
              <span className="rounded-full bg-red-100 px-3 py-1 text-red-800">
                ❌ {preview.error_count} inválidos
              </span>
            </div>

            {preview.errors.length > 0 ? (
              <ul className="mt-3 list-disc space-y-1 pl-5 text-sm text-destructive">
                {preview.errors.map((e) => (
                  <li key={e}>{e}</li>
                ))}
              </ul>
            ) : null}
            {preview.warnings.length > 0 ? (
              <ul className="mt-2 list-disc space-y-1 pl-5 text-sm text-amber-800">
                {preview.warnings.map((w) => (
                  <li key={w}>{w}</li>
                ))}
              </ul>
            ) : null}

            <div className="mt-4">
              <label className="text-xs font-medium text-muted-foreground">
                N.º Picking (confirmación)
              </label>
              <Input
                className="mt-1 h-11"
                value={pickingOverride}
                onChange={(e) => setPickingOverride(e.target.value)}
              />
            </div>
          </div>

          <div className="max-h-72 overflow-auto rounded-xl border">
            <table className="w-full text-left text-sm">
              <thead className="sticky top-0 bg-muted text-xs uppercase text-muted-foreground">
                <tr>
                  <th className="px-3 py-2">Producto</th>
                  <th className="px-3 py-2">Cant.</th>
                  <th className="px-3 py-2">SEC</th>
                  <th className="px-3 py-2">Estado</th>
                </tr>
              </thead>
              <tbody>
                {preview.lines.slice(0, 200).map((ln, i) => (
                  <tr
                    key={`${ln.barcode}-${i}`}
                    className={cn(
                      "border-t",
                      ln.severity === "error" && "bg-destructive/5",
                      ln.severity === "warning" && "bg-amber-50/60",
                    )}
                  >
                    <td className="px-3 py-2">
                      <p className="font-medium leading-snug">{ln.product_name || "—"}</p>
                      <p className="font-mono text-[11px] text-muted-foreground">
                        {ln.barcode || "sin barcode"}
                      </p>
                    </td>
                    <td className="px-3 py-2 tabular-nums">{ln.requested_units}</td>
                    <td className="px-3 py-2 tabular-nums">{ln.sec ?? "—"}</td>
                    <td className="px-3 py-2 text-xs">{ln.severity}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <Button
            className="h-12 w-full text-base"
            disabled={!preview.can_import || importing || !pickingOverride.trim()}
            onClick={() => void onConfirm()}
          >
            {importing ? (
              <>
                <Loader2 className="mr-2 size-4 animate-spin" />
                Importando…
              </>
            ) : (
              "Confirmar importación"
            )}
          </Button>
        </div>
      ) : null}
    </div>
  )
}
