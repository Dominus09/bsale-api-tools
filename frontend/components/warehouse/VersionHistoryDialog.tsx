"use client"

import { Button } from "@/components/ui/button"
import type { WarehouseRevision } from "@/lib/warehouse/types"

type Props = {
  open: boolean
  revisions: WarehouseRevision[]
  currentId: number | null
  busy: boolean
  onClose: () => void
  onOpen: (revision: WarehouseRevision) => void
  onCreateDraftFrom: (revision: WarehouseRevision) => void
}

function statusLabel(s: string) {
  if (s === "draft") return "Borrador"
  if (s === "published") return "Publicado"
  if (s === "archived") return "Archivado"
  return s
}

export function VersionHistoryDialog({
  open,
  revisions,
  currentId,
  busy,
  onClose,
  onOpen,
  onCreateDraftFrom,
}: Props) {
  if (!open) return null
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/45 p-4">
      <div
        role="dialog"
        aria-modal
        className="flex max-h-[85vh] w-full max-w-2xl flex-col rounded-lg border border-slate-200 bg-white shadow-xl"
      >
        <div className="border-b border-slate-100 px-4 py-3">
          <h2 className="text-base font-semibold text-slate-900">Historial de versiones</h2>
          <p className="mt-1 text-xs text-slate-600">
            Las revisiones published/archived son de solo lectura. Restaurar crea un{" "}
            <strong>nuevo borrador</strong> (no modifica el histórico ni es rollback SQL).
          </p>
        </div>
        <div className="flex-1 overflow-y-auto px-4 py-2">
          <table className="w-full text-left text-xs">
            <thead className="sticky top-0 bg-white text-slate-500">
              <tr>
                <th className="py-2 pr-2">ID</th>
                <th className="py-2 pr-2">Estado</th>
                <th className="py-2 pr-2">Etiqueta</th>
                <th className="py-2 pr-2">v</th>
                <th className="py-2 pr-2">Publicado</th>
                <th className="py-2">Acciones</th>
              </tr>
            </thead>
            <tbody>
              {revisions.map((r) => (
                <tr
                  key={r.id}
                  className={`border-t border-slate-100 ${
                    currentId === r.id ? "bg-sky-50" : ""
                  }`}
                >
                  <td className="py-2 pr-2 font-mono">{r.id}</td>
                  <td className="py-2 pr-2">{statusLabel(r.status)}</td>
                  <td className="py-2 pr-2">{r.label || "—"}</td>
                  <td className="py-2 pr-2">{r.row_version}</td>
                  <td className="py-2 pr-2">
                    {r.published_at ? new Date(r.published_at).toLocaleString("es-CL") : "—"}
                  </td>
                  <td className="py-2">
                    <div className="flex flex-wrap gap-1">
                      <Button
                        type="button"
                        size="sm"
                        variant="outline"
                        disabled={busy}
                        onClick={() => onOpen(r)}
                      >
                        Abrir
                      </Button>
                      {r.status !== "draft" ? (
                        <Button
                          type="button"
                          size="sm"
                          variant="secondary"
                          disabled={busy}
                          onClick={() => onCreateDraftFrom(r)}
                        >
                          Nuevo borrador
                        </Button>
                      ) : null}
                    </div>
                  </td>
                </tr>
              ))}
              {!revisions.length ? (
                <tr>
                  <td colSpan={6} className="py-6 text-center text-slate-500">
                    Sin revisiones
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
        <div className="flex justify-end border-t border-slate-100 px-4 py-3">
          <Button type="button" variant="outline" onClick={onClose} disabled={busy}>
            Cerrar
          </Button>
        </div>
      </div>
    </div>
  )
}
