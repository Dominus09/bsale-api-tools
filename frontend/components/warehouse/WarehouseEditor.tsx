"use client"

import dynamic from "next/dynamic"
import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import {
  BookOpen,
  Copy,
  Crosshair,
  Expand,
  FlipHorizontal2,
  Grid2x2,
  History,
  Loader2,
  Maximize2,
  Move,
  Plus,
  Printer,
  Redo2,
  Ruler,
  Save,
  ShieldCheck,
  Square,
  Trash2,
  Undo2,
  Upload,
  ZoomIn,
  MousePointer2,
  Boxes,
} from "lucide-react"
import { Button } from "@/components/ui/button"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import { PropertiesPanel } from "@/components/warehouse/PropertiesPanel"
import { SymbologyDialog } from "@/components/warehouse/SymbologyDialog"
import { GeneratePositionsDialog } from "@/components/warehouse/GeneratePositionsDialog"
import { AisleSpacingDialog } from "@/components/warehouse/AisleSpacingDialog"
import { PublishDialog } from "@/components/warehouse/PublishDialog"
import { VersionHistoryDialog } from "@/components/warehouse/VersionHistoryDialog"
import { PrintLayoutSheet } from "@/components/warehouse/PrintLayoutSheet"
import {
  createDraftFromRevision,
  evaluateWarehousePublish,
  getOrCreateDraft,
  getWarehouseCoordinateMeta,
  getWarehouseRevision,
  listWarehouseFacilities,
  listWarehouseFloors,
  listWarehouseRevisions,
  publishWarehouseRevision,
  saveWarehouseBatch,
  validateWarehouseRevision,
  type GeneratePreviewResult,
  type PublishEvaluation,
} from "@/lib/warehouse/api"
import { STANDARD_PALLET_M } from "@/lib/warehouse/style"
import type { ViewOrientationDeg } from "@/lib/warehouse/coords"
import type {
  EditorTool,
  ElementUpsertPayload,
  SymbologyCatalog,
  WarehouseElement,
  WarehouseElementType,
  WarehouseFacility,
  WarehouseFloor,
  WarehouseRevision,
} from "@/lib/warehouse/types"
import { WarehouseApiError } from "@/lib/warehouse/types"
import { validateElements } from "@/lib/warehouse/validation"
import { cn } from "@/lib/utils"

const WarehouseCanvas = dynamic(
  () => import("@/components/warehouse/WarehouseCanvas").then((m) => m.WarehouseCanvas),
  {
    ssr: false,
    loading: () => (
      <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
        Cargando canvas…
      </div>
    ),
  },
)

type HistorySnap = WarehouseElement[]

function clientKeyFor(physicalId: number, layoutId?: number | null) {
  if (physicalId > 0) return `pe-${physicalId}`
  return `tmp-${layoutId ?? Math.random().toString(36).slice(2)}`
}

function toLocalElements(
  items: Array<{ physical: WarehouseElement["physical"]; geometry: WarehouseElement["geometry"] }>,
): WarehouseElement[] {
  return items.map((e) => ({
    ...e,
    clientKey: clientKeyFor(e.physical.id, e.geometry.layout_element_id),
    dirty: false,
  }))
}

function ToolBtn({
  label,
  active,
  onClick,
  children,
}: {
  label: string
  active?: boolean
  onClick: () => void
  children: React.ReactNode
}) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Button
          type="button"
          size="icon"
          variant={active ? "default" : "ghost"}
          className="h-9 w-9"
          onClick={onClick}
          aria-label={label}
        >
          {children}
        </Button>
      </TooltipTrigger>
      <TooltipContent side="right">{label}</TooltipContent>
    </Tooltip>
  )
}

export function WarehouseEditor() {
  const [facilities, setFacilities] = useState<WarehouseFacility[]>([])
  const [floors, setFloors] = useState<WarehouseFloor[]>([])
  const [facilityId, setFacilityId] = useState<number | null>(null)
  const [floorId, setFloorId] = useState<number | null>(null)
  const [revision, setRevision] = useState<WarehouseRevision | null>(null)
  const [elements, setElements] = useState<WarehouseElement[]>([])
  const [selectedKey, setSelectedKey] = useState<string | null>(null)
  const [tool, setTool] = useState<EditorTool>("select")
  const [gridM, setGridM] = useState(0.1)
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [conflictMsg, setConflictMsg] = useState<string | null>(null)
  const [symbologyOpen, setSymbologyOpen] = useState(false)
  const [generatorOpen, setGeneratorOpen] = useState(false)
  const [spacingOpen, setSpacingOpen] = useState(false)
  const [catalog, setCatalog] = useState<SymbologyCatalog | null>(null)
  const [fitRequest, setFitRequest] = useState(0)
  const [viewOrientationDeg, setViewOrientationDeg] = useState<ViewOrientationDeg>(0)
  const [wide, setWide] = useState(false)
  const [dirty, setDirty] = useState(false)
  const [tmpId, setTmpId] = useState(-1)
  const [serverIssues, setServerIssues] = useState<string[]>([])
  const [readOnly, setReadOnly] = useState(false)
  const [revisions, setRevisions] = useState<WarehouseRevision[]>([])
  const [historyOpen, setHistoryOpen] = useState(false)
  const [publishOpen, setPublishOpen] = useState(false)
  const [publishEval, setPublishEval] = useState<PublishEvaluation | null>(null)
  const [evaluatingPublish, setEvaluatingPublish] = useState(false)
  const [publishing, setPublishing] = useState(false)
  const [printAt, setPrintAt] = useState<string | null>(null)
  const historyRef = useRef<{ past: HistorySnap[]; future: HistorySnap[] }>({
    past: [],
    future: [],
  })

  const floor = floors.find((f) => f.id === floorId) || null
  const facility = facilities.find((f) => f.id === facilityId) || null
  const floorLengthM = Number(floor?.length_m) || 23.1
  const floorWidthM = Number(floor?.width_m) || 13.7
  const isDraft = revision?.status === "draft" && !readOnly
  const editable = isDraft
  const spacingSector = facility?.code === "BC" && floor?.code === "PISO1"
    ? elements.find(e => e.physical.code === "DER" && e.physical.element_type === "sector" && !e.pendingRetire && !e.pendingRemoveGeometry)
    : undefined

  const issues = useMemo(
    () => validateElements(elements, { length_m: floorLengthM, width_m: floorWidthM }),
    [elements, floorLengthM, floorWidthM],
  )
  const conflictKeys = useMemo(
    () => new Set(issues.filter((i) => i.code === "overlap").map((i) => i.clientKey!).filter(Boolean)),
    [issues],
  )

  const pushHistory = useCallback((next: WarehouseElement[] | ((prev: WarehouseElement[]) => WarehouseElement[])) => {
    setElements((prev) => {
      const resolved = typeof next === "function" ? next(prev) : next
      historyRef.current.past.push(structuredClone(prev))
      if (historyRef.current.past.length > 80) historyRef.current.past.shift()
      historyRef.current.future = []
      return resolved
    })
    setDirty(true)
  }, [])

  const undo = () => {
    setElements((prev) => {
      const snap = historyRef.current.past.pop()
      if (!snap) return prev
      historyRef.current.future.push(structuredClone(prev))
      return snap
    })
    setDirty(true)
  }
  const redo = () => {
    setElements((prev) => {
      const snap = historyRef.current.future.pop()
      if (!snap) return prev
      historyRef.current.past.push(structuredClone(prev))
      return snap
    })
    setDirty(true)
  }

  useEffect(() => {
    const onBeforeUnload = (e: BeforeUnloadEvent) => {
      if (!dirty) return
      e.preventDefault()
      e.returnValue = ""
    }
    window.addEventListener("beforeunload", onBeforeUnload)
    return () => window.removeEventListener("beforeunload", onBeforeUnload)
  }, [dirty])

  useEffect(() => {
    let cancelled = false
    ;(async () => {
      setLoading(true)
      setError(null)
      try {
        const [meta, facs] = await Promise.all([
          getWarehouseCoordinateMeta(),
          listWarehouseFacilities(),
        ])
        if (cancelled) return
        setCatalog(meta.symbology_catalog)
        setFacilities(facs)
        const bc = facs.find((f) => f.code === "BC") || facs[0]
        if (bc) setFacilityId(bc.id)
      } catch (e) {
        if (!cancelled) {
          setError(e instanceof Error ? e.message : "No se pudo cargar warehouse")
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    })()
    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    if (facilityId == null) return
    let cancelled = false
    ;(async () => {
      try {
        const fl = await listWarehouseFloors(facilityId)
        if (cancelled) return
        setFloors(fl)
        const piso1 = fl.find((f) => f.code === "PISO1") || fl[0]
        if (piso1) setFloorId(piso1.id)
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : "Error al listar pisos")
      }
    })()
    return () => {
      cancelled = true
    }
  }, [facilityId])

  const refreshRevisionList = useCallback(async (fid: number) => {
    try {
      setRevisions(await listWarehouseRevisions(fid))
    } catch {
      /* ignore list errors in UI chrome */
    }
  }, [])

  const loadRevisionBundle = useCallback(async (revId: number, asReadOnly: boolean) => {
    const bundle = await getWarehouseRevision(revId)
    setRevision(bundle.revision)
    setElements(toLocalElements(bundle.elements))
    setSelectedKey(null)
    setDirty(false)
    setReadOnly(asReadOnly || bundle.revision.status !== "draft")
    historyRef.current = { past: [], future: [] }
    setFitRequest((n) => n + 1)
    if (bundle.floor?.id) await refreshRevisionList(bundle.floor.id)
  }, [refreshRevisionList])

  const loadDraft = useCallback(async (fid: number) => {
    setLoading(true)
    setError(null)
    setConflictMsg(null)
    try {
      const { revision: rev } = await getOrCreateDraft(fid)
      await loadRevisionBundle(rev.id, false)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al cargar borrador")
    } finally {
      setLoading(false)
    }
  }, [loadRevisionBundle])

  useEffect(() => {
    if (floorId != null) void loadDraft(floorId)
  }, [floorId, loadDraft])

  const openValidateOnly = async () => {
    if (!revision) return
    setError(null)
    try {
      const report = await validateWarehouseRevision(revision.id)
      setServerIssues(
        (report.issues || [])
          .slice(0, 12)
          .map((i) => `${i.severity}: ${String(i.message || i.code)}`),
      )
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al validar")
    }
  }

  const openPublish = async () => {
    if (!revision || dirty || revision.status !== "draft") return
    setPublishOpen(true)
    setEvaluatingPublish(true)
    setPublishEval(null)
    try {
      setPublishEval(await evaluateWarehousePublish(revision.id))
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al evaluar publicación")
      setPublishOpen(false)
    } finally {
      setEvaluatingPublish(false)
    }
  }

  const confirmPublish = async () => {
    if (!revision) return
    setPublishing(true)
    setError(null)
    try {
      const result = await publishWarehouseRevision(revision.id, {
        expected_row_version: revision.row_version,
      })
      setPublishOpen(false)
      await loadRevisionBundle(result.revision.id, true)
      if (floorId != null) await refreshRevisionList(floorId)
    } catch (e) {
      if (e instanceof WarehouseApiError && e.status === 409) {
        setConflictMsg(e.message)
      } else {
        setError(e instanceof Error ? e.message : "Error al publicar")
      }
    } finally {
      setPublishing(false)
    }
  }

  const openHistory = async () => {
    if (floorId == null) return
    await refreshRevisionList(floorId)
    setHistoryOpen(true)
  }

  const openHistorical = async (rev: WarehouseRevision) => {
    if (dirty && !confirm("Hay cambios sin guardar. ¿Continuar?")) return
    setLoading(true)
    setError(null)
    try {
      await loadRevisionBundle(rev.id, rev.status !== "draft")
      setHistoryOpen(false)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al abrir revisión")
    } finally {
      setLoading(false)
    }
  }

  const restoreFrom = async (rev: WarehouseRevision) => {
    if (dirty && !confirm("Hay cambios sin guardar. ¿Continuar?")) return
    setLoading(true)
    setError(null)
    try {
      const created = await createDraftFromRevision(
        rev.id,
        `Borrador desde rev ${rev.id}`,
      )
      await loadRevisionBundle(created.revision.id, false)
      setHistoryOpen(false)
      if (floorId != null) await refreshRevisionList(floorId)
    } catch (e) {
      setError(e instanceof Error ? e.message : "No se pudo crear borrador desde histórico")
    } finally {
      setLoading(false)
    }
  }

  const printLayout = () => {
    setPrintAt(new Date().toLocaleString("es-CL"))
    requestAnimationFrame(() => {
      window.print()
    })
  }

  const selected = elements.find((e) => e.clientKey === selectedKey) || null

  const patchElement = (
    key: string,
    patch: {
      physical?: Partial<WarehouseElement["physical"]>
      geometry?: Partial<WarehouseElement["geometry"]>
    },
  ) => {
    if (!editable) return
    pushHistory((prev) =>
      prev.map((e) =>
        e.clientKey === key
          ? {
              ...e,
              dirty: true,
              physical: { ...e.physical, ...patch.physical },
              geometry: { ...e.geometry, ...patch.geometry },
            }
          : e,
      ),
    )
  }

  const createAt = (x_m: number, y_m: number) => {
    if (!editable) return
    let element_type: WarehouseElementType = "obstacle"
    let width_m = 1
    let depth_m = 1
    let code = "NUEVO"
    let name = "Nuevo elemento"
    let parent_id: number | null = selected?.physical.id ?? null

    if (tool === "create_sector") {
      element_type = "sector"
      width_m = 4
      depth_m = 2
      code = `SEC-${Math.abs(tmpId)}`
      name = "Nuevo sector"
      parent_id =
        elements.find((e) => e.physical.element_type === "facility_boundary")?.physical.id ?? null
    } else if (tool === "create_position") {
      element_type = "position"
      width_m = STANDARD_PALLET_M.length
      depth_m = STANDARD_PALLET_M.width
      const parentCode = selected?.physical.code || "POS"
      const siblingCount =
        elements.filter(
          (e) =>
            e.physical.element_type === "position" &&
            e.physical.parent_id === (selected?.physical.id ?? null),
        ).length + 1
      const suffix = String(siblingCount).padStart(2, "0")
      // IZ usa D/A opcional; pasillos Pn-nn
      if (selected?.physical.element_type === "sector" && selected.physical.code === "IZ") {
        code = `IZ-${suffix}-D`
      } else if (selected?.physical.element_type === "aisle") {
        code = `${selected.physical.code}-${suffix}`
      } else {
        code = `${parentCode}-${suffix}`
      }
      name = `Posición ${code}`
      parent_id = selected?.physical.id ?? null
      if (!parent_id) {
        setError("Seleccione un sector o pasillo antes de crear una posición")
        return
      }
    } else if (tool === "create_structure") {
      // Estructura vs zona sueltos: si el padre es IZ → sugerir loose_storage
      if (selected?.physical.code === "IZ" || selected?.physical.element_type === "sector") {
        element_type = "loose_storage"
        width_m = 2.52
        depth_m = 1.06
        code = selected?.physical.code === "IZ" ? "IZ-SU-N" : `SU-${Math.abs(tmpId)}`
        name = "Zona productos sueltos"
        parent_id = selected?.physical.id ?? null
      } else {
        element_type = "obstacle"
        width_m = 0.6
        depth_m = 0.6
        code = `OBS-${Math.abs(tmpId)}`
        name = "Estructura / obstáculo"
      }
    }

    const id = tmpId
    setTmpId((t) => t - 1)
    const storageMeta =
      element_type === "loose_storage"
        ? { storage_kind: "loose_no_pallet", not_pallet_positions: true }
        : {}
    const neu: WarehouseElement = {
      clientKey: clientKeyFor(id),
      dirty: true,
      isNew: true,
      physical: {
        id,
        facility_id: facilityId || 0,
        floor_id: floorId || 0,
        parent_id,
        element_type,
        code,
        name,
        status: "active",
        meta_json: storageMeta,
      },
      geometry: {
        x_m,
        y_m,
        elevation_m: 0,
        width_m,
        depth_m,
        height_clear_m: null,
        rotation_deg: 0,
        geom_type: "rect",
        polygon_m: null,
        stack_levels_max: null,
        validation_status: "pending_validation",
        constraints_json: {
          ...storageMeta,
          stacking_authorized: false,
        },
        observations: "Creado en editor — pendiente de validación",
        meta_json: { provisional: true, ...storageMeta },
      },
    }
    pushHistory((prev) => [...prev, neu])
    setSelectedKey(neu.clientKey)
    setTool("select")
  }

  const applyGeneratorPreview = (preview: GeneratePreviewResult) => {
    if (!selected || !editable || selected.physical.id <= 0 || preview.can_incorporate !== true) return
    const parentId = selected.physical.id
    if (preview.positions.some((p) => p.parent_code !== selected.physical.code)) {
      setError("La vista previa pertenece a otro sector. Genere una nueva.")
      return
    }
    let nextTmp = tmpId
    const existingCodes = new Set(
      elements.filter((e) => !e.pendingRetire).map((e) => e.physical.code.toLowerCase()),
    )
    const proposedCodes = preview.positions.map((p) => p.code.toLowerCase())
    if (new Set(proposedCodes).size !== proposedCodes.length || proposedCodes.some((code) => existingCodes.has(code))) {
      setError("Existen códigos duplicados. No se incorporó ninguna posición.")
      return
    }
    const created: WarehouseElement[] = []
    for (const p of preview.positions) {
      const id = nextTmp
      nextTmp -= 1
      const g = p.geometry as WarehouseElement["geometry"]
      created.push({
        clientKey: clientKeyFor(id),
        dirty: true,
        isNew: true,
        physical: {
          id,
          facility_id: facilityId || 0,
          floor_id: floorId || 0,
          parent_id: parentId,
          element_type: "position",
          code: p.code,
          name: p.name,
          status: "active",
          meta_json: { storage_kind: "pallet_standard", generated: true },
        },
        geometry: {
          x_m: Number(g.x_m),
          y_m: Number(g.y_m),
          elevation_m: Number(g.elevation_m || 0),
          width_m: Number(g.width_m),
          depth_m: Number(g.depth_m),
          height_clear_m: g.height_clear_m == null ? null : Number(g.height_clear_m),
          rotation_deg: Number(g.rotation_deg || 0),
          geom_type: "rect",
          polygon_m: null,
          stack_levels_max: null,
          validation_status: "pending_validation",
          constraints_json: (g.constraints_json as Record<string, unknown>) || {
            generated: true,
            storage_kind: "pallet_standard",
            stacking_authorized: false,
          },
          observations: (g.observations as string) || null,
          meta_json: (g.meta_json as Record<string, unknown>) || { provisional: true },
        },
      })
      existingCodes.add(p.code.toLowerCase())
    }
    setTmpId(nextTmp)
    if (!created.length) {
      setError(
        "Ninguna posición nueva se incorporó (¿códigos ya existentes?). El generador no sustituye identidades.",
      )
      setGeneratorOpen(false)
      return
    }
    pushHistory((prev) => [...prev, ...created])
    setGeneratorOpen(false)
    setSelectedKey(created[0].clientKey)
  }

  const duplicateSelected = () => {
    if (!selected || !editable) return
    const id = tmpId
    setTmpId((t) => t - 1)
    const neu: WarehouseElement = {
      ...structuredClone(selected),
      clientKey: clientKeyFor(id),
      dirty: true,
      isNew: true,
      physical: {
        ...selected.physical,
        id,
        code: `${selected.physical.code}-C`,
        name: `${selected.physical.name} (copia)`,
      },
      geometry: {
        ...selected.geometry,
        x_m: selected.geometry.x_m + 0.3,
        y_m: selected.geometry.y_m + 0.3,
        layout_element_id: null,
      },
    }
    pushHistory((prev) => [...prev, neu])
    setSelectedKey(neu.clientKey)
  }

  const deleteSelected = () => {
    if (!selected || !editable) return
    const key = selected.clientKey
    if (selected.isNew || selected.physical.id < 0) {
      pushHistory((prev) => prev.filter((e) => e.clientKey !== key))
      setSelectedKey(null)
      return
    }
    // Retiro del borrador: backend preserva published si aplica
    pushHistory((prev) =>
      prev.map((e) => (e.clientKey === key ? { ...e, pendingRetire: true, dirty: true } : e)),
    )
    setSelectedKey(null)
  }

  const save = async () => {
    if (!revision || !editable) return
    const hardErrors = issues.filter((i) => i.severity === "error")
    if (hardErrors.length) {
      setError(hardErrors.map((i) => i.message).join(" · "))
      return
    }
    setSaving(true)
    setError(null)
    setConflictMsg(null)
    try {
      const upserts: ElementUpsertPayload[] = elements
        .filter((e) => !e.pendingRetire && !e.pendingRemoveGeometry && (e.dirty || e.isNew))
        .map((e) => ({
          physical_element_id: e.physical.id > 0 ? e.physical.id : null,
          parent_id: e.physical.parent_id,
          element_type: e.physical.element_type,
          code: e.physical.code,
          name: e.physical.name,
          meta_json: e.physical.meta_json || {},
          geometry: {
            x_m: e.geometry.x_m,
            y_m: e.geometry.y_m,
            elevation_m: e.geometry.elevation_m,
            width_m: e.geometry.width_m,
            depth_m: e.geometry.depth_m,
            height_clear_m: e.geometry.height_clear_m,
            rotation_deg: e.geometry.rotation_deg,
            geom_type: e.geometry.geom_type || "rect",
            validation_status: e.geometry.validation_status,
            constraints_json: e.geometry.constraints_json || {},
            observations: e.geometry.observations,
            meta_json: e.geometry.meta_json || {},
            stack_levels_max: e.geometry.stack_levels_max,
          },
        }))
      const retire_physical_ids = elements
        .filter((e) => e.pendingRetire && e.physical.id > 0)
        .map((e) => e.physical.id)
      const remove_geometry_physical_ids = elements
        .filter((e) => e.pendingRemoveGeometry && e.physical.id > 0 && !e.pendingRetire)
        .map((e) => e.physical.id)

      const result = await saveWarehouseBatch(revision.id, {
        expected_row_version: revision.row_version,
        upserts,
        retire_physical_ids,
        remove_geometry_physical_ids,
      })
      setRevision(result.revision)
      // Recargar desde servidor para alinear ids y snaps
      const bundle = await getWarehouseRevision(result.revision.id)
      setRevision(bundle.revision)
      setElements(toLocalElements(bundle.elements))
      setDirty(false)
      historyRef.current = { past: [], future: [] }
      try {
        const report = await validateWarehouseRevision(bundle.revision.id)
        setServerIssues(
          (report.issues || [])
            .filter((i) => i.severity === "geom_conflict" || i.severity === "integrity_error")
            .slice(0, 8)
            .map((i) => String(i.message || i.code)),
        )
      } catch {
        setServerIssues([])
      }
    } catch (e) {
      if (e instanceof WarehouseApiError && e.status === 409) {
        setConflictMsg(
          `${e.message}. Recargue el borrador para obtener la versión actual antes de sobrescribir.`,
        )
      } else {
        setError(e instanceof Error ? e.message : "Error al guardar")
      }
    } finally {
      setSaving(false)
    }
  }

  const visibleElements = elements.filter((e) => !e.pendingRetire)

  if (loading && !revision) {
    return (
      <div className="flex h-[70vh] items-center justify-center gap-2 text-muted-foreground">
        <Loader2 className="h-5 w-5 animate-spin" />
        Cargando Inteligencia de Bodega…
      </div>
    )
  }

  return (
    <div
      className={cn(
        "flex min-w-0 w-full max-w-full flex-col gap-2",
        wide ? "fixed inset-0 z-40 bg-card p-3" : "h-[calc(100vh-7rem)]",
      )}
    >
      <div className="flex flex-wrap items-center gap-2 rounded-lg border border-border bg-card px-3 py-2 shadow-sm">
        <div className="min-w-[160px]">
          <Select
            value={facilityId != null ? String(facilityId) : undefined}
            onValueChange={(v) => {
              if (dirty && !confirm("Hay cambios sin guardar. ¿Continuar?")) return
              setViewOrientationDeg(0)
              setFacilityId(Number(v))
            }}
          >
            <SelectTrigger className="h-9">
              <SelectValue placeholder="Instalación" />
            </SelectTrigger>
            <SelectContent>
              {facilities.map((f) => (
                <SelectItem key={f.id} value={String(f.id)}>
                  {f.code} — {f.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="min-w-[140px]">
          <Select
            value={floorId != null ? String(floorId) : undefined}
            onValueChange={(v) => {
              if (dirty && !confirm("Hay cambios sin guardar. ¿Continuar?")) return
              setFloorId(Number(v))
            }}
          >
            <SelectTrigger className="h-9">
              <SelectValue placeholder="Piso" />
            </SelectTrigger>
            <SelectContent>
              {floors.map((f) => (
                <SelectItem key={f.id} value={String(f.id)}>
                  {f.code} — {f.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <span
          className={cn(
            "rounded-full px-2.5 py-1 text-xs font-medium",
            revision?.status === "draft"
              ? "bg-lq-warning/10 text-lq-warning"
              : revision?.status === "published"
                ? "bg-lq-success/10 text-lq-success"
                : "bg-muted text-muted-foreground",
          )}
        >
          {revision ? `${revision.status} · v${revision.row_version}` : "sin revisión"}
          {readOnly ? " · solo lectura" : ""}
        </span>
        <span
          className={cn(
            "rounded-full px-2.5 py-1 text-xs font-medium",
            dirty ? "bg-lq-warning/10 text-lq-warning" : "bg-lq-success/10 text-lq-success",
          )}
        >
          {dirty ? "Cambios sin guardar" : "Sin cambios pendientes"}
        </span>
        <div className="ml-auto flex flex-wrap items-center gap-1">
          <Button
            type="button"
            variant="outline"
            size="sm"
            disabled={
              !editable ||
              !selected ||
              !["sector", "aisle"].includes(selected.physical.element_type)
            }
            onClick={() => setGeneratorOpen(true)}
          >
            <Grid2x2 className="mr-1.5 h-4 w-4" />
            Generar posiciones
          </Button>
          {spacingSector ? (
            <Button type="button" variant="outline" size="sm" disabled={!editable || saving || loading}
              title="Ajustar separación B entre filas de pasillos vecinos sin regenerar posiciones"
              onClick={() => setSpacingOpen(true)}>
              <Ruler className="mr-1.5 h-4 w-4" /> Separación pasillos
            </Button>
          ) : null}
          <Button type="button" variant="outline" size="sm" onClick={() => setSymbologyOpen(true)}>
            <BookOpen className="mr-1.5 h-4 w-4" />
            Simbología
          </Button>
          <Button
            type="button"
            variant="outline"
            size="sm"
            disabled={!revision}
            onClick={() => void openValidateOnly()}
          >
            <ShieldCheck className="mr-1.5 h-4 w-4" />
            Validar plano
          </Button>
          <Button
            type="button"
            variant="outline"
            size="sm"
            disabled={!revision || dirty || revision.status !== "draft"}
            onClick={() => void openPublish()}
            title={dirty ? "Guarde el borrador antes de publicar" : undefined}
          >
            <Upload className="mr-1.5 h-4 w-4" />
            Publicar plano
          </Button>
          <Button type="button" variant="outline" size="sm" onClick={() => void openHistory()}>
            <History className="mr-1.5 h-4 w-4" />
            Historial
          </Button>
          <Button type="button" variant="outline" size="sm" disabled={!revision} onClick={printLayout}>
            <Printer className="mr-1.5 h-4 w-4" />
            Imprimir
          </Button>
          <Button type="button" variant="outline" size="sm" onClick={() => setFitRequest((n) => n + 1)}>
            <Maximize2 className="mr-1.5 h-4 w-4" />
            Ajustar plano
          </Button>
          <Button type="button" variant="outline" size="sm" onClick={() => setWide((w) => !w)}>
            <Expand className="mr-1.5 h-4 w-4" />
            {wide ? "Salir de vista amplia" : "Vista amplia"}
          </Button>
          <Button type="button" size="sm" disabled={!dirty || saving || !editable} onClick={() => void save()}>
            {saving ? <Loader2 className="mr-1.5 h-4 w-4 animate-spin" /> : <Save className="mr-1.5 h-4 w-4" />}
            Guardar borrador
          </Button>
        </div>
      </div>

      <div className="rounded-md border border-lq-warning/30 bg-lq-warning/10 px-3 py-2 text-xs text-lq-warning">
        {facility?.code === "BL"
          ? "Bodega de Lata · Plano provisional: 17,60 × 6,00 m; 5,50 m útiles de referencia. Filas 01–12 de izquierda a derecha desde el patio. A = frente, E = fondo. BL01A: espacio muerto. BL01E: retirar mueble y verificar antes de ocupar. Postes, sensores y accesos por confirmar."
          : facility?.code === "BC"
            ? "Bodega Central · Plano de referencia. Oficina, escalera y portón requieren verificación. Origen métrico: +X hacia el fondo, +Y hacia DER. Vista canónica: entrada abajo, IZ izquierda."
            : "Plano de referencia: confirmar dimensiones, obstáculos y accesos en terreno."}
        {" "}Publicar no certifica seguridad ni autoriza apilamiento.
      </div>

      {error ? (
        <div className="rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
          {error}
        </div>
      ) : null}
      {conflictMsg ? (
        <div className="flex flex-wrap items-center gap-2 rounded-md border border-lq-warning/30 bg-lq-warning/10 px-3 py-2 text-sm text-lq-warning">
          <span>{conflictMsg}</span>
          <Button
            type="button"
            size="sm"
            variant="outline"
            onClick={() => floorId != null && void loadDraft(floorId)}
          >
            Recargar borrador
          </Button>
        </div>
      ) : null}

      <div className="flex min-h-0 min-w-0 flex-1 gap-2">
        <aside className="flex w-12 shrink-0 flex-col items-center gap-1 rounded-lg border border-border bg-card py-2 shadow-sm">
          <ToolBtn label="Seleccionar" active={tool === "select"} onClick={() => setTool("select")}>
            <MousePointer2 className="h-4 w-4" />
          </ToolBtn>
          <ToolBtn
            label="Crear sector"
            active={tool === "create_sector"}
            onClick={() => setTool("create_sector")}
          >
            <Square className="h-4 w-4" />
          </ToolBtn>
          <ToolBtn
            label="Crear posición (pallet 1,26×1,06)"
            active={tool === "create_position"}
            onClick={() => setTool("create_position")}
          >
            <Plus className="h-4 w-4" />
          </ToolBtn>
          <ToolBtn
            label="Crear estructura / zona sueltos (sobre sector)"
            active={tool === "create_structure"}
            onClick={() => setTool("create_structure")}
          >
            <Boxes className="h-4 w-4" />
          </ToolBtn>
          <ToolBtn
            label="Generar posiciones (sector/pasillo seleccionado)"
            onClick={() => {
              if (!selected || !["sector", "aisle"].includes(selected.physical.element_type)) {
                setError("Seleccione un sector o pasillo para generar posiciones")
                return
              }
              setGeneratorOpen(true)
            }}
          >
            <Grid2x2 className="h-4 w-4" />
          </ToolBtn>
          <ToolBtn label="Mover" active={tool === "move"} onClick={() => setTool("move")}>
            <Move className="h-4 w-4" />
          </ToolBtn>
          <ToolBtn label="Girar" active={tool === "rotate"} onClick={() => setTool("rotate")}>
            <FlipHorizontal2 className="h-4 w-4" />
          </ToolBtn>
          <ToolBtn label="Redimensionar" active={tool === "resize"} onClick={() => setTool("resize")}>
            <Maximize2 className="h-4 w-4" />
          </ToolBtn>
          <ToolBtn label="Duplicar" onClick={duplicateSelected}>
            <Copy className="h-4 w-4" />
          </ToolBtn>
          <ToolBtn label="Eliminar / retirar del borrador" onClick={deleteSelected}>
            <Trash2 className="h-4 w-4" />
          </ToolBtn>
          <ToolBtn label="Medir distancia" active={tool === "measure"} onClick={() => setTool("measure")}>
            <Ruler className="h-4 w-4" />
          </ToolBtn>
          <ToolBtn label="Desplazar vista" active={tool === "pan"} onClick={() => setTool("pan")}>
            <Crosshair className="h-4 w-4" />
          </ToolBtn>
          <ToolBtn label="Deshacer (sesión)" onClick={undo}>
            <Undo2 className="h-4 w-4" />
          </ToolBtn>
          <ToolBtn label="Rehacer (sesión)" onClick={redo}>
            <Redo2 className="h-4 w-4" />
          </ToolBtn>
          <ToolBtn label="Ajustar zoom al plano" onClick={() => setFitRequest((n) => n + 1)}>
            <ZoomIn className="h-4 w-4" />
          </ToolBtn>
          <div className="mt-2 px-1">
            <Select value={String(gridM)} onValueChange={(v) => setGridM(Number(v))}>
              <SelectTrigger className="h-8 w-10 px-1 text-[10px]" title="Cuadrícula">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="0">Off</SelectItem>
                <SelectItem value="0.1">0.1</SelectItem>
                <SelectItem value="0.25">0.25</SelectItem>
                <SelectItem value="0.5">0.5</SelectItem>
                <SelectItem value="1">1</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </aside>

        <main className="relative min-h-0 min-w-0 flex-1 overflow-hidden rounded-lg border border-border bg-card shadow-sm">
          <WarehouseCanvas
            key={floorId ?? "no-floor"}
            elements={visibleElements}
            floorLengthM={floorLengthM}
            floorWidthM={floorWidthM}
            selectedKey={selectedKey}
            tool={editable ? tool : "select"}
            gridM={gridM}
            conflictKeys={conflictKeys}
            onSelect={setSelectedKey}
            onChangeElement={(key, geom) => {
              if (!editable) return
              patchElement(key, { geometry: geom })
            }}
            onCreateAt={createAt}
            fitRequest={fitRequest}
            viewOrientationDeg={viewOrientationDeg}
            onViewOrientationChange={setViewOrientationDeg}
          />
        </main>

        <aside className="w-72 shrink-0 overflow-hidden rounded-lg border border-border bg-card shadow-sm">
          <PropertiesPanel
            element={selected}
            parents={visibleElements}
            readOnly={!editable}
            onChange={(patch) => selected && editable && patchElement(selected.clientKey, patch)}
          />
          {issues.length > 0 ? (
            <div className="border-t border-border p-2 text-[11px] text-muted-foreground">
              <div className="mb-1 font-semibold">Validación local</div>
              <ul className="max-h-28 space-y-1 overflow-y-auto">
                {issues.slice(0, 12).map((i, idx) => (
                  <li
                    key={`${i.code}-${idx}`}
                    className={i.severity === "error" ? "text-destructive" : "text-lq-warning"}
                  >
                    {i.message}
                  </li>
                ))}
              </ul>
            </div>
          ) : null}
          {serverIssues.length > 0 ? (
            <div className="border-t border-border p-2 text-[11px] text-destructive">
              <div className="mb-1 font-semibold">Validación servidor</div>
              <ul className="max-h-24 space-y-1 overflow-y-auto">
                {serverIssues.map((m, idx) => (
                  <li key={idx}>{m}</li>
                ))}
              </ul>
            </div>
          ) : null}
        </aside>
      </div>

      <SymbologyDialog open={symbologyOpen} onOpenChange={setSymbologyOpen} catalog={catalog} />
      <GeneratePositionsDialog
        open={generatorOpen}
        onOpenChange={setGeneratorOpen}
        parent={
          selected && ["sector", "aisle"].includes(selected.physical.element_type) ? selected : null
        }
        floorLengthM={floorLengthM}
        floorWidthM={floorWidthM}
        existing={visibleElements}
        onConfirm={applyGeneratorPreview}
      />
      {spacingOpen && spacingSector && revision && editable ? (
        <AisleSpacingDialog
          key={revision.id + ":" + revision.row_version + ":" + spacingSector.physical.id}
          revisionId={revision.id}
          sectorId={spacingSector.physical.id}
          elements={elements}
          floorLengthM={floorLengthM}
          floorWidthM={floorWidthM}
          onClose={() => setSpacingOpen(false)}
          onApply={(next, source, revisionId) => {
            if (!editable || saving || elements !== source || revision.id !== revisionId) {
              setError("El borrador cambió durante la validación. Abra nuevamente la herramienta.")
              setSpacingOpen(false)
              return
            }
            pushHistory(next)
            setError(null)
            setServerIssues([])
            setSpacingOpen(false)
          }}
        />
      ) : null}
      <PublishDialog
        open={publishOpen}
        evaluating={evaluatingPublish}
        publishing={publishing}
        evaluation={publishEval}
        onClose={() => setPublishOpen(false)}
        onConfirm={() => void confirmPublish()}
      />
      <VersionHistoryDialog
        open={historyOpen}
        revisions={revisions}
        currentId={revision?.id ?? null}
        busy={loading || publishing}
        onClose={() => setHistoryOpen(false)}
        onOpen={(r) => void openHistorical(r)}
        onCreateDraftFrom={(r) => void restoreFrom(r)}
      />
      {printAt ? (
        <PrintLayoutSheet
          facility={facility}
          floor={floor}
          revision={revision}
          elements={visibleElements}
          catalog={catalog}
          generatedAt={printAt}
        />
      ) : null}
      <style
        dangerouslySetInnerHTML={{
          __html: `
        @media print {
          body * { visibility: hidden !important; }
          .print-layout-sheet, .print-layout-sheet * { visibility: visible !important; }
          .print-layout-sheet {
            display: block !important;
            position: absolute; left: 0; top: 0; width: 100%;
            padding: 12mm; background: white;
          }
        }`,
        }}
      />
    </div>
  )
}
