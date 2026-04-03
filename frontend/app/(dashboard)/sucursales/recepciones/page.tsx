"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogTrigger,
  DialogFooter,
} from "@/components/ui/dialog"
import {
  Plus,
  Trash2,
  ScanLine,
  Upload,
  Download,
  Save,
  Package,
  Clock,
  CheckCircle,
  FileSpreadsheet,
} from "lucide-react"

type ReceptionRow = {
  id: string
  fecha: string
  sucursal: string
  codigoBarras: string
  producto: string
  cantidad: number
  costo: number
  proveedor: string
  observaciones: string
  estado: "pendiente" | "revisado" | "exportado"
}

const initialRows: ReceptionRow[] = [
  {
    id: "1",
    fecha: "2026-03-15",
    sucursal: "Centro",
    codigoBarras: "7801234567890",
    producto: "Leche Entera 1L",
    cantidad: 50,
    costo: 850,
    proveedor: "Colun",
    observaciones: "",
    estado: "pendiente",
  },
  {
    id: "2",
    fecha: "2026-03-15",
    sucursal: "Centro",
    codigoBarras: "7801234567891",
    producto: "Pan de Molde Integral",
    cantidad: 30,
    costo: 1200,
    proveedor: "Ideal",
    observaciones: "Lote nuevo",
    estado: "revisado",
  },
  {
    id: "3",
    fecha: "2026-03-14",
    sucursal: "Norte",
    codigoBarras: "7801234567892",
    producto: "Aceite Vegetal 1L",
    cantidad: 24,
    costo: 2100,
    proveedor: "Chef",
    observaciones: "",
    estado: "exportado",
  },
]

const sucursales = ["Centro", "Norte", "Sur", "Poniente"]
const proveedores = ["Colun", "Ideal", "Chef", "Nestlé", "Unilever", "P&G", "Carozzi"]

const estadoConfig = {
  pendiente: { label: "Pendiente", color: "bg-yellow-500 text-white" },
  revisado: { label: "Revisado", color: "bg-blue-500 text-white" },
  exportado: { label: "Exportado", color: "bg-green-500 text-white" },
}

export default function RecepcionesPage() {
  const [rows, setRows] = useState<ReceptionRow[]>(initialRows)
  const [scanDialogOpen, setScanDialogOpen] = useState(false)
  const [barcodeInput, setBarcodeInput] = useState("")

  const addRow = () => {
    const newRow: ReceptionRow = {
      id: Date.now().toString(),
      fecha: new Date().toISOString().split("T")[0],
      sucursal: "Centro",
      codigoBarras: "",
      producto: "",
      cantidad: 1,
      costo: 0,
      proveedor: "",
      observaciones: "",
      estado: "pendiente",
    }
    setRows([...rows, newRow])
  }

  const deleteRow = (id: string) => {
    setRows(rows.filter((row) => row.id !== id))
  }

  const updateRow = (id: string, field: keyof ReceptionRow, value: string | number) => {
    setRows(
      rows.map((row) =>
        row.id === id ? { ...row, [field]: value } : row
      )
    )
  }

  const handleScan = () => {
    if (barcodeInput) {
      const newRow: ReceptionRow = {
        id: Date.now().toString(),
        fecha: new Date().toISOString().split("T")[0],
        sucursal: "Centro",
        codigoBarras: barcodeInput,
        producto: `Producto ${barcodeInput.slice(-4)}`,
        cantidad: 1,
        costo: 0,
        proveedor: "",
        observaciones: "",
        estado: "pendiente",
      }
      setRows([...rows, newRow])
      setBarcodeInput("")
      setScanDialogOpen(false)
    }
  }

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat("es-CL", {
      style: "currency",
      currency: "CLP",
    }).format(value)
  }

  const stats = {
    total: rows.length,
    pendientes: rows.filter((r) => r.estado === "pendiente").length,
    revisados: rows.filter((r) => r.estado === "revisado").length,
    exportados: rows.filter((r) => r.estado === "exportado").length,
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold text-foreground">Recepción de Mercadería</h1>
          <p className="text-sm text-muted-foreground">
            Registra los productos recibidos antes de cargarlos al sistema
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Dialog open={scanDialogOpen} onOpenChange={setScanDialogOpen}>
            <DialogTrigger asChild>
              <Button variant="outline">
                <ScanLine className="mr-2 h-4 w-4" />
                Escanear
              </Button>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Escanear Código de Barras</DialogTitle>
                <DialogDescription>
                  Escanea o ingresa manualmente el código de barras del producto
                </DialogDescription>
              </DialogHeader>
              <div className="py-4">
                <Input
                  placeholder="Escanea o ingresa el código de barras"
                  value={barcodeInput}
                  onChange={(e) => setBarcodeInput(e.target.value)}
                  onKeyDown={(e) => e.key === "Enter" && handleScan()}
                  autoFocus
                />
              </div>
              <DialogFooter>
                <Button onClick={handleScan}>Agregar Producto</Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>
          <Button variant="outline">
            <Upload className="mr-2 h-4 w-4" />
            Importar Excel
          </Button>
          <Button variant="outline">
            <Download className="mr-2 h-4 w-4" />
            Exportar Excel
          </Button>
          <Button>
            <Save className="mr-2 h-4 w-4" />
            Guardar Recepción
          </Button>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-4">
        <Card>
          <CardContent className="flex items-center gap-4 p-4">
            <div className="rounded-lg bg-muted p-2">
              <Package className="h-5 w-5 text-muted-foreground" />
            </div>
            <div>
              <p className="text-2xl font-semibold">{stats.total}</p>
              <p className="text-sm text-muted-foreground">Total Items</p>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="flex items-center gap-4 p-4">
            <div className="rounded-lg bg-yellow-100 p-2">
              <Clock className="h-5 w-5 text-yellow-600" />
            </div>
            <div>
              <p className="text-2xl font-semibold">{stats.pendientes}</p>
              <p className="text-sm text-muted-foreground">Pendientes</p>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="flex items-center gap-4 p-4">
            <div className="rounded-lg bg-blue-100 p-2">
              <CheckCircle className="h-5 w-5 text-blue-600" />
            </div>
            <div>
              <p className="text-2xl font-semibold">{stats.revisados}</p>
              <p className="text-sm text-muted-foreground">Revisados</p>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="flex items-center gap-4 p-4">
            <div className="rounded-lg bg-green-100 p-2">
              <FileSpreadsheet className="h-5 w-5 text-green-600" />
            </div>
            <div>
              <p className="text-2xl font-semibold">{stats.exportados}</p>
              <p className="text-sm text-muted-foreground">Exportados</p>
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle>Detalle de Recepción</CardTitle>
          <Button onClick={addRow} size="sm">
            <Plus className="mr-2 h-4 w-4" />
            Agregar Fila
          </Button>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full min-w-[1200px]">
              <thead>
                <tr className="border-b border-border text-left text-sm font-medium text-muted-foreground">
                  <th className="pb-3">Fecha</th>
                  <th className="pb-3">Sucursal</th>
                  <th className="pb-3">Código de Barras</th>
                  <th className="pb-3">Producto</th>
                  <th className="pb-3 text-right">Cantidad</th>
                  <th className="pb-3 text-right">Costo</th>
                  <th className="pb-3">Proveedor</th>
                  <th className="pb-3">Observaciones</th>
                  <th className="pb-3 text-center">Estado</th>
                  <th className="pb-3"></th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => (
                  <tr key={row.id} className="border-b border-border last:border-0">
                    <td className="py-2">
                      <Input
                        type="date"
                        value={row.fecha}
                        onChange={(e) => updateRow(row.id, "fecha", e.target.value)}
                        className="h-8 w-32"
                      />
                    </td>
                    <td className="py-2">
                      <Select
                        value={row.sucursal}
                        onValueChange={(v) => updateRow(row.id, "sucursal", v)}
                      >
                        <SelectTrigger className="h-8 w-28">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {sucursales.map((s) => (
                            <SelectItem key={s} value={s}>
                              {s}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </td>
                    <td className="py-2">
                      <Input
                        value={row.codigoBarras}
                        onChange={(e) => updateRow(row.id, "codigoBarras", e.target.value)}
                        className="h-8 w-32 font-mono text-xs"
                        placeholder="7801234567890"
                      />
                    </td>
                    <td className="py-2">
                      <Input
                        value={row.producto}
                        onChange={(e) => updateRow(row.id, "producto", e.target.value)}
                        className="h-8 w-40"
                        placeholder="Nombre del producto"
                      />
                    </td>
                    <td className="py-2">
                      <Input
                        type="number"
                        value={row.cantidad}
                        onChange={(e) => updateRow(row.id, "cantidad", parseInt(e.target.value) || 0)}
                        className="h-8 w-20 text-right"
                        min={1}
                      />
                    </td>
                    <td className="py-2">
                      <Input
                        type="number"
                        value={row.costo}
                        onChange={(e) => updateRow(row.id, "costo", parseInt(e.target.value) || 0)}
                        className="h-8 w-24 text-right"
                        min={0}
                      />
                    </td>
                    <td className="py-2">
                      <Select
                        value={row.proveedor}
                        onValueChange={(v) => updateRow(row.id, "proveedor", v)}
                      >
                        <SelectTrigger className="h-8 w-28">
                          <SelectValue placeholder="Seleccionar" />
                        </SelectTrigger>
                        <SelectContent>
                          {proveedores.map((p) => (
                            <SelectItem key={p} value={p}>
                              {p}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </td>
                    <td className="py-2">
                      <Input
                        value={row.observaciones}
                        onChange={(e) => updateRow(row.id, "observaciones", e.target.value)}
                        className="h-8 w-32"
                        placeholder="Notas"
                      />
                    </td>
                    <td className="py-2 text-center">
                      <Select
                        value={row.estado}
                        onValueChange={(v) => updateRow(row.id, "estado", v as ReceptionRow["estado"])}
                      >
                        <SelectTrigger className="h-8 w-28">
                          <Badge className={estadoConfig[row.estado].color}>
                            {estadoConfig[row.estado].label}
                          </Badge>
                        </SelectTrigger>
                        <SelectContent>
                          {Object.entries(estadoConfig).map(([key, config]) => (
                            <SelectItem key={key} value={key}>
                              {config.label}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </td>
                    <td className="py-2">
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => deleteRow(row.id)}
                        className="h-8 w-8 text-muted-foreground hover:text-destructive"
                      >
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
