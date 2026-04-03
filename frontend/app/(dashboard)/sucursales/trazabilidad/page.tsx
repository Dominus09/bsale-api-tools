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
  ScanLine,
  Trash2,
  CheckCircle,
  Package,
  AlertTriangle,
  Eye,
  EyeOff,
} from "lucide-react"

type ScannedProduct = {
  id: string
  codigoBarras: string
  producto: string
  stockSistema: number
  ubicacion: string
  timestamp: string
}

type MissingProduct = {
  id: string
  codigoBarras: string
  producto: string
  stockSistema: number
  ubicacionEsperada: string
}

const initialScanned: ScannedProduct[] = [
  {
    id: "1",
    codigoBarras: "7801234567890",
    producto: "Coca-Cola 2L",
    stockSistema: 45,
    ubicacion: "Góndola A1",
    timestamp: "14:32",
  },
  {
    id: "2",
    codigoBarras: "7801234567891",
    producto: "Sprite 2L",
    stockSistema: 32,
    ubicacion: "Góndola A1",
    timestamp: "14:33",
  },
  {
    id: "3",
    codigoBarras: "7801234567892",
    producto: "Fanta 2L",
    stockSistema: 28,
    ubicacion: "Góndola A2",
    timestamp: "14:35",
  },
]

const mockMissingProducts: MissingProduct[] = [
  {
    id: "1",
    codigoBarras: "7801234567893",
    producto: "Pepsi 2L",
    stockSistema: 15,
    ubicacionEsperada: "Góndola A1",
  },
  {
    id: "2",
    codigoBarras: "7801234567894",
    producto: "Agua Mineral 1.5L",
    stockSistema: 40,
    ubicacionEsperada: "Góndola A3",
  },
  {
    id: "3",
    codigoBarras: "7801234567895",
    producto: "Jugo de Naranja 1L",
    stockSistema: 22,
    ubicacionEsperada: "Góndola B1",
  },
]

const sucursales = ["Centro", "Norte", "Sur", "Poniente"]
const gondolas = ["Góndola A1", "Góndola A2", "Góndola A3", "Góndola B1", "Góndola B2", "Góndola C1"]

export default function TrazabilidadPage() {
  const [scannedProducts, setScannedProducts] = useState<ScannedProduct[]>(initialScanned)
  const [barcodeInput, setBarcodeInput] = useState("")
  const [selectedSucursal, setSelectedSucursal] = useState("Centro")
  const [selectedGondola, setSelectedGondola] = useState("Góndola A1")
  const [revisionComplete, setRevisionComplete] = useState(false)
  const [showMissing, setShowMissing] = useState(false)

  const handleScan = () => {
    if (barcodeInput) {
      const now = new Date()
      const newProduct: ScannedProduct = {
        id: Date.now().toString(),
        codigoBarras: barcodeInput,
        producto: `Producto ${barcodeInput.slice(-4)}`,
        stockSistema: Math.floor(Math.random() * 50) + 10,
        ubicacion: selectedGondola,
        timestamp: `${now.getHours().toString().padStart(2, "0")}:${now.getMinutes().toString().padStart(2, "0")}`,
      }
      setScannedProducts([newProduct, ...scannedProducts])
      setBarcodeInput("")
    }
  }

  const removeProduct = (id: string) => {
    setScannedProducts(scannedProducts.filter((p) => p.id !== id))
  }

  const handleFinalizarRevision = () => {
    setRevisionComplete(true)
    setShowMissing(true)
  }

  const handleNuevaRevision = () => {
    setScannedProducts([])
    setRevisionComplete(false)
    setShowMissing(false)
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold text-foreground">Revisión de Góndola</h1>
          <p className="text-sm text-muted-foreground">
            Escanea los productos exhibidos para comparar con el inventario
          </p>
        </div>
        {revisionComplete ? (
          <Button onClick={handleNuevaRevision}>Nueva Revisión</Button>
        ) : (
          <Button onClick={handleFinalizarRevision} disabled={scannedProducts.length === 0}>
            <CheckCircle className="mr-2 h-4 w-4" />
            Finalizar Revisión
          </Button>
        )}
      </div>

      <div className="grid gap-4 md:grid-cols-4">
        <Card>
          <CardContent className="flex items-center gap-4 p-4">
            <div className="rounded-lg bg-blue-100 p-2">
              <ScanLine className="h-5 w-5 text-blue-600" />
            </div>
            <div>
              <p className="text-2xl font-semibold">{scannedProducts.length}</p>
              <p className="text-sm text-muted-foreground">Escaneados</p>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="flex items-center gap-4 p-4">
            <div className="rounded-lg bg-green-100 p-2">
              <Eye className="h-5 w-5 text-green-600" />
            </div>
            <div>
              <p className="text-2xl font-semibold">{scannedProducts.length}</p>
              <p className="text-sm text-muted-foreground">Exhibidos</p>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="flex items-center gap-4 p-4">
            <div className="rounded-lg bg-yellow-100 p-2">
              <EyeOff className="h-5 w-5 text-yellow-600" />
            </div>
            <div>
              <p className="text-2xl font-semibold">
                {showMissing ? mockMissingProducts.length : "—"}
              </p>
              <p className="text-sm text-muted-foreground">No Exhibidos</p>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="flex items-center gap-4 p-4">
            <div className={`rounded-lg p-2 ${revisionComplete ? "bg-green-100" : "bg-gray-100"}`}>
              <CheckCircle
                className={`h-5 w-5 ${revisionComplete ? "text-green-600" : "text-gray-400"}`}
              />
            </div>
            <div>
              <p className="text-2xl font-semibold">{revisionComplete ? "Sí" : "No"}</p>
              <p className="text-sm text-muted-foreground">Completada</p>
            </div>
          </CardContent>
        </Card>
      </div>

      {!revisionComplete && (
        <Card>
          <CardHeader>
            <CardTitle>Escanear Producto</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-end gap-4">
              <div className="flex-1 space-y-2">
                <label className="text-sm font-medium">Sucursal</label>
                <Select value={selectedSucursal} onValueChange={setSelectedSucursal}>
                  <SelectTrigger>
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
              </div>
              <div className="flex-1 space-y-2">
                <label className="text-sm font-medium">Góndola</label>
                <Select value={selectedGondola} onValueChange={setSelectedGondola}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {gondolas.map((g) => (
                      <SelectItem key={g} value={g}>
                        {g}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="flex-[2] space-y-2">
                <label className="text-sm font-medium">Código de Barras</label>
                <div className="flex gap-2">
                  <Input
                    placeholder="Escanea o ingresa el código"
                    value={barcodeInput}
                    onChange={(e) => setBarcodeInput(e.target.value)}
                    onKeyDown={(e) => e.key === "Enter" && handleScan()}
                    className="font-mono"
                  />
                  <Button onClick={handleScan}>
                    <ScanLine className="mr-2 h-4 w-4" />
                    Agregar
                  </Button>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      <Card>
        <CardHeader>
          <CardTitle>Productos Escaneados</CardTitle>
        </CardHeader>
        <CardContent>
          {scannedProducts.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-12 text-muted-foreground">
              <Package className="mb-4 h-12 w-12" />
              <p>No hay productos escaneados</p>
              <p className="text-sm">Escanea un código de barras para comenzar</p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-border text-left text-sm font-medium text-muted-foreground">
                    <th className="pb-3">Hora</th>
                    <th className="pb-3">Código de Barras</th>
                    <th className="pb-3">Producto</th>
                    <th className="pb-3 text-right">Stock Sistema</th>
                    <th className="pb-3">Ubicación</th>
                    {!revisionComplete && <th className="pb-3"></th>}
                  </tr>
                </thead>
                <tbody>
                  {scannedProducts.map((product) => (
                    <tr
                      key={product.id}
                      className="border-b border-border last:border-0 hover:bg-muted/50"
                    >
                      <td className="py-3 text-sm text-muted-foreground">{product.timestamp}</td>
                      <td className="py-3 font-mono text-sm">{product.codigoBarras}</td>
                      <td className="py-3 font-medium">{product.producto}</td>
                      <td className="py-3 text-right">{product.stockSistema}</td>
                      <td className="py-3">
                        <Badge variant="secondary">{product.ubicacion}</Badge>
                      </td>
                      {!revisionComplete && (
                        <td className="py-3">
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => removeProduct(product.id)}
                            className="h-8 w-8 text-muted-foreground hover:text-destructive"
                          >
                            <Trash2 className="h-4 w-4" />
                          </Button>
                        </td>
                      )}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>

      {showMissing && (
        <Card className="border-yellow-200 bg-yellow-50">
          <CardHeader>
            <div className="flex items-center gap-2">
              <AlertTriangle className="h-5 w-5 text-yellow-600" />
              <CardTitle className="text-yellow-800">Productos en Stock No Exhibidos</CardTitle>
            </div>
          </CardHeader>
          <CardContent>
            {mockMissingProducts.length === 0 ? (
              <p className="text-center text-muted-foreground py-4">
                Todos los productos en stock están exhibidos
              </p>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead>
                    <tr className="border-b border-yellow-200 text-left text-sm font-medium text-yellow-800">
                      <th className="pb-3">Código de Barras</th>
                      <th className="pb-3">Producto</th>
                      <th className="pb-3 text-right">Stock Disponible</th>
                      <th className="pb-3">Ubicación Esperada</th>
                    </tr>
                  </thead>
                  <tbody>
                    {mockMissingProducts.map((product) => (
                      <tr
                        key={product.id}
                        className="border-b border-yellow-200 last:border-0"
                      >
                        <td className="py-3 font-mono text-sm">{product.codigoBarras}</td>
                        <td className="py-3 font-medium">{product.producto}</td>
                        <td className="py-3 text-right font-semibold text-yellow-700">
                          {product.stockSistema}
                        </td>
                        <td className="py-3">
                          <Badge className="bg-yellow-200 text-yellow-800">
                            {product.ubicacionEsperada}
                          </Badge>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  )
}
