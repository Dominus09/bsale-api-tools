"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "@/components/ui/dialog"
import {
  ScanLine,
  Trash2,
  Printer,
  FileText,
  Package,
  Tag,
} from "lucide-react"

type LabelProduct = {
  id: string
  codigoBarras: string
  producto: string
  precio: number
  cantidad: number
}

const initialProducts: LabelProduct[] = [
  {
    id: "1",
    codigoBarras: "7801234567890",
    producto: "Coca-Cola 2L",
    precio: 1890,
    cantidad: 2,
  },
  {
    id: "2",
    codigoBarras: "7801234567891",
    producto: "Sprite 2L",
    precio: 1690,
    cantidad: 1,
  },
  {
    id: "3",
    codigoBarras: "7801234567892",
    producto: "Detergente Omo 3kg",
    precio: 5490,
    cantidad: 3,
  },
]

export default function EtiquetasPage() {
  const [products, setProducts] = useState<LabelProduct[]>(initialProducts)
  const [barcodeInput, setBarcodeInput] = useState("")
  const [previewOpen, setPreviewOpen] = useState(false)

  const handleScan = () => {
    if (barcodeInput) {
      const existing = products.find((p) => p.codigoBarras === barcodeInput)
      if (existing) {
        setProducts(
          products.map((p) =>
            p.codigoBarras === barcodeInput
              ? { ...p, cantidad: p.cantidad + 1 }
              : p
          )
        )
      } else {
        const newProduct: LabelProduct = {
          id: Date.now().toString(),
          codigoBarras: barcodeInput,
          producto: `Producto ${barcodeInput.slice(-4)}`,
          precio: Math.floor(Math.random() * 5000) + 500,
          cantidad: 1,
        }
        setProducts([...products, newProduct])
      }
      setBarcodeInput("")
    }
  }

  const removeProduct = (id: string) => {
    setProducts(products.filter((p) => p.id !== id))
  }

  const updateQuantity = (id: string, cantidad: number) => {
    if (cantidad < 1) return
    setProducts(products.map((p) => (p.id === id ? { ...p, cantidad } : p)))
  }

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat("es-CL", {
      style: "currency",
      currency: "CLP",
    }).format(value)
  }

  const totalLabels = products.reduce((sum, p) => sum + p.cantidad, 0)

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold text-foreground">Generador de Etiquetas</h1>
          <p className="text-sm text-muted-foreground">
            Genera etiquetas de precios para imprimir
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" onClick={() => setPreviewOpen(true)} disabled={products.length === 0}>
            <FileText className="mr-2 h-4 w-4" />
            Vista Previa PDF
          </Button>
          <Button disabled={products.length === 0}>
            <Printer className="mr-2 h-4 w-4" />
            Generar Etiquetas
          </Button>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-3">
        <Card>
          <CardContent className="flex items-center gap-4 p-4">
            <div className="rounded-lg bg-blue-100 p-2">
              <Package className="h-5 w-5 text-blue-600" />
            </div>
            <div>
              <p className="text-2xl font-semibold">{products.length}</p>
              <p className="text-sm text-muted-foreground">Productos</p>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="flex items-center gap-4 p-4">
            <div className="rounded-lg bg-green-100 p-2">
              <Tag className="h-5 w-5 text-green-600" />
            </div>
            <div>
              <p className="text-2xl font-semibold">{totalLabels}</p>
              <p className="text-sm text-muted-foreground">Total Etiquetas</p>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="flex items-center gap-4 p-4">
            <div className="rounded-lg bg-purple-100 p-2">
              <Printer className="h-5 w-5 text-purple-600" />
            </div>
            <div>
              <p className="text-2xl font-semibold">{Math.ceil(totalLabels / 24)}</p>
              <p className="text-sm text-muted-foreground">Páginas A4</p>
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Escanear Producto</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex gap-4">
            <Input
              placeholder="Escanea o ingresa el código de barras"
              value={barcodeInput}
              onChange={(e) => setBarcodeInput(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && handleScan()}
              className="max-w-md font-mono"
            />
            <Button onClick={handleScan}>
              <ScanLine className="mr-2 h-4 w-4" />
              Agregar
            </Button>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle>Productos para Etiquetar</CardTitle>
            {products.length > 0 && (
              <Button
                variant="outline"
                size="sm"
                onClick={() => setProducts([])}
                className="text-destructive hover:text-destructive"
              >
                Limpiar Lista
              </Button>
            )}
          </div>
        </CardHeader>
        <CardContent>
          {products.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-12 text-muted-foreground">
              <Tag className="mb-4 h-12 w-12" />
              <p>No hay productos agregados</p>
              <p className="text-sm">Escanea un código de barras para comenzar</p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-border text-left text-sm font-medium text-muted-foreground">
                    <th className="pb-3">Código de Barras</th>
                    <th className="pb-3">Producto</th>
                    <th className="pb-3 text-right">Precio</th>
                    <th className="pb-3 text-center">Cantidad</th>
                    <th className="pb-3"></th>
                  </tr>
                </thead>
                <tbody>
                  {products.map((product) => (
                    <tr
                      key={product.id}
                      className="border-b border-border last:border-0 hover:bg-muted/50"
                    >
                      <td className="py-3 font-mono text-sm">{product.codigoBarras}</td>
                      <td className="py-3 font-medium">{product.producto}</td>
                      <td className="py-3 text-right font-semibold">
                        {formatCurrency(product.precio)}
                      </td>
                      <td className="py-3">
                        <div className="flex items-center justify-center gap-2">
                          <Button
                            variant="outline"
                            size="icon"
                            className="h-8 w-8"
                            onClick={() => updateQuantity(product.id, product.cantidad - 1)}
                          >
                            -
                          </Button>
                          <span className="w-8 text-center">{product.cantidad}</span>
                          <Button
                            variant="outline"
                            size="icon"
                            className="h-8 w-8"
                            onClick={() => updateQuantity(product.id, product.cantidad + 1)}
                          >
                            +
                          </Button>
                        </div>
                      </td>
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
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>

      <Dialog open={previewOpen} onOpenChange={setPreviewOpen}>
        <DialogContent className="max-w-4xl max-h-[80vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Vista Previa de Etiquetas</DialogTitle>
            <DialogDescription>
              Previsualiza las etiquetas antes de imprimirlas
            </DialogDescription>
          </DialogHeader>
          <div className="py-4">
            <div className="border border-border rounded-lg p-8 bg-white">
              <div className="grid grid-cols-4 gap-4">
                {products.flatMap((product) =>
                  Array.from({ length: product.cantidad }).map((_, idx) => (
                    <div
                      key={`${product.id}-${idx}`}
                      className="border border-dashed border-gray-300 p-3 rounded flex flex-col items-center justify-center text-center"
                    >
                      <div className="w-full h-8 bg-gray-100 rounded mb-2 flex items-center justify-center">
                        <span className="text-xs font-mono">{product.codigoBarras}</span>
                      </div>
                      <p className="text-xs font-medium line-clamp-2 mb-1">
                        {product.producto}
                      </p>
                      <p className="text-lg font-bold text-primary">
                        {formatCurrency(product.precio)}
                      </p>
                    </div>
                  ))
                )}
              </div>
            </div>
            <p className="text-sm text-muted-foreground mt-4 text-center">
              Las etiquetas se imprimirán en formato A4 (24 etiquetas por página)
            </p>
          </div>
          <div className="flex justify-end gap-2">
            <Button variant="outline" onClick={() => setPreviewOpen(false)}>
              Cerrar
            </Button>
            <Button>
              <Printer className="mr-2 h-4 w-4" />
              Imprimir
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  )
}
