"use client"

import { useEffect, useState } from "react"
import { AlertTriangle, Loader2, Search, Download } from "lucide-react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { getMarginAnalysis, type MarginProduct } from "@/lib/api"

const statusColors: Record<string, string> = {
  LOW_MARGIN: "bg-red-500 text-white",
  OK: "bg-green-500 text-white",
  HIGH_MARGIN: "bg-yellow-500 text-white",
  ULTRA_HIGH_MARGIN: "bg-purple-500 text-white",
}

const statusLabels: Record<string, string> = {
  LOW_MARGIN: "Bajo",
  OK: "OK",
  HIGH_MARGIN: "Alto",
  ULTRA_HIGH_MARGIN: "Ultra Alto",
}

export default function MarginsPage() {
  const [products, setProducts] = useState<MarginProduct[]>([])
  const [filteredProducts, setFilteredProducts] = useState<MarginProduct[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [searchQuery, setSearchQuery] = useState("")
  const [statusFilter, setStatusFilter] = useState<string>("all")

  useEffect(() => {
    async function loadData() {
      try {
        const data = await getMarginAnalysis()
        setProducts(data)
        setFilteredProducts(data)
      } catch (err) {
        setError(err instanceof Error ? err.message : "Error al cargar datos")
      } finally {
        setIsLoading(false)
      }
    }

    loadData()
  }, [])

  useEffect(() => {
    let result = products

    if (searchQuery) {
      result = result.filter((p) =>
        p.product_name.toLowerCase().includes(searchQuery.toLowerCase())
      )
    }

    if (statusFilter !== "all") {
      result = result.filter((p) => p.status === statusFilter)
    }

    setFilteredProducts(result)
  }, [searchQuery, statusFilter, products])

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat("es-CL", {
      style: "currency",
      currency: "CLP",
      minimumFractionDigits: 0,
    }).format(value)
  }

  if (isLoading) {
    return (
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex h-full items-center justify-center">
        <Card className="w-full max-w-md">
          <CardContent className="flex flex-col items-center py-8">
            <AlertTriangle className="mb-4 h-12 w-12 text-destructive" />
            <p className="text-center text-muted-foreground">{error}</p>
          </CardContent>
        </Card>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-2xl font-bold text-foreground">Análisis de Márgenes</h1>
          <p className="text-muted-foreground">
            Revisión detallada de márgenes por producto
          </p>
        </div>
        <Button variant="outline" className="gap-2">
          <Download className="h-4 w-4" />
          Exportar
        </Button>
      </div>

      {/* Filters */}
      <Card>
        <CardContent className="flex flex-col gap-4 pt-6 sm:flex-row">
          <div className="relative flex-1">
            <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              placeholder="Buscar producto..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-10"
            />
          </div>
          <Select value={statusFilter} onValueChange={setStatusFilter}>
            <SelectTrigger className="w-full sm:w-48">
              <SelectValue placeholder="Filtrar por estado" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">Todos los estados</SelectItem>
              <SelectItem value="LOW_MARGIN">Bajo margen</SelectItem>
              <SelectItem value="OK">OK</SelectItem>
              <SelectItem value="HIGH_MARGIN">Alto margen</SelectItem>
              <SelectItem value="ULTRA_HIGH_MARGIN">Ultra alto</SelectItem>
            </SelectContent>
          </Select>
        </CardContent>
      </Card>

      {/* Stats Summary */}
      <div className="grid gap-4 sm:grid-cols-4">
        <Card>
          <CardContent className="pt-6">
            <div className="text-2xl font-bold text-red-500">
              {products.filter((p) => p.status === "LOW_MARGIN").length}
            </div>
            <p className="text-sm text-muted-foreground">Bajo margen</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="text-2xl font-bold text-green-500">
              {products.filter((p) => p.status === "OK").length}
            </div>
            <p className="text-sm text-muted-foreground">OK</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="text-2xl font-bold text-yellow-500">
              {products.filter((p) => p.status === "HIGH_MARGIN").length}
            </div>
            <p className="text-sm text-muted-foreground">Alto margen</p>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="pt-6">
            <div className="text-2xl font-bold text-purple-500">
              {products.filter((p) => p.status === "ULTRA_HIGH_MARGIN").length}
            </div>
            <p className="text-sm text-muted-foreground">Ultra alto</p>
          </CardContent>
        </Card>
      </div>

      {/* Products Table */}
      <Card>
        <CardHeader>
          <CardTitle>Productos ({filteredProducts.length})</CardTitle>
          <CardDescription>
            Lista completa de productos con análisis de márgenes
          </CardDescription>
        </CardHeader>
        <CardContent>
          {filteredProducts.length === 0 ? (
            <p className="py-8 text-center text-muted-foreground">
              No se encontraron productos
            </p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-border">
                    <th className="pb-3 text-left text-sm font-medium text-muted-foreground">
                      Producto
                    </th>
                    <th className="pb-3 text-right text-sm font-medium text-muted-foreground">
                      Costo
                    </th>
                    <th className="pb-3 text-right text-sm font-medium text-muted-foreground">
                      Precio
                    </th>
                    <th className="pb-3 text-right text-sm font-medium text-muted-foreground">
                      Margen
                    </th>
                    <th className="pb-3 text-center text-sm font-medium text-muted-foreground">
                      Estado
                    </th>
                    <th className="pb-3 text-right text-sm font-medium text-muted-foreground">
                      Precio Sugerido
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {filteredProducts.map((product, index) => (
                    <tr
                      key={product.id ?? `product-${index}`}
                      className="border-b border-border last:border-0 hover:bg-muted/50"
                    >
                      <td className="py-3 text-sm font-medium">{product.product_name ?? "—"}</td>
                      <td className="py-3 text-right text-sm">
                        {product.cost != null ? formatCurrency(product.cost) : "—"}
                      </td>
                      <td className="py-3 text-right text-sm">
                        {product.price != null ? formatCurrency(product.price) : "—"}
                      </td>
                      <td className="py-3 text-right text-sm font-medium">
                        {product.margin != null ? `${product.margin.toFixed(1)}%` : "—"}
                      </td>
                      <td className="py-3 text-center">
                        <Badge className={statusColors[product.status] ?? "bg-gray-500 text-white"}>
                          {statusLabels[product.status] ?? "—"}
                        </Badge>
                      </td>
                      <td className="py-3 text-right text-sm text-muted-foreground">
                        {product.suggested_price != null ? formatCurrency(product.suggested_price) : "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
