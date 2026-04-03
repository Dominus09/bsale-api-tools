"use client"

import { useEffect, useState } from "react"
import { AlertTriangle, Loader2, Package, Search, AlertCircle } from "lucide-react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { getProductsWithoutCost, type ProductWithoutCost } from "@/lib/api"

export default function ProductsWithoutCostPage() {
  const [products, setProducts] = useState<ProductWithoutCost[]>([])
  const [filteredProducts, setFilteredProducts] = useState<ProductWithoutCost[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [searchQuery, setSearchQuery] = useState("")

  useEffect(() => {
    async function loadData() {
      try {
        const data = await getProductsWithoutCost()
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
    if (searchQuery) {
      setFilteredProducts(
        products.filter((p) =>
          p.product_name.toLowerCase().includes(searchQuery.toLowerCase()) ||
          p.sku?.toLowerCase().includes(searchQuery.toLowerCase())
        )
      )
    } else {
      setFilteredProducts(products)
    }
  }, [searchQuery, products])

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
      <div>
        <h1 className="text-2xl font-bold text-foreground">Productos sin Costo</h1>
        <p className="text-muted-foreground">
          Productos que no tienen costo asignado en el sistema
        </p>
      </div>

      {/* Alert Banner */}
      {products.length > 0 && (
        <Card className="border-yellow-200 bg-yellow-50">
          <CardContent className="flex items-center gap-4 pt-6">
            <AlertCircle className="h-6 w-6 text-yellow-600" />
            <div>
              <p className="font-medium text-yellow-800">
                {products.length} productos sin costo asignado
              </p>
              <p className="text-sm text-yellow-700">
                Estos productos no pueden ser incluidos en el análisis de márgenes
              </p>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Search */}
      <Card>
        <CardContent className="pt-6">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              placeholder="Buscar por nombre o SKU..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-10"
            />
          </div>
        </CardContent>
      </Card>

      {/* Products List */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Package className="h-5 w-5 text-muted-foreground" />
            Lista de Productos ({filteredProducts.length})
          </CardTitle>
          <CardDescription>
            Productos que requieren actualización de costos
          </CardDescription>
        </CardHeader>
        <CardContent>
          {filteredProducts.length === 0 ? (
            <div className="flex flex-col items-center py-12">
              <div className="mb-4 flex h-16 w-16 items-center justify-center rounded-full bg-green-100">
                <Package className="h-8 w-8 text-green-500" />
              </div>
              <p className="text-lg font-medium text-foreground">
                {products.length === 0
                  ? "Todos los productos tienen costo"
                  : "No se encontraron resultados"}
              </p>
              <p className="text-muted-foreground">
                {products.length === 0
                  ? "No hay productos pendientes de actualización"
                  : "Intenta con otro término de búsqueda"}
              </p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-border">
                    <th className="pb-3 text-left text-sm font-medium text-muted-foreground">
                      Producto
                    </th>
                    <th className="pb-3 text-left text-sm font-medium text-muted-foreground">
                      SKU
                    </th>
                    <th className="pb-3 text-left text-sm font-medium text-muted-foreground">
                      Categoría
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {filteredProducts.map((product) => (
                    <tr
                      key={product.id}
                      className="border-b border-border last:border-0 hover:bg-muted/50"
                    >
                      <td className="py-3">
                        <div className="flex items-center gap-3">
                          <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-muted">
                            <Package className="h-4 w-4 text-muted-foreground" />
                          </div>
                          <span className="font-medium">{product.product_name}</span>
                        </div>
                      </td>
                      <td className="py-3 text-sm text-muted-foreground">
                        {product.sku || "—"}
                      </td>
                      <td className="py-3 text-sm text-muted-foreground">
                        {product.category || "Sin categoría"}
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
