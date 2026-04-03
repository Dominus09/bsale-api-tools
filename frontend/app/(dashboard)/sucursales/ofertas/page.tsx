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
  Edit,
  Search,
  Download,
  FileText,
  FileSpreadsheet,
  Tag,
  Percent,
  Calendar,
} from "lucide-react"

type Offer = {
  id: string
  producto: string
  codigoBarras: string
  precioNormal: number
  precioOferta: number
  fechaInicio: string
  fechaFin: string
  tipo: "oferta" | "remate"
  activo: boolean
}

const initialOffers: Offer[] = [
  {
    id: "1",
    producto: "Coca-Cola 2L",
    codigoBarras: "7801234567890",
    precioNormal: 1890,
    precioOferta: 1490,
    fechaInicio: "2026-03-15",
    fechaFin: "2026-03-22",
    tipo: "oferta",
    activo: true,
  },
  {
    id: "2",
    producto: "Detergente Omo 3kg",
    codigoBarras: "7801234567891",
    precioNormal: 5490,
    precioOferta: 3990,
    fechaInicio: "2026-03-14",
    fechaFin: "2026-03-21",
    tipo: "oferta",
    activo: true,
  },
  {
    id: "3",
    producto: "Yogurt Soprole Pack 6",
    codigoBarras: "7801234567892",
    precioNormal: 2990,
    precioOferta: 1290,
    fechaInicio: "2026-03-16",
    fechaFin: "2026-03-17",
    tipo: "remate",
    activo: true,
  },
  {
    id: "4",
    producto: "Galletas Triton 140g",
    codigoBarras: "7801234567893",
    precioNormal: 890,
    precioOferta: 590,
    fechaInicio: "2026-03-10",
    fechaFin: "2026-03-15",
    tipo: "oferta",
    activo: false,
  },
]

export default function OfertasPage() {
  const [offers, setOffers] = useState<Offer[]>(initialOffers)
  const [searchTerm, setSearchTerm] = useState("")
  const [filterTipo, setFilterTipo] = useState<string>("all")
  const [dialogOpen, setDialogOpen] = useState(false)
  const [editingOffer, setEditingOffer] = useState<Offer | null>(null)
  const [formData, setFormData] = useState<Partial<Offer>>({
    producto: "",
    codigoBarras: "",
    precioNormal: 0,
    precioOferta: 0,
    fechaInicio: new Date().toISOString().split("T")[0],
    fechaFin: "",
    tipo: "oferta",
  })

  const filteredOffers = offers.filter((offer) => {
    const matchesSearch =
      offer.producto.toLowerCase().includes(searchTerm.toLowerCase()) ||
      offer.codigoBarras.includes(searchTerm)
    const matchesTipo = filterTipo === "all" || offer.tipo === filterTipo
    return matchesSearch && matchesTipo
  })

  const handleSave = () => {
    if (editingOffer) {
      setOffers(
        offers.map((o) =>
          o.id === editingOffer.id ? { ...o, ...formData } : o
        )
      )
    } else {
      const newOffer: Offer = {
        id: Date.now().toString(),
        producto: formData.producto || "",
        codigoBarras: formData.codigoBarras || "",
        precioNormal: formData.precioNormal || 0,
        precioOferta: formData.precioOferta || 0,
        fechaInicio: formData.fechaInicio || "",
        fechaFin: formData.fechaFin || "",
        tipo: formData.tipo || "oferta",
        activo: true,
      }
      setOffers([...offers, newOffer])
    }
    resetForm()
  }

  const resetForm = () => {
    setDialogOpen(false)
    setEditingOffer(null)
    setFormData({
      producto: "",
      codigoBarras: "",
      precioNormal: 0,
      precioOferta: 0,
      fechaInicio: new Date().toISOString().split("T")[0],
      fechaFin: "",
      tipo: "oferta",
    })
  }

  const handleEdit = (offer: Offer) => {
    setEditingOffer(offer)
    setFormData(offer)
    setDialogOpen(true)
  }

  const handleDelete = (id: string) => {
    setOffers(offers.filter((o) => o.id !== id))
  }

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat("es-CL", {
      style: "currency",
      currency: "CLP",
    }).format(value)
  }

  const calculateDiscount = (normal: number, oferta: number) => {
    return Math.round(((normal - oferta) / normal) * 100)
  }

  const stats = {
    totalOfertas: offers.filter((o) => o.tipo === "oferta").length,
    totalRemates: offers.filter((o) => o.tipo === "remate").length,
    activas: offers.filter((o) => o.activo).length,
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold text-foreground">Ofertas y Remates</h1>
          <p className="text-sm text-muted-foreground">
            Gestiona las promociones y productos en liquidación
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline">
            <FileText className="mr-2 h-4 w-4" />
            Exportar PDF
          </Button>
          <Button variant="outline">
            <FileSpreadsheet className="mr-2 h-4 w-4" />
            Exportar Excel
          </Button>
          <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
            <DialogTrigger asChild>
              <Button onClick={() => resetForm()}>
                <Plus className="mr-2 h-4 w-4" />
                Nueva Oferta
              </Button>
            </DialogTrigger>
            <DialogContent className="max-w-md">
              <DialogHeader>
                <DialogTitle>
                  {editingOffer ? "Editar Oferta" : "Nueva Oferta"}
                </DialogTitle>
                <DialogDescription>
                  {editingOffer ? "Modifica los datos de la oferta" : "Ingresa los datos de la nueva oferta o remate"}
                </DialogDescription>
              </DialogHeader>
              <div className="space-y-4 py-4">
                <div className="space-y-2">
                  <label className="text-sm font-medium">Producto</label>
                  <Input
                    value={formData.producto}
                    onChange={(e) =>
                      setFormData({ ...formData, producto: e.target.value })
                    }
                    placeholder="Nombre del producto"
                  />
                </div>
                <div className="space-y-2">
                  <label className="text-sm font-medium">Código de Barras</label>
                  <Input
                    value={formData.codigoBarras}
                    onChange={(e) =>
                      setFormData({ ...formData, codigoBarras: e.target.value })
                    }
                    placeholder="7801234567890"
                  />
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Precio Normal</label>
                    <Input
                      type="number"
                      value={formData.precioNormal}
                      onChange={(e) =>
                        setFormData({
                          ...formData,
                          precioNormal: parseInt(e.target.value) || 0,
                        })
                      }
                    />
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Precio Oferta</label>
                    <Input
                      type="number"
                      value={formData.precioOferta}
                      onChange={(e) =>
                        setFormData({
                          ...formData,
                          precioOferta: parseInt(e.target.value) || 0,
                        })
                      }
                    />
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Fecha Inicio</label>
                    <Input
                      type="date"
                      value={formData.fechaInicio}
                      onChange={(e) =>
                        setFormData({ ...formData, fechaInicio: e.target.value })
                      }
                    />
                  </div>
                  <div className="space-y-2">
                    <label className="text-sm font-medium">Fecha Fin</label>
                    <Input
                      type="date"
                      value={formData.fechaFin}
                      onChange={(e) =>
                        setFormData({ ...formData, fechaFin: e.target.value })
                      }
                    />
                  </div>
                </div>
                <div className="space-y-2">
                  <label className="text-sm font-medium">Tipo</label>
                  <Select
                    value={formData.tipo}
                    onValueChange={(v) =>
                      setFormData({ ...formData, tipo: v as "oferta" | "remate" })
                    }
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="oferta">Oferta</SelectItem>
                      <SelectItem value="remate">Remate</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
              <DialogFooter>
                <Button variant="outline" onClick={resetForm}>
                  Cancelar
                </Button>
                <Button onClick={handleSave}>Guardar</Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-3">
        <Card>
          <CardContent className="flex items-center gap-4 p-4">
            <div className="rounded-lg bg-blue-100 p-2">
              <Tag className="h-5 w-5 text-blue-600" />
            </div>
            <div>
              <p className="text-2xl font-semibold">{stats.totalOfertas}</p>
              <p className="text-sm text-muted-foreground">Ofertas</p>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="flex items-center gap-4 p-4">
            <div className="rounded-lg bg-red-100 p-2">
              <Percent className="h-5 w-5 text-red-600" />
            </div>
            <div>
              <p className="text-2xl font-semibold">{stats.totalRemates}</p>
              <p className="text-sm text-muted-foreground">Remates</p>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="flex items-center gap-4 p-4">
            <div className="rounded-lg bg-green-100 p-2">
              <Calendar className="h-5 w-5 text-green-600" />
            </div>
            <div>
              <p className="text-2xl font-semibold">{stats.activas}</p>
              <p className="text-sm text-muted-foreground">Activas</p>
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle>Listado de Promociones</CardTitle>
            <div className="flex items-center gap-2">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                <Input
                  placeholder="Buscar producto..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="pl-9 w-64"
                />
              </div>
              <Select value={filterTipo} onValueChange={setFilterTipo}>
                <SelectTrigger className="w-32">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">Todos</SelectItem>
                  <SelectItem value="oferta">Ofertas</SelectItem>
                  <SelectItem value="remate">Remates</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-border text-left text-sm font-medium text-muted-foreground">
                  <th className="pb-3">Producto</th>
                  <th className="pb-3 text-right">Precio Normal</th>
                  <th className="pb-3 text-right">Precio Oferta</th>
                  <th className="pb-3 text-center">Descuento</th>
                  <th className="pb-3 text-center">Período</th>
                  <th className="pb-3 text-center">Tipo</th>
                  <th className="pb-3 text-center">Estado</th>
                  <th className="pb-3"></th>
                </tr>
              </thead>
              <tbody>
                {filteredOffers.map((offer) => (
                  <tr
                    key={offer.id}
                    className="border-b border-border last:border-0 hover:bg-muted/50"
                  >
                    <td className="py-3">
                      <div className="font-medium">{offer.producto}</div>
                      <div className="text-xs text-muted-foreground font-mono">
                        {offer.codigoBarras}
                      </div>
                    </td>
                    <td className="py-3 text-right text-muted-foreground line-through">
                      {formatCurrency(offer.precioNormal)}
                    </td>
                    <td className="py-3 text-right font-semibold text-green-600">
                      {formatCurrency(offer.precioOferta)}
                    </td>
                    <td className="py-3 text-center">
                      <Badge variant="secondary" className="bg-red-100 text-red-700">
                        -{calculateDiscount(offer.precioNormal, offer.precioOferta)}%
                      </Badge>
                    </td>
                    <td className="py-3 text-center text-sm text-muted-foreground">
                      {offer.fechaInicio} - {offer.fechaFin}
                    </td>
                    <td className="py-3 text-center">
                      <Badge
                        className={
                          offer.tipo === "oferta"
                            ? "bg-blue-500 text-white"
                            : "bg-orange-500 text-white"
                        }
                      >
                        {offer.tipo === "oferta" ? "Oferta" : "Remate"}
                      </Badge>
                    </td>
                    <td className="py-3 text-center">
                      <Badge
                        className={
                          offer.activo
                            ? "bg-green-500 text-white"
                            : "bg-gray-400 text-white"
                        }
                      >
                        {offer.activo ? "Activa" : "Inactiva"}
                      </Badge>
                    </td>
                    <td className="py-3">
                      <div className="flex items-center justify-end gap-1">
                        <Button
                          variant="ghost"
                          size="icon"
                          onClick={() => handleEdit(offer)}
                          className="h-8 w-8"
                        >
                          <Edit className="h-4 w-4" />
                        </Button>
                        <Button
                          variant="ghost"
                          size="icon"
                          onClick={() => handleDelete(offer.id)}
                          className="h-8 w-8 text-muted-foreground hover:text-destructive"
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </div>
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
