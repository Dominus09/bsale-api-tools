"use client"

import { useEffect, useMemo, useState } from "react"
import { Edit, Plus, Search, Upload } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import {
  createSupplier,
  getSuppliers,
  type Supplier,
  updateSupplier,
} from "@/lib/api"

type FormState = {
  name: string
  contact_name: string
  phone: string
  email: string
  notes: string
}

const emptyForm: FormState = {
  name: "",
  contact_name: "",
  phone: "",
  email: "",
  notes: "",
}

// Feature flags locales para habilitar módulos futuros sin rehacer la pantalla.
const featureFlags = {
  bulkUpload: false,
  bulkEdit: false,
  purchaseOrders: false,
}

export default function Page() {
  const [suppliers, setSuppliers] = useState<Supplier[]>([])
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState("")
  const [searchTerm, setSearchTerm] = useState("")
  const [dialogOpen, setDialogOpen] = useState(false)
  const [editingSupplier, setEditingSupplier] = useState<Supplier | null>(null)
  const [form, setForm] = useState<FormState>(emptyForm)

  const filteredSuppliers = useMemo(() => {
    const term = searchTerm.trim().toLowerCase()
    if (!term) return suppliers
    return suppliers.filter((s) => s.name.toLowerCase().includes(term))
  }, [suppliers, searchTerm])

  async function loadSuppliers(name?: string) {
    setLoading(true)
    setError("")
    try {
      const data = await getSuppliers(name)
      setSuppliers(data)
    } catch {
      setError("No se pudieron cargar los proveedores")
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadSuppliers()
  }, [])

  function openCreateDialog() {
    setEditingSupplier(null)
    setForm(emptyForm)
    setDialogOpen(true)
  }

  function openEditDialog(supplier: Supplier) {
    setEditingSupplier(supplier)
    setForm({
      name: supplier.name ?? "",
      contact_name: supplier.contact_name ?? "",
      phone: supplier.phone ?? "",
      email: supplier.email ?? "",
      notes: supplier.notes ?? "",
    })
    setDialogOpen(true)
  }

  function closeDialog() {
    if (saving) return
    setDialogOpen(false)
    setEditingSupplier(null)
    setForm(emptyForm)
  }

  async function handleSave() {
    const cleanName = form.name.trim()
    if (!cleanName) {
      setError("El nombre del proveedor es obligatorio")
      return
    }

    setSaving(true)
    setError("")
    try {
      if (editingSupplier) {
        const updated = await updateSupplier(editingSupplier.id, {
          name: cleanName,
          contact_name: form.contact_name.trim() || null,
          phone: form.phone.trim() || null,
          email: form.email.trim() || null,
          notes: form.notes.trim() || null,
        })
        setSuppliers((prev) => prev.map((s) => (s.id === updated.id ? updated : s)))
      } else {
        const created = await createSupplier({
          name: cleanName,
          contact_name: form.contact_name.trim() || null,
          phone: form.phone.trim() || null,
          email: form.email.trim() || null,
          notes: form.notes.trim() || null,
        })
        setSuppliers((prev) => [created, ...prev])
      }
      closeDialog()
    } catch {
      setError("No se pudo guardar el proveedor")
    } finally {
      setSaving(false)
    }
  }

  async function handleToggleActive(supplier: Supplier) {
    setError("")
    try {
      const updated = await updateSupplier(supplier.id, {
        is_active: !supplier.is_active,
      })
      setSuppliers((prev) => prev.map((s) => (s.id === updated.id ? updated : s)))
    } catch {
      setError("No se pudo actualizar el estado del proveedor")
    }
  }

  async function handleBackendSearch() {
    await loadSuppliers(searchTerm)
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold text-foreground">Proveedores</h1>
          <p className="text-sm text-muted-foreground">Administra proveedores de compras</p>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            disabled={!featureFlags.bulkUpload}
            title="Próximamente: carga masiva de proveedores"
          >
            <Upload className="mr-2 h-4 w-4" />
            Carga masiva
          </Button>
          <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
            <DialogTrigger asChild>
              <Button onClick={openCreateDialog}>
                <Plus className="mr-2 h-4 w-4" />
                Nuevo proveedor
              </Button>
            </DialogTrigger>
            <DialogContent className="max-w-md">
              <DialogHeader>
                <DialogTitle>{editingSupplier ? "Editar proveedor" : "Nuevo proveedor"}</DialogTitle>
                <DialogDescription>
                  {editingSupplier
                    ? "Modifica los datos del proveedor seleccionado"
                    : "Ingresa los datos del nuevo proveedor"}
                </DialogDescription>
              </DialogHeader>
              <div className="space-y-3 py-2">
                <Input
                  placeholder="Nombre *"
                  value={form.name}
                  onChange={(e) => setForm((prev) => ({ ...prev, name: e.target.value }))}
                />
                <Input
                  placeholder="Nombre de contacto"
                  value={form.contact_name}
                  onChange={(e) => setForm((prev) => ({ ...prev, contact_name: e.target.value }))}
                />
                <Input
                  placeholder="Teléfono"
                  value={form.phone}
                  onChange={(e) => setForm((prev) => ({ ...prev, phone: e.target.value }))}
                />
                <Input
                  placeholder="Email"
                  value={form.email}
                  onChange={(e) => setForm((prev) => ({ ...prev, email: e.target.value }))}
                />
                <Input
                  placeholder="Notas"
                  value={form.notes}
                  onChange={(e) => setForm((prev) => ({ ...prev, notes: e.target.value }))}
                />
              </div>
              <DialogFooter>
                <Button variant="outline" onClick={closeDialog} disabled={saving}>
                  Cancelar
                </Button>
                <Button onClick={handleSave} disabled={saving}>
                  {saving ? "Guardando..." : "Guardar"}
                </Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>
        </div>
      </div>

      {error ? (
        <div className="rounded-md border border-destructive/30 bg-destructive/10 px-3 py-2 text-sm text-destructive">
          {error}
        </div>
      ) : null}

      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle>Listado de Proveedores</CardTitle>
            <div className="flex items-center gap-2">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                <Input
                  placeholder="Buscar proveedor..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="w-64 pl-9"
                />
              </div>
              <Button variant="outline" onClick={handleBackendSearch} disabled={loading}>
                Buscar
              </Button>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-border text-left text-sm font-medium text-muted-foreground">
                  <th className="pb-3">ID</th>
                  <th className="pb-3">Nombre</th>
                  <th className="pb-3">Contacto</th>
                  <th className="pb-3">Teléfono</th>
                  <th className="pb-3">Email</th>
                  <th className="pb-3 text-center">Activo</th>
                  <th className="pb-3 text-right">Acciones</th>
                </tr>
              </thead>
              <tbody>
                {loading ? (
                  <tr>
                    <td colSpan={7} className="py-6 text-center text-sm text-muted-foreground">
                      Cargando proveedores...
                    </td>
                  </tr>
                ) : filteredSuppliers.length === 0 ? (
                  <tr>
                    <td colSpan={7} className="py-6 text-center text-sm text-muted-foreground">
                      No hay proveedores para mostrar
                    </td>
                  </tr>
                ) : (
                  filteredSuppliers.map((supplier) => (
                    <tr key={supplier.id} className="border-b border-border last:border-0 hover:bg-muted/50">
                      <td className="py-3">{supplier.id}</td>
                      <td className="py-3 font-medium">{supplier.name}</td>
                      <td className="py-3">{supplier.contact_name || "-"}</td>
                      <td className="py-3">{supplier.phone || "-"}</td>
                      <td className="py-3">{supplier.email || "-"}</td>
                      <td className="py-3 text-center">
                        <span
                          className={
                            supplier.is_active
                              ? "inline-flex rounded-full bg-green-500 px-2 py-1 text-xs font-medium text-white"
                              : "inline-flex rounded-full bg-gray-400 px-2 py-1 text-xs font-medium text-white"
                          }
                        >
                          {supplier.is_active ? "Sí" : "No"}
                        </span>
                      </td>
                      <td className="py-3">
                        <div className="flex items-center justify-end gap-2">
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => openEditDialog(supplier)}
                            className="h-8 w-8"
                          >
                            <Edit className="h-4 w-4" />
                          </Button>
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => handleToggleActive(supplier)}
                          >
                            {supplier.is_active ? "Desactivar" : "Activar"}
                          </Button>
                        </div>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
