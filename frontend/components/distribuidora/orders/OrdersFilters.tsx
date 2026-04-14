"use client"

import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Checkbox } from "@/components/ui/checkbox"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"

export type SellerOption = { user_id: number; label: string }

export type MunicipalityOption = { value: string; label: string }

type OrdersFiltersProps = {
  dateFrom: string
  onDateFromChange: (isoDate: string) => void
  dateTo: string
  onDateToChange: (isoDate: string) => void
  deliverySearch: string
  onDeliverySearchChange: (value: string) => void
  sellerOptions: SellerOption[]
  sellerUserId: string
  onSellerUserIdChange: (value: string) => void
  municipalityOptions: MunicipalityOption[]
  municipalityKey: string
  onMunicipalityKeyChange: (value: string) => void
  onlyNotInvoiced: boolean
  onOnlyNotInvoicedChange: (value: boolean) => void
  loading?: boolean
}

export function OrdersFilters({
  dateFrom,
  onDateFromChange,
  dateTo,
  onDateToChange,
  deliverySearch,
  onDeliverySearchChange,
  sellerOptions,
  sellerUserId,
  onSellerUserIdChange,
  municipalityOptions,
  municipalityKey,
  onMunicipalityKeyChange,
  onlyNotInvoiced,
  onOnlyNotInvoicedChange,
  loading,
}: OrdersFiltersProps) {
  return (
    <div className="flex flex-col gap-4 rounded-lg border bg-card p-4 shadow-sm">
      <h2 className="text-sm font-semibold text-foreground">Filtros</h2>
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6">
        <div className="space-y-2">
          <Label htmlFor="orders-date-from">Emisión desde</Label>
          <Input
            id="orders-date-from"
            type="date"
            value={dateFrom}
            onChange={(e) => onDateFromChange(e.target.value)}
            disabled={loading}
          />
        </div>
        <div className="space-y-2">
          <Label htmlFor="orders-date-to">Emisión hasta</Label>
          <Input
            id="orders-date-to"
            type="date"
            value={dateTo}
            onChange={(e) => onDateToChange(e.target.value)}
            disabled={loading}
          />
        </div>
        <div className="space-y-2 xl:col-span-2">
          <Label htmlFor="orders-delivery">Observaciones / día entrega</Label>
          <Input
            id="orders-delivery"
            type="text"
            placeholder="Ej. jueves o jueves,viernes"
            value={deliverySearch}
            onChange={(e) => onDeliverySearchChange(e.target.value)}
            disabled={loading}
          />
        </div>
        <div className="space-y-2">
          <Label>Vendedor</Label>
          <Select
            value={sellerUserId === "" ? "__all_sellers__" : sellerUserId}
            onValueChange={(v) =>
              onSellerUserIdChange(v === "__all_sellers__" ? "" : v)
            }
            disabled={loading}
          >
            <SelectTrigger className="w-full">
              <SelectValue placeholder="Todos" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="__all_sellers__">Todos</SelectItem>
              {sellerOptions.map((o) => (
                <SelectItem key={o.user_id} value={String(o.user_id)}>
                  {o.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="space-y-2">
          <Label>Ciudad / comuna</Label>
          <Select
            value={municipalityKey === "" ? "__all_muni__" : municipalityKey}
            onValueChange={(v) =>
              onMunicipalityKeyChange(v === "__all_muni__" ? "" : v)
            }
            disabled={loading}
          >
            <SelectTrigger className="w-full">
              <SelectValue placeholder="Todas" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="__all_muni__">Todas</SelectItem>
              {municipalityOptions.map((m) => (
                <SelectItem key={m.value} value={m.value}>
                  {m.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="flex items-end pb-1">
          <label className="flex cursor-pointer items-center gap-2 text-sm">
            <Checkbox
              checked={onlyNotInvoiced}
              onCheckedChange={(c) => onOnlyNotInvoicedChange(c === true)}
              disabled={loading}
            />
            Solo no facturadas
          </label>
        </div>
      </div>
    </div>
  )
}
