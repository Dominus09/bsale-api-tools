import { getApiBaseUrl } from "@/lib/api-base"

export type OrderRow = {
  id: number
  client_name: string | null
  rut: string | null
  payment_method: string | null
  price_list: string | null
  delivery_date: string | null
  total: number
  status: string | null
  created_at: string
}

export type OrderItemDetail = {
  product_name: string | null
  barcode: string | null
  quantity: number
  price: number
}

export type OrderDetail = {
  id: number
  client_name: string | null
  client_rut: string | null
  rut: string | null
  payment_method: string | null
  price_list: string | null
  delivery_date: string | null
  notes: string | null
  contact_name: string | null
  contact_phone: string | null
  total: number
  status: string | null
  created_at: string
  items: OrderItemDetail[]
}

const ORDERS_PAGE_SIZE = 20

export async function getOrders(params: {
  page: number
  limit?: number
  status?: string
}): Promise<OrderRow[]> {
  const limit = params.limit ?? ORDERS_PAGE_SIZE
  const qs = new URLSearchParams({
    page: String(params.page),
    limit: String(limit),
  })
  if (params.status && params.status !== "all") {
    qs.set("status", params.status)
  }
  const res = await fetch(`${getApiBaseUrl()}/orders?${qs.toString()}`)

  if (!res.ok) {
    throw new Error("Error cargando pedidos")
  }

  return res.json()
}

export { ORDERS_PAGE_SIZE }

export async function getOrderById(id: number): Promise<OrderDetail> {
  const res = await fetch(`${getApiBaseUrl()}/orders/${id}`)

  if (!res.ok) {
    throw new Error("Error cargando pedido")
  }

  return res.json()
}

export type UpdateOrderStatusResponse = {
  id: number
  status: string
}

export async function updateOrderStatus(
  id: number,
  status: string,
): Promise<UpdateOrderStatusResponse> {
  const res = await fetch(`${getApiBaseUrl()}/orders/${id}/status`, {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ status }),
  })

  if (!res.ok) {
    throw new Error("Error actualizando estado")
  }

  return res.json()
}
