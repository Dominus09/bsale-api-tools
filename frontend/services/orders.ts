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

export async function getOrders(): Promise<OrderRow[]> {
  const res = await fetch("https://api.quillotana.cl/orders")

  if (!res.ok) {
    throw new Error("Error cargando pedidos")
  }

  return res.json()
}

export async function getOrderById(id: number): Promise<OrderDetail> {
  const res = await fetch(`https://api.quillotana.cl/orders/${id}`)

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
  const res = await fetch(`https://api.quillotana.cl/orders/${id}/status`, {
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
