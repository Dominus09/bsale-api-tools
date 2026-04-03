export type OrderRow = {
  id: number
  client_name: string | null
  rut: string | null
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
  rut: string | null
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
