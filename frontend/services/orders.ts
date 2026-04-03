export type OrderRow = {
  id: number
  client_name: string | null
  rut: string | null
  total: number
  status: string | null
  created_at: string
}

export async function getOrders(): Promise<OrderRow[]> {
  const res = await fetch("https://api.quillotana.cl/orders")

  if (!res.ok) {
    throw new Error("Error cargando pedidos")
  }

  return res.json()
}
