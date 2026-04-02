/**
 * Base de la API sin barra final — evita URLs como ...cl//api/catalog
 */
const rawBase =
  (typeof process !== "undefined" && process.env.NEXT_PUBLIC_API_URL) ||
  "https://api.quillotana.cl";

export const API_URL = String(rawBase).replace(/\/+$/, "");

export async function loginClient(rut: string): Promise<Response> {
  return fetch(`${API_URL}/login-client`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ rut }),
  });
}

export async function getCatalog(
  price_list: string,
  in_stock?: boolean,
): Promise<Response> {
  const qs = new URLSearchParams({ price_list });
  if (in_stock === true) {
    qs.set("in_stock", "true");
  }
  return fetch(`${API_URL}/api/catalog?${qs.toString()}`);
}
