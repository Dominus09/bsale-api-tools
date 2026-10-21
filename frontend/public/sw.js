/* La Quillotana ERP — Service Worker PWA (v1, alcance mínimo y seguro).
 *
 * Política de caché deliberadamente conservadora:
 * - SOLO recursos estáticos seguros: íconos PWA, manifest y la página
 *   informativa de falta de conexión (/offline).
 * - NUNCA respuestas de API (/api-upstream, /api), datos autenticados,
 *   tokens, costos, precios, clientes ni stock.
 * - Las navegaciones van siempre a la red; sin conexión se muestra /offline.
 *   Nada aparenta haberse guardado sin conexión.
 * - Sin caché del shell HTML ni de JS de la app: tras un despliegue no queda
 *   JavaScript desactualizado servido por este worker (los assets con hash
 *   los gestiona la caché HTTP normal del navegador).
 * Para invalidar todo en un despliegue futuro, subir VERSION.
 */
const VERSION = "lq-erp-v1";
const STATIC_CACHE = `${VERSION}-static`;
const OFFLINE_URL = "/offline";
const PRECACHE = [
  OFFLINE_URL,
  "/manifest.webmanifest",
  "/icons/pwa/icon-192.png",
  "/icons/pwa/icon-512.png",
  "/icons/pwa/icon-maskable-512.png",
  "/icons/pwa/apple-touch-icon.png",
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches
      .open(STATIC_CACHE)
      .then((cache) => cache.addAll(PRECACHE))
      .then(() => self.skipWaiting()),
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) =>
        Promise.all(
          keys
            .filter((key) => !key.startsWith(VERSION))
            .map((key) => caches.delete(key)),
        ),
      )
      .then(() => self.clients.claim()),
  );
});

self.addEventListener("fetch", (event) => {
  const { request } = event;
  if (request.method !== "GET") return;

  const url = new URL(request.url);
  if (url.origin !== self.location.origin) return;

  // Jamás interceptar ni cachear API ni datos autenticados/operativos.
  if (
    url.pathname.startsWith("/api-upstream") ||
    url.pathname.startsWith("/api/")
  ) {
    return;
  }

  // Navegaciones: red primero; sin conexión, página informativa.
  if (request.mode === "navigate") {
    event.respondWith(fetch(request).catch(() => caches.match(OFFLINE_URL)));
    return;
  }

  // Íconos y manifest: caché del worker con reposición desde red.
  if (PRECACHE.includes(url.pathname)) {
    event.respondWith(
      caches.match(request).then(
        (hit) =>
          hit ||
          fetch(request).then((response) => {
            const copy = response.clone();
            caches.open(STATIC_CACHE).then((cache) => cache.put(request, copy));
            return response;
          }),
      ),
    );
  }
});
