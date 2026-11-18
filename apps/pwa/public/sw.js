/* Quillotana Operativa PWA — SW offline-safe.
 * - NUNCA responde null/undefined a respondWith (Safari: "Returned response is null").
 * - NUNCA cachea /api-upstream, /api, /login ni Authorization.
 * - Navegación: network-first → HTML cacheado → offline.html → Response sintético.
 * - Assets estáticos: cache-first con fallback de red; nunca null.
 */
const VERSION = "lq-pwa-v4-offline-shell";
const STATIC_CACHE = `${VERSION}-static`;
const PAGE_CACHE = `${VERSION}-pages`;
const ASSET_CACHE = `${VERSION}-assets`;
const OFFLINE_HTML = "/offline.html";
const OFFLINE_ROUTE = "/offline";

const PRECACHE = [
  OFFLINE_HTML,
  OFFLINE_ROUTE,
  "/manifest.webmanifest",
  "/icons/pwa/icon-192.png",
  "/icons/pwa/icon-512.png",
  "/icons/pwa/icon-maskable-512.png",
  "/icons/pwa/apple-touch-icon.png",
];

const FALLBACK_HTML = `<!doctype html><html lang="es"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/><title>Sin conexión</title><style>body{margin:0;background:#0b0d12;color:#f3f4f6;font-family:system-ui,sans-serif;padding:24px}a{color:#ffb4bc}</style></head><body><h1>Sin conexión</h1><p>No hay red. Reabre la app cuando tengas señal o ve a <a href="/despachos">Mis despachos</a> si ya sincronizaste datos.</p></body></html>`;

function isApiPath(pathname) {
  return (
    pathname.startsWith("/api-upstream") ||
    pathname.startsWith("/api/") ||
    pathname === "/api" ||
    pathname === "/login"
  );
}

function syntheticOfflineHtml() {
  return new Response(FALLBACK_HTML, {
    status: 503,
    statusText: "Offline",
    headers: {
      "Content-Type": "text/html; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

function syntheticOfflineText() {
  return new Response("Sin conexión", {
    status: 503,
    statusText: "Offline",
    headers: {
      "Content-Type": "text/plain; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

async function matchFirst(urls) {
  for (const u of urls) {
    try {
      const hit = await caches.match(u, { ignoreSearch: false });
      if (hit) return hit;
      const hitIgnore = await caches.match(u, { ignoreSearch: true });
      if (hitIgnore) return hitIgnore;
    } catch {
      /* continue */
    }
  }
  return null;
}

async function safeOfflineNavigation() {
  const hit = await matchFirst([OFFLINE_HTML, OFFLINE_ROUTE]);
  return hit || syntheticOfflineHtml();
}

async function putCopy(cacheName, request, response) {
  try {
    if (!response || !response.ok) return;
    if (request.method !== "GET") return;
    const cache = await caches.open(cacheName);
    await cache.put(request, response.clone());
  } catch {
    /* quota / opaque */
  }
}

self.addEventListener("install", (event) => {
  event.waitUntil(
    (async () => {
      const cache = await caches.open(STATIC_CACHE);
      await Promise.all(
        PRECACHE.map(async (url) => {
          try {
            await cache.add(url);
          } catch {
            /* un recurso no debe tumbar el install */
          }
        }),
      );
      // Garantiza shell offline aunque /offline Next falle.
      try {
        const existing = await cache.match(OFFLINE_HTML);
        if (!existing) {
          await cache.put(OFFLINE_HTML, syntheticOfflineHtml());
        }
      } catch {
        /* ignore */
      }
      await self.skipWaiting();
    })(),
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    (async () => {
      const keys = await caches.keys();
      await Promise.all(
        keys
          .filter((key) => !key.startsWith(VERSION))
          .map((key) => caches.delete(key)),
      );
      await self.clients.claim();
    })(),
  );
});

self.addEventListener("fetch", (event) => {
  const { request } = event;
  if (request.method !== "GET") return;

  let url;
  try {
    url = new URL(request.url);
  } catch {
    return;
  }
  if (url.origin !== self.location.origin) return;
  if (isApiPath(url.pathname)) return;

  // App Router RSC / data: no cachear; nunca null.
  const isRsc =
    request.headers.get("RSC") === "1" ||
    request.headers.get("Next-Router-State-Tree") != null ||
    url.searchParams.has("_rsc");

  if (request.mode === "navigate") {
    event.respondWith(handleNavigate(request));
    return;
  }

  if (isRsc) {
    event.respondWith(
      fetch(request).catch(async () => {
        const cached = await caches.match(request);
        return cached || syntheticOfflineText();
      }),
    );
    return;
  }

  event.respondWith(handleAsset(request));
});

async function handleNavigate(request) {
  try {
    const fresh = await fetch(request);
    if (fresh && fresh.ok) {
      await putCopy(PAGE_CACHE, request, fresh);
      return fresh;
    }
    // Respuesta no OK: intentar cache / offline (nunca null).
    const cached = await caches.match(request);
    if (cached) return cached;
    if (fresh) return fresh;
    return safeOfflineNavigation();
  } catch {
    const cached = await caches.match(request);
    if (cached) return cached;
    const pageHit = await matchFirst([request.url, OFFLINE_HTML, OFFLINE_ROUTE]);
    return pageHit || syntheticOfflineHtml();
  }
}

async function handleAsset(request) {
  const cached = await caches.match(request);
  if (cached) return cached;
  try {
    const fresh = await fetch(request);
    if (fresh && fresh.ok) {
      const path = new URL(request.url).pathname;
      if (path.startsWith("/_next/static/") || path.startsWith("/icons/")) {
        await putCopy(ASSET_CACHE, request, fresh);
      }
      return fresh;
    }
    return fresh || syntheticOfflineText();
  } catch {
    const again = await caches.match(request);
    return again || syntheticOfflineText();
  }
}
