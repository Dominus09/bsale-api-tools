/* Quillotana Operativa PWA — SW offline-safe.
 * - NUNCA responde null/undefined a respondWith (Safari: "Returned response is null").
 * - NUNCA cachea /api-upstream, /api, /login ni Authorization.
 * - NUNCA sirve HTML de otra ruta (p.ej. /despachos) para /despachos/:id/clientes:
 *   eso provoca "Application error: a client-side exception" en Next App Router.
 * - Navegación: network-first → HTML exacto en cache → offline.html → sintético.
 * - Assets estáticos: cache-first; nunca null.
 */
const VERSION = "lq-pwa-v8-offline-route-guard";
const STATIC_CACHE = `${VERSION}-static`;
const PAGE_CACHE = `${VERSION}-pages`;
const ASSET_CACHE = `${VERSION}-assets`;
const OFFLINE_HTML = "/offline.html";
const OFFLINE_ROUTE = "/offline";
/** Shell de app para arranque frío SOLO cuando la URL pedida es /despachos. */
const APP_SHELL = "/despachos";

const PRECACHE = [
  OFFLINE_HTML,
  OFFLINE_ROUTE,
  APP_SHELL,
  "/",
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

/** JS vacío válido: evita SyntaxError si un chunk no está en cache offline. */
function syntheticOfflineScript() {
  return new Response(
    "/* offline: chunk no cacheado — reabre online y sincroniza el despacho */",
    {
      status: 503,
      statusText: "Offline chunk",
      headers: {
        "Content-Type": "application/javascript; charset=utf-8",
        "Cache-Control": "no-store",
        "X-LQ-Offline-Chunk": "1",
      },
    },
  );
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

async function matchExactPage(request) {
  const url = new URL(request.url);
  const candidates = [request, request.url, url.pathname, url.origin + url.pathname];
  for (const c of candidates) {
    try {
      const hit = await caches.match(c, { ignoreSearch: false });
      if (hit) return hit;
      const hitIgnore = await caches.match(c, { ignoreSearch: true });
      if (hitIgnore) return hitIgnore;
    } catch {
      /* continue */
    }
  }
  return null;
}

/**
 * Fallback offline para navegación.
 * NUNCA devolver APP_SHELL para una ruta distinta: Next hidrata mal y muestra
 * "Application error: a client-side exception has occurred".
 */
async function safeOfflineNavigation(request) {
  const path = new URL(request.url).pathname;
  if (path === APP_SHELL || path === "/") {
    const hit = await matchFirst([APP_SHELL, "/", OFFLINE_HTML, OFFLINE_ROUTE]);
    return hit || syntheticOfflineHtml();
  }
  const offline = await matchFirst([OFFLINE_HTML, OFFLINE_ROUTE]);
  return offline || syntheticOfflineHtml();
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

  const isRsc =
    request.headers.get("RSC") === "1" ||
    request.headers.get("Next-Router-State-Tree") != null ||
    url.searchParams.has("_rsc");

  if (request.mode === "navigate") {
    event.respondWith(handleNavigate(request));
    return;
  }

  if (isRsc) {
    // Online: fetch normal. Offline: 503 vacío (no text genérico a Flight).
    event.respondWith(
      (async () => {
        try {
          return await fetch(request);
        } catch {
          const cached = await caches.match(request);
          if (cached) return cached;
          return new Response("", {
            status: 503,
            statusText: "Offline RSC",
            headers: {
              "Content-Type": "text/plain; charset=utf-8",
              "Cache-Control": "no-store",
              "X-LQ-Offline-RSC": "1",
            },
          });
        }
      })(),
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
    const cached = await matchExactPage(request);
    if (cached) return cached;
    if (fresh) return fresh;
    return safeOfflineNavigation(request);
  } catch {
    const cached = await matchExactPage(request);
    if (cached) return cached;
    return safeOfflineNavigation(request);
  }
}

async function handleAsset(request) {
  const cached = await caches.match(request);
  if (cached) return cached;
  const path = new URL(request.url).pathname;
  try {
    const fresh = await fetch(request);
    if (fresh && fresh.ok) {
      const ct = (fresh.headers.get("content-type") || "").toLowerCase();
      if (
        ct.includes("text/html") &&
        (path === "/" ||
          path === "/offline" ||
          path === "/offline.html" ||
          path.startsWith("/despachos"))
      ) {
        await putCopy(PAGE_CACHE, request, fresh);
      } else if (path.startsWith("/_next/static/") || path.startsWith("/icons/")) {
        await putCopy(ASSET_CACHE, request, fresh);
      }
      return fresh;
    }
    if (path.startsWith("/_next/static/") && path.endsWith(".js")) {
      return syntheticOfflineScript();
    }
    return fresh || syntheticOfflineText();
  } catch {
    const again = await caches.match(request);
    if (again) return again;
    if (path.startsWith("/_next/static/") && path.endsWith(".js")) {
      return syntheticOfflineScript();
    }
    return syntheticOfflineText();
  }
}
