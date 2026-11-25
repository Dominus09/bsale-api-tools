"""Convención de coordenadas métricas (SoT). El frontend debe usar la misma.

Origen: esquina de entrada/IZ del facility_boundary (x=0, y=0).
+X: largo del galpón / longitudinal (metros). Entrada en x≈0; fondo en x→length.
+Y: ancho del galpón / transversal (metros). IZ en y≈0; DER en y→width.
Rotación: grados antihorario; pivote = centro del rectángulo
  (x_m + width_m/2, y_m + depth_m/2).
elevation_m: cota del piso de edificio (0 = losa).
stack_levels_max: niveles de apilamiento autorizados (futuro) — independiente
  de height_clear_m; NULL = desconocido / no autorizado.
No almacenar píxeles.

Pantalla (editor, orientación de vista 0°): plano longitudinal —
  ancho (Y) horizontal (IZ izquierda), largo (X) vertical (entrada abajo).
  La orientación de vista (0/90/180/270) es solo display; no muta SoT.
"""

from __future__ import annotations

COORDINATE_CONVENTION = {
    "origin": "entrance_iz_corner_of_facility_boundary",
    "axis_x": "length_meters_entrance_to_back",
    "axis_y": "width_meters_iz_to_der",
    "rotation": "degrees_ccw_about_rect_center",
    "units": "meters",
    "elevation_m": "building_floor_slab_reference",
    "stack_levels_max": "authorized_pallet_stack_levels_future_independent_of_height_clear",
    "pixels": "never_source_of_truth",
    "screen_default": "longitudinal_entrance_bottom_iz_left",
    "view_orientation": "display_only_0_90_180_270_does_not_mutate_geometry",
}

ELEMENT_TYPES = frozenset(
    {
        "facility_boundary",
        "sector",
        "aisle",
        "position",
        "loose_storage",
        "wall",
        "pillar",
        "door",
        "gate",
        "stair",
        "office",
        "circulation",
        "obstacle",
        "unusable",
        "beam",
        "other",
    }
)

# Clasificación de almacenamiento (meta/constraints; sin tablas nuevas).
STORAGE_KINDS = frozenset(
    {
        "pallet_standard",
        "loose_no_pallet",
        "special_restricted",
        "circulation",
        "unusable",
        "permanent_structure",
    }
)

VALIDATION_STATUSES = frozenset({"draft", "pending_validation", "validated"})
REVISION_STATUSES = frozenset({"draft", "published", "archived"})

# Catálogo centralizado — UI Simbología (slice 1.2) y tooltips.
SYMBOLOGY_CATALOG = {
    "nomenclature": {
        "BC": "Bodega Central",
        "BND": "Límite físico de la instalación (p. ej. BC-BND)",
        "BC-BND": "Límite físico de Bodega Central",
        "IZ": "Sector izquierdo",
        "DER": "Sector derecho",
        "CIRC": "Circulación central",
        "P1": "Pasillo 1 (cerca de la oficina)",
        "P2": "Pasillo 2",
        "P3": "Pasillo 3",
        "P4": "Pasillo 4",
        "P5": "Pasillo 5",
        "P6": "Pasillo 6",
        "P7": "Pasillo 7 (fondo, junto a la escalera)",
        "Pn": "Pasillo n (P1…P7)",
        "Pn-mm": "Pasillo n, posición mm (ej. P2-03)",
        "IZ-nn-D": "Sector IZ, posición nn, fila delantera (D)",
        "IZ-nn-A": "Sector IZ, posición nn, fila trasera (A)",
        "D": "En códigos IZ-nn-D: fila delantera (hacia circulación). No confundir con otros usos.",
        "A": "En códigos IZ-nn-A: fila trasera. No confundir con otros usos.",
        "SU": "Productos sueltos (sin pallet). Solo como sufijo/zona, p. ej. IZ-SU.",
        "IZ-SU": "Zona de productos sueltos del sector izquierdo (no son posiciones de pallet).",
        "BAE": "Bajo escalera (almacenamiento especial candidato).",
        "BAE-01": "Bajo escalera, posición candidata 1 (pendiente de validación; no segura por defecto).",
        "BAE-02": "Bajo escalera, posición candidata 2 (pendiente de validación; no segura por defecto).",
        "OFC": "Oficina",
        "ESC": "Escalera (mantener acceso despejado)",
        "PORTON": "Portón / acceso principal",
        "GL": "Galpón de lata (nomenclatura futura, no implementada en layout actual)",
        "CT": "Contenedor (nomenclatura futura)",
        "PAL": "Identificador de pallet (fase futura; no confundir con posiciones ni con SU)",
        "niveles": "Niveles de apilamiento (futuro; independientes de height_clear_m)",
    },
    "nomenclature_context_notes": [
        "D/A solo significan delante/atrás en códigos de fila IZ (IZ-nn-D / IZ-nn-A).",
        "SU significa productos sueltos; no es un código de pasillo ni de pallet.",
        "BAE significa bajo escalera; no reutilizar BAE como prefijo de pasillo.",
        "PAL es identificador futuro de pallet con contenido; no usarlo para posiciones vacías.",
    ],
    "nomenclature_examples": [
        {
            "code": "P2-03",
            "parts": [
                {"token": "P2", "meaning": "Pasillo 2"},
                {"token": "03", "meaning": "Posición 3 dentro del pasillo"},
            ],
        },
        {
            "code": "IZ-08-D",
            "parts": [
                {"token": "IZ", "meaning": "Sector izquierdo"},
                {"token": "08", "meaning": "Posición 8"},
                {"token": "D", "meaning": "Fila delantera"},
            ],
        },
        {
            "code": "IZ-SU",
            "parts": [
                {"token": "IZ", "meaning": "Sector izquierdo"},
                {"token": "SU", "meaning": "Productos sueltos (sin pallet)"},
            ],
        },
        {
            "code": "BAE-01",
            "parts": [
                {"token": "BAE", "meaning": "Bajo escalera"},
                {"token": "01", "meaning": "Candidato 1"},
            ],
        },
    ],
    "symbols": [
        {"type": "facility_boundary", "label_es": "Límite de instalación", "shape": "outline_rect"},
        {"type": "sector", "label_es": "Sector", "shape": "filled_rect"},
        {"type": "position", "label_es": "Posición estándar (pallet)", "shape": "filled_rect_small"},
        {"type": "loose_storage", "label_es": "Almacenamiento sin pallet / sueltos", "shape": "hatched_rect"},
        {"type": "aisle", "label_es": "Pasillo", "shape": "filled_rect"},
        {"type": "wall", "label_es": "Muro", "shape": "thick_line"},
        {"type": "pillar", "label_es": "Pilar", "shape": "square"},
        {"type": "stair", "label_es": "Escalera", "shape": "hatched_rect"},
        {"type": "gate", "label_es": "Portón", "shape": "gap_mark"},
        {"type": "door", "label_es": "Puerta", "shape": "gap_mark"},
        {"type": "office", "label_es": "Oficina", "shape": "filled_rect"},
        {"type": "obstacle", "label_es": "Obstáculo", "shape": "hatched_rect"},
        {"type": "circulation", "label_es": "Circulación", "shape": "filled_rect"},
        {"type": "unusable", "label_es": "Espacio no utilizable", "shape": "hatched_rect"},
        {"type": "beam", "label_es": "Viga", "shape": "thick_line"},
        {"type": "other", "label_es": "Otro / especial", "shape": "outline_rect"},
    ],
    "colors_layout_view": {
        "facility_boundary": {"fill": "rgba(15,23,42,0.02)", "stroke": "#334155", "label_es": "Límite"},
        "sector": {"fill": "rgba(37,99,235,0.06)", "stroke": "#3b82f6", "label_es": "Sectores"},
        "position": {"fill": "rgba(14,165,233,0.22)", "stroke": "#0284c7", "label_es": "Posiciones pallet"},
        "loose_storage": {
            "fill": "rgba(202,138,4,0.18)",
            "stroke": "#a16207",
            "label_es": "Almacenamiento sin pallet (SU)",
        },
        "special_storage": {
            "fill": "rgba(234,88,12,0.16)",
            "stroke": "#c2410c",
            "dash": [4, 3],
            "label_es": "Almacenamiento especial / restringido (p. ej. BAE)",
        },
        "aisle": {"fill": "rgba(99,102,241,0.10)", "stroke": "#6366f1", "label_es": "Pasillos"},
        "circulation": {"fill": "rgba(148,163,184,0.12)", "stroke": "#64748b", "label_es": "Circulación"},
        "structure": {
            "fill": "rgba(100,116,139,0.28)",
            "stroke": "#475569",
            "label_es": "Estructuras (muros, pilares, vigas, obstáculos)",
        },
        "office": {"fill": "rgba(217,119,6,0.16)", "stroke": "#b45309", "label_es": "Oficina"},
        "stair": {"fill": "rgba(126,34,206,0.14)", "stroke": "#7c3aed", "label_es": "Escalera"},
        "gate": {"fill": "rgba(22,163,74,0.28)", "stroke": "#15803d", "label_es": "Portón"},
        "selection": {"stroke": "#ea580c", "strokeWidth": 2.5, "label_es": "Selección activa"},
        "pending_validation": {
            "dash": [6, 4],
            "badge": "#f59e0b",
            "label_es": "Pendiente de validación / restricciones pendientes",
        },
        "extends_outside": {
            "stroke": "#db2777",
            "dash": [2, 3],
            "label_es": "Posiciones fuera del límite nominal del sector",
        },
        "geom_conflict": {
            "stroke": "#dc2626",
            "fill": "rgba(220,38,38,0.14)",
            "label_es": "Conflicto geométrico",
        },
    },
    "colors_future_views": [
        "ocupación",
        "productos / familias",
        "vencimientos",
        "FIFO/FEFO",
    ],
    "colors_note": (
        "Vista actual: Layout físico. No se muestran estados de inventario. "
        "Colores de ocupación, productos, vencimientos y FIFO/FEFO se añadirán "
        "en vistas futuras sin alterar esta leyenda de layout."
    ),
}
