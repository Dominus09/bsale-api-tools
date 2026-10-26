"""Inspección temporal de Excel PRO de referencia (no es job productivo)."""
from __future__ import annotations

from pathlib import Path

from openpyxl import load_workbook
from openpyxl.utils import get_column_letter


def color_hex(c):
    if c is None:
        return None
    rgb = getattr(c, "rgb", None)
    if rgb and isinstance(rgb, str) and rgb not in ("00000000", "0"):
        return rgb
    theme = getattr(c, "theme", None)
    indexed = getattr(c, "indexed", None)
    if theme is not None:
        return f"theme:{theme}"
    if indexed is not None:
        return f"idx:{indexed}"
    return None


def cell_info(ws, r, c):
    cell = ws.cell(r, c)
    fill = color_hex(cell.fill.fgColor) if cell.fill and cell.fill.fill_type else None
    font_color = color_hex(cell.font.color) if cell.font and cell.font.color else None
    return {
        "v": cell.value,
        "fill": fill,
        "font": font_color,
        "bold": cell.font.bold if cell.font else None,
        "size": cell.font.size if cell.font else None,
        "num": cell.number_format,
        "align": cell.alignment.horizontal if cell.alignment else None,
    }


refs = [
    Path(r"c:\Users\user\OneDrive\MIO\Escritorio\La_Quillotana_Excel_Revision_PRO.xlsx"),
    Path(r"c:\Users\user\OneDrive\MIO\Escritorio\La_Quillotana_Auditoria_Completa_PRO.xlsx"),
]

for p in refs:
    print("=" * 80)
    print(p.name, "exists", p.exists(), "size", p.stat().st_size if p.exists() else None)
    wb = load_workbook(p, data_only=False)
    print("sheets:", wb.sheetnames)
    print("merged sample:", list(wb[wb.sheetnames[0]].merged_cells.ranges)[:12])
    for name in wb.sheetnames:
        ws = wb[name]
        print("-" * 60)
        print(
            f"SHEET: {name} max_row={ws.max_row} max_col={ws.max_column} "
            f"freeze={ws.freeze_panes} autofilter={ws.auto_filter.ref}"
        )
        widths = []
        for i in range(1, min(ws.max_column, 30) + 1):
            letter = get_column_letter(i)
            dim = ws.column_dimensions.get(letter)
            widths.append((letter, round(dim.width, 2) if dim and dim.width else None))
        print("widths:", widths)
        print("first 12 rows preview (non-empty):")
        for r in range(1, min(14, ws.max_row) + 1):
            row_vals = []
            for c in range(1, min(ws.max_column, 12) + 1):
                info = cell_info(ws, r, c)
                if info["v"] is not None or info["fill"]:
                    row_vals.append((c, info))
            if row_vals:
                print(f" R{r}:")
                for c, info in row_vals:
                    print(
                        f"   c{c} v={info['v']!r} fill={info['fill']} font={info['font']} "
                        f"bold={info['bold']} size={info['size']} num={info['num']}"
                    )
        dvs = list(ws.data_validations.dataValidation) if ws.data_validations else []
        print("validations", len(dvs))
        for dv in dvs[:8]:
            print(" ", dv.sqref, dv.type, getattr(dv, "formula1", None))
        if ws.max_row >= 20:
            print("row mid sample (first data-ish):")
            for r in (8, 9, 10, 54, 55):
                if r > ws.max_row:
                    continue
                vals = [ws.cell(r, c).value for c in range(1, min(8, ws.max_column) + 1)]
                fills = [
                    color_hex(ws.cell(r, c).fill.fgColor)
                    if ws.cell(r, c).fill and ws.cell(r, c).fill.fill_type
                    else None
                    for c in range(1, min(5, ws.max_column) + 1)
                ]
                print(f"  R{r} vals={vals} fills={fills}")
