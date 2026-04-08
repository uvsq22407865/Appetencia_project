# src/utils.py
import re
import unicodedata
from typing import Any, Optional, Tuple
from openpyxl.cell.cell import Cell


def norm(s: str) -> str:
    if s is None:
        return ""
    s = str(s).replace("\u00a0", " ").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace("’", "'")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def fill_signature(cell: Cell) -> Optional[Tuple[Any, ...]]:
    """
    Signature robuste d'une couleur Excel (marche pour rgb/theme/indexed).
    Retourne None si pas de fill utile.
    """
    fill = cell.fill
    if fill is None or fill.patternType is None:
        return None

    fg = fill.fgColor
    if fg is None:
        return None

    # on encode toutes les infos utiles
    return (
        fill.patternType,         # "solid" le plus souvent
        fg.type,                  # "rgb" | "theme" | "indexed"
        getattr(fg, "rgb", None),
        getattr(fg, "theme", None),
        getattr(fg, "indexed", None),
        getattr(fg, "tint", None),
    )
