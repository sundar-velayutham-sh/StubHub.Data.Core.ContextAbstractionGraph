"""Token estimation utilities."""
from __future__ import annotations

import json
from typing import Any


def estimate_tokens(data: Any) -> int:
    """Estimate token count. ~4 chars per token for English/JSON text."""
    if isinstance(data, str):
        return len(data) // 4
    return len(json.dumps(data, default=str)) // 4
