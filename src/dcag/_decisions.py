"""Decision trace persistence — write/read/search decision traces as JSON files."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class DecisionStore:
    """Persists decision traces as JSON files indexed by entity.

    Storage layout:
        {base_dir}/{entity_name}/{run_id}.json
    """

    def __init__(self, base_dir: Path):
        self._base_dir = Path(base_dir)

    def write(
        self,
        run_id: str,
        workflow_id: str,
        entity: str,
        facts: dict[str, Any],
        confidence: str,
        valid_until: str | None = None,
    ) -> Path:
        """Write a decision trace to disk.

        Returns:
            Path to the written JSON file.
        """
        entity_dir = self._base_dir / entity
        entity_dir.mkdir(parents=True, exist_ok=True)

        trace = {
            "workflow": workflow_id,
            "run_id": run_id,
            "entity": entity,
            "decided_at": datetime.now(timezone.utc).isoformat(),
            "facts": facts,
            "confidence": confidence,
        }
        if valid_until:
            trace["valid_until"] = valid_until

        path = entity_dir / f"{run_id}.json"
        path.write_text(json.dumps(trace, indent=2, default=str))
        return path

    def load(self, entity: str) -> list[dict[str, Any]]:
        """Load all decision traces for an entity.

        Returns:
            List of decision dicts, sorted by decided_at (newest first).
        """
        entity_dir = self._base_dir / entity
        if not entity_dir.exists():
            return []

        decisions = []
        for path in entity_dir.glob("*.json"):
            decisions.append(json.loads(path.read_text()))

        decisions.sort(key=lambda d: d.get("decided_at", ""), reverse=True)
        return decisions

    def search_by_entity(self, entity: str) -> list[dict[str, Any]]:
        """Search for decisions matching an entity. Alias for load()."""
        return self.load(entity)
