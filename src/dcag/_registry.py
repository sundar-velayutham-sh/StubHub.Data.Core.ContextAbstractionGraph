"""Tool capability registry — formalizes the degradation matrix."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dcag.types import ToolDirective

# Default tool requirements — which capabilities each tool needs
DEFAULT_TOOL_REQUIREMENTS: dict[str, list[str]] = {
    "dbt_mcp.compile": ["dbt_available", "dbt_mcp_available"],
    "dbt_mcp.parse": ["dbt_available", "dbt_mcp_available"],
    "dbt_mcp.test": ["dbt_available", "dbt_mcp_available"],
    "dbt_mcp.show": ["dbt_available", "dbt_mcp_available"],
    "dbt_mcp.get_lineage_dev": ["dbt_available", "dbt_mcp_available"],
    "dbt_mcp.get_node_details_dev": ["dbt_available", "dbt_mcp_available"],
    # Always available — no capability requirements
    "snowflake_mcp.execute_query": [],
    "snowflake_mcp.describe_table": [],
    "snowflake_mcp.list_tables": [],
    "github_cli.read_file": ["github_available"],
    "github_cli.search_code": ["github_available"],
    "github_cli.create_pr": ["github_available"],
}


@dataclass
class ToolRegistry:
    """Runtime tool availability. Populated by step 0 capabilities report.

    Static edges (workflow YAML) define what MAY be used.
    Registry resolves what IS available based on runtime capabilities.
    """
    _capabilities: dict[str, bool] = field(default_factory=dict)
    _tool_requirements: dict[str, list[str]] = field(
        default_factory=lambda: dict(DEFAULT_TOOL_REQUIREMENTS)
    )

    def update_capabilities(self, capabilities: dict[str, Any]) -> None:
        """Update from step 0 output (e.g., {dbt_available: True, dbt_mcp_available: False})."""
        for k, v in capabilities.items():
            self._capabilities[k] = bool(v)

    def resolve_available(self, step_tools: list[ToolDirective]) -> list[ToolDirective]:
        """Filter step's declared tools to only those currently available."""
        return [t for t in step_tools if self._is_available(t.name)]

    def _is_available(self, tool_name: str) -> bool:
        reqs = self._tool_requirements.get(tool_name, [])
        return all(self._capabilities.get(r, True) for r in reqs)

    def get_resolution_report(self, step_tools: list[ToolDirective]) -> dict:
        """Report what was requested vs what's available (for observability)."""
        requested = [t.name for t in step_tools]
        available = [t.name for t in self.resolve_available(step_tools)]
        filtered = [t for t in requested if t not in available]
        return {
            "requested": requested,
            "available": available,
            "filtered_out": filtered,
            "capabilities": dict(self._capabilities),
        }
