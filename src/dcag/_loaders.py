"""YAML loaders for personas, knowledge, and workflows."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from dcag.types import (
    Budget,
    ManifestEntry,
    PersonaBundle,
    StepDef,
    ToolDirective,
    WorkflowDef,
)


class PersonaLoader:
    """Loads persona YAML into PersonaBundle."""

    def __init__(self, personas_dir: Path):
        self._dir = personas_dir

    def load(self, persona_id: str) -> PersonaBundle:
        path = self._dir / f"{persona_id}.yml"
        if not path.exists():
            raise FileNotFoundError(f"Persona '{persona_id}' not found at {path}")
        with open(path) as f:
            data = yaml.safe_load(f)
        p = data["persona"]
        return PersonaBundle(
            id=p["id"],
            name=p["name"],
            description=p["description"].strip(),
            domain_knowledge=p.get("domain_knowledge", []),
            heuristics=p.get("default_heuristics", []),
            anti_patterns=p.get("default_anti_patterns", []),
            quality_standards=p.get("quality_standards", {}),
        )

    def merge(
        self,
        base: PersonaBundle,
        step_heuristics: list[str] | None = None,
        step_anti_patterns: list[str] | None = None,
        step_knowledge: list[str] | None = None,
    ) -> PersonaBundle:
        """Merge step overrides into persona. Step items come FIRST (more specific)."""
        return PersonaBundle(
            id=base.id,
            name=base.name,
            description=base.description,
            domain_knowledge=list(base.domain_knowledge) + (step_knowledge or []),
            heuristics=(step_heuristics or []) + list(base.heuristics),
            anti_patterns=(step_anti_patterns or []) + list(base.anti_patterns),
            quality_standards=base.quality_standards,
        )


class KnowledgeLoader:
    """Loads knowledge YAML files."""

    def __init__(self, knowledge_dir: Path):
        self._dir = knowledge_dir

    def load(self, knowledge_id: str) -> dict[str, Any]:
        path = self._dir / f"{knowledge_id}.yml"
        if not path.exists():
            raise FileNotFoundError(f"Knowledge '{knowledge_id}' not found at {path}")
        with open(path) as f:
            data = yaml.safe_load(f)
        return data.get("knowledge", data)

    def load_multiple(self, ids: list[str]) -> dict[str, dict]:
        return {kid: self.load(kid) for kid in ids}

    def estimate_tokens(self, knowledge: dict) -> int:
        return len(json.dumps(knowledge, default=str)) // 4


class WorkflowLoader:
    """Loads workflow YAML into WorkflowDef."""

    def __init__(self, workflows_dir: Path):
        self._dir = workflows_dir

    def load(self, workflow_id: str) -> WorkflowDef:
        path = self._dir / f"{workflow_id}.yml"
        if not path.exists():
            raise FileNotFoundError(f"Workflow '{workflow_id}' not found at {path}")
        with open(path) as f:
            data = yaml.safe_load(f)
        wf = data["workflow"]
        return WorkflowDef(
            id=wf["id"],
            name=wf["name"],
            persona=wf["persona"],
            inputs=wf.get("inputs", {}),
            steps=[self._parse_step(s) for s in wf["steps"]],
        )

    def load_manifest(self) -> list[ManifestEntry]:
        path = self._dir / "manifest.yml"
        if not path.exists():
            raise FileNotFoundError(f"Manifest not found at {path}")
        with open(path) as f:
            data = yaml.safe_load(f)
        return [
            ManifestEntry(
                id=w["id"],
                name=w["name"],
                persona=w["persona"],
                keywords=w.get("triggers", {}).get("keywords", []),
                input_pattern=w.get("triggers", {}).get("input_pattern"),
            )
            for w in data["workflows"]
        ]

    def _parse_step(self, raw: dict) -> StepDef:
        mode = raw["mode"]
        execute_type = None
        if mode == "execute":
            if "template" in raw:
                execute_type = "template"
            elif "script" in raw:
                execute_type = "script"
            elif "delegate" in raw:
                execute_type = "delegate"

        tools = []
        for t in raw.get("tools", []):
            if isinstance(t, dict):
                tools.append(ToolDirective(
                    name=t["name"],
                    instruction=t.get("instruction", ""),
                    usage_pattern=t.get("usage_pattern"),
                ))
            else:
                tools.append(ToolDirective(name=str(t), instruction=""))

        ctx = raw.get("context", {})
        validation = raw.get("validation", {})
        structural = (
            validation.get("structural", [])
            if isinstance(validation, dict)
            else validation
        )

        budget = None
        if "budget" in raw:
            b = raw["budget"]
            budget = Budget(**{k: v for k, v in b.items()})

        return StepDef(
            id=raw["id"],
            mode=mode,
            execute_type=execute_type,
            template=raw.get("template"),
            script=raw.get("script"),
            delegate=raw.get("delegate"),
            tools=tools,
            instruction=raw.get("instruction"),
            context_static=ctx.get("static", []),
            context_dynamic=ctx.get("dynamic", []),
            context_knowledge=ctx.get("knowledge", []),
            heuristics=raw.get("heuristics", []),
            anti_patterns=raw.get("anti_patterns", []),
            quality_criteria=raw.get("quality_criteria", []),
            output_schema=raw.get("output_schema"),
            validation=structural,
            requires_approval=raw.get("requires_approval", False),
            budget=budget,
            transitions=raw.get("transitions"),
            fallback_on_failure=raw.get("fallback_on_failure"),
            cache_as=raw.get("cache_as"),
            context_cache=ctx.get("cache", []),
            context_decisions=ctx.get("decisions", []),
            loop=raw.get("loop"),
        )
