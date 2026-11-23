"""Enforce workspace dependency edges using Cargo's metadata graph."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tomllib
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import NotRequired, TypedDict

ROOT = Path(__file__).resolve().parents[1]


class CargoDependency(TypedDict):
    name: str
    rename: str | None
    path: NotRequired[str]
    kind: str | None
    target: str | None


class CargoPackage(TypedDict):
    id: str
    name: str
    manifest_path: str
    dependencies: list[CargoDependency]


class CargoNodeDependency(TypedDict):
    pkg: str


class CargoNode(TypedDict):
    id: str
    deps: list[CargoNodeDependency]


class CargoResolve(TypedDict):
    nodes: list[CargoNode]


class CargoMetadata(TypedDict):
    packages: list[CargoPackage]
    workspace_members: list[str]
    workspace_default_members: list[str]
    resolve: CargoResolve | None


@dataclass(frozen=True)
class DependencyPolicy:
    allowed: dict[str, frozenset[str]]
    pure_roots: frozenset[str]
    forbidden: frozenset[str]

    @classmethod
    def load(cls, path: Path) -> DependencyPolicy:
        with path.open("rb") as source:
            data = tomllib.load(source)
        policy = cls(
            allowed={name: frozenset(deps) for name, deps in data["allowed"].items()},
            pure_roots=frozenset(data["purity"]["roots"]),
            forbidden=frozenset(data["purity"]["forbidden"]),
        )
        for name, deps in policy.allowed.items():
            unknown = deps - policy.allowed.keys()
            if unknown:
                raise ValueError(f"policy {name}: unknown targets {sorted(unknown)}")
        if policy.pure_roots - policy.allowed.keys():
            raise ValueError("purity roots must be listed in the edge policy")
        return policy


def read_metadata(manifest: Path) -> CargoMetadata:
    result = subprocess.run(
        [
            "cargo",
            "metadata",
            "--manifest-path",
            str(manifest.resolve()),
            "--locked",
            "--all-features",
            "--format-version",
            "1",
        ],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=120,
    )
    # Cargo is the JSON boundary; all graph operations below use named metadata fields.
    metadata: CargoMetadata = json.loads(result.stdout)
    return metadata


def cycle_errors(graph: dict[str, set[str]]) -> list[str]:
    active: list[str] = []
    visited: set[str] = set()
    errors: list[str] = []

    def visit(name: str) -> None:
        if name in active:
            cycle = active[active.index(name) :] + [name]
            errors.append(f"workspace cycle: {' -> '.join(cycle)}")
            return
        if name in visited:
            return
        active.append(name)
        for dependency in sorted(graph[name]):
            visit(dependency)
        active.pop()
        visited.add(name)

    for name in sorted(graph):
        visit(name)
    return errors


def check_metadata(metadata: CargoMetadata, policy: DependencyPolicy) -> list[str]:
    packages = {package["id"]: package for package in metadata["packages"]}
    members = [packages[package_id] for package_id in metadata["workspace_members"]]
    names = {package["name"] for package in members}
    errors = [
        f"missing workspace member: {name}"
        for name in sorted(policy.allowed.keys() - names)
    ]
    errors.extend(
        f"unclassified workspace member: {name}"
        for name in sorted(names - policy.allowed.keys())
    )
    if len(names) != len(members):
        errors.append("workspace members must have unique package names")

    paths = {
        Path(package["manifest_path"]).resolve().parent: package["name"]
        for package in members
    }
    graph: dict[str, set[str]] = {name: set() for name in names}
    for package in members:
        source = package["name"]
        for dependency in package["dependencies"]:
            path = dependency.get("path")
            target = paths.get(Path(path).resolve()) if path else None
            context = f"{dependency.get('kind') or 'normal'}, target={dependency.get('target') or 'all'}"
            if path and target is None:
                errors.append(
                    f"{source}: unclassified path dependency {dependency['name']} ({context})"
                )
            if target is not None:
                graph[source].add(target)
                if target not in policy.allowed.get(source, frozenset()):
                    alias = dependency.get("rename") or dependency["name"]
                    errors.append(
                        f"forbidden workspace edge: {source} -> {target} (alias={alias}, {context})"
                    )
            elif dependency["name"] in policy.allowed:
                errors.append(
                    f"{source}: {dependency['name']} must use the workspace path dependency"
                )

    errors.extend(cycle_errors(graph))
    resolve = metadata["resolve"]
    if resolve is None:
        return errors + [
            "resolved dependency graph is required; run metadata without --no-deps"
        ]
    nodes = {node["id"]: node for node in resolve["nodes"]}
    # A missing node must fail closed, rather than silently truncating the purity walk.
    if packages.keys() - nodes.keys():
        errors.append("resolved dependency graph is incomplete")
        return errors

    for root in sorted(policy.pure_roots & names):
        root_id = next(package["id"] for package in members if package["name"] == root)
        pending = deque([(root_id, (root,))])
        visited: set[str] = set()
        while pending:
            package_id, chain = pending.popleft()
            if package_id in visited:
                continue
            visited.add(package_id)
            if packages[package_id]["name"] in policy.forbidden:
                errors.append(f"forbidden core dependency: {' -> '.join(chain)}")
            for dependency in nodes[package_id]["deps"]:
                child = dependency["pkg"]
                pending.append((child, (*chain, packages[child]["name"])))
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest-path", type=Path, default=ROOT / "Cargo.toml"
    )
    parser.add_argument(
        "--policy", type=Path, default=ROOT / "workspace-deps.toml"
    )
    args = parser.parse_args()
    try:
        policy = DependencyPolicy.load(args.policy)
        errors = check_metadata(read_metadata(args.manifest_path), policy)
    except subprocess.CalledProcessError as error:
        print(
            f"workspace dependency check: cargo metadata failed\n{error.stderr}",
            file=sys.stderr,
        )
        return 1
    except (
        OSError,
        ValueError,
        KeyError,
        TypeError,
        subprocess.TimeoutExpired,
    ) as error:
        print(f"workspace dependency check failed: {error}", file=sys.stderr)
        return 1

    if errors:
        print("workspace dependency check failed:", file=sys.stderr)
        for error in errors:
            print(f"  {error}", file=sys.stderr)
        return 1
    print(
        f"workspace dependency check passed: {len(policy.allowed)} members; dependency policy verified"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
