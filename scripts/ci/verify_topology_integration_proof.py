"""Require a passing integration test and fresh evidence from all four running nodes."""

import json
import os
import xml.etree.ElementTree as ET
from pathlib import Path

NODES = {"participant1", "participant2", "sequencer1", "mediator1"}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def verify(
    evidence_path: Path,
    junit_path: Path,
    suite: str,
    expected_mode: str,
    commit: str,
    run_id: str,
    run_attempt: str,
) -> str:
    require(expected_mode in {"enabled", "disabled"}, "An independent expectation is required")
    expected = expected_mode == "enabled"
    evidence = json.loads(evidence_path.read_text())
    for key, value in {
        "suite": suite,
        "commit": commit,
        "run_id": run_id,
        "run_attempt": run_attempt,
        "nightly_marker": str(expected).lower(),
    }.items():
        require(evidence.get(key) == value, f"Wrong or stale proof field: {key}")
    require(evidence.get("expected_validation") is expected, "Wrong proof expectation")
    require(evidence.get("general_checks_enabled") is True, "General checks must remain enabled")
    require(evidence.get("ping_succeeded") is True, "The real ping must succeed")

    cases = [
        case for case in ET.parse(junit_path).iter("testcase") if case.get("classname") == suite
    ]
    require(len(cases) == 1, "The named integration test must actually execute exactly once")
    require(
        all(cases[0].find(status) is None for status in ("skipped", "failure", "error")),
        "The integration test must pass, not skip or fail",
    )
    nodes = evidence.get("nodes", [])
    require(
        len(nodes) == 4 and {node.get("node") for node in nodes} == NODES, "Missing node evidence"
    )
    rows = []
    for node in nodes:
        name = node["node"]
        require(node.get("config_override") is expected, f"{name}: wrong runtime configuration")
        require(node.get("validator") is expected, f"{name}: wrong actual validator")
        require(node.get("lookup_succeeded") is True, f"{name}: topology lookup did not succeed")
        require(
            node.get("storage_class") == "Postgres", f"{name}: real Postgres storage is required"
        )
        allowed = (
            {"ValidatingTopologySnapshot"}
            if expected
            else {"CachingTopologySnapshot", "WriteThroughCacheTopologySnapshot"}
        )
        require(node.get("snapshot_class") in allowed, f"{name}: unexpected or opaque snapshot")
        rows.append(f"| {name} | {node['snapshot_class']} | {str(expected).lower()} | passed |")
    return "\n".join(
        [
            f"Topology integration proof: **{expected_mode}** — ping and JUnit passed.",
            "",
            "| Node | Actual snapshot | Validator | Topology lookup |",
            "| --- | --- | --- | --- |",
            *rows,
            "",
        ]
    )


if __name__ == "__main__":
    summary = verify(
        Path(os.environ["CANTON_TOPOLOGY_PROOF_EVIDENCE"]),
        Path(".ci-test-results/merged/TEST-merged.xml"),
        os.environ["PROOF_SUITE"],
        os.environ["CANTON_TOPOLOGY_VALIDATION_EXPECTED"],
        os.environ["GITHUB_SHA"],
        os.environ["GITHUB_RUN_ID"],
        os.environ["GITHUB_RUN_ATTEMPT"],
    )
    print(summary)
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as output:
        output.write(summary)
