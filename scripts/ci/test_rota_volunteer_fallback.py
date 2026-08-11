"""Guard that the CircleCI red-main volunteer fallback stays in sync with the roster.

The Slack red-main notification (`.circleci/config/commands/@slack.yml`) pings a hardcoded
set of "usual suspects" when it cannot select a live rota person, most importantly when the
job runs with no repo checkout and so cannot read the roster file at runtime. Those ids must
mirror the `fallback_pool` in `roster_people.json`, which is the source of truth. This test
fails if the two drift apart, so a change to the pool is not silently forgotten in the config.
"""

import json
import re
from pathlib import Path

import select_rota

# Generated config that actually runs on CircleCI. It is produced from the fragment under
# .circleci/config/ by .circleci/build-config.sh, so checking it also covers a stale rebuild.
CONFIG_PATH = Path(__file__).resolve().parents[2] / ".circleci" / "config.yml"


def _expected_ids_from_roster() -> str:
    roster = json.loads(select_rota.ROSTER_PATH.read_text(encoding="utf-8"))
    id_by_name = roster["slack_id_by_name"]
    return ",".join(id_by_name[name] for name in roster["fallback_pool"])


def _usual_suspects_in_config() -> str:
    text = CONFIG_PATH.read_text(encoding="utf-8")
    match = re.search(r'USUAL_SUSPECTS="([^"]*)"', text)
    assert match is not None, f"USUAL_SUSPECTS not found in {CONFIG_PATH}"
    return match.group(1)


def test_usual_suspects_match_roster_fallback_pool() -> None:
    expected = _expected_ids_from_roster()
    assert _usual_suspects_in_config() == expected, (
        "The USUAL_SUSPECTS fallback in .circleci/config.yml is out of sync with the "
        "fallback_pool in roster_people.json. Update the USUAL_SUSPECTS line in "
        f".circleci/config/commands/@slack.yml to {expected!r} and re-run "
        ".circleci/build-config.sh."
    )
