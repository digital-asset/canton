#!/usr/bin/env bash
# Copyright (c) 2026, Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.

### TEMPLATED MANAGED FILE, DO NOT EDIT
## If you need to modify this file, please edit relevant file in https://github.com/DACH-NY/grafana-tools

set -eu -o pipefail

# Source: https://andidog.de/blog/2022-04-21-grafana-dashboards-best-practices-dashboards-as-code#_fast_develop_deploy_view_cycle
# Usage: ./watch.sh dashboard.jsonnet

error() {
	>&2 echo "ERROR:" "${@}"
	exit 1
}

[ -n "${GRAFANA_API_KEY:-}" ] || error "Invalid GRAFANA_API_KEY (create one with Editor permissions at: Administration > Users and access > Service accounts)"
[[ "${GRAFANA_URL:-}" =~ ^https?://[^/]+/$ ]] || error "Invalid GRAFANA_URL (example: 'http://localhost:3000/' incl. slash at end)"

[ $# = 1 ] || error "Usage: $(basename "${0}") <JSONNET_FILE_OF_DASHBOARD>"
dashboard_jsonnet_file="${1}"

rendered_json_file="/tmp/$(basename "${dashboard_jsonnet_file%.jsonnet}").rendered.json"

cat >/tmp/render-and-upload-dashboard.sh <<-EOF
	#!/usr/bin/env bash
	set -euo pipefail
	clear

	# Render
	echo "Will render to \${2}"
	JSONNET_PATH="\$(realpath vendor)"
	export JSONNET_PATH
	jsonnet-lint "\${1}"
	jsonnet -o "\${2}" "\${1}" --ext-str grafonnet_version=latest

	# Enable editable flag and upload via Grafana API
	cat "\${2}" \
		| jq '{"dashboard":.,"folderId":0,"overwrite":true} | .dashboard.editable = true' \
		| curl \
			--fail-with-body \
			-sS \
			-X POST \
			-H "Authorization: Bearer \${GRAFANA_API_KEY}" \
			-H "Content-Type: application/json" \
			--data-binary @- "${GRAFANA_URL}api/dashboards/db" \
		&& printf '\nDashboard uploaded at: %s\n' "$(date)" \
		|| { >&2 printf '\nERROR: Failed to upload dashboard\n'; exit 1; }
EOF
chmod +x /tmp/render-and-upload-dashboard.sh

echo "${dashboard_jsonnet_file}" | entr /tmp/render-and-upload-dashboard.sh /_ "${rendered_json_file}"
