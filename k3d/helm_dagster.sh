#!/usr/bin/env bash
# #247 on k3d: install the real `dagster` Helm chart with orchestrator/deploy/helm-values.yaml,
# the code-location image built from orchestrator/deploy/Dockerfile, and the policy from the
# bucket; then set the `neo4j` pool from the daemon and launch `status_job`, which runs in its
# own run pod (dagster-postgres, includeConfigInLaunchedRuns) against the Compose stack.
#
# Prereqs: `just fresh`, `just k3d-up`, a backup of the demo group (any smoke), and
#   docker build -f orchestrator/deploy/Dockerfile -t neo4j-backup-dagster:k3d .
set -euo pipefail
cd "$(dirname "$0")/.."
CTX="k3d-neo4j-backup"
K="kubectl --context $CTX"
NS=dagster
HOST_IP=$($K get cm coredns -n kube-system -o jsonpath='{.data.NodeHosts}' | awk '/host.k3d.internal/{print $1}')
echo ">> host as seen from pods: $HOST_IP"

k3d image import neo4j-backup-dagster:k3d -c neo4j-backup >/dev/null

# the policy, read by the code location from the bucket (NEO4J_BACKUP_POLICY=s3://...)
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1 \
  orchestrator/.venv/bin/python - <<'EOF'
import boto3
boto3.client("s3", endpoint_url="http://localhost:9000").upload_file(
    "policies/demo.yaml", "neo4j-backups", "_config/policy.yaml")
EOF

$K create namespace $NS --dry-run=client -o yaml | $K apply -f - >/dev/null
$K -n $NS delete secret neo4j-backup-env --ignore-not-found >/dev/null
$K -n $NS create secret generic neo4j-backup-env \
  --from-literal=NEO4J_PASSWORD=devpassword \
  --from-literal=NEO4J_BOLT_URI="neo4j://$HOST_IP:7687" \
  --from-literal=NEO4J_BACKUP_SOURCE="$HOST_IP:6362" \
  --from-literal=BACKUP_BUCKET=neo4j-backups \
  --from-literal=AWS_ACCESS_KEY_ID=minioadmin \
  --from-literal=AWS_SECRET_ACCESS_KEY=minioadmin \
  --from-literal=AWS_ENDPOINT_URL_S3="http://$HOST_IP:9000" >/dev/null

# helm-values.yaml itself, with only the local differences: the local image, a service account
# the chart creates (no IRSA here), no scratch PVC (status_job needs none), the policy in the
# emulator. (Helm replaces lists, so an override file would replace the whole deployment.)
VALS=$(mktemp)
orchestrator/.venv/bin/python - "$VALS" <<'PY'
import sys, yaml
v = yaml.safe_load(open("orchestrator/deploy/helm-values.yaml"))
d = v["dagster-user-deployments"]["deployments"][0]
d["image"].update(repository="neo4j-backup-dagster", tag="k3d", pullPolicy="Never")
d["serviceAccount"] = {"create": True, "name": "neo4j-backup"}
d["resources"]["requests"] = {"cpu": "100m", "memory": "512Mi"}
d["volumes"], d["volumeMounts"] = [], []
env = {e["name"]: e for e in d["env"]}
env["NEO4J_BACKUP_POLICY"]["value"] = "s3://neo4j-backups/_config/policy.yaml"
env["AWS_REGION"]["value"] = "us-east-1"
yaml.safe_dump(v, open(sys.argv[1], "w"))
PY
helm upgrade --install dagster dagster/dagster --version 1.13.30 --kube-context $CTX -n $NS \
  -f "$VALS" \
  --wait --timeout 15m >/dev/null
echo ">> chart installed"
$K -n $NS get pods

DAEMON=$($K -n $NS get deploy -o name | grep daemon)
$K -n $NS exec "$DAEMON" -- dagster instance concurrency set neo4j 4
$K -n $NS exec "$DAEMON" -- dagster instance concurrency get neo4j

WEB=$($K -n $NS get svc -o name | grep webserver)
$K -n $NS port-forward "$WEB" 3070:80 >/dev/null 2>&1 &
PF=$!
trap 'kill $PF 2>/dev/null || true' EXIT
sleep 5
orchestrator/.venv/bin/python - <<'EOF'
import json, time, urllib.request
URL = "http://localhost:3070/graphql"
def gql(q, v=None):
    req = urllib.request.Request(URL, data=json.dumps({"query": q, "variables": v or {}}).encode(),
                                 headers={"Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=60).read())["data"]
repos = gql("{ repositoriesOrError { ... on RepositoryConnection { nodes { name location { name } "
            "schedules { name } sensors { name } } } } }")["repositoriesOrError"]["nodes"]
repo = next(r for r in repos if r["location"]["name"] == "neo4j-backup")
print(">> location loaded:", len(repo["schedules"]), "schedules,",
      sorted(s["name"] for s in repo["sensors"]))
r = gql("""mutation($p: ExecutionParams!) { launchRun(executionParams: $p) {
  __typename ... on LaunchRunSuccess { run { runId } } ... on PythonError { message }
  ... on InvalidSubsetError { message } ... on RunConfigValidationInvalid { errors { message } } } }""",
        {"p": {"selector": {"repositoryLocationName": "neo4j-backup",
                            "repositoryName": repo["name"], "jobName": "status_job"},
               "runConfigData": {}}})["launchRun"]
assert r["__typename"] == "LaunchRunSuccess", r
rid = r["run"]["runId"]
print(">> launched status_job", rid)
for _ in range(120):
    st = gql("query($id: ID!) { runOrError(runId: $id) { ... on Run { status } } }",
             {"id": rid})["runOrError"]["status"]
    if st in ("SUCCESS", "FAILURE", "CANCELED"):
        break
    time.sleep(5)
logs = gql("""query($id: ID!) { logsForRun(runId: $id) { ... on EventConnection { events {
  __typename ... on MessageEvent { message } } } } }""", {"id": rid})["logsForRun"]["events"]
msgs = [e.get("message", "") for e in logs]
print(">> run", st)
for m in msgs:
    if "target" in m.lower() or "stale" in m.lower() or "fresh" in m.lower():
        print("   ", m[:200])
# the demo group was just backed up, so nothing is stale: SUCCESS. A FAILURE whose message
# names stale targets still proves the run pod worked; anything else is a deploy problem.
assert st == "SUCCESS" or any("rpo_minutes" in m for m in msgs), msgs[-15:]
print("PASS: Dagster on the Helm chart (code location, run pod, pool) with helm-values.yaml")
EOF
