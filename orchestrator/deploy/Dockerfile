# Code-location image for neo4j_backup_dagster: the Neo4j Enterprise image supplies the JDK and
# neo4j-admin (subprocess runner mode), matching the cluster's version; the code location is
# installed into a venv beside it, with the k8s runner and Postgres instance-storage support.
# Build from the repo root, pinning Dagster to the version your instance runs:
#
#   docker build -f orchestrator/deploy/Dockerfile \
#     --build-arg NEO4J_VERSION=2026.05.0-enterprise --build-arg DAGSTER_VERSION=1.13.30 \
#     -t neo4j-backup-dagster:0.7.0 .
ARG NEO4J_VERSION=2026.05.0-enterprise
FROM neo4j:${NEO4J_VERSION}
ARG DAGSTER_VERSION=""

# neo4j-admin needs the licence accepted, as the server does
ENV NEO4J_ACCEPT_LICENSE_AGREEMENT=yes

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-venv \
 && rm -rf /var/lib/apt/lists/*

COPY orchestrator /opt/neo4j-backup/orchestrator
RUN python3 -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir \
      "/opt/neo4j-backup/orchestrator[k8s,postgres]" \
      ${DAGSTER_VERSION:+"dagster==${DAGSTER_VERSION}"}

ENV PATH=/opt/venv/bin:$PATH
# the Neo4j image's entrypoint starts a server; a code location starts Dagster instead
ENTRYPOINT []
CMD ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", \
     "-m", "neo4j_backup_dagster.definitions"]
