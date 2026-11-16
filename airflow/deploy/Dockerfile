# Airflow worker image with the neo4j-backup DAGs. Build from the repo root:
#
#   docker build -f airflow/deploy/Dockerfile --target k8s \
#     --build-arg AIRFLOW_VERSION=3.2.2 --build-arg PYTHON_VERSION=3.12 \
#     -t airflow-neo4j-backup:0.7.0 .
#
# Targets:
#   k8s         RUNNER_MODE=k8s: neo4j-admin runs in pods started from RUNNER_IMAGE (a Neo4j
#               Enterprise image), so the worker needs only Python.
#   subprocess  RUNNER_MODE=subprocess: neo4j-admin runs on the worker; adds the JDK and
#               neo4j-admin from the Neo4j image of your cluster's version.
ARG AIRFLOW_VERSION=3.2.2
ARG PYTHON_VERSION=3.12
ARG NEO4J_VERSION=2026.05.0-enterprise

FROM neo4j:${NEO4J_VERSION} AS neo4j

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION} AS k8s
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION
COPY --chown=airflow:root orchestrator /opt/neo4j-backup/orchestrator
# the package without its Dagster dependency, and the core's own dependencies under Airflow's
# constraints for this image's Python
RUN pip install --no-cache-dir --no-deps /opt/neo4j-backup/orchestrator \
 && pip install --no-cache-dir "neo4j>=5" "boto3>=1.34" "pydantic>=2" "pyyaml>=6" \
      "apache-airflow==${AIRFLOW_VERSION}" "apache-airflow-providers-cncf-kubernetes" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
COPY --chown=airflow:root airflow/dags /opt/airflow/dags

FROM k8s AS subprocess
USER root
COPY --from=neo4j /opt/java/openjdk /opt/java/openjdk
COPY --from=neo4j /var/lib/neo4j /var/lib/neo4j
ENV JAVA_HOME=/opt/java/openjdk NEO4J_HOME=/var/lib/neo4j \
    NEO4J_ACCEPT_LICENSE_AGREEMENT=yes \
    PATH=/var/lib/neo4j/bin:/opt/java/openjdk/bin:$PATH
USER airflow
