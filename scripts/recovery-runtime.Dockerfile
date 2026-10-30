# Explicit Phase 31 restore-only runtime, not an application deployment image.
# The build context is the bounded, generated .local/phase31/runtime directory.
FROM eclipse-temurin:21-jdk-jammy@sha256:4e61d0b3471137e239397789661f6adeaf8db9adcfae45916b22f628c8a32dc0 AS java
FROM python:3.13.9-slim-bookworm@sha256:b685a4fa58bb19d1814d78a1ec0f0208f351452724f78b20212c984d6e124a34
COPY --from=java /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="/opt/java/openjdk/bin:${PATH}"
ENV PYTHONDONTWRITEBYTECODE=1 MLFLOW_DISABLE_AGENT_HINT=1
COPY requirements.txt /runtime/requirements.txt
RUN pip install --no-cache-dir --require-hashes -r /runtime/requirements.txt
# Only code and already-built pinned JVM classes/jars, never source .local data.
ARG REPOSITORY_PATH
COPY python ${REPOSITORY_PATH}/model-control-plane/src
COPY jvm /runtime/jvm
ENV PYTHONPATH=${REPOSITORY_PATH}/model-control-plane/src
WORKDIR /runtime
ENTRYPOINT ["python", "-B", "-m", "sentinelaegisforge_control_plane.recovery.cli"]
