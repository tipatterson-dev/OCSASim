# =============================================================================
# OpenSensorHub Node — Multi-stage Docker build
# Clones osh-node-dev-template, builds with Gradle, runs on slim JRE.
# =============================================================================

# ── Stage 1: Build ──────────────────────────────────────────────────────────
FROM eclipse-temurin:17-jdk AS builder

RUN apt-get update && apt-get install -y git unzip && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Clone the official OSH node dev template
ARG OSH_TEMPLATE_REPO=https://github.com/opensensorhub/osh-node-dev-template.git
ARG OSH_TEMPLATE_BRANCH=master
RUN git clone --depth 1 --branch ${OSH_TEMPLATE_BRANCH} ${OSH_TEMPLATE_REPO} .

# Clone osh-core and osh-addons into include/ so the gradle files can resolve
# `includeBuild` (osh-core) and the project paths under include/osh-addons/...
ARG OSH_CORE_REPO=https://github.com/opensensorhub/osh-core.git
ARG OSH_CORE_BRANCH=master
ARG OSH_ADDONS_REPO=https://github.com/opensensorhub/osh-addons.git
ARG OSH_ADDONS_BRANCH=mqtt-csapi-pt3-update
RUN git clone --depth 1 --branch ${OSH_CORE_BRANCH}   ${OSH_CORE_REPO}   include/osh-core \
 && git clone --depth 1 --branch ${OSH_ADDONS_BRANCH} ${OSH_ADDONS_REPO} include/osh-addons

# Overlay the vendored gradle files (wire in MQTT + fakeweather modules)
COPY gradle/settings.gradle settings.gradle
COPY gradle/build.gradle    build.gradle

# Build the distribution (skip tests and OSGi bundle)
# Produces build/distributions/osh-node-<version>.zip
RUN chmod +x gradlew && ./gradlew build -x test -x osgi

# Unpack the distribution archive to a stable path
RUN mkdir -p /dist && \
    unzip -q build/distributions/osh-node-*.zip -d /dist && \
    mv /dist/osh-node-* /dist/osh-node

# ── Stage 2: Runtime ────────────────────────────────────────────────────────
FROM eclipse-temurin:21-jre-alpine

LABEL maintainer="cr31.dev@gmail.com"
LABEL description="OpenSensorHub node for OCSASim dev/test"

RUN apk add --no-cache bash \
 && addgroup -S osh && adduser -S osh -G osh

WORKDIR /opt/osh-node

# Copy unpacked distribution from builder
COPY --from=builder /dist/osh-node/ .

# Overlay our custom config (added at compose time via volume or COPY)
COPY config/ ./config/

# Fix permissions
RUN chown -R osh:osh /opt/osh-node && chmod +x launch.sh

USER osh

# HTTP API (Connected Systems API)
EXPOSE 8282
# MQTT broker (embedded HiveMQ)
EXPOSE 1883
# Admin web UI (optional)
EXPOSE 8181

# Persistent data volume
VOLUME ["/opt/osh-node/db"]

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=5 \
    CMD wget -q --spider http://localhost:8282/sensorhub/api/systems || exit 1

ENTRYPOINT ["./launch.sh"]
