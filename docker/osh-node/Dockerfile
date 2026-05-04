# =============================================================================
# osh-node — Runtime image for the OSH node
#
# Pulls the unpacked OSH distribution from the locally-tagged osh-builder
# image (built via `docker compose --profile build build osh-builder`),
# overlays this project's config, and launches.
#
# Rebuild order:
#   1. docker compose --profile build build osh-builder   # only when OSH source changed
#   2. docker compose build osh-node-1                    # quick — just copies + config
# =============================================================================

FROM ocsasim/osh-builder:latest AS dist

FROM eclipse-temurin:21-jre-alpine

LABEL maintainer="cr31.dev@gmail.com"
LABEL description="OpenSensorHub node for OCSASim dev/test"

RUN apk add --no-cache bash \
 && addgroup -S osh && adduser -S osh -G osh

WORKDIR /opt/osh-node

COPY --from=dist /dist/osh-node/ ./
COPY config/ ./config/

RUN chown -R osh:osh /opt/osh-node && chmod +x launch.sh

USER osh

# HTTP API (Connected Systems API)
EXPOSE 8282
# MQTT broker (embedded HiveMQ)
EXPOSE 1883
# Admin web UI
EXPOSE 8181

VOLUME ["/opt/osh-node/db"]

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=5 \
    CMD wget -q --spider http://localhost:8282/sensorhub/api/systems || exit 1

ENTRYPOINT ["./launch.sh"]
