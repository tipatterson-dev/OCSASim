# =============================================================================
# osh-builder — Gradle build image for the OSH node
#
# Produces an image whose final layer contains the unpacked OSH distribution at
# /dist/osh-node, ready for `COPY --from=ocsasim/osh-builder:latest` in the
# runtime Dockerfile.
#
# Built explicitly on demand (compose profile=build) so the runtime container
# rebuild does not re-run the Java/Gradle build chain. To rebuild from scratch:
#
#   docker compose --profile build build osh-builder
#   docker compose build osh-node-1   # then runtime images use the cached image
#
# Build-time arguments let you swap branches without editing the Dockerfile.
# Defaults pull `master` everywhere except osh-addons, which uses the
# mqtt-csapi-pt3-update branch (the project's reason for a custom build).
# =============================================================================

FROM eclipse-temurin:17-jdk

RUN apt-get update && apt-get install -y --no-install-recommends git unzip \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build

ARG OSH_TEMPLATE_REPO=https://github.com/opensensorhub/osh-node-dev-template.git
ARG OSH_TEMPLATE_BRANCH=master
RUN git clone --depth 1 --branch ${OSH_TEMPLATE_BRANCH} ${OSH_TEMPLATE_REPO} .

ARG OSH_CORE_REPO=https://github.com/opensensorhub/osh-core.git
ARG OSH_CORE_BRANCH=master
ARG OSH_ADDONS_REPO=https://github.com/opensensorhub/osh-addons.git
ARG OSH_ADDONS_BRANCH=mqtt-csapi-pt3-update
RUN git clone --depth 1 --branch ${OSH_CORE_BRANCH}   ${OSH_CORE_REPO}   include/osh-core \
 && git clone --depth 1 --branch ${OSH_ADDONS_BRANCH} ${OSH_ADDONS_REPO} include/osh-addons

# Overlay the project's settings.gradle / build.gradle (wires in MQTT modules).
COPY gradle/settings.gradle settings.gradle
COPY gradle/build.gradle    build.gradle

RUN chmod +x gradlew && ./gradlew build -x test -x osgi

RUN mkdir -p /dist \
 && unzip -q build/distributions/osh-node-*.zip -d /dist \
 && mv /dist/osh-node-* /dist/osh-node

# Image is consumed only via `COPY --from=...`; this CMD is just a friendly
# message if a user runs the image directly by accident.
CMD ["sh", "-c", "echo 'osh-builder: copy /dist/osh-node into a runtime image' && exit 0"]
