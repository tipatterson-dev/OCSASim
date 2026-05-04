# =============================================================================
# OCSASim sim runner — Python + uv runtime
#
# Builds a self-contained image of OCSASim's `--mode sims` runner. oshconnect
# is fetched from test.pypi.org per pyproject.toml's [tool.uv.sources] block.
# The default CMD points at the dockerized config that targets the osh-node-1
# compose service by name; mount your own at /config/sim_config.json to override.
# =============================================================================

FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app

# uv needs UV_LINK_MODE=copy to avoid hardlink warnings when /root and /app
# straddle different mount points inside the build container.
ENV UV_LINK_MODE=copy \
    UV_NO_PROGRESS=1 \
    PYTHONUNBUFFERED=1

# Install deps first so subsequent code edits don't bust this layer.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project

# Now ship the project itself. `web/` is intentionally omitted — the sim
# runner doesn't import it, and skipping it keeps the layer lean.
COPY main.py ./main.py
COPY sims/  ./sims/
COPY examples/ ./examples/

# `uv sync` again to pick up the project-as-package install. With deps cached
# from the prior layer this is fast.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen

# A non-root user mirrors the osh-node container's posture.
RUN useradd --create-home --shell /bin/sh sim \
 && chown -R sim:sim /app
USER sim

ENTRYPOINT ["uv", "run", "--frozen", "python", "main.py"]
CMD ["--mode", "sims", "--config", "/config/sim_config.json"]
