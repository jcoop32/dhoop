# ── Stage 1: builder ──────────────────────────────────────────────────────────
# Full toolchain; FetchContent pulls and compiles all C++ deps here.
FROM debian:bookworm-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        git \
        ca-certificates \
        libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Copy build definition first for better layer caching.
# A changed CMakeLists.txt re-fetches deps; a changed main.cpp only recompiles.
COPY CMakeLists.txt .
COPY main.cpp       .

RUN cmake -B build \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_EXE_LINKER_FLAGS="-static-libgcc -static-libstdc++" \
    && cmake --build build --parallel "$(nproc)"

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
# Only the compiled binary — no compiler, no source, no CMake cache.
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        libssl3 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/build/whoop_worker /usr/local/bin/whoop_worker

# Default environment — overridden by docker-compose
ENV REDIS_URL=redis://redis:6379 \
    REDIS_STREAM=whoop_raw_stream \
    CLICKHOUSE_HOST=clickhouse \
    CLICKHOUSE_PORT=9000

ENTRYPOINT ["/usr/local/bin/whoop_worker"]
