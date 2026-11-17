# carnet-server + pouls, built against Debian's gRPC/protobuf/librdkafka.
FROM debian:trixie AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential cmake ninja-build pkg-config \
      libgrpc++-dev protobuf-compiler-grpc libprotobuf-dev protobuf-compiler \
      librdkafka-dev libgtest-dev libssl-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY proto ./proto
COPY cpp ./cpp
RUN cmake -S cpp -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build \
    && ctest --test-dir build --output-on-failure

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      libgrpc++1.51t64 libprotobuf32t64 librdkafka++1 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 10001 carnet && mkdir -p /data && chown carnet /data
COPY --from=build /src/build/carnet-server /src/build/pouls /src/build/carnet-bench /usr/local/bin/
USER 10001
VOLUME /data
ENV CARNET_DIR=/data
