# syntax=docker/dockerfile:1.7
FROM debian:bookworm-slim AS build

RUN apt-get update \
    && apt-get install --yes --no-install-recommends g++ \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY agent-cpp/agent.cpp ./agent.cpp
COPY agent-cpp/tier1 ./tier1
RUN g++ -O2 -std=c++17 -Wall -Wextra -pthread agent.cpp -o /fsp-cpp-agent

FROM debian:bookworm-slim
RUN groupadd --gid 10001 fsp \
    && useradd --uid 10001 --gid fsp --create-home --home-dir /home/fsp fsp \
    && mkdir -p /artifacts /var/lib/fsp \
    && chown -R 10001:10001 /var/lib/fsp
COPY --from=build /fsp-cpp-agent /usr/local/bin/fsp-cpp-agent

USER 10001:10001
EXPOSE 9400
ENTRYPOINT ["/usr/local/bin/fsp-cpp-agent"]
