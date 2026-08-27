FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY agent.cpp .
RUN g++ -O2 -std=c++17 -pthread agent.cpp -o agent

FROM debian:bookworm-slim
COPY --from=build /src/agent /usr/local/bin/fsp-cpp-agent
USER 10001:10001
EXPOSE 9400
ENTRYPOINT ["/usr/local/bin/fsp-cpp-agent"]
