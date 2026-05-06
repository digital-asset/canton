# test/Dockerfile
# syntax=docker/dockerfile:1.7
FROM fullstorydev/grpcurl:v1.9.2 AS grpc

FROM debian:bookworm-slim
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl jq bash netcat-openbsd file procps \
    && rm -rf /var/lib/apt/lists/*

COPY --from=grpc /bin/grpcurl /usr/local/bin/grpcurl

COPY run.sh /test/run.sh
RUN chmod +x /test/run.sh /usr/local/bin/grpcurl

ENV HOSTPORT="participant:7012" \
    SERVICE="com.digitalasset.canton.admin.participant.v30.SynchronizerConnectivityService" \
    METHOD="ListConnectedSynchronizers" \
    TIMEOUT="60" \
    INTERVAL="1"

CMD ["/bin/bash", "-lc", "/test/run.sh"]
