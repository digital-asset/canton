ARG PG_VERSION=17
FROM postgres:${PG_VERSION}

# Re-declare ARG so it's visible inside this build stage
ARG PG_VERSION

# 1. Install dependencies
RUN apt-get update && apt-get install -y \
    postgresql-server-dev-${PG_VERSION} \
    git \
    make \
    gcc \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 2. Clone and Build (Using the correct dash '-' character)
RUN git clone --depth 1 https://github.com/pg-uint/pg-uint128.git /tmp/pg-uint128 \
    && cd /tmp/pg-uint128 \
    && make \
    && make install

# 3. VERIFICATION STEP
# The file created by this repo is 'uint128.control'
RUN ls /usr/share/postgresql/${PG_VERSION}/extension/uint128.control

# 4. Cleanup
RUN rm -rf /tmp/pg_uint128 \
    && apt-get purge -y postgresql-server-dev-${PG_VERSION} git make gcc \
    && apt-get autoremove -y

# 5. Automatically enable the extension on startup
RUN echo "CREATE EXTENSION IF NOT EXISTS uint128;" > /docker-entrypoint-initdb.d/init-uint128.sql