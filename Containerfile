#################
## Build Image ##
#################
FROM rust:alpine as builder

# Update system packages and install setup dependencies
RUN apk update --no-cache && \
    apk add --no-cache \
        musl-dev \
        npm

# Copy files to build Rust Application
WORKDIR /opt/fletcher
COPY ./build.rs ./build.rs
COPY ./Cargo.toml ./Cargo.toml
COPY ./Cargo.lock ./Cargo.lock
COPY ./images ./images
COPY ./migrations ./migrations
COPY ./package.json ./package.json
COPY ./package-lock.json ./package-lock.json
COPY ./.sqlx ./.sqlx
COPY ./src ./src
COPY ./tailwind.css ./tailwind.css

# Copy files for workspace sub packages
COPY ./key_hasher/Cargo.toml ./key_hasher/Cargo.toml
COPY ./key_hasher/src ./key_hasher/src

# Accept compile mode as an argument (default: release)
ARG BUILD_MODE=release

# Build Rust Application
ENV SQLX_OFFLINE=true
RUN if [ "$BUILD_MODE" = "release" ]; then \
      cargo build --locked --release; \
    else \
      cargo build --locked; \
    fi


###################
## Runtime Image ##
###################

FROM alpine:3

# Update system packages and install setup dependencies
RUN apk update --no-cache && \
    apk upgrade --quiet && \
    apk add --no-cache \
        alpine-conf \
        curl && \
    rm -rf /var/cache/apk/*

# Setup Fletcher user
RUN setup-user fletcher
USER fletcher
WORKDIR /home/fletcher

# Accept compile mode as an argument (default: release)
ARG BUILD_MODE=release

# Copy over complied runtime binary
COPY --from=builder \
    /opt/fletcher/target/${BUILD_MODE}/fletcher \
    /usr/local/bin/fletcher

# Setup Healthcheck
HEALTHCHECK CMD curl --fail http://localhost:3000/spec

# Run Fletcher
ENV RUST_BACKTRACE=full
ENTRYPOINT ["fletcher"]
