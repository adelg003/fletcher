#################
## Build Image ##
#################
FROM rust:alpine as builder

# Setup dependencies
RUN apk add --no-cache \
  musl-dev

# Copy files to build Rust Application
WORKDIR /opt/fletcher
COPY ./Cargo.toml ./Cargo.toml
COPY ./Cargo.lock ./Cargo.lock
COPY ./.sqlx ./.sqlx
COPY ./src ./src

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

# Setup dependencies
RUN apk add --no-cache \
  alpine-conf \
  curl

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
