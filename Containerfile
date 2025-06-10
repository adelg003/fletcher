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

# Build Rust Application
RUN SQLX_OFFLINE=true \
  cargo build --release --locked


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

# Copy over complied runtime binary
COPY --from=builder \
  /opt/fletcher/target/release/fletcher \
  /usr/local/bin/fletcher

# Setup Healthcheck
HEALTHCHECK CMD curl --fail http://localhost:3000/spec

# Run Fletcher
ENV RUST_BACKTRACE=full
ENTRYPOINT ["fletcher"]
