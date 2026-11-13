FROM rust:latest

WORKDIR /app

# Parallel HTTP/2 requests to crates.io stall on the server's network
ENV CARGO_HTTP_MULTIPLEXING=false

RUN cargo install cargo-watch
COPY Cargo.toml Cargo.lock ./
# Build only the dependencies, then drop the dummy crate's artifacts so the
# real source isn't mistaken for already built
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build \
    && rm -rf src target/debug/insiit-backend-rust* \
       target/debug/deps/insiit_backend_rust-* \
       target/debug/.fingerprint/insiit-backend-rust-*

COPY . .
EXPOSE 3700
CMD ["cargo", "watch", "--exec", "run"]
