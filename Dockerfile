FROM rust:latest

WORKDIR /app

RUN cargo install cargo-watch
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build && rm -rf src

COPY . .
EXPOSE 3700
CMD ["cargo", "watch", "--exec", "run"]
