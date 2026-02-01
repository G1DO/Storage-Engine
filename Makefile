.PHONY: build test lint bench clean fmt check

build:
	cargo build

test:
	cargo test

lint:
	cargo clippy -- -W clippy::all

bench:
	cargo bench

clean:
	cargo clean

fmt:
	cargo fmt

check:
	cargo check
