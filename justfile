default:
    just --list

check:
    @echo "Checking code..."
    cargo check --all-targets
    @echo "Code check completed."

test:
    @echo "Running tests..."
    cargo test
    @echo "Tests completed."

lint-toml:
    @echo "Linting TOML files..."
    taplo fmt --check
    @echo "TOML linting completed."

lint-fmt:
    @echo "Checking Rust code formatting..."
    cargo +nightly fmt --check
    @echo "Rust code formatting check completed."

lint-clippy:
    @echo "Running Clippy linter..."
    cargo clippy --all-targets -- -D warnings \
      -D clippy::uninlined-format-args \
      -D clippy::manual-is-multiple-of
    @echo "Clippy linting completed."

lint: lint-toml lint-fmt lint-clippy
