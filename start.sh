#!/bin/bash

APP_NAME=$(grep name Cargo.toml | cut -d"\"" -f2)
args=()
extra_args=false
for arg in "$@"; do
    if [[ "$arg" == "--" ]]; then
        extra_args=true
        continue
    fi
    if [ "$extra_args" = true ]; then
        args+=("$arg")
    fi
done

read_env() {
  local filePath=".env"

  if [ ! -f "$filePath" ]; then
    echo "missing ${filePath}"
    exit 1
  fi

  while read -r LINE; do
    # Remove leading and trailing whitespaces, and carriage return
    CLEANED_LINE=$(echo "$LINE" | awk '{$1=$1};1' | tr -d '\r')

    if [[ $CLEANED_LINE != '#'* ]] && [[ $CLEANED_LINE == *'='* ]]; then
      export "$CLEANED_LINE"
    fi
  done < "$filePath"
}

read_env

if [ "$1" == "dev" ]; then
    cargo run -- ${args[@]}
elif [ "$1" == "build" ]; then
    cargo build --release
elif [ "$1" == "test" ]; then
    cargo test --release -- --nocapture ${args[@]}
elif [ "$1" == "prod" ]; then
    if [ ! -f ./target/release/$APP_NAME ]; then
        cargo build --release
    fi
    ./target/release/$APP_NAME ${args[@]}
elif [ "$1" == "watch" ]; then
    set -x
    arg="run -- ${args[@]}"
    cargo watch -x "$arg"
else
    cargo build --release
    ./target/release/$APP_NAME ${args[@]}
fi
