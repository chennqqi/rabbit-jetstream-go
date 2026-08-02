#!/usr/bin/env sh
set -eu

name="rabbit-jetstream-go-test-$$"
cleanup() {
  docker stop "$name" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker run --rm -d --name "$name" -p 127.0.0.1::4222 nats:2.12.4-alpine -js >/dev/null
port="$(docker port "$name" 4222/tcp | sed 's/.*://')"
attempt=0
until nc -z 127.0.0.1 "$port"; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 40 ]; then
    echo "NATS did not become ready" >&2
    exit 1
  fi
  sleep 0.25
done
NATS_URL="nats://127.0.0.1:$port" go test -tags=integration -count=1 ./...
