#!/usr/bin/env bash
# Kafka Connect + Debezium PostgreSQL plugin -- local install
# Energy Commerce and Retail Media Analytics Platform
# Author: Sharique Mohammad
# Date: August 2026
#
# Downloads an Apache Kafka distribution (for the Connect scripts) and the
# Debezium PostgreSQL connector plugin into cdc/plugins/. Idempotent: skips a
# component that is already present. No container runtime.

set -euo pipefail

KAFKA_VERSION="${KAFKA_VERSION:-3.9.0}"
KAFKA_SCALA="${KAFKA_SCALA:-2.13}"
DEBEZIUM_VERSION="${DEBEZIUM_VERSION:-2.7.3.Final}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
KAFKA_HOME="${KAFKA_HOME:-$HOME/kafka}"
PLUGIN_DIR="$REPO_ROOT/cdc/plugins"

mkdir -p "$PLUGIN_DIR" "$REPO_ROOT/cdc/connect/offsets"

if [ ! -x "$KAFKA_HOME/bin/connect-standalone.sh" ]; then
    echo "Installing Apache Kafka $KAFKA_VERSION -> $KAFKA_HOME"
    tmp="$(mktemp -d)"
    curl -fsSL -o "$tmp/kafka.tgz" \
        "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz"
    mkdir -p "$KAFKA_HOME"
    tar -xzf "$tmp/kafka.tgz" -C "$KAFKA_HOME" --strip-components=1
    rm -rf "$tmp"
else
    echo "Kafka already present at $KAFKA_HOME"
fi

if [ ! -d "$PLUGIN_DIR/debezium-connector-postgres" ]; then
    echo "Installing Debezium PostgreSQL connector $DEBEZIUM_VERSION -> $PLUGIN_DIR"
    tmp="$(mktemp -d)"
    curl -fsSL -o "$tmp/debezium.tar.gz" \
        "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz"
    tar -xzf "$tmp/debezium.tar.gz" -C "$PLUGIN_DIR"
    rm -rf "$tmp"
else
    echo "Debezium PostgreSQL plugin already present"
fi

echo
echo "Done. Add to your shell profile / .env:"
echo "  export KAFKA_HOME=$KAFKA_HOME"
