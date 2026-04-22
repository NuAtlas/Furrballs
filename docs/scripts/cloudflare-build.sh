#!/usr/bin/env bash
set -euo pipefail

# Cloudflare Pages does not include Quarto by default.
# Install a pinned Quarto CLI release in the build workspace, then render docs.
QUARTO_VERSION="${QUARTO_VERSION:-1.7.34}"
QUARTO_ARCH="${QUARTO_ARCH:-linux-amd64}"
QUARTO_URL="https://github.com/quarto-dev/quarto-cli/releases/download/v${QUARTO_VERSION}/quarto-${QUARTO_VERSION}-${QUARTO_ARCH}.tar.gz"
QUARTO_ROOT="${PWD}/.quarto-cli"

mkdir -p "${QUARTO_ROOT}"
curl -fsSL "${QUARTO_URL}" -o /tmp/quarto.tar.gz
tar -xzf /tmp/quarto.tar.gz -C "${QUARTO_ROOT}"
export PATH="${QUARTO_ROOT}/quarto-${QUARTO_VERSION}/bin:${PATH}"

cd docs
quarto render --to html
