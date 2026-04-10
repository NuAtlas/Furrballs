*# Quarto Setup (Headless Ubuntu VPS)

This project treats [whitepaper.qmd](whitepaper.qmd) as the source of truth.

## 1. Install Quarto (Ubuntu/Debian, no GUI)

Automatic latest install (recommended):

```bash
set -euo pipefail

ARCH="amd64"
LATEST_TAG="$(curl -fsSLI -o /dev/null -w '%{url_effective}' https://github.com/quarto-dev/quarto-cli/releases/latest | sed 's#.*/tag/v##')"
DEB="quarto-${LATEST_TAG}-linux-${ARCH}.deb"
URL="https://github.com/quarto-dev/quarto-cli/releases/download/v${LATEST_TAG}/${DEB}"

wget -O "/tmp/${DEB}" "$URL"
sudo apt update
sudo apt install -y "/tmp/${DEB}"
quarto --version
```

Pinned install (optional for reproducibility):

```bash
set -euo pipefail

# choose a version from https://github.com/quarto-dev/quarto-cli/releases
QUARTO_VERSION="1.7.34"
DEB="quarto-${QUARTO_VERSION}-linux-amd64.deb"
URL="https://github.com/quarto-dev/quarto-cli/releases/download/v${QUARTO_VERSION}/${DEB}"

wget -O "/tmp/${DEB}" "$URL"
sudo apt update
sudo apt install -y "/tmp/${DEB}"
quarto --version
```

If `sudo` is unavailable, ask your VPS provider/admin to install the same `.deb` globally.

## 2. Optional: PDF engine for `format: pdf`

Quarto needs a LaTeX engine for PDF output.

Small footprint option:

```bash
sudo apt install -y texlive-latex-recommended texlive-latex-extra texlive-fonts-recommended
```

## 3. Render the white paper

From repo root:

```bash
cd docs
quarto render whitepaper.qmd
```

Outputs are written to `docs/_output`.

## 4. Useful checks

```bash
quarto check
quarto render whitepaper.qmd --to html
quarto render whitepaper.qmd --to latex
quarto render whitepaper.qmd --to pdf
quarto render whitepaper.qmd --to docx
```

## 5. Upgrade Quarto later

Use the automatic latest install block again. It will fetch the newest release and upgrade the installed package.

Quick verify command:

```bash
quarto --version
```
