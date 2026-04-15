## Quarto Setup (Headless Ubuntu VPS + Cloudflare Pages)

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

## 3. Render the docs site

From repo root:

```bash
cd docs
quarto render
```

Outputs are written to `docs/_site`.

## 4. Useful checks

```bash
quarto check
quarto render
quarto preview
quarto render whitepaper.qmd --to html
quarto render whitepaper.qmd --to latex
quarto render whitepaper.qmd --to pdf
quarto render whitepaper.qmd --to docx
```

## 5. Cloudflare Pages configuration

Connect this Git repository to Cloudflare Pages and use these build settings:

- Framework preset: `None`
- Build command: `bash docs/scripts/cloudflare-build.sh`
- Build output directory: `docs/_site`

Notes:

- The Quarto project now builds a website with a root `index.html` and `whitepaper.html`.
- `CNAME` is included in the output via `project.resources`.
- `docs/scripts/cloudflare-build.sh` installs a pinned Quarto release in the build workspace before rendering.
- Optional: pin/override version with environment variable `QUARTO_VERSION` in Pages settings.

## 6. Upgrade Quarto later

Use the automatic latest install block again. It will fetch the newest release and upgrade the installed package.

Quick verify command:

```bash
quarto --version
```
