#!/usr/bin/env bash
set -euo pipefail

FIGDIR="$(cd "$(dirname "$0")/../paper/figures" && pwd)"
generated=0

for pdf in "$FIGDIR"/*.pdf; do
    [ -f "$pdf" ] || continue
    png="${pdf%.pdf}.png"
    if [ ! -f "$png" ] || [ "$pdf" -nt "$png" ]; then
        if command -v pdftoppm &>/dev/null; then
            pdftoppm -png -r 200 -singlefile "$pdf" "${png%.png}"
            echo "  Generated $(basename "$png") from $(basename "$pdf")"
            generated=$((generated + 1))
        elif command -v python3 &>/dev/null; then
            python3 -c "
from pdf2image import convert_from_path
images = convert_from_path('${pdf}', dpi=200)
images[0].save('${png}')
print(f'  Generated $(basename $png) from $(basename $pdf) via pdf2image')
" 2>/dev/null && generated=$((generated + 1)) || {
                echo "  WARNING: Cannot convert $(basename "$pdf") — install poppler-utils or pdf2image"
            }
        else
            echo "  WARNING: Cannot convert $(basename "$pdf") — no pdftoppm or python3 found"
        fi
    fi
done

if [ "$generated" -gt 0 ]; then
    echo "Generated $generated PNG figure(s)"
else
    echo "All figure PNGs up to date"
fi
