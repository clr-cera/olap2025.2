#!/bin/bash
set -e

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(dirname "$SCRIPT_DIR")

# Function to render diagrams
render_diagrams() {
    echo "Rendering diagrams..."

    if ! command -v npx &> /dev/null; then
        echo "npx is not installed. Please install Node.js and npm."
        exit 1
    fi

    SCALE=${1:-2}

    find $SCRIPT_DIR -name "*.mmd" | while read -r file; do
        if [ -f "$file" ]; then
            dir=$(dirname "$file")
            filename=$(basename -- "$file")
            filename_no_ext="${filename%.*}"
            output_file="${dir}/${filename_no_ext}.png"
            
            echo "Rendering $file to $output_file..."
            
            npx @mermaid-js/mermaid-cli -i "$file" -o "$output_file" -t neutral -s "$SCALE"
        fi
    done

    echo "All diagrams have been rendered."
}

# Function to render slides
render_slides() {
    echo "Rendering slides..."
    if ! command -v npx &> /dev/null; then
        echo "npx is not installed. Please install Node.js and npm."
        exit 1
    fi
    
    echo "Rendering slides_1..."
    npx @marp-team/marp-cli "$REPO_ROOT/report/slides_1/slides.md" --pdf --allow-local-files -o "$REPO_ROOT/report/slides_1/slides.pdf" --config-file "$REPO_ROOT/report/.marprc"

    echo "Rendering slides_2..."
    npx @marp-team/marp-cli "$REPO_ROOT/report/slides_2/final_presentation.md" --pdf --allow-local-files -o "$REPO_ROOT/report/slides_2/final_presentation.pdf" --config-file "$REPO_ROOT/report/.marprc"
}

# Function to render document
render_doc() {
    echo "Rendering document..."
    if ! command -v tectonic &> /dev/null; then
        echo "tectonic is not installed. Please install tectonic."
        exit 1
    fi
    
    echo "Building report/doc_1/00-main.tex..."
    (cd "$REPO_ROOT/report/doc_1" && tectonic 00-main.tex)
}

# Main logic
if [ -z "$1" ]; then
    echo "Usage: $0 {diagrams|slides|doc|all}"
    exit 1
fi

case "$1" in
    diagrams)
        render_diagrams
        ;;
    slides)
        render_slides
        ;;
    doc)
        render_doc
        ;;
    all)
        render_diagrams
        render_slides
        render_doc
        ;;
    *)
        echo "Invalid argument: $1"
        echo "Usage: $0 {diagrams|slides|doc|all}"
        exit 1
        ;;
esac
