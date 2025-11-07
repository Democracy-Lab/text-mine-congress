#!/bin/bash

INPUT_DIR="$HOME/Desktop/congress_snippets"
PROMPT="You are a high quality OCR tool. Provide me with a word-for-word transcription of this text. Do not repeat phrases and stop at the final line."

for f in "$INPUT_DIR"/*; do
    extension="${f##*.}"
    base="${f%.*}"

    out_file="${base}.txt"   # <- exact same name, just .txt

    echo "Processing: $f"
    ollama run llama3.2-vision-ocr "$f" "$PROMPT" > "$out_file"
done
