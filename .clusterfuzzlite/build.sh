#!/bin/bash -eu

cd "$SRC/weaver"

cargo +nightly fuzz build -O --debug-assertions

readonly target_dir="fuzz/target/x86_64-unknown-linux-gnu/release"
for target in nzb_parser yenc_article; do
    cp "$target_dir/$target" "$OUT/$target"
    cp "fuzz/dictionaries/$target.dict" "$OUT/$target.dict"
    cp "fuzz/options/$target.options" "$OUT/$target.options"
    (
        cd "fuzz/seeds/$target"
        python3 -m zipfile -c "$OUT/${target}_seed_corpus.zip" ./*
    )
done
