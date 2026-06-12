#!/bin/sh

# Derived from:
# rustc --print cfg --target x86_64-unknown-linux-musl -C target-cpu=haswell
WEAVER_X86_64_REQUIRED_FEATURES="avx avx2 bmi1 bmi2 cmpxchg16b f16c fma fxsr lzcnt movbe pclmulqdq popcnt rdrand sse sse2 sse3 sse4.1 sse4.2 ssse3 xsave xsaveopt"
WEAVER_ARM64_REQUIRED_FEATURES="aes crc32 dotprod fp16 lse neon rdm sha2"

to_lower() {
    printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

normalize_x86_feature() {
    case "$(to_lower "$1")" in
        avx | avx1.0) printf '%s\n' "avx" ;;
        avx2 | avx2.0) printf '%s\n' "avx2" ;;
        bmi1) printf '%s\n' "bmi1" ;;
        bmi2) printf '%s\n' "bmi2" ;;
        cx16 | cmpxchg16b) printf '%s\n' "cmpxchg16b" ;;
        f16c) printf '%s\n' "f16c" ;;
        fma) printf '%s\n' "fma" ;;
        fxsr) printf '%s\n' "fxsr" ;;
        abm | lzcnt) printf '%s\n' "lzcnt" ;;
        movbe) printf '%s\n' "movbe" ;;
        pclmul | pclmulqdq) printf '%s\n' "pclmulqdq" ;;
        popcnt) printf '%s\n' "popcnt" ;;
        rdrand) printf '%s\n' "rdrand" ;;
        sse) printf '%s\n' "sse" ;;
        sse2) printf '%s\n' "sse2" ;;
        pni | sse3) printf '%s\n' "sse3" ;;
        sse4_1 | sse4.1) printf '%s\n' "sse4.1" ;;
        sse4_2 | sse4.2) printf '%s\n' "sse4.2" ;;
        ssse3) printf '%s\n' "ssse3" ;;
        osxsave | xsave) printf '%s\n' "xsave" ;;
        xsaveopt) printf '%s\n' "xsaveopt" ;;
    esac
}

normalize_arm64_feature() {
    case "$(to_lower "$1")" in
        aes) printf '%s\n' "aes" ;;
        crc | crc32) printf '%s\n' "crc32" ;;
        asimd | neon) printf '%s\n' "neon" ;;
        fphp | asimdhp | fp16) printf '%s\n' "fp16" ;;
        atomics | lse) printf '%s\n' "lse" ;;
        asimdrdm | rdm) printf '%s\n' "rdm" ;;
        asimddp | dotprod) printf '%s\n' "dotprod" ;;
        sha2) printf '%s\n' "sha2" ;;
    esac
}

normalized_features_from_cpuinfo() {
    cpuinfo_path=$1
    arch=$2

    [ -r "$cpuinfo_path" ] || return 1

    awk -F: '/^(flags|Features)[[:space:]]*:/{print $2}' "$cpuinfo_path" \
        | tr ' ' '\n' \
        | while IFS= read -r token; do
            token=$(printf '%s' "$token" | tr -d '\r\t ')
            [ -n "$token" ] || continue
            case "$arch" in
                amd64) normalize_x86_feature "$token" ;;
                arm64) normalize_arm64_feature "$token" ;;
            esac
        done \
        | awk 'NF { seen[$0] = 1 } END { for (feature in seen) print feature }'
}

feature_set_has_all() {
    features=$1
    shift

    for required in "$@"; do
        printf '%s\n' "$features" | grep -Fx "$required" >/dev/null 2>&1 || return 1
    done
    return 0
}

select_linux_lane_for_arch() {
    arch=$1
    cpuinfo_path=${2:-/proc/cpuinfo}

    features=$(normalized_features_from_cpuinfo "$cpuinfo_path" "$arch") || {
        printf '%s\n' "portable"
        return 0
    }

    [ -n "$features" ] || {
        printf '%s\n' "portable"
        return 0
    }

    case "$arch" in
        amd64)
            if feature_set_has_all "$features" $WEAVER_X86_64_REQUIRED_FEATURES; then
                printf '%s\n' "haswell"
            else
                printf '%s\n' "portable"
            fi
            ;;
        arm64)
            if feature_set_has_all "$features" $WEAVER_ARM64_REQUIRED_FEATURES; then
                printf '%s\n' "cortex-a76"
            else
                printf '%s\n' "portable"
            fi
            ;;
        *)
            printf '%s\n' "portable"
            ;;
    esac
}

selected_payload_name() {
    arch=$1
    cpuinfo_path=${2:-/proc/cpuinfo}

    case "$(select_linux_lane_for_arch "$arch" "$cpuinfo_path")" in
        haswell) printf '%s\n' "weaver-haswell" ;;
        cortex-a76) printf '%s\n' "weaver-cortex-a76" ;;
        *) printf '%s\n' "weaver-portable" ;;
    esac
}

optimized_payload_name() {
    arch=$1

    case "$arch" in
        amd64) printf '%s\n' "weaver-haswell" ;;
        arm64) printf '%s\n' "weaver-cortex-a76" ;;
    esac
}

portable_payload_name() {
    printf '%s\n' "weaver-portable"
}

detect_container_arch() {
    if [ -n "${WEAVER_CONTAINER_ARCH:-}" ]; then
        printf '%s\n' "$WEAVER_CONTAINER_ARCH"
        return 0
    fi

    case "$(uname -m)" in
        x86_64 | amd64) printf '%s\n' "amd64" ;;
        aarch64 | arm64) printf '%s\n' "arm64" ;;
        *) printf '%s\n' "unknown" ;;
    esac
}
