#!/usr/bin/env bash
# Build portable packages from an explicitly committed source tree.
set -euo pipefail
cd "$(dirname "$0")/.."
REPO="$PWD"
SOURCE_COMMIT="$(git rev-parse HEAD)"
VERSION="${VERSION:-$(git describe --tags --always --dirty)}"
if [ -n "$(git status --porcelain --untracked-files=normal)" ]; then
  echo '请先提交源码再构建，确保源码包与程序一致。' >&2
  exit 1
fi
OUT="${OUT:-$PWD/dist/$VERSION}"
if [ -e "$OUT" ]; then echo "输出目录已存在，不覆盖：$OUT" >&2; exit 1; fi
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
# Build and copy documentation only from committed files. A private node list,
# config, runtime directory or ignored file in the checkout cannot enter a package.
BUILD_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/ccodex-release.XXXXXX")"
trap 'rm -rf "$BUILD_ROOT"' EXIT
git archive "$SOURCE_COMMIT" | tar -x -C "$BUILD_ROOT"
cd "$BUILD_ROOT"
export CGO_ENABLED=0 COPYFILE_DISABLE=1 GOWORK=off
for target in windows-amd64 windows-arm64 darwin-arm64 darwin-amd64; do
  name="ccodex-sleep-state-$target"
  mkdir -p "$OUT/$name"
  binary=ccodex-sleep-state
  if [[ "$target" == windows-* ]]; then binary="$binary.exe"; fi
  GOOS="${target%-*}" GOARCH="${target#*-}" go build -buildvcs=false -trimpath -ldflags="-s -w -X main.version=$VERSION" -o "$OUT/$name/$binary" ./cmd/ccodex-sleep-state
  cp LICENSE THIRD_PARTY_NOTICES.md README.md CHANGELOG.md "$OUT/$name/"
  printf '%s\n' "$SOURCE_COMMIT" > "$OUT/$name/SOURCE_COMMIT"
  cp -R docs "$OUT/$name/"
  if [[ "$target" == windows-* ]]; then
    cp scripts/start.cmd "$OUT/$name/"
    (cd "$OUT" && zip -qr "$name.zip" "$name")
  else
    cp scripts/start.command "$OUT/$name/"
    chmod +x "$OUT/$name/start.command"
    tar -czf "$OUT/$name.tar.gz" -C "$OUT" "$name"
  fi
done
mkdir "$OUT/source"
git -C "$REPO" archive "$SOURCE_COMMIT" | tar -x -C "$OUT/source"
(cd "$OUT/source" && go mod vendor)
tar -czf "$OUT/ccodex-sleep-state-source.tar.gz" -C "$OUT/source" .
(cd "$OUT" && shasum -a 256 *.zip *.tar.gz > SHA256SUMS && shasum -a 256 -c SHA256SUMS)
printf '构建完成：%s\n' "$OUT"
