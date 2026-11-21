#!/usr/bin/env bash
set -euo pipefail

fail() { printf 'Error: %s\n' "$*" >&2; exit 1; }
usage() { echo 'Usage: resolve-pdi-build.sh [--build A.B.C.D-N] [--series A.B[.C.D]] [--output-dir DIRECTORY]'; }

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
output_dir="$script_dir/pentaho-platform/target/runtime"
build=''
series='11.0.0.4'
while (( $# )); do
  case "$1" in
    --build|--series|--output-dir)
      (( $# >= 2 )) || fail "Missing value for $1"
      case "$1" in
        --build) build=$2 ;;
        --series) series=$2 ;;
        --output-dir) output_dir=$2 ;;
      esac
      shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) fail "Unknown argument: $1" ;;
  esac
done
[[ "$series" =~ ^[0-9]+\.[0-9]+(\.[0-9]+\.[0-9]+)?$ ]] || fail 'Series must match A.B or A.B.C.D'
[[ -z "$build" || "$build" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+-[0-9]+$ ]] || fail 'Build must be empty or match A.B.C.D-N'
if [[ -n "$build" ]]; then series=${build%-*}; fi
[[ -n "$output_dir" && "$output_dir" != *$'\n'* && "$output_dir" != *$'\r'* ]] || fail 'Invalid output directory'
for tool in curl jq; do command -v "$tool" >/dev/null || fail "$tool is required"; done
if command -v sha256sum >/dev/null && command -v sha1sum >/dev/null; then
  hash_file() { if [[ $1 == SHA256 ]]; then sha256sum "$2"; else sha1sum "$2"; fi | awk '{print $1}'; }
elif command -v shasum >/dev/null; then
  hash_file() { if [[ $1 == SHA256 ]]; then shasum -a 256 "$2"; else shasum -a 1 "$2"; fi | awk '{print $1}'; }
else
  fail 'sha256sum and sha1sum, or shasum, are required'
fi

mkdir -p -- "$output_dir"
output_dir=$(cd -- "$output_dir" && pwd -P)
catalog_url='https://build.eng.pentaho.com/fileMapping.json'
# The catalog redirects over HTTPS; its contents are untrusted and checked below.
catalog=$(curl -q -fsSL --proto '=https' --proto-redir '=https' "$catalog_url") || fail 'Unable to fetch the build catalog'
entry=$(jq -ce --arg requested "$build" --arg series "$series" '
  [.folders[]?.groups[]?.files | objects | to_entries[]
   | select(.key | test("^pdi-ee-client-[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+-[0-9]+\\.zip$"))
   | . + {build: (.key | ltrimstr("pdi-ee-client-") | rtrimstr(".zip"))}
  | . + {release: (.build | split("-")[0])}
  | select(.release == $series or (.release | startswith($series + ".")))
   | select($requested == "" or .build == $requested)
  | . + {number: (.build | split("-")[1] | sub("^0+"; "") | if . == "" then "0" else . end)}]
  | if length == 0 then error("No matching build") else . end
  | sort_by([(.release | split(".") | map(tonumber)), (.number | length), .number, .build])
  | .[-1].build as $selected
  | map(select(.build == $selected))
  | unique_by([.build, .value.url, .value.checksum])
  | if length != 1 then error("Ambiguous build entries") else .[0] end
  | {build, url: .value.url, checksum: .value.checksum}
' <<< "$catalog") || fail 'No unique matching build in the catalog'
build=$(jq -er '.build | strings' <<< "$entry") || fail 'Invalid catalog build'
url=$(jq -er '.url | strings' <<< "$entry") || fail 'Invalid catalog URL'
checksum=$(jq -er '.checksum | strings' <<< "$entry") || fail 'Missing catalog checksum'
[[ "$build" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+-[0-9]+$ ]] || fail 'Invalid catalog build'
archive="$output_dir/pdi-ee-client-$build.zip"
expected_url="https://repo-cache.eng.pentaho.com/artifactory/pnt-mvn/com/pentaho/di/pdi-ee-client/$build/pdi-ee-client-$build.zip"
[[ "$url" == "$expected_url" ]] || fail 'Catalog archive URL is not the trusted canonical URL'
if [[ "$checksum" =~ ^SHA1=([[:xdigit:]]{40})$ ]]; then
  algorithm=SHA1
elif [[ "$checksum" =~ ^SHA256=([[:xdigit:]]{64})$ ]]; then
  algorithm=SHA256
else
  fail 'Catalog checksum must be SHA1 or SHA256 with a valid hexadecimal digest'
fi
expected_digest=$(printf '%s' "${BASH_REMATCH[1]}" | tr '[:upper:]' '[:lower:]')
part=''
cleanup() { if [[ -n "$part" ]]; then rm -f -- "$part"; fi; }
trap cleanup EXIT
if [[ -f "$archive" ]] && [[ "$(hash_file "$algorithm" "$archive")" == "$expected_digest" ]]; then
  echo "Using verified cached archive: $archive" >&2
else
  echo "Downloading $build from trusted repository" >&2
  part=$(mktemp "$archive.part.XXXXXX")
  # Artifactory may return a signed CloudFront URL. Validate the destination
  # before fetching it and never print the signed query string or send credentials.
  response=$(curl -q -fsS --proto '=https' -o "$part" -w '%{http_code}\n%{redirect_url}' "$expected_url") || fail 'Archive download failed'
  http_code=${response%%$'\n'*}
  redirect=${response#*$'\n'}
  if [[ "$http_code" == 302 ]]; then
    [[ "$redirect" =~ ^https://d23wq0ijrm3vi6\.cloudfront\.net/filestore/ && "$redirect" != *$'\n'* && "$redirect" != *$'\r'* ]] || fail 'Untrusted archive redirect'
    http_code=$(curl -q -fsS --proto '=https' -o "$part" -w '%{http_code}' "$redirect") || fail 'Archive download failed'
  fi
  [[ "$http_code" == 200 ]] || fail 'Archive server returned an unexpected HTTP status'
  [[ "$(hash_file "$algorithm" "$part")" == "$expected_digest" ]] || fail 'Downloaded archive checksum mismatch'
  mv -f -- "$part" "$archive"
  part=''
fi
sha256=$(hash_file SHA256 "$archive")
metadata="$output_dir/pdi-build-provenance.txt"
part=$(mktemp "$metadata.part.XXXXXX")
printf 'build=%s\ncatalog=%s\nsource=%s\npublished_checksum=%s\nsha256=%s\n' \
  "$build" "$catalog_url" "$expected_url" "$checksum" "$sha256" > "$part"
mv -f -- "$part" "$metadata"
part=''
printf 'build=%s\narchive=%s\nsha256=%s\n' "$build" "$archive" "$sha256"
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  printf 'build=%s\narchive=%s\nsha256=%s\n' "$build" "$archive" "$sha256" >> "$GITHUB_OUTPUT"
fi