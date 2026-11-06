#!/usr/bin/env bash
# Keep the selected PDI runtime clean; treat JDBC as a separately supplied dependency.
set -euo pipefail
pdi_dir=$1
archive_runtime=/tmp/vertica-it-distribution/data-integration
override=/tmp/vertica-it-driver/vertica-jdbc.jar
fallback=/tmp/vertica-it-bundled-driver
mkdir -p "$fallback"
shopt -s nullglob

is_vertica_driver() {
  # Inspect classes, not just filenames. Avoid grep -q/SIGPIPE under pipefail.
  unzip -Z1 "$1" > /tmp/vertica-it-jar-entries
  grep -Fx 'com/vertica/jdbc/Driver.class' /tmp/vertica-it-jar-entries >/dev/null
}

# Exact distribution ZIPs may omit JDBC although the automation image supplies it.
# Save only this external dependency before replacing the entire base PDI runtime.
for jar_file in "$pdi_dir"/lib/*.jar; do
  if is_vertica_driver "$jar_file"; then cp "$jar_file" "$fallback/"; fi
done
if [[ -f "$archive_runtime/pan.sh" ]]; then
  rm -rf "$pdi_dir"
  cp -a "$archive_runtime" "$pdi_dir"
fi

source_label='selected PDI runtime'
if [[ -f "$override" ]]; then
  if ! is_vertica_driver "$override"; then
    echo 'Configured Vertica JDBC override lacks com.vertica.jdbc.Driver' >&2
    exit 1
  fi
  # Do not load an older bundled driver alongside the explicit override.
  for jar_file in "$pdi_dir"/lib/*.jar; do
    if is_vertica_driver "$jar_file"; then rm -f "$jar_file"; fi
  done
  cp "$override" "$pdi_dir/lib/vertica-jdbc.jar"
  source_label='operator-provided override'
else
  found=false
  for jar_file in "$pdi_dir"/lib/*.jar; do
    if is_vertica_driver "$jar_file"; then found=true; fi
  done
  if [[ "$found" == false ]]; then
    bundled=("$fallback"/*.jar)
    if (( ${#bundled[@]} == 0 )); then
      echo 'No Vertica JDBC driver in selected runtime or base image. Provide integration-test/conf/vertica-jdbc.jar or -Dvertica.it.jdbc-file=/absolute/path.' >&2
      exit 1
    fi
    cp "${bundled[@]}" "$pdi_dir/lib/"
    source_label='automation base image (preserved across exact-runtime replacement)'
  fi
fi

report=/home/devuser/pentaho/vertica-it-driver.txt
printf 'Vertica JDBC source: %s\n' "$source_label" > "$report"
for jar_file in "$pdi_dir"/lib/*.jar; do
  if is_vertica_driver "$jar_file"; then
    # Native COPY must also be present, not merely a JDBC entry point.
    grep -Fx 'com/vertica/jdbc/VerticaCopyStream.class' /tmp/vertica-it-jar-entries >/dev/null
    sha256sum "$jar_file" >> "$report"
  fi
done
cat "$report"
rm -rf "$fallback" /tmp/vertica-it-jar-entries