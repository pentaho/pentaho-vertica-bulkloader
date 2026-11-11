# SP-7583 Vertica Bulk Loader integration tests

## Overview

The standalone [integration-test Maven reactor](integration-test/pom.xml) tests
real Pan/Kitchen, native Vertica COPY and independent `vsql` database checks.
Fabric8 starts disposable PDI and Vertica containers; JUnit 5 tests run through
Maven Failsafe. The suites cover failure propagation, normal EOF, concurrent
copies, slow input, transformation Abort and pooled connection recovery.
See the [QA summary](integration-test/SP-7583-QA-summary.md) for measured results
and outstanding acceptance coverage.

The default PDI base is `repo.pentaho.com/pnt-docker/automation/pdi-client:10.2.0.11`;
the database is `repo.pentaho.com/docker/vertica-ce:24.1.0-0`. Tests create their
own user and tables on a private Docker network. Never point them at a shared
database. The default plugin source is `bundled`; `local` replaces the plugin in
the selected distribution with the assembly built from this checkout.

## Prerequisites

- Java 11 and Maven on the host; Bash, `curl`, `jq` and SHA-1/SHA-256 utilities.
- Docker CLI and daemon capable of `linux/amd64` builds and containers; Apple
  Silicon requires amd64 emulation. Allow sufficient memory and disk.
- Access to the internal Maven and Docker registries, build catalog, archive
  repository and license service, with the appropriate credentials configured.
- An approved local ELM service configuration at `integration-test/conf/.elmLicInfo.plt`
  or selected using `-Dvertica.it.license-file=/absolute/path`. This file is
  ignored by Git; do not print or upload it. A valid entitlement and service
  connectivity are required.
- The supplied JDBC 6 JAR in [integration-test/conf](integration-test/conf/README.md)
  is the default. Neither the driver nor the license configuration is downloaded.

## Run locally

Run from the repository root. The resolver verifies the catalog checksum, checks
the trusted archive URL and records SHA-256 provenance. Exact builds are cached
in `integration-test/pentaho-platform/target/runtime`.

```bash
resolution=$(bash integration-test/resolve-pdi-build.sh --build 10.2.0.11-453)
build=$(printf '%s\n' "$resolution" | sed -n 's/^build=//p')
archive=$(printf '%s\n' "$resolution" | sed -n 's/^archive=//p')

# Test the plugin bundled with the exact distribution.
mvn --batch-mode --no-transfer-progress -f integration-test/pom.xml verify \
  -DrunIntegrationTests -Dvertica.it.source=bundled \
  -Dvertica.it.pdi-build="$build" -Dvertica.it.pdi-archive="$archive" \
  -Dvertica.it.run-id=sp7583-bundled -Dvertica.it.test-md5-auth=true

# Build and test the 10.2 plugin from this checkout.
mvn --batch-mode --no-transfer-progress install
mvn --batch-mode --no-transfer-progress -f integration-test/pom.xml verify \
  -DrunIntegrationTests -Dvertica.it.source=local \
  -Dvertica.it.pdi-build="$build" -Dvertica.it.pdi-archive="$archive" \
  -Dvertica.it.run-id=sp7583-local -Dvertica.it.test-md5-auth=true
```

Both examples use JDBC `06.00.0000` and enable MD5 only for the disposable
`qa_loader` user because that driver cannot authenticate against the test
server's default SHA-512. MD5 is weak and must not be used on shared or
production databases. Maven's MD5 option defaults to `false`.

For a different existing driver, add `-Dvertica.it.jdbc-file=/absolute/path/to/driver.jar`.
To exercise the selected distribution's driver, or the image driver when the
distribution has none, set `vertica.it.jdbc-file` to a nonexistent path and
disable MD5. This intentionally invokes the fallback; verify the recorded driver
source and checksum. The build 453 archive has no Vertica driver in its `lib`
directory, so the tested image supplies JDBC `9.2.1-0`. Its results and limitations
are documented separately in the QA summary.

Use a unique lowercase Docker-safe run ID and separate evidence directories for
each run, for example `-Dvertica.it.evidence-dir=$PWD/integration-test/pentaho-platform/target/evidence/my-run`.
Copy `target/failsafe-reports` into that evidence directory before the next run;
the reports and default top-level evidence labels are otherwise overwritten.
Concurrent runs need separate checkouts because staging and reports are shared
within a checkout. Add `-Dit.test=VerticaBulkLoaderLifecycleIT` to run only the
lifecycle suite. Never interpret a timeout or forced termination as a pass.

Without `vertica.it.pdi-archive`, Maven uses the image runtime. An exact archive
must contain `data-integration/pan.sh` and replaces the whole image runtime.
`vertica.it.pdi-build` is a provenance label, not a verifier. For local plugin
mode, build exactly one assembly ZIP or specify `vertica.it.plugin-zip` explicitly.
The resolver also accepts `--series 10.2.0.11` to select the latest catalog build;
use `--build` when a specific build is required.

## CI

The [GitHub workflow](.github/workflows/integration-tests.yml) runs on pushes and
same-repository pull requests to `10.2`, and supports manual dispatch and reusable
workflow calls. Fork pull requests do not run the credentialed job. Configure
independent required reviewers for the `integration-tests` environment; review
the checkout and workflow before granting credentials. Never use
`pull_request_target` for untrusted code.

| Input | Default and behavior |
| --- | --- |
| `build` | Optional exact build, e.g. `10.2.0.11-453`; when set it takes precedence over `series`. |
| `series` | Optional two-part release family or four-part maintenance series; defaults to `10.2.0.11`. |
| `source` | `local` builds and tests the checkout; `bundled` tests the selected distribution's plugin. |
| `pdi_image` | Optional override or immutable digest; otherwise inferred from the selected build (`10.2.0.11` for 10.2.0.11). |
| `jdbc_file` | Optional path to another JDBC JAR inside the runner container, such as `/it-conf/other-driver.jar`; the supplied JDBC 6 JAR is the default. |
| `test_md5_auth` | Enable MD5 for the disposable test user; defaults to `true` for JDBC 6 and can be disabled for another driver. |

The `k8s` runner needs `PDIA_AC_CONTAINER_IMAGE`, `ARTIFACTORY_HOST` and
`ARTIFACTORY_BASE_URL` variables, `PENTAHO_CICD_ONE_USER` and
`PENTAHO_CICD_ONE_KEY` secrets, a Docker daemon, the declared Maven cache mount,
and the `/it-conf` runner mount. The job image must supply Java 11 for the 10.2
checkout, Maven, Docker, Bash, `curl` and `jq`; it also needs registry, catalog,
repository and licensing service access. `/it-conf` provides the license
configuration and can provide an alternate JDBC JAR selected through `jdbc_file`.
The workflow preserves existing checkout files. Remote execution on `k8s` has
**not** been validated.

## Assertions, evidence and cleanup

The seven serial tests cover healthy large-stream EOF, positional non-direct
mapping, empty input, permission and CHECK failures, Kitchen's error branch, and
recovery after repeated failures. The separate five-test lifecycle suite adds
multi-copy EOF and failure, slow finite input, Abort during server-confirmed
active COPY, and pooled failure/recovery in one Kitchen JVM. Successful launchers
must exit 0. Expected failures must exit naturally with code 1 and the loader's
SQL error plus the expected server detail; unexpected cleanup or uncaught
missing-class errors fail the test. Database assertions verify rows and session
closure. Each launcher runs in a separate process group. After 120 seconds the
watchdog sends QUIT for diagnostics and forcibly terminates any remaining process
group after a timeout; a timeout or forced termination is a failure, never an
accepted SQL failure.

The platform module's `target/failsafe-reports` and `target/evidence` contain test
reports, build labels, logs, status and process/session diagnostics; reports and
top-level build labels are overwritten on the next run. Resolver provenance is in
`target/runtime/pdi-build-provenance.txt`. CI uploads Failsafe XML/text and
evidence JSON/log/text for seven days, not Docker contexts or license files. Logs
should be reviewed before sharing. JDBC and candidate ZIP source and SHA-256 are
recorded; no automatic whole-image or installed-plugin checksum manifest is claimed.

Derived images and `target/stage` and `target/docker` contexts contain the license
configuration. Do not publish them. CI attempts run-scoped cleanup even on failure;
after interrupted local runs remove only that run's containers, network and derived
image, plus the generated staging/build contexts. Preserve the supplied
configuration, test evidence and other operators' Docker resources. See the
[QA summary](integration-test/SP-7583-QA-summary.md) for what remains untested.