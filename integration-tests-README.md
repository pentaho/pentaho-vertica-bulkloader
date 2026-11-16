# SP-7772 Vertica Bulk Loader integration tests

## Overview

The standalone [integration-test Maven reactor](integration-test/pom.xml) exercises
SP-7772, the 11.0 backport of PDI-20802, with real Pan/Kitchen, native Vertica
COPY and independent `vsql` checks.
Fabric8 builds a PDI image and starts disposable PDI and Vertica containers; JUnit 5
tests run through Maven Failsafe. A separate lifecycle suite tests real multi-copy,
slow-input, transformation Abort and pooled-recovery paths. The design follows the Big Data and OpenLineage
integration-test patterns. It does not modify the existing samples or require
Python or Docker Compose. See the [QA summary](integration-test/SP-7772-QA-summary.md)
for results, known failures, coverage and outstanding automation.

The standalone default base is `repo.pentaho.com/pnt-docker/automation/pdi-client:11.0.0.4`;
the database is `repo.pentaho.com/docker/vertica-ce:24.1.0-0`. The fixtures use
`VERTICA5`, `vertica-hostname`, `vmart` and port 5433 on a private Docker network;
the Java suite creates its own test user and tables. Do not point the suite at a
shared database. Standalone Maven defaults to the bundled plugin; CI defaults to
the plugin built from the checkout.

## Prerequisites

- Java 17 and Maven on the host for this 11.0 checkout. Bash, `curl`, `jq` and SHA-1/SHA-256 utilities are
  required for build resolution. The PDI image provides its own Java runtime.
- Docker CLI and daemon capable of `linux/amd64` builds and containers; Apple
  Silicon requires amd64 emulation. Allow sufficient memory and disk.
- Access to internal Maven and Docker registries, the build catalog and the PDI
  archive repository; configure Maven and Docker credentials separately.
- The selected PDI image may provide an ELM configuration. To override it, place
  an approved file at `integration-test/conf/.elmLicInfo.plt` or select one with
  `-Dvertica.it.license-file=/absolute/path`. It is a
  license-service connection, not an offline entitlement: the container needs
  service connectivity and a valid entitlement. The default local file is
  ignored by Git; do not print or upload its contents.
- A Vertica JDBC driver in the selected distribution or image, or an explicit
  existing driver JAR; see [configuration](integration-test/conf/README.md).
  Neither the license configuration nor JDBC is downloaded by the suite.

## Run locally

Run from the repository root with a JDK appropriate for the checkout. The resolver
searches the official catalog by version and defaults to `11.0.0.4`. For a two-part family such as `11.0`, it
chooses the highest numeric maintenance series and then the highest build; a
four-part series such as `11.0.0.4` limits selection to that series. An exact
`--build` overrides `--series`. The resolver downloads the distribution if
necessary, verifies its published SHA-1 or SHA-256, rejects unexpected archive
URLs and records SHA-256 provenance.
Archives are cached in the platform module's `target/runtime`.

```bash
# Select the latest 11.0.0.4 build, or pin the build under validation.
resolution=$(bash integration-test/resolve-pdi-build.sh --series 11.0.0.4)
# To pin a build instead: resolution=$(bash integration-test/resolve-pdi-build.sh --build 11.0.0.4-332)
build=$(printf '%s\n' "$resolution" | sed -n 's/^build=//p')
archive=$(printf '%s\n' "$resolution" | sed -n 's/^archive=//p')
image=repo.pentaho.com/pnt-docker/automation/pdi-client:11.0.0.4

# Bundled plugin from the exact distribution.
mvn --batch-mode --no-transfer-progress -f integration-test/pom.xml verify \
  -DrunIntegrationTests -Dvertica.it.source=bundled \
  -Dvertica.it.pdi-image="$image" -Dvertica.it.pdi-build="$build" \
  -Dvertica.it.pdi-archive="$archive" -Dvertica.it.run-id=qa-bundled

# Candidate plugin from this checkout; root build also runs unit tests.
mvn --batch-mode --no-transfer-progress clean install
mvn --batch-mode --no-transfer-progress -f integration-test/pom.xml verify \
  -DrunIntegrationTests -Dvertica.it.source=local \
  -Dvertica.it.pdi-image="$image" -Dvertica.it.pdi-build="$build" \
  -Dvertica.it.pdi-archive="$archive" -Dvertica.it.run-id=qa-local
```

Maven alone does not resolve or download the distribution:
without `vertica.it.pdi-archive` it uses the image runtime. The exact archive must
contain `data-integration/pan.sh` at its root; it replaces the whole image runtime.
`bundled` tests that distribution's plugin; `local` replaces it with the single
assembly from the root build, irrespective of branch version. Multiple candidate
ZIPs fail rather than choosing an ambiguous one. `vertica.it.plugin-zip` selects
another local candidate explicitly.
`vertica.it.pdi-build` is a provenance label, not a verifier; use the resolver for
verified build selection. Override `vertica.it.pdi-image` or
`vertica.it.vertica-image` when necessary, preferably with immutable digests.

By default the harness uses a distribution driver, or the image driver if the
distribution contains none. Build 332 has no Vertica JDBC JAR in `lib`, and
the tested `11.0.0.4` image supplies JDBC 24.4. Image tags are mutable: check
the recorded driver source and checksum. An explicitly selected
`-Dvertica.it.jdbc-file=/absolute/path/to/driver.jar` takes precedence over
runtime and image drivers and must exist.
Maven and CI default to normal authentication (`vertica.it.test-md5-auth=false`).
JDBC 6 requires opt-in MD5 for the disposable `qa_loader` user. MD5 is weak:
never enable it on a production or shared server.

Always use `verify` for Failsafe verification and normal container teardown. Use a
unique lowercase Docker-safe `vertica.it.run-id`. Concurrent runs require separate
checkouts: run IDs isolate Docker names, **not a shared workspace**:
use separate checkouts/build directories for concurrent runs because staging and
reports use common target directories.

To run only the lifecycle suite, add `-Dit.test=VerticaBulkLoaderLifecycleIT` to
the integration Maven command. Do not interpret failures as expected passes or
suppress them in CI. The inherited fixture labels mention PDI-20802 because
SP-7772 is its 11.0 backport; they do not select a different plugin or release.

## SP-7791 long-type coverage

`VerticaBulkLoaderLongTypesIT` tests existing `LONG VARCHAR` and `LONG VARBINARY`
columns using Pan and native COPY. Run it with
`-Dit.test=VerticaBulkLoaderLongTypesIT`. The Janino scenario temporarily enables
the bundled plugin in the disposable container and restores its marker afterward.

For the two opt-in 32,000,000-byte tests, resolve build `11.0.0.4-335` above
and use a suitably provisioned Docker host:

```bash
mvn --batch-mode --no-transfer-progress -f integration-test/pom.xml verify \
  -DrunIntegrationTests -Dvertica.it.source=bundled \
  -Dvertica.it.pdi-image="$image" -Dvertica.it.pdi-build="$build" \
  -Dvertica.it.pdi-archive="$archive" -Dvertica.it.run-id=sp7791-wide \
  '-Dit.test=VerticaBulkLoaderLongTypesIT#maximumWidth*' \
  -Dvertica.it.run-wide-tests=true \
  '-Dvertica.it.pdi-java-options=-Xms256m -Xmx4g'
```

Both maximum-width tests failed with the default heap on build 335 and passed
with the 4 GB workaround. See the [SP-7791 QA summary](integration-test/SP-7791-QA-summary.md)
for the release-specific results and the separately tracked memory limitation.

## CI

[The GitHub workflow](.github/workflows/integration-tests.yml) runs on pushes and
same-repository PRs to `11.0`, and supports manual dispatch
and reusable workflow calls. Fork PRs do not run the credentialed job. Configure
independent required reviewers for the `integration-tests` environment; review
the checkout and workflow before granting credentials. Never use
`pull_request_target` for untrusted code.

| Input | Default and behavior |
| --- | --- |
| `build` | Optional exact build, e.g. `11.0.0.4-332`; when set it takes precedence over `series`. |
| `series` | Optional two-part release family or four-part maintenance series; defaults to `11.0.0.4`. |
| `source` | `local` builds and tests the checkout; `bundled` tests the selected distribution's plugin. |
| `pdi_image` | Optional override or immutable digest; otherwise inferred from the selected build (`11.0.0.4` for 11.0.0.4). |
| `jdbc_file` | Optional JDBC JAR path inside the runner container; otherwise use the distribution or image driver. |
| `test_md5_auth` | Enable MD5 for the disposable test user for compatible legacy-driver tests; defaults to `false`. |

The `k8s` runner needs `PDIA_AC_CONTAINER_IMAGE`, `ARTIFACTORY_HOST` and
`ARTIFACTORY_BASE_URL` variables, `PENTAHO_CICD_ONE_USER` and
`PENTAHO_CICD_ONE_KEY` secrets, a Docker daemon, the declared Maven cache mount,
and the `/it-conf` runner mount. The job image must supply a suitable JDK for the
checkout (Java 17 for 11.0), Maven, Docker,
Bash, `curl` and `jq`; it also needs registry, catalog, repository and licensing
service access. `/it-conf` can provide an approved license configuration and an
alternate JAR selected through `jdbc_file`. Without an external configuration,
the run relies on the selected PDI image. The workflow preserves existing
checkout files. Remote `k8s` validation results and limitations for the original
harness are in the [QA summary](integration-test/SP-7772-QA-summary.md). The
[long-type PR integration run](https://github.com/pentaho/pentaho-vertica-bulkloader/actions/runs/43897253829)
passed on `k8s` with PDI `11.0.0.4-335` and the checkout-built plugin. It ran
25 normal integration tests and skipped the two opt-in maximum-width tests.

## Assertions, evidence and cleanup

The original seven serial tests cover healthy large-stream EOF, positional non-direct mapping,
empty input, permission and CHECK failures, Kitchen's error branch, and recovery
after repeated failures. The separate five-test lifecycle suite adds multi-copy
EOF and failure, slow finite input, Abort during server-confirmed active COPY,
and pooled failure/recovery in one Kitchen JVM. Successful launchers must exit 0.
Expected SQL failures must exit naturally with code 1 and the loader's SQL error
plus the expected server detail; the intentional Abort also exits with code 1.
Unexpected cleanup or uncaught missing-class errors fail the test;
database assertions verify rows and session closure. Each launcher runs in a
separate process group. After 120 seconds the watchdog sends QUIT for diagnostics
and forcibly terminates any remaining process group after a timeout; a timeout or
forced termination is a failure, never an accepted SQL failure.

The platform module's `target/failsafe-reports` and `target/evidence` contain test
reports, build labels, logs, status and process/session diagnostics; reports and
top-level build labels are overwritten on the next run. Resolver provenance is in
`target/runtime/pdi-build-provenance.txt`. CI uploads Failsafe XML/text and
evidence JSON/log/text for 30 days, not Docker contexts or license files. Logs
should be reviewed before sharing. JDBC and candidate ZIP source and SHA-256 are
recorded; no JVM class-load trace or automatic whole-image or installed-plugin
checksum manifest is claimed.

### Reading test evidence

Start with `failsafe-reports/failsafe-summary.xml` for overall results. The
`TEST-...VerticaBulkLoaderIT.xml` and `TEST-...VerticaBulkLoaderLifecycleIT.xml`
reports contain individual test cases; a passing case has no `failure`, `error`
or `skipped` child. The corresponding `.txt` reports summarize each test class
and include failure details when present.

For SP-7791, also inspect `TEST-...VerticaBulkLoaderLongTypesIT.xml` and its
`.txt` report. The normal CI run skips `maximumWidthLongVarchar` and
`maximumWidthLongVarbinary`; skipped cases have no execution evidence in that run.
See the [SP-7791 QA summary](integration-test/SP-7791-QA-summary.md#remaining-coverage-and-evidence)
for the CI ZIP and separately retained local maximum-width reports and logs.

To investigate one test, find its `<test-name>-<id>` directory under
`evidence/PDI-20802-<id>/`. There is a separate suite directory for each test
class. The numeric suffixes distinguish generated files and directories; they
do not indicate execution order. A test may have several `pan-<id>` or
`kitchen-<id>` invocation directories.

The long-type suite instead uses `evidence/PDI-20182-<id>/<test-name>-<id>/`.
Its normal cases use Pan; the two over-width cases each have an expected failing
Pan invocation and a successful recovery invocation. Interpret each status in
the context of the scenario and its JUnit result.

For a local run with `vertica.it.evidence-dir`, replace the `evidence/` root in
these paths with that run's configured directory. Match saved reports to the same
run; the default Failsafe report directory is overwritten by later runs.

| File | Contents |
| --- | --- |
| `pan-<id>.log`, `kitchen-<id>.log` | Detailed transformation or job output, including loader messages and errors. |
| `<name>.log.command` | Command that produced the corresponding log; SQL statements are redacted. |
| `<name>.log.status` | Command exit status, or `HOST_TIMEOUT_OR_INTERRUPTION`. |
| `vsql-<id>.log` | Database setup or query output, including row and session counts. |
| `loader-sessions-<id>.log` | Snapshot of Vertica sessions for the test loader user. |
| `pdi-processes-<id>.log` | Snapshot of processes in the PDI container. |
| `docker-cp-<id>.log` | Output from copying a rendered fixture into the PDI container. |

For `copyStartPermissionDenied`, check the Pan log for the permission error,
the expected launcher status `exit=1`, and the database and session evidence.
A nonzero launcher status is not automatically a test failure: interpret it
using the scenario and JUnit result. Exit codes 124 and 137, host timeouts and
forced termination are failures. For `repeatedFailureThenRecovery`, inspect all
three Pan invocations: two expected failures followed by a successful load. The
Kitchen failure-branch and pooled recovery scenarios use Kitchen logs.

At the evidence root, `build.txt` records run configuration, `candidate.txt`
identifies the candidate and checksum when a candidate is used,
`pdi-build-provenance.txt` records the resolved build and archive provenance,
and `vertica.log` contains Vertica container output. Each suite's `setup/`
contains setup SQL output, `jdbc-provenance-<id>.log` and
`generated-directories-<id>.log` with their command and status files.

CI does not upload the rendered transformation or job files. Because SQL is
redacted in recorded commands, a `vsql` output alone may not identify which
query produced it; consult the test code and the scenario table in the
[QA summary](integration-test/SP-7772-QA-summary.md). Session and process
snapshots do not prove the absence of all in-JVM thread leaks. Review evidence
before sharing: logs and metadata may contain internal paths, URLs or other
sensitive operational details.

### Cleanup

License overrides are copied into the running container, not the derived image or
build context. The base image may contain license configuration, and older build
contexts may contain overrides. Do not publish derived images or build contexts.
CI attempts run-scoped cleanup even on failure;
after interrupted local runs remove only that run's containers, network and derived
image, plus the generated staging/build contexts. Preserve the supplied
configuration and other operators' Docker resources. See the
[QA summary](integration-test/SP-7772-QA-summary.md) for what remains untested.
