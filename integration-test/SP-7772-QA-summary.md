# SP-7772 QA validation summary

## Scope

[SP-7772](https://pentaho-eng.atlassian.net/browse/SP-7772) is the 11.0
backport of PDI-20802, which addresses Vertica Bulk Loader hangs after certain
Vertica SQL exceptions. The test target is PDI 11.0.0.4 with the plugin built
from the 11.0 branch, as well as the plugin bundled with the tested PDI build.
The initial backport was merged in [PR #106](https://github.com/pentaho/pentaho-vertica-bulkloader/pull/106),
commit `3718dcbd`; the JDBC 24 follow-up was merged in
[PR #108](https://github.com/pentaho/pentaho-vertica-bulkloader/pull/108),
commit `2ac850697eb271f74fa4e8bbd2d0e584eeb9fd36`. The SP-7772 Jira comment
identifies `11.0.0.4-332` as containing the follow-up.

## Results

### Latest: build 332 and JDBC 24

On November 12, 2026, the published `11.0.0.4-332` distribution and the 11.0
candidate based on `2ac85069` were tested separately against disposable Vertica
CE `24.1.0-0`, with JDBC `24.4.0-0` and default SHA-512 authentication. The
candidate was built with Java 17; the PDI container used Java 21.0.10 on Ubuntu
22.04.5 LTS, Linux x86-64, on an ARM64 macOS host.

| Plugin under test | Result | Duration / finish (UTC) |
| --- | --- | --- |
| Plugin bundled with `11.0.0.4-332` | 12 passed, 0 failed, 0 skipped | 4:30 / 13:48:49 |
| 11.0 branch candidate | 12 passed, 0 failed, 0 skipped | 4:35 / 13:53:38 |

Both variants passed the seven original and five lifecycle scenarios, including
natural SQL failure and recovery, active-COPY Abort and pooled same-JVM recovery.
The candidate unit suite subsequently passed 72 tests, including two tests for
pooled-lease release ordering; these are mock-based unit tests, not integration
proof of the forced-stop race. The bundled and candidate binaries are separate
variants and are not assumed to be identical.

The initial bundled run reported 11/12 because its pooled test expected a log
message not emitted by this PDI release. The log showed pool creation, the
expected permission error and a successful retry; subsequent row and cleanup
assertions were not reached. After changing the assertion to the release's
successful pool-creation message, a focused pooled test and the full bundled
suite passed, including independent row and clean-log assertions. The initial
failure and focused rerun are retained separately.

| Provenance | Value |
| --- | --- |
| PDI archive | `11.0.0.4-332`; catalog SHA-1 `e3d2f791aa442c4040fd024e098668b2ddc8ea22`; SHA-256 `906814a1bfef1b8c7364bf46ae0d1afa5e3869a8fbb3e476bb91470203cf1549` |
| PDI automation image | `repo.pentaho.com/pnt-docker/automation/pdi-client:11.0`; digest `sha256:42b6923ed529001b05f4610e542b8bf714660aa07b30c86b11db2c238425b517` |
| JDBC override | `vertica-jdbc-24.4.0-0.jar`; SHA-256 `cc720c960d2280643b5c1d463a15e05a97fe9f456b30d1dfb5f16405ecbb0637` |
| JDBC source image | `repo.pentaho.com/pnt-docker/automation/pdi-client:11.1`; digest `sha256:f138c0cbc75245022430f9123c1f16f1f740b0b7ebc8026f43a9f8f3a4b3dfc6` |
| Candidate assembly used in integration run | SHA-256 `42e98d040d448be24bf61a6e23a546edfd4665b97687cc9059eb5af153b4aa8c` |

The resolver verified the archive against the published catalog checksum. The
332 distribution contains no Vertica JDBC JAR in `lib`; the freshly pulled 11.0
image contains JDBC 9.2, so the verified JDBC 24 JAR was extracted from the source
image and supplied explicitly. Both test classes in both final runs recorded the
override and matching checksum, and neither runtime contained another
Vertica-named JDBC JAR. No JVM class-load trace was captured; the provenance
records the staged driver, not independent proof of class origin.

Local evidence is retained under the platform module's `target/evidence` in
`sp7772-332-bundled-final` and `sp7772-332-candidate`, with the initial run and
focused probe in `sp7772-332-bundled` and `sp7772-332-poolprobe`. These are local
generated artifacts, not committed certification; review logs before sharing.

### Historical: build 330 and JDBC 6

On November 10, 2026, both suites were run locally against PDI
`11.0.0.4-330`, Vertica CE `24.1.0-0`, and the supplied JDBC `06.00.0000`
driver. The disposable `qa_loader` user was explicitly configured for MD5
authentication, because this JDBC driver cannot authenticate using the test
server's default SHA-512. MD5 is weak and this setting is for isolated test
databases only, never a production recommendation.

| Plugin under test | Result | Duration / finish (UTC) |
| --- | --- | --- |
| 11.0 branch candidate at `3718dcbd` | 11 of 12 integration tests passed; pooled recovery failed | 4:41 / 15:30:19 |
| Plugin bundled with `11.0.0.4-330` | 11 of 12 integration tests passed; pooled recovery failed | 4:39 / 15:25:15 |

The 11.0 branch build also passed 68 unit tests with no failures, errors or skips.
These unit tests are separate from the real integration evidence. The two plugin
core JARs have different hashes. Inspection of the bundled JAR confirms that it
contains the backported worker-lifecycle methods; the hashes do not establish
byte-for-byte or source-commit equivalence. The PDI-20802 results from 11.1
must not be interpreted as results for 11.0.0.4.

| Real integration scenario | 11.0 branch candidate | 11.0.0.4-330 bundled |
| --- | --- | --- |
| Healthy 100,000-row load | Pass | Pass |
| Non-direct default mapping | Pass | Pass |
| Empty input | Pass | Pass |
| COPY-start permission denied | Pass | Pass |
| Server CHECK rejection | Pass | Pass |
| Kitchen failure branch | Pass | Pass |
| Repeated failure then recovery | Pass | Pass |
| Multiple copies complete at EOF | Pass | Pass |
| Slow input completes at EOF | Pass | Pass |
| Multiple copies stop after COPY failure | Pass | Pass |
| Transformation Abort during active COPY | Pass | Pass |
| Pooled connection recovery in one Kitchen JVM | Fail: pool initialization | Fail: pool initialization |

### Pooled recovery: both plugin variants

The test revokes `INSERT`, expects the first pooled load to fail at COPY, then
restores permission and retries in the same Kitchen JVM. Both variants failed
earlier, while PDI initialized the pool: the supplied JDBC 6 driver's
`com.vertica.jdbc.VerticaConnectionImpl` does not implement
`java.sql.Connection.isValid(int)`.

```text
Unable to pre-load connection to the connection pool
Receiver class com.vertica.jdbc.VerticaConnectionImpl does not define or inherit an implementation of the resolved method 'abstract boolean isValid(int)' of interface java.sql.Connection.
```

The failure hop restored permission and attempted the retry, but both pool
initializations failed before COPY. Kitchen exited naturally with code `1`,
not the expected `0`. This does not test the underlying pooled-wrapper lease race.

### Permission denial and Abort during COPY

Both plugin variants passed the COPY-start permission-denied test: Pan reported
the Vertica permission error and exited naturally with the expected failure code.
Both also passed the transformation Abort test, which confirms COPY was active
at Abort and checks for uncaught worker and cleanup errors. These observations
cover the tested executions, not every possible shutdown timing.

## Provenance and evidence

| Item | Tested value |
| --- | --- |
| PDI archive | `11.0.0.4-330`; catalog SHA-1 `7beab897f55b6b63a4db6e20c7fca3481ef75109`; SHA-256 `15a1a43f5e1e3223888c65eb53f607bf4cffb545c4c42df5650c037db77bed5b` |
| PDI automation image | `repo.pentaho.com/pnt-docker/automation/pdi-client:11.0`, image ID `sha256:42b6923ed529001b05f4610e542b8bf714660aa07b30c86b11db2c238425b517` |
| JDBC 6 | SHA-256 `3fea0ca1ead071d3a1ce022232d19c8200e889b2b657575fa1b9b551a11dd224`, confirmed inside the PDI containers |
| Candidate assembly | `vertica-bulkloader-plugin-11.0.0.0-SNAPSHOT.zip`, SHA-256 `38cf79acc1320aa4128463188edee001653a6308c863c9c51e3c2574f2a38698` |
| Candidate core JAR | SHA-256 `010946d9e338d6156449991f3c4e86631313fe0da4f7a3bc7359c8b962d24f2a` |
| Bundled core JAR | SHA-256 `85354274fc0751e163db5c62a0af344e574a13c13241f0a76d966c223847479d` |
| Vertica | `repo.pentaho.com/docker/vertica-ce:24.1.0-0` |

The local platform module's `target/evidence/sp7772-330-local` and
`target/evidence/sp7772-330-bundled` directories retain separate Failsafe
reports, build labels, logs, driver provenance and diagnostics. They are local
generated artifacts, not committed certification; review logs before sharing.
The resolver records catalog provenance in `target/runtime`. The ELM file is
a license-service configuration, not an offline entitlement; service access
and a valid entitlement were required.

## Coverage and limitations

The real Pan/Kitchen and Vertica integration suites cover healthy and empty
loads, SQL failures and recovery, multi-copy execution, slow input, transformation
Abort and pooled recovery. Both variants' permission-denied and CHECK-rejection
cases demonstrate natural failure for those specific SQL errors, and their Kitchen
failure-branch cases verify an audit marker and propagated job failure.

SP-7772's acceptance criteria require the step to terminate and report failure,
the transformation not to hang or leave active threads, and job error handling
to run without manual termination for Vertica SQL exceptions. The passing proxy
cases provide partial evidence, not proof for every SQL exception or pooled
connection. Original mergeout-queue, disk-space and broken-projection failures,
Spoon Stop, slow COPY finalization, the exact pooled-wrapper lease race and
remote CI require separate validation. A timeout or forced termination is a
failure. See the [execution guide](../integration-tests-README.md) for setup and
evidence details.