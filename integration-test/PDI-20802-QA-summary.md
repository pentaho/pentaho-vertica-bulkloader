# PDI-20802 QA validation summary

## Scope and conclusion

[PDI-20802](https://pentaho-eng.atlassian.net/browse/PDI-20802) addresses Vertica
Bulk Loader hangs after certain Vertica SQL exceptions. The expected behavior is
controlled step and transformation failure, execution of the job error branch,
and termination without manual intervention. The fix was merged to master in
[PR #104](https://github.com/pentaho/pentaho-vertica-bulkloader/pull/104), commit
`528bd20ebd85c20502596900be5bad5d07ea992b`.

**Result: cleanup and pooled-load failures found on PDI 11.1.0.0-461.** The previously executed
candidate and bundled suites passed their original assertions, but their logs
show unexpected cleanup failures that those assertions did not check. New real
integration tests also reproduce a pooled COPY worker crash and hang. Spoon Stop,
slow COPY finalization, the exact pooled timeout race, the original cluster-fault
reproductions and remote `k8s` CI remain **not validated**. Do not treat these
automated results alone as proof of all ticket criteria.

## Executed validation

Run locally on November 5, 2026 using the master-based `PDI_20802_tests` worktree,
Java 17 for Maven, the exact 11.1.0.0-461 distribution, the `11.1` automation
base image with Java 21, real Vertica CE 24.1.0-0, the supplied ELM service
configuration and image-supplied JDBC. No license or JDBC override was supplied.

| Validation | Result | Maven duration / finish (UTC) |
| --- | --- | --- |
| Master candidate on 461 | 7 integration tests passed their original assertions; cleanup exception was missed | 3:54 / 18:09:47 |
| Bundled distribution plugin on 461 | 7 integration tests passed their original assertions; cleanup exception was missed | 3:52 / 18:13:57 |
| Master candidate, lifecycle suite | 5 real integration tests: 2 passed, 3 failed | 4:27 / 23:43:40 |
| Master candidate, stricter original suite | 7 real integration tests: 3 passed, 4 failed on cleanup error | 2:40 / 23:47:25 |

The candidate was built from merged master commit `528bd20`; it was overlaid onto
the 461 runtime. The bundled run used the plugin already present in the 461 ZIP.
The original assertion results do not establish that their plugin binaries are
identical, prove when the fix entered a published build, or prove clean cleanup.
The 461 ZIP did not include a Vertica JDBC driver, so the harness preserved JDBC
9.2.1-0 from the automation image. The plugin declares a provided JDBC 6 dependency;
whether 9.2.1-0 and the tested library packaging are supported must be confirmed
before attributing these failures to a product defect rather than deployment compatibility.

Both earlier runs contain five affected Pan/Kitchen launcher logs. They report
`NoClassDefFoundError: com/vertica/jdbc/SConnection` from `cancelRunningCopy`
as an unexpected database-cleanup error. In the new multi-copy run, all three
copies logged that cleanup error. The image-supplied JDBC 9.2.1-0 JAR does not
contain that class; behavior with other drivers remains to be investigated.

With pooling enabled, the master candidate acquires a real pooled connection,
then the COPY worker throws `NoClassDefFoundError` for
`org/apache/commons/dbcp/DelegatingConnection` in `unwrapConnection`. Kitchen
does not exit naturally and reaches the 120-second watchdog. The tested image
contains `commons-dbcp2`, not the legacy `commons-dbcp` JAR. The pooled run does
not reach recovery or prove the reviewed wrapper-lease race. The watchdog kill
is a **failure**, not successful cleanup.

| Provenance | Value |
| --- | --- |
| PDI archive | `11.1.0.0-461`; catalog SHA-1 `c4d5844610a7778100babd6c01eb7e83d9fa8545`; SHA-256 `29729d25c931b43ad6b685a03d5cfeb20c9458606661330338490a6ba288798a` |
| PDI base image | `repo.pentaho.com/pnt-docker/automation/pdi-client:11.1`, digest `sha256:2ace33b7d1a13c4e095d86c0750d914d53bc337d100afeff5c9e2a1c2d82a412` |
| Master candidate assembly | `vertica-bulkloader-plugin-11.1.0.0-SNAPSHOT.zip`, SHA-256 `f2d6d4aa63070f179558ff27e3cceb23d217032b9c39b51c4145de8fa1669137`; recorded after the run from the unchanged candidate ZIP |
| Vertica JDBC | Image-supplied 9.2.1-0, preserved across runtime replacement; SHA-256 `2791104a77cd22fbd6605157e18e27bd71eef24cbffb3a04e2dacdaad7e0d89d` |
| Vertica database | `repo.pentaho.com/docker/vertica-ce:24.1.0-0` |

The repository ELM file configures a license-service connection, not an offline
entitlement; service access and a valid entitlement are required. No license or
JDBC driver is downloaded. The resolver verified the 461 archive against the
catalog. On November 5, latest family resolution selected `11.1.0.0-461` for
`11.1`, `11.0.0.4-328` for `11.0`, and `10.2.0.11-452` for `10.2`; only 461 was
integration-tested in this worktree. Latest can change; these are not fixed defaults.

Local generated evidence is retained under the platform module's `target/evidence`
in separate `pdi20802-461-local` and `pdi20802-461-bundled` directories. Each
contains its build labels, catalog provenance, Failsafe report, Pan/Kitchen and
SQL logs, selected driver provenance and diagnostics. The candidate checksum file
was generated after the successful run from the same assembly. These are local
build artifacts, not committed certification; archive or attach reviewed reports
and logs as appropriate. Do not include license contents or Docker build contexts.
The final lifecycle run and its copied Failsafe report are in
`pdi20802-lifecycle-final2`; the stricter original-suite run is in
`pdi20802-original-strict`. The earlier combined lifecycle run was contaminated by a
watchdog-orphaned JVM; it is retained as `pdi20802-lifecycle-final`, but its
post-timeout session assertions must not be interpreted as independent results.

## Automated coverage

The [Java suite](pentaho-platform/src/test/java/org/pentaho/di/verticabulkload/it/VerticaBulkLoaderIT.java)
uses actual Pan/Kitchen, native Vertica COPY and independent database assertions.

| Scenario | Assertions |
| --- | --- |
| `healthyEofLargeStream` | 100,000 mapped/direct rows finish at EOF and preserve values. |
| `defaultMappingNonDirect` | 17 positional/non-direct rows preserve values. |
| `emptyInput` | Empty stream succeeds without target rows. |
| `copyStartPermissionDenied` | COPY permission error fails naturally and commits no rows. |
| `serverCheckRejectionAborts` | Server CHECK rejection fails naturally and commits no rows. |
| `kitchenFailureBranch` | Error hop writes exactly one audit marker; ABORT leaves the job failed. |
| `repeatedFailureThenRecovery` | Two failures are followed by zero sessions and a successful 23-row load; cleanup errors were missed. |

In the stricter rerun, the first three healthy scenarios passed and the four SQL
failure scenarios failed on the cleanup error. The repeated-failure test stopped
at its first failure and did not reach recovery in that rerun.
In each failure, the assertion that output must not contain
`Unexpected error during Vertica bulk-load database cleanup.`,
`java.lang.NoClassDefFoundError`, or `Exception in thread "` found the first two
strings. The logged missing class was `com.vertica.jdbc.SConnection`.

Healthy launchers must exit 0. Negative cases require natural exit 1, the loader's
SQL error and expected server detail; a generic producer error, timeout or forced
kill cannot pass. Target state and zero remaining test-user sessions are checked
independently. SQL-failure tests now reject unexpected cleanup errors and missing
classes. The 120-second QUIT watchdog and targeted process-group cleanup are a
failure boundary, not evidence of successful recovery. The suite does not prove absence of every
in-JVM thread leak or cover every SQL exception. Its connection has pooling
disabled, and the recovery test starts separate PDI JVMs; it does not exercise
pooled-wrapper reuse.

Session closure after Pan exits does not prove that plugin cleanup succeeded:
the operating system can close connections when the JVM exits. The original
negative tests missed the driver-class cleanup error noted above.

The separate [lifecycle suite](pentaho-platform/src/test/java/org/pentaho/di/verticabulkload/it/VerticaBulkLoaderLifecycleIT.java)
uses real PDI and Vertica without mocked driver or pool behavior:

| Scenario | Final result on master candidate and 461 |
| --- | --- |
| `multipleCopiesCompleteAtEof` | **Pass:** three copies process rows; all 100,000 values commit. |
| `slowInputCompletesAtEof` | **Pass:** 100 rows stream through a FIFO for over five seconds and commit at EOF. This is not a delayed COPY-finalization test. |
| `multipleCopiesStopAfterCopyFailure` | **Fail:** all copies terminate and zero rows commit, but unexpected cancellation-cleanup errors are logged. |
| `transformationAbortDuringCopy` | **Fail:** Vertica records COPY active at Abort and zero rows commit, but cancellation cleanup logs the same error. |
| `pooledConnectionRecoversInSameKitchenJvm` | **Fail:** a real pool is created, but the worker's missing legacy DBCP class causes a hang before recovery. |

The multi-copy failure and Abort cases failed the same cleanup-log assertion as
the original suite, after their earlier scenario assertions passed. The pooled
case failed the natural-exit assertion: expected exit code `0`, actual `124`
after the watchdog timeout. Its later SQL-error, recovery and row assertions
were not reached.

## Outstanding pooled-connection integration scenario

**Not run:** forced stop or cancellation during an active COPY where COPY and
the cancellation operation outlive the shutdown deadline. The
[master review follow-up](https://github.com/pentaho/pentaho-vertica-bulkloader/pull/104)
and [10.2 backport](https://github.com/pentaho/pentaho-vertica-bulkloader/pull/105)
address a race in which returning the pooled wrapper early permits another load
to borrow a connection still used by the old worker. Closing the physical
connection at the deadline must not release that wrapper until **both** COPY
and cancellation have exited.

Automate on each target branch with its own plugin and a disposable Vertica
instance, using a single PDI JVM with pooling enabled and test-only instrumentation
or another controlled mechanism that can independently hold and release COPY and
cancellation. Record the physical connection, pool lease and both thread states.
After the deadline, assert that the physical connection was closed while the
wrapper remains leased and unavailable to a competing borrower. Release the two
threads separately in each order; verify neither disconnect nor wrapper release
occurs while either remains alive. Once both exit, verify disconnect and wrapper
release complete, rollback occurs when the connection is still open, and the
pool and server sessions return to baseline. A timeout or process kill is not a
pass. The present Pan/Kitchen harness has no such stall or pool-lease observation
mechanism; the new same-JVM recovery test does not establish this race and is
currently blocked before COPY by the missing DBCP class.

## Remaining automation

These scenarios are **not yet automated or validated**. Use only a disposable dedicated Vertica
cluster with synthetic data, dedicated schema/user/resource pool, DBA approval,
an owner, known-good snapshot, committed-row baseline, resource/time limits and
restore plan. Never fill a shared disk, corrupt a shared projection or disable
cluster safeguards on a shared service. Record build, plugin/JDBC hashes, Vertica
version, topology and pool settings. First establish a healthy large-stream and
Kitchen baseline on copies of fixtures; do not modify repository samples.

1. Implement opt-in, vendor-supported fault orchestration for **mergeout queue
   bloat**, **disk-space failure**, and **broken projection** on DBA-approved
   disposable clusters. Confirm each exact fault using server metrics before
   asserting the original SQL error, natural failure, error hop and rollback.
2. Automate **Spoon Stop** with a controlled UI/display and server-confirmed
   active COPY. The implemented transformation Abort case does not test the UI.
3. Delay the real COPY finalization after input EOF for longer than the shutdown
   deadline, using a controlled transport or server mechanism. Throttled input
   before EOF alone does not establish this behavior.
4. Add multiple-copy forced-stop and pooled timeout/lease instrumentation, then
   verify worker states, competing borrowers, natural termination and session cleanup.
5. Capture process and server diagnostics, provenance and timing for each case;
   automatically restore and dispose of the dedicated resources. A watchdog kill
   is not a pass. Fault procedures and limits require DBA approval before use.

See the [integration-test guide](../integration-tests-README.md) for reproduction,
branch/build selection, CI prerequisites and sensitive cleanup.