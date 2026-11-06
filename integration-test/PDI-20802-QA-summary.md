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
| Bundled distribution plugin, both strict suites | 12 real integration tests: 5 passed, 7 failed; same scenarios as candidate | 6:17 / Nov 6 13:40:25 |
| Bundled plugin plus DBCP 1.4, pooled test only | Recovery completed; test failed on separate JDBC cleanup error | 2:51 / Nov 6 13:44:38 |

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

### Bundled and candidate comparison

On November 6, both strict suites were run against the plugin bundled in the
11.1.0.0-461 archive. The same 12 scenarios had the same pass/fail outcomes as
the previous master-candidate runs: the original suite passed 3 and failed 4,
and the lifecycle suite passed 2 and failed 3. In both plugin variants the six
SQL-failure or Abort cases failed the cleanup-log assertion with the missing
`com.vertica.jdbc.SConnection`; the pooled test failed after 120 seconds with
the missing `org.apache.commons.dbcp.DelegatingConnection`. The bundled and
candidate plugin JARs and loader class files have different hashes, so identical
binaries are not assumed. Both runs used the same PDI archive, automation image
digest, Vertica image and image-supplied JDBC 9.2.1-0 checksum listed below.

A separate diagnostic kept the bundled plugin core JAR byte-for-byte unchanged
and added the locally cached `commons-dbcp:commons-dbcp:1.4` JAR to its plugin
`lib` directory. Only the pooled recovery test was rerun. The missing DBCP class
and watchdog hang did not recur: the first COPY reached Vertica's permission
error, the job restored access, the retry completed naturally, and independent
assertions verified 100,000 correct rows. The test still **failed** its final
clean-log assertion because the first load's cancellation logged the separate
missing `com.vertica.jdbc.SConnection` class. These observations strongly support
the missing legacy DBCP class as the pooled hang's trigger in this environment;
they do not validate the wrapper-lease race or establish the correct production
packaging. The modified plugin archive was a test-only diagnostic, not a fix or
an endorsement to add an old dependency to production.

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
`pdi20802-original-strict`. The strict bundled comparison and the DBCP diagnostic,
including copied Failsafe reports, are in `pdi20802-bundled-comparison-1106` and
`pdi20802-dbcp1-diagnostic-1106`, respectively. The earlier combined lifecycle run was contaminated by a
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

## Pooled recovery test details

`pooledConnectionRecoversInSameKitchenJvm` uses a real PDI Kitchen job, Vertica
connection pool, JDBC driver and Vertica server. It is intended to check that a
failed load with pooling enabled does not prevent a subsequent load in the same
Kitchen JVM from succeeding. The scenario is:

1. Create a test table and user, then revoke the user's `INSERT` privilege while
   retaining the access needed for table metadata. Enable pooling on the loader's
   database connection and start a transformation generating 100,000 rows.
2. Expect Vertica to reject the first COPY with a permission error. The job must
   follow the transformation's failure hop; an unexpected successful first load
   goes to an Abort job entry instead.
3. On the failure hop, an admin connection restores `INSERT` permission. The job
   reruns the same transformation, with pooling enabled, in the same Kitchen JVM.
4. Expect Kitchen to finish naturally with exit code `0`, log the pool acquisition,
   the original SQL permission error and the recovery job entry, and leave no
   loader sessions. Independently query Vertica for exactly 100,000 rows with the
   expected ID and payload. The log must not contain unexpected cleanup errors,
   `NoClassDefFoundError`, or an uncaught thread exception.

The test does not assert an intermediate zero-row count after the denied load,
nor does it verify that the retry borrows the same physical connection or prove
the pooled-wrapper timeout race described below.

**Relationship to step execution:** This failure occurs during execution of the
Vertica Bulk Loader step itself, not in the test assertion or Docker harness.
The step's COPY worker calls `runCopy`, which attempts to obtain the underlying
Vertica connection from the pooled wrapper before creating the COPY stream.
That plugin code encounters the missing class and the worker exits unexpectedly;
the test watchdog only detects and terminates the subsequent hang. No COPY is
sent to Vertica, so this is not the intended server-side permission failure.
The missing class may depend on the tested runtime's dependency packaging or
class-loader visibility; this result alone does not establish a defect in every
supported installation or prove that the original PDI-20802 failure is unfixed.

**Observed failure:** With either the candidate or the unmodified bundled plugin,
PDI created a pool and acquired a pooled connection, but the first load's COPY
worker failed while trying to unwrap that connection, before creating or starting
the Vertica COPY stream:

```text
Exception in thread "pool-5-thread-1" java.lang.NoClassDefFoundError: org/apache/commons/dbcp/DelegatingConnection
    at org.pentaho.di.verticabulkload.VerticaBulkLoader.unwrapConnection(VerticaBulkLoader.java:929)
    at org.pentaho.di.verticabulkload.VerticaBulkLoader.getVerticaConnection(VerticaBulkLoader.java:921)
   at org.pentaho.di.verticabulkload.VerticaBulkLoader.getVerticaConnection(VerticaBulkLoader.java:911)
    at org.pentaho.di.verticabulkload.VerticaBulkLoader.createVerticaCopyStream(VerticaBulkLoader.java:906)
    at org.pentaho.di.verticabulkload.VerticaBulkLoader.runCopy(VerticaBulkLoader.java:401)
Caused by: java.lang.ClassNotFoundException: org.apache.commons.dbcp.DelegatingConnection
```

The worker's `runCopy` catches `Exception`, but `NoClassDefFoundError` is a Java
`Error`, so it escapes that handler. Kitchen did not terminate naturally; after
120 seconds the test watchdog terminated the process group. The first assertion
failed because the expected exit code was `0` and the actual code was `124`.
The permission rejection, failure hop, grant and recovery were not reached, so
their assertions were not evaluated. The pool acquisition is an observation in
the retained log, not an assertion reached by this failed test. Any session
closure after the watchdog kill is not proof of natural cleanup.

**What the missing legacy DBCP class means:** Apache Commons DBCP is a JDBC
connection-pooling library. The plugin's `unwrapConnection` directly references
`org.apache.commons.dbcp.DelegatingConnection`, a wrapper class in the older
`commons-dbcp` 1.x package, and the core POM declares `commons-dbcp:commons-dbcp:1.4`.
When a pooled connection is passed to the loader, Java must resolve that class
to evaluate the `instanceof` check. In this run the plugin's class loader could
not find it. The selected PDI 11.1.0.0-461 archive contains
`commons-dbcp2-2.9.0.jar`, whose classes use the different
`org.apache.commons.dbcp2` namespace; DBCP2 does not provide the older class
under its original name. The tested candidate plugin assembly did not include a
legacy DBCP JAR. The exact cause of the missing runtime dependency or class-loader
visibility, and whether this packaging is expected for a supported installation,
still need developer investigation. This is separate from the missing Vertica
`SConnection` class reported by the non-pooled cancellation tests.

The final run's Kitchen log and diagnostics are retained locally under
`integration-test/pentaho-platform/target/evidence/pdi20802-lifecycle-final2/`
in the `pooledConnectionRecoversInSameKitchenJvm-*` directory. These generated
artifacts are not committed; review them before sharing.

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