# SP-7583 QA validation summary

## Scope and conclusion

[SP-7583](https://pentaho-eng.atlassian.net/browse/SP-7583) concerns Vertica
Bulk Loader hangs after certain Vertica SQL exceptions. The expected behavior is
natural step and transformation failure, no remaining worker threads, and job
error handling without manual termination. The Jira comment identifies
`10.2.0.11-453` as a build containing the fix.

The build 453 tests provide partial evidence, not full acceptance. With the
supplied JDBC 6 driver both plugin variants passed 11 of 12 scenarios; pooled
recovery failed before COPY. With the automation image's JDBC 9.2.1-0 driver both
variants passed 5 of 12; six failure/Abort scenarios reported cleanup errors and
the pooled scenario timed out. Original cluster-health faults, the exact pooled
lease race, Spoon Stop, slow COPY finalization and remote CI remain unvalidated.

## Automated results

On November 11, 2026, the real integration suites ran against the exact PDI
`10.2.0.11-453` archive, Vertica CE `24.1.0-0`, and either the bundled plugin or
the 10.2 candidate built from commit `cc313f0b`. The candidate root build ran on
Java 11 and passed 68 unit tests; those are separate from integration evidence.
The PDI automation image used Java 17.

| Driver and authentication | Plugin | Integration result | Maven duration / finish (UTC) |
| --- | --- | --- | --- |
| JDBC 6; test-user MD5 | Bundled | 11 of 12 passed; pooled initialization failed | 4:38 / 00:16:52 |
| JDBC 6; test-user MD5 | 10.2 candidate | 11 of 12 passed; pooled initialization failed | 4:41 / 00:21:54 |
| Image JDBC 9.2.1-0; default authentication | Bundled | 5 of 12 passed; six cleanup failures and pooled timeout | 6:09 / 00:28:36 |
| Image JDBC 9.2.1-0; default authentication | 10.2 candidate | 5 of 12 passed; six cleanup failures and pooled timeout | 6:08 / 00:35:05 |

The two plugin variants produced the same results for each driver in these runs.
JDBC 6 required MD5 only for the disposable `qa_loader` account because it could
not use the server's default SHA-512 authentication. MD5 is weak and is not a
recommendation for shared or production databases.

| Real integration scenario | JDBC 6: both plugins | Image JDBC 9.2.1-0: both plugins |
| --- | --- | --- |
| Healthy 100,000-row load | Pass | Pass |
| Non-direct default mapping | Pass | Pass |
| Empty input | Pass | Pass |
| COPY-start permission denied | Pass | Fail: cleanup error |
| Server CHECK rejection | Pass | Fail: cleanup error |
| Kitchen failure branch | Pass | Fail: cleanup error |
| Repeated failure then recovery | Pass | Fail: cleanup error |
| Multiple copies complete at EOF | Pass | Pass |
| Slow input completes at EOF | Pass | Pass |
| Multiple copies stop after COPY failure | Pass | Fail: cleanup error |
| Transformation Abort during active COPY | Pass | Fail: cleanup error |
| Pooled connection recovery in one Kitchen JVM | Fail: pool initialization | Fail: watchdog timeout |

### Pooled recovery with JDBC 6

The test revokes `INSERT`, expects the first pooled load to fail at COPY, then
restores permission and retries in the same Kitchen JVM. Both variants failed
earlier during pool initialization: the supplied JDBC 6
`com.vertica.jdbc.VerticaConnectionImpl` does not implement
`java.sql.Connection.isValid(int)`. The failure hop restored permission and
attempted the retry, but both initializations failed before COPY. Kitchen exited
naturally with code `1`, not the expected `0`. This does not exercise the pooled
connection lease race.

### Image JDBC 9.2.1-0 cleanup and pooled failures

Six scenarios recorded `NoClassDefFoundError: com/vertica/jdbc/SConnection`
during loader cleanup; the strict tests reject these unexpected errors even
when the intended SQL failure is reported. In the pooled scenario, the worker
threw `NoClassDefFoundError: org/apache/commons/dbcp/DelegatingConnection`, and
Kitchen did not terminate naturally. The watchdog captured a thread dump and
terminated the process after 120 seconds, yielding exit `124`. This is a failure,
not evidence of recovery, and does not establish the underlying lease race.

## Provenance and evidence

| Item | Tested value |
| --- | --- |
| PDI archive | `10.2.0.11-453`; catalog SHA-1 `c69b38590b702f1e8b76faea57e6ffe6a81f253a`; SHA-256 `d0ac67928ec246b492de64ad6efa40a00cd90ee080a9e18bbcaf26a08d77dbf8` |
| PDI image | `repo.pentaho.com/pnt-docker/automation/pdi-client:10.2.0.11`; image ID `sha256:ac2b6496b3e47d7268f631e6fda143e3deab9d1f4351683b89cb372a346219c0` |
| JDBC 6 override | SHA-256 `3fea0ca1ead071d3a1ce022232d19c8200e889b2b657575fa1b9b551a11dd224` |
| Image JDBC 9.2.1-0 | SHA-256 `2791104a77cd22fbd6605157e18e27bd71eef24cbffb3a04e2dacdaad7e0d89d` |
| Candidate assembly | SHA-256 `56ed6a8a0bab9f75e169564e39c5326a7aec941067ff58c260e20598e374a436` |
| Candidate core JAR | SHA-256 `9aa1b395637503a4b852087a4b0a5e40a73fa722ef6eac0f034071a738654a31` |
| Bundled core JAR | SHA-256 `1c0b9f338051bffe84d3c0c4637fc91dfe1e599262740009bbaaaf46dcec081f` |
| Vertica | `repo.pentaho.com/docker/vertica-ce:24.1.0-0` |

The platform module's local `target/evidence/sp7583-453-bundled`,
`sp7583-453-local`, `sp7583-453-image-bundled` and `sp7583-453-image-local`
directories contain separate Failsafe reports, build labels, logs, driver
provenance and diagnostics. These generated artifacts are not committed
certification; review logs before sharing. The resolver records catalog provenance
in `target/runtime`. The ELM file configures a license service, not an offline
entitlement; valid entitlement and service connectivity were required.

## Coverage and outstanding validation

The tests exercise healthy and empty loads, permission and CHECK failures,
Kitchen error handling, repeated failures and recovery, multiple copies, slow
input, Abort during server-confirmed active COPY and pooled recovery. The
permission-denied and CHECK tests with JDBC 6 demonstrate natural failure for
those particular errors; the Kitchen failure branch checks an audit marker and
propagated job failure. The suite checks test-user sessions but does not prove
the absence of every in-JVM thread leak or cover every SQL exception.

On a dedicated disposable cluster, with DBA approval, bounded resource limits,
synthetic data and a restoration plan, reproduce mergeout-queue bloat, disk-space
failure and broken projection separately. Confirm the intended fault, retain SQL
errors and server/PDI logs, capture thread and session state, and measure natural
termination and error-hop behavior. Verify rollback and a healthy load after
restoration. Separately validate Spoon Stop, slow COPY finalization and the exact
pooled lease race. Record pass/fail/not-run and evidence; never induce these faults
on a shared system. Remote CI has not been run. A watchdog kill is always a failure.

See the [execution guide](../integration-tests-README.md) for setup and cleanup.