# SP-7583 QA validation summary

## Scope and conclusion

[SP-7583](https://pentaho-eng.atlassian.net/browse/SP-7583) concerns Vertica
Bulk Loader hangs after certain Vertica SQL exceptions. The expected behavior is
natural step and transformation failure, no remaining worker threads, and job
error handling without manual termination. The latest Jira build comment
identifies `10.2.0.11-456` (SP RC 18) as containing the follow-up. The 10.2
scope includes PR #101 (`6e9c9630`), PR #105 (`cc313f0b`) and JDBC 24 follow-up
PR #109 (`941639a6`).

On November 12, 2026, build 456 passed all 12 integration scenarios with both
the published bundled plugin and the rebased 10.2 candidate, using the updated
automation image's JDBC 24.4 driver and normal authentication. This is not full
acceptance: original cluster-health faults, the exact pooled lease race, Spoon
Stop, slow COPY finalization and remote CI remain unvalidated.

## Automated results

| Plugin | Integration result | Maven duration / finish (UTC) |
| --- | --- | --- |
| Build 456 bundled | 12 passed, 0 failed, 0 skipped | 3:31 / 14:43:43 |
| Rebased 10.2 candidate | 12 passed, 0 failed, 0 skipped | 4:22 / 14:48:25 |

| Provenance | Value |
| --- | --- |
| PDI archive | `pdi-ee-client-10.2.0.11-456.zip`; catalog SHA-1 `f78d41b07fdef3dffd9bf40d174a88dae3702742`; SHA-256 `5f9e4ecdf5d1b7fbfde8b58fdb46c38021c5a8f26d52764846045f7b496ab06e` |
| Bundled plugin | Vertica Bulk Loader included in the verified `10.2.0.11-456` archive |
| Candidate assembly | `vertica-bulkloader-plugin-10.2.0.0-SNAPSHOT.zip`, built from the 10.2 branch at `941639a6`; SHA-256 `30293cd34ec619c5c50445b694e0018da83687fe660d62eb95a959bdf404c9bc` |
| PDI automation image | `repo.pentaho.com/pnt-docker/automation/pdi-client:10.2.0.11`; image ID `sha256:d1aa367d5ac25fcbdf8ca168eb52fd15068fc1ea52a96289c455d95532659d5d` |
| Staged Vertica JDBC | Image-supplied `vertica-jdbc-24.4.0-0.jar`; SHA-256 `cc720c960d2280643b5c1d463a15e05a97fe9f456b30d1dfb5f16405ecbb0637`; default authentication, no JDBC override |
| Vertica | `repo.pentaho.com/docker/vertica-ce:24.1.0-0`; image ID `sha256:9248dbc3ee429827f89a96758b88d7a286762d50918e2e4bbfcda2dd18cb1cd9` |
| Runtime | Linux x86_64, Ubuntu 22.04.5, OpenJDK `17.0.21.0.3`; Docker amd64 emulation on macOS 15.8.2 ARM64 |

The resolver verified the archive against the published catalog checksum. The
archive has no Vertica JDBC JAR in `lib`, so the image driver was preserved
across archive replacement and recorded for both test classes in each run.
Each final runtime had that JAR in `lib`; no JVM
class-load trace was captured, so this is staged-driver provenance rather than
proof of the loaded class origin. The local `target/evidence/sp7583-456-bundled`
and `sp7583-456-candidate` directories contain separate reports, logs and build
provenance. Generated evidence is not committed; review logs before sharing.
The ELM file configures a license service, not an offline entitlement; valid
entitlement and service connectivity were required.

The Java 11 candidate build also passed 72 mock-based and other unit tests,
including both pooled lease cleanup orderings. These do not prove a real pooled
forced-stop race. A focused pooled recovery probe passed before the full runs;
its assertion uses the pool-creation message observed in the 10.2 runtime.

The integration suites cover 100,000-row healthy load, positional non-direct
mapping, empty input, COPY-start permission denial, server CHECK rejection,
Kitchen failure handling, repeated failure/recovery, multi-copy EOF, slow finite
input, multi-copy failure, Abort during active COPY and pooled failure/recovery
in one Kitchen JVM. The latter does not prove reuse of the same physical
connection or exercise the competing-borrower forced-stop race.

### Historical build 453 results

The build 453 tests provided partial evidence, not full acceptance. With JDBC 6
both variants passed 11 of 12 scenarios; pooled recovery failed before COPY.
With the earlier automation image's JDBC 9.2.1-0 driver both variants passed
5 of 12; six failure/Abort scenarios reported cleanup errors and pooled
recovery timed out. These are historical results, not build 456 results.

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

### Historical build 453 provenance

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
permission-denied and CHECK tests demonstrate natural failure for those
particular errors; the Kitchen failure branch checks an audit marker and
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