# SP-7772 QA validation summary

## Scope

[SP-7772](https://pentaho-eng.atlassian.net/browse/SP-7772) is the 11.0
backport of PDI-20802, which addresses Vertica Bulk Loader hangs after certain
Vertica SQL exceptions. The test target is PDI 11.0.0.4 with the plugin built
from the 11.0 branch, as well as the plugin bundled with the tested PDI build.
The branch tip is `3718dcbd` ([PR #106](https://github.com/pentaho/pentaho-vertica-bulkloader/pull/106)).

## Results

On November 10, 2026, both suites were run locally against PDI
`11.0.0.4-328`, Vertica CE `24.1.0-0`, and the supplied JDBC `06.00.0000`
driver. The disposable `qa_loader` user was explicitly configured for MD5
authentication, because this JDBC driver cannot authenticate using the test
server's default SHA-512. MD5 is weak and this setting is for isolated test
databases only, never a production recommendation.

| Plugin under test | Result | Duration / finish (UTC) |
| --- | --- | --- |
| 11.0 branch candidate at `3718dcbd` | 11 of 12 integration tests passed; pooled recovery failed | 4:33 / 14:12:06 |
| Plugin bundled with `11.0.0.4-328` | 9 of 12 integration tests passed; permission failure, Abort and pooled recovery failed | 6:28 / 14:19:05 |

The 11.0 branch build also passed 68 unit tests with no failures, errors or skips.
These unit tests are separate from the real integration evidence. The two plugin
core JARs have different hashes; the bundled result does not establish whether
that published build contains the backport. The PDI-20802 results from 11.1
must not be interpreted as results for 11.0.0.4.

| Real integration scenario | 11.0 branch candidate | 11.0.0.4-328 bundled |
| --- | --- | --- |
| Healthy 100,000-row load | Pass | Pass |
| Non-direct default mapping | Pass | Pass |
| Empty input | Pass | Pass |
| COPY-start permission denied | Pass | Fail: watchdog timeout |
| Server CHECK rejection | Pass | Pass |
| Kitchen failure branch | Pass | Pass |
| Repeated failure then recovery | Pass | Pass |
| Multiple copies complete at EOF | Pass | Pass |
| Slow input completes at EOF | Pass | Pass |
| Multiple copies stop after COPY failure | Pass | Pass |
| Transformation Abort during active COPY | Pass | Fail: uncaught worker exception |
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

### Permission denial: bundled plugin

Vertica reported `[Vertica][VJDBC](4367) ERROR: Permission denied for relation
qa_target`, but Pan did not terminate naturally. The test watchdog terminated
the process group after 120 seconds; the observed exit code was `124`, not the
expected `1`. The thread dump includes `VerticaBulkLoaderData.close` and
`VerticaBulkLoader.processRow`. The same test passed with the 11.0 branch
candidate. A watchdog kill is a failure, not evidence of successful cleanup.

### Abort during COPY: bundled plugin

The test confirmed that COPY was active when the transformation Abort fired.
Pan terminated, but its log contained an uncaught worker exception, violating
the strict clean-log assertion:

```text
Exception in thread "pool-4-thread-1" java.lang.NullPointerException: Cannot invoke "String.getBytes(String)" because "<parameter1>" is null
	at com.vertica.util.ProtocolUtils.encodeUTF8(Unknown Source)
	at com.vertica.io.CopyErrorRequestMessage.send(Unknown Source)
	...
	at com.vertica.jdbc.VerticaCopyStream.execute(Unknown Source)
	at org.pentaho.di.verticabulkload.VerticaBulkLoader$1.run(VerticaBulkLoader.java:336)
```

The corresponding candidate test passed. This observation does not by itself
establish the exact root cause of the exception or prove when a fix entered a
published build.

## Provenance and evidence

| Item | Tested value |
| --- | --- |
| PDI archive | `11.0.0.4-328`; catalog SHA-1 `65295dc3817009673f8776abaf1b3fe23f497dab`; SHA-256 `73ddd1888b557998a9685d8d23fbc2b5a6b18f8fdc984eb829db6f9a950deca3` |
| PDI automation image | `repo.pentaho.com/pnt-docker/automation/pdi-client:11.0`, image ID `sha256:42b6923ed529001b05f4610e542b8bf714660aa07b30c86b11db2c238425b517` |
| JDBC 6 | SHA-256 `3fea0ca1ead071d3a1ce022232d19c8200e889b2b657575fa1b9b551a11dd224`, confirmed inside the PDI containers |
| Candidate assembly | `vertica-bulkloader-plugin-11.0.0.0-SNAPSHOT.zip`, SHA-256 `8d62a94e0801c964cb4d2e2993516e249d68b47b611b2378430e245783fda88c` |
| Candidate core JAR | SHA-256 `3e50dd610cf146ac6e1a0de76b4be3dd568522ced7f6dabc47c3a4128850d424` |
| Bundled core JAR | SHA-256 `dc98ca40c33b42c3d96918a7f1e18683b87d41f9d1add33e79cb7a2049b345c4` |
| Vertica | `repo.pentaho.com/docker/vertica-ce:24.1.0-0` |

The local platform module's `target/evidence/sp7772-1104-local-1110` and
`target/evidence/sp7772-1104-bundled-1110` directories retain separate Failsafe
reports, build labels, logs, driver provenance and diagnostics. They are local
generated artifacts, not committed certification; review logs before sharing.
The resolver records catalog provenance in `target/runtime`. The ELM file is
a license-service configuration, not an offline entitlement; service access
and a valid entitlement were required.

## Coverage and limitations

The real Pan/Kitchen and Vertica integration suites cover healthy and empty
loads, SQL failures and recovery, multi-copy execution, slow input, transformation
Abort and pooled recovery. The candidate's permission-denied and CHECK-rejection
cases demonstrate natural failure for those specific SQL errors, and its Kitchen
failure-branch case verifies an audit marker and propagated job failure. The
bundled permission-denied case instead hung until the watchdog intervened.

SP-7772's acceptance criteria require the step to terminate and report failure,
the transformation not to hang or leave active threads, and job error handling
to run without manual termination for Vertica SQL exceptions. The passing proxy
cases provide partial evidence, not proof for every SQL exception or pooled
connection. Original mergeout-queue, disk-space and broken-projection failures,
Spoon Stop, slow COPY finalization, the exact pooled-wrapper lease race and
remote CI require separate validation. A timeout or forced termination is a
failure. See the [execution guide](../integration-tests-README.md) for setup and
evidence details.