# PDI-20802 live KTR probes

The original failure matrix targets a disposable Vertica instance at `localhost:5433`, database `vmart`, with the
passwordless `dbadmin` test account. The exact mergeout queue-bloat probe uses the separate Vertica 24.4 setup
documented below at `localhost:5544`, with user `998` and password `vertica`. Do not use either setup outside an
isolated local environment.

Prepare the probe objects:

```powershell
Get-Content .\setup.sql |
  docker exec -i pdi20802-vertica-live /opt/vertica/bin/vsql `
    -X -v ON_ERROR_STOP=1 -U dbadmin -d vmart -f -
```

Run a transformation with PDI 10.2:

```powershell
& "$env:PDI_HOME\Pan.bat" "/file:$PWD\pdi20802_success.ktr" /level:Basic
```

| Transformation | Expected result |
| --- | --- |
| `pdi20802_success.ktr` | Exit 0; 10,000 rows in `pdi20802_success`. |
| `pdi20802_missing_table.ktr` | Exit 1 promptly; `pdi20802_missing` must remain absent. |
| `pdi20802_permission_denied.ktr` | Exit 1 at COPY authorization; target count is unchanged. |
| `pdi20802_unsupported_type.ktr` | Exit 1 with `INTERVAL YEAR TO MONTH not supported`. |
| `pdi20802_row_rejection_abort.ktr` | Exit 1; oversized first value aborts COPY; zero rows. |
| `pdi20802_row_rejection_continue.ktr` | Exit 0; all 100 oversized values are rejected; zero rows. |
| `pdi20802_manual_abort.ktr` | Exit 1 after row 50,001; COPY is interrupted; zero rows. |
| `pdi20802_connection_loss.ktr` | Keep running until Vertica is terminated during COPY, then exit 1 promptly. |
| `pdi20802_mergeout_queue_bloat.ktr` | Against Vertica 24.4, exit 0 after 128 concurrent COPY streams and 131,072 committed rows; the server log must show `MERGEOUT QUEUE BLOAT` detection and resolution. |

There are two different reproductions in this directory. `pdi20802_mergeout_queue_bloat.ktr` is a successful server
condition probe: its expected result is Pan exit `0`, with the Health Watchdog detecting and resolving mergeout queue
bloat. It was not used as the before/after hang test. The test that failed before the change is
`pdi20802_permission_denied.ktr`: with the untouched vendor JAR, Vertica error 4367 was logged but Pan was still
running after the 30-second watchdog; with the fixed JAR, the same KTR returned Pan status `1` 985 ms after the error.
The mergeout KTR was also run with the untouched vendor JAR after truncating its target. It passed in about three
seconds, committed all 131,072 rows, and produced the same Health Watchdog detection/resolution events. Therefore,
the mergeout KTR is not the test that fails before the change; it is a successful server-condition probe with both
plugin versions.

The source-level regression is `shouldNotJoinTheWorkerWhenCopyFailureStopsTheTransformation` in
`core/src/test/java/org/pentaho/di/verticabulkload/VerticaBulkLoaderTest.java`. Applied to the baseline source at
`80d03845`, it failed after 2.408 seconds because the worker attempted to join itself. The same test passes with the
fixed source. This is the direct unit-test proof that the shutdown change is required.

For the connection-loss probe, start Pan and wait for both field-mapping log lines before running:

```powershell
docker kill pdi20802-vertica-live
```

Restart the container afterward and verify `public.pdi20802_large` contains zero rows.

## Exact Vertica 24.4 mergeout queue-bloat probe

Vertica 24.1 is retained for the failure A/B tests above. It does not expose the `MergeoutBlockParameter`
capability needed for this reproduction. The exact server condition was reproduced with this disposable image:

| Setting | Value |
| --- | --- |
| Image | `opentext/vertica-k8s:24.4.0-1-multiarch` |
| Container | `vertica_24_4_pdi20802` |
| Host ports | `5544 -> 5433` for SQL; `8444 -> 8443` for embedded HTTPS |
| Database and node | `vmart`; `v_vmart_node0001` |
| Server version | `Vertica Analytic Database v24.4.0-1` |
| SQL identity | User `998`; password `vertica` |
| Catalog | `/home/dbadmin/data/vmart/v_vmart_node0001_catalog` |
| Workload settings | `MergeoutBlockParameter=1`; `MaxClientSessions=400` |

The 24.4 image is an operator/Kubernetes image rather than a ready standalone database. Start it with a persistent
data volume and publish both ports, then bootstrap the single-node database manually. The tested container metadata
was equivalent to:

```powershell
docker run --detach `
  --name vertica_24_4_pdi20802 `
  --publish 5544:5433 `
  --publish 8444:8443 `
  --volume vertica_24_4_pdi20802_data:/home/dbadmin/data `
  opentext/vertica-k8s:24.4.0-1-multiarch
```

Before bootstrap, ensure the catalog/data directory is owned by the Vertica runtime identity:

```powershell
docker exec -u 0 vertica_24_4_pdi20802 chown -R 998:996 /home/dbadmin/data
```

The database creation command used the image CA and admin certificate. Run it as `998:996`:

```powershell
docker exec -u 998:996 vertica_24_4_pdi20802 `
  /opt/vertica/bin/vcluster create_db `
  --db-name vmart `
  --hosts 127.0.0.1 `
  --catalog-path /home/dbadmin/data `
  --data-path /home/dbadmin/data `
  --password vertica `
  --ca-cert-file /opt/vertica/config/https_certs/rootca.pem `
  --cert-file /opt/vertica/config/vcluster_server/admin.pem `
  --key-file /opt/vertica/config/vcluster_server/admin.key `
  --config-param HttpServerConf=/opt/vertica/config/https_certs/httpstls.json `
  --config /opt/vertica/config/vertica_cluster.yaml `
  --force-overwrite-file `
  --force-cleanup-on-failure `
  --force-removal-at-creation `
  --skip-package-install `
  --startup-timeout 120 `
  --verbose
```

The image requires mutual TLS for the node-management agent. `httpstls.json` must contain PEM contents, not file
paths. Create a client certificate whose common name is `998`, with `clientAuth` extended key usage, signed by the
image root CA. The tested certificate paths were `/opt/vertica/config/https_certs/998.pem` and
`/opt/vertica/config/https_certs/998.key`. The following command creates the certificate inside the disposable
container. The root CA private key is used only during this local bootstrap step:

```powershell
docker exec -u 0 vertica_24_4_pdi20802 sh -lc "
  cd /opt/vertica/config/https_certs &&
  printf '[client]\nextendedKeyUsage=clientAuth\nsubjectAltName=DNS:998\n' > 998.ext &&
  openssl req -new -newkey rsa:2048 -nodes \
    -keyout 998.key -out 998.csr -subj '/CN=998' &&
  openssl x509 -req -in 998.csr \
    -CA rootca.pem -CAkey rootca.key -CAcreateserial \
    -out 998.pem -days 365 -sha256 \
    -extfile 998.ext -extensions client &&
  chown 998:996 998.key 998.csr 998.pem &&
  chmod 400 998.key
"
```

Build `httpstls.json` from the PEM contents. Passing file paths in the `key`, `certificate`, or
`ca_certificates` fields does not work with this image:

```powershell
@'
import json
from pathlib import Path

base = Path('/opt/vertica/config/https_certs')
config = {
    'name': 'server',
    'cipher_suites': '',
    'mode': 2,
    'key': (base / '998.key').read_text(),
    'certificate': (base / '998.pem').read_text(),
    'chain_certs': [],
    'ca_certificates': [(base / 'rootca.pem').read_text()],
}
(base / 'httpstls.json').write_text(json.dumps(config, separators=(',', ':')))
'@ | docker exec -i -u 0 vertica_24_4_pdi20802 python3 -
docker exec -u 0 vertica_24_4_pdi20802 chown 998:996 /opt/vertica/config/https_certs/httpstls.json
docker exec -u 0 vertica_24_4_pdi20802 chmod 600 /opt/vertica/config/https_certs/httpstls.json
```

Start the node-management agent after the TLS file exists. It listens on port `5554`; an HTTP `404` from `/` is
expected and proves that the listener is ready:

```powershell
docker exec -d -u 998:996 vertica_24_4_pdi20802 /opt/vertica/bin/node_management_agent
docker exec -u 0 vertica_24_4_pdi20802 curl `
  --cacert /opt/vertica/config/https_certs/rootca.pem `
  --cert /opt/vertica/config/https_certs/998.pem `
  --key /opt/vertica/config/https_certs/998.key `
  --max-time 5 -sS -o /dev/null -w 'NMA HTTP %{http_code}\n' https://127.0.0.1:5554/
```

Verify the Vertica node-management health endpoint with:

```powershell
docker exec -u 0 vertica_24_4_pdi20802 curl `
  --cacert /opt/vertica/config/https_certs/rootca.pem `
  --cert /opt/vertica/config/https_certs/998.pem `
  --key /opt/vertica/config/https_certs/998.key `
  --max-time 5 -sS https://127.0.0.1:8443/v1/health
```

If a failed `create_db --force-cleanup-on-failure` removes `vertica_cluster.yaml` while leaving the catalog, restart
the existing node directly instead of deleting the disposable volume:

```text
/opt/vertica/bin/vertica -D /home/dbadmin/data/vmart/v_vmart_node0001_catalog -C vmart -n v_vmart_node0001 -h 127.0.0.1 -p 5433 -P 4803 -Y ipv4
```

The legacy JDBC driver sends MD5 authentication, while Vertica 24.4 defaults to SHA512. Add narrowly scoped local
and Docker-bridge trust records for the disposable user. The TLS record is also required by the node-management
agent:

```sql
CREATE AUTHENTICATION pdi_tls METHOD 'tls' HOST TLS '127.0.0.1/32';
GRANT AUTHENTICATION pdi_tls TO "998";
CREATE AUTHENTICATION pdi_trust_local METHOD 'trust' LOCAL;
CREATE AUTHENTICATION pdi_trust_docker METHOD 'trust' HOST '172.17.0.0/16';
GRANT AUTHENTICATION pdi_trust_local TO "998";
GRANT AUTHENTICATION pdi_trust_docker TO "998";
ALTER DATABASE vmart SET MergeoutBlockParameter = 1;
ALTER DATABASE vmart SET MaxClientSessions = 400;
```

`MaxClientSessions=400` is needed because each of the 128 loader copies uses more than one database session. The
mergeout target is intentionally created separately from [`setup.sql`](setup.sql); that script resets only the
original 24.1 matrix:

```sql
CREATE TABLE public.pdi20802_mergeout (
  id INTEGER,
  payload VARCHAR(128)
);
```

Confirm the parameter and target before running:

```powershell
Write-Output "select version(), current_database(), current_user" |
  docker exec -i -u 998:996 vertica_24_4_pdi20802 vsql -h 127.0.0.1 -p 5433 -U 998 -w vertica -At
Write-Output "select get_config_parameter('MergeoutBlockParameter')" |
  docker exec -i -u 998:996 vertica_24_4_pdi20802 vsql -h 127.0.0.1 -p 5433 -U 998 -w vertica -At
```

Run from this directory with the deployed fixed plugin. The KTR uses `localhost:5544`, database `vmart`, user `998`,
password `vertica`, 128 loader copies, and 131,072 generated rows:

```powershell
$env:PDI_HOME = 'D:\Software-D\pdi-ee-client-10.2.0.9-418\data-integration'
& "$env:PDI_HOME\Pan.bat" "/file:$PWD\pdi20802_mergeout_queue_bloat.ktr" /level:Basic
$LASTEXITCODE
```

For evidence runs, launch Pan's Java process directly and keep it owned by the invoking shell; some `Pan.bat`
installations detach the Java child. The validated run used `C:\Pentaho\java\bin\java.exe` and the following
command from `data-integration`:

```powershell
$java = 'C:\Pentaho\java\bin\java.exe'
$di = 'D:\Software-D\pdi-ee-client-10.2.0.9-418\data-integration'
$work = 'D:\tickets\PDI-20802\pentaho-vertica-bulkloader\samples\pdi20802'
Set-Location $di
$javaArgs = @(
  '--add-opens=java.base/sun.net.www.protocol.jar=ALL-UNNAMED',
  '--add-opens=java.base/java.lang=ALL-UNNAMED',
  '--add-opens=java.base/java.io=ALL-UNNAMED',
  '--add-opens=java.base/java.lang.reflect=ALL-UNNAMED',
  '--add-opens=java.base/java.net=ALL-UNNAMED',
  '--add-opens=java.base/java.security=ALL-UNNAMED',
  '--add-opens=java.base/java.util=ALL-UNNAMED',
  '--add-opens=java.base/sun.net.www.protocol.file=ALL-UNNAMED',
  '--add-opens=java.base/sun.net.www.protocol.ftp=ALL-UNNAMED',
  '--add-opens=java.base/sun.net.www.protocol.http=ALL-UNNAMED',
  '--add-opens=java.base/sun.net.www.protocol.https=ALL-UNNAMED',
  '--add-opens=java.base/sun.reflect.misc=ALL-UNNAMED',
  '--add-opens=java.management/javax.management=ALL-UNNAMED',
  '--add-opens=java.management/javax.management.openmbean=ALL-UNNAMED',
  '--add-opens=java.naming/com.sun.jndi.ldap=ALL-UNNAMED',
  '--add-opens=java.base/java.math=ALL-UNNAMED',
  '--add-opens=java.base/sun.nio.ch=ALL-UNNAMED',
  '--add-opens=java.base/java.nio=ALL-UNNAMED',
  '--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED',
  '-Xms1024m', '-Xmx2048m',
  '-Djava.library.path=libswt\win64;native-lib\win64;..\native-lib\win64',
  '-Djava.locale.providers=COMPAT,SPI',
  '-DKETTLE_HOME=', '-DKETTLE_REPOSITORY=', '-DKETTLE_USER=', '-DKETTLE_PASSWORD=',
  '-DKETTLE_PLUGIN_PACKAGES=', '-DKETTLE_LOG_SIZE_LIMIT=', '-DKETTLE_JNDI_ROOT=',
  '-Dcom.google.protobuf.use_unsafe_pre22_gencode=true',
  '-Dorg.eclipse.swt.browser.DefaultType=edge',
  '-jar', 'launcher\launcher.jar',
  '-lib', '..\libswt\win64;native-lib\win64;..\native-lib\win64',
  '-main', 'org.pentaho.di.pan.Pan',
  '-initialDir', "$work\",
  '/file:pdi20802_mergeout_queue_bloat.ktr', '/level:Basic'
)
& $java @javaArgs
$LASTEXITCODE
```

For a repeatable evidence run, start Java directly, redirect both streams, and kill the process tree if it exceeds
three minutes. Starting Java directly avoids `cmd.exe` interpreting the semicolons in the Java library-path
argument. The `$javaArgs` array is the one defined above:

```powershell
$stdout = Join-Path $work 'pdi20802_mergeout_queue_bloat.stdout.log'
$stderr = Join-Path $work 'pdi20802_mergeout_queue_bloat.stderr.log'
$pan = Start-Process -FilePath $java -ArgumentList $javaArgs `
  -WorkingDirectory $di -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru
if (-not $pan.WaitForExit(180000)) {
  taskkill.exe /PID $pan.Id /T /F | Out-Null
  throw 'Pan exceeded the three-minute watchdog.'
}
$pan.Refresh()
Write-Output "Pan exit code: $($pan.ExitCode)"
Get-Content $stdout
Get-Content $stderr
```

Capture the server-side Health Watchdog lines from the catalog log, then verify the target, sessions, and process state:

```powershell
docker exec -u 998:996 vertica_24_4_pdi20802 sh -lc "
  grep -E 'Enabling Health Watchdog Mergeout Module|Detected MERGEOUT QUEUE BLOAT|Resolved MERGEOUT QUEUE BLOAT' \
    /home/dbadmin/data/vmart/v_vmart_node0001_catalog/vertica.log
"
Write-Output 'select count(*) from public.pdi20802_mergeout' |
  docker exec -i -u 998:996 vertica_24_4_pdi20802 vsql -h 127.0.0.1 -p 5433 -U 998 -w vertica -At
Write-Output 'select count(*) from v_monitor.database_connections' |
  docker exec -i -u 998:996 vertica_24_4_pdi20802 vsql -h 127.0.0.1 -p 5433 -U 998 -w vertica -At
Get-CimInstance Win32_Process |
  Where-Object { $_.CommandLine -match 'pdi20802_mergeout_queue_bloat|launcher.jar.*org.pentaho.di.pan.Pan' }
```

The final clean run returned Pan exit code `0` and logged:

```text
Pan - Finished!
Pan - Start=2026/09/11 13:17:50.380, Stop=2026/09/11 13:17:54.676
Pan - Processing ended after 4 seconds.
```

The target count was `131072`, `MergeoutBlockParameter` was `1`, active database connections returned to `0`, and no
matching Pan or Java process remained. The Vertica log recorded the decisive Health Watchdog behavior as three
detection/resolution pairs:

```text
2026-09-11 12:17:53.990 Detected MERGEOUT QUEUE BLOAT. Blocking DML/DDLs.
2026-09-11 12:17:53.999 Resolved MERGEOUT QUEUE BLOAT. Unblocking DML/DDLs.
2026-09-11 12:17:54.238 Detected MERGEOUT QUEUE BLOAT. Blocking DML/DDLs.
2026-09-11 12:17:54.480 Resolved MERGEOUT QUEUE BLOAT. Unblocking DML/DDLs.
2026-09-11 12:17:54.498 Detected MERGEOUT QUEUE BLOAT. Blocking DML/DDLs.
2026-09-11 12:17:54.641 Resolved MERGEOUT QUEUE BLOAT. Unblocking DML/DDLs.
```

The exact message text is the server-side proof. Vertica also logs `Enabling Health Watchdog Mergeout Module`, but
the queue-bloat detection and resolution messages are the decisive evidence for this workload.

After testing, remove only the disposable 24.4 container and its volume. Do not use this cleanup command while the
24.1 A/B matrix is still running:

```powershell
docker rm --force --volumes vertica_24_4_pdi20802
```