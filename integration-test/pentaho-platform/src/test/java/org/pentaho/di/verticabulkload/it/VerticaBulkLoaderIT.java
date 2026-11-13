package org.pentaho.di.verticabulkload.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SP-7583 real native COPY lifecycle regressions, using the BigDataPluginIT execution pattern:
 * Docker CLI -> installed Pan/Kitchen -> actual JDBC COPY, followed by independent database checks.
 * No mocks, host JDBC connection, published database ports, or environment-based connection metadata.
 *
 * <p>Failsafe must run this suite serially against its Fabric8-owned pair of containers. JUnit also
 * isolates the class and disables concurrent methods because the seven scenarios share QA tables.
 * Required properties: vertica.it.pdi-container-id, vertica.it.vertica-container-id and an absolute
 * host vertica.it.evidence-dir. Fixtures are assembled beneath the PDI root's transformations/jobs.
 * Every invocation retains rendered inputs, SQL/launcher logs, exact status, and process/session state.
 * The ephemeral qa_loader password is deliberately static fixture data, never a runtime secret.
 */
@TestInstance( TestInstance.Lifecycle.PER_CLASS )
@Execution( ExecutionMode.SAME_THREAD )
@Isolated( "SP-7583 shares Vertica QA tables and qa_loader" )
class VerticaBulkLoaderIT {
  private static final System.Logger LOGGER = System.getLogger( VerticaBulkLoaderIT.class.getName() );
  private static final String PDI_ROOT = "/home/devuser/pentaho/design-tools/data-integration";
  private static final String TRANSFORMATIONS = PDI_ROOT + "/transformations/vertica_bulk_loader";
  private static final String JOBS = PDI_ROOT + "/jobs/vertica_bulk_loader";
  private static final String PAYLOAD = "SP-7583 café 日本語 | quote' \\. & <xml> \"quoted\"";
  private static final Duration SQL_TIMEOUT = Duration.ofSeconds( 30 );
  private static final Duration HOST_TIMEOUT = Duration.ofSeconds( 150 );
  private static final String WATCHDOG = "setsid timeout --signal=QUIT --kill-after=10s 120s \"$@\" & "
    + "group=$!; wait \"$group\"; result=$?; "
    + "if [ \"$result\" -eq 124 ] || [ \"$result\" -eq 137 ]; then "
    + "kill -KILL -- -\"$group\" 2>/dev/null || true; fi; exit \"$result\"";
  private String pdiContainer;
  private String verticaContainer;
  private Path suiteEvidence;
  private Path evidence;
  private String generatedTransformations;
  private String generatedJobs;

  @BeforeAll
  void setUpDatabase() throws Exception {
    pdiContainer = requiredProperty( "vertica.it.pdi-container-id" );
    verticaContainer = requiredProperty( "vertica.it.vertica-container-id" );
    Path root = Path.of( requiredProperty( "vertica.it.evidence-dir" ) );
    assertThat( root.isAbsolute() ).as( "Evidence must be an absolute HOST path" ).isTrue();
    Files.createDirectories( root );
    suiteEvidence = Files.createTempDirectory( root, "SP-7583-" );
    evidence = Files.createDirectories( suiteEvidence.resolve( "setup" ) );
    requireSuccess( DockerUtils.exec( evidence, "jdbc-provenance", SQL_TIMEOUT, pdiContainer,
      "cat", "/home/devuser/pentaho/vertica-it-driver.txt" ) );
    String runId = suiteEvidence.getFileName().toString();
    generatedTransformations = TRANSFORMATIONS + "/generated/" + runId;
    generatedJobs = JOBS + "/generated/" + runId;
    requireSuccess( DockerUtils.exec( evidence, "generated-directories", SQL_TIMEOUT, pdiContainer,
      "mkdir", "-p", generatedTransformations, generatedJobs ) );
    sql( "DROP USER IF EXISTS qa_loader CASCADE; "
      + "CREATE USER qa_loader IDENTIFIED BY 'qa_ephemeral'; "
      + "GRANT USAGE ON SCHEMA public TO qa_loader; "
      + "DROP TABLE IF EXISTS public.qa_audit CASCADE; "
      + "CREATE TABLE public.qa_audit (marker VARCHAR(100));" );
    if ( Boolean.getBoolean( "vertica.it.test-md5-auth" ) ) {
      sql( "ALTER USER qa_loader SECURITY_ALGORITHM 'MD5' IDENTIFIED BY 'qa_ephemeral'" );
    }
  }

  @BeforeEach
  void resetDatabase( TestInfo info ) throws Exception {
    evidence = Files.createTempDirectory( suiteEvidence, info.getTestMethod().orElseThrow().getName() + "-" );
    sql( "DROP TABLE IF EXISTS public.qa_target CASCADE; "
      + "CREATE TABLE public.qa_target (id INT, payload VARCHAR(512)); "
      + "GRANT SELECT, INSERT ON public.qa_target TO qa_loader; "
      + "TRUNCATE TABLE public.qa_audit;" );
  }

  @AfterEach
  void retainStateAndCheckSessions() throws Exception {
    if ( evidence != null && pdiContainer != null && verticaContainer != null ) {
      collectState( evidence );
      assertNoLoaderSessions();
    }
  }

  /** SP-7583: mapped fields, UTF-8/escaping and 100k rows must drain and commit at healthy EOF. */
  @Test
  @Tag( "SP-7583" )
  void healthyEofLargeStream() throws Exception {
    runPdi( 100_000, PAYLOAD, true, true, false, null );
    assertRows( 100_000, PAYLOAD );
  }

  /** SP-7583: default positional mapping and non-direct COPY must preserve the same native values. */
  @Test
  @Tag( "SP-7583" )
  void defaultMappingNonDirect() throws Exception {
    runPdi( 17, PAYLOAD, false, false, false, null );
    assertRows( 17, PAYLOAD );
  }

  /** SP-7583: zero upstream rows must finish cleanly without starting a blocked COPY worker. */
  @Test
  @Tag( "SP-7583" )
  void emptyInput() throws Exception {
    runPdi( 0, PAYLOAD, true, true, false, null );
    assertThat( sql( "SELECT COUNT(*) FROM public.qa_target" ) ).isEqualTo( "0" );
  }

  /** SP-7583: SELECT metadata remains permitted; COPY fails before draining a large producer pipe. */
  @Test
  @Tag( "SP-7583" )
  void copyStartPermissionDenied() throws Exception {
    sql( "REVOKE INSERT ON public.qa_target FROM qa_loader" );
    String output = runPdi( 100_000, PAYLOAD, true, true, false, "permission denied|insufficient privilege" );
    assertThat( sql( "SELECT COUNT(*) FROM public.qa_target" ) ).isEqualTo( "0" );
    assertNoCleanupError( output );
  }

  /** SP-7583: an enabled server CHECK rejects COPY data and propagates the original SQL error. */
  @Test
  @Tag( "SP-7583" )
  void serverCheckRejectionAborts() throws Exception {
    sql( "ALTER TABLE public.qa_target ADD CONSTRAINT required_payload "
      + "CHECK (payload <> 'reject-me') ENABLED" );
    String output = runPdi( 100_000, "reject-me", true, true, false, "constraint|reject" );
    assertThat( sql( "SELECT COUNT(*) FROM public.qa_target" ) ).isEqualTo( "0" );
    assertNoCleanupError( output );
  }

  /** SP-7583: Kitchen must take the red hop, commit one admin audit marker, then fail via ABORT. */
  @Test
  @Tag( "SP-7583" )
  void kitchenFailureBranch() throws Exception {
    sql( "REVOKE INSERT ON public.qa_target FROM qa_loader" );
    String output = runPdi( 100_000, PAYLOAD, true, true, true,
      "permission denied|insufficient privilege" );
    assertThat( output ).doesNotContain( "SP-7583 ERROR: COPY unexpectedly succeeded" );
    assertThat( sql( "SELECT COUNT(*) FROM public.qa_audit" ) ).isEqualTo( "1" );
    assertThat( sql( "SELECT marker FROM public.qa_audit" ) ).isEqualTo( "SP-7583:handled" );
    assertThat( sql( "SELECT COUNT(*) FROM public.qa_target" ) ).isEqualTo( "0" );
    assertNoCleanupError( output );
  }

  /** SP-7583: two real early COPY failures must close sessions and not poison a later healthy run. */
  @Test
  @Tag( "SP-7583" )
  void repeatedFailureThenRecovery() throws Exception {
    sql( "REVOKE INSERT ON public.qa_target FROM qa_loader" );
    for ( int attempt = 0; attempt < 2; attempt++ ) {
      String output = runPdi( 100_000, PAYLOAD, true, true, false, "permission denied|insufficient privilege" );
      assertThat( sql( "SELECT COUNT(*) FROM public.qa_target" ) ).isEqualTo( "0" );
      assertNoCleanupError( output );
    }
    sql( "GRANT INSERT ON public.qa_target TO qa_loader" );
    runPdi( 23, PAYLOAD, true, true, false, null );
    assertRows( 23, PAYLOAD );
  }

  private String runPdi( int rows, String payload, boolean direct, boolean mapping, boolean kitchen,
                         String expectedSqlError ) throws Exception {
    return runPdi( rows, payload, direct, mapping, kitchen, expectedSqlError, 1 );
  }

  String runPdi( int rows, String payload, boolean direct, boolean mapping, boolean kitchen,
                 String expectedSqlError, int copies ) throws Exception {
    return runPdi( rows, payload, direct, mapping, kitchen, expectedSqlError, copies, "load.ktr", false, false );
  }

  String runPdiAbort( int rows, String payload ) throws Exception {
    return runPdi( rows, payload, true, true, false, null, 1, "abort.ktr", true, false );
  }

  String runPdiSlowInput( int rows, String payload ) throws Exception {
    return runPdi( rows, payload, true, true, false, null, 1, "slow-input.ktr", false, false );
  }

  String runPdiPooledRecovery( int rows, String payload ) throws Exception {
    return runPdi( rows, payload, true, true, true, null, 1, "load.ktr", false, true );
  }

  private String runPdi( int rows, String payload, boolean direct, boolean mapping, boolean kitchen,
                         String expectedSqlError, int copies, String resource, boolean abort, boolean pooledRecovery )
    throws Exception {
    Path invocation = Files.createTempDirectory( evidence, kitchen ? "kitchen-" : "pan-" );
    String name = invocation.getFileName().toString();
    String transformation = generatedTransformations + "/" + name + ".ktr";
    Map<String, String> tokens = new LinkedHashMap<>();
    tokens.put( "ROWS", Integer.toString( rows ) );
    tokens.put( "PAYLOAD", payload );
    tokens.put( "DIRECT", direct ? "Y" : "N" );
    tokens.put( "MAPPING", mapping ? "Y" : "N" );
    tokens.put( "COPIES", Integer.toString( copies ) );
    tokens.put( "POOLING", pooledRecovery ? "Y" : "N" );
    String fifo = generatedTransformations + "/" + name + ".fifo";
    tokens.put( "FIFO", fifo );
    Path input = render( "transformations/vertica_bulk_loader/" + resource,
      invocation.resolve( "load.ktr" ), tokens );
    try {
      DockerUtils.copyToContainer( invocation, pdiContainer, input, transformation );
      String fixture = transformation;
      if ( kitchen ) {
        fixture = generatedJobs + "/" + name + ".kjb";
        String jobResource = pooledRecovery ? "pooled-recovery.kjb" : "failure-handler.kjb";
        Path job = render( "jobs/vertica_bulk_loader/" + jobResource,
          invocation.resolve( "failure-handler.kjb" ), Map.of( "TRANSFORMATION", transformation ) );
        DockerUtils.copyToContainer( invocation, pdiContainer, job, fixture );
      }
      DockerUtils.ExecResult result;
      if ( "slow-input.ktr".equals( resource ) ) {
        String script = "fifo=$1; launcher=$2; fixture=$3; rows=$4; payload=$5; "
          + "mkfifo \"$fifo\" || exit 2; "
          + "(for ((row=0; row<rows; row++)); do printf '%s,%s\\n' 7 \"$payload\"; "
          + "sleep 0.075; done > \"$fifo\") & writer=$!; "
          + "trap 'kill \"$writer\" 2>/dev/null || true; rm -f -- \"$fifo\"' EXIT; "
          + "bash \"$launcher\" \"-file=$fixture\" -level=Detailed; result=$?; "
          + "if [ \"$result\" -ne 0 ]; then exit \"$result\"; fi; wait \"$writer\" || exit 2";
        result = DockerUtils.run( invocation, "pan", HOST_TIMEOUT,
          "exec", "--env", "TZ=UTC", "--workdir", PDI_ROOT, pdiContainer,
          "bash", "-c", WATCHDOG, "--", "bash", "-c", script, "--",
          fifo, PDI_ROOT + "/pan.sh", fixture, Integer.toString( rows ), payload );
      } else {
        result = DockerUtils.run( invocation, kitchen ? "kitchen" : "pan", HOST_TIMEOUT,
          "exec", "--env", "TZ=UTC", "--workdir", PDI_ROOT, pdiContainer,
          "bash", "-c", WATCHDOG, "--", "bash",
          PDI_ROOT + ( kitchen ? "/kitchen.sh" : "/pan.sh" ), "-file=" + fixture, "-level=Detailed" );
      }
      String output = result.output();
      assertThat( result.exitCode() ).as( "Natural PDI exit; 124/137/timeouts are failures. Evidence: %s%n%s",
        result.log(), output ).isEqualTo( expectedSqlError == null && !abort ? 0 : 1 );
      if ( abort ) {
        assertThat( output ).contains( "SP-7583 intentional abort during COPY" );
      }
      if ( expectedSqlError != null ) {
        // A generic Pan failure or producer-side IOException alone does not prove SQL propagation.
        assertThat( output ).contains( "SQL Error during statement execution." );
        assertThat( output ).containsPattern( "(?im)^.*SP-7583 bulk load.*SQL Error during statement execution\\." );
        assertThat( output.toLowerCase( Locale.ROOT ) ).containsPattern( expectedSqlError );
      }
      assertNoLoaderSessions();
      return output;
    } finally {
      // This runs for assertion failures and host timeouts as well as natural launcher exits.
      collectState( invocation );
    }
  }

  void assertRows( int count, String payload ) throws Exception {
    assertThat( sql( "SELECT COUNT(*) FROM public.qa_target" ) ).isEqualTo( Integer.toString( count ) );
    assertThat( sql( "SELECT COUNT(*) FROM public.qa_target WHERE id=7 AND payload="
      + sqlLiteral( payload ) ) ).isEqualTo( Integer.toString( count ) );
    assertThat( sql( "SELECT COUNT(DISTINCT payload) FROM public.qa_target" ) ).isEqualTo( "1" );
    assertThat( sql( "SELECT MIN(payload) FROM public.qa_target" ) ).isEqualTo( payload );
  }

  static void assertNoCleanupError( String output ) {
    assertThat( output ).doesNotContain( "Unexpected error during Vertica bulk-load database cleanup.",
      "java.lang.NoClassDefFoundError", "Exception in thread \"" );
  }

  private void assertNoLoaderSessions() throws Exception {
    assertThat( sql( "SELECT COUNT(*) FROM v_monitor.sessions WHERE user_name='qa_loader'" ) )
      .as( "PDI must close every qa_loader session without forced database cleanup" ).isEqualTo( "0" );
  }

  String sql( String statement ) throws Exception {
    DockerUtils.ExecResult result = DockerUtils.exec( evidence, "vsql", SQL_TIMEOUT, verticaContainer,
      "/opt/vertica/bin/vsql", "-X", "-q", "-A", "-t", "-w", "-U", "dbadmin", "-d", "vmart", "-p", "5433",
      "-v", "ON_ERROR_STOP=1", "-c", statement );
    requireSuccess( result );
    return result.output().strip();
  }

  private void collectState( Path destination ) {
    // Best effort diagnostics must never mask the assertion/timeout that caused collection.
    try {
      DockerUtils.exec( destination, "pdi-processes", Duration.ofSeconds( 15 ), pdiContainer,
        "ps", "-eo", "pid,ppid,stat,etime,args" );
    } catch ( IOException e ) {
      recordDiagnosticFailure( destination, "process-state-error-", e );
    } catch ( InterruptedException e ) {
      Thread.currentThread().interrupt();
      return;
    }
    try {
      DockerUtils.exec( destination, "loader-sessions", Duration.ofSeconds( 15 ), verticaContainer,
        "/opt/vertica/bin/vsql", "-X", "-w", "-U", "dbadmin", "-d", "vmart", "-p", "5433",
        "-v", "ON_ERROR_STOP=1", "-c", "SELECT * FROM v_monitor.sessions WHERE user_name='qa_loader'" );
    } catch ( IOException e ) {
      recordDiagnosticFailure( destination, "session-state-error-", e );
    } catch ( InterruptedException e ) {
      Thread.currentThread().interrupt();
    }
  }

  private static void recordDiagnosticFailure( Path destination, String prefix, IOException failure ) {
    try {
      Files.writeString( Files.createTempFile( destination, prefix, ".log" ), failure.toString() );
    } catch ( IOException recordingFailure ) {
      failure.addSuppressed( recordingFailure );
      LOGGER.log( System.Logger.Level.WARNING, "Could not retain diagnostic evidence in " + destination, failure );
    }
  }

  private static Path render( String resource, Path destination, Map<String, String> tokens ) throws IOException {
    String xml;
    try ( InputStream input = VerticaBulkLoaderIT.class.getClassLoader().getResourceAsStream( resource ) ) {
      if ( input == null ) {
        throw new IOException( "Missing integration fixture: " + resource );
      }
      xml = new String( input.readAllBytes(), StandardCharsets.UTF_8 );
    }
    for ( Map.Entry<String, String> token : tokens.entrySet() ) {
      xml = xml.replace( "@@" + token.getKey() + "@@", xmlEscape( token.getValue() ) );
    }
    assertThat( xml ).as( "All fixture tokens must be rendered before docker cp: %s", resource )
      .doesNotContain( "@@", "${" );
    return Files.writeString( destination, xml, StandardCharsets.UTF_8 );
  }

  private static String xmlEscape( String value ) {
    return value.replace( "&", "&amp;" ).replace( "<", "&lt;" ).replace( ">", "&gt;" )
      .replace( "\"", "&quot;" ).replace( "'", "&apos;" );
  }

  private static String sqlLiteral( String value ) {
    // E literals make backslash handling explicit, independent of standard_conforming_strings.
    return "E'" + value.replace( "\\", "\\\\" ).replace( "'", "''" ) + "'";
  }

  private static String requiredProperty( String name ) {
    String value = System.getProperty( name );
    assertThat( value ).as( "Required Failsafe system property %s", name ).isNotBlank().doesNotStartWith( "${" );
    return value;
  }

  private static void requireSuccess( DockerUtils.ExecResult result ) throws IOException {
    assertThat( result.exitCode() ).as( "Docker/SQL command failed; evidence: %s%n%s", result.log(), result.output() )
      .isZero();
  }
}