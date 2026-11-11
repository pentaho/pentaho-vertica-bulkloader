package org.pentaho.di.verticabulkload.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance( TestInstance.Lifecycle.PER_CLASS )
@Execution( ExecutionMode.SAME_THREAD )
@Isolated( "SP-7583 lifecycle tests share Vertica QA tables and qa_loader" )
class VerticaBulkLoaderLifecycleIT {
  private static final String PAYLOAD = "SP-7583 multi-copy";
  private final VerticaBulkLoaderIT harness = new VerticaBulkLoaderIT();

  @BeforeAll
  void setUpDatabase() throws Exception {
    harness.setUpDatabase();
  }

  @BeforeEach
  void resetDatabase( TestInfo info ) throws Exception {
    harness.resetDatabase( info );
  }

  @AfterEach
  void retainStateAndCheckSessions() throws Exception {
    harness.retainStateAndCheckSessions();
  }

  @Test
  void multipleCopiesCompleteAtEof() throws Exception {
    String output = harness.runPdi( 100_000, PAYLOAD, true, true, false, null, 3 );
    for ( int copy = 0; copy < 3; copy++ ) {
      assertThat( output ).containsPattern( "(?m)^.*SP-7583 bulk load\\." + copy
        + " - Finished processing \\(I=\\d+, O=\\d+, R=[1-9]\\d*, W=[1-9]\\d*, U=\\d+, E=0\\)" );
    }
    harness.assertRows( 100_000, PAYLOAD );
  }

  @Test
  void slowInputCompletesAtEof() throws Exception {
    String output = harness.runPdiSlowInput( 100, PAYLOAD );
    Matcher start = Pattern.compile( "(?m)^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3})"
      + " - SP-7583 slow rows\\.0 - Finished dispatching" ).matcher( output );
    Matcher finish = Pattern.compile( "(?m)^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3})"
      + " - SP-7583 slow rows\\.0 - Finished processing "
      + "\\(I=100, O=0, R=0, W=100, U=\\d+, E=0\\)" ).matcher( output );
    assertThat( start.find() ).as( "Slow input started" ).isTrue();
    assertThat( finish.find() ).as( "Slow input read every row" ).isTrue();
    DateTimeFormatter format = DateTimeFormatter.ofPattern( "yyyy-MM-dd HH:mm:ss.SSS" );
    assertThat( Duration.between( LocalDateTime.parse( start.group( 1 ), format ),
      LocalDateTime.parse( finish.group( 1 ), format ) ) ).isGreaterThan( Duration.ofSeconds( 5 ) );
    harness.assertRows( 100, PAYLOAD );
  }

  @Test
  void multipleCopiesStopAfterCopyFailure() throws Exception {
    harness.sql( "REVOKE INSERT ON public.qa_target FROM qa_loader" );
    String output = harness.runPdi( 100_000, PAYLOAD, true, true, false,
      "permission denied|insufficient privilege", 3 );
    for ( int copy = 0; copy < 3; copy++ ) {
      assertThat( output ).containsPattern( "(?m)^.*SP-7583 bulk load\\." + copy
        + " - Finished processing \\(I=\\d+, O=\\d+, R=\\d+, W=\\d+, U=\\d+, E=\\d+\\)" );
    }
    assertThat( harness.sql( "SELECT COUNT(*) FROM public.qa_target" ) ).isEqualTo( "0" );
    VerticaBulkLoaderIT.assertNoCleanupError( output );
  }

  @Test
  void pooledConnectionRecoversInSameKitchenJvm() throws Exception {
    harness.sql( "REVOKE INSERT ON public.qa_target FROM qa_loader" );
    String output = harness.runPdiPooledRecovery( 100_000, PAYLOAD );
    assertThat( output ).contains( "Pooled connection acquired for database [Vertica QA loader]" );
    assertThat( output ).contains( "SQL Error during statement execution." );
    assertThat( output.toLowerCase( Locale.ROOT ) ).containsPattern( "permission denied|insufficient privilege" );
    assertThat( output ).contains( "SP-7583 pooled recovery" );
    harness.assertRows( 100_000, PAYLOAD );
    VerticaBulkLoaderIT.assertNoCleanupError( output );
  }

  @Test
  void transformationAbortDuringCopy() throws Exception {
    String startedAt = harness.sql( "SELECT NOW()::VARCHAR" );
    String output = harness.runPdiAbort( 100_000, PAYLOAD );
    assertThat( output ).containsPattern( "(?m)^.*SP-7583 bulk load\\.0 - Finished processing "
      + "\\(I=\\d+, O=[1-9]\\d*, R=[1-9]\\d*, W=[1-9]\\d*, U=\\d+, E=\\d+\\)" );
    Matcher abortLog = Pattern.compile( "(?m)^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3})"
      + " - SP-7583 abort\\.0 - .*SP-7583 intentional abort during COPY" ).matcher( output );
    assertThat( abortLog.find() ).as( "Abort timestamp in UTC" ).isTrue();
    String abortTime = abortLog.group( 1 ).replace( ' ', 'T' ) + "Z";
    assertThat( harness.sql( "SELECT COUNT(*) FROM v_monitor.query_requests WHERE user_name='qa_loader' "
      + "AND request ILIKE '%COPY%qa_target%' AND start_timestamp >= '" + startedAt + "'::TIMESTAMPTZ "
      + "AND start_timestamp <= '" + abortTime + "'::TIMESTAMPTZ "
      + "AND (end_timestamp IS NULL OR end_timestamp >= '" + abortTime + "'::TIMESTAMPTZ)" ) )
      .as( "Vertica must be executing the COPY request when Abort fires" ).isNotEqualTo( "0" );
    assertThat( harness.sql( "SELECT COUNT(*) FROM public.qa_target" ) ).isEqualTo( "0" );
    VerticaBulkLoaderIT.assertNoCleanupError( output );
  }
}