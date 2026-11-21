package org.pentaho.di.verticabulkload.it;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DockerUtilsTest {
  /** A finished command must not trigger the diagnostic intended for slow commands. */
  @Test
  void skipsDiagnosticForFinishedProcess() throws Exception {
    Process process = new ProcessBuilder( "/bin/sh", "-c", "exit 0" ).start();
    assertThat( process.waitFor( 5, TimeUnit.SECONDS ) ).isTrue();
    AtomicInteger snapshots = new AtomicInteger();
    assertThat( DockerUtils.waitFor( process, Duration.ofSeconds( 5 ), Duration.ZERO,
      snapshots::incrementAndGet ) ).isTrue();
    assertThat( snapshots.get() ).isZero();
  }

  /** A slow command must trigger one diagnostic while it is still running. */
  @Test
  void capturesDiagnosticForRunningProcess() throws Exception {
    Process process = new ProcessBuilder( "/bin/cat" ).start();
    AtomicInteger snapshots = new AtomicInteger();
    try {
      assertThat( DockerUtils.waitFor( process, Duration.ofSeconds( 5 ), Duration.ZERO, () -> {
        assertThat( process.isAlive() ).isTrue();
        snapshots.incrementAndGet();
        process.destroyForcibly();
      } ) ).isTrue();
      assertThat( snapshots.get() ).isEqualTo( 1 );
    } finally {
      process.destroyForcibly();
    }
  }

  /** A snapshot must not turn a command that remains running past its deadline into a success. */
  @Test
  void preservesDeadlineAfterDiagnostic() throws Exception {
    Process process = new ProcessBuilder( "/bin/cat" ).start();
    AtomicInteger snapshots = new AtomicInteger();
    try {
      assertThat( DockerUtils.waitFor( process, Duration.ofNanos( 1 ), Duration.ZERO,
        snapshots::incrementAndGet ) ).isFalse();
      assertThat( snapshots.get() ).isEqualTo( 1 );
      assertThat( process.isAlive() ).isTrue();
    } finally {
      process.destroyForcibly();
    }
  }

  /** A diagnostic scheduled at the host deadline must not extend or delay that deadline. */
  @Test
  void skipsDiagnosticAtDeadline() throws Exception {
    Process process = new ProcessBuilder( "/bin/cat" ).start();
    AtomicInteger snapshots = new AtomicInteger();
    try {
      assertThat( DockerUtils.waitFor( process, Duration.ZERO, Duration.ZERO,
        snapshots::incrementAndGet ) ).isFalse();
      assertThat( snapshots.get() ).isZero();
    } finally {
      process.destroyForcibly();
    }
  }

  @Test
  void redactsSqlOnlyFromRecordedVsqlCommand() {
    List<String> command = List.of( "docker", "exec", "vertica", "/opt/vertica/bin/vsql", "-c", "sensitive SQL" );
    assertThat( DockerUtils.evidenceCommand( command ) ).containsExactly( "docker", "exec", "vertica",
      "/opt/vertica/bin/vsql", "-c", "[SQL redacted]" );
    assertThat( command.get( command.size() - 1 ) ).isEqualTo( "sensitive SQL" );
    assertThat( DockerUtils.evidenceCommand( List.of( "docker", "exec", "pdi", "bash", "-c", "echo test" ) ) )
      .containsExactly( "docker", "exec", "pdi", "bash", "-c", "echo test" );
  }

  @Test
  void allowsQuarantineWarningButRejectsOtherCleanupErrors() {
    VerticaBulkLoaderIT.assertNoCleanupError(
      "Vertica bulk-load cleanup did not stop within 5000 ms; remaining daemon threads are quarantined." );
    assertThatThrownBy( () -> VerticaBulkLoaderIT.assertNoCleanupError(
      "Unexpected error during Vertica bulk-load database cleanup." ) ).isInstanceOf( AssertionError.class );
    assertThatThrownBy( () -> VerticaBulkLoaderIT.assertNoCleanupError( "java.lang.NoClassDefFoundError" ) )
      .isInstanceOf( AssertionError.class );
    assertThatThrownBy( () -> VerticaBulkLoaderIT.assertNoCleanupError( "Exception in thread \"worker\"" ) )
      .isInstanceOf( AssertionError.class );
    VerticaBulkLoaderIT.assertNoCleanupError( "normal shutdown" );
  }
}