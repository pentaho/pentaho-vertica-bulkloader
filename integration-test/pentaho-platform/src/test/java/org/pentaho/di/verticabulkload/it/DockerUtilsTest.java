package org.pentaho.di.verticabulkload.it;

import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DockerUtilsTest {
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