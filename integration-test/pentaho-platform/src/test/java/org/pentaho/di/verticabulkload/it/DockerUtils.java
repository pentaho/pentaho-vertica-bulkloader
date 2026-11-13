package org.pentaho.di.verticabulkload.it;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Local Docker CLI adapter, following the BigData integration tests' DockerUtils approach.
 * Fabric8 owns the containers; this helper neither starts nor removes them. All output is redirected
 * to unique host files before waiting, so verbose Pan/Kitchen output cannot deadlock a pipe reader.
 */
final class DockerUtils {
  private DockerUtils() {
  }

  static final class ExecResult {
    private final int exitCode;
    private final Path log;

    private ExecResult( int exitCode, Path log ) {
      this.exitCode = exitCode;
      this.log = log;
    }

    int exitCode() {
      return exitCode;
    }

    Path log() {
      return log;
    }

    String output() throws IOException {
      return Files.readString( log, StandardCharsets.UTF_8 );
    }
  }

  static ExecResult exec( Path evidence, String label, Duration timeout, String container, String... command )
    throws IOException, InterruptedException {
    List<String> arguments = new ArrayList<>( Arrays.asList( "exec", container ) );
    arguments.addAll( Arrays.asList( command ) );
    return run( evidence, label, timeout, arguments.toArray( new String[ 0 ] ) );
  }

  static void copyToContainer( Path evidence, String container, Path source, String destination )
    throws IOException, InterruptedException {
    ExecResult result = run( evidence, "docker-cp", Duration.ofSeconds( 30 ),
      "cp", source.toString(), container + ":" + destination );
    if ( result.exitCode() != 0 ) {
      throw new IOException( "docker cp failed; evidence: " + result.log() + "\n" + result.output() );
    }
  }

  static ExecResult run( Path evidence, String label, Duration timeout, String... arguments )
    throws IOException, InterruptedException {
    Files.createDirectories( evidence );
    Path log = Files.createTempFile( evidence, label + "-", ".log" );
    List<String> command = new ArrayList<>();
    command.add( "docker" );
    command.addAll( Arrays.asList( arguments ) );
    Files.write( log.resolveSibling( log.getFileName() + ".command" ), evidenceCommand( command ),
      StandardCharsets.UTF_8 );
    Process process = new ProcessBuilder( command ).redirectErrorStream( true ).redirectOutput( log.toFile() ).start();
    boolean completed = false;
    try {
      // These commands never need interactive input (in particular, vsql uses --no-password).
      process.getOutputStream().close();
      completed = process.waitFor( timeout.toMillis(), TimeUnit.MILLISECONDS );
      if ( !completed ) {
        process.destroyForcibly();
        process.waitFor( 5, TimeUnit.SECONDS );
        throw new IOException( "Host watchdog expired after " + timeout + "; this is NOT an expected SQL failure. "
          + "The in-container watchdog bounds the remote process independently. Evidence: " + log );
      }
      return new ExecResult( process.exitValue(), log );
    } finally {
      if ( process.isAlive() ) {
        process.destroyForcibly();
      }
      Files.writeString( log.resolveSibling( log.getFileName() + ".status" ),
        completed ? "exit=" + process.exitValue() : "HOST_TIMEOUT_OR_INTERRUPTION", StandardCharsets.UTF_8 );
    }
  }

  static List<String> evidenceCommand( List<String> command ) {
    List<String> recorded = new ArrayList<>( command );
    if ( recorded.contains( "/opt/vertica/bin/vsql" ) ) {
      int sqlOption = recorded.indexOf( "-c" );
      if ( sqlOption >= 0 && sqlOption + 1 < recorded.size() ) {
        recorded.set( sqlOption + 1, "[SQL redacted]" );
      }
    }
    return recorded;
  }
}