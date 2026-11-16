package org.pentaho.di.verticabulkload.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance( TestInstance.Lifecycle.PER_CLASS )
@Execution( ExecutionMode.SAME_THREAD )
@Isolated( "PDI-20182 shares Vertica QA tables and qa_loader" )
class VerticaBulkLoaderLongTypesIT {
  private final VerticaBulkLoaderIT harness = new VerticaBulkLoaderIT();

  /** Initializes the isolated database and evidence directory for this suite. */
  @BeforeAll
  void setUpDatabase() throws Exception {
    harness.setUpDatabase( "PDI-20182-" );
  }

  /** Recreates the source and target tables with long text and binary columns. */
  @BeforeEach
  void resetDatabase( TestInfo info ) throws Exception {
    harness.resetDatabase( info );
    harness.sql( "DROP TABLE IF EXISTS public.qa_long_source CASCADE; "
      + "DROP TABLE public.qa_target CASCADE; "
      + "CREATE TABLE public.qa_long_source (source_id INT, source_text LONG VARCHAR(66000), "
      + "source_binary LONG VARBINARY(66000)); "
      + "CREATE TABLE public.qa_target (id INT, payload_text LONG VARCHAR(66000), "
      + "payload_binary LONG VARBINARY(66000)); "
      + "GRANT SELECT ON public.qa_long_source TO qa_loader; "
      + "GRANT SELECT, INSERT ON public.qa_target TO qa_loader" );
  }

  /** Retains diagnostics and verifies that the loader left no database sessions behind. */
  @AfterEach
  void retainStateAndCheckSessions() throws Exception {
    harness.retainStateAndCheckSessions();
  }

  /** Loads small UTF-8 text and binary using explicit mapping; both values must match exactly. */
  @Test
  void mappedDirectCopiesTextAndBinary() throws Exception {
    harness.sql( "INSERT INTO public.qa_long_source VALUES (1, 'text-' || CHR(233), HEX_TO_BINARY('0080ff')); COMMIT" );
    assertThat( harness.sql( "SELECT COUNT(*) FROM public.qa_long_source" ) ).isEqualTo( "1" );
    String output = harness.runLongTypes( true, true );
    VerticaBulkLoaderIT.assertNoCleanupError( output );
    assertThat( harness.sql( "SELECT COUNT(*) FROM public.qa_target" ) ).isEqualTo( "1" );
    assertThat( harness.sql( "SELECT SHA256(payload_text::VARCHAR(65000)), "
      + "SHA256(payload_binary::VARBINARY(65000)) FROM public.qa_target WHERE id=1" ) )
      .isEqualTo( sha256( "text-\u00e9".getBytes( StandardCharsets.UTF_8 ) ) + "|"
        + sha256( new byte[] { 0, (byte) 0x80, (byte) 0xff } ) );
  }

  /** Loads long text alongside ordinary binary; both target values must match the source. */
  @Test
  void longVarcharWithOrdinaryVarbinary() throws Exception {
    replaceTarget( "LONG VARCHAR(66000)", "VARBINARY(65000)" );
    assertSmallRoundTrip();
  }

  /** Loads long binary alongside ordinary text; both target values must match the source. */
  @Test
  void longVarbinaryWithOrdinaryVarchar() throws Exception {
    replaceTarget( "VARCHAR(65000)", "LONG VARBINARY(66000)" );
    assertSmallRoundTrip();
  }

  /** Loads into a default-width long text column; the text and ordinary binary value must both match. */
  @Test
  void defaultWidthLongVarchar() throws Exception {
    replaceTarget( "LONG VARCHAR", "VARBINARY(65000)" );
    assertSmallRoundTrip();
  }

  /** Loads into a default-width long binary column; the binary and ordinary text value must both match. */
  @Test
  void defaultWidthLongVarbinary() throws Exception {
    replaceTarget( "VARCHAR(65000)", "LONG VARBINARY" );
    assertSmallRoundTrip();
  }

  /** Loads 32,000,000 bytes into maximum-width long text; every chunk and the length must match. */
  @Test
  @EnabledIfSystemProperty( named = "vertica.it.run-wide-tests", matches = "true" )
  void maximumWidthLongVarchar() throws Exception {
    assertMaximumWidthValue( true );
  }

  /** Loads 32,000,000 bytes into maximum-width long binary; every chunk and the length must match. */
  @Test
  @EnabledIfSystemProperty( named = "vertica.it.run-wide-tests", matches = "true" )
  void maximumWidthLongVarbinary() throws Exception {
    assertMaximumWidthValue( false );
  }

  /** Generates and verifies every 32,000-byte chunk of a maximum-width text or binary value. */
  private void assertMaximumWidthValue( boolean text ) throws Exception {
    String textType = text ? "LONG VARCHAR(32000000)" : "VARCHAR(65000)";
    String binaryType = text ? "VARBINARY(65000)" : "LONG VARBINARY(32000000)";
    replaceTarget( textType, binaryType );
    harness.sql( "DROP TABLE public.qa_long_source CASCADE; CREATE TABLE public.qa_long_source "
      + "(source_id INT, source_text " + textType + ", source_binary " + binaryType + "); "
      + "GRANT SELECT ON public.qa_long_source TO qa_loader; "
      + "INSERT INTO public.qa_long_source VALUES (1, "
      + ( text ? "REPEAT('a'::LONG VARCHAR(32000000), 32000000)" : "'small'" ) + ", "
      + ( text ? "HEX_TO_BINARY('01')" : "REPEAT(HEX_TO_BINARY('80')::LONG VARBINARY(32000000), 32000000)" )
      + "); COMMIT" );
    String column = text ? "payload_text" : "payload_binary";
    String cast = text ? "VARCHAR(65000)" : "VARBINARY(65000)";
    byte[] expected = new byte[ 32000 ];
    Arrays.fill( expected, text ? (byte) 'a' : (byte) 0x80 );
    VerticaBulkLoaderIT.assertNoCleanupError( harness.runLongTypes( true, true ) );
    assertThat( harness.sql( "SELECT COUNT(*), MAX(LENGTH(" + column + ")) FROM public.qa_target" ) )
      .isEqualTo( "1|32000000" );
    assertThat( harness.sql( "WITH positions AS (SELECT ROW_NUMBER() OVER() AS position "
      + "FROM v_catalog.columns first_columns CROSS JOIN v_catalog.columns second_columns LIMIT 1000) "
      + "SELECT COUNT(*) FROM public.qa_target CROSS JOIN positions WHERE SHA256(SUBSTR(" + column
      + ", (position - 1) * 32000 + 1, 32000)::" + cast + ") = '" + sha256( expected ) + "'" ) )
      .isEqualTo( "1000" );
  }

  /** Loads text and binary at 64,999, 65,000 and 65,001 bytes; every value and length must match. */
  @Test
  void copiesValuesAroundShortTypeLimit() throws Exception {
    for ( int size = 64999; size <= 65001; size++ ) {
      harness.sql( "INSERT INTO public.qa_long_source VALUES (" + size + ", "
        + "REPEAT('a'::LONG VARCHAR(66000), " + ( size - 2 ) + ") || CHR(233), "
        + "REPEAT(HEX_TO_BINARY('80')::LONG VARBINARY(66000), " + size + ")); COMMIT" );
    }
    assertThat( harness.sql( "SELECT COUNT(*) FROM public.qa_long_source" ) ).isEqualTo( "3" );
    VerticaBulkLoaderIT.assertNoCleanupError( harness.runLongTypes( true, true ) );
    assertThat( harness.sql( "SELECT COUNT(*) FROM public.qa_target" ) ).isEqualTo( "3" );
    for ( int size = 64999; size <= 65001; size++ ) {
      byte[] binary = new byte[ size ];
      Arrays.fill( binary, (byte) 0x80 );
      assertLongValues( size, "a".repeat( size - 2 ) + "\u00e9", binary );
    }
  }

  /** Loads long UTF-8 text and patterned binary beyond 65,000 bytes; every chunk and length must match. */
  @Test
  void mappedDirectCopiesValuesBeyondShortTypeLimit() throws Exception {
    harness.sql( "INSERT INTO public.qa_long_source VALUES (2, "
      + "REPEAT('a'::LONG VARCHAR(66000), 65001) || CHR(233), "
      + "REPEAT(HEX_TO_BINARY('0080ff')::LONG VARBINARY(66000), 21667)); COMMIT" );
    assertThat( harness.sql( "SELECT COUNT(*) FROM public.qa_long_source" ) ).isEqualTo( "1" );
    String output = harness.runLongTypes( true, true );
    VerticaBulkLoaderIT.assertNoCleanupError( output );
    assertThat( harness.sql( "SELECT COUNT(*) FROM public.qa_target WHERE id=2" ) ).isEqualTo( "1" );
    String text = "a".repeat( 65001 ) + "\u00e9";
    byte[] binary = new byte[ 65001 ];
    for ( int position = 0; position < binary.length; position += 3 ) {
      binary[ position ] = 0;
      binary[ position + 1 ] = (byte) 0x80;
      binary[ position + 2 ] = (byte) 0xff;
    }
    assertLongValues( 2, text, binary );
  }

  /** Compares the target's text and binary chunks and lengths with independently generated values. */
  private void assertLongValues( int id, String text, byte[] binary ) throws Exception {
    for ( int offset = 0; offset < text.length(); offset += 32000 ) {
      String chunk = text.substring( offset, Math.min( offset + 32000, text.length() ) );
      assertThat( harness.sql( "SELECT SHA256(SUBSTR(payload_text, " + ( offset + 1 )
        + ", 32000)::VARCHAR(65000)) FROM public.qa_target WHERE id=" + id ) )
        .isEqualTo( sha256( chunk.getBytes( StandardCharsets.UTF_8 ) ) );
    }
    for ( int offset = 0; offset < binary.length; offset += 32000 ) {
      byte[] chunk = Arrays.copyOfRange( binary, offset, Math.min( offset + 32000, binary.length ) );
      assertThat( harness.sql( "SELECT SHA256(SUBSTR(payload_binary, " + ( offset + 1 )
        + ", 32000)::VARBINARY(65000)) FROM public.qa_target WHERE id=" + id ) ).isEqualTo( sha256( chunk ) );
    }
    assertThat( harness.sql( "SELECT LENGTH(payload_text), LENGTH(payload_binary) FROM public.qa_target WHERE id=" + id ) )
      .isEqualTo( text.length() + "|" + binary.length );
  }

  /** Loads null and empty long values without direct or explicit mapping; the two states must remain distinct. */
  @Test
  void positionalNonDirectCopiesNullAndEmptyValues() throws Exception {
    harness.sql( "INSERT INTO public.qa_long_source VALUES (1, NULL, NULL); "
      + "INSERT INTO public.qa_long_source VALUES (2, '', HEX_TO_BINARY('')); COMMIT" );
    assertThat( harness.sql( "SELECT COUNT(*) FROM public.qa_long_source" ) ).isEqualTo( "2" );
    String output = harness.runLongTypes( false, false );
    VerticaBulkLoaderIT.assertNoCleanupError( output );
    assertThat( harness.sql( "SELECT COUNT(*) FROM public.qa_target WHERE id=1 "
      + "AND payload_text IS NULL AND payload_binary IS NULL" ) ).isEqualTo( "1" );
    assertThat( harness.sql( "SELECT COUNT(*) FROM public.qa_target WHERE id=2 "
      + "AND payload_text IS NOT NULL AND payload_binary IS NOT NULL "
      + "AND LENGTH(payload_text)=0 AND LENGTH(payload_binary)=0" ) ).isEqualTo( "1" );
    assertThat( harness.sql( "SELECT COUNT(*) FROM public.qa_target" ) ).isEqualTo( "2" );
  }

  /** Converts strings to UTF-8 binary with Janino; null, empty and long values must load without corruption. */
  @Test
  void janinoConvertsTextToLongBinary() throws Exception {
    harness.sql( "INSERT INTO public.qa_long_source VALUES (1, 'text-' || CHR(233), NULL); "
      + "INSERT INTO public.qa_long_source VALUES (2, NULL, NULL); "
      + "INSERT INTO public.qa_long_source VALUES (3, '', NULL); "
      + "INSERT INTO public.qa_long_source VALUES (4, REPEAT('a'::LONG VARCHAR(66000), 65001) || CHR(233), NULL); "
      + "COMMIT" );
    VerticaBulkLoaderIT.assertNoCleanupError( harness.runLongTypesJanino() );
    assertThat( harness.sql( "SELECT COUNT(*) FROM public.qa_target" ) ).isEqualTo( "4" );
    assertLongValues( 1, "text-\u00e9", "text-\u00e9".getBytes( StandardCharsets.UTF_8 ) );
    assertThat( harness.sql( "SELECT COUNT(*) FROM public.qa_target WHERE id=2 "
      + "AND payload_text IS NULL AND payload_binary IS NULL" ) ).isEqualTo( "1" );
    assertThat( harness.sql( "SELECT COUNT(*) FROM public.qa_target WHERE id=3 "
      + "AND payload_text IS NOT NULL AND payload_binary IS NOT NULL "
      + "AND LENGTH(payload_text)=0 AND LENGTH(payload_binary)=0" ) ).isEqualTo( "1" );
    String longText = "a".repeat( 65001 ) + "\u00e9";
    assertLongValues( 4, longText, longText.getBytes( StandardCharsets.UTF_8 ) );
  }

  /** Loads long text and binary through three copies; each copy must process rows without loss or corruption. */
  @Test
  void multipleCopiesPreserveLongValues() throws Exception {
    harness.sql( "INSERT INTO public.qa_long_source SELECT ROW_NUMBER() OVER(), 'multi-' || CHR(233), "
      + "HEX_TO_BINARY('0080ff') FROM v_catalog.columns first_columns CROSS JOIN v_catalog.columns second_columns "
      + "LIMIT 600; COMMIT" );
    assertThat( harness.sql( "SELECT COUNT(*) FROM public.qa_long_source" ) ).isEqualTo( "600" );
    String output = harness.runLongTypes( true, true, 3 );
    VerticaBulkLoaderIT.assertNoCleanupError( output );
    for ( int copy = 0; copy < 3; copy++ ) {
      assertThat( output ).containsPattern( "(?m)^.*Load long types\\." + copy
        + " - Finished processing \\(I=\\d+, O=\\d+, R=[1-9]\\d*, W=[1-9]\\d*, U=\\d+, E=0\\)" );
    }
    assertThat( harness.sql( "SELECT COUNT(*), COUNT(DISTINCT id) FROM public.qa_target" ) ).isEqualTo( "600|600" );
    assertThat( harness.sql( "SELECT COUNT(*) FROM public.qa_target WHERE "
      + "payload_text::VARCHAR(65000) = 'multi-' || CHR(233) "
      + "AND payload_binary::VARBINARY(65000) = HEX_TO_BINARY('0080ff')" ) ).isEqualTo( "600" );
  }

  /** Loads 1,048,575 long-column rows; every identifier and text/binary value must survive without duplication. */
  @Test
  void highVolumeLongColumnsPreserveAllRows() throws Exception {
    harness.sql( "INSERT INTO public.qa_long_source SELECT ROW_NUMBER() OVER(), 'volume-' || CHR(233), "
      + "HEX_TO_BINARY('0080ff') FROM v_catalog.columns first_columns CROSS JOIN v_catalog.columns second_columns "
      + "CROSS JOIN v_catalog.columns third_columns "
      + "LIMIT 1048575; COMMIT" );
    assertThat( harness.sql( "SELECT COUNT(*) FROM public.qa_long_source" ) ).isEqualTo( "1048575" );
    VerticaBulkLoaderIT.assertNoCleanupError( harness.runLongTypes( true, true ) );
    assertThat( harness.sql( "SELECT COUNT(*), COUNT(DISTINCT id), MIN(id), MAX(id) FROM public.qa_target" ) )
      .isEqualTo( "1048575|1048575|1|1048575" );
    assertThat( harness.sql( "SELECT COUNT(*) FROM public.qa_target WHERE "
      + "payload_text::VARCHAR(65000) = 'volume-' || CHR(233) "
      + "AND payload_binary::VARBINARY(65000) = HEX_TO_BINARY('0080ff')" ) ).isEqualTo( "1048575" );
  }

  /** Rejects over-width long text; existing rows must survive and a subsequent valid load must succeed. */
  @Test
  void overWidthTextFailsAndRecovers() throws Exception {
    assertOverWidthFailureAndRecovery( "REPEAT('a'::LONG VARCHAR(66000), 65002)", "HEX_TO_BINARY('01')" );
  }

  /** Rejects over-width long binary; existing rows must survive and a subsequent valid load must succeed. */
  @Test
  void overWidthBinaryFailsAndRecovers() throws Exception {
    assertOverWidthFailureAndRecovery( "'valid'", "REPEAT(HEX_TO_BINARY('80')::LONG VARBINARY(66000), 65002)" );
  }

  /** Verifies rejection, committed-data preservation and recovery for one over-width source expression. */
  private void assertOverWidthFailureAndRecovery( String textExpression, String binaryExpression ) throws Exception {
    replaceTarget( "LONG VARCHAR(65001)", "LONG VARBINARY(65001)" );
    harness.sql( "INSERT INTO public.qa_target VALUES (9, 'prior', HEX_TO_BINARY('01')); "
      + "INSERT INTO public.qa_long_source VALUES (1, 'valid', HEX_TO_BINARY('01')); "
      + "INSERT INTO public.qa_long_source VALUES (2, " + textExpression + ", " + binaryExpression + "); COMMIT" );
    String output = harness.runLongTypesExpectingSqlError( "too long|exceed|length|size|truncat|reject" );
    VerticaBulkLoaderIT.assertNoCleanupError( output );
    assertThat( harness.sql( "SELECT COUNT(*), MIN(id), MAX(id) FROM public.qa_target" ) ).isEqualTo( "1|9|9" );
    assertThat( harness.sql( "SELECT COUNT(*) FROM public.qa_target WHERE id=9 "
      + "AND payload_text::VARCHAR(65000)='prior' AND payload_binary::VARBINARY(65000)=HEX_TO_BINARY('01')" ) )
      .isEqualTo( "1" );
    harness.sql( "TRUNCATE TABLE public.qa_long_source; "
      + "INSERT INTO public.qa_long_source VALUES (3, 'recovered', HEX_TO_BINARY('0080ff')); COMMIT" );
    VerticaBulkLoaderIT.assertNoCleanupError( harness.runLongTypes( true, true ) );
    assertThat( harness.sql( "SELECT COUNT(*) FROM public.qa_target" ) ).isEqualTo( "2" );
    assertThat( harness.sql( "SELECT COUNT(*) FROM public.qa_target WHERE id=3 "
      + "AND payload_text::VARCHAR(65000)='recovered' "
      + "AND payload_binary::VARBINARY(65000)=HEX_TO_BINARY('0080ff')" ) ).isEqualTo( "1" );
  }

  /** Recreates the target with the requested text and binary types and loader permissions. */
  private void replaceTarget( String textType, String binaryType ) throws Exception {
    harness.sql( "DROP TABLE public.qa_target CASCADE; CREATE TABLE public.qa_target (id INT, payload_text "
      + textType + ", payload_binary " + binaryType + "); GRANT SELECT, INSERT ON public.qa_target TO qa_loader" );
  }

  /** Loads and independently verifies one small UTF-8 and binary value in the configured target. */
  private void assertSmallRoundTrip() throws Exception {
    harness.sql( "INSERT INTO public.qa_long_source VALUES (1, 'text-' || CHR(233), HEX_TO_BINARY('0080ff')); COMMIT" );
    VerticaBulkLoaderIT.assertNoCleanupError( harness.runLongTypes( true, true ) );
    assertThat( harness.sql( "SELECT COUNT(*) FROM public.qa_target WHERE id=1" ) ).isEqualTo( "1" );
    assertThat( harness.sql( "SELECT SHA256(payload_text::VARCHAR(65000)), "
      + "SHA256(payload_binary::VARBINARY(65000)) FROM public.qa_target WHERE id=1" ) )
      .isEqualTo( sha256( "text-\u00e9".getBytes( StandardCharsets.UTF_8 ) ) + "|"
        + sha256( new byte[] { 0, (byte) 0x80, (byte) 0xff } ) );
  }

  /** Returns the SHA-256 digest of the supplied bytes as lowercase hexadecimal. */
  private static String sha256( byte[] value ) throws Exception {
    StringBuilder digest = new StringBuilder();
    for ( byte octet : MessageDigest.getInstance( "SHA-256" ).digest( value ) ) {
      digest.append( String.format( "%02x", octet & 0xff ) );
    }
    return digest.toString();
  }
}