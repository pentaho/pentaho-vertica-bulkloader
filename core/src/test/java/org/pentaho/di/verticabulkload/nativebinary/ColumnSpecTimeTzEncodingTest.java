/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.di.verticabulkload.nativebinary;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.row.value.ValueMetaTimestamp;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static java.util.concurrent.TimeUnit.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Andrey Khayrutdinov
 */
public class ColumnSpecTimeTzEncodingTest {

  private static final long TODAY_MILLIS = 0;

  private ColumnSpec spec;
  private ByteBuffer buffer;

  @Before
  public void setUp() throws Exception {
    buffer = ByteBuffer.allocate( 1024 );
    buffer.order( ByteOrder.LITTLE_ENDIAN );

    spec = new ColumnSpec( ColumnSpec.ConstantWidthType.TIMETZ );
    spec.setMainBuffer( buffer );
  }

  @Test
  public void timeInUtc() throws Exception {
    testTimeIn( "UTC" );
  }

  @Test
  public void timeInUtcPlus12() throws Exception {
    testTimeIn( "Asia/Anadyr" );
  }

  @Test
  public void timeInUtcPlus14() throws Exception {
    testTimeIn( "Pacific/Apia" );
  }

  @Test
  public void timeInUtcMinus10() throws Exception {
    testTimeIn( "US/Hawaii" );
  }


  private void testTimeIn( String tzCode ) throws Exception {
    TimeZone current = TimeZone.getDefault();
    try {
      TimeZone tz = TimeZone.getTimeZone( tzCode );
      assertNotNull( tzCode, tz );
      TimeZone.setDefault( tz );
      assertEncodesProperly( "00:00:00", encodeValue( 0, 0, 0, tz ) );
    } finally {
      TimeZone.setDefault( current );
    }
  }

  private void assertEncodesProperly( String timestamp, long expectedValue ) throws Exception {
    Date date = parseTime( timestamp );
    spec.encode( new ValueMetaTimestamp(), new Timestamp( date.getTime() ) );
    buffer.flip();

    long actual = buffer.getLong();
    if ( actual != expectedValue ) {
      long actualUtc = toUtc( actual );
      long expectedUtc = toUtc( expectedValue );
      assertEquals( timestamp, expectedUtc, actualUtc );
    }
  }

  private static long encodeValue( int hour, int minute, int second, TimeZone tz ) {
    long upper40 = HOURS.toMicros( hour ) + MINUTES.toMicros( minute ) + SECONDS.toMicros( second );
    long lower24 = HOURS.toSeconds( 24 ) + MILLISECONDS.toSeconds( tz.getOffset( TODAY_MILLIS ) );
    return ( upper40 << 24 ) + lower24;
  }

  private static long toUtc( long value ) {
    long lower24 = value & 0xFFFFFF;
    long upper40 = value >> 24;

    int deltaHours = (int) SECONDS.toHours( lower24 - 24 * 3600 );
    int hour = (int) MICROSECONDS.toHours( upper40 );
    upper40 -= HOURS.toMicros( hour );
    hour -= deltaHours;
    if ( hour < 0 ) {
      hour += 24;
    }

    int minute = (int) MICROSECONDS.toMinutes( upper40 );
    upper40 -= MINUTES.toMicros( minute );

    int second = (int) MICROSECONDS.toSeconds( upper40 );

    return encodeValue( hour, minute, second, TimeZone.getTimeZone( "UTC" ) );
  }

  private static Date parseTime( String str ) throws Exception {
    return new SimpleDateFormat( "HH:mm:ss" ).parse( str );
  }
}
