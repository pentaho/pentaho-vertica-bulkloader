/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
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

import static org.junit.Assert.assertEquals;

/**
 * @author Andrey Khayrutdinov
 */
public class ColumnSpecTimestampEncodingTest {

  private ColumnSpec spec;
  private ByteBuffer buffer;

  @Before
  public void setUp() throws Exception {
    buffer = ByteBuffer.allocate( 1024 );
    buffer.order( ByteOrder.LITTLE_ENDIAN );

    spec = new ColumnSpec( ColumnSpec.ConstantWidthType.TIMESTAMP );
    spec.setMainBuffer( buffer );
  }

  @Test
  public void pdi13667() throws Exception {
    // SELECT TIMESTAMPDIFF ('microsecond',('jan 1, 2000 00:00:00'), ('mar 20, 2015 01:00:00'))
    // -> 480128400000000
    assertEncodesProperly( "2015-03-20 01:00:00", 480128400000000L );
  }

  @Test
  public void dateIsEarlierThan2000() throws Exception {
    // SELECT TIMESTAMPDIFF ('microsecond',('jan 1, 2000 00:00:00'), ('feb 28, 1990 02:00:00'))
    // -> -310514400000000
    assertEncodesProperly( "1990-02-28 02:00:00", -310514400000000L );
  }

  @Test
  public void dateIsEqualToBaseDate() throws Exception {
    // SELECT TIMESTAMPDIFF ('microsecond',('jan 1, 2000 00:00:00'), ('jan 1, 2000 00:00:00'))
    // -> 0
    assertEncodesProperly( "2000-01-01 00:00:00", 0 );
  }

  @Test
  public void dateDiffersOnlyInHours() throws Exception {
    // SELECT TIMESTAMPDIFF ('microsecond',('jan 1, 2000 00:00:00'), ('jan 1, 2000 11:00:00')) "jan 1, 2000 11:00:00"
    // -> 39600000000
    assertEncodesProperly( "2000-01-01 11:00:00", 39600000000L );
  }

  private void assertEncodesProperly( String timestamp, long expectedValue ) throws Exception {
    Date date = parseDate( timestamp );
    spec.encode( new ValueMetaTimestamp(), new Timestamp( date.getTime() ) );
    buffer.flip();
    assertEquals( timestamp, expectedValue, buffer.getLong() );
  }

  private static Date parseDate( String str ) throws Exception {
    return new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).parse( str );
  }
}