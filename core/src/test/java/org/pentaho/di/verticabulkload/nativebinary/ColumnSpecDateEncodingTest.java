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

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.row.value.ValueMetaDate;

/**
 * @author Andrey Khayrutdinov
 */
public class ColumnSpecDateEncodingTest {

  private ColumnSpec spec;
  private ByteBuffer buffer;

  @Before
  public void setUp() throws Exception {
    buffer = ByteBuffer.allocate( 1024 );
    buffer.order( ByteOrder.LITTLE_ENDIAN );

    spec = new ColumnSpec( ColumnSpec.ConstantWidthType.DATE );
    spec.setMainBuffer( buffer );
  }

  @Test
  public void dateIsGreaterThanBaseDate() throws Exception {
    // SELECT DATEDIFF('day', TO_DATE('2000-01-01','YYYY-MM-DD'), TO_DATE('2015-03-20','YYYY-MM-DD'))
    // -> 5557
    assertEncodesProperly( "2015-03-20", 5557 );
  }

  @Test
  public void dateIsEqualToBaseDate() throws Exception {
    // SELECT DATEDIFF('day', TO_DATE('2000-01-01','YYYY-MM-DD'), TO_DATE('2000-01-01','YYYY-MM-DD'))
    // -> 0
    assertEncodesProperly( "2000-01-01", 0 );
  }

  @Test
  public void dateIsLessThanBaseDate() throws Exception {
    // SELECT DATEDIFF('day', TO_DATE('2000-01-01','YYYY-MM-DD'), TO_DATE('1990-02-28','YYYY-MM-DD'))
    // -> -3594
    assertEncodesProperly( "1990-02-28", -3594 );
  }

  @Test
  public void testNullInput() throws Exception {
    spec.encode( null, "" );
    buffer.flip();
    assertEquals( 0, buffer.position() );

    buffer.clear();
    spec.encode( new ValueMetaDate(), null );
    buffer.flip();
    assertEquals( 0, buffer.position() );
  }

  private void assertEncodesProperly( String dt, long expectedValue ) throws Exception {
    spec.encode( new ValueMetaDate(), parseDate( dt ) );
    buffer.flip();
    assertEquals( dt, expectedValue, buffer.getLong() );
  }

  private static Date parseDate( String str ) throws Exception {
    return new SimpleDateFormat( "yyyy-MM-dd" ).parse( str );
  }
}
