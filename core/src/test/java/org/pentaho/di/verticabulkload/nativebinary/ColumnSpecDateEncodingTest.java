/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
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
