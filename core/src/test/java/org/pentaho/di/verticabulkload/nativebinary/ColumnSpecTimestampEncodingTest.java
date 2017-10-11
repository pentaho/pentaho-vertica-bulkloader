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