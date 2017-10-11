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
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * @author Andrey Khayrutdinov
 */
public class ColumnSpecTimestampTzEncodingTest {

  private ColumnSpec spec;
  private ByteBuffer buffer;

  private Calendar calendar;
  private long utcJan1of2000;

  @Before
  public void setUp() throws Exception {
    buffer = ByteBuffer.allocate( 1024 );
    buffer.order( ByteOrder.LITTLE_ENDIAN );

    spec = new ColumnSpec( ColumnSpec.ConstantWidthType.TIMESTAMPTZ );
    spec.setMainBuffer( buffer );

    Calendar utcCalendar = Calendar.getInstance( TimeZone.getTimeZone( "UTC" ) );
    utcCalendar.set( 2000, 0, 1, 0, 0, 0 );
    utcCalendar.set( Calendar.MILLISECOND, 0 );
    utcJan1of2000 = utcCalendar.getTimeInMillis();

    calendar = Calendar.getInstance();
    calendar.clear();
  }

  @Test
  public void dateIsGreaterThanBaseDate() throws Exception {
    calendar.set( 2015, 2, 20 );
    assertEncodesProperly( "2015-03-20 00:00:00", calendar.getTime() );
  }

  @Test
  public void dateIsEqualToBaseDate() throws Exception {
    calendar.set( 2000, 0, 1 );
    assertEncodesProperly( "2000-01-01 00:00:00", calendar.getTime() );
  }

  @Test
  public void dateIsLessThanBaseDate() throws Exception {
    calendar.set( 1990, 1, 28 );
    assertEncodesProperly( "1990-02-28 00:00:00", calendar.getTime() );
  }

  private void assertEncodesProperly( String timestamp, Date dt ) throws Exception {
    Date date = parseDate( timestamp );
    spec.encode( new ValueMetaTimestamp(), new Timestamp( date.getTime() ) );
    buffer.flip();
    long expectedValue = TimeUnit.MILLISECONDS.toMicros( dt.getTime() - utcJan1of2000 );
    assertEquals( timestamp, expectedValue, buffer.getLong() );
  }

  private static Date parseDate( String str ) throws Exception {
    return new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ).parse( str );
  }
}
