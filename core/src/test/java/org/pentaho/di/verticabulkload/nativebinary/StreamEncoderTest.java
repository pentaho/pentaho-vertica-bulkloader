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
import static org.junit.Assert.fail;

import java.io.PipedInputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * @author Tatsiana_Kasiankova
 * 
 */
public class StreamEncoderTest {

  private static final int NUM_ROWS_TO_BUFFER = 500;
  private static final int MAXIMUM_BUFFER_SIZE = Integer.MAX_VALUE - 8;
  private PipedInputStream inputStream = mock( PipedInputStream.class );
  private List<ColumnSpec> columns;

  @Before
  public void setUp() {
    columns = new ArrayList<>();
  }

  @Test
  public void testStreamEncoderConstructor_NoException_ByteBufferIsPositiveInt() throws Exception {
    int maxTypeLenght = 500;
    ColumnSpec cs = new ColumnSpec( ColumnSpec.VariableWidthType.VARCHAR, maxTypeLenght );
    columns.add( cs );
    try {
      StreamEncoder stEncoder = new StreamEncoder( columns, inputStream );

      long expectedBufferSize = getExpectedBufferSize( maxTypeLenght, columns.size() );

      // adding bytes for data size, since it is a varchar field
      expectedBufferSize += columns.size() * 4 * maxTypeLenght;

      assertEquals( expectedBufferSize, stEncoder.getBuffer().capacity() );
    } catch ( Exception e ) {
      fail( "There is not expected exception expected But was: " + e );
    }
  }

  @Test
  public void testCountedBufferSizeIsInt_WhenToBufferAllRowMaxSizeRequiresMoreThenInt() throws Exception {

    try {
      assertEquals( MAXIMUM_BUFFER_SIZE, getSpyStreamEncoder().countMainByteBufferSize() );
    } catch ( Exception e ) {
      fail( "There is no exception expected But was: " + e );
    }
  }

  private static long getExpectedBufferSize( int maxTypeLenght, int columnCount ) {
    return getRowMaxSize( maxTypeLenght, columnCount ) * (long) NUM_ROWS_TO_BUFFER;
  }

  private static int getRowMaxSize( int maxTypeLenght, int columnCount ) {
    return getRowHeaderSize( columnCount ) + getAllColumnsMaxSize( maxTypeLenght, columnCount );

  }

  private static int getAllColumnsMaxSize( int maxTypeLenght, int columnCount ) {
    return maxTypeLenght * columnCount;

  }

  private static int getRowHeaderSize( int colCount ) {
    return 4 + columnCountAsBytes( colCount );

  }

  private static int columnCountAsBytes( int colCount ) {
    return (int) Math.ceil( (double) colCount / 8.0d );
  }

  private static StreamEncoder getSpyStreamEncoder() throws Exception {
    int maxTypeLenght = 500;
    ColumnSpec cs = new ColumnSpec( ColumnSpec.VariableWidthType.VARCHAR, maxTypeLenght );
    List<ColumnSpec> columns = new ArrayList<>();
    columns.add( cs );
    PipedInputStream inputStream = mock( PipedInputStream.class );
    StreamEncoder stEncoder = new StreamEncoder( columns, inputStream );
    StreamEncoder stEncoderSpy = spy( stEncoder );
    when( stEncoderSpy.getRowMaxSize() ).thenReturn( Integer.MAX_VALUE + 25 );
    return stEncoderSpy;
  }

}
