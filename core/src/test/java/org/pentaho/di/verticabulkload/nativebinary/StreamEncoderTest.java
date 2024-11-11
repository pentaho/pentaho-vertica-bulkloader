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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

import java.io.PipedInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaString;

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
  public void testStreamEncoderGoodInputType() {

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "float_column" ) );
    Object[] goodObjectData = { "124" };

    ColumnSpec columnSpec = new ColumnSpec( ColumnSpec.ConstantWidthType.INTEGER_64 );
    columns.add( columnSpec );

    try {
      StreamEncoder streamEncoder = new StreamEncoder( columns, inputStream );
      ByteBuffer byteBuffer = streamEncoder.getBuffer().duplicate();

      streamEncoder.writeRow( rowMeta, goodObjectData );
      ByteBuffer newByteBuffer = streamEncoder.getBuffer().duplicate();

      assertNotSame( byteBuffer, newByteBuffer );
    } catch ( Exception ex ) {
      fail( "Unexpected Exception occurred during testStreamEncoderGoodInputType: " + ex );
    }
  }

  @Test
  public void testStreamEncoderBadInputType() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "float_column" ) );
    Object[] badObjectData = { "aa" };

    ColumnSpec columnSpec = new ColumnSpec( ColumnSpec.ConstantWidthType.INTEGER_64 );
    columns.add( columnSpec );
    ByteBuffer byteBuffer = null;
    StreamEncoder streamEncoder = null;
    try {
      streamEncoder = new StreamEncoder( columns, inputStream );
      byteBuffer = streamEncoder.getBuffer().duplicate();

      streamEncoder.writeRow( rowMeta, badObjectData );
      fail( "Test failed to throw a KettleValueException when encountering bad data" );

    } catch ( KettleValueException ex ) {
      assertNotNull( byteBuffer );
      assertNotNull( streamEncoder );
      ByteBuffer newByteBuffer = streamEncoder.getBuffer().duplicate();
      assertEquals( byteBuffer, newByteBuffer );

    } catch ( Exception allOtherExceptions ) {
      //If we didn't get the KettleValueException, then the test did not end the way we expected it to
      fail( "Test failed to throw the appropriate exception when encountering bad data" );
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
