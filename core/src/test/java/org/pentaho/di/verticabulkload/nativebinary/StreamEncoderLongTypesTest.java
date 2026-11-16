/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/

package org.pentaho.di.verticabulkload.nativebinary;

import java.io.PipedInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaBinary;
import org.pentaho.di.core.row.value.ValueMetaString;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class StreamEncoderLongTypesTest {

  @Test
  public void testLongVarcharEncoding() throws Exception {
    char[] characters = new char[ 65001 ];
    Arrays.fill( characters, 'a' );
    String value = new String( characters ) + "\u00e9";
    byte[] expected = value.getBytes( StandardCharsets.UTF_8 );
    ColumnSpec column = new ColumnSpec( ColumnSpec.VariableWidthType.LONG_VARCHAR, expected.length );
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "long_varchar" ) );

    try ( PipedInputStream input = new PipedInputStream() ) {
      StreamEncoder encoder = new StreamEncoder( Collections.singletonList( column ), input );
      assertEquals( ColumnType.LONG_VARCHAR, column.type );
      assertEquals( 4 + 1 + 4 + expected.length, encoder.getRowMaxSize() );
      encoder.writeHeader();
      encoder.writeRow( rowMeta, new Object[] { value } );
      assertEncodedValue( encoder.getBuffer(), expected );
    }
  }

  @Test
  public void testLongVarbinaryEncoding() throws Exception {
    byte[] value = { 0, 1, (byte) 0xff, 0, 42 };
    ColumnSpec column = new ColumnSpec( ColumnSpec.VariableWidthType.LONG_VARBINARY, value.length );
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaBinary( "long_varbinary" ) );

    try ( PipedInputStream input = new PipedInputStream() ) {
      StreamEncoder encoder = new StreamEncoder( Collections.singletonList( column ), input );
      assertEquals( ColumnType.LONG_VARBINARY, column.type );
      assertEquals( 4 + 1 + 4 + value.length, encoder.getRowMaxSize() );
      encoder.writeHeader();
      encoder.writeRow( rowMeta, new Object[] { value } );
      assertEncodedValue( encoder.getBuffer(), value );
    }
  }

  private static void assertEncodedValue( ByteBuffer buffer, byte[] expected ) {
    ByteBuffer encoded = buffer.duplicate().order( ByteOrder.LITTLE_ENDIAN );
    encoded.flip();
    encoded.position( 20 );
    assertEquals( -1, encoded.getInt() );
    assertEquals( 4 + expected.length, encoded.getInt() );
    assertEquals( 0, encoded.get() );
    assertEquals( expected.length, encoded.getInt() );
    byte[] actual = new byte[ expected.length ];
    encoded.get( actual );
    assertArrayEquals( expected, actual );
    assertEquals( 0, encoded.remaining() );
  }
}
