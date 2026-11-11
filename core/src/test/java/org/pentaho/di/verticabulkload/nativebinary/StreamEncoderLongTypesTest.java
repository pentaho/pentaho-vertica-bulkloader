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
