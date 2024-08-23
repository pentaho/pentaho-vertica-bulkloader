/*!
* This program is free software; you can redistribute it and/or modify it under the
* terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
* Foundation.
*
* You should have received a copy of the GNU Lesser General Public License along with this
* program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
* or from the Free Software Foundation, Inc.,
* 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*
* This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
* without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
* See the GNU Lesser General Public License for more details.
*
* Copyright (c) 2002-2019 Hitachi Vantara..  All rights reserved.
*/

package org.pentaho.di.verticabulkload.nativebinary;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Collections;
import java.util.List;

import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;

public class StreamEncoder {

  // public for test purposes
  // TODO: maybe this needs to be a configurable setting, but I don't know how important it is.
  public static final int NUM_ROWS_TO_BUFFER = 500;

  private static final byte BYTE_ZERO = (byte) 0;
  private static final byte BYTE_FULL = (byte) 0xFF;
  private static final byte BYTE_LF = (byte) 0x0A;
  private static final byte BYTE_CR = (byte) 0x0D;

  private static final int MAX_CHAR_LENGTH = 65000;
  
  //https://docs.vertica.com/24.1.x/en/sql-reference/data-types/long-data-types/
  private static final int MAX_LONG_VARCHAR_LENGTH = 32000000;

  private static final int MAXIMUM_BUFFER_SIZE = Integer.MAX_VALUE - 8;

  protected WritableByteChannel channel;
  private PipedOutputStream pipedOutputStream;

  private int columnCount;
  private int rowMaxSize;

  private ByteBuffer buffer;

  private Charset charset;

  private final List<ColumnSpec> columns;
  private final BitSet rowNulls;

  public void close() throws IOException {
    flushAndClose();
  }

  public StreamEncoder( List<ColumnSpec> columns, PipedInputStream inputStream ) throws IOException {
    this.columns = Collections.unmodifiableList( columns );
    this.columnCount = this.columns.size();
    this.rowNulls = new BitSet( this.columnCount );

    this.charset = Charset.forName( "UTF-8" );

    boolean usingLongVarchar = columns.stream().anyMatch( cs -> ColumnType.LONG_VARCHAR.equals( cs.type ) );
    
    CharBuffer charBuffer = CharBuffer.allocate( usingLongVarchar ? MAX_LONG_VARCHAR_LENGTH : MAX_CHAR_LENGTH );
    CharsetEncoder charEncoder = charset.newEncoder();

    this.pipedOutputStream = new PipedOutputStream( inputStream );
    this.channel = Channels.newChannel( pipedOutputStream );

    this.rowMaxSize = 4 + this.rowNulls.numBytes();

    for ( ColumnSpec column : columns ) {
      switch ( column.type ) {
        case LONG_VARBINARY:
        case VARBINARY:
          this.rowMaxSize += 4; // consider data size bytes for variable length field
          break;
        case LONG_VARCHAR:
        case VARCHAR:
          this.rowMaxSize += 4; // consider data size bytes for variable length field
        case NUMERIC:
        case CHAR:
          column.setCharBuffer( charBuffer );
          column.setCharEncoder( charEncoder );
          break;
        default:
          break;
      }
      this.rowMaxSize += column.getMaxLength();
    }

    this.buffer = ByteBuffer.allocate( countMainByteBufferSize() );
    this.buffer.order( ByteOrder.LITTLE_ENDIAN );
    this.buffer.clear();

    for ( ColumnSpec column : columns ) {
      column.setMainBuffer( buffer );
    }
  }

  /**
   * For junit test purpose
   * 
   * @return the rowMaxSize
   */
  int countMainByteBufferSize() {
    long bufferSize = (long) getRowMaxSize() * NUM_ROWS_TO_BUFFER;
    return (int) ( bufferSize > 0 && bufferSize < MAXIMUM_BUFFER_SIZE ? bufferSize : MAXIMUM_BUFFER_SIZE );
  }

  public void writeHeader() throws IOException {
    // File signature
    buffer.put( "NATIVE".getBytes( charset ) ).put( BYTE_LF ).put( BYTE_FULL ).put( BYTE_CR ).put( BYTE_LF ).put(
        BYTE_ZERO );

    // Header area length (5 bytes for next three puts + (4 * N columns))
    buffer.putInt( 5 + ( 4 * columnCount ) );

    // NATIVE file version
    buffer.putShort( (short) 1 );

    // Filler (Always 0)
    buffer.put( BYTE_ZERO );

    // Number of columns
    buffer.putShort( (short) columnCount );

    for ( ColumnSpec column : columns ) {
      buffer.putInt( column.bytes );
    }

  }

  public void writeRow( RowMetaInterface rowMeta, Object[] row ) throws IOException, KettleValueException {
    if ( row == null ) {
      flushAndClose();
      return;
    } else if ( row.length < columnCount ) {
      throw new IllegalArgumentException( "Invalid incoming row for given column spec." );
    }

    rowNulls.clear();
    checkAndFlushBuffer();

    int rowDataSize = 0;

    // record the start of this row so we can come back and update the size and nulls
    int rowDataSizeFieldPosition = buffer.position();
    // Marking the original position, in case we need to reset
    buffer.mark();
    buffer.putInt( rowDataSize );
    int rowNullsFieldPosition = buffer.position();
    rowNulls.writeBytesTo( buffer );

    try {
      for ( int i = 0; i < columnCount; i++ ) {
        ColumnSpec colSpec = columns.get( i );
        Object value = row[ i ];
        ValueMetaInterface valueMeta = rowMeta.getValueMeta( i );

        if ( value == null ) {
          rowNulls.setBit( i );
        } else {
          colSpec.encode( valueMeta, value );
          rowDataSize += colSpec.bytes;
        }
      }
    } catch ( KettleValueException ex ) {
      //restore the buffer before the row
      buffer.reset();
      throw ex;
    }

    // Now fill in the row header
    buffer.putInt( rowDataSizeFieldPosition, rowDataSize );
    rowNulls.writeBytesTo( rowNullsFieldPosition, buffer );
  }

  private void flushAndClose() throws IOException {
    flushBuffer();
    channel.close();
    pipedOutputStream.flush();
    pipedOutputStream.close();
  }

  private void checkAndFlushBuffer() throws IOException {
    if ( buffer.position() + rowMaxSize > buffer.capacity() ) {
      flushBuffer();
    }
  }

  private void flushBuffer() throws IOException {
    buffer.flip();
    channel.write( buffer );
    buffer.clear();
  }

  private class BitSet {
    private byte[] bytes;
    private boolean dirty = false;
    private int numBits;
    private int numBytes;

    private BitSet( int numBits ) {
      this.numBits = numBits;
      this.numBytes = (int) Math.ceil( (double) numBits / 8.0d );
      bytes = new byte[this.numBytes];
    }

    /**
     * Sets the bit in the BitSet to 1. The first column (index 0) bit is the msb.
     * 
     * @param bitIndex
     *          bit index (first bit index is 0)
     */
    private void setBit( int bitIndex ) {
      if ( bitIndex < 0 || bitIndex >= numBits ) {
        throw new IllegalArgumentException( "Invalid bit index" );
      }

      int byteIdx = (int) Math.floor( (double) bitIndex / 8.0d );

      int bitIdx = bitIndex - ( byteIdx * 8 );
      bytes[byteIdx] |= ( 1 << ( 7 - bitIdx ) );

      dirty = true;
    }

    private void clear() {
      if ( dirty ) {
        for ( int i = 0; i < numBytes; i++ ) {
          bytes[i] = BYTE_ZERO;
        }
        dirty = false;
      }
    }

    private int numBytes() {
      return bytes.length;
    }

    private void writeBytesTo( ByteBuffer buf ) {
      buf.put( bytes );
    }

    private void writeBytesTo( int index, ByteBuffer buf ) {
      for ( int i = 0; i < bytes.length; i++ ) {
        buf.put( index + i, bytes[i] );
      }
    }
  }

  public ByteBuffer getBuffer() {
    return this.buffer;
  }

  /**For junit test purpose
   * @return the rowMaxSize
   */
  int getRowMaxSize() {
    return rowMaxSize;
  }

}
