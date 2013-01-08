/*
 * This plugin was initially developed by Daniel Einspanjer and is provided under the terms 
 * of the GNU Lesser General Public License, Version 2.1. You may not use 
 * this file except in compliance with the license. If you need a copy of the license, 
 * please go to http://www.gnu.org/licenses/lgpl-2.1.txt. 
 *
 * Software distributed under the GNU Lesser Public License is distributed on an "AS IS" 
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or  implied. Please refer to 
 * the license for the specific language governing your rights and limitations.
*/

package plugin.com.vertica.kettle.bulkloader;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class VerticaBulkLoaderData extends BaseStepData implements StepDataInterface
{
	private static final byte BYTE_ZERO = (byte)0;
	private static final byte BYTE_ONE = (byte)1;
	private static final byte BYTE_FULL = (byte)0xFF;
	private static final byte BYTE_LF = (byte)0x0A;
	private static final byte BYTE_CR = (byte)0x0D;
	
	private static final int MAX_CHAR_LENGTH = 65000;

	protected  Database db;
    protected  int selectedRowFieldIndices[];
    
    protected RowMetaInterface outputRowMeta;
    protected RowMetaInterface insertRowMeta;

    protected DatabaseMeta databaseMeta;

	private WritableByteChannel channel;
	private PipedOutputStream pipedOutputStream;

    protected volatile Thread workerThread;
    
    private boolean initialized = false;
    private int columnCount;
    private int varcharCount;
    private int rowMaxSize;
    private int rowHeaderSize;
    private int bufferSize;
    
    private ByteBuffer buffer;
    
    private Charset charset;
    private CharBuffer charBuffer;
    private CharsetEncoder charEncoder;

    protected VerticaBulkLoaderData()
	{
		super();
		
		db=null;
	
		pipedOutputStream = new PipedOutputStream();
		channel = Channels.newChannel(pipedOutputStream);
	    charBuffer = CharBuffer.allocate(MAX_CHAR_LENGTH);
	    charset = Charset.forName("UTF-8");
		charEncoder = charset.newEncoder();
	}
    
    protected void close() throws KettleException {
    		try
    		{
    			channel.close();
    			pipedOutputStream.close();
    		}
    		catch (IOException e)
    		{
    			throw new KettleException("I/O Error during statement termination.", e);
    		}
    }

	public void connectInputStream(PipedInputStream sink) throws IOException {
		pipedOutputStream.connect(sink);
	}

	public void writeHeader() {
		if (!initialized) {
			throw new IllegalAccessError("Attempting to write to stream before initialization");
		}
		
		// File signature
		buffer.put("NATIVE".getBytes(charset)).put(BYTE_LF).put(BYTE_FULL).put(BYTE_CR).put(BYTE_LF).put(BYTE_ZERO);

		// Header area length (5 bytes for next three puts + (4 * N columns))
		buffer.putInt(5 + (4 * columnCount));

		// NATIVE file version
		buffer.putShort((short)1);

		// Filler (Always 0)
		buffer.put(BYTE_ZERO);

		// Number of columns
		buffer.putShort((short)columnCount);
		
		for (ValueMetaInterface valueMeta : insertRowMeta.getValueMetaList()) {
			valueMeta.getType();
		}
	}
	
	private enum Converters {
		INTEGER,
		BOOLEAN,
		FLOAT,
		CHAR,
		VARCHAR,
		DATE,
		TIME,
		TIMETZ,
		TIMESTAMP,
		TIMESTAMPZ,
		INTERVAL,
		BINARY,
		VARBINARY,
		NUMERIC
	}
	
	private class BitSet {
		private byte[]	bytes;
		private boolean dirty = false;
		private int numBits;
		private int numBytes;

		private BitSet(int numBits) {
			this.numBits = numBits;
			this.numBytes = (int) Math.ceil((double) numBits / 8.0d);
			bytes = new byte[this.numBytes];
		}

		private void setBit(int bitIndex) {
			if (bitIndex < 0 || bitIndex >= numBits) {
				throw new IllegalArgumentException("Invalid bit index");
			}

			int byteIdx = (int) Math.floor((double) bitIndex / 8.0d);

			int bitIdx = bitIndex - (byteIdx * 8);
			bytes[byteIdx] |= (1 << bitIdx);
			
			dirty = true;
		}

		private boolean isDirty() {
			return dirty;
		}
		
		private void clear() {
			if (dirty) {
				for (int i = 0; i < numBytes; i++) {
					bytes[i] = BYTE_ZERO;
				}
				dirty = false;
			}
		}
		
		private byte[] toBytes() {
			return bytes;
		}

		private int numBytes() {
			return bytes.length;
		}

		private void writeBytes(ByteBuffer buf) {
			buf.put(bytes);
		}
		private void writeBytes(int index, ByteBuffer buf) {
			for (int i = 0; i < bytes.length; i++) {
				buf.put(index + i, bytes[i]);
			}
		}
	}
}