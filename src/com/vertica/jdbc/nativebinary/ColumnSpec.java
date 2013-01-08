package com.vertica.jdbc.nativebinary;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;

public class ColumnSpec {	
	private static final byte BYTE_ZERO = (byte)0;
	private static final byte BYTE_ONE = (byte)1;
	private static final byte BYTE_SPACE = (byte)0x20;
	public enum ConstantWidthType {
		INTEGER_8(ColumnType.INTEGER, 1),
		INTEGER_16(ColumnType.INTEGER, 2),
		INTEGER_32(ColumnType.INTEGER, 4),
		INTEGER_64(ColumnType.INTEGER, 8),
		BOOLEAN(ColumnType.BOOLEAN, 1),
		FLOAT(ColumnType.FLOAT, 8),
		DATE(ColumnType.DATE, 8),
		TIME(ColumnType.TIME, 8),
		TIMETZ(ColumnType.TIMETZ, 8),
		TIMESTAMP(ColumnType.TIMESTAMP, 8),
		TIMESTAMPZ(ColumnType.TIMESTAMPZ, 8),
		INTERVAL(ColumnType.INTERVAL, 8);
		
		private final ColumnType type;
		private final int bytes;
		private ConstantWidthType(ColumnType type, int bytes) {
			this.type = type;
			this.bytes = bytes;
		}
	}

	public enum VariableWidthType {
		VARCHAR(ColumnType.VARCHAR),
		VARBINARY(ColumnType.VARBINARY);
		
		private final ColumnType type;
		private final int bytes = -1;
		private VariableWidthType(ColumnType type) {
			this.type = type;
		}
	}
	
	public enum UserDefinedWidthType {
		CHAR(ColumnType.CHAR),
		BINARY(ColumnType.BINARY);
		
		private final ColumnType type;
		private UserDefinedWidthType(ColumnType type) {
			this.type = type;
		}
	}

	public enum PrecisionScaleWidthType {
		NUMERIC(ColumnType.NUMERIC);
		
		private final ColumnType type;
		private PrecisionScaleWidthType(ColumnType type) {
			this.type = type;
		}
	}
	
	public final ColumnType type;
	public int bytes;
	public final int scale;
	private CharBuffer	charBuffer;
	private CharsetEncoder	charEncoder;
	private ByteBuffer	mainBuffer;
	
	public ColumnSpec(PrecisionScaleWidthType precisionScaleWidthType, int precision, int scale) {
		this.type = precisionScaleWidthType.type;
		this.bytes = Math.round((precision / 19 + 1) * 8);
		this.scale = scale;
	}

	public ColumnSpec(UserDefinedWidthType userDefinedWidthType, int bytes) {
		this.type = userDefinedWidthType.type;
		this.bytes = bytes;
		this.scale = 0;
	}
	
	public ColumnSpec(ConstantWidthType constantWidthType) {
		this.type = constantWidthType.type;
		this.bytes = constantWidthType.bytes;
		this.scale = 0;
	}
	
	public ColumnSpec(VariableWidthType variableWidthType) {
		this.type = variableWidthType.type;
		this.bytes = variableWidthType.bytes;
		this.scale = 0;
	}

	public void setCharBuffer(CharBuffer charBuffer) {
		this.charBuffer = charBuffer;
	}

	public void setCharEncoder(CharsetEncoder charEncoder) {
		this.charEncoder = charEncoder;
	}

	public void setMainBuffer(ByteBuffer buffer) {
		this.mainBuffer = buffer;
	}

	public void encode(Object value) throws CharacterCodingException {
		if (value == null) return;
		int prevPosition,length;
		ByteBuffer inputBinary;
		
		switch (this.type) {
			case BINARY:
				inputBinary = (ByteBuffer)value;
				length = inputBinary.limit();
				this.mainBuffer.put(inputBinary);
				for (int i = 0; i < (this.bytes - length); i++) {
					this.mainBuffer.put(BYTE_ZERO);
				}
				break;
			case BOOLEAN:
				this.mainBuffer.put(((Boolean)value).booleanValue() ? BYTE_ONE : BYTE_ZERO);
				break;
			case CHAR:
				this.charBuffer.clear();
				this.charEncoder.reset();
				this.charBuffer.put((String)value);
				this.charBuffer.flip();
				prevPosition = this.mainBuffer.position();
				this.charEncoder.encode(this.charBuffer,this.mainBuffer, true);
				int encodedLength = this.mainBuffer.position() - prevPosition;
				for (int i = 0; i < (this.bytes - encodedLength); i++) {
					this.mainBuffer.put(BYTE_SPACE);
				}
				break;
			case DATE:
				this.mainBuffer.putLong((Long)value);
				break;
			case FLOAT:
				this.mainBuffer.putDouble((Double) value);
				break;
			case INTEGER:
				switch (this.bytes){
					case 1:
						this.mainBuffer.put(((Byte)value).byteValue());
						break;
					case 2:
						this.mainBuffer.putShort((Short)value);
						break;
					case 4:
						this.mainBuffer.putInt((Integer)value);
						break;
					case 8:
						this.mainBuffer.putLong((Long)value);
						break;
					default:
						throw new IllegalArgumentException("Invalid byte size for Integer type");
				}
				break;
			case INTERVAL:
				this.mainBuffer.putLong((Long)value);
				break;
			case NUMERIC:
				throw new UnsupportedOperationException("Encoding for TIMETZ data type is not implemented");
				// break;
			case TIME:
				this.mainBuffer.putLong((Long)value);
				break;
			case TIMESTAMP:
				this.mainBuffer.putLong((Long)value);
				break;
			case TIMESTAMPZ:
				this.mainBuffer.putLong((Long)value);
				break;
			case TIMETZ:
				throw new UnsupportedOperationException("Encoding for TIMETZ data type is not implemented");
				// break;
			case VARBINARY:
				inputBinary = (ByteBuffer)value;
				prevPosition = this.mainBuffer.position();
				this.mainBuffer.put(inputBinary);
				this.bytes = this.mainBuffer.position() - prevPosition;
				break;
			case VARCHAR:
				this.charBuffer.clear();
				this.charEncoder.reset();
				this.charBuffer.put((String)value);
				this.charBuffer.flip();
				prevPosition = this.mainBuffer.position();
				this.charEncoder.encode(this.charBuffer,this.mainBuffer, true);
				this.bytes = this.mainBuffer.position() - prevPosition;
				break;

			default:
				throw new IllegalArgumentException("Invalid ColumnType");
				// break;
		}
	}
}
