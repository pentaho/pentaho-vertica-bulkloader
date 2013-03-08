package com.vertica.jdbc.nativebinary;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.util.Calendar;
import java.util.TimeZone;

import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.ValueMetaInterface;

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
		TIMESTAMPTZ(ColumnType.TIMESTAMPTZ, 8),
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
	private final Calendar calendarLocalTZ = Calendar.getInstance();
	private final Calendar calendarUTC = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
	private static final Calendar julianStartDateCalendarUTC;
	private static final Calendar julianStartDateCalendarLocalTZ;

	static {
		julianStartDateCalendarUTC = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		julianStartDateCalendarUTC.clear();
		julianStartDateCalendarUTC.set(2000, 0, 1, 0, 0, 0);
		julianStartDateCalendarLocalTZ = Calendar.getInstance();
		julianStartDateCalendarLocalTZ.clear();
		julianStartDateCalendarLocalTZ.set(2000, 0, 1, 0, 0, 0);
	}

	public ColumnSpec(PrecisionScaleWidthType precisionScaleWidthType, int precision, int scale) {
		this.type = precisionScaleWidthType.type;
		this.bytes = -1 ; // NUMERIC is encoded as VARCHAR (length = -1)
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

	public void encode(ValueMetaInterface valueMeta, Object value) throws CharacterCodingException, UnsupportedEncodingException, KettleValueException {
		if (value == null) return;
		int prevPosition, length, sizePosition;
		byte[] inputBinary;
		long milliSeconds;
		
		switch (this.type) {
			case BINARY:
				inputBinary = valueMeta.getBinaryString(value);
				length = inputBinary.length;
				this.mainBuffer.put(inputBinary);
				for (int i = 0; i < (this.bytes - length); i++) {
					this.mainBuffer.put(BYTE_ZERO);
				}
				break;
			case BOOLEAN:
				this.mainBuffer.put((valueMeta.getBoolean(value)).booleanValue() ? BYTE_ONE : BYTE_ZERO);
				break;
			case CHAR:
				this.charBuffer.clear();
				this.charEncoder.reset();
				this.charBuffer.put(valueMeta.getString(value));
				this.charBuffer.flip();
				prevPosition = this.mainBuffer.position();
				this.charEncoder.encode(this.charBuffer,this.mainBuffer, true);
				int encodedLength = this.mainBuffer.position() - prevPosition;
				for (int i = 0; i < (this.bytes - encodedLength); i++) {
					this.mainBuffer.put(BYTE_SPACE);
				}
				break;
			case DATE:        
				//Get Julian date for 01/01/2000
				long julianStart = toJulian(2000, 1, 1);
				calendarLocalTZ.setTime(valueMeta.getDate(value));
				long julianEnd = toJulian(
						calendarLocalTZ.get(Calendar.YEAR), 
						calendarLocalTZ.get(Calendar.MONTH)+1,
						calendarLocalTZ.get(Calendar.DAY_OF_MONTH)
						);
 				this.mainBuffer.putLong(new Long(julianEnd - julianStart));
				break;
			case FLOAT:
				this.mainBuffer.putDouble(valueMeta.getNumber(value));
				break;
			case INTEGER:
				switch (this.bytes){
					case 1:
						this.mainBuffer.put(valueMeta.getInteger(value).byteValue());
						break;
					case 2:
						this.mainBuffer.putShort(valueMeta.getInteger(value).shortValue());
						break;
					case 4:
						this.mainBuffer.putInt(valueMeta.getInteger(value).intValue());
						break;
					case 8:
						this.mainBuffer.putLong(valueMeta.getInteger(value));
						break;
					default:
						throw new IllegalArgumentException("Invalid byte size for Integer type");
				}
				break;
			case INTERVAL:
				this.mainBuffer.putLong(valueMeta.getInteger(value));
				break;
			case TIME:
				// 64-bit integer in little-endian format containing the number of microseconds since midnight in the UTC time zone. 
				// We actually use the local time instead of the UTC time because UTC time was giving wrong results.
				calendarLocalTZ.setTime(valueMeta.getDate(value));
				milliSeconds = calendarLocalTZ.get(Calendar.HOUR_OF_DAY) * 3600 * 1000
					+ calendarLocalTZ.get(Calendar.MINUTE) * 60 * 1000
					+ calendarLocalTZ.get(Calendar.SECOND) * 1000
					+ calendarLocalTZ.get(Calendar.MILLISECOND);
				this.mainBuffer.putLong(milliSeconds*1000);
				break;
			case TIMETZ:
				// 64-bit value where Upper 40 bits contain the number of microseconds since midnight and Lower 24 bits contain time zone as the UTC offset in microseconds calculated as follows: Time zone is logically from -24hrs to +24hrs from UTC. Instead it is represented here as a number between 0hrs to 48hrs. Therefore, 24hrs should be added to the actual time zone to calculate it.
				calendarUTC.setTime(valueMeta.getDate(value));
				milliSeconds = calendarUTC.get(Calendar.HOUR_OF_DAY) * 3600 * 1000
					+ calendarUTC.get(Calendar.MINUTE) * 60 * 1000
					+ calendarUTC.get(Calendar.SECOND) * 1000
					+ calendarUTC.get(Calendar.MILLISECOND);
				final long timeZoneOffsetMicroseconds = 24 * 3600 ;
				this.mainBuffer.putLong(((milliSeconds * 1000) << 8*3) + timeZoneOffsetMicroseconds);
				break;
			case TIMESTAMP:
				// 64-bit integer in little-endian format containing the number of microseconds since Julian day: Jan 01 2000 00:00:00.
				calendarLocalTZ.setTime(valueMeta.getDate(value));
				milliSeconds = calendarLocalTZ.getTimeInMillis() - julianStartDateCalendarLocalTZ.getTimeInMillis();
				this.mainBuffer.putLong(new Long(milliSeconds * 1000));
				break;
			case TIMESTAMPTZ:
				// A 64-bit integer in little-endian format containing the number of microseconds since Julian day: Jan 01 2000 00:00:00 in the UTC timezone.
				calendarUTC.setTime(valueMeta.getDate(value));
				milliSeconds = calendarUTC.getTimeInMillis() - julianStartDateCalendarUTC.getTimeInMillis();
				this.mainBuffer.putLong(new Long(milliSeconds * 1000));
				break;
			case VARBINARY:
				sizePosition = this.mainBuffer.position();
				this.mainBuffer.putInt(0);        
				prevPosition = this.mainBuffer.position();
				this.mainBuffer.put(valueMeta.getBinaryString(value));
				this.mainBuffer.putInt(sizePosition, this.mainBuffer.position() - prevPosition);        
				this.bytes = this.mainBuffer.position() - sizePosition;
				break;
			case NUMERIC:
				// Numeric is encoded as VARCHAR. COPY statement uses is as a FILLER column for Vertica itself
				// to convert into internal NUMERIC data format.
			case VARCHAR:
				this.charBuffer.clear();
				this.charEncoder.reset();
				this.charBuffer.put(valueMeta.getString(value));
				this.charBuffer.flip();
				sizePosition = this.mainBuffer.position();
				this.mainBuffer.putInt(0);
				prevPosition = this.mainBuffer.position();
				this.charEncoder.encode(this.charBuffer, this.mainBuffer, true);
				int dataLength = this.mainBuffer.position() - prevPosition;
				this.mainBuffer.putInt(sizePosition, dataLength);
				this.bytes= this.mainBuffer.position() - sizePosition;
				break;

			default:
				throw new IllegalArgumentException("Invalid ColumnType");
				// break;
		}
	}
  
  
/**
  * Returns the Julian day number that begins at noon of
  * this day, Positive year signifies A.D., negative year B.C.
  * Remember that the year after 1 B.C. was 1 A.D.
  * NOTE THAT day and month are base 1 (January == 1)
  * ref :
  *  Numerical Recipes in C, 2nd ed., Cambridge University Press 1992
  */
  // Gregorian Calendar adopted Oct. 15, 1582 (2299161)
  public static int JGREG= 15 + 31*(10+12*1582);
  public static double HALFSECOND = 0.5;

  private static long toJulian(int year, int month, int day) {
   int julianYear = year;
   if (year < 0) julianYear++;
   int julianMonth = month;
   if (month > 2) {
     julianMonth++;
   }
   else {
     julianYear--;
     julianMonth += 13;
   }

   double julian = (java.lang.Math.floor(365.25 * julianYear)
        + java.lang.Math.floor(30.6001*julianMonth) + day + 1720995.0);
   if (day + 31 * (month + 12 * year) >= JGREG) {
     // change over to Gregorian calendar
     int ja = (int)(0.01 * julianYear);
     julian += 2 - ja + (0.25 * ja);
   }
   return (long) java.lang.Math.floor(julian);
 }
  
  
  
  
}
