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

package org.pentaho.di.verticabulkload;

import com.google.common.annotations.VisibleForTesting;
import com.vertica.jdbc.VerticaConnection;
import com.vertica.jdbc.VerticaCopyStream;
import org.apache.commons.dbcp.DelegatingConnection;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.verticabulkload.nativebinary.ColumnSpec;
import org.pentaho.di.verticabulkload.nativebinary.ColumnType;
import org.pentaho.di.verticabulkload.nativebinary.StreamEncoder;

import javax.sql.PooledConnection;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PipedInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;


public class VerticaBulkLoader extends BaseStep implements StepInterface {
  private static Class<?> PKG = VerticaBulkLoader.class; // for i18n purposes, needed by Translator2!!

  private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat( "yyyy/MM/dd HH:mm:ss" );
  private VerticaBulkLoaderMeta meta;
  private VerticaBulkLoaderData data;
  private FileOutputStream exceptionLog;
  private FileOutputStream rejectedLog;

  public VerticaBulkLoader( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    meta = (VerticaBulkLoaderMeta) smi;
    data = (VerticaBulkLoaderData) sdi;

    Object[] r = getRow(); // this also waits for a previous step to be
    // finished.
    if ( r == null ) { // no more input to be expected...

      try {
        data.close();
      } catch ( IOException ioe ) {
        throw new KettleStepException( "Error releasing resources", ioe );
      }
      return false;
    }

    if ( first ) {

      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getStepname(), null, null, this );

      RowMetaInterface tableMeta = meta.getTableRowMetaInterface();

      if ( !meta.specifyFields() ) {

        // Just take the whole input row
        data.insertRowMeta = getInputRowMeta().clone();
        data.selectedRowFieldIndices = new int[data.insertRowMeta.size()];

        data.colSpecs = new ArrayList<>( data.insertRowMeta.size() );

        for ( int insertFieldIdx = 0; insertFieldIdx < data.insertRowMeta.size(); insertFieldIdx++ ) {
          data.selectedRowFieldIndices[insertFieldIdx] = insertFieldIdx;
          ValueMetaInterface inputValueMeta = data.insertRowMeta.getValueMeta( insertFieldIdx );
          ValueMetaInterface insertValueMeta = inputValueMeta.clone();
          ValueMetaInterface targetValueMeta = tableMeta.getValueMeta( insertFieldIdx );
          insertValueMeta.setName( targetValueMeta.getName() );
          data.insertRowMeta.setValueMeta( insertFieldIdx, insertValueMeta );
          ColumnSpec cs = getColumnSpecFromField( inputValueMeta, insertValueMeta, targetValueMeta );
          data.colSpecs.add( insertFieldIdx, cs );
        }

      } else {

        int numberOfInsertFields = meta.getFieldDatabase().length;
        data.insertRowMeta = new RowMeta();
        data.colSpecs = new ArrayList<>( numberOfInsertFields );

        // Cache the position of the selected fields in the row array
        data.selectedRowFieldIndices = new int[numberOfInsertFields];
        for ( int insertFieldIdx = 0; insertFieldIdx < numberOfInsertFields; insertFieldIdx++ ) {

          String inputFieldName = meta.getFieldStream()[insertFieldIdx];
          int inputFieldIdx = getInputRowMeta().indexOfValue( inputFieldName );
          if ( inputFieldIdx < 0 ) {
            throw new KettleStepException( BaseMessages.getString( PKG,
                "VerticaBulkLoader.Exception.FieldRequired", inputFieldName ) ); //$NON-NLS-1$
          }
          data.selectedRowFieldIndices[insertFieldIdx] = inputFieldIdx;

          String insertFieldName = meta.getFieldDatabase()[insertFieldIdx];
          ValueMetaInterface inputValueMeta = getInputRowMeta().getValueMeta( inputFieldIdx );
          if ( inputValueMeta == null ) {
            throw new KettleStepException( BaseMessages.getString( PKG,
                "VerticaBulkLoader.Exception.FailedToFindField", meta.getFieldStream()[insertFieldIdx] ) ); //$NON-NLS-1$
          }
          ValueMetaInterface insertValueMeta = inputValueMeta.clone();
          insertValueMeta.setName( insertFieldName );
          data.insertRowMeta.addValueMeta( insertValueMeta );

          ValueMetaInterface targetValueMeta = tableMeta.searchValueMeta( insertFieldName );
          ColumnSpec cs = getColumnSpecFromField( inputValueMeta, insertValueMeta, targetValueMeta );
          data.colSpecs.add( insertFieldIdx, cs );
        }

      }

      try {
        data.pipedInputStream = new PipedInputStream();
        if ( data.colSpecs == null || data.colSpecs.isEmpty() ) {
          return false;
        }
        data.encoder = createStreamEncoder( data.colSpecs, data.pipedInputStream );

        initializeWorker();
        data.encoder.writeHeader();

      } catch ( IOException ioe ) {
        throw new KettleStepException( "Error creating stream encoder", ioe );
      }

    }

    try {
      Object[] outputRowData = writeToOutputStream( r );
      if ( outputRowData != null ) {
        putRow( data.outputRowMeta, outputRowData ); // in case we want it
        // go further...
        incrementLinesOutput();
      }

      if ( checkFeedback( getLinesRead() ) ) {
        if ( log.isBasic() ) {
          logBasic( "linenr " + getLinesRead() );
        } //$NON-NLS-1$
      }
    } catch ( KettleException e ) {
      logError( "Because of an error, this step can't continue: ", e );
      setErrors( 1 );
      stopAll();
      setOutputDone(); // signal end to receiver(s)
      return false;
    } catch ( IOException e ) {
      e.printStackTrace();
    }

    return true;
  }

  @VisibleForTesting
  void initializeLogFiles() throws KettleException {
    // Checking only for null, if we do a isEmpty check, the way the dialog sends the data will either be null (if not set)
    // or if you add an empty string with a space, it will see it as not empty. So only a null check needed here.
    try {
      if ( meta.getExceptionsFileName() != null ) {
        exceptionLog = new FileOutputStream( meta.getExceptionsFileName(),  true );
      }
      if ( meta.getRejectedDataFileName() != null ) {
        rejectedLog = new FileOutputStream( meta.getRejectedDataFileName(), true );
      }
    } catch ( FileNotFoundException ex ) {
      throw new KettleException( ex );
    }
  }
  @VisibleForTesting
  void writeExceptionRejectionLogs( KettleValueException valueException, Object[] outputRowData ) throws IOException {
    String dateTimeString = ( SIMPLE_DATE_FORMAT.format( new Date( System.currentTimeMillis() ) ) ) + " - ";
    logError( BaseMessages.getString( PKG, "VerticaBulkLoader.Exception.RowRejected",
      Arrays.stream( outputRowData ).map( Object::toString ).collect( Collectors.joining( " | " ) ) ) );

    if ( exceptionLog != null ) {
      // Replace used to ensure timestamps are being added appropriately (some messages are multi-line)
      exceptionLog.write( ( dateTimeString + valueException.getMessage().replace( System.lineSeparator(),
        System.lineSeparator() + dateTimeString ) ).getBytes() );
      exceptionLog.write( System.lineSeparator().getBytes() );
      for ( StackTraceElement element : valueException.getStackTrace() ) {
        exceptionLog.write( ( dateTimeString + "at " + element.toString() + System.lineSeparator() ).getBytes() );
      }
      exceptionLog.write(
        ( dateTimeString + "Caused by: " + valueException.getClass().toString() + System.lineSeparator() ).getBytes() );
      // Replace used to ensure timestamps are being added appropriately (some messages are multi-line)
      exceptionLog.write( ( ( dateTimeString + valueException.getCause().getMessage().replace( System.lineSeparator(),
        System.lineSeparator() + dateTimeString ) ).getBytes() ) );
      exceptionLog.write( System.lineSeparator().getBytes() );
    }
    if ( rejectedLog != null ) {
      rejectedLog.write( ( dateTimeString + BaseMessages.getString( PKG, "VerticaBulkLoader.Exception.RowRejected",
        Arrays.stream( outputRowData ).map( Object::toString ).collect( Collectors.joining( " | " ) ) ) ).getBytes() );
      for ( Object outputRowDatum : outputRowData ) {
        rejectedLog.write( ( outputRowDatum.toString() + " | " ).getBytes() );
      }
      rejectedLog.write( System.lineSeparator().getBytes() );
    }
  }

  @VisibleForTesting
  void closeLogFiles() throws KettleException {
    try {
      if ( exceptionLog != null ) {
        exceptionLog.close();
      }
      if ( rejectedLog != null ) {
        rejectedLog.close();
      }
    } catch ( IOException exception ) {
      throw new KettleException( exception );
    }
  }

  private ColumnSpec getColumnSpecFromField( ValueMetaInterface inputValueMeta, ValueMetaInterface insertValueMeta,
      ValueMetaInterface targetValueMeta ) {
    logBasic( "Mapping input field " + inputValueMeta.getName() + " (" + inputValueMeta.getTypeDesc() + ")"
        + " to target column " + insertValueMeta.getName() + " (" + targetValueMeta.getOriginalColumnTypeName() + ") " );

    String targetColumnTypeName = targetValueMeta.getOriginalColumnTypeName().toUpperCase();

    if ( targetColumnTypeName.equals( "INTEGER" ) || targetColumnTypeName.equals( "BIGINT" ) ) {
      return new ColumnSpec( ColumnSpec.ConstantWidthType.INTEGER_64 );
    } else if ( targetColumnTypeName.equals( "BOOLEAN" ) ) {
      return new ColumnSpec( ColumnSpec.ConstantWidthType.BOOLEAN );
    } else if ( targetColumnTypeName.equals( "FLOAT" ) || targetColumnTypeName.equals( "DOUBLE PRECISION" ) ) {
      return new ColumnSpec( ColumnSpec.ConstantWidthType.FLOAT );
    } else if ( targetColumnTypeName.equals( "CHAR" ) ) {
      return new ColumnSpec( ColumnSpec.UserDefinedWidthType.CHAR, targetValueMeta.getLength() );
    } else if ( targetColumnTypeName.equals( "VARCHAR" ) ) {
      return new ColumnSpec( ColumnSpec.VariableWidthType.VARCHAR, targetValueMeta.getLength() );
    } else if ( targetColumnTypeName.equals( "DATE" ) ) {
      if ( inputValueMeta.isDate() == false ) {
        throw new IllegalArgumentException( "Field " + inputValueMeta.getName()
            + " must be a Date compatible type to match target column " + insertValueMeta.getName() );
      } else {
        return new ColumnSpec( ColumnSpec.ConstantWidthType.DATE );
      }
    } else if ( targetColumnTypeName.equals( "TIME" ) ) {
      if ( inputValueMeta.isDate() == false ) {
        throw new IllegalArgumentException( "Field " + inputValueMeta.getName()
            + " must be a Date compatible type to match target column " + insertValueMeta.getName() );
      } else {
        return new ColumnSpec( ColumnSpec.ConstantWidthType.TIME );
      }
    } else if ( targetColumnTypeName.equals( "TIMETZ" ) ) {
      if ( inputValueMeta.isDate() == false ) {
        throw new IllegalArgumentException( "Field " + inputValueMeta.getName()
            + " must be a Date compatible type to match target column " + insertValueMeta.getName() );
      } else {
        return new ColumnSpec( ColumnSpec.ConstantWidthType.TIMETZ );
      }
    } else if ( targetColumnTypeName.equals( "TIMESTAMP" ) ) {
      if ( inputValueMeta.isDate() == false ) {
        throw new IllegalArgumentException( "Field " + inputValueMeta.getName()
            + " must be a Date compatible type to match target column " + insertValueMeta.getName() );
      } else {
        return new ColumnSpec( ColumnSpec.ConstantWidthType.TIMESTAMP );
      }
    } else if ( targetColumnTypeName.equals( "TIMESTAMPTZ" ) ) {
      if ( inputValueMeta.isDate() == false ) {
        throw new IllegalArgumentException( "Field " + inputValueMeta.getName()
            + " must be a Date compatible type to match target column " + insertValueMeta.getName() );
      } else {
        return new ColumnSpec( ColumnSpec.ConstantWidthType.TIMESTAMPTZ );
      }
    } else if ( targetColumnTypeName.equals( "INTERVAL" ) || targetColumnTypeName.equals( "INTERVAL DAY TO SECOND" ) ) {
      if ( inputValueMeta.isDate() == false ) {
        throw new IllegalArgumentException( "Field " + inputValueMeta.getName()
            + " must be a Date compatible type to match target column " + insertValueMeta.getName() );
      } else {
        return new ColumnSpec( ColumnSpec.ConstantWidthType.INTERVAL );
      }
    } else if ( targetColumnTypeName.equals( "BINARY" ) ) {
      return new ColumnSpec( ColumnSpec.VariableWidthType.VARBINARY, targetValueMeta.getLength() );
    } else if ( targetColumnTypeName.equals( "VARBINARY" ) ) {
      return new ColumnSpec( ColumnSpec.VariableWidthType.VARBINARY, targetValueMeta.getLength() );
    } else if ( targetColumnTypeName.equals( "NUMERIC" ) ) {
      return new ColumnSpec( ColumnSpec.PrecisionScaleWidthType.NUMERIC, targetValueMeta.getLength(), targetValueMeta
          .getPrecision() );
    }
    throw new IllegalArgumentException( "Column type " + targetColumnTypeName + " not supported." ); //$NON-NLS-1$
  }

  private void initializeWorker() {
    final String dml = buildCopyStatementSqlString();

    data.workerThread = Executors.defaultThreadFactory().newThread( new Runnable() {
      @Override
      public void run() {
        try {
          VerticaCopyStream stream = createVerticaCopyStream( dml );
          stream.start();
          stream.addStream( data.pipedInputStream );
          setLinesRejected( stream.getRejects().size() );
          stream.execute();
          long rowsLoaded = stream.finish();
          if ( getLinesOutput() != rowsLoaded ) {
            logMinimal( String.format( "%d records loaded out of %d records sent.", rowsLoaded, getLinesOutput() ) );
          }
          data.db.disconnect();
        } catch ( SQLException | IllegalStateException e ) {
          if ( e.getCause() instanceof InterruptedIOException ) {
            logBasic( "SQL statement interrupted by halt of transformation" );
          } else {
            logError( "SQL Error during statement execution.", e );
            setErrors( 1 );
            stopAll();
            setOutputDone(); // signal end to receiver(s)
          }
        }
      }
    } );

    data.workerThread.start();
  }

  private String buildCopyStatementSqlString() {
    final DatabaseMeta databaseMeta = data.db.getDatabaseMeta();

    StringBuilder sb = new StringBuilder( 150 );
    sb.append( "COPY " );

    sb.append( databaseMeta.getQuotedSchemaTableCombination( environmentSubstitute( meta.getSchemaName() ),
        environmentSubstitute( meta.getTableName() ) ) );

    sb.append( " (" );
    final RowMetaInterface fields = data.insertRowMeta;
    for ( int i = 0; i < fields.size(); i++ ) {
      if ( i > 0 ) {
        sb.append( ", " );
      }
      ColumnType columnType = data.colSpecs.get( i ).type;
      ValueMetaInterface valueMeta = fields.getValueMeta( i );
      switch ( columnType ) {
        case NUMERIC:
          sb.append( "TMPFILLERCOL" ).append( i ).append( " FILLER VARCHAR(1000), " );
          // Force columns to be quoted:
          sb.append( databaseMeta.getStartQuote() + valueMeta.getName() + databaseMeta.getEndQuote() );
          sb.append( " AS CAST(" ).append( "TMPFILLERCOL" ).append( i ).append( " AS NUMERIC" );
          sb.append( ")" );
          break;
        default:
          // Force columns to be quoted:
          sb.append( databaseMeta.getStartQuote() + valueMeta.getName() + databaseMeta.getEndQuote() );
          break;
      }
    }
    sb.append( ")" );

    sb.append( " FROM STDIN NATIVE " );

    if ( !Const.isEmpty( meta.getExceptionsFileName() ) ) {
      sb.append( "EXCEPTIONS E'" ).append( meta.getExceptionsFileName().replace( "'", "\\'" ) ).append( "' " );
    }

    if ( !Const.isEmpty( meta.getRejectedDataFileName() ) ) {
      sb.append( "REJECTED DATA E'" ).append( meta.getRejectedDataFileName().replace( "'", "\\'" ) ).append( "' " );
    }

    // TODO: Should eventually get a preference for this, but for now, be backward compatible.
    sb.append( "ENFORCELENGTH " );

    if ( meta.isAbortOnError() ) {
      sb.append( "ABORT ON ERROR " );
    }

    if ( meta.isDirect() ) {
      sb.append( "DIRECT " );
    }

    if ( !Const.isEmpty( meta.getStreamName() ) ) {
      sb.append( "STREAM NAME E'" ).append( environmentSubstitute( meta.getStreamName() ).replace( "'", "\\'" ) )
          .append( "' " );
    }

    // XXX: I believe the right thing to do here is always use NO COMMIT since we want Kettle's configuration to drive.
    // NO COMMIT does not seem to work even when the transformation setting 'make the transformation database
    // transactional' is on
    // sb.append("NO COMMIT");

    logDebug( "copy stmt: " + sb.toString() );

    return sb.toString();
  }

  private Object[] writeToOutputStream( Object[] r ) throws KettleException, IOException {
    assert ( r != null );

    Object[] insertRowData = r;
    Object[] outputRowData = r;

    if ( meta.specifyFields() ) {
      insertRowData = new Object[data.selectedRowFieldIndices.length];
      for ( int idx = 0; idx < data.selectedRowFieldIndices.length; idx++ ) {
        insertRowData[idx] = r[data.selectedRowFieldIndices[idx]];
      }
    }

    try {
      data.encoder.writeRow( data.insertRowMeta, insertRowData );
    } catch ( KettleValueException valueException ) {
      /*
      *  If we are to abort, we should continue throwing the exception. If we are not aborting, we need to set the
      *  outputRowData to null, so the next step knows not to add it and continue. We also need to write to the
      *  rejected log what data failed (print out the outputRowData before null'ing it) and write to the error log the
      *  issue.
      */
      //write outputRowData -> Rejected Row
      //write Error Log as to why it was rejected
      writeExceptionRejectionLogs( valueException, outputRowData );
      if ( meta.isAbortOnError() ) {
        throw valueException;
      }
      outputRowData = null;
    } catch ( IOException e ) {
      if ( !data.isStopped() ) {
        throw new KettleException( "I/O Error during row write.", e );
      }
    }

    return outputRowData;
  }

  protected void verifyDatabaseConnection() throws KettleException {
    // Confirming Database Connection is defined.
    if ( meta.getDatabaseMeta() == null ) {
      throw new KettleException( BaseMessages.getString( PKG, "VerticaBulkLoaderMeta.Error.NoConnection" ) );
    }
  }

  @Override
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (VerticaBulkLoaderMeta) smi;
    data = (VerticaBulkLoaderData) sdi;

    if ( super.init( smi, sdi ) ) {
      try {
        // Validating that the connection has been defined.
        verifyDatabaseConnection();
        data.databaseMeta = meta.getDatabaseMeta();
        initializeLogFiles();

        data.db = new Database( this, meta.getDatabaseMeta() );
        data.db.shareVariablesWith( this );

        if ( getTransMeta().isUsingUniqueConnections() ) {
          synchronized ( getTrans() ) {
            data.db.connect( getTrans().getThreadName(), getPartitionID() );
          }
        } else {
          data.db.connect( getPartitionID() );
        }

        if ( log.isBasic() ) {
          logBasic( "Connected to database [" + meta.getDatabaseMeta() + "]" );
        }

        data.db.setAutoCommit( false );

        return true;
      } catch ( KettleException e ) {
        logError( "An error occurred intialising this step: " + e.getMessage() );
        stopAll();
        setErrors( 1 );
      }
    }
    return false;
  }

  @Override
  public void markStop() {
    // Close the exception/rejected loggers at the end
    try {
      closeLogFiles();
    } catch ( KettleException ex ) {
      logError( BaseMessages.getString( PKG, "VerticaBulkLoader.Exception.ClosingLogError", ex ) );
    }
    super.markStop();
  }

  @Override
  public void stopRunning( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface )
    throws KettleException {
    setStopped( true );
    if ( data.workerThread != null ) {
      synchronized ( data.workerThread ) {
        if ( data.workerThread.isAlive() && !data.workerThread.isInterrupted() ) {
          try {
            data.workerThread.interrupt();
            data.workerThread.join();
          } catch ( InterruptedException e ) { // Checkstyle:OFF:
          }
          // Checkstyle:ONN:
        }
      }
    }

    super.stopRunning( stepMetaInterface, stepDataInterface );
  }

  @Override
  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (VerticaBulkLoaderMeta) smi;
    data = (VerticaBulkLoaderData) sdi;

    // allow data to be garbage collected immediately:
    data.colSpecs = null;
    data.encoder = null;

    setOutputDone();

    try {
      if ( getErrors() > 0 ) {
        data.db.rollback();
      }
    } catch ( KettleDatabaseException e ) {
      logError( "Unexpected error rolling back the database connection.", e );
    }

    if ( data.workerThread != null ) {
      try {
        data.workerThread.join();
      } catch ( InterruptedException e ) { // Checkstyle:OFF:
      }
      // Checkstyle:ONN:
    }

    if ( data.db != null ) {
      data.db.disconnect();
    }
    super.dispose( smi, sdi );
  }

  @VisibleForTesting
  StreamEncoder createStreamEncoder( List<ColumnSpec> colSpecs, PipedInputStream pipedInputStream ) throws IOException {
    return new StreamEncoder( colSpecs, pipedInputStream );
  }

  @VisibleForTesting
  VerticaCopyStream createVerticaCopyStream( String dml ) throws SQLException {
    return new VerticaCopyStream( getVerticaConnection(), dml );
  }

  @VisibleForTesting
  VerticaConnection getVerticaConnection() throws SQLException {
    Connection conn = data.db.getConnection();
    if ( conn != null ) {
      if ( conn instanceof VerticaConnection ) {
        return (VerticaConnection) conn;
      } else {
        Connection underlyingConn = null;
        if ( conn instanceof DelegatingConnection ) {
          DelegatingConnection pooledConn = (DelegatingConnection) conn;
          underlyingConn = pooledConn.getInnermostDelegate();
        } else if ( conn instanceof javax.sql.PooledConnection ) {
          PooledConnection pooledConn = (PooledConnection) conn;
          underlyingConn = pooledConn.getConnection();
        } else {
          // Last resort - attempt to use unwrap to get at the connection.
          try {
            if ( conn.isWrapperFor( VerticaConnection.class ) ) {
              VerticaConnection vc = conn.unwrap( VerticaConnection.class );
              return vc;
            }
          } catch ( SQLException Ignored ) {
            // Ignored - the connection doesn't support unwrap or the connection cannot be
            // unwrapped into a VerticaConnection.
          }
        }
        if ( ( underlyingConn != null ) && ( underlyingConn instanceof VerticaConnection ) ) {
          return (VerticaConnection) underlyingConn;
        }
      }
      throw new IllegalStateException( "Could not retrieve a VerticaConnection from " + conn.getClass().getName() );
    } else {
      throw new IllegalStateException( "Could not retrieve a VerticaConnection from null" );
    }
  }

}
