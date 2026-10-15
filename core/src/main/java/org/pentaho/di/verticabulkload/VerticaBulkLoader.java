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


package org.pentaho.di.verticabulkload;

import com.google.common.annotations.VisibleForTesting;
import com.vertica.core.VConnection;
import com.vertica.jdbc.SConnection;
import com.vertica.jdbc.VerticaConnection;
import com.vertica.jdbc.VerticaCopyStream;
import com.vertica.support.exceptions.GeneralException;
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
import java.io.PipedInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.commons.lang.StringUtils.isNotBlank;


/**
 * Loads rows through a bounded, in-memory pipe shared by two threads.
 *
 * <p>The PDI step thread encodes rows into the pipe; the COPY worker reads those bytes through JDBC.
 * If COPY fails, closing the input side releases a producer waiting for pipe space. A stopped flag
 * alone cannot release a write that is already blocked.
 *
 * <p>Healthy EOF lets COPY finish before disconnecting. Forced stop uses a separate cleanup thread
 * and a shared waiting deadline, so an unresponsive driver cannot hold the stopping thread indefinitely.
 */
public class VerticaBulkLoader extends BaseStep implements StepInterface {
  private static Class<?> PKG = VerticaBulkLoader.class; // for i18n purposes, needed by Translator2!!

  private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat( "yyyy/MM/dd HH:mm:ss" );
  private static final long WORKER_SHUTDOWN_TIMEOUT_MILLIS = 5000L;
  private VerticaBulkLoaderMeta meta;
  private VerticaBulkLoaderData data;
  private FileOutputStream exceptionLog;
  private FileOutputStream rejectedLog;
  // Protect publication/start of owned threads and the first stop deadline, never blocking JDBC calls or joins.
  private final Object workerLifecycleLock = new Object();
  private final AtomicReference<CopyState> copyState = new AtomicReference<>( CopyState.RUNNING );
  // Thread publication and the shutdown deadline are guarded by workerLifecycleLock.
  private Thread databaseCleanupThread;
  private long workerShutdownDeadline;

  /**
   * Permission to finalize COPY, not the worker's Thread.State or the database's transaction status.
   */
  private enum CopyState {
    // Also covers initialization before the worker starts; finalization has not been claimed.
    RUNNING,
    // Finalization won its claim. The driver may still be working, or the worker may already have ended.
    FINISHING,
    // No new finalization claim is allowed; previously claimed JDBC work may still be in flight.
    STOPPED
  }

  public VerticaBulkLoader( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    meta = (VerticaBulkLoaderMeta) smi;
    data = (VerticaBulkLoaderData) sdi;

    // getRow() may wait for upstream data, so do not start or resume work after a stop request.
    if ( isWorkerStopRequested() ) {
      return false;
    }

    Object[] r = getRow();
    if ( isWorkerStopRequested() ) {
      return false;
    }
    if ( r == null ) {

      try {
        // Flush and close the producer's output side: the worker reads the remaining bytes, then EOF.
        // This is not forced shutdown, which closes the input side to stop a blocked producer.
        data.close();
      } catch ( IOException ioe ) {
        throw new KettleStepException( "Error releasing resources", ioe );
      }
      return false;
    }

    if ( first && !initializeRowStream() ) {
      return false;
    }

    try {
      Object[] outputRowData = writeToOutputStream( r );
      // The worker can fail while this row is being encoded; stop before publishing more output.
      if ( isWorkerStopRequested() ) {
        return false;
      }
      if ( outputRowData != null ) {
        // Rows are forwarded before asynchronous COPY completes; this counter is not a committed row count.
        putRow( data.outputRowMeta, outputRowData );
        incrementLinesOutput();
      }

      if ( checkFeedback( getLinesRead() ) && log.isBasic() ) {
        logBasic( "linenr " + getLinesRead() ); //$NON-NLS-1$
      }
    } catch ( KettleException e ) {
      logError( "Because of an error, this step can't continue: ", e );
      setErrors( 1 );
      stopAll();
      setOutputDone(); // signal end to receiver(s)
      return false;
    } catch ( IOException e ) {
      // Report producer-side failures through PDI so a closed pipe cannot leave the transformation waiting.
      logError( "I/O Error during row write.", e );
      setErrors( 1 );
      stopAll();
      setOutputDone(); // signal end to receiver(s)
      return false;
    }

    return true;
  }

  private boolean initializeRowStream() throws KettleException {
    first = false;
    data.outputRowMeta = getInputRowMeta().clone();
    meta.getFields( data.outputRowMeta, getStepname(), null, null, this );

    RowMetaInterface tableMeta = meta.getTableRowMetaInterface();
    if ( meta.specifyFields() ) {
      initializeSelectedFields( tableMeta );
    } else {
      initializeAllFields( tableMeta );
    }

    if ( data.colSpecs.isEmpty() ) {
      return false;
    }
    try {
      data.pipedInputStream = new PipedInputStream();
      data.encoder = createStreamEncoder( data.colSpecs, data.pipedInputStream );
      // Start the reader before producing the native stream; encoder flushes can wait for pipe space.
      if ( !initializeWorker() ) {
        return false;
      }
      data.encoder.writeHeader();
      return true;
    } catch ( IOException ioe ) {
      throw new KettleStepException( "Error creating stream encoder", ioe );
    }
  }

  private void initializeAllFields( RowMetaInterface tableMeta ) {
    // Keep incoming field order, but use target column names and native widths for COPY.
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
      data.colSpecs.add( getColumnSpecFromField( inputValueMeta, insertValueMeta, targetValueMeta ) );
    }
  }

  private void initializeSelectedFields( RowMetaInterface tableMeta ) throws KettleStepException {
    // COPY follows the configured column order; remember each corresponding index in the original PDI row.
    int numberOfInsertFields = meta.getFieldDatabase().length;
    data.insertRowMeta = new RowMeta();
    data.colSpecs = new ArrayList<>( numberOfInsertFields );
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
            "VerticaBulkLoader.Exception.FailedToFindField", inputFieldName ) ); //$NON-NLS-1$
      }
      ValueMetaInterface insertValueMeta = inputValueMeta.clone();
      insertValueMeta.setName( insertFieldName );
      data.insertRowMeta.addValueMeta( insertValueMeta );
      ValueMetaInterface targetValueMeta = tableMeta.searchValueMeta( insertFieldName );
      data.colSpecs.add( getColumnSpecFromField( inputValueMeta, insertValueMeta, targetValueMeta ) );
    }
  }

  @VisibleForTesting
  void initializeLogFiles() throws KettleException {
    try {
      if ( isNotBlank( meta.getExceptionsFileName() ) ) {
        exceptionLog = new FileOutputStream( meta.getExceptionsFileName(),  true );
      }
      if ( isNotBlank( meta.getRejectedDataFileName() ) ) {
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

  /**
   * Publishes and starts the COPY reader as one operation with respect to stop.
   * A thread created while stop wins remains unstarted and unpublished.
   */
  private boolean initializeWorker() {
    final String dml = buildCopyStatementSqlString();
    // The worker retains its own pipe reference even when disposal clears producer-owned data.
    final PipedInputStream workerInputStream = data.pipedInputStream;
    Thread workerThread = createWorkerThread( () -> runCopy( dml, workerInputStream ) );
    // A JDBC call that ignores cancellation must not, by itself, keep the JVM alive after forced stop.
    workerThread.setDaemon( true );
    synchronized ( workerLifecycleLock ) {
      if ( isWorkerStopRequested() ) {
        closePipedInputStream( workerInputStream );
        return false;
      }
      data.workerThread = workerThread;
      workerThread.start();
      return true;
    }
  }

  /**
   * Runs the COPY sequence on the consumer thread, including failures before any bytes are read.
   * Connection cleanup belongs to the single database cleanup thread, not this worker.
   */
  private void runCopy( String dml, PipedInputStream workerInputStream ) {
    try {
      if ( isWorkerStopRequested() ) {
        return;
      }
      VerticaCopyStream stream = createVerticaCopyStream( dml );
      stream.start();
      stream.addStream( workerInputStream );
      setLinesRejected( stream.getRejects().size() );
      // The producer supplies bytes concurrently; normal output closure lets this read reach EOF.
      stream.execute();
      if ( !beginCopyFinalization() ) {
        return;
      }
      long rowsLoaded = stream.finish();
      if ( isWorkerStopRequested() ) {
        return;
      }
      if ( getLinesOutput() != rowsLoaded ) {
        logMinimal( String.format( "%d records loaded out of %d records sent.", rowsLoaded, getLinesOutput() ) );
      }
    } catch ( Exception e ) {
      // Release the local handoff before stopAll(): a producer may already be blocked inside a pipe write.
      closePipedInputStream( workerInputStream );
      if ( isWorkerStopRequested() ) {
        logBasic( "SQL statement interrupted by halt of transformation" );
      } else {
        logError( "SQL Error during statement execution.", e );
        setErrors( 1 );
        setStopped( true );
        stopAll();
        setOutputDone();
      }
    }
  }

  /**
   * Competes atomically with stop, rather than trusting an earlier "not stopped" observation.
   * If this claim wins first, finalization is logically in flight and stop can only request cancellation.
   * Never hold the lifecycle lock across finish(): blocked JDBC must not prevent stop publication.
   */
  @VisibleForTesting
  boolean beginCopyFinalization() {
    return !isStopped() && copyState.compareAndSet( CopyState.RUNNING, CopyState.FINISHING );
  }

  private boolean isWorkerStopRequested() {
    return copyState.get() == CopyState.STOPPED || isStopped();
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
          // The encoder sends NUMERIC as text; a filler column lets Vertica cast it to its native numeric type.
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
      if ( !isWorkerStopRequested() ) {
        throw new KettleException( "I/O Error during row write.", e );
      }
      // Pipe closure is expected after stop; processRow() checks stop again before forwarding this row.
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
    stopWorker();

    super.stopRunning( stepMetaInterface, stepDataInterface );
  }

  @Override
  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (VerticaBulkLoaderMeta) smi;
    data = (VerticaBulkLoaderData) sdi;

    // The worker uses its captured pipe, not the producer's encoder or column specifications.
    data.colSpecs = null;
    data.encoder = null;

    setOutputDone();

    // Forced cleanup must wake blocked producers; normal EOF must let COPY finish first.
    if ( isWorkerStopRequested() || getErrors() > 0 ) {
      stopWorker();
    } else {
      waitForWorker();
      // The cleanup claim rechecks stop under the lifecycle lock; this observation alone cannot grant ownership.
      if ( isWorkerStopRequested() || getErrors() > 0 ) {
        stopWorker();
      } else {
        Thread cleanupThread = startDatabaseCleanup();
        waitForCompletion( cleanupThread, "database cleanup" );
        if ( isWorkerStopRequested() || getErrors() > 0 ) {
          stopWorker();
        }
      }
    }
    super.dispose( smi, sdi );
  }

  private void closePipedInputStream() {
    closePipedInputStream( data == null ? null : data.pipedInputStream );
  }

  private void closePipedInputStream( PipedInputStream inputStream ) {
    if ( inputStream != null ) {
      try {
        inputStream.close();
      } catch ( IOException e ) {
        logDebug( "Unable to close the Vertica bulk-load input stream.", e );
      }
    }
  }

  /**
   * Prevents new finalization claims and releases the pipe, then bounds waits for the owned threads.
   * JDBC cleanup runs separately because cancel, rollback, and disconnect can each block.
   * Repeated stop/dispose calls reuse the first deadline rather than granting another timeout.
   */
  private void stopWorker() {
    long shutdownTimeoutMillis = workerShutdownTimeoutMillis();
    Thread workerThread;
    long deadline;
    synchronized ( workerLifecycleLock ) {
      // The same atomic state decides whether stop or beginCopyFinalization() wins.
      if ( copyState.getAndSet( CopyState.STOPPED ) != CopyState.STOPPED ) {
        workerShutdownDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos( shutdownTimeoutMillis );
      }
      setStopped( true );
      deadline = workerShutdownDeadline;
      workerThread = data == null ? null : data.workerThread;
    }

    // Closing the pipe wakes a producer blocked because the worker stopped reading.
    closePipedInputStream();
    // stopAll() may invoke this on the COPY worker itself; do not interrupt or wait for that caller.
    if ( workerThread != null && workerThread != Thread.currentThread() ) {
      workerThread.interrupt();
    }
    Thread cleanupThread = startDatabaseCleanup();
    // Both waits spend the remaining part of one budget; cleanup does not receive a fresh timeout.
    waitForThreadUntil( workerThread, deadline );
    waitForThreadUntil( cleanupThread, deadline );
    if ( isAliveOtherThanCurrentThread( workerThread ) || isAliveOtherThanCurrentThread( cleanupThread ) ) {
      // Daemonization contains the wait; it does not prove the driver stopped or the server released its session.
      logError( "Vertica bulk-load cleanup did not stop within " + shutdownTimeoutMillis
        + " ms; remaining daemon threads are quarantined." );
      setErrors( 1 );
    }
  }

  /**
   * Starts at most one cleanup owner, even when stop and disposal overlap.
   * Stop winning the claim requires rollback; normal cleanup winning means stop must wait for that same owner.
   * The lock only publishes/starts the thread; the blocking JDBC work runs outside it.
   */
  private Thread startDatabaseCleanup() {
    synchronized ( workerLifecycleLock ) {
      if ( databaseCleanupThread == null && data != null && data.db != null ) {
        final Database database = data.db;
        final boolean forcedCleanup = isWorkerStopRequested() || getErrors() > 0;
        databaseCleanupThread = createDatabaseCleanupThread( () -> {
          if ( forcedCleanup ) {
            cleanupDatabase( database );
          } else {
            database.disconnect();
          }
        } );
        databaseCleanupThread.setDaemon( true );
        databaseCleanupThread.setUncaughtExceptionHandler( ( thread, failure ) -> {
          logError( "Unexpected error during Vertica bulk-load database cleanup.", failure );
          setErrors( 1 );
        } );
        databaseCleanupThread.start();
      }
      return databaseCleanupThread;
    }
  }

  private void cleanupDatabase( Database database ) {
    try {
      cancelRunningCopy( database );
    } catch ( SQLException | IllegalStateException e ) {
      logError( "Unable to cancel the active Vertica COPY operation.", e );
      setErrors( 1 );
    } finally {
      // Cancellation failure must not prevent the remaining cleanup attempts.
      rollbackAndDisconnect( database );
    }
  }

  private void rollbackAndDisconnect( Database database ) {
    try {
      Connection connection = database.getConnection();
      // Database.disconnect() can commit, so even a manual stop must roll back first.
      if ( connection != null && !connection.isClosed() ) {
        database.rollback();
      }
    } catch ( KettleDatabaseException | SQLException e ) {
      logError( "Unexpected error rolling back the database connection.", e );
      setErrors( 1 );
    } finally {
      database.disconnect();
    }
  }

  /**
   * Uses the pinned JDBC 6 driver's underlying connection because VerticaCopyStream has no public cancel API.
   * This may block too, so callers perform it on the daemon cleanup thread.
   */
  @VisibleForTesting
  void cancelRunningCopy( Database database ) throws SQLException {
    Connection jdbcConnection = database.getConnection();
    if ( jdbcConnection == null || jdbcConnection.isClosed() ) {
      return;
    }
    VerticaConnection connection = getVerticaConnection( jdbcConnection );
    if ( connection instanceof SConnection
      && ( (SConnection) connection ).getDSIConnection() instanceof VConnection ) {
      try {
        ( (VConnection) ( (SConnection) connection ).getDSIConnection() ).cancelCurrentStatement();
      } catch ( GeneralException e ) {
        throw new SQLException( "Unable to cancel the active Vertica COPY operation.", e );
      }
    } else {
      logDebug( "The Vertica connection does not expose legacy COPY cancellation; disconnecting instead." );
    }
  }

  private void waitForThreadUntil( Thread thread, long deadline ) {
    if ( !isAliveOtherThanCurrentThread( thread ) ) {
      return;
    }
    long remainingNanos = deadline - System.nanoTime();
    if ( remainingNanos <= 0 ) {
      return;
    }
    try {
      // Keep the sub-millisecond remainder: converting it to join(0) would mean waiting forever.
      TimeUnit.NANOSECONDS.timedJoin( thread, remainingNanos );
    } catch ( InterruptedException e ) {
      Thread.currentThread().interrupt();
    }
  }

  private boolean isAliveOtherThanCurrentThread( Thread thread ) {
    return thread != null && thread != Thread.currentThread() && thread.isAlive();
  }

  /**
   * Lets healthy EOF finish COPY before disconnect, without imposing a timeout on valid long loads.
   * Short joins observe concurrent stop/error requests so dispose() can switch to forced cleanup.
   */
  private void waitForWorker() {
    Thread workerThread;
    synchronized ( workerLifecycleLock ) {
      workerThread = data == null ? null : data.workerThread;
    }
    waitForCompletion( workerThread, "COPY" );
  }

  private void waitForCompletion( Thread thread, String operation ) {
    try {
      while ( isAliveOtherThanCurrentThread( thread ) && !isWorkerStopRequested() && getErrors() == 0 ) {
        thread.join( 100L );
      }
    } catch ( InterruptedException e ) {
      Thread.currentThread().interrupt();
      logError( "Interrupted while waiting for Vertica " + operation + " completion.", e );
      setErrors( 1 );
      setStopped( true );
    }
  }

  @VisibleForTesting
  Thread createWorkerThread( Runnable worker ) {
    return Executors.defaultThreadFactory().newThread( worker );
  }

  @VisibleForTesting
  Thread createDatabaseCleanupThread( Runnable cleanup ) {
    return Executors.defaultThreadFactory().newThread( cleanup );
  }

  @VisibleForTesting
  long workerShutdownTimeoutMillis() {
    return WORKER_SHUTDOWN_TIMEOUT_MILLIS;
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
    return getVerticaConnection( data.db.getConnection() );
  }

  private VerticaConnection getVerticaConnection( Connection connection ) throws SQLException {
    if ( connection == null ) {
      throw new IllegalStateException( "Could not retrieve a VerticaConnection from null" );
    }
    if ( connection instanceof VerticaConnection ) {
      return (VerticaConnection) connection;
    }
    Connection underlyingConnection = unwrapConnection( connection );
    if ( underlyingConnection instanceof VerticaConnection ) {
      return (VerticaConnection) underlyingConnection;
    }
    throw new IllegalStateException( "Could not retrieve a VerticaConnection from " + connection.getClass().getName() );
  }

  private Connection unwrapConnection( Connection connection ) throws SQLException {
    if ( connection instanceof DelegatingConnection ) {
      return ( (DelegatingConnection) connection ).getInnermostDelegate();
    }
    if ( connection instanceof PooledConnection ) {
      return ( (PooledConnection) connection ).getConnection();
    }
    try {
      if ( connection.isWrapperFor( VerticaConnection.class ) ) {
        return connection.unwrap( VerticaConnection.class );
      }
    } catch ( SQLException e ) {
      logDebug( "Unable to unwrap the JDBC connection as a VerticaConnection.", e );
    }
    // The caller reports an unsupported connection; generic JDBC is not sufficient for VerticaCopyStream.
    return null;
  }

}
