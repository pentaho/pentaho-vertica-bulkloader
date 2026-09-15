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



package org.pentaho.di.verticabulkload;

import com.vertica.jdbc.VerticaConnection;
import com.vertica.jdbc.VerticaCopyStream;
import org.apache.commons.dbcp.DelegatingConnection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaPluginType;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.verticabulkload.nativebinary.ColumnSpec;
import org.pentaho.di.verticabulkload.nativebinary.StreamEncoder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PipedInputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.matchers.JUnitMatchers.containsString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test for {@link VerticaBulkLoader}.
 */
public class VerticaBulkLoaderTest {

  private VerticaBulkLoaderMeta loaderMeta;
  private VerticaBulkLoaderData loaderData;
  private VerticaBulkLoader loader;
  private File tempException;
  private File tempRejected;
  private String kettleValueExceptionMsg;
  private KettleValueException kettleValueException;
  private Object[] rowData;

  @BeforeClass
  public static void initEnvironment() throws Exception {
    KettleEnvironment.init();
  }

  @AfterClass
  public static void shutdownEnvironment() throws Exception {
    KettleEnvironment.shutdown();
  }

  @Before
  public void setUp() throws KettleException, IOException, SQLException {
    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.init( true );

    loaderData = new VerticaBulkLoaderData();
    loaderMeta = spy( new VerticaBulkLoaderMeta() );

    tempException = File.createTempFile( "except-", "-log" );
    tempRejected = File.createTempFile( "reject-", "-log" );

    TransMeta transMeta = new TransMeta();
    transMeta.setName( "loader" );

    PluginRegistry pluginRegistry = PluginRegistry.getInstance();

    String loaderPid = pluginRegistry.getPluginId( StepPluginType.class, loaderMeta );
    StepMeta stepMeta = new StepMeta( loaderPid, "loader", loaderMeta );
    Trans trans = new Trans( transMeta );
    transMeta.addStep( stepMeta );
    trans.setRunning( true );

    loaderMeta.setDatabaseMeta( mock( DatabaseMeta.class ) );

    loader = spy( new VerticaBulkLoader( stepMeta, loaderData, 1, transMeta, trans ) );

    loaderMeta.setExceptionsFileName( tempException.getAbsolutePath() );
    loaderMeta.setRejectedDataFileName( tempRejected.getAbsolutePath() );
    loader.init( loaderMeta, loaderData );
    loader.setStopped( false );

    VerticaCopyStream copyStream = mock( VerticaCopyStream.class );
    when( copyStream.getRejects() ).thenReturn( Collections.emptyList() );
    doReturn( copyStream ).when( loader ).createVerticaCopyStream( anyString() );
    kettleValueExceptionMsg = "Test Kettle Value Exception";
    kettleValueException = new KettleValueException( kettleValueExceptionMsg, new Exception( "Throwable Exception" ) );
    rowData = new Object[] {"this", "is", "bad", "data" };
  }

  @After
  public void tearDown() {
    if ( tempException != null ) {
      tempException.delete();
    }
    if ( tempRejected != null ) {
      tempRejected.delete();
    }
  }

  @Test
  public void testNoDatabaseConnection() {
    loaderMeta.setDatabaseMeta( null );
    // Verify that the initializing will return false due to the connection not being defined.
    assertFalse( loader.init( loaderMeta, loaderData ) );
    try {
      // Verify that the database connection being set to null throws a KettleException with the following message.
      loader.verifyDatabaseConnection();
    } catch ( KettleException aKettleException ) {
      assertThat( aKettleException.getMessage(), containsString( "There is no connection defined in this step" ) );
    }
  }

  /**
   * This tests the getConnection call with different circumstances - First, with a regular VerticaConnection - Next,
   * with a DelegatingConnection (from DBCP) with a VerticaConnection as the innermostDelegate - Next, with a
   * DelegatingConnection with a java.sql.Connection mock - Finally, with a java.sql.Connection mock
   * 
   * @throws Exception
   */
  @Test
  public void testGetConnection() throws Exception {
    Connection connection1 = mock( VerticaConnection.class );
    DelegatingConnection connection2 = mock( DelegatingConnection.class );
    when( connection2.getInnermostDelegate() ).thenReturn( connection1 );
    DelegatingConnection connection3 = mock( DelegatingConnection.class );
    when( connection3.getInnermostDelegate() ).thenReturn( mock( java.sql.Connection.class ) );
    Connection connection4 = mock( java.sql.Connection.class );

    loaderData.db.setConnection( connection1 );
    Connection rtn = loader.getVerticaConnection();
    assertTrue( connection1 == rtn ); // Should just return the object in loaderData.db
    loaderData.db.setConnection( connection2 );
    rtn = loader.getVerticaConnection();
    assertTrue( connection1 == rtn ); // Should return the innermost delegate. If it didn't, throw exception
    loaderData.db.setConnection( connection3 );
    try {
      rtn = loader.getVerticaConnection();
      fail( "Expected IllegalStateException" );
    } catch ( IllegalStateException expected ) {

    }

    loaderData.db.setConnection( connection4 );
    try {
      rtn = loader.getVerticaConnection();
      fail( "Expected IllegalStateException" );
    } catch ( IllegalStateException expected ) {

    }

  }

  /**
   * Testing boundary condition of buffer size handling.
   * <p>
   * Given 4 varchar fields of different sizes. When loaded data amount is getting close to a buffer size, then the
   * buffer should not be overflowed.
   * </p>
   */
  @Test
  @SuppressWarnings( "unchecked" )
  public void shouldFlushBufferBeforeItOverflows() throws KettleException, IOException {
    // given
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "Test1" ) );
    rowMeta.addValueMeta( new ValueMetaString( "Test2" ) );
    rowMeta.addValueMeta( new ValueMetaString( "Test3" ) );
    rowMeta.addValueMeta( new ValueMetaString( "Test4" ) );
    loader.setInputRowMeta( rowMeta );

    RowMeta tableMeta = new RowMeta();
    tableMeta.addValueMeta( getValueMetaString( "TestData1", 19 ) );
    tableMeta.addValueMeta( getValueMetaString( "TestData2", 4 ) );
    tableMeta.addValueMeta( getValueMetaString( "TestData3", 7 ) );
    tableMeta.addValueMeta( getValueMetaString( "TestData4", 8 ) );
    doReturn( tableMeta ).when( loaderMeta ).getTableRowMetaInterface();

    loader.init( loaderMeta, loaderData );
    loader.setStopped( false );
    when( loader.getRow() ).thenReturn( new String[] { "19 characters------", "4 ch", "7 chara", "8 charac" } );

    doAnswer( invocation -> {
      List colSpecs = (List) invocation.getArguments()[0];
      PipedInputStream pipedInputStream = (PipedInputStream) invocation.getArguments()[1];
      return new MockChannelStreamEncoder( colSpecs, pipedInputStream );
    } ).when( loader ).createStreamEncoder( any(), any() );

    // when
    try {
      for ( int i = 0; i < StreamEncoder.NUM_ROWS_TO_BUFFER + 1; i++ ) {
        loader.processRow( loaderMeta, loaderData );
      }
    } catch ( BufferOverflowException e ) {
      fail( e.getMessage() );
    }

    // then no BufferOverflowException should be thrown
  }

  /**
   * [PDI-17400] Testing the refactored ability of Abort on Error with Vertica. We verify that we handle the data row
   * correctly if the feature is on or off (false if it's on, true if it's off).
   */
  @Test
  public void abortOnErrorTest() {
    try {
      RowMeta rowMeta = new RowMeta();
      rowMeta.addValueMeta( new ValueMetaString( "string_column" ) );
      rowMeta.addValueMeta( new ValueMetaInteger( "integer_column" ) );
      Object[] goodObjectData = { "onetwothreefour", 124L };
      Object[] badObjectData = { "onetwothreefour", "onetwothreefour" };
      loader.setInputRowMeta( rowMeta );

      RowMeta tableMeta = new RowMeta();
      tableMeta.addValueMeta( getValueMetaString( "StringData", 15 ) );
      tableMeta.addValueMeta( getValueMetaInteger( "IntegerData", 15 ) );
      doReturn( tableMeta ).when( loaderMeta ).getTableRowMetaInterface();

      loader.init( loaderMeta, loaderData );
      loader.setStopped( false );
      when( loader.getRow() ).thenReturn( goodObjectData );

      doAnswer( invocation -> {
        List colSpecs = (List) invocation.getArguments()[ 0 ];
        PipedInputStream pipedInputStream = (PipedInputStream) invocation.getArguments()[ 1 ];
        return new MockChannelStreamEncoder( colSpecs, pipedInputStream );
      } ).when( loader ).createStreamEncoder( any(), any() );
      // Verify that the good row returns with a true load value
      assertTrue( loader.processRow( loaderMeta, loaderData ) );
      loader.stopRunning( loaderMeta, loaderData );
      loader.setStopped( false );


      when( loader.getRow() ).thenReturn( badObjectData );
      loaderMeta.setAbortOnError( true );

      assertFalse( loader.processRow( loaderMeta, loaderData ) );

      loaderMeta.setAbortOnError( false );
      loader.setStopped( false );
      assertTrue( loader.processRow( loaderMeta, loaderData ) );
    } catch ( Exception ex ) {
      fail( "No unforeseen exceptions should be thrown" );
    }
  }

  /**
   * Testing the functionality of the Exception and Rejection logs and how we handle the input form the user behind
   * the scenes.
   */
  @Test
  public void logFilesInitializeAndWritingTest() {
    String rowString = "this | is | bad | data";

    // Verify that setting the values does not throw errors in our process
    // Verify that we are able to print out the exception and rejection logs as well.
    loaderMeta.setExceptionsFileName( tempException.getAbsolutePath() );
    loaderMeta.setRejectedDataFileName( tempRejected.getAbsolutePath() );
    try {
      loader.initializeLogFiles();
      loader.writeExceptionRejectionLogs( kettleValueException, rowData );
      BufferedReader exceptReader = new BufferedReader( new FileReader( tempException ) );
      assertTrue( exceptReader.lines().anyMatch( streamLine -> streamLine.contains( kettleValueExceptionMsg ) ) );
      BufferedReader rejectReader = new BufferedReader( new FileReader( tempRejected ) );
      assertTrue( rejectReader.lines().anyMatch( streamLine -> streamLine.contains( rowString ) ) );
      loader.closeLogFiles();
    } catch ( KettleException | IOException nullIssueException ) {
      fail( "Nulling the Exception/Rejection logs should not throw an Exception: " + nullIssueException );
    }

    // Next verify that setting either FileName to a bad path will throw an exception
    loaderMeta.setExceptionsFileName( File.separator + "Bad_Location" );
    loaderMeta.setRejectedDataFileName( tempRejected.getAbsolutePath() );
    try {
      loader.initializeLogFiles();
      fail( "Exception Filename is Null: Giving an incorrect file path should throw this exception,"
        + " if not, something else is wrong." );
    } catch ( KettleException ex ) {
      // also verify the init method throws a false
      assertFalse( loader.init( loaderMeta, loaderData ) );
    }

    loaderMeta.setExceptionsFileName( tempException.getAbsolutePath() );
    loaderMeta.setRejectedDataFileName( File.separator + "Bad_Location" );
    try {
      loader.initializeLogFiles();
      fail( "Rejected Filename is Null: Giving an incorrect file path should throw this exception,"
        + " if not, something else is wrong." );
    } catch ( KettleException ex ) {
      // also verify the init method throws a false
      assertFalse( loader.init( loaderMeta, loaderData ) );
    }
  }

  @Test
  public void nullLogFilesAllowed() {
    runUndefinedLogFilesTest( null, null, "Nulling" );
  }

  @Test
  public void emptyLogFilesAllowed() {
    runUndefinedLogFilesTest( "", "", "Emptying" );
  }

  @Test
  public void blankjLogFilesAllowed() {
    runUndefinedLogFilesTest( "   ", "  \n", "Blanking" );
  }

  @Test( timeout = 5000 )
  public void shouldStopWhenVerticaCopyFailsWhileTheProducerFlushes() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "input" ) );
    loader.setInputRowMeta( rowMeta );

    RowMeta tableMeta = new RowMeta();
    tableMeta.addValueMeta( getValueMetaString( "target", 10 ) );
    doReturn( tableMeta ).when( loaderMeta ).getTableRowMetaInterface();
    loader.init( loaderMeta, loaderData );
    loader.setStopped( false );

    CountDownLatch flushStarted = new CountDownLatch( 1 );
    VerticaCopyStream copyStream = mock( VerticaCopyStream.class );
    when( copyStream.getRejects() ).thenReturn( Collections.emptyList() );
    doAnswer( invocation -> {
      if ( !flushStarted.await( 2, TimeUnit.SECONDS ) ) {
        throw new SQLException( "The test producer did not reach the pipe flush" );
      }
      throw new SQLException( "Request failed due to the current cluster health state" );
    } ).when( copyStream ).execute();
    doReturn( copyStream ).when( loader ).createVerticaCopyStream( anyString() );
    doAnswer( invocation -> {
      loader.stopRunning( loaderMeta, loaderData );
      return null;
    } ).when( loader ).stopAll();
    doAnswer( invocation -> new SignallingStreamEncoder( (List<ColumnSpec>) invocation.getArguments()[ 0 ],
      (PipedInputStream) invocation.getArguments()[ 1 ], flushStarted ) ).when( loader )
      .createStreamEncoder( any(), any() );
    when( loader.getRow() ).thenReturn( new Object[] { "0123456789" } );

    boolean result = true;
    try {
      for ( int row = 0; row < StreamEncoder.NUM_ROWS_TO_BUFFER * 2; row++ ) {
        result = loader.processRow( loaderMeta, loaderData );
        if ( !result ) {
          break;
        }
      }

      Thread workerThread = loaderData.workerThread;
      workerThread.join( 2000 );
      assertFalse( workerThread.isAlive() );
      assertTrue( loader.getErrors() > 0 );
      assertTrue( loader.isStopped() );
      assertFalse( loader.processRow( loaderMeta, loaderData ) );
    } finally {
      loader.stopRunning( loaderMeta, loaderData );
    }
  }

  @Test( timeout = 5000 )
  public void shouldNotJoinTheWorkerWhenCopyFailureStopsTheTransformation() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "input" ) );
    loader.setInputRowMeta( rowMeta );

    RowMeta tableMeta = new RowMeta();
    tableMeta.addValueMeta( getValueMetaString( "target", 10 ) );
    doReturn( tableMeta ).when( loaderMeta ).getTableRowMetaInterface();
    loader.init( loaderMeta, loaderData );
    loader.setStopped( false );

    VerticaCopyStream copyStream = mock( VerticaCopyStream.class );
    when( copyStream.getRejects() ).thenReturn( Collections.emptyList() );
    doThrow( new SQLException( "Request failed due to the current cluster health state" ) )
      .when( copyStream ).execute();
    doReturn( copyStream ).when( loader ).createVerticaCopyStream( anyString() );
    doAnswer( invocation -> {
      loader.stopRunning( loaderMeta, loaderData );
      return null;
    } ).when( loader ).stopAll();
    when( loader.getRow() ).thenReturn( new Object[] { "value" } );

    loader.processRow( loaderMeta, loaderData );

    Thread workerThread = loaderData.workerThread;
    workerThread.join( 2000 );

    assertTrue( workerThread.isDaemon() );
    assertFalse( workerThread.isAlive() );
    assertTrue( loader.getErrors() > 0 );
    assertTrue( loader.isStopped() );
  }

  @Test( timeout = 8000 )
  public void shouldNotWaitIndefinitelyWhenWorkerIgnoresInterrupt() throws Exception {
    CountDownLatch workerStarted = new CountDownLatch( 1 );
    CountDownLatch releaseWorker = new CountDownLatch( 1 );
    Thread unresponsiveWorker = new Thread( () -> {
      workerStarted.countDown();
      while ( releaseWorker.getCount() > 0 ) {
        try {
          releaseWorker.await();
        } catch ( InterruptedException ignored ) {
          // Simulate a JDBC operation that does not respond to interruption.
        }
      }
    } );
    unresponsiveWorker.setDaemon( true );
    loaderData.workerThread = unresponsiveWorker;
    unresponsiveWorker.start();
    assertTrue( workerStarted.await( 1, TimeUnit.SECONDS ) );

    try {
      loader.stopRunning( loaderMeta, loaderData );

      assertTrue( unresponsiveWorker.isAlive() );
      assertTrue( loader.getErrors() > 0 );
    } finally {
      releaseWorker.countDown();
      unresponsiveWorker.interrupt();
      unresponsiveWorker.join( 1000 );
    }
  }

  @Test( timeout = 3000 )
  public void shouldWaitForSuccessfulWorkerBeforeDisconnectingDuringDispose() throws Exception {
    Database database = mock( Database.class );
    loaderData.db = database;
    loader.setErrors( 0 );
    loader.setStopped( false );
    CountDownLatch workerStarted = new CountDownLatch( 1 );
    CountDownLatch releaseWorker = new CountDownLatch( 1 );
    CountDownLatch disconnectCalled = new CountDownLatch( 1 );
    Thread successfulWorker = new Thread( () -> {
      workerStarted.countDown();
      try {
        releaseWorker.await();
      } catch ( InterruptedException e ) {
        Thread.currentThread().interrupt();
      }
    } );
    successfulWorker.setDaemon( true );
    loaderData.workerThread = successfulWorker;
    successfulWorker.start();
    assertTrue( workerStarted.await( 1, TimeUnit.SECONDS ) );
    doAnswer( invocation -> {
      disconnectCalled.countDown();
      return null;
    } ).when( database ).disconnect();

    Thread disposeThread = new Thread( () -> loader.dispose( loaderMeta, loaderData ) );
    disposeThread.setDaemon( true );
    disposeThread.start();

    try {
      assertFalse( disconnectCalled.await( 200, TimeUnit.MILLISECONDS ) );
      releaseWorker.countDown();
      disposeThread.join( 1000 );
      assertFalse( disposeThread.isAlive() );
      assertTrue( disconnectCalled.await( 1, TimeUnit.SECONDS ) );
    } finally {
      releaseWorker.countDown();
      successfulWorker.interrupt();
      successfulWorker.join( 1000 );
    }
  }

  @Test
  public void shouldNotRollbackClosedConnectionDuringFailureDispose() throws Exception {
    Database database = mock( Database.class );
    Connection connection = mock( Connection.class );
    when( database.getConnection() ).thenReturn( connection );
    when( connection.isClosed() ).thenReturn( true );
    loaderData.db = database;
    loader.setErrors( 1 );
    loader.setStopped( true );

    loader.dispose( loaderMeta, loaderData );

    verify( database, never() ).rollback();
  }

  @Test( timeout = 5000 )
  public void shouldStopBlockedProducerWithoutReportingManualStopAsAnError() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "input" ) );
    loader.setInputRowMeta( rowMeta );

    RowMeta tableMeta = new RowMeta();
    tableMeta.addValueMeta( getValueMetaString( "target", 10 ) );
    doReturn( tableMeta ).when( loaderMeta ).getTableRowMetaInterface();
    loader.init( loaderMeta, loaderData );
    loader.setErrors( 0 );
    loader.setStopped( false );

    CountDownLatch flushStarted = new CountDownLatch( 1 );
    VerticaCopyStream copyStream = mock( VerticaCopyStream.class );
    when( copyStream.getRejects() ).thenReturn( Collections.emptyList() );
    doAnswer( invocation -> {
      try {
        new CountDownLatch( 1 ).await();
        return null;
      } catch ( InterruptedException e ) {
        throw new SQLException( "Connection closed during COPY" );
      }
    } ).when( copyStream ).execute();
    doReturn( copyStream ).when( loader ).createVerticaCopyStream( anyString() );
    doAnswer( invocation -> new SignallingStreamEncoder( (List<ColumnSpec>) invocation.getArguments()[ 0 ],
      (PipedInputStream) invocation.getArguments()[ 1 ], flushStarted ) ).when( loader )
      .createStreamEncoder( any(), any() );
    when( loader.getRow() ).thenReturn( new Object[] { "0123456789" } );

    AtomicBoolean processResult = new AtomicBoolean( true );
    AtomicReference<Throwable> processFailure = new AtomicReference<>();
    Thread producerThread = new Thread( () -> {
      try {
        for ( int row = 0; row < StreamEncoder.NUM_ROWS_TO_BUFFER * 2; row++ ) {
          processResult.set( loader.processRow( loaderMeta, loaderData ) );
          if ( !processResult.get() ) {
            break;
          }
        }
      } catch ( Throwable t ) {
        processFailure.set( t );
      }
    } );
    producerThread.setDaemon( true );
    producerThread.start();
    assertTrue( flushStarted.await( 2, TimeUnit.SECONDS ) );

    loader.stopRunning( loaderMeta, loaderData );
    producerThread.join( 1000 );

    assertFalse( producerThread.isAlive() );
    assertTrue( processFailure.get() == null );
    assertFalse( processResult.get() );
    assertTrue( loader.getErrors() == 0 );
  }

  @Test( timeout = 5000 )
  public void shouldStopWhenCopyStreamCreationFails() throws Exception {
    assertCopyStageFailureStopsStep( CopyStage.CREATE, new SQLException( "Unable to create COPY stream" ) );
  }

  @Test( timeout = 5000 )
  public void shouldStopWhenCopyStartFails() throws Exception {
    assertCopyStageFailureStopsStep( CopyStage.START, new SQLException( "Unable to start COPY" ) );
  }

  @Test( timeout = 5000 )
  public void shouldStopWhenCopyInputRegistrationFails() throws Exception {
    assertCopyStageFailureStopsStep( CopyStage.ADD_STREAM, new SQLException( "Unable to register COPY input" ) );
  }

  @Test( timeout = 5000 )
  public void shouldStopWhenRejectLookupFails() throws Exception {
    assertCopyStageFailureStopsStep( CopyStage.GET_REJECTS,
      new IllegalStateException( "Unable to inspect rejected rows" ) );
  }

  @Test( timeout = 5000 )
  public void shouldStopWhenCopyExecuteFails() throws Exception {
    assertCopyStageFailureStopsStep( CopyStage.EXECUTE, new SQLException( "Unable to execute COPY" ) );
  }

  @Test( timeout = 5000 )
  public void shouldStopWhenCopyFinishFails() throws Exception {
    assertCopyStageFailureStopsStep( CopyStage.FINISH, new SQLException( "Unable to finish COPY" ) );
  }

  @Test( timeout = 5000 )
  public void shouldStopWhenDriverThrowsRuntimeException() throws Exception {
    assertCopyStageFailureStopsStep( CopyStage.EXECUTE, new IllegalStateException( "Driver state is invalid" ) );
  }

  @Test( timeout = 5000 )
  public void shouldWakeProducerWhenCopyFailsDuringEndOfInputFlush() throws Exception {
    configureSingleColumnLoader();
    CountDownLatch flushStarted = new CountDownLatch( 1 );
    VerticaCopyStream copyStream = mock( VerticaCopyStream.class );
    when( copyStream.getRejects() ).thenReturn( Collections.emptyList() );
    doAnswer( invocation -> {
      if ( !flushStarted.await( 2, TimeUnit.SECONDS ) ) {
        throw new SQLException( "The EOF flush did not start" );
      }
      throw new SQLException( "COPY failed during EOF flush" );
    } ).when( copyStream ).execute();
    doReturn( copyStream ).when( loader ).createVerticaCopyStream( anyString() );
    doAnswer( invocation -> new SignallingStreamEncoder( (List<ColumnSpec>) invocation.getArguments()[ 0 ],
      (PipedInputStream) invocation.getArguments()[ 1 ], flushStarted ) ).when( loader )
      .createStreamEncoder( any(), any() );
    AtomicInteger rowsRemaining = new AtomicInteger( 250 );
    when( loader.getRow() ).thenAnswer( invocation -> rowsRemaining.getAndDecrement() > 0
      ? new Object[] { "0123456789" } : null );

    for ( int row = 0; row < 250; row++ ) {
      assertTrue( loader.processRow( loaderMeta, loaderData ) );
    }
    try {
      loader.processRow( loaderMeta, loaderData );
      fail( "Expected the closed COPY pipe to fail the EOF flush" );
    } catch ( KettleException expected ) {
      assertThat( expected.getMessage(), containsString( "Error releasing resources" ) );
    }

    loaderData.workerThread.join( 2000 );
    assertFalse( loaderData.workerThread.isAlive() );
    assertTrue( loader.getErrors() > 0 );
    assertTrue( loader.isStopped() );
  }

  private void assertCopyStageFailureStopsStep( CopyStage stage, Exception failure ) throws Exception {
    configureSingleColumnLoader();
    VerticaCopyStream copyStream = mock( VerticaCopyStream.class );
    when( copyStream.getRejects() ).thenReturn( Collections.emptyList() );

    switch ( stage ) {
      case CREATE:
        doThrow( failure ).when( loader ).createVerticaCopyStream( anyString() );
        break;
      case START:
        doAnswer( invocation -> {
          throw failure;
        } ).when( copyStream ).start();
        break;
      case ADD_STREAM:
        doAnswer( invocation -> {
          throw failure;
        } ).when( copyStream ).addStream( any() );
        break;
      case GET_REJECTS:
        when( copyStream.getRejects() ).thenAnswer( invocation -> {
          throw failure;
        } );
        break;
      case EXECUTE:
        doAnswer( invocation -> {
          throw failure;
        } ).when( copyStream ).execute();
        break;
      case FINISH:
        when( copyStream.finish() ).thenAnswer( invocation -> {
          throw failure;
        } );
        break;
      default:
        throw new IllegalArgumentException( "Unhandled COPY stage " + stage );
    }

    if ( stage != CopyStage.CREATE ) {
      doReturn( copyStream ).when( loader ).createVerticaCopyStream( anyString() );
    }
    when( loader.getRow() ).thenReturn( new Object[] { "0123456789" } );

    loader.processRow( loaderMeta, loaderData );
    loaderData.workerThread.join( 2000 );

    assertFalse( loaderData.workerThread.isAlive() );
    assertTrue( loader.getErrors() > 0 );
    assertTrue( loader.isStopped() );
  }

  private void configureSingleColumnLoader() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "input" ) );
    loader.setInputRowMeta( rowMeta );

    RowMeta tableMeta = new RowMeta();
    tableMeta.addValueMeta( getValueMetaString( "target", 10 ) );
    doReturn( tableMeta ).when( loaderMeta ).getTableRowMetaInterface();
    loader.setErrors( 0 );
    loader.setStopped( false );
    doAnswer( invocation -> {
      loader.stopRunning( loaderMeta, loaderData );
      return null;
    } ).when( loader ).stopAll();
  }

  private void runUndefinedLogFilesTest( String exceptionsFileName, String rejectedDataFileName, String prefix ) {
    loaderMeta.setExceptionsFileName( exceptionsFileName );
    loaderMeta.setRejectedDataFileName( rejectedDataFileName );
    try {
      loader.initializeLogFiles();
      loader.writeExceptionRejectionLogs( kettleValueException, rowData );
      loader.closeLogFiles();
    } catch ( KettleException | IOException nullIssueException ) {
      fail( prefix + " the Exception/Rejection logs should not throw an Exception: " + nullIssueException );
    }
  }

  /**
   * Testing boundary condition of buffer size handling.
   * <p>
   * Given 7 varchar fields of small sizes. When loaded data amount is getting close to a buffer size, then the buffer
   * should not be overflowed.
   * </p>
   */
  @Test
  @SuppressWarnings( "unchecked" )
  public void shouldFlushBufferBeforeItOverflowsOnSmallFieldSizes() throws KettleException, IOException {
    // given
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "Test1" ) );
    rowMeta.addValueMeta( new ValueMetaString( "Test2" ) );
    rowMeta.addValueMeta( new ValueMetaString( "Test3" ) );
    rowMeta.addValueMeta( new ValueMetaString( "Test4" ) );
    rowMeta.addValueMeta( new ValueMetaString( "Test5" ) );
    rowMeta.addValueMeta( new ValueMetaString( "Test6" ) );
    rowMeta.addValueMeta( new ValueMetaString( "Test7" ) );
    loader.setInputRowMeta( rowMeta );

    RowMeta tableMeta = new RowMeta();
    tableMeta.addValueMeta( getValueMetaString( "TestData1", 1 ) );
    tableMeta.addValueMeta( getValueMetaString( "TestData2", 1 ) );
    tableMeta.addValueMeta( getValueMetaString( "TestData3", 1 ) );
    tableMeta.addValueMeta( getValueMetaString( "TestData4", 1 ) );
    tableMeta.addValueMeta( getValueMetaString( "TestData5", 1 ) );
    tableMeta.addValueMeta( getValueMetaString( "TestData6", 1 ) );
    tableMeta.addValueMeta( getValueMetaString( "TestData7", 1 ) );
    doReturn( tableMeta ).when( loaderMeta ).getTableRowMetaInterface();

    loader.init( loaderMeta, loaderData );
    loader.setStopped( false );
    when( loader.getRow() ).thenReturn( new String[] { "1", "1", "1", "1", "1", "1", "1" } );

    doAnswer( invocation -> {
      List colSpecs = (List) invocation.getArguments()[0];
      PipedInputStream pipedInputStream = (PipedInputStream) invocation.getArguments()[1];
      return new MockChannelStreamEncoder( colSpecs, pipedInputStream );
    } ).when( loader ).createStreamEncoder( any(), any() );

    // when
    try {
      for ( int i = 0; i < StreamEncoder.NUM_ROWS_TO_BUFFER + 1; i++ ) {
        loader.processRow( loaderMeta, loaderData );
      }
    } catch ( BufferOverflowException e ) {
      fail( e.getMessage() );
    }

    // then no BufferOverflowException should be thrown
  }

  private class MockChannelStreamEncoder extends StreamEncoder {
    private MockChannelStreamEncoder( List<ColumnSpec> columns, PipedInputStream inputStream ) throws IOException {
      super( columns, inputStream );
      channel = mock( WritableByteChannel.class );
    }
  }

  private static class SignallingStreamEncoder extends StreamEncoder {
    private SignallingStreamEncoder( List<ColumnSpec> columns, PipedInputStream inputStream,
        CountDownLatch flushStarted ) throws IOException {
      super( columns, inputStream );
      WritableByteChannel pipeChannel = channel;
      channel = new WritableByteChannel() {
        @Override
        public int write( ByteBuffer source ) throws IOException {
          flushStarted.countDown();
          return pipeChannel.write( source );
        }

        @Override
        public boolean isOpen() {
          return pipeChannel.isOpen();
        }

        @Override
        public void close() throws IOException {
          pipeChannel.close();
        }
      };
    }
  }

  private enum CopyStage {
    CREATE,
    START,
    ADD_STREAM,
    GET_REJECTS,
    EXECUTE,
    FINISH
  }

  private static ValueMetaString getValueMetaString( String testData3, int length ) {
    ValueMetaString tableValueMeta = new ValueMetaString( testData3 );
    tableValueMeta.setLength( length );
    tableValueMeta.setOriginalColumnTypeName( "VARCHAR" );
    return tableValueMeta;
  }

  private static ValueMetaInteger getValueMetaInteger( String testData, int length ) {
    ValueMetaInteger tableValueMeta = new ValueMetaInteger( testData );
    tableValueMeta.setLength( length );
    tableValueMeta.setOriginalColumnTypeName( "INTEGER" );
    return tableValueMeta;
  }
}
