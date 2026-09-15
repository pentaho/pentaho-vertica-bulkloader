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

import com.vertica.core.VConnection;
import com.vertica.jdbc.SConnection;
import com.vertica.jdbc.VerticaConnection;
import com.vertica.jdbc.VerticaCopyStream;
import org.apache.commons.dbcp.DelegatingConnection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InOrder;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.PooledConnection;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.matchers.JUnitMatchers.containsString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

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
  public void tearDown() throws KettleException {
    if ( loaderData.workerThread != null ) {
      loader.stopRunning( loaderMeta, loaderData );
    }
    loader.closeLogFiles();
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
    assertSame( connection1, rtn ); // Should just return the object in loaderData.db
    loaderData.db.setConnection( connection2 );
    rtn = loader.getVerticaConnection();
    assertSame( connection1, rtn ); // Should return the innermost delegate. If it didn't, throw exception
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

  @Test
  public void shouldUnwrapJdbcWrapperAndPooledConnection() throws Exception {
    VerticaConnection verticaConnection = mock( VerticaConnection.class );
    Connection wrapper = mock( Connection.class );
    when( wrapper.isWrapperFor( VerticaConnection.class ) ).thenReturn( true );
    when( wrapper.unwrap( VerticaConnection.class ) ).thenReturn( verticaConnection );
    loaderData.db.setConnection( wrapper );
    assertSame( verticaConnection, loader.getVerticaConnection() );

    Connection pooledConnection = mock( Connection.class, withSettings().extraInterfaces( PooledConnection.class ) );
    when( ( (PooledConnection) pooledConnection ).getConnection() ).thenReturn( verticaConnection );
    loaderData.db.setConnection( pooledConnection );
    assertSame( verticaConnection, loader.getVerticaConnection() );
  }

  @Test
  public void shouldPreserveConnectionErrorWhenUnwrapIsUnsupported() throws Exception {
    Connection connection = mock( Connection.class );
    when( connection.isWrapperFor( VerticaConnection.class ) ).thenThrow( new SQLException( "Unwrap unsupported" ) );
    loaderData.db.setConnection( connection );
    try {
      loader.getVerticaConnection();
      fail( "Expected an unsupported connection to be rejected" );
    } catch ( IllegalStateException expected ) {
      assertThat( expected.getMessage(), containsString( "Could not retrieve a VerticaConnection" ) );
    }
  }

  @Test
  public void shouldMapSelectedFieldsInConfiguredOrder() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta( new ValueMetaString( "first" ) );
    rowMeta.addValueMeta( new ValueMetaString( "second" ) );
    loader.setInputRowMeta( rowMeta );
    RowMeta tableMeta = new RowMeta();
    tableMeta.addValueMeta( getValueMetaString( "target_first", 10 ) );
    tableMeta.addValueMeta( getValueMetaString( "target_second", 10 ) );
    doReturn( tableMeta ).when( loaderMeta ).getTableRowMetaInterface();
    loaderMeta.setSpecifyFields( true );
    loaderMeta.setFieldStream( new String[] { "second", "first" } );
    loaderMeta.setFieldDatabase( new String[] { "target_second", "target_first" } );
    StreamEncoder encoder = mock( StreamEncoder.class );
    doReturn( encoder ).when( loader ).createStreamEncoder( any(), any() );
    doReturn( new Object[] { "one", "two" } ).when( loader ).getRow();

    assertTrue( loader.processRow( loaderMeta, loaderData ) );

    assertArrayEquals( new int[] { 1, 0 }, loaderData.selectedRowFieldIndices );
    assertEquals( "target_second", loaderData.insertRowMeta.getValueMeta( 0 ).getName() );
    assertEquals( "target_first", loaderData.insertRowMeta.getValueMeta( 1 ).getName() );
    verify( encoder ).writeRow( loaderData.insertRowMeta, new Object[] { "two", "one" } );
  }

  @Test
  public void shouldRejectMissingSelectedInputFieldBeforeStartingWorker() throws Exception {
    configureSingleColumnLoader();
    loaderMeta.setSpecifyFields( true );
    loaderMeta.setFieldStream( new String[] { "missing" } );
    loaderMeta.setFieldDatabase( new String[] { "target" } );
    doReturn( new Object[] { "value" } ).when( loader ).getRow();

    try {
      loader.processRow( loaderMeta, loaderData );
      fail( "Expected the missing input field to be rejected" );
    } catch ( KettleStepException expected ) {
      assertThat( expected.getMessage(), containsString( "missing" ) );
    }
    assertNull( loaderData.workerThread );
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
      doReturn( goodObjectData, badObjectData, badObjectData ).when( loader ).getRow();

      doAnswer( invocation -> {
        List colSpecs = (List) invocation.getArguments()[ 0 ];
        PipedInputStream pipedInputStream = (PipedInputStream) invocation.getArguments()[ 1 ];
        return new MockChannelStreamEncoder( colSpecs, pipedInputStream );
      } ).when( loader ).createStreamEncoder( any(), any() );
      // Verify that the good row returns with a true load value
      assertTrue( loader.processRow( loaderMeta, loaderData ) );
      loaderMeta.setAbortOnError( false );
      assertTrue( loader.processRow( loaderMeta, loaderData ) );
      loaderMeta.setAbortOnError( true );
      assertFalse( loader.processRow( loaderMeta, loaderData ) );
    } catch ( Exception ex ) {
      fail( "No unforeseen exceptions should be thrown: " + ex );
    }
  }

  /**
   * Verifies log contents and file-open failures without relying on root-directory permissions.
   */
  @Test
  public void logFilesInitializeAndWritingTest() throws KettleException, IOException {
    String rowString = "this | is | bad | data";
    // A regular file cannot be a parent directory, regardless of OS or write permissions.
    String invalidLogPath = new File( tempException, "invalid.log" ).getAbsolutePath();

    loader.closeLogFiles();
    loaderMeta.setExceptionsFileName( tempException.getAbsolutePath() );
    loaderMeta.setRejectedDataFileName( tempRejected.getAbsolutePath() );
    loader.initializeLogFiles();
    loader.writeExceptionRejectionLogs( kettleValueException, rowData );
    try ( BufferedReader exceptReader = new BufferedReader( new FileReader( tempException ) );
          BufferedReader rejectReader = new BufferedReader( new FileReader( tempRejected ) ) ) {
      assertTrue( exceptReader.lines().anyMatch( streamLine -> streamLine.contains( kettleValueExceptionMsg ) ) );
      assertTrue( rejectReader.lines().anyMatch( streamLine -> streamLine.contains( rowString ) ) );
    }
    loader.closeLogFiles();

    loaderMeta.setExceptionsFileName( invalidLogPath );
    loaderMeta.setRejectedDataFileName( tempRejected.getAbsolutePath() );
    try {
      loader.initializeLogFiles();
      fail( "Opening an exception log beneath a regular file should fail." );
    } catch ( KettleException ex ) {
      assertFalse( loader.init( loaderMeta, loaderData ) );
    }

    loaderMeta.setExceptionsFileName( tempException.getAbsolutePath() );
    loaderMeta.setRejectedDataFileName( invalidLogPath );
    try {
      loader.initializeLogFiles();
      fail( "Opening a rejection log beneath a regular file should fail." );
    } catch ( KettleException ex ) {
      // Initialization opened the exception log before failing on the rejection log; close it before retrying.
      loader.closeLogFiles();
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

  @Test( timeout = 3000 )
  public void shouldNotWaitIndefinitelyWhenWorkerIgnoresInterrupt() throws Exception {
    doReturn( 100L ).when( loader ).workerShutdownTimeoutMillis();
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
    // Closing PipedInputStream does not notify writers; allow its one-second space poll plus scheduling margin.
    producerThread.join( 2000 );

    assertFalse( producerThread.isAlive() );
    assertNull( processFailure.get() );
    assertFalse( processResult.get() );
    assertEquals( 0, loader.getErrors() );
  }

  @Test( timeout = 3000 )
  public void shouldNotStartWorkerWhenStopWinsPublicationRace() throws Exception {
    configureSingleColumnLoader();
    CountDownLatch workerFactoryEntered = new CountDownLatch( 1 );
    CountDownLatch releaseWorkerFactory = new CountDownLatch( 1 );
    AtomicReference<Thread> createdWorker = new AtomicReference<>();
    AtomicReference<Throwable> processFailure = new AtomicReference<>();
    doAnswer( invocation -> {
      Runnable workerTask = invocation.getArgument( 0 );
      Thread worker = new Thread( workerTask );
      createdWorker.set( worker );
      workerFactoryEntered.countDown();
      if ( !releaseWorkerFactory.await( 2, TimeUnit.SECONDS ) ) {
        throw new IllegalStateException( "Worker factory was not released" );
      }
      return worker;
    } ).when( loader ).createWorkerThread( any( Runnable.class ) );
    doReturn( new Object[] { "value" } ).when( loader ).getRow();

    Thread producerThread = new Thread( () -> {
      try {
        assertFalse( loader.processRow( loaderMeta, loaderData ) );
      } catch ( Throwable failure ) {
        processFailure.set( failure );
      }
    } );
    producerThread.setDaemon( true );
    producerThread.start();
    try {
      assertTrue( workerFactoryEntered.await( 1, TimeUnit.SECONDS ) );
      loader.stopRunning( loaderMeta, loaderData );
    } finally {
      releaseWorkerFactory.countDown();
      producerThread.join( 1000 );
    }

    assertFalse( producerThread.isAlive() );
    assertNull( processFailure.get() );
    assertNotNull( createdWorker.get() );
    assertSame( Thread.State.NEW, createdWorker.get().getState() );
    assertNull( loaderData.workerThread );
  }

  @Test( timeout = 3000 )
  public void shouldBoundAndQuarantineBlockingDatabaseCleanup() throws Exception {
    Database database = mock( Database.class );
    loaderData.db = database;
    loader.setErrors( 0 );
    CountDownLatch disconnectStarted = new CountDownLatch( 1 );
    CountDownLatch releaseDisconnect = new CountDownLatch( 1 );
    AtomicReference<Thread> cleanupThread = new AtomicReference<>();
    doAnswer( invocation -> {
      Runnable cleanupTask = invocation.getArgument( 0 );
      Thread thread = new Thread( cleanupTask );
      cleanupThread.set( thread );
      return thread;
    } ).when( loader ).createDatabaseCleanupThread( any( Runnable.class ) );
    doAnswer( invocation -> {
      disconnectStarted.countDown();
      awaitIgnoringInterrupts( releaseDisconnect );
      return null;
    } ).when( database ).disconnect();
    doReturn( 100L, 2000L ).when( loader ).workerShutdownTimeoutMillis();

    try {
      long startedAt = System.nanoTime();
      loader.stopRunning( loaderMeta, loaderData );
      assertTrue( TimeUnit.NANOSECONDS.toMillis( System.nanoTime() - startedAt ) < 1000 );
      assertTrue( disconnectStarted.await( 1, TimeUnit.SECONDS ) );
      assertTrue( cleanupThread.get().isDaemon() );
      assertTrue( cleanupThread.get().isAlive() );
      assertTrue( loader.getErrors() > 0 );

      startedAt = System.nanoTime();
      loader.dispose( loaderMeta, loaderData );
      assertTrue( TimeUnit.NANOSECONDS.toMillis( System.nanoTime() - startedAt ) < 1000 );
      verify( loader, times( 1 ) ).createDatabaseCleanupThread( any( Runnable.class ) );
    } finally {
      releaseDisconnect.countDown();
      if ( cleanupThread.get() != null ) {
        cleanupThread.get().join( 1000 );
      }
    }
    verify( database, times( 1 ) ).disconnect();
  }

  @Test( timeout = 3000 )
  public void shouldNotFinishCopyAfterWorkerShutdownTimesOut() throws Exception {
    configureSingleColumnLoader();
    CountDownLatch executeStarted = new CountDownLatch( 1 );
    CountDownLatch releaseExecute = new CountDownLatch( 1 );
    VerticaCopyStream copyStream = mock( VerticaCopyStream.class );
    when( copyStream.getRejects() ).thenReturn( Collections.emptyList() );
    doAnswer( invocation -> {
      executeStarted.countDown();
      awaitIgnoringInterrupts( releaseExecute );
      return null;
    } ).when( copyStream ).execute();
    doReturn( copyStream ).when( loader ).createVerticaCopyStream( anyString() );
    doReturn( new Object[] { "value" } ).when( loader ).getRow();
    doReturn( 100L ).when( loader ).workerShutdownTimeoutMillis();

    assertTrue( loader.processRow( loaderMeta, loaderData ) );
    Thread workerThread = loaderData.workerThread;
    try {
      assertTrue( executeStarted.await( 1, TimeUnit.SECONDS ) );
      loader.stopRunning( loaderMeta, loaderData );
      assertTrue( workerThread.isDaemon() );
      assertTrue( workerThread.isAlive() );
      assertTrue( loader.getErrors() > 0 );
      verify( copyStream, never() ).finish();
    } finally {
      releaseExecute.countDown();
      workerThread.join( 1000 );
    }

    assertFalse( workerThread.isAlive() );
    verify( copyStream, never() ).finish();
  }

  @Test( timeout = 3000 )
  public void shouldNotFinalizeCopyWhenStopWinsFinalizationTransition() throws Exception {
    configureSingleColumnLoader();
    CountDownLatch finalizationReached = new CountDownLatch( 1 );
    CountDownLatch releaseFinalization = new CountDownLatch( 1 );
    AtomicBoolean enteringFinalization = new AtomicBoolean();
    VerticaCopyStream copyStream = mock( VerticaCopyStream.class );
    when( copyStream.getRejects() ).thenReturn( Collections.emptyList() );
    doReturn( copyStream ).when( loader ).createVerticaCopyStream( anyString() );
    doReturn( new Object[] { "value" } ).when( loader ).getRow();
    doReturn( 100L ).when( loader ).workerShutdownTimeoutMillis();
    doAnswer( invocation -> {
      enteringFinalization.set( true );
      return invocation.callRealMethod();
    } ).when( loader ).beginCopyFinalization();
    doAnswer( invocation -> {
      boolean stopped = (boolean) invocation.callRealMethod();
      if ( enteringFinalization.get() && Thread.currentThread() == loaderData.workerThread && !stopped ) {
        finalizationReached.countDown();
        awaitIgnoringInterrupts( releaseFinalization );
      }
      return stopped;
    } ).when( loader ).isStopped();

    assertTrue( loader.processRow( loaderMeta, loaderData ) );
    try {
      assertTrue( finalizationReached.await( 1, TimeUnit.SECONDS ) );
      loader.stopRunning( loaderMeta, loaderData );
    } finally {
      releaseFinalization.countDown();
      loaderData.workerThread.join( 1000 );
    }

    assertFalse( loaderData.workerThread.isAlive() );
    verify( copyStream, never() ).finish();
  }

  @Test( timeout = 3000 )
  public void shouldBoundStopWhenCopyAlreadyOwnsFinalization() throws Exception {
    configureSingleColumnLoader();
    CountDownLatch finishStarted = new CountDownLatch( 1 );
    CountDownLatch releaseFinish = new CountDownLatch( 1 );
    VerticaCopyStream copyStream = mock( VerticaCopyStream.class );
    when( copyStream.getRejects() ).thenReturn( Collections.emptyList() );
    when( copyStream.finish() ).thenAnswer( invocation -> {
      finishStarted.countDown();
      awaitIgnoringInterrupts( releaseFinish );
      return 1L;
    } );
    doReturn( copyStream ).when( loader ).createVerticaCopyStream( anyString() );
    doReturn( new Object[] { "value" } ).when( loader ).getRow();
    doReturn( 100L ).when( loader ).workerShutdownTimeoutMillis();

    assertTrue( loader.processRow( loaderMeta, loaderData ) );
    try {
      assertTrue( finishStarted.await( 1, TimeUnit.SECONDS ) );
      long startedAt = System.nanoTime();
      loader.stopRunning( loaderMeta, loaderData );
      assertTrue( TimeUnit.NANOSECONDS.toMillis( System.nanoTime() - startedAt ) < 1000 );
      assertTrue( loader.getErrors() > 0 );
      verify( loader ).cancelRunningCopy( loaderData.db );
    } finally {
      releaseFinish.countDown();
      loaderData.workerThread.join( 1000 );
    }
    assertFalse( loaderData.workerThread.isAlive() );
    verify( copyStream, times( 1 ) ).finish();
  }

  @Test
  public void shouldCancelRunningCopyThroughVerticaDriverConnection() throws Exception {
    Database database = mock( Database.class );
    SConnection connection = mock( SConnection.class, withSettings().extraInterfaces( VerticaConnection.class ) );
    VConnection driverConnection = mock( VConnection.class );
    when( database.getConnection() ).thenReturn( connection );
    when( connection.getDSIConnection() ).thenReturn( driverConnection );

    loader.cancelRunningCopy( database );

    verify( driverConnection ).cancelCurrentStatement();
  }

  @Test
  public void shouldRollbackManualStopBeforeDisconnecting() throws Exception {
    Database database = mock( Database.class );
    Connection connection = mock( VerticaConnection.class );
    when( database.getConnection() ).thenReturn( connection );
    loaderData.db = database;
    loader.setErrors( 0 );

    loader.stopRunning( loaderMeta, loaderData );

    InOrder cleanupOrder = inOrder( database );
    cleanupOrder.verify( database ).rollback();
    cleanupOrder.verify( database ).disconnect();
    assertEquals( 0, loader.getErrors() );
  }

  @Test
  public void shouldDisconnectEvenWhenCopyCancellationFails() throws Exception {
    Database database = mock( Database.class );
    when( database.getConnection() ).thenReturn( mock( Connection.class ) );
    loaderData.db = database;
    loader.setErrors( 0 );
    doThrow( new SQLException( "Cancellation failed" ) ).when( loader ).cancelRunningCopy( database );

    loader.stopRunning( loaderMeta, loaderData );

    verify( database ).rollback();
    verify( database ).disconnect();
    assertEquals( 1, loader.getErrors() );
  }

  @Test
  public void shouldReportUnexpectedDatabaseCleanupFailure() throws Exception {
    Database database = mock( Database.class );
    loaderData.db = database;
    loader.setErrors( 0 );
    doThrow( new IllegalStateException( "Disconnect failed" ) ).when( database ).disconnect();

    loader.stopRunning( loaderMeta, loaderData );

    assertEquals( 1, loader.getErrors() );
  }

  @Test( timeout = 3000 )
  public void shouldLeaveHealthyCompletionWaitWhenForcedStopTimesOut() throws Exception {
    loaderData.db = mock( Database.class );
    loader.setErrors( 0 );
    CountDownLatch releaseWorker = new CountDownLatch( 1 );
    CountDownLatch completionWaitReached = new CountDownLatch( 1 );
    Thread workerThread = new Thread( () -> awaitIgnoringInterrupts( releaseWorker ) );
    workerThread.setDaemon( true );
    loaderData.workerThread = workerThread;
    Thread disposeThread = new Thread( () -> loader.dispose( loaderMeta, loaderData ) );
    disposeThread.setDaemon( true );
    AtomicInteger errorReads = new AtomicInteger();
    doAnswer( invocation -> {
      if ( Thread.currentThread() == disposeThread && errorReads.incrementAndGet() == 2 ) {
        completionWaitReached.countDown();
      }
      return invocation.callRealMethod();
    } ).when( loader ).getErrors();
    doReturn( 100L ).when( loader ).workerShutdownTimeoutMillis();

    workerThread.start();
    disposeThread.start();
    try {
      assertTrue( completionWaitReached.await( 1, TimeUnit.SECONDS ) );
      loader.stopRunning( loaderMeta, loaderData );
      disposeThread.join( 1000 );
      assertFalse( disposeThread.isAlive() );
      assertTrue( workerThread.isAlive() );
      assertTrue( loader.getErrors() > 0 );
    } finally {
      releaseWorker.countDown();
      workerThread.join( 1000 );
      disposeThread.join( 1000 );
    }
  }

  @Test( timeout = 3000 )
  public void shouldForceCleanupWhenCompletionWaitIsInterrupted() throws Exception {
    configureSingleColumnLoader();
    CountDownLatch executeStarted = new CountDownLatch( 1 );
    CountDownLatch releaseExecute = new CountDownLatch( 1 );
    VerticaCopyStream copyStream = mock( VerticaCopyStream.class );
    when( copyStream.getRejects() ).thenReturn( Collections.emptyList() );
    doAnswer( invocation -> {
      executeStarted.countDown();
      awaitIgnoringInterrupts( releaseExecute );
      return null;
    } ).when( copyStream ).execute();
    doReturn( copyStream ).when( loader ).createVerticaCopyStream( anyString() );
    doReturn( new Object[] { "value" } ).when( loader ).getRow();
    doReturn( 100L ).when( loader ).workerShutdownTimeoutMillis();

    assertTrue( loader.processRow( loaderMeta, loaderData ) );
    try {
      assertTrue( executeStarted.await( 1, TimeUnit.SECONDS ) );
      Thread.currentThread().interrupt();
      loader.dispose( loaderMeta, loaderData );
      assertTrue( Thread.currentThread().isInterrupted() );
      assertTrue( loader.isStopped() );
      assertTrue( loader.getErrors() > 0 );
    } finally {
      Thread.interrupted();
      releaseExecute.countDown();
      loaderData.workerThread.join( 1000 );
    }
    assertFalse( loaderData.workerThread.isAlive() );
    verify( copyStream, never() ).finish();
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

  private static void awaitIgnoringInterrupts( CountDownLatch latch ) {
    boolean interrupted = false;
    while ( latch.getCount() > 0 ) {
      try {
        latch.await();
      } catch ( InterruptedException e ) {
        // Model a driver call that cannot be released by interruption.
        interrupted = true;
      }
    }
    if ( interrupted ) {
      Thread.currentThread().interrupt();
    }
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
