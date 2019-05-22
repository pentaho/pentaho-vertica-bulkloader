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

import com.vertica.jdbc.VerticaConnection;
import com.vertica.jdbc.VerticaCopyStream;
import org.apache.commons.dbcp.DelegatingConnection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
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
import java.nio.channels.WritableByteChannel;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.matchers.JUnitMatchers.containsString;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
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

    doReturn( mock( VerticaCopyStream.class ) ).when( loader ).createVerticaCopyStream( anyString() );
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
      when( loader.getRow() ).thenReturn( goodObjectData );

      doAnswer( invocation -> {
        List colSpecs = (List) invocation.getArguments()[ 0 ];
        PipedInputStream pipedInputStream = (PipedInputStream) invocation.getArguments()[ 1 ];
        return new MockChannelStreamEncoder( colSpecs, pipedInputStream );
      } ).when( loader ).createStreamEncoder( any(), any() );
      // Verify that the good row returns with a true load value
      assertTrue( loader.processRow( loaderMeta, loaderData ) );


      when( loader.getRow() ).thenReturn( badObjectData );
      loaderMeta.setAbortOnError( true );

      assertFalse( loader.processRow( loaderMeta, loaderData ) );

      loaderMeta.setAbortOnError( false );
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
