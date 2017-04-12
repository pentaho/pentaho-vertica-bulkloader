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
* Copyright (c) 2002-2017 Pentaho Corporation..  All rights reserved.
*/

package org.pentaho.di.verticabulkload;

import com.vertica.jdbc.VerticaCopyStream;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaPluginType;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.verticabulkload.nativebinary.ColumnSpec;
import org.pentaho.di.verticabulkload.nativebinary.StreamEncoder;

import java.io.IOException;
import java.io.PipedInputStream;
import java.nio.BufferOverflowException;
import java.nio.channels.WritableByteChannel;
import java.sql.SQLException;
import java.util.List;

/**
 * Unit test for {@link VerticaBulkLoader}.
 */
public class VerticaBulkLoaderTest {

  private VerticaBulkLoaderMeta loaderMeta;
  private VerticaBulkLoaderData loaderData;
  private VerticaBulkLoader loader;

  @BeforeClass
  public static void initEnvironment() throws Exception {
    KettleEnvironment.init();
  }

  @Before
  public void setUp() throws KettlePluginException, SQLException {
    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.init( true );

    loaderData = new VerticaBulkLoaderData();
    loaderMeta = Mockito.spy( new VerticaBulkLoaderMeta() );

    TransMeta transMeta = new TransMeta();
    transMeta.setName( "loader" );

    PluginRegistry pluginRegistry = PluginRegistry.getInstance();

    String loaderPid = pluginRegistry.getPluginId( StepPluginType.class, loaderMeta );
    StepMeta stepMeta = new StepMeta( loaderPid, "loader", loaderMeta );
    Trans trans = new Trans( transMeta );
    transMeta.addStep( stepMeta );
    trans.setRunning( true );

    loaderMeta.setDatabaseMeta( Mockito.mock( DatabaseMeta.class ) );

    loader = Mockito.spy( new VerticaBulkLoader( stepMeta, loaderData, 1, transMeta, trans ) );

    Mockito.doReturn( Mockito.mock( VerticaCopyStream.class ) ).when( loader ).createVerticaCopyStream( Matchers.anyString() );
  }

  /**
   * Testing boundary condition of buffer size handling.
   * <p>
   *     Given 4 varchar fields of different sizes.
   *     When loaded data amount is getting close to a buffer size,
   *     then the buffer should not be overflowed.
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
    Mockito.doReturn( tableMeta ).when( loaderMeta ).getTableRowMetaInterface();

    loader.init( loaderMeta, loaderData );
    Mockito.when( loader.getRow() ).thenReturn( new String[] { "19 characters------", "4 ch", "7 chara", "8 charac" } );

    Mockito.doAnswer( invocation -> {
      List colSpecs = (List) invocation.getArguments()[ 0 ];
      PipedInputStream pipedInputStream = (PipedInputStream) invocation.getArguments()[ 1 ];
      return new MockChannelStreamEncoder( colSpecs, pipedInputStream );
    } ).when( loader ).createStreamEncoder( Matchers.any(), Matchers.any() );

    // when
    try {
      for ( int i = 0; i < StreamEncoder.NUM_ROWS_TO_BUFFER + 1; i++ ) {
        loader.processRow( loaderMeta, loaderData );
      }
    } catch ( BufferOverflowException e ) {
      Assert.fail( e.getMessage() );
    }

    // then no BufferOverflowException should be thrown
  }

  /**
   * Testing boundary condition of buffer size handling.
   * <p>
   *     Given 7 varchar fields of small sizes.
   *     When loaded data amount is getting close to a buffer size,
   *     then the buffer should not be overflowed.
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
    Mockito.doReturn( tableMeta ).when( loaderMeta ).getTableRowMetaInterface();

    loader.init( loaderMeta, loaderData );
    Mockito.when( loader.getRow() ).thenReturn( new String[] { "1", "1", "1", "1", "1", "1", "1" } );

    Mockito.doAnswer( invocation -> {
      List colSpecs = (List) invocation.getArguments()[ 0 ];
      PipedInputStream pipedInputStream = (PipedInputStream) invocation.getArguments()[ 1 ];
      return new MockChannelStreamEncoder( colSpecs, pipedInputStream );
    } ).when( loader ).createStreamEncoder( Matchers.any(), Matchers.any() );

    // when
    try {
      for ( int i = 0; i < StreamEncoder.NUM_ROWS_TO_BUFFER + 1; i++ ) {
        loader.processRow( loaderMeta, loaderData );
      }
    } catch ( BufferOverflowException e ) {
      Assert.fail( e.getMessage() );
    }

    // then no BufferOverflowException should be thrown
  }

  private class MockChannelStreamEncoder extends StreamEncoder {
    private MockChannelStreamEncoder( List<ColumnSpec> columns, PipedInputStream inputStream ) throws IOException {
      super( columns, inputStream );
      channel = Mockito.mock( WritableByteChannel.class );
    }
  }

  private static ValueMetaString getValueMetaString( String testData3, int length ) {
    ValueMetaString tableValueMeta = new ValueMetaString( testData3 );
    tableValueMeta.setLength( length );
    tableValueMeta.setOriginalColumnTypeName( "VARCHAR" );
    return tableValueMeta;
  }
}
