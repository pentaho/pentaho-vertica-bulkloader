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

import org.junit.Test;
import org.mockito.Mockito;
import org.pentaho.di.core.row.RowMeta;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

public class VerticaBulkLoaderMetaTest {
  @Test
  public void testProvidesModeler() throws Exception {
    VerticaBulkLoaderMeta verticaBulkLoaderMeta = new VerticaBulkLoaderMeta();
    verticaBulkLoaderMeta.setFieldDatabase( new String[] {"f1", "f2", "f3"} );
    verticaBulkLoaderMeta.setFieldStream( new String[] {"s4", "s5", "s6"} );

    VerticaBulkLoaderData verticaBulkLoaderData = new VerticaBulkLoaderData();
    verticaBulkLoaderData.insertRowMeta = Mockito.mock( RowMeta.class );
    assertEquals( verticaBulkLoaderData.insertRowMeta, verticaBulkLoaderMeta.getRowMeta( verticaBulkLoaderData ) );

    verticaBulkLoaderMeta.setSpecifyFields( false );
    assertEquals( 0, verticaBulkLoaderMeta.getDatabaseFields().size() );
    assertEquals( 0, verticaBulkLoaderMeta.getStreamFields().size() );

    verticaBulkLoaderMeta.setSpecifyFields( true );
    assertEquals( 3, verticaBulkLoaderMeta.getDatabaseFields().size() );
    assertEquals( "f1", verticaBulkLoaderMeta.getDatabaseFields().get( 0 ) );
    assertEquals( "f2", verticaBulkLoaderMeta.getDatabaseFields().get( 1 ) );
    assertEquals( "f3", verticaBulkLoaderMeta.getDatabaseFields().get( 2 ) );
    assertEquals( 3, verticaBulkLoaderMeta.getStreamFields().size() );
    assertEquals( "s4", verticaBulkLoaderMeta.getStreamFields().get( 0 ) );
    assertEquals( "s5", verticaBulkLoaderMeta.getStreamFields().get( 1 ) );
    assertEquals( "s6", verticaBulkLoaderMeta.getStreamFields().get( 2 ) );
  }

  @Test
  public void testGetXml() throws Exception {
    VerticaBulkLoaderMeta vbl = new VerticaBulkLoaderMeta();
    vbl.setDefault();
    vbl.setFieldDatabase( new String[] { "fieldDB1", "fieldDB2", "fieldDB3", "fieldDB4", "fieldDB5" } );
    vbl.setFieldStream( new String[] { "fieldStr1", "fieldStr2", "fieldStr3" } );

    try {
      vbl.getXML();
      fail( "Before calling afterInjectionSynchronization, should have thrown an ArrayIndexOOB" );
    } catch ( Exception expected ) {
      // Do Nothing
    }
    vbl.afterInjectionSynchronization();
    //run without a exception
    vbl.getXML();

    int targetSz = vbl.getFieldDatabase().length;

    assertEquals( targetSz, vbl.getFieldStream().length );
  }
}
