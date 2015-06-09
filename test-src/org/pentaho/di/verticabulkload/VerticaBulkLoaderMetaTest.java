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
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/
package org.pentaho.di.verticabulkload;

import org.junit.Test;
import org.mockito.Mockito;
import org.pentaho.di.core.row.RowMeta;

import static org.junit.Assert.*;

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
}
