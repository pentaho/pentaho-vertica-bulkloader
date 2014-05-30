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

import org.pentaho.di.verticabulkload.nativebinary.ColumnSpec;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import org.pentaho.di.verticabulkload.nativebinary.StreamEncoder;
import java.io.IOException;
import java.io.PipedInputStream;
import java.util.List;

public class VerticaBulkLoaderData extends BaseStepData implements StepDataInterface {
  protected Database db;
  protected DatabaseMeta databaseMeta;

  protected StreamEncoder encoder;

  protected int[] selectedRowFieldIndices;

  protected RowMetaInterface outputRowMeta;
  protected RowMetaInterface insertRowMeta;

  protected PipedInputStream pipedInputStream;

  protected volatile Thread workerThread;

  protected List<ColumnSpec> colSpecs;

  protected VerticaBulkLoaderData() {
    super();

    db = null;
  }

  public void close() throws IOException {

    if ( encoder != null ) {
      encoder.close();
    }

  }

}
