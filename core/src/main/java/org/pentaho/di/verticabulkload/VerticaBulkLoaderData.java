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

  public RowMetaInterface getInsertRowMeta() {
    return insertRowMeta;
  }

  public void close() throws IOException {

    if ( encoder != null ) {
      encoder.close();
    }

  }

}
