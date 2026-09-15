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

/**
 * Per-step connection and encoding state.
 * The producer owns row encoding; the worker consumes the pipe, with its lifecycle coordinated by the loader.
 */
public class VerticaBulkLoaderData extends BaseStepData implements StepDataInterface {
  protected Database db;
  protected DatabaseMeta databaseMeta;

  protected StreamEncoder encoder;

  // COPY-column position -> incoming row index; selected fields may be reordered without changing downstream rows.
  protected int[] selectedRowFieldIndices;

  // Downstream row metadata stays separate from the column names/order used by native COPY encoding.
  protected RowMetaInterface outputRowMeta;
  protected RowMetaInterface insertRowMeta;

  protected PipedInputStream pipedInputStream;

  // Published and captured under VerticaBulkLoader's worker lifecycle lock.
  protected Thread workerThread;

  protected List<ColumnSpec> colSpecs;

  protected VerticaBulkLoaderData() {
    super();

    db = null;
  }

  public RowMetaInterface getInsertRowMeta() {
    return insertRowMeta;
  }

  /**
   * Flushes and closes the producer side at healthy EOF, allowing the worker to finish reading.
   * Flushing may wait for the reader; forced stop instead closes the input through VerticaBulkLoader.
   */
  public void close() throws IOException {

    if ( encoder != null ) {
      encoder.close();
    }

  }

}
