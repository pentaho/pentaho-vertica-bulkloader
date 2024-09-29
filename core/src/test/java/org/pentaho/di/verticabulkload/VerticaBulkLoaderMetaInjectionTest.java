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

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.injection.BaseMetadataInjectionTest;

public class VerticaBulkLoaderMetaInjectionTest extends BaseMetadataInjectionTest<VerticaBulkLoaderMeta> {
  @Before
  public void setup() {
    setup( new VerticaBulkLoaderMeta() );
  }

  @Test
  public void testInjection() throws Exception {
    check( "SCHEMANAME", new StringGetter() {
      public String get() {
        return meta.getSchemaName();
      }
    } );
    check( "TABLENAME", new StringGetter() {
      public String get() {
        return meta.getTablename();
      }
    } );
    check( "DIRECT", new BooleanGetter() {
      public boolean get() {
        return meta.isDirect();
      }
    } );
    check( "ABORTONERROR", new BooleanGetter() {
      public boolean get() {
        return meta.isAbortOnError();
      }
    } );
    check( "EXCEPTIONSFILENAME", new StringGetter() {
      public String get() {
        return meta.getExceptionsFileName();
      }
    } );
    check( "REJECTEDDATAFILENAME", new StringGetter() {
        public String get() {
          return meta.getRejectedDataFileName();
        }
      } );
    check( "STREAMNAME", new StringGetter() {
        public String get() {
          return meta.getStreamName();
        }
      } );
    check( "FIELDSTREAM", new StringGetter() {
        public String get() {
          return meta.getFieldStream()[0];
        }
      } );
    check( "FIELDDATABASE", new StringGetter() {
        public String get() {
          return meta.getFieldDatabase()[0];
        }
      } );
    check( "CONNECTIONNAME", new StringGetter() {
        public String get() {
          return "My Connection";
        }
      }, "My Connection" );
  }
}
