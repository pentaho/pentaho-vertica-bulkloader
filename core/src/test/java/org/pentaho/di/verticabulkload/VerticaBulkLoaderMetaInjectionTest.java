/*! ******************************************************************************
*
* Pentaho Data Integration
*
* Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
*
*******************************************************************************
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
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
