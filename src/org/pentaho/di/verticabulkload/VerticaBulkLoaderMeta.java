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

import java.util.List;
import java.util.Map;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.DBCache;
import org.pentaho.di.core.ProvidesDatabaseConnectionInformation;
import org.pentaho.di.core.SQLStatement;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.shared.SharedObjectInterface;
import org.pentaho.di.trans.DatabaseImpact;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

@Step( id = "VerticaBulkLoader", image = "vertica.svg",
    i18nPackageName = "plugin.com.vertica.kettle.bulkloader", name = "VerticaBulkLoaderMeta.TypeLongDesc",
    description = "VerticaBulkLoaderMeta.TypeTooltipDesc",
    categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Bulk" )
public class VerticaBulkLoaderMeta extends BaseStepMeta implements StepMetaInterface,
    ProvidesDatabaseConnectionInformation {
  private static Class<?> PKG = VerticaBulkLoaderMeta.class; // for i18n purposes, needed by Translator2!!

  private DatabaseMeta databaseMeta;
  private String schemaName;
  private String tablename;

  private boolean direct = true;
  private boolean abortOnError = true;

  private String exceptionsFileName;
  private String rejectedDataFileName;
  private String streamName;

  /** Do we explicitly select the fields to update in the database */
  private boolean specifyFields;

  /** Fields containing the values in the input stream to insert */
  private String[] fieldStream;

  /** Fields in the table to insert */
  private String[] fieldDatabase;

  public VerticaBulkLoaderMeta() {
    super(); // allocate BaseStepMeta

    fieldStream = new String[0];
    fieldDatabase = new String[0];
  }

  public void allocate( int nrRows ) {
    fieldStream = new String[nrRows];
    fieldDatabase = new String[nrRows];
  }

  public void loadXML( Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters )
    throws KettleXMLException {
    readData( stepnode, databases );
  }

  public Object clone() {
    VerticaBulkLoaderMeta retval = (VerticaBulkLoaderMeta) super.clone();

    int nrStream = fieldStream.length;
    int nrDatabase = fieldDatabase.length;

    retval.fieldStream = new String[nrStream];
    retval.fieldDatabase = new String[nrDatabase];

    for ( int i = 0; i < nrStream; i++ ) {
      retval.fieldStream[i] = fieldStream[i];
    }

    for ( int i = 0; i < nrDatabase; i++ ) {
      retval.fieldDatabase[i] = fieldDatabase[i];
    }

    return retval;
  }

  /**
   * @return Returns the database.
   */
  public DatabaseMeta getDatabaseMeta() {
    return databaseMeta;
  }

  /**
   * @param database
   *          The database to set.
   */
  public void setDatabaseMeta( DatabaseMeta database ) {
    this.databaseMeta = database;
  }

  /**
   * @deprecated use {@link #getTableName()} 
   */
  public String getTablename() {
    return getTableName();
  }

  /**
   * @return Returns the tablename.
   */
  public String getTableName() {
    return tablename;
  }

  /**
   * @param tablename
   *          The tablename to set.
   */
  public void setTablename( String tablename ) {
    this.tablename = tablename;
  }

  /**
   * @param specifyFields
   *          The specify fields flag to set.
   */
  public void setSpecifyFields( boolean specifyFields ) {
    this.specifyFields = specifyFields;
  }

  /**
   * @return Returns the specify fields flag.
   */
  public boolean specifyFields() {
    return specifyFields;
  }

  public boolean isDirect() {
    return direct;
  }

  public void setDirect( boolean direct ) {
    this.direct = direct;
  }

  public boolean isAbortOnError() {
    return abortOnError;
  }

  public void setAbortOnError( boolean abortOnError ) {
    this.abortOnError = abortOnError;
  }

  public String getExceptionsFileName() {
    return exceptionsFileName;
  }

  public void setExceptionsFileName( String exceptionsFileName ) {
    this.exceptionsFileName = exceptionsFileName;
  }

  public String getRejectedDataFileName() {
    return rejectedDataFileName;
  }

  public void setRejectedDataFileName( String rejectedDataFileName ) {
    this.rejectedDataFileName = rejectedDataFileName;
  }

  public String getStreamName() {
    return streamName;
  }

  public void setStreamName( String streamName ) {
    this.streamName = streamName;
  }

  public boolean isSpecifyFields() {
    return specifyFields;
  }

  private void readData( Node stepnode, List<? extends SharedObjectInterface> databases ) throws KettleXMLException {
    try {
      String con = XMLHandler.getTagValue( stepnode, "connection" );
      databaseMeta = DatabaseMeta.findDatabase( databases, con );
      schemaName = XMLHandler.getTagValue( stepnode, "schema" );
      tablename = XMLHandler.getTagValue( stepnode, "table" );

      // If not present it will be false to be compatible with pre-v3.2
      specifyFields = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "specify_fields" ) );

      Node fields = XMLHandler.getSubNode( stepnode, "fields" ); //$NON-NLS-1$
      int nrRows = XMLHandler.countNodes( fields, "field" ); //$NON-NLS-1$

      allocate( nrRows );

      for ( int i = 0; i < nrRows; i++ ) {
        Node knode = XMLHandler.getSubNodeByNr( fields, "field", i ); //$NON-NLS-1$

        fieldDatabase[i] = XMLHandler.getTagValue( knode, "column_name" ); //$NON-NLS-1$
        fieldStream[i] = XMLHandler.getTagValue( knode, "stream_name" ); //$NON-NLS-1$
      }

      exceptionsFileName = XMLHandler.getTagValue( stepnode, "exceptions_filename" );
      rejectedDataFileName = XMLHandler.getTagValue( stepnode, "rejected_data_filename" );
      abortOnError = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "abort_on_error" ) );
      direct = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "direct" ) );
      streamName = XMLHandler.getTagValue( stepnode, "stream_name" );

    } catch ( Exception e ) {
      throw new KettleXMLException( "Unable to load step info from XML", e );
    }
  }

  public void setDefault() {
    databaseMeta = null;
    tablename = "";

    // To be compatible with pre-v3.2 (SB)
    specifyFields = false;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    " + XMLHandler.addTagValue( "connection", databaseMeta == null ? "" : databaseMeta.getName() ) );
    retval.append( "    " + XMLHandler.addTagValue( "schema", schemaName ) );
    retval.append( "    " + XMLHandler.addTagValue( "table", tablename ) );
    retval.append( "    " + XMLHandler.addTagValue( "specify_fields", specifyFields ) );

    retval.append( "    <fields>" ).append( Const.CR ); //$NON-NLS-1$

    for ( int i = 0; i < fieldDatabase.length; i++ ) {
      retval.append( "        <field>" ).append( Const.CR ); //$NON-NLS-1$
      retval.append( "          " ).append( XMLHandler.addTagValue( "column_name", fieldDatabase[i] ) ); //$NON-NLS-1$ //$NON-NLS-2$
      retval.append( "          " ).append( XMLHandler.addTagValue( "stream_name", fieldStream[i] ) ); //$NON-NLS-1$ //$NON-NLS-2$
      retval.append( "        </field>" ).append( Const.CR ); //$NON-NLS-1$
    }
    retval.append( "    </fields>" ).append( Const.CR ); //$NON-NLS-1$

    retval.append( "    " + XMLHandler.addTagValue( "exceptions_filename", exceptionsFileName ) );
    retval.append( "    " + XMLHandler.addTagValue( "rejected_data_filename", rejectedDataFileName ) );
    retval.append( "    " + XMLHandler.addTagValue( "abort_on_error", abortOnError ) );
    retval.append( "    " + XMLHandler.addTagValue( "direct", direct ) );
    retval.append( "    " + XMLHandler.addTagValue( "stream_name", streamName ) );

    return retval.toString();
  }

  public void readRep( Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> counters )
    throws KettleException {
    try {
      databaseMeta = rep.loadDatabaseMetaFromStepAttribute( id_step, "id_connection", databases );
      schemaName = rep.getStepAttributeString( id_step, "schema" );
      tablename = rep.getStepAttributeString( id_step, "table" );
      specifyFields = rep.getStepAttributeBoolean( id_step, "specify_fields" );

      int nrCols = rep.countNrStepAttributes( id_step, "field_column_name" ); //$NON-NLS-1$
      int nrStreams = rep.countNrStepAttributes( id_step, "field_stream_name" ); //$NON-NLS-1$

      int nrRows = ( nrCols < nrStreams ? nrStreams : nrCols );
      allocate( nrRows );

      for ( int idx = 0; idx < nrRows; idx++ ) {
        fieldDatabase[idx] = Const.NVL( rep.getStepAttributeString( id_step, idx, "field_column_name" ), "" ); //$NON-NLS-1$ //$NON-NLS-2$
        fieldStream[idx] = Const.NVL( rep.getStepAttributeString( id_step, idx, "field_stream_name" ), "" ); //$NON-NLS-1$ //$NON-NLS-2$
      }

      exceptionsFileName = rep.getStepAttributeString( id_step, "exceptions_filename" );
      rejectedDataFileName = rep.getStepAttributeString( id_step, "rejected_data_filename" );
      abortOnError = rep.getStepAttributeBoolean( id_step, "abort_on_error" );
      direct = rep.getStepAttributeBoolean( id_step, "direct" );
      streamName = rep.getStepAttributeString( id_step, "stream_name" );
    } catch ( Exception e ) {
      throw new KettleException( "Unexpected error reading step information from the repository", e );
    }
  }

  public void saveRep( Repository rep, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    try {
      rep.saveDatabaseMetaStepAttribute( id_transformation, id_step, "id_connection", databaseMeta );
      rep.saveStepAttribute( id_transformation, id_step, "schema", schemaName );
      rep.saveStepAttribute( id_transformation, id_step, "table", tablename );
      rep.saveStepAttribute( id_transformation, id_step, "specify_fields", specifyFields );

      int nrRows = ( fieldDatabase.length < fieldStream.length ? fieldStream.length : fieldDatabase.length );
      for ( int idx = 0; idx < nrRows; idx++ ) {
        String columnName = ( idx < fieldDatabase.length ? fieldDatabase[idx] : "" );
        String streamName = ( idx < fieldStream.length ? fieldStream[idx] : "" );
        rep.saveStepAttribute( id_transformation, id_step, idx, "field_column_name", columnName ); //$NON-NLS-1$
        rep.saveStepAttribute( id_transformation, id_step, idx, "field_stream_name", streamName ); //$NON-NLS-1$
      }

      rep.saveStepAttribute( id_transformation, id_step, "exceptions_filename", exceptionsFileName );
      rep.saveStepAttribute( id_transformation, id_step, "rejected_data_filename", rejectedDataFileName );
      rep.saveStepAttribute( id_transformation, id_step, "abort_on_error", abortOnError );
      rep.saveStepAttribute( id_transformation, id_step, "direct", direct );
      rep.saveStepAttribute( id_transformation, id_step, "stream_name", streamName );
    } catch ( Exception e ) {
      throw new KettleException( "Unable to save step information to the repository for id_step=" + id_step, e );
    }
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
      String[] input, String[] output, RowMetaInterface info ) {
    if ( databaseMeta != null ) {
      CheckResult cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
              "VerticaBulkLoaderMeta.CheckResult.ConnectionExists" ), stepMeta );
      remarks.add( cr );

      Database db = new Database( transMeta, databaseMeta );
      db.shareVariablesWith( transMeta );
      try {
        db.connect();

        cr =
            new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
                "VerticaBulkLoaderMeta.CheckResult.ConnectionOk" ), stepMeta );
        remarks.add( cr );

        if ( !Const.isEmpty( transMeta.environmentSubstitute( tablename ) ) ) {
          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination( transMeta.environmentSubstitute( schemaName ), transMeta
                  .environmentSubstitute( tablename ) );
          // Check if this table exists...
          if ( db.checkTableExists( schemaTable ) ) {
            cr =
                new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
                    "VerticaBulkLoaderMeta.CheckResult.TableAccessible", schemaTable ), stepMeta );
            remarks.add( cr );

            RowMetaInterface r = db.getTableFields( schemaTable );
            if ( r != null ) {
              cr =
                  new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
                      "VerticaBulkLoaderMeta.CheckResult.TableOk", schemaTable ), stepMeta );
              remarks.add( cr );

              String error_message = "";
              boolean error_found = false;
              // OK, we have the table fields.
              // Now see what we can find as previous step...
              if ( prev != null && prev.size() > 0 ) {
                cr =
                    new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
                        "VerticaBulkLoaderMeta.CheckResult.FieldsReceived", "" + prev.size() ), stepMeta );
                remarks.add( cr );

                if ( !specifyFields() ) {
                  // Starting from prev...
                  for ( int i = 0; i < prev.size(); i++ ) {
                    ValueMetaInterface pv = prev.getValueMeta( i );
                    int idx = r.indexOfValue( pv.getName() );
                    if ( idx < 0 ) {
                      error_message += "\t\t" + pv.getName() + " (" + pv.getTypeDesc() + ")" + Const.CR;
                      error_found = true;
                    }
                  }
                  if ( error_found ) {
                    error_message =
                        BaseMessages.getString( PKG, "VerticaBulkLoaderMeta.CheckResult.FieldsNotFoundInOutput",
                            error_message );

                    cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
                    remarks.add( cr );
                  } else {
                    cr =
                        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
                            "VerticaBulkLoaderMeta.CheckResult.AllFieldsFoundInOutput" ), stepMeta );
                    remarks.add( cr );
                  }
                } else {
                  // Specifying the column names explicitly
                  for ( int i = 0; i < getFieldDatabase().length; i++ ) {
                    int idx = r.indexOfValue( getFieldDatabase()[i] );
                    if ( idx < 0 ) {
                      error_message += "\t\t" + getFieldDatabase()[i] + Const.CR;
                      error_found = true;
                    }
                  }
                  if ( error_found ) {
                    error_message =
                        BaseMessages.getString( PKG, "VerticaBulkLoaderMeta.CheckResult.FieldsSpecifiedNotInTable",
                            error_message );

                    cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
                    remarks.add( cr );
                  } else {
                    cr =
                        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
                            "VerticaBulkLoaderMeta.CheckResult.AllFieldsFoundInOutput" ), stepMeta );
                    remarks.add( cr );
                  }
                }

                error_message = "";
                if ( !specifyFields() ) {
                  // Starting from table fields in r...
                  for ( int i = 0; i < getFieldDatabase().length; i++ ) {
                    ValueMetaInterface rv = r.getValueMeta( i );
                    int idx = prev.indexOfValue( rv.getName() );
                    if ( idx < 0 ) {
                      error_message += "\t\t" + rv.getName() + " (" + rv.getTypeDesc() + ")" + Const.CR;
                      error_found = true;
                    }
                  }
                  if ( error_found ) {
                    error_message =
                        BaseMessages.getString( PKG, "VerticaBulkLoaderMeta.CheckResult.FieldsNotFound", error_message );

                    cr = new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, error_message, stepMeta );
                    remarks.add( cr );
                  } else {
                    cr =
                        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
                            "VerticaBulkLoaderMeta.CheckResult.AllFieldsFound" ), stepMeta );
                    remarks.add( cr );
                  }
                } else {
                  // Specifying the column names explicitly
                  for ( int i = 0; i < getFieldStream().length; i++ ) {
                    int idx = prev.indexOfValue( getFieldStream()[i] );
                    if ( idx < 0 ) {
                      error_message += "\t\t" + getFieldStream()[i] + Const.CR;
                      error_found = true;
                    }
                  }
                  if ( error_found ) {
                    error_message =
                        BaseMessages.getString( PKG, "VerticaBulkLoaderMeta.CheckResult.FieldsSpecifiedNotFound",
                            error_message );

                    cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta );
                    remarks.add( cr );
                  } else {
                    cr =
                        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
                            "VerticaBulkLoaderMeta.CheckResult.AllFieldsFound" ), stepMeta );
                    remarks.add( cr );
                  }
                }
              } else {
                cr =
                    new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
                        "VerticaBulkLoaderMeta.CheckResult.NoFields" ), stepMeta );
                remarks.add( cr );
              }
            } else {
              cr =
                  new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
                      "VerticaBulkLoaderMeta.CheckResult.TableNotAccessible" ), stepMeta );
              remarks.add( cr );
            }
          } else {
            cr =
                new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
                    "VerticaBulkLoaderMeta.CheckResult.TableError", schemaTable ), stepMeta );
            remarks.add( cr );
          }
        } else {
          cr =
              new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
                  "VerticaBulkLoaderMeta.CheckResult.NoTableName" ), stepMeta );
          remarks.add( cr );
        }
      } catch ( KettleException e ) {
        cr =
            new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
                "VerticaBulkLoaderMeta.CheckResult.UndefinedError", e.getMessage() ), stepMeta );
        remarks.add( cr );
      } finally {
        db.disconnect();
      }
    } else {
      CheckResult cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
              "VerticaBulkLoaderMeta.CheckResult.NoConnection" ), stepMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      CheckResult cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
              "VerticaBulkLoaderMeta.CheckResult.ExpectedInputOk" ), stepMeta );
      remarks.add( cr );
    } else {
      CheckResult cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
              "VerticaBulkLoaderMeta.CheckResult.ExpectedInputError" ), stepMeta );
      remarks.add( cr );
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
      Trans trans ) {
    return new VerticaBulkLoader( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  public StepDataInterface getStepData() {
    return new VerticaBulkLoaderData();
  }

  public void analyseImpact( List<DatabaseImpact> impact, TransMeta transMeta, StepMeta stepMeta,
      RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info ) {
    // The values that are entering this step are in "prev":
    if ( prev != null ) {
      for ( int i = 0; i < prev.size(); i++ ) {
        ValueMetaInterface v = prev.getValueMeta( i );
        DatabaseImpact ii =
            new DatabaseImpact( DatabaseImpact.TYPE_IMPACT_WRITE, transMeta.getName(), stepMeta.getName(), databaseMeta
                .getDatabaseName(), transMeta.environmentSubstitute( tablename ), v.getName(), v.getName(), v != null
                ? v.getOrigin() : "?", "", "Type = " + v.toStringMeta() );
        impact.add( ii );
      }
    }
  }

  public SQLStatement getSQLStatements( TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev ) {
    SQLStatement retval = new SQLStatement( stepMeta.getName(), databaseMeta, null ); // default: nothing to do!

    if ( databaseMeta != null ) {
      if ( prev != null && prev.size() > 0 ) {
        if ( !Const.isEmpty( transMeta.environmentSubstitute( tablename ) ) ) {
          Database db = new Database( transMeta, databaseMeta );
          db.shareVariablesWith( transMeta );
          try {
            db.connect();

            String schemaTable =
                databaseMeta.getQuotedSchemaTableCombination( transMeta.environmentSubstitute( schemaName ), transMeta
                    .environmentSubstitute( tablename ) );
            String cr_table = db.getDDL( schemaTable, prev );

            // Empty string means: nothing to do: set it to null...
            if ( cr_table == null || cr_table.length() == 0 ) {
              cr_table = null;
            }

            retval.setSQL( cr_table );
          } catch ( KettleDatabaseException dbe ) {
            retval.setError( BaseMessages.getString( PKG, "VerticaBulkLoaderMeta.Error.ErrorConnecting", dbe
                .getMessage() ) );
          } finally {
            db.disconnect();
          }
        } else {
          retval.setError( BaseMessages.getString( PKG, "VerticaBulkLoaderMeta.Error.NoTable" ) );
        }
      } else {
        retval.setError( BaseMessages.getString( PKG, "VerticaBulkLoaderMeta.Error.NoInput" ) );
      }
    } else {
      retval.setError( BaseMessages.getString( PKG, "VerticaBulkLoaderMeta.Error.NoConnection" ) );
    }

    return retval;
  }

  public RowMetaInterface getRequiredFields( VariableSpace space ) throws KettleException {
    String realTableName = space.environmentSubstitute( tablename );
    String realSchemaName = space.environmentSubstitute( schemaName );

    if ( databaseMeta != null ) {
      Database db = new Database( loggingObject, databaseMeta );
      try {
        db.connect();

        if ( !Const.isEmpty( realTableName ) ) {
          String schemaTable = databaseMeta.getQuotedSchemaTableCombination( realSchemaName, realTableName );

          // Check if this table exists...
          if ( db.checkTableExists( schemaTable ) ) {
            return db.getTableFields( schemaTable );
          } else {
            throw new KettleException( BaseMessages.getString( PKG, "VerticaBulkLoaderMeta.Exception.TableNotFound" ) );
          }
        } else {
          throw new KettleException( BaseMessages.getString( PKG, "VerticaBulkLoaderMeta.Exception.TableNotSpecified" ) );
        }
      } catch ( Exception e ) {
        throw new KettleException( BaseMessages.getString( PKG, "VerticaBulkLoaderMeta.Exception.ErrorGettingFields" ),
            e );
      } finally {
        db.disconnect();
      }
    } else {
      throw new KettleException( BaseMessages.getString( PKG, "VerticaBulkLoaderMeta.Exception.ConnectionNotDefined" ) );
    }

  }

  public DatabaseMeta[] getUsedDatabaseConnections() {
    if ( databaseMeta != null ) {
      return new DatabaseMeta[] { databaseMeta };
    } else {
      return super.getUsedDatabaseConnections();
    }
  }

  /**
   * @return Fields containing the values in the input stream to insert.
   */
  public String[] getFieldStream() {
    return fieldStream;
  }

  /**
   * @param fieldStream
   *          The fields containing the values in the input stream to insert in the table.
   */
  public void setFieldStream( String[] fieldStream ) {
    this.fieldStream = fieldStream;
  }

  /**
   * @return Fields containing the fieldnames in the database insert.
   */
  public String[] getFieldDatabase() {
    return fieldDatabase;
  }

  /**
   * @param fieldDatabase
   *          The fields containing the names of the fields to insert.
   */
  public void setFieldDatabase( String[] fieldDatabase ) {
    this.fieldDatabase = fieldDatabase;
  }

  /**
   * @return the schemaName
   */
  public String getSchemaName() {
    return schemaName;
  }

  /**
   * @param schemaName
   *          the schemaName to set
   */
  public void setSchemaName( String schemaName ) {
    this.schemaName = schemaName;
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  public RowMetaInterface getTableRowMetaInterface() throws KettleException {

    if ( databaseMeta != null ) {
      // TODO
      DBCache.getInstance().clear( databaseMeta.getName() );

      Database db = new Database( loggingObject, databaseMeta );
      try {
        db.connect();

        if ( !Const.isEmpty( tablename ) ) {
          String schemaTable = databaseMeta.getQuotedSchemaTableCombination( schemaName, tablename );

          // Check if this table exists...
          if ( db.checkTableExists( schemaTable ) ) {
            return db.getTableFields( schemaTable );
          } else {
            throw new KettleException( BaseMessages.getString( PKG, "VerticaBulkLoaderMeta.Exception.TableNotFound" ) );
          }
        } else {
          throw new KettleException( BaseMessages.getString( PKG, "VerticaBulkLoaderMeta.Exception.TableNotSpecified" ) );
        }
      } catch ( Exception e ) {
        throw new KettleException( BaseMessages.getString( PKG, "VerticaBulkLoaderMeta.Exception.ErrorGettingFields" ),
            e );
      } finally {
        db.disconnect();
      }
    } else {
      throw new KettleException( BaseMessages.getString( PKG, "VerticaBulkLoaderMeta.Exception.ConnectionNotDefined" ) );
    }

  }

  @Override
  public String getMissingDatabaseConnectionInformationMessage() {
    // use default message
    return null;
  }

}
