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

package org.pentaho.di.ui.verticabulkload;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.SQLStatement;
import org.pentaho.di.core.SourceToTargetMapping;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.ui.core.database.dialog.DatabaseExplorerDialog;
import org.pentaho.di.ui.core.database.dialog.SQLEditor;
import org.pentaho.di.ui.core.dialog.EnterMappingDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;
import org.pentaho.di.verticabulkload.VerticaBulkLoaderMeta;



/**
 * Dialog class for table output step.
 * 
 * @author Matt Casters
 */
public class VerticaBulkLoaderDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = VerticaBulkLoaderMeta.class; // for i18n purposes, needed by Translator2!!

  private CTabFolder wTabFolder;
  private FormData fdTabFolder;

  private CTabItem wMainTab, wFieldsTab;
  private FormData fdMainComp, fdFieldsComp;

  private CCombo wConnection;

  private Label wlSchema;
  private TextVar wSchema;
  private FormData fdlSchema, fdSchema;

  private Label wlTable;
  private Button wbTable;
  private TextVar wTable;
  private FormData fdlTable, fdbTable, fdTable;

  private Label wlExceptionsLogFile;
  private TextVar wExceptionsLogFile;
  private FormData fdlExceptionsLogFile, fdExceptionsLogFile;

  private Label wlRejectedDataLogFile;
  private TextVar wRejectedDataLogFile;
  private FormData fdlRejectedDataLogFile, fdRejectedDataLogFile;

  private Label wlStreamName;
  private TextVar wStreamName;
  private FormData fdlStreamName, fdStreamName;

  private Label wlAbortOnError;
  private Button wAbortOnError;
  private FormData fdlAbortOnError, fdAbortOnError;

  private Label wlDirect;
  private Button wDirect;
  private FormData fdlDirect, fdDirect;

  private Label wlSpecifyFields;
  private Button wSpecifyFields;
  private FormData fdlSpecifyFields, fdSpecifyFields;

  private Label wlFields;
  private TableView wFields;

  private Button wGetFields;
  private FormData fdGetFields;

  private Button wDoMapping;
  private FormData fdDoMapping;

  private VerticaBulkLoaderMeta input;

  private Map<String, Integer> inputFields;

  private ColumnInfo[] ciFields;

  /**
   * List of ColumnInfo that should have the field names of the selected database table
   */
  private List<ColumnInfo> tableFieldColumns = new ArrayList<ColumnInfo>();

  /**
   * Constructor.
   */
  public VerticaBulkLoaderDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (VerticaBulkLoaderMeta) in;
    inputFields = new HashMap<String, Integer>();
  }

  /**
   * Open the dialog.
   */
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        input.setChanged();
      }
    };
    FocusListener lsFocusLost = new FocusAdapter() {
      public void focusLost( FocusEvent arg0 ) {
        setTableFieldCombo();
      }
    };
    backupChanged = input.hasChanged();

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.DialogTitle" ) );

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( "System.Label.StepName" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );

    // Connection line
    wConnection = addConnectionLine( shell, wStepname, middle, margin );
    if ( input.getDatabaseMeta() == null && transMeta.nrDatabases() == 1 ) {
      wConnection.select( 0 );
    }
    wConnection.addModifyListener( lsMod );
    wConnection.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent event ) {
        setFlags();
      }
    } );

    // Schema line...
    wlSchema = new Label( shell, SWT.RIGHT );
    wlSchema.setText( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.TargetSchema.Label" ) ); //$NON-NLS-1$
    props.setLook( wlSchema );
    fdlSchema = new FormData();
    fdlSchema.left = new FormAttachment( 0, 0 );
    fdlSchema.right = new FormAttachment( middle, -margin );
    fdlSchema.top = new FormAttachment( wConnection, margin * 2 );
    wlSchema.setLayoutData( fdlSchema );

    wSchema = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSchema );
    wSchema.addModifyListener( lsMod );
    wSchema.addFocusListener( lsFocusLost );
    fdSchema = new FormData();
    fdSchema.left = new FormAttachment( middle, 0 );
    fdSchema.top = new FormAttachment( wConnection, margin * 2 );
    fdSchema.right = new FormAttachment( 100, 0 );
    wSchema.setLayoutData( fdSchema );

    // Table line...
    wlTable = new Label( shell, SWT.RIGHT );
    wlTable.setText( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.TargetTable.Label" ) );
    props.setLook( wlTable );
    fdlTable = new FormData();
    fdlTable.left = new FormAttachment( 0, 0 );
    fdlTable.right = new FormAttachment( middle, -margin );
    fdlTable.top = new FormAttachment( wSchema, margin );
    wlTable.setLayoutData( fdlTable );

    wbTable = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbTable );
    wbTable.setText( BaseMessages.getString( "System.Button.Browse" ) );
    fdbTable = new FormData();
    fdbTable.right = new FormAttachment( 100, 0 );
    fdbTable.top = new FormAttachment( wSchema, margin );
    wbTable.setLayoutData( fdbTable );

    wTable = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTable );
    wTable.addModifyListener( lsMod );
    wTable.addFocusListener( lsFocusLost );
    fdTable = new FormData();
    fdTable.top = new FormAttachment( wSchema, margin );
    fdTable.left = new FormAttachment( middle, 0 );
    fdTable.right = new FormAttachment( wbTable, -margin );
    wTable.setLayoutData( fdTable );

    SelectionAdapter lsSelMod = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };

    // Specify fields
    wlSpecifyFields = new Label( shell, SWT.RIGHT );
    wlSpecifyFields.setText( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.SpecifyFields.Label" ) );
    props.setLook( wlSpecifyFields );
    fdlSpecifyFields = new FormData();
    fdlSpecifyFields.left = new FormAttachment( 0, 0 );
    fdlSpecifyFields.top = new FormAttachment( wbTable, margin );
    fdlSpecifyFields.right = new FormAttachment( middle, -margin );
    wlSpecifyFields.setLayoutData( fdlSpecifyFields );
    wSpecifyFields = new Button( shell, SWT.CHECK );
    props.setLook( wSpecifyFields );
    fdSpecifyFields = new FormData();
    fdSpecifyFields.left = new FormAttachment( middle, 0 );
    fdSpecifyFields.top = new FormAttachment( wbTable, margin );
    fdSpecifyFields.right = new FormAttachment( 100, 0 );
    wSpecifyFields.setLayoutData( fdSpecifyFields );
    wSpecifyFields.addSelectionListener( lsSelMod );

    // If the flag is off, gray out the fields tab e.g.
    wSpecifyFields.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        setFlags();
      }
    } );

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF KEY TAB ///
    // /
    wMainTab = new CTabItem( wTabFolder, SWT.NONE );
    wMainTab.setText( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.MainTab.CTabItem" ) ); //$NON-NLS-1$

    FormLayout mainLayout = new FormLayout();
    mainLayout.marginWidth = 3;
    mainLayout.marginHeight = 3;

    Composite wMainComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wMainComp );
    wMainComp.setLayout( mainLayout );

    fdMainComp = new FormData();
    fdMainComp.left = new FormAttachment( 0, 0 );
    fdMainComp.top = new FormAttachment( 0, 0 );
    fdMainComp.right = new FormAttachment( 100, 0 );
    fdMainComp.bottom = new FormAttachment( 100, 0 );
    wMainComp.setLayoutData( fdMainComp );

    // Insert directly to ROS
    wlDirect = new Label( wMainComp, SWT.RIGHT );
    wlDirect.setText( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.InsertDirect.Label" ) );
    wlDirect.setToolTipText( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.InsertDirect.Tooltip" ) );
    props.setLook( wlDirect );
    fdlDirect = new FormData();
    fdlDirect.left = new FormAttachment( 0, 0 );
    fdlDirect.top = new FormAttachment( 0, margin );
    fdlDirect.right = new FormAttachment( middle, -margin );
    wlDirect.setLayoutData( fdlDirect );
    wDirect = new Button( wMainComp, SWT.CHECK );
    wDirect.setToolTipText( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.InsertDirect.Tooltip" ) );
    props.setLook( wDirect );
    fdDirect = new FormData();
    fdDirect.left = new FormAttachment( middle, 0 );
    fdDirect.top = new FormAttachment( 0, margin );
    fdDirect.right = new FormAttachment( 100, 0 );
    wDirect.setLayoutData( fdDirect );
    wDirect.addSelectionListener( lsSelMod );

    // Abort on error
    wlAbortOnError = new Label( wMainComp, SWT.RIGHT );
    wlAbortOnError.setText( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.AbortOnError.Label" ) );
    wlAbortOnError.setToolTipText( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.AbortOnError.Tooltip" ) );
    props.setLook( wlAbortOnError );
    fdlAbortOnError = new FormData();
    fdlAbortOnError.left = new FormAttachment( 0, 0 );
    fdlAbortOnError.top = new FormAttachment( wDirect, margin );
    fdlAbortOnError.right = new FormAttachment( middle, -margin );
    wlAbortOnError.setLayoutData( fdlAbortOnError );
    wAbortOnError = new Button( wMainComp, SWT.CHECK );
    wAbortOnError.setToolTipText( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.AbortOnError.Tooltip" ) );
    props.setLook( wAbortOnError );
    fdAbortOnError = new FormData();
    fdAbortOnError.left = new FormAttachment( middle, 0 );
    fdAbortOnError.top = new FormAttachment( wDirect, margin );
    fdAbortOnError.right = new FormAttachment( 100, 0 );
    wAbortOnError.setLayoutData( fdAbortOnError );
    wAbortOnError.addSelectionListener( lsSelMod );

    // ExceptionsLogFile line...
    wlExceptionsLogFile = new Label( wMainComp, SWT.RIGHT );
    wlExceptionsLogFile.setText( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.ExceptionsLogFile.Label" ) ); //$NON-NLS-1$
    wlExceptionsLogFile.setToolTipText( BaseMessages.getString( PKG,
        "VerticaBulkLoaderDialog.ExceptionsLogFile.Tooltip" ) ); //$NON-NLS-1$
    props.setLook( wlExceptionsLogFile );
    fdlExceptionsLogFile = new FormData();
    fdlExceptionsLogFile.left = new FormAttachment( 0, 0 );
    fdlExceptionsLogFile.right = new FormAttachment( middle, -margin );
    fdlExceptionsLogFile.top = new FormAttachment( wAbortOnError, margin * 2 );
    wlExceptionsLogFile.setLayoutData( fdlExceptionsLogFile );

    wExceptionsLogFile = new TextVar( transMeta, wMainComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wExceptionsLogFile.setToolTipText( BaseMessages
        .getString( PKG, "VerticaBulkLoaderDialog.ExceptionsLogFile.Tooltip" ) ); //$NON-NLS-1$
    props.setLook( wExceptionsLogFile );
    wExceptionsLogFile.addModifyListener( lsMod );
    wExceptionsLogFile.addFocusListener( lsFocusLost );
    fdExceptionsLogFile = new FormData();
    fdExceptionsLogFile.left = new FormAttachment( middle, 0 );
    fdExceptionsLogFile.top = new FormAttachment( wAbortOnError, margin * 2 );
    fdExceptionsLogFile.right = new FormAttachment( 100, 0 );
    wExceptionsLogFile.setLayoutData( fdExceptionsLogFile );

    // RejectedDataLogFile line...
    wlRejectedDataLogFile = new Label( wMainComp, SWT.RIGHT );
    wlRejectedDataLogFile.setText( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.RejectedDataLogFile.Label" ) ); //$NON-NLS-1$
    wlRejectedDataLogFile.setToolTipText( BaseMessages.getString( PKG,
        "VerticaBulkLoaderDialog.RejectedDataLogFile.Tooltip" ) ); //$NON-NLS-1$
    props.setLook( wlRejectedDataLogFile );
    fdlRejectedDataLogFile = new FormData();
    fdlRejectedDataLogFile.left = new FormAttachment( 0, 0 );
    fdlRejectedDataLogFile.right = new FormAttachment( middle, -margin );
    fdlRejectedDataLogFile.top = new FormAttachment( wExceptionsLogFile, margin * 2 );
    wlRejectedDataLogFile.setLayoutData( fdlRejectedDataLogFile );

    wRejectedDataLogFile = new TextVar( transMeta, wMainComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wRejectedDataLogFile.setToolTipText( BaseMessages.getString( PKG,
        "VerticaBulkLoaderDialog.RejectedDataLogFile.Tooltip" ) ); //$NON-NLS-1$
    props.setLook( wRejectedDataLogFile );
    wRejectedDataLogFile.addModifyListener( lsMod );
    wRejectedDataLogFile.addFocusListener( lsFocusLost );
    fdRejectedDataLogFile = new FormData();
    fdRejectedDataLogFile.left = new FormAttachment( middle, 0 );
    fdRejectedDataLogFile.top = new FormAttachment( wExceptionsLogFile, margin * 2 );
    fdRejectedDataLogFile.right = new FormAttachment( 100, 0 );
    wRejectedDataLogFile.setLayoutData( fdRejectedDataLogFile );

    // StreamName line...
    wlStreamName = new Label( wMainComp, SWT.RIGHT );
    wlStreamName.setText( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.StreamName.Label" ) ); //$NON-NLS-1$
    wlStreamName.setToolTipText( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.StreamName.Tooltip" ) ); //$NON-NLS-1$
    props.setLook( wlStreamName );
    fdlStreamName = new FormData();
    fdlStreamName.left = new FormAttachment( 0, 0 );
    fdlStreamName.right = new FormAttachment( middle, -margin );
    fdlStreamName.top = new FormAttachment( wRejectedDataLogFile, margin * 2 );
    wlStreamName.setLayoutData( fdlStreamName );

    wStreamName = new TextVar( transMeta, wMainComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStreamName.setToolTipText( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.StreamName.Tooltip" ) ); //$NON-NLS-1$
    props.setLook( wStreamName );
    wStreamName.addModifyListener( lsMod );
    wStreamName.addFocusListener( lsFocusLost );
    fdStreamName = new FormData();
    fdStreamName.left = new FormAttachment( middle, 0 );
    fdStreamName.top = new FormAttachment( wRejectedDataLogFile, margin * 2 );
    fdStreamName.right = new FormAttachment( 100, 0 );
    wStreamName.setLayoutData( fdStreamName );

    wMainComp.layout();
    wMainTab.setControl( wMainComp );

    //
    // Fields tab...
    //
    wFieldsTab = new CTabItem( wTabFolder, SWT.NONE );
    wFieldsTab.setText( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.FieldsTab.CTabItem.Title" ) ); //$NON-NLS-1$

    Composite wFieldsComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wFieldsComp );

    FormLayout fieldsCompLayout = new FormLayout();
    fieldsCompLayout.marginWidth = Const.FORM_MARGIN;
    fieldsCompLayout.marginHeight = Const.FORM_MARGIN;
    wFieldsComp.setLayout( fieldsCompLayout );

    // The fields table
    wlFields = new Label( wFieldsComp, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.InsertFields.Label" ) ); //$NON-NLS-1$
    props.setLook( wlFields );
    FormData fdlUpIns = new FormData();
    fdlUpIns.left = new FormAttachment( 0, 0 );
    fdlUpIns.top = new FormAttachment( 0, margin );
    wlFields.setLayoutData( fdlUpIns );

    int tableCols = 2;
    int UpInsRows = ( input.getFieldStream() != null ? input.getFieldStream().length : 1 );

    ciFields = new ColumnInfo[tableCols];
    ciFields[0] =
        new ColumnInfo(
            BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.ColumnInfo.TableField" ), ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false ); //$NON-NLS-1$
    ciFields[1] =
        new ColumnInfo(
            BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.ColumnInfo.StreamField" ), ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false ); //$NON-NLS-1$
    tableFieldColumns.add( ciFields[0] );
    wFields =
        new TableView( transMeta, wFieldsComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL
            | SWT.H_SCROLL, ciFields, UpInsRows, lsMod, props );

    wGetFields = new Button( wFieldsComp, SWT.PUSH );
    wGetFields.setText( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.GetFields.Button" ) ); //$NON-NLS-1$
    fdGetFields = new FormData();
    fdGetFields.top = new FormAttachment( wlFields, margin );
    fdGetFields.right = new FormAttachment( 100, 0 );
    wGetFields.setLayoutData( fdGetFields );

    wDoMapping = new Button( wFieldsComp, SWT.PUSH );
    wDoMapping.setText( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.DoMapping.Button" ) ); //$NON-NLS-1$
    fdDoMapping = new FormData();
    fdDoMapping.top = new FormAttachment( wGetFields, margin );
    fdDoMapping.right = new FormAttachment( 100, 0 );
    wDoMapping.setLayoutData( fdDoMapping );

    wDoMapping.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event arg0 ) {
        generateMappings();
      }
    } );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( wDoMapping, -margin );
    fdFields.bottom = new FormAttachment( 100, -2 * margin );
    wFields.setLayoutData( fdFields );

    fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment( 0, 0 );
    fdFieldsComp.top = new FormAttachment( 0, 0 );
    fdFieldsComp.right = new FormAttachment( 100, 0 );
    fdFieldsComp.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData( fdFieldsComp );

    wFieldsComp.layout();
    wFieldsTab.setControl( wFieldsComp );

    //
    // Search the fields in the background
    //

    final Runnable runnable = new Runnable() {
      public void run() {
        StepMeta stepMeta = transMeta.findStep( stepname );
        if ( stepMeta != null ) {
          try {
            RowMetaInterface row = transMeta.getPrevStepFields( stepMeta );

            // Remember these fields...
            for ( int i = 0; i < row.size(); i++ ) {
              inputFields.put( row.getValueMeta( i ).getName(), Integer.valueOf( i ) );
            }

            setComboBoxes();
          } catch ( KettleException e ) {
            log.logError( toString(), BaseMessages.getString( "System.Dialog.GetFieldsFailed.Message" ) );
          }
        }
      }
    };
    new Thread( runnable ).start();

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( "System.Button.OK" ) );
    wCreate = new Button( shell, SWT.PUSH );
    wCreate.setText( BaseMessages.getString( "System.Button.SQL" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOK, wCancel, wCreate }, margin, null );

    fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wSpecifyFields, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOK, -margin );
    wTabFolder.setLayoutData( fdTabFolder );

    // Add listeners
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsCreate = new Listener() {
      public void handleEvent( Event e ) {
        sql();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };
    lsGet = new Listener() {
      public void handleEvent( Event e ) {
        get();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wCreate.addListener( SWT.Selection, lsCreate );
    wCancel.addListener( SWT.Selection, lsCancel );
    wGetFields.addListener( SWT.Selection, lsGet );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );
    wSchema.addSelectionListener( lsDef );
    wTable.addSelectionListener( lsDef );

    wbTable.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        getTableName();
      }
    } );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    wTabFolder.setSelection( 0 );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    setTableFieldCombo();
    input.setChanged( backupChanged );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;
  }

  /**
   * Reads in the fields from the previous steps and from the ONE next step and opens an EnterMappingDialog with this
   * information. After the user did the mapping, those information is put into the Select/Rename table.
   */
  private void generateMappings() {

    // Determine the source and target fields...
    //
    RowMetaInterface sourceFields;
    RowMetaInterface targetFields;

    try {
      sourceFields = transMeta.getPrevStepFields( stepMeta );
    } catch ( KettleException e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG,
          "VerticaBulkLoaderDialog.DoMapping.UnableToFindSourceFields.Title" ), BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.DoMapping.UnableToFindSourceFields.Message" ), e );
      return;
    }

    // refresh data
    input.setDatabaseMeta( transMeta.findDatabase( wConnection.getText() ) );
    input.setTablename( transMeta.environmentSubstitute( wTable.getText() ) );
    StepMetaInterface stepMetaInterface = stepMeta.getStepMetaInterface();
    try {
      targetFields = stepMetaInterface.getRequiredFields( transMeta );
    } catch ( KettleException e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG,
          "VerticaBulkLoaderDialog.DoMapping.UnableToFindTargetFields.Title" ), BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.DoMapping.UnableToFindTargetFields.Message" ), e );
      return;
    }

    String[] inputNames = new String[sourceFields.size()];
    for ( int i = 0; i < sourceFields.size(); i++ ) {
      ValueMetaInterface value = sourceFields.getValueMeta( i );
      inputNames[i] = value.getName() + EnterMappingDialog.STRING_ORIGIN_SEPARATOR + value.getOrigin() + ")";
    }

    // Create the existing mapping list...
    //
    List<SourceToTargetMapping> mappings = new ArrayList<SourceToTargetMapping>();
    StringBuffer missingSourceFields = new StringBuffer();
    StringBuffer missingTargetFields = new StringBuffer();

    int nrFields = wFields.nrNonEmpty();
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wFields.getNonEmpty( i );
      String source = item.getText( 2 );
      String target = item.getText( 1 );

      int sourceIndex = sourceFields.indexOfValue( source );
      if ( sourceIndex < 0 ) {
        missingSourceFields.append( Const.CR + "   " + source + " --> " + target );
      }
      int targetIndex = targetFields.indexOfValue( target );
      if ( targetIndex < 0 ) {
        missingTargetFields.append( Const.CR + "   " + source + " --> " + target );
      }
      if ( sourceIndex < 0 || targetIndex < 0 ) {
        continue;
      }

      SourceToTargetMapping mapping = new SourceToTargetMapping( sourceIndex, targetIndex );
      mappings.add( mapping );
    }

    // show a confirm dialog if some missing field was found
    //
    if ( missingSourceFields.length() > 0 || missingTargetFields.length() > 0 ) {

      String message = "";
      if ( missingSourceFields.length() > 0 ) {
        message +=
            BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.DoMapping.SomeSourceFieldsNotFound",
                missingSourceFields.toString() )
                + Const.CR;
      }
      if ( missingTargetFields.length() > 0 ) {
        message +=
            BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.DoMapping.SomeTargetFieldsNotFound",
                missingSourceFields.toString() )
                + Const.CR;
      }
      message += Const.CR;
      message +=
          BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.DoMapping.SomeFieldsNotFoundContinue" ) + Const.CR;
      boolean goOn =
          MessageDialog.openConfirm( shell, BaseMessages.getString( PKG,
              "VerticaBulkLoaderDialog.DoMapping.SomeFieldsNotFoundTitle" ), message );
      if ( !goOn ) {
        return;
      }
    }
    EnterMappingDialog d =
        new EnterMappingDialog( VerticaBulkLoaderDialog.this.shell, sourceFields.getFieldNames(), targetFields
            .getFieldNames(), mappings );
    mappings = d.open();

    // mappings == null if the user pressed cancel
    //
    if ( mappings != null ) {
      // Clear and re-populate!
      //
      wFields.table.removeAll();
      wFields.table.setItemCount( mappings.size() );
      for ( int i = 0; i < mappings.size(); i++ ) {
        SourceToTargetMapping mapping = (SourceToTargetMapping) mappings.get( i );
        TableItem item = wFields.table.getItem( i );
        item.setText( 2, sourceFields.getValueMeta( mapping.getSourcePosition() ).getName() );
        item.setText( 1, targetFields.getValueMeta( mapping.getTargetPosition() ).getName() );
      }
      wFields.setRowNums();
      wFields.optWidth( true );
    }
  }

  private void setTableFieldCombo() {
    Runnable fieldLoader = new Runnable() {
      public void run() {
        // clear
        for ( int i = 0; i < tableFieldColumns.size(); i++ ) {
          ColumnInfo colInfo = (ColumnInfo) tableFieldColumns.get( i );
          colInfo.setComboValues( new String[] {} );
        }
        if ( !Const.isEmpty( wTable.getText() ) ) {
          DatabaseMeta ci = transMeta.findDatabase( wConnection.getText() );
          if ( ci != null ) {
            Database db = new Database( loggingObject, ci );
            try {
              db.connect();

              String schemaTable =
                  ci.getQuotedSchemaTableCombination( transMeta.environmentSubstitute( wSchema.getText() ), transMeta
                      .environmentSubstitute( wTable.getText() ) );
              RowMetaInterface r = db.getTableFields( schemaTable );
              if ( null != r ) {
                String[] fieldNames = r.getFieldNames();
                if ( null != fieldNames ) {
                  for ( int i = 0; i < tableFieldColumns.size(); i++ ) {
                    ColumnInfo colInfo = (ColumnInfo) tableFieldColumns.get( i );
                    colInfo.setComboValues( fieldNames );
                  }
                }
              }
            } catch ( Exception e ) {
              for ( int i = 0; i < tableFieldColumns.size(); i++ ) {
                ColumnInfo colInfo = (ColumnInfo) tableFieldColumns.get( i );
                colInfo.setComboValues( new String[] {} );
              }
              // ignore any errors here. drop downs will not be
              // filled, but no problem for the user
            }
          }
        }
      }
    };
    shell.getDisplay().asyncExec( fieldLoader );
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<String, Integer>();

    // Add the currentMeta fields...
    fields.putAll( inputFields );

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<String>( keySet );

    String[] fieldNames = (String[]) entries.toArray( new String[entries.size()] );

    Const.sortStrings( fieldNames );
    ciFields[1].setComboValues( fieldNames );
  }

  public void setFlags() {
    boolean specifyFields = wSpecifyFields.getSelection();
    wFields.setEnabled( specifyFields );
    wGetFields.setEnabled( specifyFields );

  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( input.getSchemaName() != null ) {
      wSchema.setText( input.getSchemaName() );
    }
    if ( input.getTableName() != null ) {
      wTable.setText( input.getTableName() );
    }
    if ( input.getDatabaseMeta() != null ) {
      wConnection.setText( input.getDatabaseMeta().getName() );
    }

    if ( input.getExceptionsFileName() != null ) {
      wExceptionsLogFile.setText( input.getExceptionsFileName() );
    }
    if ( input.getRejectedDataFileName() != null ) {
      wRejectedDataLogFile.setText( input.getRejectedDataFileName() );
    }
    if ( input.getStreamName() != null ) {
      wStreamName.setText( input.getStreamName() );
    }

    wDirect.setSelection( input.isDirect() );
    wAbortOnError.setSelection( input.isAbortOnError() );

    wSpecifyFields.setSelection( input.specifyFields() );

    for ( int i = 0; i < input.getFieldDatabase().length; i++ ) {
      TableItem item = wFields.table.getItem( i );
      if ( input.getFieldDatabase()[i] != null ) {
        item.setText( 1, input.getFieldDatabase()[i] );
      }
      if ( input.getFieldStream()[i] != null ) {
        item.setText( 2, input.getFieldStream()[i] );
      }
    }

    setFlags();

    wStepname.selectAll();
  }

  private void cancel() {
    stepname = null;
    input.setChanged( backupChanged );
    dispose();
  }

  private void getInfo( VerticaBulkLoaderMeta info ) {
    info.setSchemaName( wSchema.getText() );
    info.setTablename( wTable.getText() );
    info.setDatabaseMeta( transMeta.findDatabase( wConnection.getText() ) );

    info.setExceptionsFileName( wExceptionsLogFile.getText() );
    info.setRejectedDataFileName( wRejectedDataLogFile.getText() );
    info.setStreamName( wStreamName.getText() );

    info.setDirect( wDirect.getSelection() );
    info.setAbortOnError( wAbortOnError.getSelection() );

    info.setSpecifyFields( wSpecifyFields.getSelection() );

    int nrRows = wFields.nrNonEmpty();
    info.allocate( nrRows );
    for ( int i = 0; i < nrRows; i++ ) {
      TableItem item = wFields.getNonEmpty( i );
      ( info.getFieldDatabase() )[i] = Const.NVL( item.getText( 1 ), "" );
      ( info.getFieldStream() )[i] = Const.NVL( item.getText( 2 ), "" );
    }
  }

  private void ok() {
    if ( Const.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText(); // return value

    getInfo( input );

    if ( input.getDatabaseMeta() == null ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.ConnectionError.DialogMessage" ) );
      mb.setText( BaseMessages.getString( "System.Dialog.Error.Title" ) );
      mb.open();
    }

    dispose();
  }

  private void getTableName() {
    // New class: SelectTableDialog
    int connr = wConnection.getSelectionIndex();
    if ( connr >= 0 ) {
      DatabaseMeta inf = transMeta.getDatabase( connr );

      log.logDebug( toString(), BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.Log.LookingAtConnection", inf
          .toString() ) );

      DatabaseExplorerDialog std = new DatabaseExplorerDialog( shell, SWT.NONE, inf, transMeta.getDatabases() );
      std.setSelectedSchemaAndTable( wSchema.getText(), wTable.getText() );
      if ( std.open() ) {
        wSchema.setText( Const.NVL( std.getSchemaName(), "" ) );
        wTable.setText( Const.NVL( std.getTableName(), "" ) );
      }
    } else {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.ConnectionError2.DialogMessage" ) );
      mb.setText( BaseMessages.getString( "System.Dialog.Error.Title" ) );
      mb.open();
    }

  }

  /**
   * Fill up the fields table with the incoming fields.
   */
  private void get() {
    try {
      RowMetaInterface r = transMeta.getPrevStepFields( stepname );
      if ( r != null && !r.isEmpty() ) {
        BaseStepDialog.getFieldsFromPrevious( r, wFields, 1, new int[] { 1, 2 }, new int[] {}, -1, -1, null );
      }
    } catch ( KettleException ke ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.FailedToGetFields.DialogTitle" ),
          BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.FailedToGetFields.DialogMessage" ), ke ); //$NON-NLS-1$ //$NON-NLS-2$
    }

  }

  // Generate code for create table...
  // Conversions done by Database
  //
  private void sql() {
    try {
      VerticaBulkLoaderMeta info = new VerticaBulkLoaderMeta();
      getInfo( info );
      RowMetaInterface prev = transMeta.getPrevStepFields( stepname );
      StepMeta stepMeta = transMeta.findStep( stepname );

      if ( info.specifyFields() ) {
        // Only use the fields that were specified.
        RowMetaInterface prevNew = new RowMeta();

        for ( int i = 0; i < info.getFieldDatabase().length; i++ ) {
          ValueMetaInterface insValue = prev.searchValueMeta( info.getFieldStream()[i] );
          if ( insValue != null ) {
            ValueMetaInterface insertValue = insValue.clone();
            insertValue.setName( info.getFieldDatabase()[i] );
            prevNew.addValueMeta( insertValue );
          } else {
            throw new KettleStepException( BaseMessages.getString( PKG,
                "VerticaBulkLoaderDialog.FailedToFindField.Message", info.getFieldStream()[i] ) ); //$NON-NLS-1$
          }
        }
        prev = prevNew;
      }

      SQLStatement sql = info.getSQLStatements( transMeta, stepMeta, prev );
      if ( !sql.hasError() ) {
        if ( sql.hasSQL() ) {
          SQLEditor sqledit =
              new SQLEditor( shell, SWT.NONE, info.getDatabaseMeta(), transMeta.getDbCache(), sql.getSQL() );
          sqledit.open();
        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
          mb.setMessage( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.NoSQL.DialogMessage" ) );
          mb.setText( BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.NoSQL.DialogTitle" ) );
          mb.open();
        }
      } else {
        MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
        mb.setMessage( sql.getError() );
        mb.setText( BaseMessages.getString( "System.Dialog.Error.Title" ) );
        mb.open();
      }
    } catch ( KettleException ke ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.BuildSQLError.DialogTitle" ),
          BaseMessages.getString( PKG, "VerticaBulkLoaderDialog.BuildSQLError.DialogMessage" ), ke );
    }
  }

  public String toString() {
    return this.getClass().getName();
  }
}
