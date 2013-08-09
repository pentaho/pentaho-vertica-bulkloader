/*
 * This plugin was initially developed by Daniel Einspanjer and is provided under the terms 
 * of the GNU Lesser General Public License, Version 2.1. You may not use 
 * this file except in compliance with the license. If you need a copy of the license, 
 * please go to http://www.gnu.org/licenses/lgpl-2.1.txt. 
 *
 * Software distributed under the GNU Lesser Public License is distributed on an "AS IS" 
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or  implied. Please refer to 
 * the license for the specific language governing your rights and limitations.
 */

package plugin.com.vertica.kettle.bulkloader;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PipedInputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.Executors;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import com.vertica.jdbc.VerticaConnection;
import com.vertica.jdbc.VerticaCopyStream;
import com.vertica.jdbc.nativebinary.ColumnSpec;
import com.vertica.jdbc.nativebinary.ColumnType;
import com.vertica.jdbc.nativebinary.StreamEncoder;


public class VerticaBulkLoader extends BaseStep implements StepInterface
{
	private static Class<?> PKG = VerticaBulkLoader.class; // for i18n purposes, needed by Translator2!!

	private VerticaBulkLoaderMeta meta;
	private VerticaBulkLoaderData data;

	public VerticaBulkLoader(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans)
	{
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	
	@Override
	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
		meta = (VerticaBulkLoaderMeta) smi;
		data = (VerticaBulkLoaderData) sdi;

		Object[] r = getRow(); // this also waits for a previous step to be
								// finished.
		if (r == null) // no more input to be expected...
		{

			try {
				data.close();
			} catch (IOException ioe) {
				throw new KettleStepException("Error releasing resources", ioe);
			}
			return false;
		}

		if (first) {

			first = false;

			data.outputRowMeta = getInputRowMeta().clone();
			meta.getFields(data.outputRowMeta, getStepname(), null, null, this);

			RowMetaInterface tableMeta = meta.getTableRowMetaInterface();

			if (!meta.specifyFields()) {
				
				// Just take the whole input row
				data.insertRowMeta = getInputRowMeta().clone();
				data.selectedRowFieldIndices = new int[data.insertRowMeta.size()];

				data.colSpecs = new ArrayList<ColumnSpec>(data.insertRowMeta.size());
				
				for (int insertFieldIdx = 0; insertFieldIdx < data.insertRowMeta.size(); insertFieldIdx++) {
					data.selectedRowFieldIndices[insertFieldIdx] = insertFieldIdx;
					ValueMetaInterface inputValueMeta = data.insertRowMeta.getValueMeta(insertFieldIdx);
					ValueMetaInterface insertValueMeta = inputValueMeta.clone();
					ValueMetaInterface targetValueMeta = tableMeta.getValueMeta(insertFieldIdx);
					insertValueMeta.setName(targetValueMeta.getName());
					data.insertRowMeta.setValueMeta(insertFieldIdx, insertValueMeta);
					ColumnSpec cs = getColumnSpecFromField(inputValueMeta, insertValueMeta, targetValueMeta);
					data.colSpecs.add(insertFieldIdx, cs);
				}
				
			} else {

				int numberOfInsertFields = meta.getFieldDatabase().length ;
				data.insertRowMeta = new RowMeta();
				data.colSpecs = new ArrayList<ColumnSpec>(numberOfInsertFields);

				// Cache the position of the selected fields in the row array
				data.selectedRowFieldIndices = new int[numberOfInsertFields];
				for (int insertFieldIdx = 0; insertFieldIdx < numberOfInsertFields; insertFieldIdx++) {

					String inputFieldName = meta.getFieldStream()[insertFieldIdx];
					int inputFieldIdx = getInputRowMeta().indexOfValue(inputFieldName);
					if (inputFieldIdx < 0) {
						throw new KettleStepException(BaseMessages.getString(PKG, "VerticaBulkLoader.Exception.FieldRequired", inputFieldName)); //$NON-NLS-1$
					}
					data.selectedRowFieldIndices[insertFieldIdx] = inputFieldIdx;

					String insertFieldName = meta.getFieldDatabase()[insertFieldIdx];
					ValueMetaInterface inputValueMeta = getInputRowMeta().getValueMeta(inputFieldIdx);
					if (inputValueMeta == null) {
						throw new KettleStepException(BaseMessages.getString(PKG, "VerticaBulkLoader.Exception.FailedToFindField", meta.getFieldStream()[insertFieldIdx])); //$NON-NLS-1$ 
					}
					ValueMetaInterface insertValueMeta = inputValueMeta.clone();
					insertValueMeta.setName(insertFieldName);
					data.insertRowMeta.addValueMeta(insertValueMeta);
					
					ValueMetaInterface targetValueMeta = tableMeta.searchValueMeta(insertFieldName);
					ColumnSpec cs = getColumnSpecFromField(inputValueMeta, insertValueMeta, targetValueMeta);
					data.colSpecs.add(insertFieldIdx, cs);
				}

			}

			try {
				data.pipedInputStream = new PipedInputStream();
				data.encoder = new StreamEncoder(data.colSpecs, data.pipedInputStream);

				initializeWorker();
				data.encoder.writeHeader();

			} catch (IOException ioe) {
				throw new KettleStepException("Error creating stream encoder", ioe);
			}

		}

		try {
			Object[] outputRowData = writeToOutputStream(r);
			if (outputRowData != null) {
				putRow(data.outputRowMeta, outputRowData);  // in case we want it
															// go further...
				incrementLinesOutput();
			}

			if (checkFeedback(getLinesRead())) {
				if (log.isBasic())
					logBasic("linenr " + getLinesRead()); //$NON-NLS-1$
			}
		} catch (KettleException e) {
			logError("Because of an error, this step can't continue: ", e);
			setErrors(1);
			stopAll();
			setOutputDone(); // signal end to receiver(s)
			return false;
		} catch (IOException e) {
			e.printStackTrace();
		}

		return true;
	}


	private ColumnSpec getColumnSpecFromField(ValueMetaInterface inputValueMeta, ValueMetaInterface insertValueMeta, ValueMetaInterface targetValueMeta) {
		logBasic("Mapping input field " + inputValueMeta.getName() + " (" + inputValueMeta.getTypeDesc() + ")"
				+ " to target column " + insertValueMeta.getName() + " (" + targetValueMeta.getOriginalColumnTypeName() + ") " );

		String targetColumnTypeName = targetValueMeta.getOriginalColumnTypeName();

		if (targetColumnTypeName.equals("INTEGER") || targetColumnTypeName.equals("BIGINT")) {
			return new ColumnSpec(ColumnSpec.ConstantWidthType.INTEGER_64);
		} else if (targetColumnTypeName.equals("BOOLEAN")) {
			return new ColumnSpec(ColumnSpec.ConstantWidthType.BOOLEAN);
		} else if (targetColumnTypeName.equals("FLOAT") || targetColumnTypeName.equals("DOUBLE PRECISION")) {
			return new ColumnSpec(ColumnSpec.ConstantWidthType.FLOAT);
		} else if (targetColumnTypeName.equals("CHAR")) {
			return new ColumnSpec(ColumnSpec.UserDefinedWidthType.CHAR, targetValueMeta.getLength());
		} else if (targetColumnTypeName.equals("VARCHAR")) {
			return new ColumnSpec(ColumnSpec.VariableWidthType.VARCHAR);
		} else if (targetColumnTypeName.equals("DATE")) {
			if (inputValueMeta.isDate() == false) 
				throw new IllegalArgumentException("Field " + inputValueMeta.getName() + " must be a Date compatible type to match target column " + insertValueMeta.getName());
			else 
				return new ColumnSpec(ColumnSpec.ConstantWidthType.DATE);
		} else if (targetColumnTypeName.equals("TIME")) {
			if (inputValueMeta.isDate() == false) 
				throw new IllegalArgumentException("Field " + inputValueMeta.getName() + " must be a Date compatible type to match target column " + insertValueMeta.getName());
			else 
				return new ColumnSpec(ColumnSpec.ConstantWidthType.TIME);
		} else if (targetColumnTypeName.equals("TIMETZ")) {
			if (inputValueMeta.isDate() == false) 
				throw new IllegalArgumentException("Field " + inputValueMeta.getName() + " must be a Date compatible type to match target column " + insertValueMeta.getName());
			else 
				return new ColumnSpec(ColumnSpec.ConstantWidthType.TIMETZ);
		} else if (targetColumnTypeName.equals("TIMESTAMP")) {
			if (inputValueMeta.isDate() == false) 
				throw new IllegalArgumentException("Field " + inputValueMeta.getName() + " must be a Date compatible type to match target column " + insertValueMeta.getName());
			else 
				return new ColumnSpec(ColumnSpec.ConstantWidthType.TIMESTAMP);
		} else if (targetColumnTypeName.equals("TIMESTAMPTZ")) {
			if (inputValueMeta.isDate() == false) 
				throw new IllegalArgumentException("Field " + inputValueMeta.getName() + " must be a Date compatible type to match target column " + insertValueMeta.getName());
			else 
				return new ColumnSpec(ColumnSpec.ConstantWidthType.TIMESTAMPTZ);
		} else if (targetColumnTypeName.equals("INTERVAL") || targetColumnTypeName.equals("INTERVAL DAY TO SECOND")) {
			if (inputValueMeta.isDate() == false) 
				throw new IllegalArgumentException("Field " + inputValueMeta.getName() + " must be a Date compatible type to match target column " + insertValueMeta.getName());
			else 
				return new ColumnSpec(ColumnSpec.ConstantWidthType.INTERVAL);
		} else if (targetColumnTypeName.equals("BINARY")) {
			return new ColumnSpec(ColumnSpec.VariableWidthType.VARBINARY);
		} else if (targetColumnTypeName.equals("VARBINARY")) {
			return new ColumnSpec(ColumnSpec.VariableWidthType.VARBINARY);
		} else if (targetColumnTypeName.equals("NUMERIC")) {
			return new ColumnSpec(ColumnSpec.PrecisionScaleWidthType.NUMERIC, targetValueMeta.getLength(),targetValueMeta.getPrecision());
		}
		throw new IllegalArgumentException("Column type " + targetColumnTypeName + " not supported."); //$NON-NLS-1$ 
	}
    
	private void initializeWorker()
	{
		final String dml = buildCopyStatementSqlString();

		data.workerThread = Executors.defaultThreadFactory().newThread(new Runnable() {
			@Override
			public void run() {
				try
				{
					VerticaCopyStream stream = new VerticaCopyStream((VerticaConnection)(data.db.getConnection()), dml);
					stream.start();
					stream.addStream(data.pipedInputStream);
					setLinesRejected(stream.getRejects().size());
					stream.execute();
					long rowsLoaded = stream.finish();
					if (getLinesOutput() != rowsLoaded) {
						logMinimal(String.format("%d records loaded out of %d records sent.", rowsLoaded, getLinesOutput()));
					}
					data.db.disconnect();
				}
				catch (SQLException e)
				{
					if (e.getCause() instanceof InterruptedIOException)
					{
						logBasic("SQL statement interrupted by halt of transformation");
					}
					else
					{
						logError("SQL Error during statement execution.", e);
						setErrors(1);
						stopAll();
						setOutputDone();  // signal end to receiver(s)
					}
				} 
			}
		});

		data.workerThread.start();
	}

	private String buildCopyStatementSqlString()
	{
		final DatabaseMeta databaseMeta = data.db.getDatabaseMeta();

		StringBuilder sb = new StringBuilder(150);
		sb.append("COPY ");

		sb.append(
						databaseMeta.getQuotedSchemaTableCombination(
										environmentSubstitute(meta.getSchemaName()),
										environmentSubstitute(meta.getTablename())
						)
		);

		sb.append(" (");
		final RowMetaInterface fields = data.insertRowMeta;
		for (int i = 0; i < fields.size(); i++)
		{
			if (i > 0) sb.append(", ");
			ColumnType columnType = data.colSpecs.get(i).type;
			switch (columnType) {
			case NUMERIC:
				sb.append("TMPFILLERCOL").append(i).append(" FILLER VARCHAR(1000), ");
				// Force columns to be quoted:
				sb.append(databaseMeta.getStartQuote() + fields.getValueMeta(i).getName() + databaseMeta.getEndQuote());
				sb.append(" as TO_NUMBER(").append("TMPFILLERCOL").append(i).append(")");
				break;
			default:
				// Force columns to be quoted:
				sb.append(databaseMeta.getStartQuote() + fields.getValueMeta(i).getName() + databaseMeta.getEndQuote());
				break;
			}
		}
		sb.append(")");

		sb.append(" FROM STDIN NATIVE ");

		if (!Const.isEmpty(meta.getExceptionsFileName())) {
			sb.append("EXCEPTIONS E'").append(meta.getExceptionsFileName().replace("'", "\\'")).append("' ");
		}

		if (!Const.isEmpty(meta.getRejectedDataFileName())) {
			sb.append("REJECTED DATA E'").append(meta.getRejectedDataFileName().replace("'", "\\'")).append("' ");
		}

		//TODO: Should eventually get a preference for this, but for now, be backward compatible.
		sb.append("ENFORCELENGTH ");
		
		if (meta.isAbortOnError()) {
			sb.append("ABORT ON ERROR ");
		}

		if (meta.isDirect()) {
			sb.append("DIRECT ");
		}

		if (!Const.isEmpty(meta.getStreamName())) {
			sb.append("STREAM NAME E'").append(environmentSubstitute(meta.getStreamName()).replace("'", "\\'")).append("' ");
		}

		// XXX: I believe the right thing to do here is always use NO COMMIT since we want Kettle's configuration to drive.
		// NO COMMIT does not seem to work even when the transformation setting 'make the transformation database transactional' is on
		// sb.append("NO COMMIT");
		
		logDebug("copy stmt: "+sb.toString());
		
		return sb.toString();
	}

	private Object[] writeToOutputStream(Object[] r) throws KettleException, IOException
	{
		assert(r!=null);
    
		Object[] insertRowData = r; 
		Object[] outputRowData = r;

		if ( meta.specifyFields() )  {
			insertRowData = new Object[data.selectedRowFieldIndices.length];
			for (int idx=0;idx<data.selectedRowFieldIndices.length;idx++)
			{
				insertRowData[idx] = r[ data.selectedRowFieldIndices[idx] ];
			}           
		}

		try
		{
			data.encoder.writeRow(data.insertRowMeta, insertRowData);
		}
		catch (IOException e)
		{
			if (!data.isStopped())
				throw new KettleException("I/O Error during row write.", e);
		}


		return outputRowData;
	}

  @Override
	public boolean init(StepMetaInterface smi, StepDataInterface sdi)
	{
		meta=(VerticaBulkLoaderMeta)smi;
		data=(VerticaBulkLoaderData)sdi;

		if (super.init(smi, sdi))
		{
			try
			{
				data.databaseMeta = meta.getDatabaseMeta();

				data.db=new Database(this, meta.getDatabaseMeta());
				data.db.shareVariablesWith(this);

				if (getTransMeta().isUsingUniqueConnections())
				{
					synchronized (getTrans()) { data.db.connect(getTrans().getThreadName(), getPartitionID()); }
				}
				else
				{
					data.db.connect(getPartitionID());
				}

				if(log.isBasic()) logBasic("Connected to database ["+meta.getDatabaseMeta()+"]");

				data.db.setAutoCommit(false);

				return true;
			}
			catch(KettleException e)
			{
				logError("An error occurred intialising this step: "+e.getMessage());
				stopAll();
				setErrors(1);
			}
		}
		return false;
	}

	@Override
	public void stopRunning(StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface)
	throws KettleException
	{
		setStopped(true);
		if (data.workerThread != null)
		{
			synchronized (data.workerThread)
			{
				if (data.workerThread.isAlive() && !data.workerThread.isInterrupted())
				{
					try
					{
						data.workerThread.interrupt();
						data.workerThread.join();
					}
					catch (InterruptedException e)
					{
					}
				}
			}
		}
		
		super.stopRunning(stepMetaInterface, stepDataInterface);
	}

  @Override
	public void dispose(StepMetaInterface smi, StepDataInterface sdi)
	{
		meta=(VerticaBulkLoaderMeta)smi;
		data=(VerticaBulkLoaderData)sdi;

		// allow data to be garbage collected immediately:
		data.colSpecs = null;
		data.encoder = null;
		
		setOutputDone();

		try
		{
			if (getErrors()>0)
			{
				data.db.rollback();
			} 
		}
		catch(KettleDatabaseException e)
		{
			logError("Unexpected error rolling back the database connection.", e);
		}
                
          
		if (data.workerThread != null)
		{
			try
			{
				data.workerThread.join();
			}
			catch (InterruptedException e)
			{
			}
		}

		if (data.db!=null) {
			data.db.disconnect();
		}
		super.dispose(smi, sdi);
	}
}
