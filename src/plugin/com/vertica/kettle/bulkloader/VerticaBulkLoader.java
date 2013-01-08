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

public class VerticaBulkLoader extends BaseStep implements StepInterface
{
	private static Class<?> PKG = VerticaBulkLoader.class; // for i18n purposes, needed by Translator2!!

	private VerticaBulkLoaderMeta meta;
	private VerticaBulkLoaderData data;

	public VerticaBulkLoader(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans)
	{
		super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
	}

	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException
	{
		meta=(VerticaBulkLoaderMeta)smi;
		data=(VerticaBulkLoaderData)sdi;

		Object[] r=getRow();    // this also waits for a previous step to be finished.
		if (r==null)  // no more input to be expected...
		{
			data.close();
			return false;
		}

		if (first)
		{
			first=false;

			data.outputRowMeta = getInputRowMeta().clone();
			meta.getFields(data.outputRowMeta, getStepname(), null, null, this);

			if ( ! meta.specifyFields() )  {
				// Just take the whole input row
				data.insertRowMeta = getInputRowMeta().clone();
				data.selectedRowFieldIndices = new int[data.insertRowMeta.size()];
			}
			else  {

				data.insertRowMeta = new RowMeta();

				// Cache the position of the selected fields in the row array
				data.selectedRowFieldIndices = new int[meta.getFieldDatabase().length];
				for (int i=0;i<meta.getFieldDatabase().length;i++)
				{
					data.selectedRowFieldIndices[i]=getInputRowMeta().indexOfValue(meta.getFieldStream()[i]);
					if (data.selectedRowFieldIndices[i]<0)
					{
						throw new KettleStepException(BaseMessages.getString(PKG, "VerticaBulkLoader.Exception.FieldRequired",meta.getFieldStream()[i])); //$NON-NLS-1$
					}
				}

				for (int i=0;i<meta.getFieldDatabase().length;i++) 
				{
					ValueMetaInterface insValue = getInputRowMeta().searchValueMeta( meta.getFieldStream()[i]); 
					if ( insValue != null )
					{
						ValueMetaInterface insertValue = insValue.clone();
						insertValue.setName(meta.getFieldDatabase()[i]);
						data.insertRowMeta.addValueMeta( insertValue );
					}
					else  {
						throw new KettleStepException(BaseMessages.getString(PKG, "VerticaBulkLoader.Exception.FailedToFindField", meta.getFieldStream()[i])); //$NON-NLS-1$ 
					}
				}            	
			}

			initializeWorker();
			data.writeHeader();
		}

		try
		{
			Object[] outputRowData = writeToOutputStream(getInputRowMeta(), r);
			if (outputRowData!=null)
			{
				putRow(data.outputRowMeta, outputRowData); // in case we want it go further...
				incrementLinesOutput();
			}

			if (checkFeedback(getLinesRead())) 
			{
				if(log.isBasic()) logBasic("linenr "+getLinesRead()); //$NON-NLS-1$
			}
		}
		catch(KettleException e)
		{
			logError("Because of an error, this step can't continue: ", e);
			setErrors(1);
			stopAll();
			setOutputDone();  // signal end to receiver(s)
			return false;
		}		

		return true;
	}

	private void initializeWorker()
	{
		final String dml = buildCopyStatementSqlString();

		data.workerThread = Executors.defaultThreadFactory().newThread(new Runnable() {
			public void run() {
				try
				{
					PipedInputStream sink = new PipedInputStream();
					data.connectInputStream(sink);
					VerticaCopyStream stream = new VerticaCopyStream((VerticaConnection)(data.db.getConnection()), dml);
					stream.start();
					stream.addStream(sink);
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
				} catch (IOException e) {
					logError("Error connecting InputStream during initialization.", e);
					setErrors(1);
					stopAll();
					setOutputDone();  // signal end to receiver(s)
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

		if (meta.specifyFields())  {
			final RowMetaInterface fields = data.insertRowMeta;
			sb.append(" (");
			for (int i = 0; i < fields.size(); i++)
			{
				if (i > 0) sb.append(", ");

				sb.append(databaseMeta.quoteField(fields.getValueMeta(i).getName()));
			}
			sb.append(")");
		}

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

		//XXX: I believe the right thing to do here is always use NO COMMIT since we want Kettle's configuration to drive.
		sb.append("NO COMMIT");

		return sb.toString();
	}

	private Object[] writeToOutputStream(RowMetaInterface rowMeta, Object[] r) throws KettleException
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
			for (int i = 0; i < data.selectedRowFieldIndices.length; i++)
			{
				if (i > 0) data.writer.write(data.delimiter);

				if (insertRowData[i] == null) {
					data.writer.write(data.nullString);
				} else {
					data.writer.write(data.insertRowMeta.getString(insertRowData, i));
				}
			}

			data.writer.write(data.recordTerminator);
		}
		catch (IOException e)
		{
			if (!data.isStopped())
				throw new KettleException("I/O Error during row write.", e);
		}


		return outputRowData;
	}

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

				if (Const.isEmpty(meta.getDelimiter())) {
					data.delimiter = "|";
				} else {
					String s = meta.getDelimiter();
					s = s.replace("\\0", "\0");
					s = s.replace("\\a", "\u0007");
					s = s.replace("\\b", "\b");
					s = s.replace("\\t", "\t");
					s = s.replace("\\n", "\n");
					s = s.replace("\\v", "\u0011");
					s = s.replace("\\f", "\f");
					s = s.replace("\\r", "\r");
					data.delimiter = s.replace("'", "\\'");
				}

				if (Const.isEmpty(meta.getNullString())) {
					data.nullString = "";
				} else {
					String s = meta.getNullString();
					s = s.replace("\\0", "\0");
					s = s.replace("\\a", "\u0007");
					s = s.replace("\\b", "\b");
					s = s.replace("\\t", "\t");
					s = s.replace("\\n", "\n");
					s = s.replace("\\v", "\u0011");
					s = s.replace("\\f", "\f");
					s = s.replace("\\r", "\r");
					data.nullString = s.replace("'", "\\'");
				}

				if (Const.isEmpty(meta.getRecordTerminator())) {
					data.recordTerminator = "\n";
				} else {
					String s = meta.getRecordTerminator();
					s = s.replace("\\0", "\0");
					s = s.replace("\\a", "\u0007");
					s = s.replace("\\b", "\b");
					s = s.replace("\\t", "\t");
					s = s.replace("\\n", "\n");
					s = s.replace("\\v", "\u0011");
					s = s.replace("\\f", "\f");
					s = s.replace("\\r", "\r");
					data.recordTerminator = s.replace("'", "\\'");
				}
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
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
		super.stopRunning(stepMetaInterface, stepDataInterface);
	}

	public void dispose(StepMetaInterface smi, StepDataInterface sdi)
	{
		meta=(VerticaBulkLoaderMeta)smi;
		data=(VerticaBulkLoaderData)sdi;

		setOutputDone();

		if (getErrors()>0)
		{
			try
			{
				data.db.rollback();
			}
			catch(KettleDatabaseException e)
			{
				logError("Unexpected error rolling back the database connection.", e);
			}
		}

		if (data.workerThread != null)
		{
			try
			{
				data.workerThread.join();
			}
			catch (InterruptedException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		if (data.db!=null) {
			data.db.disconnect();
		}
		super.dispose(smi, sdi);
	}
}