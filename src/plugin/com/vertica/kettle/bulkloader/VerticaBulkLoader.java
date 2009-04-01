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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.sql.SQLException;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import com.vertica.PGStatement;

public class VerticaBulkLoader extends BaseStep implements StepInterface
{
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
            try
            {
                data.writer.flush();
                data.writer.close();
            }
            catch (IOException e)
            {
                throw new KettleException("I/O Error during statement termination.", e);
            }

            return false;
        }

        if (first)
        {
            first=false;

            data.pipedOutputStream = new PipedOutputStream();
            data.writer = new BufferedWriter(new OutputStreamWriter(data.pipedOutputStream));

            data.outputRowMeta = getInputRowMeta().clone();
            meta.getFields(data.outputRowMeta, getStepname(), null, null, this);

            if ( ! meta.specifyFields() )  {
                // Just take the input row
                data.insertRowMeta = getInputRowMeta().clone();
            }
            else  {

                data.insertRowMeta = new RowMeta();

                // 
                // Cache the position of the compare fields in Row row
                //
                data.valuenrs = new int[meta.getFieldDatabase().length];
                for (int i=0;i<meta.getFieldDatabase().length;i++)
                {
                    data.valuenrs[i]=getInputRowMeta().indexOfValue(meta.getFieldStream()[i]);
                    if (data.valuenrs[i]<0)
                    {
                        throw new KettleStepException(Messages.getString("VerticaBulkLoader.Exception.FieldRequired",meta.getFieldStream()[i])); //$NON-NLS-1$
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
                        throw new KettleStepException(Messages.getString("VerticaBulkLoader.Exception.FailedToFindField", meta.getFieldStream()[i])); //$NON-NLS-1$ 
                    }
                }            	
            }

            executeCopyStatement();
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

    private void executeCopyStatement() throws KettleException
    {
        try
        {
            final String parentThreadName = toString();
            final String dml = buildCopyStatementSqlString();
            final PipedInputStream sink = new PipedInputStream(data.pipedOutputStream);
            final PGStatement statement = (PGStatement) data.db.getConnection().createStatement();

            data.workerThread = new Thread(new Runnable() {
                public void run() {
                    try
                    {
                        statement.executeCopyIn(dml, sink);
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
            }, parentThreadName+".[worker]");

            data.workerThread.start();
        }
        catch (IOException e)
        {
            throw new KettleException("I/O Error during statement initialization.", e);
        }
        catch (SQLException e)
        {
            throw new KettleException("SQL Error during statement initialization.", e);

        }
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

        sb.append(" FROM STDIN ");

        if (!Const.isEmpty(meta.getDelimiter())) {
            sb.append("DELIMITER '").append(meta.getDelimiter().replace("'", "\\'")).append("' ");
        }

        if (!Const.isEmpty(meta.getNullString())) {
            sb.append("NULL '").append(meta.getNullString().replace("'", "\\'")).append("' ");
        }

        if (!Const.isEmpty(meta.getRecordTerminator())) {
            sb.append("RECORD TERMINATOR '").append(meta.getRecordTerminator().replace("'", "\\'")).append("' ");
        }

        if (!Const.isEmpty(meta.getExceptionsFileName())) {
            sb.append("EXCEPTIONS '").append(meta.getExceptionsFileName().replace("'", "\\'")).append("' ");
        }

        if (!Const.isEmpty(meta.getRejectedDataFileName())) {
            sb.append("REJECTED DATA '").append(meta.getRejectedDataFileName().replace("'", "\\'")).append("' ");
        }

        if (meta.isAbortOnError()) {
            sb.append("ABORT ON ERROR ");
        }

        if (meta.isDirect()) {
            sb.append("DIRECT ");
        }

        if (!Const.isEmpty(meta.getStreamName())) {
            sb.append("STREAM NAME '").append(meta.getStreamName().replace("'", "\\'")).append("' ");
        }


        return sb.toString();
    }

    private Object[] writeToOutputStream(RowMetaInterface rowMeta, Object[] r) throws KettleException
    {
        assert(r!=null);

        Object[] insertRowData = r; 
        Object[] outputRowData = r;

        if ( meta.specifyFields() )  {
            insertRowData = new Object[data.valuenrs.length];
            for (int idx=0;idx<data.valuenrs.length;idx++)
            {
                insertRowData[idx] = r[ data.valuenrs[idx] ];
            }           
        }

        try
        {
            for (int i = 0; i < insertRowData.length; i++)
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

                data.db=new Database(meta.getDatabaseMeta());
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

                data.delimiter = Const.isEmpty(meta.getDelimiter()) ? "\t" : meta.getDelimiter().replace("'", "\\'");
                data.nullString = Const.isEmpty(meta.getNullString()) ? "\\N" : meta.getNullString().replace("'", "\\'");
                data.recordTerminator = Const.isEmpty(meta.getRecordTerminator()) ? "\n" : meta.getRecordTerminator().replace("'", "\\'");

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

        try
        {
            data.workerThread.join();
        }
        catch (InterruptedException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        if (data.db!=null) {
            data.db.disconnect();
        }
        super.dispose(smi, sdi);
    }

    //
    // Run is were the action happens!
    public void run()
    {
        BaseStep.runStepThread(this, meta, data);
    }
}