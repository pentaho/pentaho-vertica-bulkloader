This is a plugin for performing bulk data loads into a Vertica Analytic Database using a COPY FROM STDIN statement through JDBC

TODO/LIMITATIONS:

No data escaping - If there is a \. the load will silently be truncated.
  If a field contains the delimiter or record terminator, that record will be rejected.
  The step should probably be enhanced with a few more options to enable checking/escaping.
    
Efficiency of InputStream implementation - I'm currently using a Piped Input/Output Stream pair with a worker
  thread to hand the data off to the JDBC driver.  I will evaluate whether it would be better
  to instead create a custom InputStream implementation that can be fed directly. (I'm guessing "yes!") 
  
Speed - On a dual quad xeon with a GB connection to the cluster, I'm averaging
  about 40k to 50k records per second for a single load stream.  I think this could improve.
  
Error row handling - The step doesn't have any way to be notified of rejected records if it is not set to abort.
  If it could receive notification of rejected records, we could even enable error handling on the step
  and allow the ETL developer to perform additional processing on those failed records.
  
Statement canceling - If the transformation aborts, the step receives an exception from the JDBC driver when
  attempting to cancel the statement.  It currently has to use stack trace parsing to determine if it should
  report this error or not, and Vertica logs some errors that I'd rather avoid seeing.  Need a better way
  to cancel an in-process stream.

Update to Vertica 4.0 and 5.0 - Most important is utilizing the new bulk loading features in those versions.
  
TEST PLAN:

Small/Medium/Large data loads
Different delimiter characters including ' as a special case
Different null strings and record terminator strings including ones with the character ' as a special case
Abort vs non-abort
Direct vs non-direct
Specifying different types of logging filenames
Multiple load step copies  on one machine
Clustered load streams
Clustered load streams with multiple step copies per machine
Special character encodings?
