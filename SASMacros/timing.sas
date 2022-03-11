
%MACRO timing(action=,Process=,Category=,Task=,TaskNum=);
/*Actions:
	1 - Initialization
	2 - Inserts
	3 - Report
*/
%IF &action. = 1 %THEN %DO;
	PROC SQL NOPRINT; /*Set up table to record parallel ETL timing*/
		CREATE TABLE workdir.Timing (Process char(24), ProcessNum num, Category char(16), Task char(24), TaskNum num, Timestamp num format=datetime.);
		INSERT INTO  workdir.Timing values('Conductor', 0, 'MainProgram', 'Initialization', 0, %sysfunc(datetime()));
		CREATE VIEW  workdir.vwTiming AS
			SELECT *
			FROM
				(
				SELECT 
					a.Category, a.Process, a.Task, a.TaskNum, a.Timestamp, b1.MinProcessNum
					,case 
						when a.Category = 'MainProgram' THEN 0
						when a.Category = 'ETL' THEN 1
						when a.Category = 'daysAgo' THEN 2 
						ELSE 999 END as sortOrder
					,a.timestamp - b.timestamp as ElapsedDuration
					,d.timestamp - c.timestamp as StepElapsedDuration
					,a.timestamp - d.timestamp as TaskDuration
				FROM 
					workdir.Timing a
					CROSS JOIN (select timestamp from workdir.Timing where task = 'Initialization' and Category = 'MainProgram') b
					LEFT JOIN (select min(a.ProcessNum) as MinProcessNum, a.Category, a.process from workdir.Timing a GROUP BY a.Category, a.process) b1 on a.Category = b1.Category AND a.process = b1.process
					LEFT JOIN workdir.Timing c on a.Process = c.Process and a.Category = c.Category and c.task = 'Initialization'
					LEFT JOIN workdir.Timing d on a.Process = d.Process and a.Category = d.Category and a.TaskNum = (d.TaskNum+1)
				) a
			ORDER BY sortOrder, MinProcessNum, a.TaskNum, timestamp;
	QUIT;
%END;
%IF &action. = 2 %THEN %DO;
	%trylock(member=workdir.Timing);
	PROC SQL NOPRINT; 
	SELECT CASE WHEN TaskNum IS NULL THEN 0 ELSE MAX(TaskNum) + 1 END INTO :LocalTN FROM workdir.timing WHERE Process=&Process. AND Category = &Category.; 
	SELECT CASE WHEN ProcessNum IS NULL THEN 0 ELSE MAX(ProcessNum)+1 END INTO :LocalPN FROM workdir.timing WHERE  Category = &Category.; 
	QUIT;
	PROC SQL NOPRINT;
	INSERT INTO  workdir.Timing values(&Process., &LocalPN., &Category., &Task., &LocalTN., %sysfunc(datetime()));
	QUIT;
	LOCK workdir.Timing CLEAR;
%END;
%IF &action. = 3 %THEN %DO;
	%PUT --------------------------------------------- Conductor Timing Report --------------------------------------------;
	%PUT | Category |Process               |Task                    |Timestamp          | ElapsedDuration  | TaskDuration;
	DATA _NULL_;
		SET workdir.vwTiming;
		PUT '| ' Category  @12'|' Process  @35'|' Task   @60'|' Timestamp datetime. @80'|' ElapsedDuration 6.2 @99'|' TaskDuration 6.2;
	RUN;
	%PUT --------------------------------------------- Conductor Timing Report --------------------------------------------;
%END;

%MEND;




