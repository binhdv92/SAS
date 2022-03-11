%macro deletedsifexists(lib,name);
    %if %sysfunc(exist(&lib..&name.)) %then %do;
		%PUT deleting &lib..&name.;
		proc datasets library=&lib. nolist;
	        delete &name.;
	    quit;
	%end;
%mend;

%Macro CheckRetrainingState(); /*This code will run until the retraining request has completed*/
%do %while (&state=running);

	%PUT Wait 30 Seconds;
	DATA _null_;
		rc=SLEEP(30,1);
	RUN;

	/* The DMBatchRetrainCurrentJob macro can be optionally run to get the status of
	 * the current batch retrain job. This macro might need to be called several times
	 * before the job completes and the final status is known. */
	%DMBatchRetrainCurrentJob(&projectId);
%end;
%MEND;

%MACRO me_int_iscmsmanrertrainspul(SPUL=);
%_timer(name=timer1, state=start);

data work.ModelStudioProjects; set isclib.ModelStudioProjects; if SPUL =: "&SPUL." ; run;
PROC SQL NOPRINT; SELECT SPUL, ProjID INTO :SPUL, :projectId FROM work.ModelStudioProjects ; QUIT;

%LET modelid=;
%dmgetchampion(&projectId);
%PUT ProjectID = &projectId;
%PUT &modelid;

%LET DebugFlag=False;
%LET datasourceUri = /dataTables/dataSources/cas~fs~cas-shared-adhoc~fs~Public/tables/ME_INT_ISCAD2_&SPUL._ALL3;
%LET outputCasLib = Public;
%LET outputTableName = ME_INT_ISCAD2_&SPUL.Scored;


/*Prep the Data for Retraining*/
%_timer(name=timer2, state=start);
%me_int_iscmodelstudiodataprep(SPUL=&SPUL,DebugFlag=&DebugFlag);
%_timer(name=timer2);

PROC SQL;
select "&SPUL." AS SPUL, Partition1Tr2Va3Te, count(*) as N from public.me_int_iscad2_&SPUL._all3 group by Partition1Tr2Va3Te;
quit;

%_timer(name=timer3, state=start);
/* The DMBatchRetrain macro is used for retraining a project with a new data table.
 * It takes auth token, datasourceUri. datasourceUri is
 * used to specify  the data table to use for retraining.*/
%DMBatchRetrain(&projectId, &datasourceUri);
%_timer(name=timer3);


%_timer(name=timer4, state=start);
%CheckRetrainingState();
%_timer(name=timer4);

/* Return Retrained Model Champion */
%dmgetchampion(&projectId);

%PUT &modelId;

%MEND;
