/************************************************************************************/
/*PURPOSE:																		 	*/
/* This auto call macro accepts a MfgUnit {PGT11, PGT21, ... KMT22} and a debug  	*/
/* flag that applies to data prep.  With these two parameters, the program will: 	*/
/*   1. Create a score dataset 													 	*/
/*   2. API Call ModelStudio to Retrain the Model Champion		 				 	*/
/*   3. Monitor for completion of models										 	*/
/*   4. TBD - Alerting for Modeling gone wrong									 	*/
/************************************************************************************/

/************************************************************************************/
/*DEPENDENCIES:																	 	*/
/*   1. Autocall Macros: 	 														*/
/* 		a. _timer 																	*/
/* 		b. me_int_iscmodelstudiodataprep 											*/
/* 		c. DMBatchRetrain 															*/
/* 		d. DMBatchRetrainCurrentJob													*/
/*		e. me_int_iscmodelsimcontrol							 					*/
/*   2. Tables: 																	*/
/* 		a. me_int_iscad2 loaded in cas-shared-adhoc 			 					*/
/************************************************************************************/

/************************************************************************************/
/* DEFINE LOCAL MACRO PROGRAMS  													*/
/************************************************************************************/
%MACRO deletedsifexists(lib,name);
    %if %sysfunc(exist(&lib..&name.)) %then %do;
		proc datasets library=&lib. nolist;
	        delete &name.;
	    quit;
	%end;
%mend;

%MACRO ModelStudioIscUnitRetrain();

/*4) get count of the number of loops needed */
PROC SQL NOPRINT; SELECT COUNT(*) INTO :nObs FROM work.ModelStudioProjects; QUIT;
%PUT &nobs;

/*5) start loop */
%_timer(name=Loop, state=start);
%do i=1 %to &nobs;

	%_timer(name=timer1, state=start);
	PROC SQL NOPRINT; SELECT SPUL, ProjID INTO :SPUL, :projectId FROM work.ModelStudioProjects WHERE rownum = &i; QUIT;
		
	%PUT Itteration: &i pulled SPUL:  &SPUL has projectId:  &projectId; 
	%LET datasourceUri = /dataTables/dataSources/cas~fs~cas-shared-adhoc~fs~Public/tables/ME_INT_ISCAD2_&SPUL._ALL3;
	%LET DebugFlag=False;

	/*Disable Model From Scoring*/	
	%me_int_iscmodelsimcontrol(spul=&SPUL, action=disable, reason=RetrainingStart);
		
	/*Prep the Data for Retraining*/
	%_timer(name=timer2, state=start);
/* 	OPTIONS MPRINT; */
	%me_int_iscmodelstudiodataprep(SPUL=&SPUL,DebugFlag=&DebugFlag,Purpose=All3);
/* 	OPTIONS NOMPRINT; */
	%_timer(name=timer2);
	
	%_timer(name=timer3, state=start);
	/* The DMBatchRetrain macro is used for retraining a project with a new data table.
	 * It takes auth token, datasourceUri. datasourceUri is
	 * used to specify  the data table to use for retraining.*/
	%DMBatchRetrain(&projectId, &datasourceUri);
	/* Tom to capture JobID for future timing (after SASTechSupport ticket completed) */
	%_timer(name=timer3);
	%_timer(name=timer1);

%end;
%_timer(name=Loop);

%MEND;

%MACRO CheckRetrainingState(); /*This code will run until the retraining request has completed*/

/*1) get count of the number of loops needed */
PROC SQL NOPRINT; SELECT COUNT(*) INTO :nObs FROM work.ModelStudioProjects; QUIT;
%PUT &nobs;

/*2) start loop:  Check the state of the jobs running in order in which they were created */
%_timer(name=RetrainLoopCheck, state=start);
%do i=1 %to &nobs;

	%_timer(name=timer1, state=start);

	PROC SQL NOPRINT; SELECT SPUL, ProjID, Active INTO :SPUL, :projectId, :tblActive FROM work.ModelStudioProjects WHERE rownum = &i; QUIT;
	%LET state=running;
	%LET fmtActive=;
		
	%PUT Itteration: &i pulled SPUL:  &SPUL has projectId:  &projectId which tblActive= &tblActive; 

	%do %while (&state=running);
	
		%PUT &SPUL Still Running. Wait 30 Seconds;
		DATA _null_;
			rc=SLEEP(30,1);
		RUN;

		/* The DMBatchRetrainCurrentJob macro can be optionally run to get the status of
		 * the current batch retrain job. This macro might need to be called several times
		 * before the job completes and the final status is known. */
		%DMBatchRetrainCurrentJob(&projectId);
	%end;

/* 	If zsvc_sas_temp_reader running get original state for model to reapply when retraining is done	 */
	%IF %UPCASE(&sysuserid) = ZSVC_SAS_TEMP_READER %THEN %DO;
		%PUT Restoring model active bit to state prior to retraining;
		
		%IF &tblActive = 1 %THEN %DO; 
			%PUT Re-Enable Model;
			%LET fmtActive =Enable; 
		%END; 				
		%ELSE %DO; 	
			%PUT Leave Disabled;
			%LET fmtActive =Disable; 
		%END;
	%END;
	%ELSE %DO; 
		/*User Retraining, enabling model*/
		%LET fmtActive =Enable; 
	%END;
	%PUT fmtActive = &fmtActive;
	/*Enable Model After Training*/	
	%me_int_iscmodelsimcontrol(spul=&SPUL, action=&fmtActive, reason=RetrainingDone);

%end;
%_timer(name=RetrainLoopCheck);

%MEND;

%MACRO me_int_iscmodelsimmsunitretrain(Unit,DebugFlag=False);
/*Create row numbers in table hosting SPUL to MSProjectIds*/
data work.ModelStudioProjects; set isclib.ModelStudioProjects; if SPUL =: "&Unit."; run;
data work.ModelStudioProjects; set work.ModelStudioProjects; rownum=_n_; run;

/* Call for Model to be retrained */
%ModelStudioIscUnitRetrain();


/* Check state of retraining */
%_timer(name=timer4, state=start);
%CheckRetrainingState();
%_timer(name=timer4);

%MEND;



/*Created a table which maps SPUL to ModelStudio ProjectID (Bill Herald provided the data)*/

/* libname IscLib '/sasdata/Projects/S6_IscModeling'; */
/*  */
/* PROC SQL;  */
/* DROP TABLE IscLib.ModelStudioProjects; */
/* CREATE TABLE IscLib.ModelStudioProjects (SPUL CHAR(6), ProjID CHAR(36), Active NUM(1)); */
/* QUIT; */
/*  */
/* PROC SQL; */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('KMT11A','f2f33d42-3df7-4010-bd0c-8f46b6b569bc',1); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('KMT11B','651d143c-36da-4818-b121-20213cb45a94',1); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('DMT11A','01f46802-3511-48af-9fc2-190f6be046de',1); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('DMT11B','7efd7c18-d89c-4e2b-94fb-d1b0fd4a4a5f',1); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('PGT11A','39679077-1f6a-44b4-8a8d-9e0ce9dfa8a5',1); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('PGT11B','fcbf694e-d747-41e2-a673-8be855dfc53f',1); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('PGT11C','124f3273-c4a9-4a86-a865-7364e7522325',1); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('PGT21A','0d043730-dcaa-4a63-99d0-2f3d77d26a6a',1); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('PGT21B','97cacb50-7df0-439a-beb7-17dd991ae7de',1); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('PGT21C','f5a4dab0-a4a9-426a-83c7-468bb2f3dffc',1); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('PGT21D','e42710e0-62c9-445d-b1be-7e70a909a87e',1); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('PGT22A','2746952a-9756-4695-932e-109a62491f26',1); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('PGT22B','343973e0-6c5d-4033-9534-69778f5c7082',1); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('DMT12A','39ab4034-9f26-4a11-9fbb-1283937e74aa',1); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('DMT12B','2e1a84c7-e80e-4bc4-a71d-204a01ceef32',1); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('DMT21A','1881619e-0354-4394-b948-6275cad61f99',1); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('DMT21B','a72c2488-97e3-4cd7-b1fc-9834d468c1ce',1); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('DMT22A','4a302414-c7a1-495c-984a-82af20e04f55',1); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('DMT22B','ba7b3450-9e30-4663-8f24-d5ee3b68f3be',1); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('KMT12A','fba0886a-4af5-4649-8faa-15bdefd0d7ac',1); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('KMT12B','c316a5ab-b170-4d5a-b77f-1a8298a0b96f',1); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('KMT21A','3021ffb1-3c26-4208-904a-568d34b47812',0); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('KMT21B','996b80af-cb2d-4079-99dd-46d3ff5bacee',0); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('KMT21C','ed018077-3e33-4431-9c37-2530ec43f578',0); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('KMT21D','1c2ac7af-77b5-436f-b956-85db0a548735',0); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('KMT22A','58d95bb6-0c13-425b-9275-06339fcec2d0',0); */
/* INSERT INTO IscLib.ModelStudioProjects VALUES ('KMT22B','879fb8f6-1663-4498-ba7b-c55348d17417',0); */
/* QUIT; */
