/************************************************************************************/
/*PURPOSE:																		 	*/
/* This auto call macro accepts a MfgUnit {PGT11, PGT21, ... KMT22} and a debug  	*/
/* flag that applies to data prep.  With these two parameters, the program will: 	*/
/*   1. Create a score dataset 													 	*/
/*   2. Score the data with a champion model from Model Studio 					 	*/
/*   3. Calculate Isc Offsets for MES 											 	*/
/*   4. Send values to MES 														 	*/
/************************************************************************************/

/************************************************************************************/
/*DEPENDENCIES:																	 	*/
/*   1. Autocall Macros: 	 														*/
/* 		a. _timer 																	*/
/* 		b. me_int_iscmodelstudiodataprep 											*/
/* 		c. dmgetchampion 															*/
/* 		d. DMScoreModel												 				*/
/*   2. Tables: 																	*/
/* 		a. me_int_iscad2 loaded in cas-shared-adhoc 				 				*/
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

%MACRO ScoreDataModelChamp(); /*This code will identify Model Champion and score input data*/

/*1) get count of the number of loops needed */
PROC SQL NOPRINT; SELECT COUNT(*) INTO :nObs FROM work.ModelStudioProjects; QUIT;
%PUT &nobs;

%LET outputCasLib = Public;

/*2) start loop:  Check the state of the jobs running in order in which they were created */
%_timer(name=ScoreDataModelChampLoop, state=start);
%do iMS=1 %to &nobs;

	%_timer(name=timer1, state=start);

	PROC SQL NOPRINT; SELECT SPUL, ProjID INTO :SPUL, :projectId FROM work.ModelStudioProjects WHERE rownum = &iMS; QUIT;
	%LET outputTableName = ME_INT_ISCAD2_&SPUL.Scored;
	
	%me_int_iscmodelstudiodataprep(SPUL=&SPUL,DebugFlag=&DebugFlag,Purpose=Score);
	
	%LET datasourceUri = /dataTables/dataSources/cas~fs~cas-shared-adhoc~fs~Public/tables/ME_INT_ISCAD2_&SPUL._Score;
		
	%PUT Itteration: &iMS pulled SPUL:  &SPUL has projectId:  &projectId; 
	/* Return Retrained Model Champion */
	%dmgetchampion(&projectId);

	/* Add ModelID to ModelStudioProjects Table*/
	PROC SQL; UPDATE work.ModelStudioProjects SET ModelId = STRIP("&ModelId.") WHERE rownum = &iMS; QUIT;
	
/* 	%PUT &SPUL:  &modelId; */
	
	/*****************************************************************************
	** Macro: Score Data Mining Model
	**
	** Description: Score the specified model using the score execution service
	******************************************************************************/
	%deletedsifexists(PUBLIC,&outputTableName);

	%let NOTES = %sysfunc(getoption(notes));
	OPTIONS NONOTES;
	
	%_timer(name=timer10, state=start);
	%DMScoreModel(&projectId, &modelId, &datasourceUri, &outputCasLib, &outputTableName);
	%_timer(name=timer10);	

	options &NOTES;

	/* CODE FOR APPLICATION OF Scored Data */
	%IF &iMS = 1 %THEN %DO;
		%deletedsifexists(work,ME_INT_ISCAD2_SCORED);

		PROC SQL;
		CREATE TABLE WORK.ME_INT_ISCAD2_SCORED AS
		SELECT SimUnitLine AS Sim, SUBSTR(SimUnitLine,1,4) AS SimUnit, sub_id, IVSweepReadTime, Rccc, P_Isc, Isc, Isc-P_Isc as Residual, Partition1Tr2Va3Te FROM PUBLIC.ME_INT_ISCAD2_&SPUL.SCORED WHERE P_Isc > 2  AND P_Isc < 3;
		QUIT;
	%END;
	%ELSE %DO;
		%IF %sysfunc(exist(WORK.ME_INT_ISCAD2_SCORED)) %THEN %DO;
			PROC SQL;
			INSERT INTO WORK.ME_INT_ISCAD2_SCORED 
			SELECT SimUnitLine AS Sim, SUBSTR(SimUnitLine,1,4) AS SimUnit, sub_id, IVSweepReadTime, Rccc, P_Isc, Isc, Isc-P_Isc as Residual, Partition1Tr2Va3Te FROM PUBLIC.ME_INT_ISCAD2_&SPUL.SCORED WHERE P_Isc > 2  AND P_Isc < 3;
			QUIT;
		%END;
		%ELSE %DO;
			PROC SQL;
			CREATE TABLE WORK.ME_INT_ISCAD2_SCORED AS
			SELECT SimUnitLine AS Sim, SUBSTR(SimUnitLine,1,4) AS SimUnit, sub_id, IVSweepReadTime, Rccc, P_Isc, Isc, Isc-P_Isc as Residual, Partition1Tr2Va3Te FROM PUBLIC.ME_INT_ISCAD2_&SPUL.SCORED WHERE P_Isc > 2  AND P_Isc < 3;
			QUIT;
		%END;
	%END;

	
	%deletedsifexists(lib=PUBLIC, name=ME_INT_ISCAD2_&SPUL._SCORE); /*Clean up the input dataset*/
	%deletedsifexists(lib=PUBLIC, name=ME_INT_ISCAD2_&SPUL.SCORED); /*Clean up the output dataset*/

	
	proc datasets lib=work nolist;
	 save ModelStudioProjects ME_INT_ISCAD2_SCORED;
	quit;

%end;
%_timer(name=ScoreDataModelChampLoop);

%MEND;

%MACRO InactiveInsert();

/*1) get count of the number of loops needed */
PROC SQL NOPRINT; SELECT COUNT(*) INTO :nObs FROM work.ModelStudioProjects; QUIT;
%PUT &nobs;

/*2) start loop:  insert 0's for lineages where model is inactive */
%_timer(name=ScoreDataModelChampLoop, state=start);
%do iMS=1 %to &nobs;

	%_timer(name=timer1, state=start);

	PROC SQL NOPRINT; SELECT SPUL, ProjID INTO :SPUL, :projectId FROM work.ModelStudioProjects WHERE rownum = &iMS; QUIT;

	%dmgetchampion(&projectId);

	/* Add ModelID to ModelStudioProjects Table*/
	PROC SQL; UPDATE work.ModelStudioProjects SET ModelId = STRIP("&ModelId.") WHERE rownum = &iMS; QUIT;
	
	%LET SimUnit = %SYSFUNC(SUBSTR(&SPUL,1,5));
	%LET sModelId = %SYSFUNC(STRIP(&ModelId.));

	%IF %SYSFUNC(SUBSTR(&SPUL,1,3)) = PGT %THEN %DO; %LET DTS=&PGTTime; %END;
	%IF %SYSFUNC(SUBSTR(&SPUL,1,3)) = KMT %THEN %DO; %LET DTS=&KMTTime; %END;
	%IF %SYSFUNC(SUBSTR(&SPUL,1,3)) = DMT %THEN %DO; %LET DTS=&DMTTime; %END;

	PROC SQL;/*ME_INT_IscModelOffsetSim*/
		INSERT INTO work.ME_INT_IscModelOffsetSimMS VALUES 
		("&SPUL.","&SPUL.",&DTS ,&UDTTime,"&SimUnit.",0,0,0,0,0,0,0,'ModelDisabled',0,0,0,"&projectId.","&sModelId.")
		;
	QUIT;


%END;
%MEND;

%MACRO SendToMes();

%let _SendToMes_timer_start = %sysfunc(datetime());

proc format; /*create format necessary to pass datetime to MES*/
picture dtpic
other='%Y-%0m-%0d %0H:%0M:%0S' (datatype=datetime)
;

data SendToMes (keep=utc endutc Lineage SimOffset rccc nobs rownumber);
	set work.ME_INT_IscModelOffsetSimMS;
	EndUtc = UTC+3*3600; /*added x-hours to utc for end date purpose*/
	format EndUtc dtpic. Utc dtpic.;
	rownumber = _n_;
	Lineage = SubStr(SIM,1,6);
run;

PROC SQL NOPRINT; SELECT COUNT(*) INTO :NOffsets FROM work.SendToMes; QUIT;

%PUT &NOffsets;

%DO i=1 %TO &NOffsets;

%LET varUTC=; %LET varEndUTC=; %LET varLineage=; %LET varOffset=; %LET varRccc=; %LET varNObs=;
PROC SQL NOPRINT; 
SELECT UTC , EndUtc ,Lineage ,SimOffset ,Rccc ,nobs INTO :varUTC, :varEndUTC, :varLineage, :varOffset, :varRccc, :varNObs	
FROM work.SendToMes WHERE rownumber = &i;
QUIT;

%PUT &i : &varUTC, &varEndUTC, &varLineage, &varOffset, &varRccc, &varnobs;

PROC SQL;
   CONNECT TO ODBC as con2
    (DATASRC="mes_sasprod" authdomain=SQLGRP_MES_SAS);

   EXECUTE (
	EXECUTE [Derating].[Insert_IscCorrectionModel_V1_0] 
	   %bquote('&varUTC.')
	  ,%bquote('&varEndUTC.')
	  ,%bquote('&varLineage.')
	  ,&varOffset
	  ,&varRccc
	  ,&varnobs
	) BY con2;

   disconnect from con2;
quit;


%END;

%MEND;

%MACRO me_int_IscModelSimMSUnitScore(Unit,DebugFlag);

%GLOBAL PGTTime KMTTime DMTTime;

%LET PGT = 		%SYSFUNC(DATEPART(%SYSFUNC(DATETIME())));
%LET PGTTime =	%SYSFUNC(DATETIME());
%LET PGTHour = 	%EVAL(%SYSFUNC(FLOOR((%SYSFUNC(DATETIME()))/3600))*3600);
%LET UDT = 		%SYSFUNC(DATEPART(%SYSFUNC(tzones2u(%SYSFUNC(DATETIME())))));
%LET UDTTime = 	%SYSFUNC(tzones2u(%SYSFUNC(DATETIME())));
%LET KMT = 		%SYSFUNC(DATEPART(%SYSFUNC(tzoneu2s(%SYSFUNC(tzones2u(%SYSFUNC(DATETIME()))),Asia/Singapore))));
%LET KMTTime =	%SYSFUNC(tzoneu2s(%SYSFUNC(tzones2u(%SYSFUNC(DATETIME()))),Asia/Singapore));
%LET KMTHour = 	%EVAL(%SYSFUNC(FLOOR((%SYSFUNC(tzoneu2s(%SYSFUNC(tzones2u(%SYSFUNC(DATETIME()))),Asia/Singapore)))/3600))*3600);
%LET DMT = 		%SYSFUNC(DATEPART(%SYSFUNC(tzoneu2s(%SYSFUNC(tzones2u(%SYSFUNC(DATETIME()))),Asia/Saigon))));
%LET DMTTime =	%SYSFUNC(tzoneu2s(%SYSFUNC(tzones2u(%SYSFUNC(DATETIME()))),Asia/Saigon));
%LET DMTHour =  %EVAL(%SYSFUNC(FLOOR((%SYSFUNC(tzoneu2s(%SYSFUNC(tzones2u(%SYSFUNC(DATETIME()))),Asia/Saigon)))/3600))*3600);

proc datasets lib=work nolist; quit;

/*Create row numbers in table hosting SPUL to MSProjectIds*/
data work.ModelStudioProjects; set isclib.ModelStudioProjects; if SPUL =: "&Unit." AND Active=1; run;
data work.ModelStudioProjects; LENGTH ModelID $36.; set work.ModelStudioProjects;  rownum=_n_; run;

PROC SQL NOPRINT; SELECT COUNT(*) INTO :nActiveModels FROM work.ModelStudioProjects; QUIT;
%PUT Number of Active Models:  &nActiveModels;

%IF &nActiveModels >= 1 %THEN %DO;
	%_timer(name=timer9, state=start);
	%ScoreDataModelChamp();
	%_timer(name=timer9);

	/*Get Most Recent RCCC for MES*/
	PROC SQL; 
	CREATE TABLE work.rccc AS SELECT DISTINCT Sim, Max(IVSweepReadTime) FORMAT=DATETIME23.3 as MRRR, RCCC, count(*) as N FROM work.ME_INT_ISCAD2_SCORED GROUP BY Sim, RCCC ORDER BY Sim, MRRR;
	QUIT;
	
	PROC SORT data=work.rccc;
		by Sim descending MRRR;
	RUN;
	
	DATA rccc;
		set rccc;
		by sim descending mrrr;
		if first.sim then do; RN = 1; end;	else RN+1;
	run;
		
	/*Calculate Test Isc Avg by Sim and Simunit, then subtract to identify offset Delta*/
	PROC SQL;
/*	210318 (TWS):  Changing from Avg to Median to calculate SimAvgIsc and SimUnitAvgIsc																																*/
/* 	CREATE TABLE work.SimIscOffset AS 		SELECT DISTINCT Sim, SimUnit, AVG(isc) as SimAvgIsc, Count(*) as SimNobs FROM work.ME_INT_ISCAD2_SCORED GROUP BY Sim, SimUnit; 											*/
/* 	CREATE TABLE work.SimUnitIscOffset AS 	SELECT DISTINCT SimUnit, AVG(SimAvgIsc) as SimUnitAvgIsc, Sum(SimNObs) as SimUnitNObs, Count(*) as NSims FROM work.SimIscOffset WHERE SimNobs > 50 GROUP BY SimUnit; 	*/

/*	210406 (TWS):  Changing from Isc to P_Isc to calculate SimAvgIsc and SimUnitAvgIsc	per request from Dat																										*/
/* 	CREATE TABLE work.SimIscOffset AS 		SELECT DISTINCT Sim, SimUnit, MEDIAN(Isc) as SimAvgIsc, Count(*) as SimNobs FROM work.ME_INT_ISCAD2_SCORED GROUP BY Sim, SimUnit; */
	CREATE TABLE work.SimIscOffset AS 		SELECT DISTINCT Sim, SimUnit, MEDIAN(P_Isc) as SimAvgIsc, Count(*) as SimNobs FROM work.ME_INT_ISCAD2_SCORED GROUP BY Sim, SimUnit;
	CREATE TABLE work.SimUnitIscOffset AS 	SELECT DISTINCT 	 SimUnit, MEDIAN(SimAvgIsc) as SimUnitAvgIsc, Sum(SimNObs) as SimUnitNObs, Count(*) as NSims FROM work.SimIscOffset WHERE SimNobs > 50 GROUP BY SimUnit;
	CREATE TABLE work.TestIscOffset AS 
		SELECT Sim, SimAvgIsc, SimUnitAvgIsc, CASE WHEN SimNobs > 50 THEN  SimUnitAvgIsc - SimAvgIsc ELSE 0 END AS DeltaIsc, SimNobs, SimUnitNObs, NSims
		FROM 
			work.SimIscOffset a 
			LEFT JOIN work.SimUnitIscOffset b on a.simunit = b.simunit 
	;
	QUIT;
	
	%deletedsifexists(work,SimIscOffset);
	%deletedsifexists(PUBLIC,SimUnitIscOffset);	
	
	PROC SQL;/*ME_INT_IscModelOffset_Pre*/
	CREATE TABLE work.ME_INT_IscModelOffset_Pre AS
	SELECT DISTINCT
		a.Sim LENGTH=6 AS Lineage
		,a.Sim
		,CASE 
			WHEN SUBSTR(a.SimUnit,1,3) = 'PGT' THEN &PGTTime 
			WHEN SUBSTR(a.SimUnit,1,3) = 'KMT' THEN &KMTTime 
			WHEN SUBSTR(a.SimUnit,1,3) = 'DMT' THEN &DMTTime 
			END	AS TimeStamp FORMAT=datetime23.3
		,&UDTTime AS UTC FORMAT=datetime23.3
		,a.SimUnit Length=5
		,d.N as NObs
/*	210318 (TWS):  Changing from Avg to Median to calculate AvgRsidual*/
/* 		,AVG(Residual) as AvgResidual */
		,MEDIAN(Residual) as AvgResidual
		,d.rccc
		,SQRT(AVG(Residual**2)) as RMSE
		/*210311 (TWS):  Adding Variance here for additional signal*/
		,VAR(Residual) AS VarError
		,MAX(P_ISC) as MaxPIsc
		,MIN(P_ISC) AS MinPIsc
	FROM 
		work.ME_INT_ISCAD2_SCORED a
		LEFT JOIN work.rccc d on a.sim = d.sim and d.rn = 1
	GROUP BY a.Sim
	;
	QUIT;
	
	PROC SQL;/*ME_INT_IscModelOffsetSim*/
	CREATE TABLE work.ME_INT_IscModelOffsetSim AS
	SELECT DISTINCT
		Lineage
		,a.Sim
		,TimeStamp 
		,UTC
		,a.SimUnit
		,SimAvgIsc
		,SimUnitAvgIsc
		,DeltaIsc
		,AvgResidual
		,(-1*AvgResidual+DeltaIsc) AS cIscOffset
		,CASE	
			WHEN NObs < 50 THEN 0
			WHEN (-1*AvgResidual+DeltaIsc) < -0.02 THEN -0.02
			WHEN (-1*AvgResidual+DeltaIsc) >  0.01 THEN  0.01
			WHEN (-1*AvgResidual+DeltaIsc) < -0.02*SimAvgIsc THEN -0.02*SimAvgIsc
			WHEN (-1*AvgResidual+DeltaIsc) BETWEEN -0.002 AND 0.002 THEN 0
			WHEN (-1*AvgResidual+DeltaIsc) <  0.01*SimAvgIsc THEN (-1*AvgResidual+DeltaIsc)
			ELSE 0.01*SimAvgIsc	END as SimOffset 
		,NObs
		,CASE	
			WHEN NObs < 50 THEN 'ObsLowerLimit'
			WHEN (-1*AvgResidual+DeltaIsc) < -0.02 THEN 'HardLowerLimit'
			WHEN (-1*AvgResidual+DeltaIsc) >  0.01 THEN 'HardUpperLimit'
			WHEN (-1*AvgResidual+DeltaIsc) < -0.02*SimAvgIsc THEN 'OffsetLowerLimit'
			WHEN (-1*AvgResidual+DeltaIsc) BETWEEN -0.002 AND 0.002 THEN 'OffsetDeadBand'
			WHEN (-1*AvgResidual+DeltaIsc) <  0.01*SimAvgIsc THEN 'CalcOffset'
			ELSE 'OffsetUpperLimit'	END as SimOffsetSource 
		,rccc
		,RMSE
		,VarError
		,MaxPIsc
		,MinPIsc
	FROM 
		work.ME_INT_IscModelOffset_Pre a
		LEFT JOIN TestIscOffset b on a.sim = b.Sim 
	;
	QUIT;
	
	%deletedsifexists(PUBLIC,ME_INT_IscModelOffset_Pre);	
	%deletedsifexists(work,rccc);	
	
	
	PROC SQL NOPRINT;
	CREATE TABLE work.ME_INT_IscModelOffsetSimMS AS 
	SELECT a.*, b.ProjId, b.ModelID
	FROM
		WORK.ME_INT_ISCMODELOFFSETSIM a
		LEFT JOIN WORK.MODELSTUDIOPROJECTS b on a.lineage = b.spul;
	QUIT;

%END;

/*  Write 0's to MES when model is inactive  */

%if %sysfunc(exist(work.ME_INT_IscModelOffsetSimMS))=0  %then %do;
	%Put DPPULIC Exist and Public Exist;
	PROC SQL;
	CREATE TABLE WORK.ME_INT_IscModelOffsetSimMS AS
	SELECT Lineage, Sim, 'TimeStamp'n, UTC, SimUnit, SimAvgIsc, SimUnitAvgIsc, DeltaIsc, AvgResidual, cIscOffset, SimOffset, NObs, SimOffsetSource, rccc, RMSE, VarError, ProjID, ModelID FROM PUBLIC.ME_INT_ISCMODELOFFSETSIMMS;
	DELETE FROM WORK.ME_INT_IscModelOffsetSimMS;
	QUIT;
%end;

data work.ModelStudioProjects; set isclib.ModelStudioProjects; if SPUL =: "&Unit." AND Active=0; run;
data work.ModelStudioProjects; LENGTH ModelID $36.; set work.ModelStudioProjects;  rownum=_n_; run;

PROC SQL NOPRINT; SELECT COUNT(*) INTO :nInActiveModels FROM work.ModelStudioProjects; QUIT;
%PUT Number of inActive Models:  &nInActiveModels;

%IF &nInActiveModels >= 1 %THEN %DO;
	%InactiveInsert();
%END; /*End inactive project work*/	

%SendToMes();

libname DPPUBLIC base "/sasdata/DPPublic/" ;
%LET UDTTime = %SYSFUNC(tzones2u(%SYSFUNC(DATETIME())));

/* Manage ME_INT_IscModelOffsetSimMS Data in DPPublic*/
%if %sysfunc(exist(DPPUBLIC.ME_INT_IscModelOffsetSimMS))=1 %then %do;
	%Put DPPULIC.ME_INT_IscModelOffsetSimMS Exist and Public.ME_INT_IscModelOffsetSimMS Exist;
	DATA DPPUBLIC.ME_INT_IscModelOffsetSimMS;
		SET DPPUBLIC.ME_INT_IscModelOffsetSimMS (WHERE=(UTC >=(&UDTTime-86400*30))) WORK.ME_INT_IscModelOffsetSimMS;
	RUN;	
%end;
%if %sysfunc(exist(DPPUBLIC.ME_INT_IscModelOffsetSimMS))=0 AND %sysfunc(exist(PUBLIC.ME_INT_IscModelOffsetSimMS))=1 %then %do;
	%Put DPPULIC.ME_INT_IscModelOffsetSimMS !Exist and Public.ME_INT_IscModelOffsetSimMS Exist;
	DATA DPPUBLIC.ME_INT_IscModelOffsetSimMS;
		SET PUBLIC.ME_INT_IscModelOffsetSimMS (WHERE=(UTC >=(&UDTTime-86400*30))) WORK.ME_INT_IscModelOffsetSimMS;
	RUN;
%END;
%if %sysfunc(exist(DPPUBLIC.ME_INT_IscModelOffsetSimMS))=0 AND %sysfunc(exist(PUBLIC.ME_INT_IscModelOffsetSimMS))=0 %then %do;
	%Put DPPULIC.ME_INT_IscModelOffsetSimMS !Exist and Public.ME_INT_IscModelOffsetSimMS !Exist;
	DATA DPPUBLIC.ME_INT_IscModelOffsetSimMS;
		SET WORK.ME_INT_IscModelOffsetSimMS;
	RUN;	
%END;

/* Manage ME_INT_ISCAD2_SCORED  data in DPPublic and Public*/
%if %sysfunc(exist(DPPUBLIC.ME_INT_ISCAD2_SCORED))=1 AND %sysfunc(exist(WORK.ME_INT_ISCAD2_SCORED))=1 %then %do;
	%Put DPPUBLIC.ME_INT_ISCAD2_SCORED Exist;
	DATA DPPUBLIC.ME_INT_ISCAD2_SCORED;
		SET DPPUBLIC.ME_INT_ISCAD2_SCORED WORK.ME_INT_ISCAD2_SCORED;
	RUN;
%end;
%if %sysfunc(exist(DPPUBLIC.ME_INT_ISCAD2_SCORED))=0 AND %sysfunc(exist(WORK.ME_INT_ISCAD2_SCORED))=1  %then %do;
	%Put DPPUBLIC.ME_INT_ISCAD2_SCORED !Exist ;
	DATA DPPUBLIC.ME_INT_ISCAD2_SCORED;
		SET WORK.ME_INT_ISCAD2_SCORED;
	RUN;
%END;

%MEND;


