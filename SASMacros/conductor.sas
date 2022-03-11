OPTIONS VALIDVARNAME=ANY NOFULLSTIMER casdatalimit=5G NOMPRINT STIMER VARLENCHK=NOWARN;
OPTIONS autosignon=yes noconnectwait noconnectpersist sascmd='!sascmd -nonotes -nonews -nosource' ;

/*SETUP MACROS*/
%MACRO conUniformDelay(sec=1);
/* 	%PUT Insert Random Delay, Between 0 and 1 Second; */
	DATA _null_;
		conUniformDelay = rand('Uniform');
		rc=SLEEP(conUniformDelay*&sec,1);
	RUN;
%MEND;
%MACRO DropIt(name=);/* Macro for Dropping a Table from CAS */
proc casutil incaslib="public";
  droptable casdata="&name";   
/*   list files;                                      */
run; quit;
%mend;
%MACRO LoadPromoteIt(name=);/* Load table to CAS and Promt */
proc casutil;              
   load data=work.&name outcaslib='public' promote ; 
   save casdata="&name" replace;
run; quit;
%mend;
%MACRO deletedsifexists(lib,name);/* Remove existing table from LASR if loaded already */
    %if %sysfunc(exist(&lib..&name.)) %then %do;
/* 		%put DeletDSifExistsMacro; */
		proc datasets library=&lib. nolist;
	        delete &name.;
	    quit;
	%end;
	%if %sysfunc(exist(&lib..&name.)) %then %do;
		%put First Attempt Failed; Trying one more time;
		%conUniformDelay(sec=1);
		proc datasets library=&lib. nolist;
	        delete &name.;
	    quit;
	%end;
%mend;
%MACRO deleteCASdsifexists(lib,name);/* Remove existing table from CAS if loaded already */ 
    %if %sysfunc(exist(&lib..&name.)) %then %do;
/* 		%put DeleteCASDSifExistsMacro; */
		%DropIt(name=&name);
	%end;
	%if %sysfunc(exist(&lib..&name.)) %then %do;
		%put First Attempt Failed; Trying one more time;
		%conUniformDelay(sec=1);
		%DropIt(name=&name);
	%end;
%mend ;
%MACRO email(Slib, Sname, email);/* Code to email contact defined in macro call*/
	%put Expected dataset does not exists (&Slib..&Sname.), need to email someone;
	filename mailbox email
			TO=(&email)
			FROM=('NoReply <NOREPLY@firstsolar.com>')
			SENDER = ('NoReply <NOREPLY@firstsolar.com>')
			IMPORTANCE='HIGH'
			replyto='NOREPLY@FirstSolar.com'
	        Subject='SAS VA Dataset Loading Failed';
	  
	DATA _NULL_;
	FILE Mailbox;
	PUT "Greetings,";
	PUT "  This is a message from a SAS.";
	PUT "Expected dataset does not exists (&Slib..&Sname.)";
	PUT "may need to do something";
	RUN;
%mend;
%MACRO checkresult(Slib,Sname,Tlib,Tname,email);/*Macro to check to see if expected dataset exists and if so, then do stuff*/
/* 	%PUT Source Data &Slib..&Sname.; */
/* 	%PUT Target Data &Tlib..&Tname ; */
	%if %sysfunc(exist(&Slib..&Sname.)) %then %do;
/* 		%put CheckResultMacro; */
			%conUniformDelay(sec=2);			
		%if %sysfunc(exist(&Tlib..&Tname.)) %then %do;
			%trylock(member=&Tlib..&Tname.);
			data &Tlib..&Tname.; /*truncate target table*/
				set &Tlib..&Tname. (obs=0);
			run;			
			DATA &Tlib..&Tname.; /*add new columns if present*/
				MERGE &Slib..&Sname. (obs=0);
				BY &MergeBy;
			RUN;
			PROC APPEND base=&Tlib..&Tname. data=&Slib..&Sname. FORCE; RUN; /*use proc append for processing speed*/
			LOCK &Tlib..&Tname. CLEAR;
		%end;
		%else %do;
			DATA &Tlib..&Tname.; /*add new columns if present*/
				MERGE &Slib..&Sname. (obs=0);
				BY &MergeBy;
			RUN;
		%end;
	%end;	
	%else %do;
		%email(&Slib, &Sname, &email);
	%end;
%mend;
%MACRO checkCASresult(Slib,Sname,Tlib,Tname,email);/*Macro to check to see if expected dataset exists and if so, then do stuff.. specifict to writing to CAS*/
/* 	%PUT Source Data &Slib..&Sname.; */
/* 	%PUT Target Data &Tlib..&Tname ; */
	%if %sysfunc(exist(&Slib..&Sname.)) %then %do;
/* 		%put CheckResultMacro; */
			%conUniformDelay(sec=1);
			%deleteCASdsifexists(&Tlib, &Tname);
			%conUniformDelay(sec=1);			
/* 			%put loadTable; */
			%LoadPromoteIt(name=&Sname);
	%end;	
	%else %do;
		%email(&Slib, &Sname, &email);
	%end;
%mend;
%MACRO etlcontrol(plant, dsn, multiEnabled, multiTaskRequest); /*control processing of ETL Requests (Serial Vs Parallel) */
	%LET PlantTableName = &Plant._&TableName;
	%IF &multiEnabled=0 OR &_MultiTaskActive = 0  OR &multiTaskRequest = 0 %THEN %DO; /*SERIAL*/
		%PUT Running &Plant. in Series;
		%Timing(action=2,Process="&Plant.",Category='ETL',Task="Initialization");
		%etlexec(Plant=&Plant,TableName=&PlantTableName,dsn=&dsn);
		%if %sysfunc(exist(work.&PlantTableName.)) %then %do;
			%LET ConCatString = &ConCatString work.&PlantTableName;
		%END;
		%ELSE %DO;
			%email(work, &PlantTableName, &email);
		%END;
	%END;
	%IF &multiEnabled=1 AND &_MultiTaskActive = 1 AND &multiTaskRequest = 1 %THEN %DO; /*PARALLEL*/
	
		%PUT Running &Plant. in Parallel;
		%syslput _USER_/remote=&Plant; 
		rsubmit &Plant wait=no inheritlib=(workdir); 
			OPTIONS VALIDVARNAME=ANY MAUTOSOURCE;
			%put NOTE:  Start Running in Parallel for &PlantTableName, &plant;	
			%Timing(action=2,Process="&Plant.",Category='ETL',Task="Initialization");
			%etlexec(Plant=&Plant,TableName=&PlantTableName,dsn=&dsn,multitask=1);		
		endrsubmit;

	%END;
%mend;
%MACRO extractdisktable(TableName, DaysAgo);/*EXTRACT LAST &DaysAgo OF DATA FROM DPPUBLIC (on-disk) TABLE*/

PROC SQL;
CREATE TABLE work.Disk_&TableName AS
	SELECT *
	FROM DPPUBLIC.&TableName 
	WHERE 
		&ProcSQLTimeFilter >= DHMS(DATE()-&DaysAgo,0,0,0)
	ORDER BY &ProcSQLOrderBy;
QUIT;

PROC SORT data=work.Disk_&TableName;
BY &MergeBy;
RUN;

LIBNAME DPPUBLIC CLEAR;

%mend;
%MACRO appenddatatable(TableName, ConCatString);/*Merge old data with new data*/

DATA work.&TableName;
	MERGE &ConCatString;
	BY &MergeBy;
RUN;

%mend;
%MACRO tablesize();/*The Following Lines Print the Size of Tables Created in WorkLibrary*/
proc sql NOPRINT;
create table WorkTables as
select libname, memname as TableName, nobs, crdate, modate, (filesize+nobs*obslen)/2000 as kB
from dictionary.tables
where 
	libname IN ('WORK')
	AND NOT(memname IN ('_PRODSAVAIL', 'WorkTables'))
ORDER BY kB Desc;
quit;

%PUT |------------------------------- Work Tables ------------------------------;
%PUT | TableName                            Nobs                   kB;
OPTION NONOTES;
DATA _NULL_;
	SET work.WorkTables;
	PUT '|' @3 TableName @34 nobs comma9. @54 kB comma9.;
RUN;
%PUT |------------------------------- Work Tables ------------------------------;

PROC SQL;
DROP TABLE Work.WorkTables;
QUIT;

%mend;
%MACRO VarExist(ds, var, returnvar);/* */
	%LOCAL rc dsid result resx;
/* 	%PUT &ds &var &returnvar; */
	%let dsid = %SYSFUNC(OPEN(&ds));
	%let &returnvar = %SYSFUNC(VARNUM(&dsid, &var));
	%let rc = %SYSFUNC(CLOSE(&dsid));
%MEND;
%MACRO parallelDaysAgo(lib=, name=, workers=, DaysAgoPlant=, DaysAgoDTS=);

options autosignon=yes 		/* Automatically handle sign on to RSUBMIT sessions */
		noconnectwait		/* Run all RSUBMIT sessions in parallel */
		noconnectpersist	/* Sign off after each RSUBMIT session ends */
		/*sascmd='!sascmd -nonews -nonotes -nosource'*/	/* Sign on to each RSUBMIT session with the same SAS command used to start this session */
		sascmd='!sascmd -nonews -nonotes -nofullstimer -stimer -nosource'	/* Sign on to each RSUBMIT session with the same SAS command used to start this session */
		nofullstimer
		stimer
		nosource
		nonotes;

	%let dsname = &lib..&name.;
	%put &dsname;
	%let dsid	= %sysfunc(open(&dsname.));
	%let n		= %sysfunc(attrn(&dsid., nlobs));
	%let rc 	= %sysfunc(close(&dsid.));

    %do w = 1 %to &workers.;

		%let firstobs	= %sysevalf(&n-(&n/&workers.)*(&workers.-&w+1)+1, floor);
		%let obs 		= %sysevalf(&n-(&n/&workers.)*(&workers.-&w.), floor);
		%let total&w. 	= %sysevalf(&obs. - &firstobs. + 1);

		%syslput _USER_ / remote=worker&w.;
	
			/* Split the data evenly among all workers and read the data in parallel sessions */
			rsubmit remote=worker&w. inheritlib=(workdir);
	
				options nosource VALIDVARNAME=ANY;

				%Timing(action=2,Process="Remote &w.",Category='DaysAgo',Task="Initialization");
	
			    data workdir._out_&w.;
					length worker 8.;	
			        set &dsname. (firstobs=&firstobs. obs=&obs. cntllev=rec);	
					worker = &w.;
			    run;
	
				%Timing(action=2,Process="Remote &w.",Category='DaysAgo',Task="Copy Table");

				PROC SQL; 
				CREATE TABLE workdir.tablea&w. AS
				SELECT 
					*
					,CASE
						WHEN SUBSTR(&DaysAgoPlant,1,3) = 'PGT' THEN (DATEPART(DATETIME())-DATEPART(&DaysAgoDTS)) 
						WHEN SUBSTR(&DaysAgoPlant,1,3) = 'KMT' THEN (DATEPART(tzoneu2s(tzones2u(DATETIME()),'Asia/Singapore'))-DATEPART(&DaysAgoDTS))
						WHEN SUBSTR(&DaysAgoPlant,1,3) = 'DMT' THEN (DATEPART(tzoneu2s(tzones2u(DATETIME()),'Asia/Saigon'))-DATEPART(&DaysAgoDTS))
					END FORMAT=BEST3. AS DaysAgo
				FROM workdir._out_&w.;
				QUIT;

				%Timing(action=2,Process="Remote &w.",Category='DaysAgo',Task="Days Ago Calc");
	
				proc datasets lib=workdir nolist nowarn;
				  delete _out_&w.;		
				  change tablea&w.=_out_&w.;		
				  run;		
				quit;
				
				%Timing(action=2,Process="Remote &w.",Category='DaysAgo',Task="Delete and Rename");

			endrsubmit;
    %end;

	%let total = 0;

	%put;
	%put Parallel daysAgo Worker Observation Count;
	%put _________________________________________;

	%do i = 1 %to &workers.;
		%put &i.: %sysfunc(compress(%qsysfunc(putn(&&total&i., comma24.) ) ) );
		%let total = %eval(&total. + &&total&i.);
	%end;

	%put _________________________________________;
	%put TOTAL: %sysfunc(compress(%qsysfunc(putn(&total., comma24.) ) ) );
	
	%put;
	%put NOTE: Waiting for workers to finish...;

    waitfor _ALL_;

    data &name.;
        set _out:;
    run;

	proc datasets lib=workdir nolist nowarn;
	  delete _out:;	
	  run;		
	quit;


%mend;
%MACRO DaysAgoCheck(TableName, DaysAgoDTS, DaysAgoPlant);/* */

/* creating global vars to be returned from macro that checks if variables are present in local dataset*/
%GLOBAL DaysAgoDTSPresent DaysAgoPlantPresent DaysAgoPresent;
%LET daysagoWorkers = 2;

/* check the source table for columns necessary for calculation*/
%IF %SYSFUNC(EXIST(work.&TableName.)) %THEN %DO; 
	%VarExist(ds=&Tablename, var=&DaysAgoDTS, returnvar=DaysAgoDTSPresent);
	%IF &DaysAgoDTSPresent = 0 %THEN %DO; %PUT &DaysAgoDTS Does Not Exist in dataset!; %END; %ELSE %DO;   %END; 

	%VarExist(ds=&Tablename, var=&DaysAgoPlant, returnvar=DaysAgoPlantPresent);
	%IF &DaysAgoPlantPresent = 0 %THEN %DO; %PUT &DaysAgoPlant Does Not Exist in dataset!; %END; %ELSE %DO;  %END; 
%END; 
%ELSE %DO; %PUT Data Set Does Not Exist!; %END; 

/* logic to create daysago column*/
%IF &DaysAgoDTSPresent > 0 AND &DaysAgoPlantPresent > 0 AND %SYSFUNC(EXIST(work.&TableName.)) %THEN %DO;
/* 	%put going to calculate days ago; */
	%VarExist(ds=&Tablename, var=DaysAgo, returnvar=DaysAgoPresent);
	
	/* create table name for second table*/
	%let tablea = &tablename.a;
/* 	%put &tablea; */
	
	%IF &DaysAgoPresent = 0 %THEN %DO; 
/* 		%PUT DaysAgo not in the dataset;  */
		%parallelDaysAgo(lib=workdir, name=&TableName., workers=&daysagoWorkers., DaysAgoDTS=&DaysAgoDTS., DaysAgoPlant=&DaysAgoPlant.);
		
	%END; 
	%ELSE %DO;   
/* 		%PUT &DaysAgo in the dataset;  */
		data work.&tablename(drop=daysago);
			set work.&tablename;
		run;		
		%parallelDaysAgo(lib=workdir, name=&TableName., workers=&daysagoWorkers., DaysAgoDTS=&DaysAgoDTS., DaysAgoPlant=&DaysAgoPlant.);
	%END;
%END;
%ELSE %DO;
	%PUT DaysAgo not calculated based on information above;
%END;
%MEND;
%MACRO conductor(TableName, DaysAgo, Email, PGT1MES, PGT2MES, KMT1MES, KMT2MES, DMT1MES, DMT2MES, MFGODS, EDW, MAXPROD, CMS, PGTLAB, KMTLAB, DMTLAB, MAXIMO, GBLFED, KMS1MES, DaysAgoDTS, DaysAgoPlant, multitask=0 );
OPTIONS NONOTES;
%PUT 2022.2 Version of the Conductor AutoCall;
%LET workpath =  %sysfunc(getoption(work));
LIBNAME workdir "&workpath.";

%Timing(action=1);
%conUniformDelay(sec=2);

%LET workpath =  %sysfunc(getoption(work));
LIBNAME workdir "&workpath.";

/* %PUT Assign MergeBy Macro Var from ProcSQLOrderBy; */
%LET MergeBy=%SYSFUNC(compbl(%SYSFUNC(tranwrd(%QUOTE(&ProcSQLOrderBy),%STR(,),%STR( )))));

%LET ConCatString=;
/* %PUT &ConCatString; */

%LET TableNameFinal = &TableName;
%PUT &TableNameFinal is the TableName;

%IF %SYSFUNC(LENGTH(&TableName)) > 27 %THEN %DO;
	%PUT Table Name Too Long;
	%LET TableName = %SYSFUNC(SUBSTR(&TableName,1,27)); /*To prevent a table name that is too long*/
%END;
%ELSE %DO;
	%PUT TableName Okay;
%END;

libname dppublic base "/sasdata/DPPublic/" ;
%if %sysfunc(exist(dppublic.&TableName.)) AND &DaysAgo > 0 %then %do;
	%put &TableName exists on disk, going to extract data.;
	%Timing(action=2,Process="History Table",Category='MainProgram',Task='Extact Start');
	%LET PlantTableName = Disk_&TableName;
	%extractDiskTable(&TableName, &DaysAgo);
	%Timing(action=2,Process="History Table",Category='MainProgram',Task='Extact Complete');
	%LET ConCatString = &ConCatString work.&PlantTableName;
	%END;

/*Configuration for Extraction Request.  Need to place multitaks extractions at the top								*/
/*  (r)equest,			(p)lant,        (a)ctive,                                          (m)ultitask,  (d)ataSrc	*/
%LET r1  = PGT1MES; %LET p1  =PGT1;	%LET a1  =_PGT1DsnActive; /*Defined in AutoExec*/ ;%LET m1  = 1; %LET d1  = PGT1MESODS;
%LET r2  = PGT2MES; %LET p2  =PGT2;	%LET a2  =_PGT2DsnActive; /*Defined in AutoExec*/ ;%LET m2  = 1; %LET d2  = PGT2MESODS;
%LET r3  = KMT1MES; %LET p3  =KMT1;	%LET a3  =_KMT1DsnActive; /*Defined in AutoExec*/ ;%LET m3  = 1; %LET d3  = KMT1MESODS;
%LET r4  = KMT2MES; %LET p4  =KMT2;	%LET a4  =_KMT2DsnActive; /*Defined in AutoExec*/ ;%LET m4  = 1; %LET d4  = KMT2MESODS;
%LET r5  = DMT1MES; %LET p5  =DMT1;	%LET a5  =_DMT1DsnActive; /*Defined in AutoExec*/ ;%LET m5  = 1; %LET d5  = DMT1MESODS;
%LET r6  = DMT2MES; %LET p6  =DMT2;	%LET a6  =_DMT2DsnActive; /*Defined in AutoExec*/ ;%LET m6  = 1; %LET d6  = DMT2MESODS;
%LET r7  = MFGODS ; %LET p7  =ODS; 	%LET a7  =_ODSDsnActive;  %LET _ODSDsnActive=1    ;%LET m7  = 0; %LET d7  = ODSProd_ODS;
%LET r8  = EDW    ; %LET p8  =EDW; 	%LET a8  =_EDWDsnActive;  %LET _EDWDsnActive=1    ;%LET m8  = 0; %LET d8  = EDW;
%LET r9  = MAXPROD; %LET p9  =MXP;	%LET a9  =_MXPDsnActive;  %LET _MXPDsnActive=1    ;%LET m9  = 0; %LET d9  = maximo_maxprod;
%LET r10 = CMS    ; %LET p10 =CMS; 	%LET a10 =_CMSDsnActive;  %LET _CMSDsnActive=1    ;%LET m10 = 0; %LET d10 = blackpearl_cms;
%LET r11 = PGTLAB ; %LET p11 =PLAB; %LET a11 =_PLABDsnActive; %LET _PLABDsnActive=1   ;%LET m11 = 0; %LET d11 = pbgmesreporting_reliabilitydb;
%LET r12 = KMTLAB ; %LET p12 =KLAB; %LET a12 =_KLABDsnActive; %LET _KLABDsnActive=1   ;%LET m12 = 0; %LET d12 = klmmesreporting_reliabilitydb;
%LET r13 = DMTLAB ; %LET p13 =DLAB; %LET a13 =_DLABDsnActive; %LET _DLABDsnActive=1   ;%LET m13 = 0; %LET d13 = DMTSQLRELIAB;
%LET r14 = MAXIMO ; %LET p14 =MXP; 	%LET a14 =_MXPDsnActive;  %LET _MXPDsnActive =1   ;%LET m14 = 0; %LET d14 = maximo_maxprod;
%LET r15 = GBLFED ; %LET p15 =GFED; %LET a15 =_GFEDDsnActive; %LET _GFEDDsnActive=0   ;%LET m15 = 0; %LET d15 = global_fed;
%LET r16 = KMS1MES; %LET p16 =KMS1; %LET a16 =_KMS1DsnActive; %LET _KMS1DsnActive=1   ;%LET m16 = 0; %LET d16 = KMS1MESODS;

%LET maxConnections = 16;
%LET startOfSeries = 7;


OPTIONS NONOTES NODATE NONUMBER;
%DO i = 1 %TO &maxConnections;
	%IF &i = &startOfSeries %THEN %DO;
		%Timing(action=2,Process="LocalODS",Category='MainProgram',Task='Start');

		%IF &MULTITASK=1 %THEN %DO; /*Wait for Remote Submit Jobs*/
			%put NOTE: ***Wait for parallel tasks***;
			waitfor _all_; *signoff _all_;
			%IF &PGT1MES = 1 AND &_PGT1DsnActive = 1 AND %sysfunc(exist(work.PGT1_&TableName)) %THEN %DO; %LET ConCatString = &ConCatString work.PGT1_&TableName; %END;
			%IF &PGT2MES = 1 AND &_PGT2DsnActive = 1 AND %sysfunc(exist(work.PGT2_&TableName)) %THEN %DO; %LET ConCatString = &ConCatString work.PGT2_&TableName; %END;
			%IF &KMT1MES = 1 AND &_KMT1DsnActive = 1 AND %sysfunc(exist(work.KMT1_&TableName)) %THEN %DO; %LET ConCatString = &ConCatString work.KMT1_&TableName; %END;
			%IF &KMT2MES = 1 AND &_KMT2DsnActive = 1 AND %sysfunc(exist(work.KMT2_&TableName)) %THEN %DO; %LET ConCatString = &ConCatString work.KMT2_&TableName; %END;
			%IF &DMT1MES = 1 AND &_DMT1DsnActive = 1 AND %sysfunc(exist(work.DMT1_&TableName)) %THEN %DO; %LET ConCatString = &ConCatString work.DMT1_&TableName; %END;
			%IF &DMT2MES = 1 AND &_DMT2DsnActive = 1 AND %sysfunc(exist(work.DMT2_&TableName)) %THEN %DO; %LET ConCatString = &ConCatString work.DMT2_&TableName; %END;
			%put NOTE: ***Parallel tasks done***;
			OPTION NONOTES;
			%Timing(action=2,Process="LocalODS",Category='MainProgram',Task='Complete');

		%END;
	%end;

	%LET avalue = &&&a&i.; %LET rvalue = &&&r&i.;
/* 	%PUT &&r&i. @ &&&rvalue. is Multitask Enabled: &&p&i. &&a&i. @ &&&avalue. &&m&i. &&d&i.; */
	%IF &&&avalue. = 1 AND &&&rvalue. = 1 %THEN %DO;
		%etlcontrol(plant=&&p&i.,dsn=&&d&i.,multiEnabled=&&m&i.,multiTaskRequest=&multitask);
	%END;
%END;

OPTION NONOTES;

%Timing(action=2,Process="ETL Control",Category='MainProgram',Task='ETL Complete');

OPTIONS NONOTES;
%PUT This to be Contact: &ConCatString;

%AppendDataTable(&TableName, &ConCatString);
%Timing(action=2,Process="Data Append",Category='MainProgram',Task='Append Complete');

%IF %LENGTH(&DaysAgoDTS) > 0 AND %LENGTH(&DaysAgoPlant) > 0 %THEN %DO;
/* 	%put going to calculate days ago; */
	%DaysAgoCheck(&TableName, &DaysAgoDTS, &DaysAgoPlant);
%END;

%IF &DaysAgo > 0 %then %do;
/* 	%put write copy back to disk; */
	libname dppublic base "/sasdata/DPPublic/" ;
	%Timing(action=2,Process="History Table",Category='MainProgram',Task='Update Started');
	%CheckResult(work, &TableName, DPPUBLIC, &TableName, &email);
	%Timing(action=2,Process="History Table",Category='MainProgram',Task='Update Complete');
%END;

cas conductorSess sessopts=(caslib=public timeout=180 locale="en_US");
LIBNAME PUBLIC CAS CASLIB="PUBLIC";

%conUniformDelay(sec=1);

%Timing(action=2,Process="CAS Table",Category='MainProgram',Task='Update Started');
%CheckCASResult(work, &TableName, public, &TableName, &email);
%Timing(action=2,Process="CAS Table",Category='MainProgram',Task='Update Complete');

%PUT This is the system Error Numb: &SYSERR;
%PUT This is the system Error Text: &syserrortext;

/*Metadata denied access:  One Retry to write data to LASR*/
%IF 
	%SYSFUNC(find(&syserrortext.,Metadata Server denied)) ge 1 
	OR %SYSFUNC(find(&syserrortext.,Write access to member)) ge 1
	OR %SYSFUNC(find(&syserrortext.,Signing)) ge 1
	OR %SYSFUNC(find(&syserrortext.,Web server)) ge 1
	OR %SYSFUNC(find(&syserrortext.,Unauthorized)) ge 1
	%THEN %DO; 
	%PUT Try again to Write to CAS;
	%CheckCASResult(work, &TableName, public, &TableName, &email);
%END;	

OPTIONS NOMPRINT;

LIBNAME PUBLIC CLEAR;

CAS conductorSess TERMINATE;

%Timing(action=2,Process="Conductor",Category='MainProgram',Task='Complete');
%Timing(action=3);

%TableSize();

LIBNAME _ALL_ CLEAR;

%mend;

