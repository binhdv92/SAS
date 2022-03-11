/*Version 3 of the S6 Conductor AutoCalls Program*/
OPTIONS VALIDVARNAME=ANY FULLSTIMER casdatalimit=5G;

/*ASSIGN LASRLIB LIBRARY NAME*/
/*Libname DPPUBLIC BASE "/data/VALoadonStart";*/
/*LIBNAME LASRLIB META Library="Visual Analytics Public LASR" METAOUT=DATA;*/


/*SETUP MACROS*/

%MACRO UniformDelay10();
	%PUT Insert Random Delay, Between 0 and 10 Seconds;

	DATA _null_;
		UniformDelay = rand('Uniform');
		rc=SLEEP(UniformDelay*10,1);
	RUN;
%MEND;
%MACRO UniformDelay240();
	/*used to dispurse start time of batch jobs with 5min constraint of SMC*/
	%PUT Insert Random Delay, Between 0 and 24 Seconds;

	DATA _null_;
		UniformDelay = rand('Uniform');
		rc=SLEEP(UniformDelay*2,1);
	RUN;
%MEND;
%MACRO UniformDelay();
	%PUT Insert Random Delay, Between 0 and 1 Second;

	DATA _null_;
		UniformDelay = rand('Uniform');
		rc=SLEEP(UniformDelay,1);
	RUN;
%MEND;

/* Macro for Dropping a Table from CAS */
%macro DropIt(name=);
proc casutil incaslib="public";
  droptable casdata="&name";   
/*   list files;                                      */
run; quit;
%mend;

/* Load table to CAS and Promt */
%macro LoadPromoteIt(name=);
proc casutil;              
   load data=work.&name outcaslib='public' promote ; 
   save casdata="&name" replace;
run; quit;
%mend;

/* Remove existing table from LASR if loaded already */ 
%macro deletedsifexists(lib,name);
    %if %sysfunc(exist(&lib..&name.)) %then %do;

		%put DeletDSifExistsMacro;

		proc datasets library=&lib. nolist;
	        delete &name.;
	    quit;

	%end;

	%if %sysfunc(exist(&lib..&name.)) %then %do;

		%put First Attempt Failed; Trying one more time;

		%UniformDelay();

		proc datasets library=&lib. nolist;
	        delete &name.;
	    quit;

	%end;
%mend;

/* Remove existing table from CAS if loaded already */ 
%macro deleteCASdsifexists(lib,name);
    %if %sysfunc(exist(&lib..&name.)) %then %do;

		%put DeleteCASDSifExistsMacro;

		%DropIt(name=&name);

	%end;

	%if %sysfunc(exist(&lib..&name.)) %then %do;

		%put First Attempt Failed; Trying one more time;

		%UniformDelay();

		%DropIt(name=&name);

	%end;
%mend ;
%macro email(Slib, Sname, email);
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
/*Macro to check to see if expected dataset exists and if so, then do stuff*/
%macro checkresult(Slib,Sname,Tlib,Tname,email);
	%PUT Source Data &Slib..&Sname.;
	%PUT Target Data &Tlib..&Tname ;
	%if %sysfunc(exist(&Slib..&Sname.)) %then %do;
		%put CheckResultMacro;

			%UniformDelay240();
			
			/*20200624:  Removed following code */
			/*
			%deletedsifexists(&Tlib, &Tname);

	   

			data &Tlib..&Tname (  );
			    set &Slib..&Sname (  );
			run;
			*/
			
		%if %sysfunc(exist(&Tlib..&Tname.)) %then %do;
			/*20200624:  Added following code */
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
%macro checkCASresult(Slib,Sname,Tlib,Tname,email);
	%PUT Source Data &Slib..&Sname.;
	%PUT Target Data &Tlib..&Tname ;
	%if %sysfunc(exist(&Slib..&Sname.)) %then %do;
		%put CheckResultMacro;

			%UniformDelay();

			%deleteCASdsifexists(&Tlib, &Tname);

			%UniformDelay();			
			%put loadTable;
			%LoadPromoteIt(name=&Sname);
	%end;	
	%else %do;
		%email(&Slib, &Sname, &email);
	%end;
%mend;
%macro pgt1etl(TableName);
PROC SQL;
   CONNECT TO ODBC as con2
    (
    DATASRC="PGT1MESODS" 
    authdomain=SQLGRP_Temp_Reader_Auth
    );

CREATE TABLE work.&TableName    AS 
SELECT * FROM CONNECTION TO con2 
(
&SQLExtraction.
);
DISCONNECT FROM con2;
QUIT;

PROC SORT data=work.&TableName;
BY &MergeBy;
RUN;

%mend;
%macro pgt2etl(TableName);
PROC SQL;
   CONNECT TO ODBC as con2
    (
    DATASRC="PGT2MESODS" 
    authdomain=SQLGRP_Temp_Reader_Auth
    );

CREATE TABLE work.&TableName    AS 
SELECT * FROM CONNECTION TO con2 
(
&SQLExtraction.
);
DISCONNECT FROM con2;
QUIT;

PROC SORT data=work.&TableName;
BY &MergeBy;
RUN;

%mend;
%macro kmt1etl(TableName);
PROC SQL;
   CONNECT TO ODBC as con2
    (
    DATASRC="KMT1MESODS" 
    authdomain=SQLGRP_Temp_Reader_Auth
    );

CREATE TABLE work.&TableName    AS 
SELECT * FROM CONNECTION TO con2 
(
&SQLExtraction.
);
DISCONNECT FROM con2;
QUIT;


PROC SORT data=work.&TableName;
BY &MergeBy;
RUN;

%mend;
%macro kmt2etl(TableName);
PROC SQL;
   CONNECT TO ODBC as con2
    (
    DATASRC="KMT2MESODS" 
    authdomain=SQLGRP_Temp_Reader_Auth
    );

CREATE TABLE work.&TableName    AS 
SELECT * FROM CONNECTION TO con2 
(
&SQLExtraction.
);
DISCONNECT FROM con2;
QUIT;


PROC SORT data=work.&TableName;
BY &MergeBy;
RUN;

%mend;
%macro dmt1etl(TableName);
PROC SQL;
   CONNECT TO ODBC as con2
    (
    DATASRC="DMT1MESODS" 
    authdomain=SQLGRP_Temp_Reader_Auth
    );

CREATE TABLE work.&TableName    AS 
SELECT * FROM CONNECTION TO con2 
(
&SQLExtraction.
);
DISCONNECT FROM con2;
QUIT;

PROC SORT data=work.&TableName;
BY &MergeBy;
RUN;

%mend;
%macro dmt2etl(TableName);
PROC SQL;
   CONNECT TO ODBC as con2
    (
    DATASRC="DMT2MESODS" 
    authdomain=SQLGRP_Temp_Reader_Auth
    );

CREATE TABLE work.&TableName    AS 
SELECT * FROM CONNECTION TO con2 
(
&SQLExtraction.
);
DISCONNECT FROM con2;
QUIT;

PROC SORT data=work.&TableName;
BY &MergeBy;
RUN;

%mend;
%macro odsetl(TableName);
PROC SQL;
   CONNECT TO ODBC as con2
    (
    DATASRC="ODSProd_ODS" 
    authdomain=SQLGRP_Temp_Reader_Auth
    );

CREATE TABLE work.&TableName    AS 
SELECT * FROM CONNECTION TO con2 
(
&SQLExtraction.
);
DISCONNECT FROM con2;

PROC SORT data=work.&TableName;
BY &MergeBy;
RUN;

QUIT;
%mend;
%macro edwetl(TableName);
PROC SQL;
   CONNECT TO ODBC as con2
    (DATASRC="EDW" authdomain=EDWAuth);

CREATE TABLE work.&TableName    AS 
SELECT * FROM CONNECTION TO con2 
(
&SQLExtraction.
);
DISCONNECT FROM con2;
QUIT;

PROC SORT data=work.&TableName;
BY &MergeBy;
RUN;

%mend;
%macro cmsetl(TableName);
PROC SQL;
   CONNECT TO ODBC as con2
    (DATASRC="blackpearl_cms" authdomain=SQLGRP_Temp_Reader_Auth);

CREATE TABLE work.&TableName    AS 
SELECT * FROM CONNECTION TO con2 
(
&SQLExtraction.
);
DISCONNECT FROM con2;
QUIT;
%mend;
%macro pgtlab(TableName);
PROC SQL;
   CONNECT TO ODBC as con2
    (DATASRC="pbgmesreporting_reliabilitydb" authdomain=SQLGRP_Temp_Reader_Auth);

CREATE TABLE work.&TableName    AS 
SELECT * FROM CONNECTION TO con2 
(
&SQLExtraction.
);
DISCONNECT FROM con2;
QUIT;
%mend;
%macro kmtlab(TableName);
PROC SQL;
   CONNECT TO ODBC as con2
    (DATASRC="klmmesreporting_reliabilitydb" authdomain=SQLGRP_Temp_Reader_Auth);

CREATE TABLE work.&TableName    AS 
SELECT * FROM CONNECTION TO con2 
(
&SQLExtraction.
);
DISCONNECT FROM con2;
QUIT;
%mend;
%macro dmtlab(TableName);
PROC SQL;
   CONNECT TO ODBC as con2
    (DATASRC="DMTSQLRELIAB" authdomain=SQLGRP_Temp_Reader_Auth);

CREATE TABLE work.&TableName    AS 
SELECT * FROM CONNECTION TO con2 
(
&SQLExtraction.
);
DISCONNECT FROM con2;
QUIT;
%mend;
%macro maximo(TableName);
PROC SQL;
   CONNECT TO ODBC as con2
    (DATASRC="maximo_maxprod" authdomain=SQLGRP_Temp_Reader_Auth);

CREATE TABLE work.&TableName    AS 
SELECT * FROM CONNECTION TO con2 
(
&SQLExtraction.
);
DISCONNECT FROM con2;
QUIT;
%mend;
%macro GlobalFed(TableName);
PROC SQL;
   CONNECT TO ODBC as con2
    (DATASRC="global_fed" authdomain=SQLGRP_Temp_Reader_Auth);

CREATE TABLE work.&TableName    AS 
SELECT * FROM CONNECTION TO con2 
(
&SQLExtraction.
);
DISCONNECT FROM con2;
QUIT;
%mend;

%macro extractdisktable(TableName, DaysAgo);
/*EXTRACT LAST &DaysAgo OF DATA FROM LASR TABLE*/
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

%macro appenddatatable(TableName, ConCatString);

/*Merge old data with new data*/

DATA work.&TableName;
	MERGE &ConCatString;
	BY &MergeBy;
RUN;

/*DATA work.OPS_S6THOUGHPUT;*/
/*MERGE Disk_OPS_S6THOUGHPUT KMT1_OPS_S6THOUGHPUT;*/
/*BY SourceLocation TimeStampHour;*/
/*RUN;*/
/*END UPDATING FROM LASR TABLE*/
%mend;
%macro tablesize();
/*The Following Lines Print the Size of Tables Created in WorkLibrary*/
proc sql NOPRINT;
create table WorkTables as
select libname, memname as TableName, nobs, crdate, modate, (filesize+nobs*obslen)/2000 as kB
from dictionary.tables
where 
	libname IN ('WORK')
	AND NOT(memname IN ('_PRODSAVAIL', 'WorkTables'))
ORDER BY FileSize Desc;
quit;

DATA _NULL_;
	SET work.Worktables;
	PUT 'NOTE: ------------------------------- Column Names ------------------------------';
	PUT 'NOTE: ' @10 'TableName' @47 'Nobs' @67 'kB';
	PUT 'NOTE: ' @10 TableName @41 nobs comma9. @61 kB comma9.;
	PUT 'NOTE: ------------------------------- Column Names ------------------------------';
RUN;

PROC SQL;
DROP TABLE Work.WorkTables;
QUIT;

%mend;

%macro VarExist(ds, var, returnvar);
	%LOCAL rc dsid result resx;
	%PUT &ds &var &returnvar;
	%let dsid = %SYSFUNC(OPEN(&ds));
	%let &returnvar = %SYSFUNC(VARNUM(&dsid, &var));
	%let rc = %SYSFUNC(CLOSE(&dsid));
%MEND;

%MACRO DaysAgoCheck(TableName, DaysAgoDTS, DaysAgoPlant);

/* creating global vars to be returned from macro that checks if variables are present in local dataset*/
%GLOBAL DaysAgoDTSPresent DaysAgoPlantPresent DaysAgoPresent;

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
	%put going to calculate days ago;
	%VarExist(ds=&Tablename, var=DaysAgo, returnvar=DaysAgoPresent);
	
	/* create table name for second table*/
	%let tablea = &tablename.a;
	%put &tablea;
	
	%IF &DaysAgoPresent = 0 %THEN %DO; 
		%PUT DaysAgo not in the dataset; 

		PROC SQL; 
		CREATE TABLE work.&tablea AS
		SELECT 
			*
			,CASE
				WHEN SUBSTR(&DaysAgoPlant,1,3) = 'PGT' THEN (DATEPART(DATETIME())-DATEPART(&DaysAgoDTS)) 
				WHEN SUBSTR(&DaysAgoPlant,1,3) = 'KMT' THEN (DATEPART(tzoneu2s(tzones2u(DATETIME()),'Asia/Singapore'))-DATEPART(&DaysAgoDTS))
				WHEN SUBSTR(&DaysAgoPlant,1,3) = 'DMT' THEN (DATEPART(tzoneu2s(tzones2u(DATETIME()),'Asia/Saigon'))-DATEPART(&DaysAgoDTS))
			END FORMAT=BEST3. AS DaysAgo
		FROM work.&tablename;
		QUIT;
		proc datasets lib=work nolist nowarn;
		  delete &tablename;		
		  change &tablea=&tablename;		
		  run;		
		quit;
		
	%END; 
	%ELSE %DO;   
		%PUT &DaysAgo in the dataset; 

		data work.&tablename(drop=daysago);
			set work.&tablename;
		run;		

		PROC SQL;
		CREATE TABLE work.&tablea AS
		SELECT 
			*
			,CASE
				WHEN SUBSTR(&DaysAgoPlant,1,3) = 'PGT' THEN (DATEPART(DATETIME())-DATEPART(&DaysAgoDTS)) 
				WHEN SUBSTR(&DaysAgoPlant,1,3) = 'KMT' THEN (DATEPART(tzoneu2s(tzones2u(DATETIME()),'Asia/Singapore'))-DATEPART(&DaysAgoDTS))
				WHEN SUBSTR(&DaysAgoPlant,1,3) = 'DMT' THEN (DATEPART(tzoneu2s(tzones2u(DATETIME()),'Asia/Saigon'))-DATEPART(&DaysAgoDTS))
			END FORMAT=BEST3. AS DaysAgo
		FROM work.&tablename;
		QUIT;
		proc datasets lib=work nolist nowarn;	
		  delete &tablename;			
		  change &tablea=&tablename;		
		  run;		
		quit;

	%END;

%END;
%ELSE %DO;
	%PUT DaysAgo not calculated based on information above;
%END;
%MEND;


%macro _fconductor(TableName, Email, PGT1MES, PGT2MES, KMT1MES, KMT2MES, DMT1MES, DMT2MES, MFGODS, EDW, MAXPROD, CMS, PGTLAB, KMTLAB, DMTLAB, MAXIMO, GBLFED );
/*Version 1 of the S6 Conductor AutoCalls Program*/
/*OPTIONS VALIDVARNAME=ANY;*/
%PUT Stage1: prepare something;
%UniformDelay240();

%PUT Assign MergeBy Macro Var from ProcSQLOrderBy;
%LET MergeBy=%SYSFUNC(compbl(%SYSFUNC(tranwrd(%QUOTE(&ProcSQLOrderBy),%STR(,),%STR( )))));

%LET ConCatString=;
%PUT &ConCatString;

%LET TableNameFinal = &TableName;
%PUT &TableNameFinal;

%IF %SYSFUNC(LENGTH(&TableName)) > 30 %THEN %DO;
	%PUT Table Name Too Long;
	%LET TableName = %SYSFUNC(SUBSTR(&TableName,1,30)); /*To prevent a table name that is too long*/
%END;
%ELSE %DO;
	%PUT TableName Okay;
%END;

libname dppublic base "/sasdata/DPPublic/" ;

%PUT Stage2: query data from ODS and merge to master table;
OPTIONS MPRINT;
%IF &PGT1MES = 1 %THEN %DO;	
	%LET PlantTableName = PGT1_&TableName;
	%PGT1ETL(&PlantTableName);
	%if %sysfunc(exist(work.&PlantTableName.)) %then %do;
		%LET ConCatString = &ConCatString work.&PlantTableName;
	%END;
	%ELSE %DO;
	%email(work, &PlantTableName, &email);
	%END;
	%END;

%PUT This to be Contact: &ConCatString;

%AppendDataTable(&TableName, &ConCatString);

%IF %LENGTH(&DaysAgoDTS) > 0 AND %LENGTH(&DaysAgoPlant) > %THEN %DO;
	%put going to calculate days ago;
	%DaysAgoCheck(&TableName, &DaysAgoDTS, &DaysAgoPlant)
%END;

/*SourceLib, SourceTab, TargetLib, TargeTab*/
%IF &DaysAgo > 0 %then %do;
	%put write copy back to disk;
	libname dppublic base "/sasdata/DPPublic/" ;
	%CheckResult(work, &TableName, DPPUBLIC, &TableName, &email);
%END;

cas mySession sessopts=(caslib=public timeout=1800 locale="en_US");
LIBNAME PUBLIC CAS CASLIB="PUBLIC";

%UniformDelay();

%CheckCASResult(work, &TableName, public, &TableName, &email);

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
	%PUT Try again to Write to LASR;
	%CheckResult(work, &TableName, lasrlib, &TableName, &email);
%END;	

/***************************/

%IF &PGT2MES = 1 %THEN %DO;	
	%LET PlantTableName = PGT2_&TableName;
	%PGT2ETL(&PlantTableName);
	%if %sysfunc(exist(work.&PlantTableName.)) %then %do;
		%LET ConCatString = &ConCatString work.&PlantTableName;
	%END;
	%ELSE %DO;
	%email(work, &PlantTableName, &email);
	%END;
	%END;
%IF &KMT1MES = 1 %THEN %DO;
	%LET PlantTableName = KMT1_&TableName;
	%KMT1ETL(&PlantTableName);
	%if %sysfunc(exist(work.&PlantTableName.)) %then %do;
		%LET ConCatString = &ConCatString work.&PlantTableName;
	%END;
	%ELSE %DO;
	%email(work, &PlantTableName, &email);
	%END;
	%END;
%IF &KMT2MES = 1 %THEN %DO;
	%LET PlantTableName = KMT2_&TableName;
	%KMT2ETL(&PlantTableName);
	%if %sysfunc(exist(work.&PlantTableName.)) %then %do;
		%LET ConCatString = &ConCatString work.&PlantTableName;
	%END;
	%ELSE %DO;
	%email(work, &PlantTableName, &email);
	%END;
	%END;
%IF &DMT1MES = 1 %THEN %DO;	
	%LET PlantTableName = DMT1_&TableName;
	%DMT1ETL(&PlantTableName);
	%if %sysfunc(exist(work.&PlantTableName.)) %then %do;
		%LET ConCatString = &ConCatString work.&PlantTableName;
	%END;
	%ELSE %DO;
	%email(work, &PlantTableName, &email);
	%END;
	%END;
%IF &DMT2MES = 1 %THEN %DO;	
	%LET PlantTableName = DMT2_&TableName;
	%DMT2ETL(&PlantTableName);
	%if %sysfunc(exist(work.&PlantTableName.)) %then %do;
		%LET ConCatString = &ConCatString work.&PlantTableName;
	%END;
	%ELSE %DO;
	%email(work, &PlantTableName, &email);
	%END;
	%END;
%IF &MFGODS = 1 %THEN %DO;	
	%LET PlantTableName = ODS_&TableName;
	%ODSETL(&PlantTableName);
	%if %sysfunc(exist(work.&PlantTableName.)) %then %do;
		%LET ConCatString = &ConCatString work.&PlantTableName;
	%END;
	%ELSE %DO;
	%email(work, &PlantTableName, &email);
	%END;
	%END;
%IF &EDW = 1 %THEN %DO;	
	%LET PlantTableName = EDW_&TableName;
	%EDWETL(&PlantTableName);
	%if %sysfunc(exist(work.&PlantTableName.)) %then %do;
		%LET ConCatString = &ConCatString work.&PlantTableName;
	%END;
	%ELSE %DO;
	%email(work, &PlantTableName, &email);
	%END;
	%END;
%IF &MAXIMO = 1 %THEN %DO;	
	%LET PlantTableName = MXP_&TableName;
	%MAXIMO(&PlantTableName);
	%if %sysfunc(exist(work.&PlantTableName.)) %then %do;
		%LET ConCatString = &ConCatString work.&PlantTableName;
	%END;
	%ELSE %DO;
	%email(work, &PlantTableName, &email);
	%END;
	%END;
%IF &CMS = 1 %THEN %DO;	
	%LET PlantTableName = CMS_&TableName;
	%CMSETL(&PlantTableName);
	%if %sysfunc(exist(work.&PlantTableName.)) %then %do;
		%LET ConCatString = &ConCatString work.&PlantTableName;
	%END;
	%ELSE %DO;
	%email(work, &PlantTableName, &email);
	%END;
	%END;
%IF &PGTLAB = 1 %THEN %DO;	
	%LET PlantTableName = PLAB_&TableName;
	%PGTLAB(&PlantTableName);
	%if %sysfunc(exist(work.&PlantTableName.)) %then %do;
		%LET ConCatString = &ConCatString work.&PlantTableName;
	%END;
	%ELSE %DO;
	%email(work, &PlantTableName, &email);
	%END;
	%END;
%IF &KMTLAB = 1 %THEN %DO;	
	%LET PlantTableName = KLAB_&TableName;
	%KMTLAB(&PlantTableName);
	%if %sysfunc(exist(work.&PlantTableName.)) %then %do;
		%LET ConCatString = &ConCatString work.&PlantTableName;
	%END;
	%ELSE %DO;
	%email(work, &PlantTableName, &email);
	%END;
	%END;
%IF &DMTLAB = 1 %THEN %DO;	
	%LET PlantTableName = DLAB_&TableName;
	%DMTLAB(&PlantTableName);
	%if %sysfunc(exist(work.&PlantTableName.)) %then %do;
		%LET ConCatString = &ConCatString work.&PlantTableName;
	%END;
	%ELSE %DO;
	%email(work, &PlantTableName, &email);
	%END;
	%END;
%IF &GBLFED = 1 %THEN %DO;	
	%LET PlantTableName = GFED_&TableName;
	%GlobalFed(&PlantTableName);
	%if %sysfunc(exist(work.&PlantTableName.)) %then %do;
		%LET ConCatString = &ConCatString work.&PlantTableName;
	%END;
	%ELSE %DO;
	%email(work, &PlantTableName, &email);
	%END;
	%END;





OPTIONS NOMPRINT;

LIBNAME PUBLIC CLEAR;

CAS mySession TERMINATE;

%TableSize();

LIBNAME _ALL_ CLEAR;

%mend;

