%macro etlexec(Plant=,TableName=,dsn=,multitask=0);

OPTIONS VALIDVARNAME=ANY MAUTOSOURCE nonotes nosource;
%IF &dsn. = EDW %THEN %DO;
	%let authdomain=EDWAuth;
%END;
%ELSE %DO;
	%LET authdomain=SQLGRP_Temp_Reader_Auth;
%END;

%Timing(action=2,Process="&plant.",Category='ETL',Task="Connection Established");

PROC SQL NOPRINT;
   CONNECT TO ODBC as con2
    (
    DATASRC="&dsn." 
    authdomain=&authdomain
    );

	CREATE TABLE workdir.&TableName    AS 
	SELECT * FROM CONNECTION TO con2 
	(
	&SQLExtraction.
	);
	DISCONNECT FROM con2;
QUIT;

%Timing(action=2,Process="&plant.",Category='ETL',Task="Query Time");

PROC SORT data=workdir.&TableName;
	BY &MergeBy;
RUN;

%Timing(action=2,Process="&plant.",Category='ETL',Task="DataSet Sort");

%mend;

