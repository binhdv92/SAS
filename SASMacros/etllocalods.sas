%macro etllocalods(Plant,TableName);
%let _start = %sysfunc(datetime());
%let NOTES = %sysfunc(getoption(notes));
options nonotes;
PROC SQL;
   CONNECT TO ODBC as con2
    (
    DATASRC="&plant.MESODS" 
    authdomain=SQLGRP_Temp_Reader_Auth
    );

CREATE TABLE workdir.&TableName    AS 
SELECT * FROM CONNECTION TO con2 
(
&SQLExtraction.
);
DISCONNECT FROM con2;
QUIT;

PROC SORT data=workdir.&TableName;
BY &MergeBy;
RUN;

data _null_;
   dur = datetime() - &_start;
   put 34*'-' / "&Plant. TOTAL DURATION:" dur time13.2 / 34*'-';
run;

options &notes;
%mend;

