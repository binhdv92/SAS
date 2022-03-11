%MACRO chtsingletest(Lineage);
OPTIONS MINOPERATOR NOMPRINT validvarname=any NOFULLSTIMER nosource nosource2 NONOTES;
options mautosource;
libname single '/sasdata/StudioTopLevel/Projects/S6_CHT_SingleTest';
options cashost="azr1sas01s110.fs.local" casport=5570;
cas myAdhocSession sessopts=(timeout=1800);    /* 30 minute timeout */
LIBNAME PUBLIC CAS CASLIB="PUBLIC";

%macro check(file);
%if %sysfunc(fileexist(&file)) ge 1 %then %do;
   %let rc=%sysfunc(filename(temp,&file));
   %let rc=%sysfunc(fdelete(&temp));
%end; 
%else %put The file &file does not exist;
%mend check;

PROC SQL;
CONNECT TO ODBC AS CON3
(DATASRC= 'PGT1MESODS' AUTHDOMAIN=SQLGRP_Temp_Reader_Auth);

CREATE TABLE WORK.Target AS
SELECT * FROM CONNECTION TO CON3
(
SELECT *
	FROM sas.CHTTechnology a
	LEFT JOIN sas.cfgCHTTargets b on a.Technology = b.Technology and a.targetsoak = b.targetsoak
	WHERE Lineage=%BQUOTE('&Lineage.')
ORDER BY Lineage
);
DISCONNECT FROM CON3;
QUIT;

DATA Target1;
SET Target;
LL=LL_RR;
L=L_R;
R=L_R;
RR=LL_RR;
KEEP Lineage Zone LL L C R RR UCL LCL StartDate EndDate;
RUN;

PROC SQL;
CREATE TABLE WORK.&lineage._ProfileData AS
SELECT *
FROM SINGLE.ME_DEP_CHT_M8DataBad
WHERE Lineage = "&Lineage.";
QUIT;

PROC SQL;
CREATE TABLE WORK.tempzoneid&lineage AS
SELECT DISTINCT
	Lineage
	,OvenProfileID
	,TSG
	,Zone
	,MEAN(AVG_LW) AS LW
	,MEAN(AVG_LETop) AS LETop
	,MEAN(AVG_LEBottom) AS LEBot
	,MEAN(AVG_LTop) AS LTop
	,MEAN(AVG_LBottom) AS LBot
	,MEAN(AVG_CTop) AS CTop
	,MEAN(AVG_CBottom) AS CBot
	,MEAN(AVG_RTop) AS RTop
	,MEAN(AVG_RBottom) AS RBot
	,MEAN(AVG_RETop) AS RETop
	,MEAN(AVG_REBottom) AS REBot
	,MEAN(AVG_RW) AS RW
FROM WORK.&lineage._ProfileData
GROUP BY OvenProfileID, Zone;
QUIT;

DATA WORK.oventemp_complete&lineage;
SET WORK.tempzoneid&lineage;
RUN;

PROC SORT DATA=WORK.&lineage._ProfileData;
BY OvenProfileID Zone ReadTime_P;
RUN;

DATA WORK.plate_stable_status&lineage;
SET WORK.&lineage._ProfileData;
BY OvenProfileID Zone ReadTime_P;
Diff_LL_P = LL_P - LAG(LL_P);
Diff_L_P = L_P-LAG(L_P);
Diff_C_P = C_P-LAG(C_P);
Diff_R_P = R_P-LAG(R_P);
Diff_RR_P = RR_P-LAG(RR_P);
IF _n_ = 1 OR Zone ^= LAG(Zone) OR OvenProfileID ^= LAG(OvenProfileID) THEN DO;
Diff_LL_P = 0;
Diff_L_P = 0;
Diff_C_P = 0;
Diff_R_P = 0;
Diff_RR_P = 0;
END;
RUN; 

PROC SQL;
CREATE TABLE WORK.plate_unstable_max&lineage AS
SELECT DISTINCT
	Lineage
	,OvenProfileID
	,BeltSpeedSetpoint
	,Zone
	,MAX(ABS(Diff_LL_P)) AS Max_Diff_LL_P
	,MAX(ABS(Diff_L_P)) AS Max_Diff_L_P
	,MAX(ABS(Diff_C_P)) AS Max_Diff_C_P
	,MAX(ABS(Diff_R_P)) AS Max_Diff_R_P
	,MAX(ABS(Diff_RR_P)) AS Max_Diff_RR_P
FROM WORK.plate_stable_status&lineage
GROUP BY OvenProfileID, Zone;
QUIT;

%LET position1=LL;
%LET position2=L;
%LET position3=C;
%LET position4=R;
%LET position5=RR;

%MACRO CleanData;
%DO j=1 %TO 5;
%DO i=1 %TO 22;
%GLOBAL paramlist&&position&j..&i.&lineage; 
PROC SQL NOPRINT;
SELECT OvenProfileID INTO: paramlist&&position&j..&i.&lineage SEPARATED BY ','
FROM WORK.plate_unstable_max&lineage WHERE Zone = &i AND Max_Diff_&&position&j.._P < 2.5;
QUIT;

PROC SORT DATA=WORK.&lineage._ProfileData OUT=WORK.&&position&j..tempfinalzone&i.&lineage;
WHERE Zone = &i AND OvenProfileID IN (&&&&paramlist&&position&j..&i.&lineage);
BY OvenProfileID ReadTime_P;
RUN;

PROC SQL NOPRINT;
CREATE TABLE WORK.&&position&j.._tempmeanzone&i.&lineage AS 
SELECT DISTINCT
	Lineage
	,OvenProfileID
	,ProfileID
	,StartTime
	,Zone
	,MEAN(&&position&j.._P) AS &&position&j.._P_Mean 
FROM WORK.&&position&j..tempfinalzone&i.&lineage
GROUP BY OvenProfileID;
QUIT;

PROC SQL;
CREATE TABLE WORK.Target_&&position&j.._Zone&i.&lineage AS
SELECT a.Lineage, a.OvenProfileID, a.ProfileID, a.Zone, a.&&position&j.._P_Mean, (a.&&position&j.._P_Mean-b.&&position&j..) AS &&position&j.._Delta
FROM WORK.&&position&j.._tempmeanzone&i.&lineage a
	LEFT JOIN WORK.Target1 b ON a.Lineage=b.Lineage AND a.Zone=b.Zone AND a.StartTime > b.StartDate AND a.StartTime < b.EndDate
ORDER BY a.Lineage, a.Zone;
QUIT;

PROC UNIVARIATE DATA=WORK.Target_&&position&j.._Zone&i.&lineage NOPRINT;
VAR &&position&j.._Delta;
OUTPUT OUT=&&position&j..stats&i.&lineage MEDIAN=median QRANGE=Qrange;
RUN;

DATA WORK.&&position&j.._tempmeanzone&i.&lineage(drop=median Qrange);
if _n_=1 then set &&position&j..stats&i.&lineage;
set WORK.Target_&&position&j.._Zone&i.&lineage;
if &&position&j.._Delta < median - 2*Qrange or &&position&j.._Delta > median + 2*Qrange then delete; 
run;

data &&position&j..his_tempfinalzone&i.&lineage; 
set oventemp_complete&lineage;
where zone<=&i;
run;

proc transpose data=&&position&j..his_tempfinalzone&i.&lineage out=T1&&position&j..his_tempfinalzone&i.&lineage;
by OvenProfileID zone;
var lw letop lebot ltop lbot ctop cbot rtop rbot retop rebot rw;
run;

DATA T1&&position&j..his_tempfinalzone&i.&lineage(DROP=Zone _NAME_);
SET T1&&position&j..his_tempfinalzone&i.&lineage;
IF Zone=1 AND _NAME_ IN ('LW', 'RW') THEN DELETE;
RUN;

proc transpose data=T1&&position&j..his_tempfinalzone&i.&lineage out=T2&&position&j..his_tempfinalzone&i.&lineage(drop=_name_);
by OvenProfileID;
var col1;
run;

proc sql;
create table &&position&j..examplezone&i.&lineage as
select distinct a.*,b.&&position&j.._P_Mean, c.ProfileID
from T2&&position&j..his_tempfinalzone&i.&lineage a 
inner join WORK.&&position&j.._tempmeanzone&i.&lineage b on a.OvenProfileID=b.OvenProfileID
inner join WORK.&lineage._ProfileData c on a.OvenProfileID=c.OvenProfileID;
quit;

PROC SQL;
CREATE TABLE WORK.&&position&j.._Zone&i._Test AS
SELECT *, 'Test' AS ROLE LENGTH=8 FROM WORK.&&position&j..examplezone&i.&lineage
WHERE ProfileID = 1
ORDER BY ProfileID;
QUIT;

/*PROC SQL;
CREATE TABLE WORK.&&position&j.._Zone&i._TV AS
SELECT * FROM WORK.&&position&j..examplezone&i.&lineage
WHERE ProfileID >= 2
ORDER BY ProfileID;
QUIT;*/

DATA WORK.&&position&j.._Zone&i._TV;
SET WORK.&&position&j..examplezone&i.&lineage;
N=RANUNI(8);
RUN;

PROC SORT DATA=WORK.&&position&j.._Zone&i._TV;
BY N;
RUN;

DATA WORK.&&position&j.._Zone&i._Train WORK.&&position&j.._Zone&i._VALID;
SET WORK.&&position&j.._Zone&i._TV NOBS=NOBS;
IF _N_<=0.7*NOBS THEN OUTPUT WORK.&&position&j.._Zone&i._TRAIN;
ELSE OUTPUT WORK.&&position&j.._Zone&i._VALID;
RUN;

DATA WORK.&&position&j.._Zone&i._Train;
SET WORK.&&position&j.._Zone&i._Train(DROP=ProfileID OvenProfileID N);
LENGTH Role $ 8.;
Role='TRAIN';
RUN;

DATA WORK.&&position&j.._Zone&i._VALID;
SET WORK.&&position&j.._Zone&i._VALID(DROP=OvenProfileID ProfileID N);
LENGTH Role $ 8.;
Role='VAL';
RUN;

DATA WORK.&&position&j.._Zone&i.;
SET WORK.&&position&j.._Zone&i._Train WORK.&&position&j.._Zone&i._VALID WORK.&&position&j.._Zone&i._Test(DROP=OvenProfileID ProfileID);
RUN;

%end;
%end;
%mend;

%CleanData;

%MACRO Model;
%let m1 = STEPWISE;
%let m2 = LASSO;
%let m3 = FORWARD;
%do j=1 %to 5;
%do i=1 %to 22;
%do k=1 %to 3;

DATA PUBLIC.&&position&j.._Zone&i.;
SET WORK.&&position&j.._Zone&i.;
RUN;

filename pScore "/sasdata/StudioTopLevel/Projects/S6_CHT_SingleTest/&lineage._&&position&j.._zone&i._m&k..sas";

PROC REGSELECT DATA=PUBLIC.&&position&j.._Zone&i.;
MODEL &&position&j.._P_Mean = col1-col%SYSEVALF(12*&i-2) @2;
PARTITION ROLEVAR=ROLE(TRAIN='TRAIN' VALIDATE='VAL' TEST='TEST');
SELECTION METHOD=&&m&k.(CHOOSE=VALIDATE);
ODS OUTPUT ParameterEstimates=WORK.m&k._ParamEst_&&position&j.._Zone&i. FitStatistics=WORK.m&k._FitStats_&&position&j.._Zone&i.; 
CODE FILE=pScore;
RUN;

DATA WORK.m&k._ParamEst_&&position&j.._Zone&i.;
RETAIN Date Lineage Zone 'TC Location'n Method Parameter Estimate;
SET WORK.m&k._ParamEst_&&position&j.._Zone&i.(DROP=Effect DF);
FORMAT Date DDMMYY9.;
LENGTH Lineage $ 6. Zone 3. 'TC Location'n $ 2. Method $ 10.;
Date = TODAY();
Lineage = "&Lineage.";
Zone = "&i.";
'TC Location'n = "&&position&j..";
Method = "&&m&k.";
RUN;

DATA WORK.m&k._FitStats_&&position&j.._Zone&i.;
RETAIN Date Lineage Zone 'TC Location'n Method Stats Value RN;
SET WORK.m&k._FitStats_&&position&j.._Zone&i.(DROP=RowId);
FORMAT Date DDMMYY9.;
LENGTH Lineage $ 6. Zone 3. 'TC Location'n $ 2. Method $ 10.;
Date = TODAY();
Lineage = "&Lineage.";
Zone = "&i.";
'TC Location'n = "&&position&j..";
Method = "&&m&k.";
RENAME Description = Stats;
RN = "&k.";
WHERE Description = 'Adj R-Sq';
RUN;

%END;

DATA WORK.ParamEst_&&position&j.._Zone&i.;
SET WORK.M1_ParamEst_&&position&j.._Zone&i. WORK.M2_ParamEst_&&position&j.._Zone&i. WORK.M3_ParamEst_&&position&j.._Zone&i.;
RUN;

DATA WORK.FitStats_&&position&j.._Zone&i.;
RETAIN Date Lineage Zone 'TC Location'n Method Stats Value RN;
SET WORK.M1_FitStats_&&position&j.._Zone&i. WORK.M2_FitStats_&&position&j.._Zone&i. WORK.M3_FitStats_&&position&j.._Zone&i.;
RUN;

PROC SQL;
CREATE TABLE WORK.FSMax_&&position&j.._Zone&i. AS
SELECT Date, Lineage, Zone, 'TC Location'n, Method, RN, Stats, Value
FROM WORK.FitStats_&&position&j.._Zone&i.
GROUP BY Date, Lineage, Zone, 'TC Location'n
HAVING Value = MAX(Value)
ORDER BY Date, Lineage, Zone, 'TC Location'n, Method, RN;
QUIT;

PROC SQL;
CREATE TABLE WORK.FSMax1_&&position&j.._Zone&i. AS
SELECT Date, Lineage, Zone, 'TC Location'n, Method, Stats, Value, RN
FROM WORK.FSMax_&&position&j.._Zone&i.
GROUP BY Date, Lineage, Zone, 'TC Location'n
HAVING RN = MAX(RN)
ORDER BY Date, Lineage, Zone, 'TC Location'n;
QUIT;

PROC SQL;
CREATE TABLE WORK.DELETEModel AS
SELECT * FROM WORK.FitStats_&&position&j.._Zone&i.
EXCEPT
SELECT * FROM WORK.FSMax1_&&position&j.._Zone&i.;
QUIT;

DATA WORK.DELETEModel;
SET WORK.DELETEModel;
Row = _N_;
RUN;

PROC SQL;
SELECT COMPRESS(CATX('_', UPCASE(Lineage), UPCASE('TC Location'n), CATX('', 'zone', Zone), CATX('', 'm', RN, '.sas'))) INTO: Model1 FROM WORK.DELETEModel WHERE Row = 1; QUIT;
%PUT &Model1;
%CHECK(/sasdata/StudioTopLevel/Projects/S6_CHT_SingleTest/&Model1.);

PROC SQL;
SELECT COMPRESS(CATX('_', UPCASE(Lineage), UPCASE('TC Location'n), CATX('', 'zone', Zone), CATX('', 'm', RN, '.sas'))) INTO: Model2 FROM WORK.DELETEModel WHERE Row = 2; QUIT;
%PUT &Model2;
%CHECK(/sasdata/StudioTopLevel/Projects/S6_CHT_SingleTest/&Model2.);

%END;

DATA WORK.ParamEst_&&position&j..;
SET WORK.ParamEst_&&position&j.._Zone:;
RUN;

DATA WORK.FitStats_&&position&j..(KEEP=Date Lineage Zone 'TC Location'n Method Stats Value);
SET WORK.FSMax1_&&position&j.._Zone:;
BY Date Lineage Zone 'TC Location'n;
RUN;

%END;

DATA single.&Lineage._Single_FitStats;
SET WORK.FitStats_LL WORK.FitStats_L WORK.FitStats_C WORK.FitStats_R WORK.FitStats_RR;
BY Date Lineage Zone 'TC Location'n;
RUN;

DATA WORK.&Lineage._SingleTh_ParamEst;
SET WORK.ParamEst_LL WORK.ParamEst_L WORK.ParamEst_C WORK.ParamEst_R WORK.ParamEst_RR;
RUN;

PROC SQL;
CREATE TABLE single.&Lineage._Single_ParamEst AS
SELECT a.Date, a.Lineage, a.Zone, a.'TC Location'n, a.Method, b.Parameter, b.Estimate, b.StdErr, b.tValue, b.Probt
FROM single.&Lineage._Single_FitStats a
	LEFT JOIN WORK.&Lineage._SingleTh_ParamEst b ON a.Lineage = b.Lineage AND a.Zone = b.Zone AND a.'TC Location'n = b.'TC Location'n AND a.Method = b.Method
ORDER BY Date, Lineage, Zone, 'TC Location'n;
QUIT;

%MEND;

%model;

cas myAdhocSession disconnect;

%MEND;