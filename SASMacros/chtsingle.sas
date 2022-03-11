%MACRO chtsingle(Lineage);
OPTIONS MAUTOSOURCE MINOPERATOR NOMPRINT validvarname=any NOFULLSTIMER nosource nosource2 NONOTES;
libname single '/sasdata/StudioTopLevel/Projects/S6_CHT_SingleTest';
libname cht '/sasdata/StudioTopLevel/Projects/S6_CHT_Single';

/* %let beltspeed=34.12000034; */

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

PROC SORT DATA=WORK.Target1;
BY Lineage StartDate EndDate Zone;
RUN;

PROC SQL;
CONNECT TO ODBC AS con4
    (DATASRC="EDW" authdomain=EDWAuth);

CREATE TABLE WORK.OvenBeltSpeed AS 
SELECT * FROM CONNECTION TO CON4 
(
SELECT DISTINCT
	SUBSTRING(EquipmentName, 1, 6) AS Lineage
	,BeltSpeedSetpoint AS BeltSpeed
	,ReadTime
FROM [mfg].[ProcessHistoryCdClOvenTransport]
WHERE SUBSTRING(EquipmentName, 1, 6)=%BQUOTE('&Lineage.') AND ReadTime > CONVERT(DATETIME, '2021-03-01' )
);
DISCONNECT FROM con4;
QUIT;

PROC SORT DATA=WORK.OvenBeltSpeed(WHERE=(BeltSpeed NE .));
BY BeltSpeed;
RUN;

DATA WORK.Speed;
SET WORK.OvenBeltSpeed;
BY BeltSpeed;
IF FIRST.BeltSpeed THEN OUTPUT;
RUN;

PROC SORT DATA=WORK.Speed;
BY DESCENDING ReadTime;
RUN;

%MACRO KMT;
%IF %SUBSTR(&Lineage., 1, 4)=KMT2 %THEN %DO;
DATA WORK.Speed(WHERE=(RN=1));
SET WORK.Speed;
RN=_N_;
RUN;
%END;
%ELSE %DO;
DATA WORK.Speed;
SET WORK.Speed;
RN=_N_;
RUN;
%END;
%MEND;
%KMT;

PROC SQL NOPRINT;
SELECT MAX(RN) INTO: RN FROM WORK.Speed;
SELECT BeltSpeedSetpoint INTO: PBS FROM SINGLE.ME_DEP_CHT_M8DataBad WHERE Lineage="&Lineage." AND ProfileID=1; QUIT;
%PUT &RN, &PBS;

%MACRO Profile;
%DO i=1 %TO &RN;
PROC SQL NOPRINT;
SELECT BeltSpeed INTO: CBS FROM WORK.Speed WHERE RN=&i.;
SELECT COUNT(DISTINCT OvenProfileID) INTO: NID FROM SINGLE.ME_DEP_CHT_M8DataBad WHERE Lineage="&Lineage." AND ABS(BeltSpeedSetpoint-&CBS)<0.001;
QUIT;
%PUT &CBS, &NID;
%IF &CBS=&PBS AND &NID>=5 %THEN %DO;
PROC SQL;
CREATE TABLE WORK.&lineage._ProfileData AS
SELECT *
FROM SINGLE.ME_DEP_CHT_M8DataBad
WHERE Lineage = "&Lineage." AND ABS(BeltSpeedSetpoint - &PBS) < 0.001;
QUIT;
%END;
%IF &NID<5 %THEN %DO;
%PUT Do Nothing;
%END;
%ELSE %DO;
PROC SQL;
CREATE TABLE WORK.&lineage._ProfileData AS
SELECT *
FROM SINGLE.ME_DEP_CHT_M8DataBad
WHERE Lineage = "&Lineage." AND ABS(BeltSpeedSetpoint - &CBS) < 0.001;
QUIT;
%END;
%END;
%MEND;
%Profile;

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
SELECT DISTINCT Lineage, OvenProfileID, ProfileID, StartTime, Zone, MEAN(&&position&j.._P) AS &&position&j.._P_Mean 
FROM WORK.&&position&j..tempfinalzone&i.&lineage
GROUP BY OvenProfileID
ORDER BY OvenProfileID;
QUIT;

PROC SQL;
CREATE TABLE WORK.Target_&&position&j.._Zone&i. AS
SELECT a.Lineage, a.OvenProfileID, a.ProfileID, a.Zone, a.&&position&j.._P_Mean, (a.&&position&j.._P_Mean - b.&&position&j..) AS &&position&j.._Delta
FROM WORK.&&position&j.._tempmeanzone&i.&lineage a
	LEFT JOIN WORK.Target1 b ON a.Lineage=b.Lineage AND a.Zone=b.Zone AND a.StartTime > b.StartDate AND a.StartTime < b.EndDate
ORDER BY a.Lineage, a.Zone;
QUIT;

PROC UNIVARIATE DATA=WORK.Target_&&position&j.._Zone&i. NOPRINT;
VAR &&position&j.._Delta;
OUTPUT OUT=&&position&j..stats&i.&lineage MEDIAN=median QRANGE=Qrange;
RUN;

DATA WORK.&&position&j.._tempmeanzone&i.&lineage(drop=median Qrange);
if _n_=1 then set &&position&j..stats&i.&lineage;
set WORK.Target_&&position&j.._Zone&i.;
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
select distinct c.Lineage, a.*,b.&&position&j.._P_Mean, c.ProfileID from T2&&position&j..his_tempfinalzone&i.&lineage a 
inner join WORK.&&position&j.._tempmeanzone&i.&lineage b on a.OvenProfileID=b.OvenProfileID
inner join WORK.&lineage._ProfileData c on a.OvenProfileID=c.OvenProfileID;
quit;

PROC SQL;
CREATE TABLE WORK.&&position&j.._Zone&i._Test AS
SELECT *, 'Test' AS _ROLE_ LENGTH=15 FROM WORK.&&position&j..examplezone&i.&lineage
GROUP BY Lineage
HAVING ProfileID = MIN(ProfileID)
ORDER BY ProfileID;
QUIT;

PROC SQL;
CREATE TABLE WORK.&&position&j.._Zone&i._TV AS
SELECT * FROM WORK.&&position&j..examplezone&i.&lineage
GROUP BY Lineage
HAVING ProfileID > MIN(ProfileID)
ORDER BY ProfileID;
QUIT;

DATA WORK.&&position&j.._Zone&i._TV;
SET WORK.&&position&j.._Zone&i._TV(DROP=Lineage OvenProfileID ProfileID);
RUN;

DATA WORK.&&position&j.._Zone&i._Test;
SET WORK.&&position&j.._Zone&i._Test(DROP=Lineage OvenProfileID ProfileID);
RUN;

%end;
%end;
%mend;

%cleandata;

%macro model;
%let m1 = STEPWISE (STOP=CV);
%let m2 = LASSO (STOP=CV);
%let m3 = FORWARD (STOP=CV);
%do j=1 %to 5;
%do i=1 %to 22;
%do k=1 %to 3;

proc glmselect data=WORK.&&position&j.._Zone&i._TV testdata=WORK.&&position&j.._Zone&i._Test maxmacro=500;
model &&position&j.._P_Mean = col1-col%sysevalf(12*&i-2) / selection=&&m&k. cvmethod=random(5);
store out=cht.&lineage._&&position&j.._zone&i._m&k.;
OUTPUT OUT=WORK.m&k._PData_&&position&j.._Zone&i. Pred Resid;
ODS OUTPUT ParameterEstimates=WORK.m&k._ParamEst_&&position&j.._Zone&i. FitStatistics=WORK.m&k._FitStats_&&position&j.._Zone&i. NObs=WORK.m&k._Nobs_&&position&j.._Zone&i.; 
run;

DATA WORK.m&k._PData_&&position&j.._Zone&i.;
RETAIN Date Lineage Zone 'TC Location'n Method &&position&j.._P_Mean p_&&position&j.._P_Mean r_&&position&j.._P_Mean;
SET WORK.m&k._PData_&&position&j.._Zone&i.(KEEP=&&position&j.._P_Mean p_&&position&j.._P_Mean r_&&position&j.._P_Mean);
FORMAT Date DDMMYY9.;
LENGTH Lineage $ 6. Zone 3. 'TC Location'n $ 2. Method $ 20.;
Date = TODAY();
Lineage = "&Lineage.";
Zone = "&i.";
'TC Location'n = "&&position&j..";
Method = SUBSTR("&&m&k.", 1, LENGTH("&&m&k.")-10);
RUN;

DATA WORK.m&k._ParamEst_&&position&j.._Zone&i.;
RETAIN Date Lineage Zone 'TC Location'n Method Parameter Estimate StdErr tValue Probt;
SET WORK.m&k._ParamEst_&&position&j.._Zone&i.(DROP=Effect DF StandardizedEst);
FORMAT Date DDMMYY9.;
LENGTH Lineage $ 6. Zone 3. 'TC Location'n $ 2. Method $ 20.;
Date = TODAY();
Lineage = "&Lineage.";
Zone = "&i.";
'TC Location'n = "&&position&j..";
Method = SUBSTR("&&m&k.", 1, LENGTH("&&m&k.")-10);
RUN;

DATA WORK.m&k._Nobs_&&position&j.._Zone&i.(KEEP=Date Lineage Zone 'TC Location'n Method DataSource N NObsRead NObsUsed NObsTraining NObsValidation);
RETAIN Date Lineage Zone 'TC Location'n Method DataSource N NObsRead NObsUsed NObsTraining NObsValidation;
SET WORK.m&k._Nobs_&&position&j.._Zone&i.
	(WHERE = (
		(DataSource = 'Analysis' AND Label = 'Number of Observations Read')
		OR
		(DataSource = 'Test' AND Label = 'Number of Observations Read')
			)
	);
FORMAT Date DDMMYY9.;
LENGTH Lineage $ 6. Zone 3. 'TC Location'n $ 2. Mothod $ 15.;
Date = TODAY();
Lineage = "&Lineage.";
Zone = "&i.";
'TC Location'n = "&&position&j..";
Method = SUBSTR("&&m&k.", 1, LENGTH("&&m&k.")-10);
RUN;

DATA WORK.m&k._FitStats_&&position&j.._Zone&i.;
RETAIN Date Lineage Zone 'TC Location'n Method Stats Value RN;
SET WORK.m&k._FitStats_&&position&j.._Zone&i.(DROP=cValue1);
FORMAT Date DDMMYY9.;
LENGTH Lineage $ 6. Zone 3. 'TC Location'n $ 2. Method $ 20.;
Date = TODAY();
Lineage = "&Lineage.";
Zone = "&i.";
'TC Location'n = "&&position&j..";
Method = SUBSTR("&&m&k.", 1, LENGTH("&&m&k.")-10);
RENAME Label1 = Stats nValue1 = Value;
RN = "&k.";
WHERE Label1 = 'Adj R-Sq';
RUN;

%END;

DATA WORK.PData_&&position&j.._Zone&i.;
SET WORK.M1_PData_&&position&j.._Zone&i. WORK.M2_PData_&&position&j.._Zone&i. WORK.M3_PData_&&position&j.._Zone&i.;
RUN;

DATA WORK.ParamEst_&&position&j.._Zone&i.;
SET WORK.M1_ParamEst_&&position&j.._Zone&i. WORK.M2_ParamEst_&&position&j.._Zone&i. WORK.M3_ParamEst_&&position&j.._Zone&i.;
RUN;

DATA WORK.Nobs_&&position&j.._Zone&i.;
SET WORK.M1_Nobs_&&position&j.._Zone&i. WORK.M2_Nobs_&&position&j.._Zone&i. WORK.M3_Nobs_&&position&j.._Zone&i.;
RUN;

DATA WORK.FitStats_&&position&j.._Zone&i.;
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
SELECT Date, Lineage, Zone, 'TC Location'n, Method, RN, Stats, Value
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
SELECT COMPRESS(CATX('_', LOWCASE(Lineage), LOWCASE('TC Location'n), CATX('', 'zone', Zone), CATX('', 'm', RN, '.sas7bitm'))) INTO: Model1 FROM WORK.DELETEModel WHERE Row = 1; QUIT;
%PUT &Model1;
%CHECK(/sasdata/StudioTopLevel/Projects/S6_CHT_Single/&Model1.);

PROC SQL;
SELECT COMPRESS(CATX('_', LOWCASE(Lineage), LOWCASE('TC Location'n), CATX('', 'zone', Zone), CATX('', 'm', RN, '.sas7bitm'))) INTO: Model2 FROM WORK.DELETEModel WHERE Row = 2; QUIT;
%PUT &Model2;
%CHECK(/sasdata/StudioTopLevel/Projects/S6_CHT_Single/&Model2.);

%END;

DATA WORK.PData_&&position&j..;
SET WORK.PData_&&position&j.._Zone:;
RUN;

DATA WORK.ParamEst_&&position&j..;
SET WORK.ParamEst_&&position&j.._Zone:;
RUN;

DATA WORK.Nobs_&&position&j..;
SET WORK.Nobs_&&position&j.._Zone:;
RUN;

DATA WORK.FitStats_&&position&j..(KEEP=Date Lineage Zone 'TC Location'n Method Stats Value);
SET WORK.FSMax1_&&position&j.._Zone:;
BY Date Lineage Zone 'TC Location'n;
RUN;

%END;

DATA cht.&Lineage._Single_FitStats;
SET WORK.FitStats_LL WORK.FitStats_L WORK.FitStats_C WORK.FitStats_R WORK.FitStats_RR;
BY Date Lineage Zone 'TC Location'n;
RUN;

DATA WORK.&Lineage._Single_ParamEst;
SET WORK.ParamEst_LL WORK.ParamEst_L WORK.ParamEst_C WORK.ParamEst_R WORK.ParamEst_RR;
RUN;

PROC SQL;
CREATE TABLE cht.&Lineage._Single_ParamEst AS
SELECT a.Date, a.Lineage, a.Zone, a.'TC Location'n, a.Method, b.Parameter, b.Estimate, b.StdErr, b.tValue, b.Probt
FROM cht.&Lineage._Single_FitStats a
	LEFT JOIN WORK.&Lineage._Single_ParamEst b ON a.Lineage = b.Lineage AND a.Zone = b.Zone AND a.'TC Location'n = b.'TC Location'n AND a.Method = b.Method
ORDER BY Date, Lineage, Zone, 'TC Location'n;
QUIT;

DATA WORK.&Lineage._Single_Nobs;
SET WORK.Nobs_LL WORK.Nobs_L WORK.Nobs_C WORK.Nobs_R WORK.Nobs_RR;
RUN;

PROC SQL;
CREATE TABLE cht.&Lineage._Single_Nobs AS
SELECT a.Date, a.Lineage, a.Zone, a.'TC Location'n, a.Method, b.DataSource, b.N, b.NObsRead, b.NObsUsed, b.NObsTraining, b.NObsValidation
FROM cht.&Lineage._Single_FitStats a
	LEFT JOIN WORK.&Lineage._Single_Nobs b ON a.Lineage = b.Lineage AND a.Zone = b.Zone AND a.'TC Location'n = b.'TC Location'n AND a.Method = b.Method
ORDER BY Date, Lineage, Zone, 'TC Location'n;
QUIT;

PROC SQL;
CREATE TABLE cht.&Lineage._Single_LL AS
SELECT a.Date, a.Lineage, a.Zone, a.'TC Location'n, a.Method, b.LL_P_Mean, b.p_LL_P_Mean, b.r_LL_P_Mean
FROM cht.&Lineage._Single_FitStats a
	LEFT JOIN WORK.PData_LL b ON a.Lineage = b.Lineage AND a.Zone = b.Zone AND a.'TC Location'n = b.'TC Location'n AND a.Method = b.Method
WHERE a.'TC Location'n = 'LL'
ORDER BY a.Date, a.Lineage, a.Zone, a.'TC Location'n;
QUIT;

PROC SQL;
CREATE TABLE cht.&Lineage._Single_L AS
SELECT a.Date, a.Lineage, a.Zone, a.'TC Location'n, a.Method, b.L_P_Mean, b.p_L_P_Mean, b.r_L_P_Mean
FROM cht.&Lineage._Single_FitStats a
	LEFT JOIN WORK.PData_L b ON a.Lineage = b.Lineage AND a.Zone = b.Zone AND a.'TC Location'n = b.'TC Location'n AND a.Method = b.Method
WHERE a.'TC Location'n = 'L'
ORDER BY a.Date, a.Lineage, a.Zone, a.'TC Location'n;
QUIT;

PROC SQL;
CREATE TABLE cht.&Lineage._Single_C AS
SELECT a.Date, a.Lineage, a.Zone, a.'TC Location'n, a.Method, b.C_P_Mean, b.p_C_P_Mean, b.r_C_P_Mean
FROM cht.&Lineage._Single_FitStats a
	LEFT JOIN WORK.PData_C b ON a.Lineage = b.Lineage AND a.Zone = b.Zone AND a.'TC Location'n = b.'TC Location'n AND a.Method = b.Method
WHERE a.'TC Location'n = 'C'
ORDER BY a.Date, a.Lineage, a.Zone, a.'TC Location'n;
QUIT;

PROC SQL;
CREATE TABLE cht.&Lineage._Single_R AS
SELECT a.Date, a.Lineage, a.Zone, a.'TC Location'n, a.Method, b.R_P_Mean, b.p_R_P_Mean, b.r_R_P_Mean
FROM cht.&Lineage._Single_FitStats a
	LEFT JOIN WORK.PData_R b ON a.Lineage = b.Lineage AND a.Zone = b.Zone AND a.'TC Location'n = b.'TC Location'n AND a.Method = b.Method
WHERE a.'TC Location'n = 'R'
ORDER BY a.Date, a.Lineage, a.Zone, a.'TC Location'n;
QUIT;

PROC SQL;
CREATE TABLE cht.&Lineage._Single_RR AS
SELECT a.Date, a.Lineage, a.Zone, a.'TC Location'n, a.Method, b.RR_P_Mean, b.p_RR_P_Mean, b.r_RR_P_Mean
FROM cht.&Lineage._Single_FitStats a
	LEFT JOIN WORK.PData_RR b ON a.Lineage = b.Lineage AND a.Zone = b.Zone AND a.'TC Location'n = b.'TC Location'n AND a.Method = b.Method
WHERE a.'TC Location'n = 'RR'
ORDER BY a.Date, a.Lineage, a.Zone, a.'TC Location'n;
QUIT;

%mend;

%model;

%MEND;