%MACRO modeltest_summary();
OPTIONS VALIDVARNAME=ANY MAUTOSOURCE;
CAS mySession SESSOPTS=(CASLIB=PUBLIC TIMEOUT=1800 LOCALE="en_US");
LIBNAME PUBLIC CAS CASLIB="PUBLIC";

PROC SQL;
CREATE TABLE WORK.SummaryCombination AS
SELECT *, . AS 'Number of training'n FROM PUBLIC.MultiTest_MultiSummary
UNION
SELECT * FROM PUBLIC.SingleTest_SingleSummary
UNION
SELECT * FROM PUBLIC.SingleThinTest_Summary
UNION
SELECT * FROM PUBLIC.TBSTest_TBSSummary;

/*Select the best model for each of the lineages following the creterial below:
Firstly, compare the skip decision of three models and select the model with Yes*/
CREATE TABLE WORK.Comparison_ModelPerformance AS
SELECT *
FROM WORK.SummaryCombination
WHERE ModelPerformance = 'Good';

DELETE FROM WORK.SummaryCombination
WHERE Lineage IN (SELECT Lineage FROM WORK.Comparison_ModelPerformance) OR Ramp = .;

CREATE TABLE WORK.UNION AS
SELECT *, (Ramp + Soak) AS Score FROM WORK.SummaryCombination
UNION
SELECT *, (Ramp + Soak) AS Score FROM WORK.Comparison_ModelPerformance;

CREATE TABLE WORK.Comparison_ModelHealth AS
SELECT * FROM WORK.UNION
WHERE Model_Health = 'Yes';

DELETE FROM WORK.UNION
WHERE Lineage IN (SELECT Lineage FROM WORK.Comparison_ModelHealth);

CREATE TABLE WORK.UNION1 AS
SELECT * FROM WORK.UNION
UNION
SELECT * FROM WORK.Comparison_ModelHealth;

/*If there are more than one model left for any lineages after the first creterial, move on to the second one.
Firstly, compare the score of three models and select the model with minimum score*/
CREATE TABLE WORK.Comparison_Score AS
SELECT *
FROM WORK.UNION1
GROUP BY Lineage
HAVING Score = MIN(Score);

/*If there are more than one model left for any lineages after the second creterial, move on to the third one.
Secondly, compare the Warning_number(renamed Oven_Instability in the reports) of three models and select the model with minimum Warning_number*/
CREATE TABLE WORK.Comparison_Oven AS
SELECT *
FROM WORK.Comparison_Score
GROUP BY Lineage
HAVING Oven_Instability = MIN(Oven_Instability);

/*If there are more than one model left for any lineages after the previous creterial, move on to the last one.
Thirdly, compare the F_number(renamed VP_Fail in the reports) of three models and select the model with minimum F_number*/
CREATE TABLE WORK.Comparison_VP AS
SELECT *
FROM WORK.Comparison_Oven
GROUP BY Lineage
HAVING VP_Fail = MIN(VP_Fail);

/*If it happens to any lineages that 2 or 3 models are both good, select the "simpler" model having fewer predicator variables as default.*/
CREATE TABLE WORK.RN AS
SELECT *,
(CASE WHEN Model = 'Single' THEN 1
	WHEN Model = 'Single_ThinGls' THEN 2
	WHEN Model = 'Multi' THEN 3
	ELSE 4
 END) AS RN
FROM WORK.Comparison_VP;

CREATE TABLE WORK.ModelSelection AS
SELECT *
FROM WORK.RN
GROUP BY Lineage
HAVING RN = MIN(RN);

CREATE TABLE WORK.M8Profiles AS
SELECT EquipmentID
		,NextProfileSkipable
		,RN
FROM PUBLIC.ME_DEP_CHT_M8Profiles
WHERE RN = 1;

CREATE TABLE WORK.M8TARGETS AS
SELECT Lineage 
	   ,Technology
       ,TargetSoak
	   ,RN
FROM PUBLIC.ME_DEP_CHT_M8TARGETS
WHERE RN = 1;

CREATE TABLE WORK.ModelSummary AS
SELECT 
	a.*
	,b.NextProfileSkipable
	,c.Technology
	,c.TargetSoak
FROM WORK.ModelSelection(DROP=ModelPerformance RN) a
	 LEFT JOIN WORK.M8Profiles b ON a.Lineage = SUBSTR(b.EquipmentID, 1, 6)
     LEFT JOIN WORK.M8TARGETS c ON a.Lineage = c.Lineage
ORDER BY a.Lineage;
QUIT;

%IF %SYSFUNC(EXIST(PUBLIC.ME_DEP_CHT_M8BHistory)) %THEN %DO;
%PUT Do Nothing;
DATA WORK.M8BHistory;
SET CHTEMAIL.ME_DEP_CHT_M8BHistory;
RUN;
%END;
%ELSE %DO;
DATA WORK.M8BHistory;
FORMAT EquipmentName $ 50. Zone1_OvenInstability 3. TimeStamp DATETIME26.;
EquipmentName = ' ';
Zone1_OvenInstability = 0;
TimeStamp = DATETIME();
RUN;
%END;

PROC SQL;
CREATE TABLE WORK.ModelSummary1 AS
SELECT 
	a.*
	,(a.Zone1_OvenInstability + b.Zone1_OvenInstability) AS Zone1_Sum
FROM WORK.ModelSummary a
	LEFT JOIN WORK.M8BHistory b ON a.Lineage = SUBSTR(b.EquipmentName, 1, 6)
GROUP BY a.Lineage
HAVING b.TimeStamp = MAX(b.TimeStamp)
ORDER BY Lineage, TimeStamp;
QUIT;

DATA WORK.ModelTest_Summary;
SET WORK.ModelSummary1;
BY Lineage TimeStamp;
LENGTH SkipDecision $ 15. ModelPerformance $ 5.;
IF Model_Health = 'Yes' AND 'Zone2-Zone22_OvenInstability'n = 0 AND Zone1_Sum < 2 AND VP_Pass = 'Yes' THEN ModelPerformance = 'Good';
ELSE IF Model_Health = 'No' OR 'Zone2-Zone22_OvenInstability'n > 1 OR Zone1_Sum = 2 OR VP_Pass = 'No' THEN ModelPerformance = 'Bad';
ELSE ModelPerformance = 'EA';
IF ModelPerformance = 'Good' AND NextProfileSkipable = 'Yes' THEN SkipDecision = 'Yes';
ELSE IF ModelPerformance = 'Good' AND NextProfileSkipable = 'No' THEN SkipDecision = 'Not Eligible';
ELSE IF ModelPerformance = 'EA' AND NextProfileSkipable = 'Yes' THEN SkipDecision = 'EA';
ELSE IF ModelPerformance = 'EA' AND NextProfileSkipable = 'No' THEN SkipDecision = 'No';
ELSE IF ModelPerformance = 'Bad' AND NextProfileSkipable = 'Yes' THEN SkipDecision = 'No';
ELSE SkipDecision = 'No';
RUN;

%CheckcasResult(WORK, ModelTest_Summary, PUBLIC, ModelTest_Summary, 'xiaoyan.li@firstsolar.com');
OPTIONS NOMPRINT;
%mend;

