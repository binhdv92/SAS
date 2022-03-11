%MACRO me_dep_cht_summary();
OPTIONS VALIDVARNAME=ANY MAUTOSOURCE;
CAS mySession SESSOPTS=(CASLIB=PUBLIC TIMEOUT=1800 LOCALE="en_US");
LIBNAME PUBLIC CAS CASLIB="PUBLIC";

PROC SQL;
/*1.Combine the Summary tables of Single, Multi and Single_ThinGls created in the Virtual15 scripts
  2.Create a new variable named Score in the combined table*/
CREATE TABLE WORK.COMBINATION AS
SELECT *, . AS Okay, . AS SPCViolation, . AS 'Number of training'n, (Ramp + Soak) AS Score
FROM PUBLIC.Summary_BELT WHERE Ramp NE .
UNION
SELECT *, (Ramp + Soak) AS Score FROM PUBLIC.SUMMARY WHERE Ramp NE .
UNION
SELECT *, (Ramp + Soak) AS Score FROM PUBLIC.SUMMARY_T WHERE Ramp NE .;

/*Select the best model for each of the lineages following the creterial below:
Firstly, compare the skip decision of three models and select the model with Yes*/
CREATE TABLE WORK.Comparison_SkipDecision AS
SELECT *
FROM WORK.COMBINATION
WHERE 'Skip Decision'n = 'Yes';

DELETE FROM WORK.COMBINATION
WHERE EquipmentName IN (SELECT EquipmentName FROM WORK.Comparison_SkipDecision);

CREATE TABLE WORK.UNION AS
SELECT * FROM WORK.COMBINATION
UNION
SELECT * FROM WORK.Comparison_SkipDecision;

/*If there are more than one model left for any lineages after the first creterial, move on to the second one.
Secondly, compare the score of three models and select the model with minimum score*/
CREATE TABLE WORK.Comparison_Score AS
SELECT *
FROM WORK.UNION
GROUP BY EquipmentName
HAVING Score = MIN(Score);

/*If there are more than one model left for any lineages after the second creterial, move on to the third one.
Thirdly, compare the Warning_number(renamed Oven_Instability in the reports) of three models and select the model with minimum Warning_number*/
CREATE TABLE WORK.Comparison_Oven AS
SELECT *
FROM WORK.Comparison_Score
GROUP BY EquipmentName
HAVING Warning_number = MIN(Warning_number);

/*If there are more than one model left for any lineages after the previous creterial, move on to the last one.
Finally, compare the F_number(renamed VP_Fail in the reports) of three models and select the model with minimum F_number*/
CREATE TABLE WORK.Comparison_VP AS
SELECT *
FROM WORK.Comparison_Oven
GROUP BY EquipmentName
HAVING F_number = MIN(F_number);

/*If it happens to any lineages that 2 or 3 models are both good, select the "simpler" model having fewer predicator variables as default.*/
CREATE TABLE WORK.RN AS
SELECT *,
(CASE WHEN Model = 'Single' THEN 1
	WHEN Model = 'Single_ThinGls' THEN 2
	ELSE 3
 END) AS RN
FROM WORK.Comparison_VP;

CREATE TABLE WORK.ModelSelection AS
SELECT *
FROM WORK.RN
GROUP BY EquipmentName
HAVING RN = MIN(RN);

/*Aug 29, 2020 Update: Delete incorrect skipped files for DMT11A*/
CREATE TABLE WORK.M8Profiles AS
SELECT EquipmentID
		,NextProfileSkipable
		,RN
FROM PUBLIC.ME_DEP_CHT_M8Profiles
WHERE NOT (EquipmentID = 'DMT11A-CHT' AND OvenProfileID IN (1466, 1467))
GROUP BY EquipmentID
HAVING RN = MIN(RN);

CREATE TABLE WORK.M8TARGETS AS
SELECT Lineage 
	   ,Technology
       ,TargetSoak
	   ,RN
FROM PUBLIC.ME_DEP_CHT_M8TARGETS
WHERE RN = 1;

CREATE TABLE WORK.ModelCombination AS
SELECT a.*
       ,b.NextProfileSkipable
       ,c.Technology
       ,c.TargetSoak
	   ,34.12 AS BeltSpeed
FROM WORK.ModelSelection a
	 LEFT JOIN WORK.M8Profiles b ON a.EquipmentName = SUBSTR(b.EquipmentID, 1, 6)
     LEFT JOIN WORK.M8TARGETS c ON a.EquipmentName = c.Lineage;

/*Aug 24, 2020 Update - Factor the NextProfileSkipable into the Skip Decision*/
CREATE TABLE WORK.ME_DEP_CHT_Summary AS
SELECT
	*
	,(CASE WHEN 'Skip Decision'n = 'Yes' AND NextProfileSkipable = 'Yes' THEN 'Yes'
		WHEN 'Skip Decision'n = 'Yes' AND NextProfileSkipable = 'No' THEN 'Not Eligible'
		WHEN 'Skip Decision'n = 'No' AND NextProfileSkipable = 'Yes' THEN 'No'
		WHEN 'Skip Decision'n = 'No' AND NextProfileSkipable = 'No' THEN 'No'
		WHEN 'Skip Decision'n = 'EA' AND NextProfileSkipable = 'Yes' THEN 'EA'
		ELSE 'No'
	END) AS SkipDecision
FROM WORK.ModelCombination;
QUIT;

%CheckcasResult(WORK, ME_DEP_CHT_Summary, PUBLIC, ME_DEP_CHT_Summary, 'xiaoyan.li@firstsolar.com');
OPTIONS NOMPRINT;
%mend;

