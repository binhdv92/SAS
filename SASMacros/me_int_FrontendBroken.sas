OPTION MAUTOSOURCE;
OPTIONS casdatalimit=ALL;

CAS;
caslib _all_ assign;

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


%macro extractdisktable(Tablename, ProcSQLTimeFilter, DaysAgo);
/*EXTRACT LAST &DaysAgo OF DATA FROM LASR TABLE*/

%deletedsifexists(work,Disk_Temp);
PROC SQL;
CREATE TABLE work.Disk_Temp AS
	SELECT *
	FROM PUBLIC.&Tablename	
	WHERE 
		&ProcSQLTimeFilter >= DHMS(DATE()-&DaysAgo,0,0,0)
	ORDER BY &ProcSQLTimeFilter;
QUIT;

%mend;


%macro uploadTabToTab (Tab1, Tab2, ProcSQLTimeFilter, DaysAgo, MergeBy); 
	cas;
	caslib _all_ assign;
	
	%let ConCatString =;
	%put &ConCatString;
	/*
	%let tab1 = Temp_SADETECTIONCUR;
	%let tab2 = ME_INT_SADETECTIONCUR;
	%let ProcSQLTimeFilter = DetectionTime;
	%let DaysAgo = 180;
	%let MergeBy = DetectionName;
	*/
    %if %sysfunc(exist(PUBLIC.&Tab2)) %then %do;
		%extractdisktable(&Tab2, &ProcSQLTimeFilter, &DaysAgo);
		PROC SORT data=work.Disk_Temp;
		BY &MergeBy;
		RUN;
		%let ConCatString = &ConCatString work.Disk_Temp;
	%end;	

	PROC SORT data=work.&tab1;
	BY &MergeBy;
	RUN;	

	DATA work.&tab2;
		MERGE &ConCatString work.&tab1;
		BY &MergeBy;
	RUN;	
	
	%checkCASresult(
		work,&Tab2
		,public,&Tab2
		,'fs120777@firstsolar.com'
	);
	%deletedsifexists(work,&tab2);	
	%deletedsifexists(work,Disk_Temp);		
%MEND;


%macro measurescrapqty ( scrapreason, scrapprocess, processId, preprocessId
	, dwellMinThreshold, dwellMinfromMeasureTimeThreshold, fallbackname
	, AlertDetectArea, AlertArea);

%put Site: &Site;
%put MeasureTime: &MeasureTime;
%put StartTime: &StartTime;
%put AlertCategory: &AlertCategory;
%put AlertWindow: &AlertWindow;

/*Getting equipInfo*/
%deletedsifexists(work,equipInfo);
PROC SQL;
CREATE TABLE work.equipInfot1 AS
	SELECT 
		 Site
		, ProcessName
		, Lineage
		, ProcessId
	FROM PUBLIC.ME_INT_SAEQUIPINFO
	WHERE processId = &processId
		and Site = "&Site"
	;
QUIT;

PROC SQL;
CREATE TABLE work.equipInfot2 AS
	SELECT * FROM work.equipInfot1
 	union all
	SELECT DISTINCT Site, ProcessName, cats ( Site, &fallbackname.) as Lineage, ProcessId FROM work.equipInfot1
	;
QUIT;

PROC SQL;
CREATE TABLE work.equipInfo AS
	SELECT 
		cats ( put( "&MeasureTime."DT, timestampstdf.), ',', Site, ',', cats (&AlertCategory., '_ScrapQty'), ',', &AlertWindow., ',', &AlertDetectArea. , ',', &AlertArea., ',', Lineage) as _id
		, cats ( Site, ',', cats (&AlertCategory., '_ScrapQty'), ',', &AlertWindow., ',', &AlertDetectArea. , ',', &AlertArea., ',', Lineage) as AlertName
		, cats (&AlertCategory., '_ScrapQty') as AlertCategory		
		, "&AlertWindow" as AlertWindowWindow
		, "&MeasureTime."DT format=datetime22. as MeasurementTime
		, *
	FROM work.equipInfot2
	WHERE Lineage not like '%N/A%'
	;
QUIT;

%deletedsifexists(work,equipInfot1);
%deletedsifexists(work,equipInfot2);

/*Getting scrapSubid*/
%deletedsifexists(work,scrapsubid);
PROC SQL;
CREATE TABLE work.scrapsubid AS
	
	SELECT 
		SubId, Site, ProcessName, ScrapReason, ReadTime 
	FROM PUBLIC.ME_INT_SARAWSCRAP
	WHERE 
		ReadTime > "&StartTime"DT and ReadTime < "&MeasureTime"DT 
		and ScrapCount = 1
		and ScrapReason in &scrapreason
		and processid >= input(scan(&scrapprocess, 1), 5.) 
		and processid <= input(scan(&scrapprocess, 2), 5.) 
		and site = "&Site"
		and YieldExclusion = 'Good'
	;
QUIT;

/*If no scrap return assign 0 to end table*/
data _NULL_; if 0 then set work.scrapsubid nobs=n; call symputx('nrows',n); stop; run;
%put no. of observations = &nrows;
%if ( &nrows = 0 ) %then
	%do;
	PROC SQL;
	CREATE TABLE work.temptable AS
		SELECT *, 0 as MeasurementValue FROM work.equipInfo
	;
	QUIT;
  	%end;

%else
  	%do;
	PROC SQL;
	CREATE TABLE work.temptable1 AS
	SELECT  
		s.SubID
		, p.ReadTime format=datetime22. as PReadTime
		, pp.ReadTime format=datetime22. as PPReadTime
		, CASE 
			WHEN p.ReadTime = . | pp.ReadTime =. THEN cats ( "&Site", &fallbackname.)
			WHEN abs (p.ReadTime - pp.ReadTime) / (60) > &dwellMinThreshold THEN cats ( "&Site", &fallbackname.)
			ELSE p.Lineage END
		  as Lineage
	FROM 
	
	work.scrapsubid s
	LEFT JOIN	
	(
	SELECT SubId, Lineage, Min (ReadTime) as ReadTime FROM PUBLIC.ME_INT_SAWORKFLOW
	WHERE SubId in (Select SubId From work.scrapsubid) AND ProcessId = &processId.
	Group by SubId, Lineage
	) p ON s.SubId = p.SubId
	LEFT JOIN	
	(
	SELECT SubId, min (ReadTime) as ReadTime FROM PUBLIC.ME_INT_SAWORKFLOW 
	WHERE  SubId in (Select SubId From work.scrapsubid) AND ProcessId = &preprocessId.
	Group by SubId
	) pp ON s.SubId = pp.SubId
	
	WHERE abs (p.ReadTime - "&MeasureTime"DT ) / (60)  < &dwellMinfromMeasureTimeThreshold
	;
	QUIT;    

	PROC SQL;
	CREATE TABLE work.temptable AS
		
		SELECT e.*, CASE WHEN v.MeasurementValue = . THEN 0 ELSE v.MeasurementValue END as MeasurementValue

		FROM work.equipInfo e
			LEFT JOIN 
			(
			SELECT Lineage, count (Lineage) as MeasurementValue FROM work.temptable1 GROUP BY Lineage
			) v
			ON e.Lineage = v.Lineage

	;	
	QUIT;	
	
	%deletedsifexists(work,temptable1);
	%end;

/*Merge to end Table*/
	%let ConCatString=;
	%let ConCatString=&ConCatString work.temptable;
	
	%if %sysfunc(exist(work.&Tablename)) %then %do;
		%let ConCatString=&ConCatString work.&Tablename;
	%end;
	%put &ConCatString;
	
	DATA work.&Tablename;
		MERGE &ConCatString;
		BY _id;
	RUN;

%deletedsifexists(work,temptable);
%deletedsifexists(work,scrapsubid);
%deletedsifexists(work,equipInfo);
%mend;


%MACRO measuretpforscrap ( equipprocessId, processId, preprocessId
	, dwellMinThreshold, fallbackname
	, AlertDetectArea, AlertArea
);

/*Setup ETL Statement and Call Data Extraction*/
%GLOBAL SQLExtraction ProcSQLOrderBy ProcSQLTimeFilter MergeBy ;
%LET ProcSQLTimeFilter 	= MeasurementTime; 				/*Enter to Column Name of the DateTime you want to filter from LASR Table*/
%LET ProcSQLOrderBy 	= MeasurementName; 	/*Enter column names separated by comma for ordering data from LASR Table*/
%LET MergeBy 			= MeasurementName; 	/***Enter column names separated by comma***/

%LET SQLExtraction = 
	SET NOCOUNT ON 
	DECLARE @measurementTime varchar(19) = &MeasureTime.
	DECLARE @StartTime varchar(19) = &StartTime.
	/* DECLARE @measurementTime DATETIME = getdate () */

	DECLARE @zoneOffset INT =  DATEDIFF(hour, getdate(), getutcdate())  
	    
	DECLARE @measurementUTC DATETIME =  DateAdd(hour, (@zoneOffset),  @measurementTime )
	DECLARE @startUTC DATETIME =  DateAdd(hour, (@zoneOffset),  @StartTime )
	DECLARE @taskStartTime DATETIME =  getdate()

	DECLARE @equipprocessId INT =  &equipprocessId.	    
	DECLARE @processId INT =  &processId.
	DECLARE @preprocessId INT =  &preprocessId.
	DECLARE @dwellMinThreshold INT =  &dwellMinThreshold.
	DECLARE @fallbackname varchar(19) = &fallbackname.
	    
	DECLARE @AlertCategory varchar(50) = &AlertCategory.+'_Throughput'
	DECLARE @AlertWindow varchar(50) =  &AlertWindow.
	DECLARE @AlertDetectArea varchar(50) = &AlertDetectArea.
	DECLARE @AlertArea varchar(50) = &AlertArea.
	
	If(OBJECT_ID('tempdb..#equipInfo') Is Not Null) Begin Drop Table #equipInfo END
	SELECT 
		ods.mfg.fn_plant() as [Site]
		, LEFT (GE.Name, 6) as Lineage
		, GP.Name as [ProcessName]
	INTO #equipInfo
	FROM [ODS].[mfg].[GlobalEquipment] as GE LEFT JOIN [ODS].[mfg].[GlobalProcess] as GP
		ON GP.ProcessId = GE.ProcessId
	WHERE GP.LegacyProcessId = @equipprocessId
	AND GE.Name IS NOT NULL
	AND LEFT (GE.Name, 4) = ods.mfg.fn_plant()
		    
	If(OBJECT_ID('tempdb..#lineageInfo') Is Not Null) Begin Drop Table #lineageInfo END
	SELECT 
		@AlertCategory as [AlertCategory]
		, 
		x.[Site] + ','
		+ @AlertCategory + ','
		+ @AlertWindow + ','
		+ @AlertDetectArea + ','
		+ @AlertArea + ','
		+ x.Lineage + ',' 
		    
		as [AlertName]
		, cast ( @measurementTime as datetime) as [MeasurementTime]
		, @AlertWindow as [Window]
		, x.* 
	INTO #lineageInfo
	FROM (SELECT * FROM #equipInfo
		UNION
		SELECT DISTINCT [Site], [Site] + @fallbackname as Lineage, ProcessName  FROM #equipInfo) x
	
	If(OBJECT_ID('tempdb..#processInfo') Is Not Null) Begin Drop Table #processInfo END
	SELECT 
		SubId
		, Lineage as [ProcessLineage]
		, convert(datetime,ReadTime) AS [ProcessReadTime]
		, ROW_NUMBER() OVER (PARTITION BY SubID ORDER BY [ReadTime]) AS RN 
	INTO #processInfo
	FROM ods.mfg.Workflow
	WHERE ProcessId = @processId
	AND ReadTimeUtc > @startUTC AND ReadTimeUtc < @measurementUTC
	
	If(OBJECT_ID('tempdb..#preprocessInfo') Is Not Null) Begin Drop Table #preprocessInfo END
	SELECT  DISTINCT SubId
		, ProcessName
		, ProcessId
		, Lineage as [ProcessLineage]
		, convert(datetime,ReadTime) AS [ProcessReadTime]
		, ROW_NUMBER() OVER (PARTITION BY SubID ORDER BY [ReadTime]) AS RN 
	INTO #preprocessInfo
	FROM ODS.mfg.Workflow 
	WHERE ProcessId = @preprocessId AND SubId in (SELECT SubId FROM #processInfo)
	
	If(OBJECT_ID('tempdb..#rawData') Is Not Null) Begin Drop Table #rawData END
	SELECT 
		CASE WHEN abs ( DATEDIFF(MINUTE, pp.ProcessReadTime, p.ProcessReadTime ) ) >= @dwellMinThreshold 
			 THEN ods.mfg.fn_plant() + @fallbackname ELSE p.ProcessLineage
		     END as [ProcessLineage] 
		, Count (ods.mfg.fn_plant()) as [Value]
	INTO #rawData
	FROM #processInfo as p LEFT JOIN (SELECT * FROM #preprocessInfo WHERE RN = 1) as pp ON p.SubID = pp.SubID
	WHERE p.RN = 1
	GROUP BY CASE WHEN abs ( DATEDIFF(MINUTE, pp.ProcessReadTime, p.ProcessReadTime ) ) >= @dwellMinThreshold 
			 THEN ods.mfg.fn_plant() + @fallbackname ELSE p.ProcessLineage END
	
	
	SELECT 
	    CONVERT ( varchar(19),  CONVERT ( varchar , DATEADD( minute, ( DATEDIFF( minute, 0, l.MeasurementTime ) / 1 ) * 1, 0 ), 120 ) ) + ',' + l.AlertName as [_id]   
		, l.AlertName as [MeasurementName]
		, l.[Site]
		, l.AlertCategory as [MeasurementCategory]
		, l.[Window] as [MeasurementWindow]
		, l.MeasurementTime
		, @taskStartTime as TaskStartTime
		, getdate () as TaskCompleteTime
		, cast ( DATEDIFF(millisecond, @taskStartTime, getdate () ) as real) / 1000  as RunTimeInSec
		, CASE WHEN v.[Value] IS NULL THEN 0 ELSE v.[Value] END as [MeasurementValue]
	FROM #lineageInfo as l LEFT JOIN #rawData as v
		ON l.Lineage = v.ProcessLineage
	WHERE 
		l.AlertName NOT LIKE '%N/A%'
;


/*Getting Data from MES*/
	%deletedsifexists(work,temp);
	%let servername=&Site.MESODS;
	proc sql;
		connect to odbc as con2(
		datasrc= %nrbquote(")&servername%nrbquote(")
		authdomain=SQLGRP_Temp_Reader_Auth
	);
	
	Create table work.temp as 
	select * from connection to con2(
	&SQLExtraction.
	);
	DISCONNECT FROM con2;
	quit;

/*Merge to end Table*/
	%let ConCatString=;
	%let ConCatString=&ConCatString work.temp;
	
	%if %sysfunc(exist(work.&Tablename)) %then %do;
		%let ConCatString=&ConCatString work.&Tablename;
	%end;
	%put &ConCatString;
	
	DATA work.&Tablename;
		MERGE &ConCatString;
		BY _id;
	RUN;
	%deletedsifexists(work,temp);

%mend;

%macro measuring (Site);

	/*BARCODE_MARKER*/
	%measurescrapqty ( 
		scrapreason = ('Broken', 'Sent to Dumpster', 'Operator Scrap Button')
		, scrapprocess = '61200,62517'
		, processId = 61175
		, preprocessId = 61400
		, dwellMinThreshold = 15
		, dwellMinfromMeasureTimeThreshold = 720
		, fallbackname = 'PRESEAM'
		, AlertDetectArea = 'FrontEnd'
		, AlertArea = 'BARCODE_MARKER'
	);

	/*CDCL2_ROLLCOAT*/
	%measurescrapqty ( 
		scrapreason = ('Broken', 'Sent to Dumpster', 'Operator Scrap Button')
		, scrapprocess = '61200,62517'
		, processId = 62025
		, preprocessId = 61400
		, dwellMinThreshold = 15
   		, dwellMinfromMeasureTimeThreshold = 720
		, fallbackname = 'FS100WIP'
		, AlertDetectArea = 'FrontEnd'
		, AlertArea = 'CDCL2_ROLLCOAT'
	);

	
	/*CHT*/
	%measurescrapqty ( 
		scrapreason = ('Broken', 'Sent to Dumpster', 'Operator Scrap Button')
		, scrapprocess = '61200,62517'
		, processId = 62242
		, preprocessId = 62242
		, dwellMinThreshold = 15
		, dwellMinfromMeasureTimeThreshold = 720
		, fallbackname = 'N/A'
		, AlertDetectArea = 'FrontEnd'
		, AlertArea = 'CHT'
	);

/*	THROUGHPUT FOR SCRAP
*/
	/*BARCODE_MARKER*/
	%measuretpforscrap ( 
		equipprocessId = 61175
		, processId = 61400
		, preprocessId = 61175
		, dwellMinThreshold = 15
		, fallbackname = 'PRESEAM'
		, AlertDetectArea = 'BARCODE_MARKER'
		, AlertArea = 'BARCODE_MARKER'
	);

	/*CDCL2_ROLLCOAT*/
	%measuretpforscrap ( 
		equipprocessId = 62025
		, processId = 62025
		, preprocessId = 61400
		, dwellMinThreshold = 15
		, fallbackname = 'FS100WIP'
		, AlertDetectArea = 'CDCL2_ROLLCOAT'
		, AlertArea = 'CDCL2_ROLLCOAT'
	);

	/*CHT*/
	%measuretpforscrap ( 
		equipprocessId = 62242
		, processId = 62242
		, preprocessId = 62242
		, dwellMinThreshold = 15
		, fallbackname = 'N/A'
		, AlertDetectArea = 'CHT'
		, AlertArea = 'CHT'
	);

%mend;


%MACRO detecting;
	/*
	FrontEnd_Broken_Last3hrs Detection logic
		Defining BestLine at each process
			Having lowest ScrapQty
				Throughput <50 will be ignore
			Delta to Bestline >5 will be trigger
			(under debating that using OCAP of 5 scrap in 3hours)
	*/
	cas;
	caslib _all_ assign;
	OPTION MAUTOSOURCE;
	
	%let tpThreshold = 50;
	%let DetectionCategory = 'FrontEndBroken_Last3hours';
	%let CompareTo = 'BestlineQty';
	
	/*throughput in Last3hrs*/	
	%deletedsifexists(work, throughput);
	proc sql noprint;
	create table work.throughput as

	select distinct
		scan(MeasurementName, 5 , ',')  as ProcessName
		, scan(MeasurementName, 6 , ',')  as Lineage
		, Mean (MeasurementValue) as Throughput
	from public.me_int_sameasurementcur 
	where MeasurementCategory = 'FrontEndBroken_Throughput'
	group by 
		 scan(MeasurementName, 5 , ',')
		, scan(MeasurementName, 6 , ',')
	;
	quit;

	/*Scrap with Tp in Last3hrs*/
	%deletedsifexists(work, scrap);
	proc sql noprint;
	create table work.scrap as

	select distinct
		&DetectionCategory as DetectionCategory
		, &CompareTo as CompareTo
		, s.MeasurementCategory
		, s.Site
		, scan(s.MeasurementName, 4 , ',')  as DetectionArea
		, scan(s.MeasurementName, 5 , ',')  as ProcessName
		, scan(s.MeasurementName, 6 , ',')  as Lineage
		, s.MeasurementValue as ScrapQty
		, t.Throughput
		, Case When t.Throughput < &tpThreshold. then .
			   When t.Throughput >= &tpThreshold. then s.MeasurementValue
		  End as WeightScrap
	from public.me_int_sameasurementcur s left join work.throughput t
			on scan(MeasurementName, 5 , ',') = t.ProcessName
			and scan(MeasurementName, 6 , ',') = t.Lineage

	where s.MeasurementCategory = 'FrontEndBroken_ScrapQty'
	;
	quit;	

	/*Comparing*/
	%deletedsifexists(work, comparing);
	proc sql noprint;
	create table work.comparing as

	select 
		s.DetectionCategory
		, s.CompareTo
		, s.Site, s.DetectionArea, s.ProcessName, s.Lineage, s.ScrapQty, s.WeightScrap, bl.Bestline as  BestlineQty
		, 	s.WeightScrap 
			- (case when bl.Bestline = . then 0 else bl.Bestline end) 
			as DetectionValue
		, r.Level1Thres, r.Level2Thres, r.Level3Thres
	from work.scrap s 
		left join 
			(
				select Site, ProcessName, min (WeightScrap) as Bestline from work.scrap Group by Site, ProcessName
			)bl
			on s.Site = bl.Site and s.ProcessName = bl.Processname
		
		left join 
			(
				select * from public.ME_INT_SAALERTRANKING 
				where DetectionCategory = &DetectionCategory and CompareTo = &CompareTo
			)r
			on s.DetectionCategory = r.DetectionCategory and s.CompareTo = r.CompareTo
	;			
	quit;
	
	/*Detection Table*/
	%deletedsifexists(work, &Tablename);

	proc sql noprint;
	create table work.&Tablename as

	select 
		cats ( put( &measureTimevar.,timestampstdf.) , ',', Site, ',', DetectionCategory, ',', &CompareTo, ',', DetectionArea , ',', ProcessName, ',', Lineage)  as _id
		, Site
		, cats (Site, ',', DetectionCategory, ',', &CompareTo, ',', DetectionArea , ',', ProcessName, ',', Lineage)  as DetectionName 
		, DetectionCategory
		, &measureTimevar format=datetime22. as DetectionTime
		, DetectionValue
		, Case When DetectionValue > Level3Thres Then 'Level3'
			   When DetectionValue > Level2Thres Then 'Level2'
			   When DetectionValue > Level1Thres Then 'Level1'
		  	   Else 'None'
		  End as Level
	from work.comparing
	;
	quit;	

%MEND;

/**************************************************************************************************/
/*Program Starting from here*/
/**************************************************************************************************/

%MACRO me_int_FrontendBroken (measureTimevar);
/* %let measureTimevar=	%sysevalf(%sysfunc(datetime())); */
proc format; picture timestampstdf other='%Y-%0m-%0d %0H:%0M:%0S' (datatype=datetime) ; run;
proc format; picture datestdf other='%Y-%0m-%0d' (datatype=datetime) ; run;

/*Defining Timestamp*/
%let MeasureTime=%sysfunc(putn( &measureTimevar,timestampstdf.));
%let MeasureTime=%nrbquote(')&MeasureTime%nrbquote(');
%put MeasureTime: &MeasureTime;

%let StartTime=%sysfunc(putn( &measureTimevar-180*60,timestampstdf.));
%let StartTime=%nrbquote(')&StartTime%nrbquote(');
%put StartTime: &StartTime;


/*Measuring*/
%let Tablename = Temp_SAMEASUREMENTCUR;
%deletedsifexists(work,&Tablename);

/*Defining AlertType*/
%let AlertCategory='FrontEndBroken';
%let AlertWindow='Last3hours';

%measuring (PGT1);
%measuring (PGT2);
%uploadTabToTab ( tab1 = Temp_SAMEASUREMENTCUR
				, tab2 = ME_INT_SAMEASUREMENTCUR
				, ProcSQLTimeFilter = MeasurementTime
				, DaysAgo = 180
				, MergeBy = MeasurementName);

%uploadTabToTab ( tab1 = Temp_SAMEASUREMENTCUR
				, tab2 = ME_INT_SAMEASUREMENTHIS
				, ProcSQLTimeFilter = MeasurementTime
				, DaysAgo = 180
				, MergeBy = _id);

/*Detecting*/
%let Tablename = Temp_SADETECTIONCUR;
%deletedsifexists(work,&Tablename);

%detecting;
%uploadTabToTab ( tab1 = Temp_SADETECTIONCUR
				, tab2 = ME_INT_SADETECTIONCUR
				, ProcSQLTimeFilter = DetectionTime
				, DaysAgo = 180
				, MergeBy = DetectionName);

%uploadTabToTab ( tab1 = Temp_SADETECTIONCUR
				, tab2 = ME_INT_SADETECTIONHIS
				, ProcSQLTimeFilter = DetectionTime
				, DaysAgo = 180
				, MergeBy = _id);
%MEND;

/*
cas;
caslib _all_ assign;
*/