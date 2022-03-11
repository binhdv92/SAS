OPTIONS CASDATALIMIT=2G NONOTES NOMPRINT;
%GLOBAL email workdir;
/* LIBNAME SASMEEXT META LIBRARY="SAS_ME_External_Sources" METAOUT=DATA; */
cas mySession sessopts=(caslib=public timeout=1800 locale="en_US");
libname SASMEEXT '/sasdata/me/external_sources';
LIBNAME LASRLIB CAS CASLIB="PUBLIC";

%LET workdir=%trim(%sysfunc(pathname(work)));

caslib _all_ assign;

%MACRO Email(Subject) / MINOPERATOR;

ODS _ALL_ CLOSE;
ODS HTML;

/* %LET email = ''; */


filename SEND email
 		TO=('tshields@firstsolar.com' 'Joseph.Drabek@firstsolar.com' )
		FROM=('FinFrameAlert <WetLineEarlyWarning@firstsolar.com>')
		SENDER = ('FinFrameAlert <NOREPLY@firstsolar.com>')
		IMPORTANCE='HIGH'
		Content_type="Text/HTML"
		replyto='NOREPLY@FirstSolar.com'
		Subject="Finishing Framing Alerts for &Subject.";

data _null_;
  infile REPORT;
  file SEND;
  input;
	/*if _n_ = 1 THEN do;
		PUT '<p><span style="font-family: 'courier new', courier;"><span style="font-size: 18px;"><strong>Passdown Hub Report.</strong></span></span></p>';
		PUT '<p><span style="font-size: 12pt;"><span style="font-family: 'courier new', courier;">Here is the most recent report information.</span></span></p>';
	end;*/
	if _infile_ ne '</html>' then put _infile_;
	else do;
		PUT '<p>&nbsp;</p>';
		PUT '<p><span style="font-family: ' courier new', courier;">Thank You,</span></p>';
		PUT '<p><span style="color: #ff6600; font-family: ' courier new' , courier;">SASMfgSupport</span></p>';
	end;
run;


ods html close;

%MEND;

%LET PGT = 		%SYSFUNC(DATEPART(%SYSFUNC(DATETIME())));
%LET PGTHour = 	%EVAL(%SYSFUNC(FLOOR((%SYSFUNC(DATETIME()))/3600))*3600);
%LET UDT = 		%SYSFUNC(DATEPART(%SYSFUNC(tzones2u(%SYSFUNC(DATETIME())))));
%LET KMT = 		%SYSFUNC(DATEPART(%SYSFUNC(tzoneu2s(%SYSFUNC(tzones2u(%SYSFUNC(DATETIME()))),Asia/Singapore))));
%LET KMTHour = 	%EVAL(%SYSFUNC(FLOOR((%SYSFUNC(tzoneu2s(%SYSFUNC(tzones2u(%SYSFUNC(DATETIME()))),Asia/Singapore)))/3600))*3600);
%LET DMT = 		%SYSFUNC(DATEPART(%SYSFUNC(tzoneu2s(%SYSFUNC(tzones2u(%SYSFUNC(DATETIME()))),Asia/Saigon))));
%LET DMTHour = %EVAL(%SYSFUNC(FLOOR((%SYSFUNC(tzoneu2s(%SYSFUNC(tzones2u(%SYSFUNC(DATETIME()))),Asia/Saigon)))/3600))*3600);

%PUT NOTE:  UDT Date is:  &UDT;
%PUT NOTE:  PGT Date is:  &PGT;
%PUT NOTE:  PGTHour Date is:  &PGTHour;
%PUT NOTE:  KMT Date is:  &KMT;
%PUT NOTE:  KMTHour Date is:  &KMTHour;
%PUT NOTE:  DMT Date is:  &DMT;
%PUT NOTE:  DMTHour Date is:  &DMTHour;

%MACRO finframealert(Plant);

%LET Plant = PGT1;

DATA work.ME_FIN_FRAMESCREWGUN /*Data only available for PGT2, DMT1, DMT2*/
	/*(KEEP=ReadTime EquipmentName Name Module AvgWarningLow AvgActual AvgWarningHigh HrsToWarn n Warn Urgent)*/;
	SET PUBLIC.ME_FIN_FRAMESCREWGUN (where=(
			(
			(Substr('EquipmentName'n, 1, 3) = 'PGT' AND (ReadTime) >= (&PGTHour-2*3600) AND (ReadTime) < &PGTHour+1*3600) 
			OR
			(Substr('EquipmentName'n, 1, 3) = 'KMT' AND (ReadTime) >= (&KMTHour-2*3600) AND (ReadTime) < &KMTHour+1*3600)
			OR
			(Substr('EquipmentName'n, 1, 3) = 'DMT' AND (ReadTime) >= (&DMTHour-2*3600) AND (ReadTime) < &DMTHour+1*3600)
			)
			AND Substr('EquipmentName'n, 1, 4) = "&Plant."
			)
		);
	TorqueFailure = 0;
	IF Substr('Parameter'n, 6,6) = 'Torque' AND Value < 2.2 THEN TorqueFailure = 1;
RUN;

DATA work.ME_FIN_FRAMESTATUS2 
	/*(KEEP=ReadTime EquipmentName Name Module AvgWarningLow AvgActual AvgWarningHigh HrsToWarn n Warn Urgent)*/;
	SET PUBLIC.ME_FIN_FRAMESTATUS2 (where=(
			(
			(Substr('SourceLocation'n, 1, 3) = 'PGT' AND (ReadTimeHour) >= (&PGTHour-1*3600) AND (ReadTimeHour) < &PGTHour+1*3600) 
			OR
			(Substr('SourceLocation'n, 1, 3) = 'KMT' AND (ReadTimeHour) >= (&KMTHour-1*3600) AND (ReadTimeHour) < &KMTHour+1*3600)
			OR
			(Substr('SourceLocation'n, 1, 3) = 'DMT' AND (ReadTimeHour) >= (&DMTHour-1*3600) AND (ReadTimeHour) < &DMTHour+1*3600)
			)
			AND Substr('EquipmentName'n, 1, 4) = "&Plant."
			)
		);
RUN;

/* PROC SQL; */
/* CREATE table work.mostrecent as */
/* SELECT */
/* 	Max(ReadTime) as MaxReadTime, EquipmentName, Name, Module, SUM(n) AS nObs, SUM(Warn) as nWarnings, SUM(Urgent) AS nUrgent */
/* FROM work.FilterFlowEarlyWarn */
/* GROUP BY EquipmentName, Name, Module; */
/* QUIT; */
/*  */
/* PROC SQL; */
/* CREATE TABLE work.final AS */
/* SELECT */
/* 	a.*, b.nObs, b.nWarnings, b.nUrgent */
/* FROM  */
/* 	work.FilterFlowEarlyWarn a */
/* 	INNER JOIN work.mostrecent b on a.EquipmentName = b.EquipmentName AND a.Name= b.name and a.Module = b.Module and a.readtime = b.MaxReadTime */
/* WHERE */
/* 	nWarnings >= 3 or Urgent = 1; */
/* QUIT; */
/*  */
/* DATA work.final; */
/* 	SET work.final (WHERE=(Substr('EquipmentName'n, 1, 4) = "&Plant.")); */
/* RUN; */
/*  */
/* PROC SQL NOPRINT; */
/* SELECT COUNT(EquipmentName) INTO :NRecords FROM work.final; */
/* QUIT; */

%PUT &NRecords;

/*MaximoPM*/

PROC SQL;
CREATE TABLE WORK.MAINT_S6_WODETAIL AS
SELECT TimeZoneOffset, siteid, area, wonum, worktype, description, AssetDescription, status, schedstart, jpnum, estdur, owner, location
FROM PUBLIC.MAINT_S6_WODETAIL
WHERE 
	(schedstart ) > datetime()
	AND (schedstart) < (datetime()+3600*12)
	AND worktype IN ('PM')
	UPCASE(AssetDescription) like '%FRAME%'
;
QUIT;

DATA WORK.WODETAIL;
	SET WORK.MAINT_S6_WODETAIL;
	LENGTH SiteId_Plant 8 Plant $ 8;
	SiteId_Plant = cats('', siteid,substr(location, 1, 1));
	IF SiteId_Plant = '10061' THEN Plant = 'PGT1';
	IF SiteId_Plant = '10062' THEN Plant = 'PGT2';
	IF SiteId_Plant = '30031' THEN Plant = 'DMT1';
	IF SiteId_Plant = '30032' THEN Plant = 'DMT2';
	IF SiteId_Plant = '30061' THEN Plant = 'KMT1';
	IF area = ' ' THEN area = 'Missing';
	IF owner = ' ' THEN owner = 'MISSING';
	LocalScheduleStart = schedstart + 3600 * TimeZoneOffset ;
	LABEL area = 'Area';
	LABEL owner = 'Owner';
	LABEL wonum = 'Work Order';
	LABEL description = 'Work Order Description';
	LABEL AssetDescription = 'Asset Description';
	LABEL status = 'Status';
	LABEL schedstart = 'Scheduled Start';
	LABEL estdur = 'Est Duration';
	LABEL LocalScheduleStart = 'Local Schedule Start';
	FORMAT LocalScheduleStart DATETIME23.3;
RUN;

PROC SQL NOPRINT;
SELECT COUNT(WONUM) INTO :PMCount 
FROM WORK.WODETAIL
WHERE Plant = "&Plant." ;
 
CREATE TABLE WORK.OUTLOOK AS
SELECT Plant, area, wonum, worktype, description, AssetDescription, status, LocalScheduleStart, jpnum, estdur, owner, location
FROM WORK.WODETAIL
WHERE Plant = "&Plant."   
ORDER BY LocalScheduleStart;
QUIT;

%PUT &PMCount;

filename REPORT "%sysfunc(pathname(work))/FilterStatus.html";


ods _ALL_ close;
OPTIONS NOCENTER;
ods htmlcss STYLE=Dove file=REPORT;
/*the escapechar below is for line breaks from SQL*/
ODS Escapechar='^';

ODS HTML;

/*Create Table Report*/
Title1 HEIGHT=4 justify=left color=black "Estimated <12 Hours Till Filter Warning Limit";
%IF &NRecords < 1 %THEN %DO;
	/* ODS TEXT="^{newline 1} ^{style [color=red font_size=2 font_weight=bold background=black] NOTE:  No Comments Detected for this Functional Area Shift Report} ^{newline 2}"; */
	ODS TEXT="^{newline 1} ^{style [font_size=2] NOTE:  No Filters Warnings Detected} ^{newline 2}";
	ODS TEXT="^{newline 1}";
%END;
%ELSE %DO;
	%PUT Write Data;
	/* SnR Safety Overview */
	PROC REPORT data=work.final
		style(report)={outputwidth=100%}
		style(header)=[just = left font_face = calibri font_size = 3 foreground = Black background = orange protectspecialchars= off];
		column ReadTime EquipmentName Name Module AvgActual AvgWarningLow AvgWarningHigh HrsToWarn;
		DEFINE ReadTime / display STYLE(column)=[just=l];
		DEFINE EquipmentName / display STYLE(column)=[just=l];
		DEFINE Name / display STYLE(column)=[just=l];
		DEFINE Module / display STYLE(column)=[just=l];
		DEFINE AvgActual / display STYLE(column)=[just=l];
		DEFINE AvgWarningLow / display STYLE(column)=[just=l];
		DEFINE AvgWarningHigh / display STYLE(column)=[just=l];
		DEFINE HrsToWarn / display STYLE(column)=[just=l];
	QUIT;
	Title1;
%END;

TITLE HEIGHT=4 "Scheduled PM's for Next Shift";
%IF &PMCount < 1 %THEN %DO;
	/* ODS TEXT="^{newline 1} ^{style [color=red font_size=2 font_weight=bold background=black] NOTE:  No Comments Detected for this Functional Area Shift Report} ^{newline 2}"; */
	ODS TEXT="^{newline 1} ^{style [font_size=2] NOTE:  No Upcoming PM's Detected} ^{newline 2}";
	ODS TEXT="^{newline 1}";
%END;
%ELSE %DO;
	%PUT Write Data;
	PROC REPORT DATA=WORK.OUTLOOK
	STYLE(report)={outputwidth=100%}
	STYLE(header) =	[just = left font_face = calibri font_size = 3 foreground = Black background = orange protectspecialchars=off];
	COLUMN Plant owner wonum AssetDescription description LocalScheduleStart status;
	DEFINE Plant / DISPLAY  STYLE(column)=[just=l cellwidth=5%];
	DEFINE owner / 'Owner' DISPLAY STYLE(column)=[just=l cellwidth=10%];
	DEFINE AssetDescription / 'Asset Description' DISPLAY STYLE(column)=[just=l cellwidth=25%];
	DEFINE wonum / 'Work Order' DISPLAY STYLE(column)=[just=l cellwidth=10%];
	DEFINE description / 'Work Order Description' DISPLAY STYLE(column)=[just=l cellwidth=30%];
	DEFINE LocalScheduleStart / 'Local Schedule Start' DISPLAY STYLE(column)=[just=l cellwidth=15%];
	DEFINE status / 'Status' DISPLAY STYLE(column)=[just=l cellwidth=5%];
	QUIT;
	TITLE;
%END;

ods html close;
/*END Create Table Report*/
%Email(&Plant);

%MEND;		


