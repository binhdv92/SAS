OPTIONS CASDATALIMIT=2G NOTES MPRINT;
options cashost="azr1sas01n100.fs.local";
%GLOBAL email workdir attach;
/* LIBNAME SASMEEXT META LIBRARY="SAS_ME_External_Sources" METAOUT=DATA; */
cas _all_ terminate;
cas mySession sessopts=(caslib=public timeout=1800 locale="en_US");
libname SASMEEXT '/sasdata/me/external_sources';
LIBNAME LASRLIB CAS CASLIB="PUBLIC";

%LET workdir=%trim(%sysfunc(pathname(work)));

caslib _all_ assign;

%MACRO Email(Subject) / MINOPERATOR;

ODS _ALL_ CLOSE;
ODS HTML;

/* %LET Subject = PGT1; */
%LET Email= %SYSFUNC(COMPRESS(%bquote(')&Subject.WASHER_Filter_WO@firstsolar.com%bquote(')));
%put &email;

/* %LET email = ''; */


filename SEND email
/*  		TO=('tshields@firstsolar.com') */
/*  	TO=('tshields@firstsolar.com' 'aaron.dick@firstsolar.com' ) */
		TO=(&email)
		BCC=('tshields@firstsolar.com')
		FROM=('WetLineEarlyWarning <WetLineEarlyWarning@firstsolar.com>')
		SENDER = ('WetLineEarlyWarning <NOREPLY@firstsolar.com>')
		IMPORTANCE='HIGH'
		Content_type="Text/HTML"
		replyto='NOREPLY@FirstSolar.com'
		Subject="Wet Tool Filter & PM Docket for &Subject."
/* 		attach=("&workdir./&plant._email1.pdf") */
		&attach;

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
		PUT '<html>';
		PUT '<body>';
/* 		PUT "<img src=cid:&Plant._&UDT._FlowPlots>"; */
		PUT '</body>';
		PUT '</html>';
		PUT '<p><span style="font-family: ' courier new', courier;">For the values in the Filter Warning Table, notification is based on a 5 hour window with the most hour being report.  Actual values at the tool may be differnt.</span></p>';
		PUT '<p>&nbsp;</p>';
		PUT '<p><span style="font-family: ' courier new', courier;">Thank You,</span></p>';
		PUT '<p><span style="color: #ff6600; font-family: ' courier new' , courier;">SASMfgEngSupport</span></p>';
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
%LET DMTHour =  %EVAL(%SYSFUNC(FLOOR((%SYSFUNC(tzoneu2s(%SYSFUNC(tzones2u(%SYSFUNC(DATETIME()))),Asia/Saigon)))/3600))*3600);

%PUT NOTE:  UDT Date is:  &UDT;
%PUT NOTE:  PGT Date is:  &PGT;
%PUT NOTE:  PGTHour Date is:  &PGTHour;
%PUT NOTE:  KMT Date is:  &KMT;
%PUT NOTE:  KMTHour Date is:  &KMTHour;
%PUT NOTE:  DMT Date is:  &DMT;
%PUT NOTE:  DMTHour Date is:  &DMTHour;

%MACRO wetbenchfilterwarning(Plant);

/* %LET Plant = PGT2; */

DATA work.FilterFlowEarlyWarn (KEEP=ReadTime EquipmentName Name Module AvgWarningLow AvgActual AvgWarningHigh HrsToWarn n Warn Urgent);
	SET PUBLIC.ME_SUB_FilterFlowEarlyWarn (where=(
			(
			AvgActual >= .25*AvgWarningLow
			AND
			(Substr('EquipmentName'n, 1, 3) = 'PGT' AND (ReadTime) >= (&PGTHour-6*3600) AND (ReadTime) < &PGTHour+2*3600) 
			OR
			(Substr('EquipmentName'n, 1, 3) = 'KMT' AND (ReadTime) >= (&KMTHour-6*3600) AND (ReadTime) < &KMTHour+2*3600)
			OR
			(Substr('EquipmentName'n, 1, 3) = 'DMT' AND (ReadTime) >= (&DMTHour-6*3600) AND (ReadTime) < &DMTHour+2*3600)
			)
			)
		);
	n = 1;
	Warn = 0;
	If (HrsToWarn >= 0 AND HrsToWarn <  24 AND HrsToWarn2 >= 0 AND HrsToWarn2 <  12 AND ABS(HrsToWarn-HrsToWarn2) < 6) THEN Warn = 1;
/* 	If (HrsToWarn < 12 AND HrsToWarn >= 0) THEN Warn = 1; */
	If (AvgActual < AvgWarningLow) THEN Warn = 1;
	IF (AvgActual < AvgWarningLow) THEN Urgent = 1;
RUN;

PROC SQL;
CREATE table work.mostrecent as
SELECT
	Max(ReadTime) as MaxReadTime, EquipmentName, Name, Module, SUM(n) AS nObs, SUM(Warn) as nWarnings, SUM(Urgent) AS nUrgent
FROM work.FilterFlowEarlyWarn
GROUP BY EquipmentName, Name, Module;
QUIT;

PROC SQL;
CREATE TABLE work.final AS
SELECT
	a.*, b.nObs, b.nWarnings, b.nUrgent
FROM 
	work.FilterFlowEarlyWarn a
	INNER JOIN work.mostrecent b on a.EquipmentName = b.EquipmentName AND a.Name= b.name and a.Module = b.Module and a.readtime = b.MaxReadTime
WHERE
	nWarnings >= 3 or Urgent = 1;
QUIT;

DATA work.final;
	SET work.final (WHERE=(Substr('EquipmentName'n, 1, 4) = "&Plant."));
RUN;

PROC SQL;
CREATE TABLE work.rawfinal AS
SELECT 
	a.*
	, catx( '-',a.'Module'n, a.'Name'n) As Stream
	, catx( '-',a.'Process'n, Substr(a.'EquipmentName'n, 5,2)) As ProcessUL
FROM 
	PUBLIC.ME_SUB_FilterFlowEarlyWarn a
	INNER JOIN work.final b on a.EquipmentName = b.EquipmentName AND a.Name= b.name and a.Module = b.Module 
where
	(
	a.AvgActual >= .25*a.AvgWarningLow
	AND
	(Substr(a.'EquipmentName'n, 1, 3) = 'PGT' AND (a.ReadTime) >= (&PGTHour-24*3600) AND (a.ReadTime) < &PGTHour+1*3600) 
	OR
	(Substr(a.'EquipmentName'n, 1, 3) = 'KMT' AND (a.ReadTime) >= (&KMTHour-24*3600) AND (a.ReadTime) < &KMTHour+1*3600)
	OR
	(Substr(a.'EquipmentName'n, 1, 3) = 'DMT' AND (a.ReadTime) >= (&DMTHour-24*3600) AND (a.ReadTime) < &DMTHour+1*3600)
	)
;
QUIT;

data work.rawfinal;
	set work.rawfinal;
	if FilterState = 'LOADED' THEN Loaded= AvgActual;
	if FilterState = 'LOADING' THEN Loading= AvgActual;
run;

PROC SQL NOPRINT;
SELECT COUNT(EquipmentName) INTO :NRecords FROM work.final;
QUIT;

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
	AND Area = 'Wets'
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
	PROC REPORT data=work.final
		style(report)={outputwidth=100%}
		style(header)=[just = left font_face = calibri font_size = 3 foreground = Black background = orange protectspecialchars= off];
		column ReadTime EquipmentName Name Module AvgActual AvgWarningLow AvgWarningHigh HrsToWarn;
		DEFINE ReadTime / display STYLE(column)=[just=l];
		DEFINE EquipmentName / display STYLE(column)=[just=l];
		DEFINE Module / display STYLE(column)=[just=l];
		DEFINE Name / display STYLE(column)=[just=l];
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
%IF &NRecords < 1 %THEN %DO;
	%put on data to plot;
	%LET Attach =;
	%put &attach;
%END;
%ELSE %DO;
	%LET Attach = attach=("&workdir./&plant._email1.pdf");
	%LET thing = %sysevalf(&NRecords/2,floor);
	%PUT &thing;
	
	ods _all_  close;
	ODS LISTING GPATH="&workdir.";
	ODS GRAPHICS / HEIGHT=7in WIDTH=10in;
/* 	ODS GRAPHICS / HEIGHT=%EVAL(4*&thing)in WIDTH=15in; */
/* 	ods graphics / reset=index outputfmt=PNG imagename="&Plant._email1"; */

options 
	orientation=landscape
	leftmargin=1cm
	rightmargin=1cm
	bottommargin=1cm
	topmargin=2cm;

	ods pdf file="&workdir./&Plant._email1.pdf" /*style=Ignite*/;
	/*Note that the image created will have an additional 1 in the file name due to use of proc sgpanel and its ability to create multiple output ods objects*/
	Title;
	proc sgpanel data=work.rawfinal aspect=.75;
	/* 			Title "&Message."; */

				panelby EquipmentName Stream / rows=1 columns=1 rowheaderpos=left  uniscale=column novarname;
 				*Scatter x=ReadTime  y=AvgActual / GROUP=FilterState2  markerattrs=(symbol=StarFilled Size=16) attrid=State/*Transparency=.9*/ ; 
				Scatter x=ReadTime  y=Loaded  /  markerattrs=(COLOR=Red symbol=CircleFilled Size=16) /*Transparency=.9*/ ; 
				Scatter x=ReadTime  y=Loading  /  markerattrs=(COLOR=Green symbol=DiamondFilled Size=16) /*Transparency=.9*/ ; 
				Scatter x=ReadTime  y=AvgWarningLow  /  markerattrs=(COLOR=Blue symbol=TriangleDownFilled Size=16) /*Transparency=.9*/ ; 
				Scatter x=ReadTime  y=AvgWarningHigh /  markerattrs=(COLOR=Orange symbol=TriangleFilled Size=16) /*Transparency=.9*/ ; 
	run;


	ods pdf close;

%END;

/* RUN; */

%Email(&Plant);

%MEND;		

