OPTIONS MINOPERATOR NOMPRINT VALIDVARNAME=ANY FULLSTIMER CASDATALIMIT=5G;

ODS PATH(PREPEND) WORK.TEMPLAT(UPDATE);

%GLOBAL email workdir PGTDDHH SkipDecision OvenHealth DTS;

%LET PGT = %SYSFUNC(DATETIME(),datetime.);
%LET PGTMinute= %SYSFUNC(MINUTE(%SYSFUNC(DATETIME())));
%LET PGTHour= %SYSFUNC(HOUR(%SYSFUNC(DATETIME())));
%LET PGTDay= %SYSFUNC(DAY(%SYSFUNC(DATEPART(%SYSFUNC(DATETIME())))));
%LET PGTDDHHMM = &PGTDay&PGTHour&PGTMinute;
%PUT &PGTDDHHMM;
LIBNAME SASMEEXT '/sasdata/me/external_sources';
LIBNAME CHTEMAIL '/sasdata/StudioTopLevel/Projects/S6_CHTVPEmail';
CAS mySession SESSOPTS=(CASLIB=PUBLIC TIMEOUT=1800 LOCALE="en_US");
LIBNAME PUBLIC CAS CASLIB="PUBLIC";

%LET workdir=%trim(%sysfunc(pathname(work)));
%PUT &workdir;

%MACRO DELETEDSIFEXISTS(LIB, NAME);
    %IF %SYSFUNC(EXIST(&LIB..&NAME.)) %THEN %DO;
		%PUT DeletDSifExistsMacro;
		PROC DATASETS LIBRARY=&LIB. NOLIST;
	        DELETE &NAME.;
	    QUIT;
	%END;
%MEND;

%MACRO UniformDelay(N=5);
	%PUT Insert Random Delay, Between 0 and 5 Seconds;

	DATA _null_;
		UniformDelay = rand('Uniform');
		rc=SLEEP(UniformDelay*5,1);
	RUN;
%MEND;
%UniformDelay();

%MACRO TrimWorkLib(LIB);
	PROC SQL ;
	SELECT
	   COUNT(*)
	  ,MEMNAME INTO : CONT,  : NAME SEPARATED BY " "
	FROM
		DICTIONARY.TABLES
	WHERE
		UPCASE(LIBNAME) EQ "&LIB."
	;
	QUIT;
	%PUT &CONT;
	%PUT &NAME;

	%DO %UNTIL (&CONT.);
	     PROC DATASETS LIB = &LIB. MEMTYPE = DATA NOLIST;
	        DELETE &NAME.;
	     RUN;
	%END;
%MEND;

%MACRO FileCheck(Dataset, Image);
/*%put Checking to see if the file exists;*/
%let count = 0;
data _NULL_;
	if 0 then set &Dataset nobs=n;
	call symputx('count',n);
	stop;
run;
%put nobs=&count;

%if &count >= 1 %then %do;
%put do nothing;
%end;
%else %do;
%put assign default image;

filename in FILESRVC folderpath='/Applications and Reporting/Programs/ApplicationsandReporting/Email/DefaultImage/' filename='DataNotFound.png'; 
filename out "&workdir./&Image..png";
	 
/* copy the file byte-for-byte  */
data _null_;
length filein 8 fileid 8;
filein = fopen('in','I',1,'B');
fileid = fopen('out','O',1,'B');
rec = '20'x;
do while(fread(filein)=0);
rc = fget(filein,rec,1);
rc = fput(fileid, rec);
rc = fwrite(fileid);
end;
rc = fclose(filein);
rc = fclose(fileid);
run;
	 
filename in clear;
filename out clear;
%end;

%MEND;

FILENAME REPORT "%sysfunc(pathname(work))/test.html";

%MACRO Email(Lineage) / MINOPERATOR;

ODS _ALL_ CLOSE;
ODS HTML; 

%LET email = ''; 

PROC SQL NOPRINT;
SELECT COMPRESS(SUBSTR(EquipmentName, 1, 6))||' '||'CHT'||" "||"&DTS."||':'||' '||SDSubject
	INTO: Subject 
FROM WORK.Summary;
QUIT;

%PUT &Subject;

PROC SQL NOPRINT;
	SELECT "'"||btrim(EmailAddress)||"'" INTO :email SEPARATED BY ' ' 
	FROM SASMEEXT.CHTEMAILLIST 
	WHERE 
		SUBSTR(Lineage, 1, 6) = "&Lineage.";
QUIT;

%PUT &Lineage;
%PUT &email;

filename SEND email
        TO=(&email)
		FROM=('S6CHTMagic8Ball<CHTM8B@FirstSolar.com>')
		SENDER=('S6PassDownHub <NOREPLY@firstsolar.com>')
		IMPORTANCE='HIGH'
		Content_type="Text/HTML"
		replyto='NOREPLY@FirstSolar.com'
		Subject="&Subject."
		attach=("&workdir./&Lineage._email1.png" inlined="&Lineage._&PGTDDHHMM._email1.png");

data _null_;
  infile REPORT;
  file SEND;
  input;
if _infile_ ne '</html>' then put _infile_;
	else do;
		put '<p><span style="font-family: ' courier new', courier;">For more information, please visit <a href="https://viya.fs.local/links/resources/report?uri=%2Freports%2Freports%2F2a8a63a7-5c2e-4ad6-8a12-c8a0dff6f6d3&page=vi77652"><span style="color:blue">SAS Summary Report</span></a></span></p>';
		put '<p><span style="font-family: ' courier new', courier;">If you have any questions, please contact <a href = "mailto:AdminMfgVirtualProfile@firstsolar.com"><span style="color:blue">Mfg Admin Group</span></a></span></p>';
		put '<p>&nbsp;</p>';
	    put '<p><span style="font-family: ' courier new', courier;">Thank You,</span></p>';
		put '<p><span style="color: #ff6600; font-family: ' courier new' , courier;">S6ReportingFolks</span></p>';
	end;
run;

ODS HTML CLOSE;
%MEND;

%MACRO CHTReport(Lineage);
OPTIONS NOCENTER;
ODS HTMLCSS STYLE=Dove FILE=REPORT;

/* Upcoming Profile Work Orders */
Title HEIGHT=3 "Upcoming Profile Work Orders";
PROC REPORT DATA=WORK.Upcoming_ProfilePM 
STYLE(REPORT) = {OUTPUTWIDTH=100%}
STYLE(HEADER) =	[JUST = CENTER FONT_FACE = calibri FONT_SIZE = 3 FOREGROUND = Black BACKGROUND = Orange PROTECTSPECIALCHARS=OFF];
COLUMN wonum controlsid description assetnum jpnum LocalSchedStart crew;
DEFINE wonum / DISPLAY ORDER=DATA STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3] 'Work Order Number';
DEFINE controlsid / DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3] 'Controls ID';
DEFINE description / DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3] 'WO Description';
DEFINE assetnum / DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3] 'Asset Number';
DEFINE jpnum / DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3] 'Job Plan Number';
DEFINE LocalSchedStart / DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3] 'Local Scheduled Start';
DEFINE crew / DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3] 'Crew';
QUIT;
TITLE;

/*Summary Table*/
PROC SQL NOPRINT;
CREATE TABLE WORK.Summary AS
SELECT
	EquipmentName
	,'Yes' AS SkipDecision
	,Model
	,Model_Health
	,Ramp
	,Soak
	,Oven_Instability
	,Zone1_OvenInstability
	,'Zone2-Zone22_OvenInstability'n
	,VP_Fail
	,VP_Pass
	,NextProfileSkipable
	,BeltSpeed
	,TargetSoak
	,Technology
	,TimeStamp
	,'SKIP PROFILE' AS SDSubject
FROM PUBLIC.ME_DEP_CHT_CompositeSummary
WHERE SUBSTR(EquipmentName, 1, 6) = "&Lineage"
ORDER BY EquipmentName, TimeStamp;
QUIT;

PROC SORT DATA=WORK.Summary;
BY EquipmentName TimeStamp;
RUN;

PROC SORT DATA=CHTEMAIL.ME_DEP_CHT_M8BHistory;
BY EquipmentName TimeStamp;
RUN;

DATA CHTEMAIL.ME_DEP_CHT_M8BHistory;
MERGE CHTEMAIL.ME_DEP_CHT_M8BHistory WORK.Summary;
BY EquipmentName TimeStamp;
RUN;

DATA _NULL_;
SET WORK.Summary;
CALL SYMPUT('Tech', Technology);
CALL SYMPUT('TargetSoak', TargetSoak);
RUN;

%LET Tech2 = %SYSFUNC(COMPRESS(&Tech));
%LET TargetSoak2 = %SYSFUNC(COMPRESS(&TargetSoak));
%PUT &Tech2;
%PUT &TargetSoak2;

Title HEIGHT=3 "Summary";
PROC REPORT DATA=WORK.Summary
STYLE(REPORT) = {OUTPUTWIDTH=100%}
STYLE(HEADER) =	[JUST = CENTER FONT_FACE = calibri FONT_SIZE = 3 FOREGROUND = Black BACKGROUND = Orange PROTECTSPECIALCHARS=OFF];
COLUMN EquipmentName SkipDecision Model Model_Health Ramp Soak Oven_Instability VP_Fail VP_Pass NextProfileSkipable BeltSpeed TimeStamp;
DEFINE EquipmentName / "Line" DISPLAY ORDER=DATA STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3];
DEFINE SkipDecision / "Skip Decision" DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3];
DEFINE Model / DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3];
DEFINE Model_Health / DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3];
DEFINE Ramp / DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3];
DEFINE Soak / DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3];
DEFINE Oven_Instability / DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3];
DEFINE VP_Fail / DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3];
DEFINE VP_Pass / DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3 BACKGROUNDCOLOR=$cback.];
DEFINE NextProfileSkipable / "Next Profile Skipable" DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3];
DEFINE Beltspeed / DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3];
DEFINE TimeStamp / DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3];
COMPUTE SkipDecision;
IF SkipDecision = 'Yes' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CX00FF00}");
IF SkipDecision = 'EA' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFFFF00}");
IF SkipDecision = 'No' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFF0000}");
IF SkipDecision = 'Not Eligible' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFFA500}");
ENDCOMP;
COMPUTE Model_Health;
IF Model_Health = 'Yes' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CX00FF00}");
IF Model_Health = 'EA' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFFFF00}");
IF Model_Health = 'No' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFF0000}");
ENDCOMP;
COMPUTE Ramp;
IF Ramp <= 4.5 THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CX00FF00}");
IF 4.5 < Ramp <= 9 THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFFFF00}");
IF Ramp > 9 THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFF0000}");
ENDCOMP;
COMPUTE Soak;
IF Soak <= 4.5 THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CX00FF00}");
IF 4.5 < Soak <= 9 THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFFFF00}");
IF Soak > 9 THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFF0000}");
ENDCOMP;
COMPUTE Oven_Instability;
IF Oven_Instability = 0 THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CX00FF00}");
IF Oven_Instability = 1 THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFFFF00}");
IF Oven_Instability > 1 THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFF0000}");
ENDCOMP;
COMPUTE VP_Fail;
IF VP_Fail = 0 THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CX00FF00}");
IF 1 <= VP_Fail <= 5 THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFFFF00}");
IF VP_Fail >= 6 THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFF0000}");
ENDCOMP;
COMPUTE VP_Pass;
IF VP_Pass = 'Yes' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CX00FF00}");
IF VP_Pass = 'EA' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFFFF00}");
IF VP_Pass = 'No' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFF0000}");
ENDCOMP;
COMPUTE NextProfileSkipable;
IF NextProfileSkipable = 'Yes' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CX00FF00}");
IF NextProfileSkipable = 'No' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFFA500}");
ENDCOMP;
QUIT;
TITLE;

/*virtual within 15min - Plot*/
PROC SQL;
CREATE TABLE WORK.Virtual15Min AS
SELECT 
	Lineage
	,Zone
	,ReadTime15
    ,(UCL - Target) AS UB
    ,(LCL - Target) AS LB
    ,(CASE WHEN 'TC Location'n = 'LL' THEN Delta ELSE . END) AS LL_Delta
    ,(CASE WHEN 'TC Location'n = 'L' THEN Delta ELSE . END) AS L_Delta
    ,(CASE WHEN 'TC Location'n = 'C' THEN Delta ELSE . END) AS C_Delta
    ,(CASE WHEN 'TC Location'n = 'R' THEN Delta ELSE . END) AS R_Delta
    ,(CASE WHEN 'TC Location'n = 'RR' THEN Delta ELSE . END) AS RR_Delta
FROM PUBLIC.ME_DEP_CHT_COMBINEVP15
WHERE Lineage = "&Lineage."
ORDER BY Zone;
QUIT;

DATA _NULL_;
SET WORK.Virtual15Min;
CALL SYMPUT('TimeStamp', ReadTime15);
RUN;

%LET TimeStamp2 = %SYSFUNC(COMPRESS(%SYSFUNC(PUTN(&TimeStamp,DATETIME23.))));
%PUT &TimeStamp2;

ODS LISTING GPATH="&workdir.";
ODS GRAPHICS / WIDTH=15in HEIGHT=6in;
ODS GRAPHICS / RESET=index outputfmt=PNG IMAGENAME="&Lineage._email";

PROC SGPLOT DATA=WORK.Virtual15Min;
TITLE J=LEFT H=12pt "&Lineage. &Tech2. @&TargetSoak2. Virtual within 15min - &TimeStamp2.";
STYLEATTRS BACKCOLOR=CXE0E0E0;
SERIES X=Zone Y=LL_Delta / LINEATTRS=(COLOR=CX53CFE0 THICKNESS=3 PATTERN=MediumDashShortDash) MARKERS MARKERATTRS=(SYMBOL=SquareFilled SIZE=9 COLOR=CX53CFE0);
SERIES X=Zone Y=L_Delta / LINEATTRS=(COLOR=CX194393 THICKNESS=3) MARKERS MARKERATTRS=(SYMBOL=CircleFilled SIZE=9 COLOR=CX194393);
SERIES X=Zone Y=C_Delta / LINEATTRS=(COLOR=CX7F0F00 THICKNESS=3) MARKERS MARKERATTRS=(SYMBOL=TriangleFilled SIZE=9 COLOR=CX7F0F00);
SERIES X=Zone Y=R_Delta / LINEATTRS=(COLOR=CX749800 THICKNESS=3) MARKERS MARKERATTRS=(SYMBOL=DiamondFilled SIZE=9 COLOR=CX749800);
SERIES X=Zone Y=RR_Delta / LINEATTRS=(COLOR=CX7C7E82 THICKNESS=3 PATTERN=MediumDashShortDash) MARKERS MARKERATTRS=(SYMBOL=StarFilled SIZE=9 COLOR=CX7C7E82);
SERIES X=Zone Y=UB / LINEATTRS=(COLOR=CX000000 THICKNESS=3 PATTERN=MediumDash);
SERIES X=Zone Y=LB / LINEATTRS=(COLOR=CX000000 THICKNESS=3 PATTERN=MediumDash);
REFLINE 0 / AXIS=Y LINEATTRS=(COLOR=CX000000);
XAXIS GRID DISPLAY=ALL VALUES=(1 TO 22 BY 1);
YAXIS GRID DISPLAY=(NOLABEL) MAX=15 MIN=-15;
RUN;
TITLE;

/* make a copy of the email image to be inserted into the HTML portion of the email */
FILENAME INa "&workdir./&Lineage._email.png";
FILENAME OUTa "&workdir./&Lineage._email1.png";

/* This code actually copies the image from the IN filename reference to the out filename reference */
DATA _NULL;
LENGTH filein 8 fileid 8;
filein = FOPEN('ina','I',1,'B');
fileid = FOPEN('outa','O',1,'B');
rec = '20'x;
DO WHILE(FREAD(filein)=0);
rc = FGET(filein,rec,1);
rc = FPUT(fileid, rec);
rc = FWRITE(fileid);
END;
rc = FCLOSE(filein);
rc = FCLOSE(fileid);
run;

/* clear the filename refernces for future use */
FILENAME INa CLEAR;
FILENAME OUTa CLEAR;

%FileCheck(WORK.Virtual15Min, &Lineage._email1);

/*virtual within 15min - Table*/
PROC SQL;
CREATE TABLE WORK.Virtual15Table AS
SELECT
	Lineage
	,Zone
	,LL
	,L
	,C
	,R
	,RR
FROM PUBLIC.ME_DEP_CHT_CombineVPMatrix
WHERE Lineage = "&Lineage."
ORDER BY Zone;
QUIT;

PROC REPORT DATA=WORK.Virtual15Table
STYLE(REPORT) = {OUTPUTWIDTH=100%}
STYLE(HEADER) =	[JUST = CENTER FONT_FACE = calibri FONT_SIZE = 3 FOREGROUND = Black BACKGROUND = Orange PROTECTSPECIALCHARS=OFF];
COLUMN Lineage Zone LL L C R RR;
DEFINE Lineage / DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3];
DEFINE Zone / DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3];
DEFINE LL / DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3];
DEFINE L / DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3];
DEFINE C / DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3];
DEFINE R / DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3];
DEFINE RR / DISPLAY STYLE(COLUMN)=[JUST=l FONT_FACE = calibri FONT_SIZE = 3];
COMPUTE LL;
IF LL = 'Pass' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CX00FF00}");
IF LL = 'Unkn' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFFFF00}");
IF LL = 'Fail' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFF0000}");
ENDCOMP;
COMPUTE L;
IF L = 'Pass' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CX00FF00}");
IF L = 'Unkn' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFFFF00}");
IF L = 'Fail' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFF0000}");
ENDCOMP;
COMPUTE C;
IF C = 'Pass' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CX00FF00}");
IF C = 'Unkn' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFFFF00}");
IF C = 'Fail' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFF0000}");
ENDCOMP;
COMPUTE R;
IF R = 'Pass' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CX00FF00}");
IF R = 'Unkn' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFFFF00}");
IF R = 'Fail' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFF0000}");
ENDCOMP;
COMPUTE RR;
IF RR = 'Pass' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CX00FF00}");
IF RR = 'Unkn' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFFFF00}");
IF RR = 'Fail' THEN CALL DEFINE(_COL_, "STYLE", "STYLE={BACKGROUND=CXFF0000}");
ENDCOMP;
QUIT;

ODS _ALL_ CLOSE;

%Email(&Lineage);
%MEND;

%Macro chtvirtualprofileemaileta(lineage) / MINOPERATOR;
%LET PBG = 		%SYSFUNC(DATETIME(),datetime.);
%LET PBGHour= 	%SYSFUNC(HOUR(%SYSFUNC(DATETIME())));
%LET PBGDate= 	%SUBSTR(%SYSFUNC(PUTN(%SYSFUNC(DATE()), MMDDYY.)), 1, 5);
%LET UDT = 		%SYSFUNC(tzones2u(%SYSFUNC(DATETIME())),datetime.);
%LET KMT = 		%SYSFUNC(tzoneu2s(%SYSFUNC(tzones2u(%SYSFUNC(DATETIME()))),Asia/Singapore),datetime.);
%LET KMTHour= 	%SYSFUNC(HOUR(%SYSFUNC(tzoneu2s(%SYSFUNC(tzones2u(%SYSFUNC(DATETIME()))),Asia/Singapore))));
%LET KMTDate= 	%SUBSTR(%SYSFUNC(PUTN(%SYSFUNC(DATEPART(%SYSFUNC(tzoneu2s(%SYSFUNC(tzones2u(%SYSFUNC(DATETIME()))),Asia/Singapore)))), MMDDYY8.)), 1, 5);
%LET DMT = 		%SYSFUNC(tzoneu2s(%SYSFUNC(tzones2u(%SYSFUNC(DATETIME()))),Asia/Saigon),datetime.);
%LET DMTHour= 	%SYSFUNC(HOUR(%SYSFUNC(tzoneu2s(%SYSFUNC(tzones2u(%SYSFUNC(DATETIME()))),Asia/Saigon))));
%LET DMTDate= 	%SUBSTR(%SYSFUNC(PUTN(%SYSFUNC(DATEPART(%SYSFUNC(tzoneu2s(%SYSFUNC(tzones2u(%SYSFUNC(DATETIME()))),Asia/Saigon)))), MMDDYY8.)), 1, 5);

%PUT NOTE:  UDT Time is:  &UDT;
%PUT NOTE:  PBG Time is:  &PBG;
%PUT NOTE:  PBG Hour is:  &PBGHour;
%PUT NOTE:  PBG Date is:  &PBGDate;
%PUT NOTE:  KMT Time is:  &KMT;
%PUT NOTE:  KMT Hour is:  &KMTHour;
%PUT NOTE:  KMT Date is:  &KMTDate;
%PUT NOTE:  DMT Time is:  &DMT;
%PUT NOTE:  DMT Hour is:  &DMTHour;
%PUT NOTE:  DMT Date is:  &DMTDate;

%LET Site = %Substr(&lineage,1,3);
%PUT NOTE:  Site = &Site;

/*Get Data for Upcoming Scheduled Profiles*/
cas mySession sessopts=(caslib=casuser timeout=1800 locale="en_US");
caslib _all_ assign;

PROC SQL;
CREATE TABLE WORK.Upcoming_ProfilePM AS
SELECT
	t1.wonum,
	t1.controlsid,
	t1.description,
	t1.assetnum,
	t1.jpnum,
	t1.schedstart +	3600*t1.TimeZoneOffset FORMAT DATETIME23. AS LocalSchedStart,
	t1.crew
FROM
	PUBLIC.ME_DEP_CHT_ProfilePM t1
WHERE
	ShiftAgo = -1
	AND SUBSTR(controlsid,1,6) = "&lineage."
;
SELECT COUNT(*) INTO :NProfile FROM WORK.Upcoming_ProfilePM;
QUIT;
RUN;

%PUT &NProfile profiles scheduled for &Lineage during next shift;


%UniformDelay;

%IF &Site = PGT AND &NProfile > 0
	%THEN %DO;
		%PUT NOTE: Time to Run PBG;	
		%LET DTS = &PBGDate;
		%CHTReport(&lineage); %TrimWorkLib(WORK);      
	%END;
%ELSE %DO;
	%PUT NOTE: Not Time to Run PBG or no profiles scheduled;
%END;

%IF &Site = KMT AND &NProfile > 0
	%THEN %DO;
		%PUT NOTE: Time to Run KLM;
		%LET DTS = &KMTDate;
		%CHTReport(&lineage); %TrimWorkLib(WORK);  
	%END;
%ELSE %DO;
	%PUT NOTE: Not Time to Run KLM or no profiles scheduled;
%END;

%IF &Site = DMT AND &NProfile > 0
	%THEN %DO;
		%PUT NOTE: Time to Run DMT;
		%LET DTS = &DMTDate;
		%CHTReport(&lineage); %TrimWorkLib(WORK);  
	%END;
%ELSE %DO;
	%PUT NOTE: Not Time to Run DMT or no profiles scheduled;
%END;
%MEND;