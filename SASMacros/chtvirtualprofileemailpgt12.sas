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
CAS mySession SESSOPTS=(CASLIB=PUBLIC TIMEOUT=1800 LOCALE="en_US");
LIBNAME PUBLIC CAS CASLIB="PUBLIC";

%LET workdir=%trim(%sysfunc(pathname(work)));
%PUT &workdir;

%MACRO UniformDelay();
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
SELECT COMPRESS(SUBSTR(controlsid, 1, 6))||' '||'CHT'||" "||"&DTS."||':'||' '||'RUN PROFILE'
	INTO: Subject 
FROM WORK.Upcoming_ProfilePM;
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
/*         TO=('xiaoyan.li@firstsolar.com') */
		FROM=('S6CHTMagic8Ball<CHTM8B@FirstSolar.com>')
		SENDER=('S6PassDownHub <NOREPLY@firstsolar.com>')
		IMPORTANCE='HIGH'
		Content_type="Text/HTML"
		replyto='NOREPLY@FirstSolar.com'
		Subject="&Subject.";
		/*attach=("&workdir./&Lineage._email1.png" inlined="&Lineage._&PGTDDHHMM._email1.png");*/

data _null_;
  infile REPORT;
  file SEND;
  input;
if _infile_ ne '</html>' then put _infile_;
	else do;
		put '<p><span style="color: #ff0000; font-size: 16pt; font-family: ' courier new', courier;">MAGIC 8-BALL RESULTS COMING SOON</span></p>';
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

ODS _ALL_ CLOSE;

%Email(&Lineage);
%MEND;

%Macro chtvirtualprofileemailpgt12(lineage) / MINOPERATOR;
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

%IF &Site = PGT AND &PBGHour IN 5 17 AND &NProfile > 0
	%THEN %DO;
		%PUT NOTE: Time to Run PBG;	
		%LET DTS = &PBGDate;
		%CHTReport(&lineage); %TrimWorkLib(WORK);      
	%END;
%ELSE %DO;
	%PUT NOTE: Not Time to Run PBG or no profiles scheduled;
%END;

%IF &Site = KMT AND &KMTHour IN 6 18 AND &NProfile > 0
	%THEN %DO;
		%PUT NOTE: Time to Run KLM;
		%LET DTS = &KMTDate;
		%CHTReport(&lineage); %TrimWorkLib(WORK);  
	%END;
%ELSE %DO;
	%PUT NOTE: Not Time to Run KLM or no profiles scheduled;
%END;

%IF &Site = DMT AND &DMTHour IN 5 17 AND &NProfile > 0
	%THEN %DO;
		%PUT NOTE: Time to Run DMT;
		%LET DTS = &DMTDate;
		%CHTReport(&lineage); %TrimWorkLib(WORK);  
	%END;
%ELSE %DO;
	%PUT NOTE: Not Time to Run DMT or no profiles scheduled;
%END;
%MEND;

