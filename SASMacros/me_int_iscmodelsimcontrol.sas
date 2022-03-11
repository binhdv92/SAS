

%MACRO me_int_iscmodelsimcontrol(SPUL=,Action=,Reason=);

libname IscLib '/sasdata/Projects/S6_IscModeling';

/*  NEED:  when retraining models, the action should be set to what it was prior to retraining*/

%IF %SYSFUNC(LENGTH(&SPUL)) = 6 AND %SYSFUNC(LENGTH(&Action)) > 0 %THEN %DO;
	%IF %UPCASE(&Action) = ENABLE %THEN %DO;
		PROC SQL; UPDATE IscLib.ModelStudioProjects SET Active = 1 WHERE SPUL = "&SPUL."; QUIT;
	%END;
	%IF %UPCASE(&Action) = DISABLE %THEN %DO;
		PROC SQL; UPDATE IscLib.ModelStudioProjects SET Active = 0 WHERE SPUL = "&SPUL."; QUIT;
	%END;
	
	/*  LOG ME_INT_IscOffsetProjectsHistory*/
	
	%LET Action2=%UPCASE(&Action); 
	%put &sysuserid ; 
	
	data _null_;
	 length 
	     UDTTime 8
	     utc_offset 8;
	  UDTTime=tzones2u(%SYSFUNC(DATETIME()));
	  utc_offset=gmtoff()/3600; /* undocumented */
	  call symput('utc_offset',utc_offset);
	  call symput('UDTTime',UDTTime);
	run;
	
	%put &UDTTime &utc_offset;
	
	PROC SQL;
	CREATE TABLE work.ME_INT_IscOffsetProjectsHistory 
		(SPUL char(6)
		,action char(7)
		,userID char(32)
		,UTCTimeStamp num format=datetime.
		,UTCOffset num
		,Reason char(32));
	INSERT INTO work.ME_INT_IscOffsetProjectsHistory
		values("&SPUL.", "&Action2.", "&sysuserid.", &UDTTime, &utc_offset, "&reason.");
	QUIT;
	
	/* Manage ME_INT_IscOffsetProjectsHistory data in DPPublic and Public*/
	%if %sysfunc(exist(IscLib.ME_INT_IscOffsetProjectsHistory))=1 AND %sysfunc(exist(PUBLIC.ME_INT_IscOffsetProjectsHistory))=1 %then %do;
		%Put IscLib Exist and Public Exist;
		DATA IscLib.ME_INT_IscOffsetProjectsHistory;
			SET IscLib.ME_INT_IscOffsetProjectsHistory (WHERE=(UTCTimeStamp >=(&UDTTime-86400*90))) WORK.ME_INT_IscOffsetProjectsHistory;
		RUN;
	%end;
	%if %sysfunc(exist(IscLib.ME_INT_IscOffsetProjectsHistory))=0 AND %sysfunc(exist(PUBLIC.ME_INT_IscOffsetProjectsHistory))=1 %then %do;
		%Put IscLib !Exist and Public Exist;
		DATA IscLib.ME_INT_IscOffsetProjectsHistory;
			SET IscLib.ME_INT_IscOffsetProjectsHistory (WHERE=(UTCTimeStamp >=(&UDTTime-86400*90))) WORK.ME_INT_IscOffsetProjectsHistory;
		RUN;
	%END;
	%if %sysfunc(exist(IscLib.ME_INT_IscOffsetProjectsHistory))=1 AND %sysfunc(exist(PUBLIC.ME_INT_IscOffsetProjectsHistory))=0 %then %do;
		%Put IscLib Exist and Public !Exist;
		DATA IscLib.ME_INT_IscOffsetProjectsHistory;
			SET IscLib.ME_INT_IscOffsetProjectsHistory (WHERE=(UTCTimeStamp >=(&UDTTime-86400*90))) WORK.ME_INT_IscOffsetProjectsHistory;
		RUN;
	%END;
	%if %sysfunc(exist(IscLib.ME_INT_IscOffsetProjectsHistory))=0 AND %sysfunc(exist(PUBLIC.ME_INT_IscOffsetProjectsHistory))=0 %then %do;
		%Put IscLib !Exist and Public !Exist;
		DATA IscLib.ME_INT_IscOffsetProjectsHistory;
			SET WORK.ME_INT_IscOffsetProjectsHistory;
		RUN;	
	%END;
	
	data work.ME_INT_IscOffsetProjectsStatus;
		SET IscLib.ModelStudioProjects;
	run;
	
	%checkcasadhocresult(work, ME_INT_IscOffsetProjectsStatus, public, ME_INT_IscOffsetProjectsStatus, 'tshields@firstsolar.com');
	%checkcasadhocresult(IscLib, ME_INT_IscOffsetProjectsHistory, public, ME_INT_IscOffsetProjectsHistory, 'tshields@firstsolar.com');
%END;

%MEND;

