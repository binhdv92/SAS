%macro _timer(name,state);
%let NOTES = %sysfunc(getoption(notes));
OPTIONS NONOTES;

%IF %length(&state)>0 %THEN %DO; /*Check for Missing State*/
	%IF &State = 1 OR %SYSFUNC(UPCASE(&State))=START OR %SYSFUNC(UPCASE(&State))=GO %THEN %DO;
		%LET Action = 1;
	%END;	
	%ELSE %DO;
		%LET Action = 0;
	%END;
%END;
%ELSE %DO;
	%LET Action = 0;
%END;

%if &action=1 %then %do;
	%global _&name.;
	%let _&name. = %sysfunc(datetime());
/* 	%put &&_&name.; */
%end;

%if &action=0 %then %do;

%put TIMER NAME:  &name;
/* Stop timer */
data _null_;
  dur = datetime() - &&_&name.;
  put 30*'-' / ' TOTAL DURATION:' dur time13.2 / 30*'-';
run;
%end;
OPTIONS &NOTES;
%mend;