/******************************************************************************
** Macro: Data Mining and Machine Learning Batch Retrain Current Job
**
** Description: Issue a request to get the current batch retrain job
**
** TEST CASE:  
%let projectId = 502f8665-5ca7-469c-a3a5-393dce4d171b;
******************************************************************************/


%macro dmgetchampion(projectId);

%let servicesBaseUrl =;

data _null_;
   length string $ 1024;
   string= getoption('SERVICESBASEURL');
   call symput('servicesBaseUrl', trim(string));
run;

/* %PUT &servicesBaseUrl; */

filename resp TEMP;
filename headers TEMP;

%let retrainingJobUri=&servicesBaseUrl.analyticsGateway/projects/&projectId;

proc http
  method="GET"
  oauth_bearer=sas_services
  url="&retrainingJobUri"
  headerout=headers
  out=resp;
  headers
  "Accept"="application/json";
run;

%LET modelid=;
libname Model json fileref=resp;
proc sql noprint;
select value into:modelid from model.alldata where p1='providerSpecificProperties' AND p2='retrainingChampionModelId';
quit;

%put Current Model:  &modelid;

/* data _null_; */
/*  infile resp; */
/*  input; */
/*  put _infile_; */
/* run; */

%mend;

