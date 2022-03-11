
/******************************************************************************
** Macro: Data Mining and Machine Learning Batch Retrain Current Job
**
** Description: Issue a request to get the current batch retrain job
**
** TEST CASE:  %let projectId = 502f8665-5ca7-469c-a3a5-393dce4d171b;
******************************************************************************/

%macro dmbatchretraincurrentjob(projectId);

%let servicesBaseUrl =;

data _null_;
   length string $ 1024;
   string= getoption('SERVICESBASEURL');
   call symput('servicesBaseUrl', trim(string));
run;

/* %PUT &servicesBaseUrl; */

filename resp TEMP;
filename headers TEMP;

%let retrainingJobUri=&servicesBaseUrl.dataMining/projects/&projectId/retrainJobs/@currentJob;
/* %put &retrainingJobUri; */

proc http
  method="GET"
  oauth_bearer=sas_services
  url="&retrainingJobUri"
  headerout=headers
  out=resp;
  headers
  "Accept"="application/vnd.sas.job.execution.job+json";
run;

%LET state=;
libname APIRES json fileref=resp;
proc sql noprint;
select value into:state from apires.alldata where p1='state';
quit;

%put &retrainingJobUri:  &state;

/* data _null_; */
/*  infile resp; */
/*  input; */
/*  put _infile_; */
/* run; */

%mend;

