
/*****************************************************************************
** Macro: Data Mining and Machine Learning Batch Retrain
**
** Description: Invoke Data Mining and Machine Learning to retrain an existing
** project outside of the user interface.
** 
** This project was not created with a data plan.
** TEST CASE:  
    %let projectId = 502f8665-5ca7-469c-a3a5-393dce4d171b;
    %let datasourceUri = /dataTables/dataSources/cas~fs~cas-shared-adhoc~fs~Public/tables/ME_INT_ISCAD2_KMT11A_ALL3;
******************************************************************************/

%macro dmbatchretrain(projectId, datasourceUri);

%let servicesBaseUrl =;

data _null_;
   length string $ 1024;
   string= getoption('SERVICESBASEURL');
   call symput('servicesBaseUrl', trim(string));
run;

%PUT &servicesBaseUrl;

filename resp TEMP;
filename headers TEMP;

%let batchRetrainUrl=&servicesBaseUrl.dataMining/projects/&projectId/retrainJobs?dataUri=&datasourceUri%nrstr(&action)=batch;

proc http
  method="POST"
  oauth_bearer=sas_services
  url="&batchRetrainUrl"
  headerout=headers
  out=resp;
  headers
  "Accept"="application/vnd.sas.job.execution.job+json"  
   ;
run;

libname DMRRESP json fileref=resp;
proc sql noprint;
select value into:state from DMRRESP.alldata where p1='state';
quit;

%put Current State:  &state;

/* data _null_; */
/*  infile resp; */
/*  input; */
/*  put _infile_; */
/* run; */

%mend;

