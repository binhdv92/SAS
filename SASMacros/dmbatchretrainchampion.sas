
/******************************************************************************
** Macro: Data Mining and Machine Learning Batch Retrain Get Champion Model
**
** Description: Issue a request to get the champion model
**
******************************************************************************/

%macro dmbatchretrainchampion(projectId);

%let servicesBaseUrl =;

data _null_;
   length string $ 1024;
   string= getoption('SERVICESBASEURL');
   call symput('servicesBaseUrl', trim(string));
run;

%PUT &servicesBaseUrl;

filename resp TEMP;
filename headers TEMP;

%let retrainingChampionUri=&servicesBaseUrl.analyticsGateway/projects/&projectId/retrainJobs/@lastJob/champion;

proc http
  method="GET"
  oauth_bearer=sas_services
  url="&retrainingChampionUri"
  headerout=headers
  out=resp;
  headers
  "Accept"="application/vnd.sas.analytics.data.mining.model+json";
run;

data _null_;
 infile resp;
 input;
 put _infile_;
run;

%mend;