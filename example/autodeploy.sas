%MACRO AutoDeploy(path, program);
/* %Let path = /Users/xxxxxx/My Folder/TestStuff; */
/* %let program = xxxxx; */

/* filename using FILESRVC to the destination a sas program and folder */

filename src filesrvc folderpath="&path." filename="&program..sas" /*debug=http*/;

filename DP filesrvc folderpath="/Applications and Reporting/Programs/DeployedPrograms" filename="&program..sas" /*debug=http*/;

filename DJ filesrvc folderpath="/Applications and Reporting/Programs/DeployedJobs" filename="&program..sas" /*debug=http*/;

/* copy the file: output return code and any message */
data _null_;
rc=fcopy("src","DP");
msg=sysmsg();
put rc=;
put msg=;
run;

/* Create Deployed Job if it does not exist*/
%if %SYSFUNC(FEXIST(DJ)) 
%then %do;
		%put Deployed Job Already Exists;
%end;
%else %do;
	data _null_;
	  file DJ;
	  put "/* Created by auto deploy macro */"
	
	/"filename JobProg FILESRVC folderpath='/Applications and Reporting/Programs/DeployedPrograms' "
	/"filename='&program..sas';"
	/"%include JobProg;";
	run;
%end;
%MEND;
