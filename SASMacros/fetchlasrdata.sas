options casdatalimit=5G;

/* caslib publiclasr drop; */


%macro DropIt(name=);
proc casutil incaslib="public";
  droptable casdata="&name";   
/*   list files;                                      */
run; quit;
%mend;

%MACRO FetchLASRData( TableName );

cas mySession sessopts=(caslib=casuser timeout=1800 locale="en_US");
LIBNAME PUBLIC CAS CASLIB="PUBLIC";
libname dppublic base "/sasdata/DPPublic/" ;

caslib publiclasr datasource=(
 srctype="lasr"
 server="azr1sas01n711.fs.local"
 SIGNER="https://sas.fs.local:8343/SASLASRAuthorization"
 PORT=10031
	username="zsvc_sas_temp_reader"
	password="{SAS002}4EE08C3F2ED88C962C59FE3F0962B4204B2931DB5B3C0C503281508C"
 tag="VAPUBLIC" 
) 
 libref=lasrlib 
;
%if %sysfunc(exist(public.&TableName.)) %then %do;

	%put DeleteCASDSifExistsMacro;

	%DropIt(name=&TableName);

%end;

proc casutil incaslib="publiclasr" outcaslib="public";
      load casdata="&TableName" casout="&TableName"  PROMOTE importoptions=( filetype="lasr" ) ;
run;

DATA dppublic.&TableName replace;
	set public.&TableName;
RUN;

LIBNAME _ALL_ CLEAR;
cas mySession terminate;

%MEND;




