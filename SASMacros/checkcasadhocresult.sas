
/* Macro for Dropping a Table from CAS */
%macro DropIt(name=);
proc casutil incaslib="public";
  droptable casdata="&name";   
/*   list files;                                      */
run; quit;
%mend;

/* Load table to CAS and Promt */
%macro LoadPromoteIt(name=);
/* OPTIONS MPRINT; */
proc casutil;              
   load data=&Slib..&name outcaslib='public' promote ; 
   save casdata="&name" replace;
run; quit;
%mend;

/* Remove existing table from CAS if loaded already */ 
%macro deleteCASdsifexists(lib,name);
    %put in deleteCASifexists;
    %if %sysfunc(exist(&lib..&name.)) %then %do;

		%put DeleteCASDSifExistsMacro;

		%DropIt(name=&name);

	%end;

	%if %sysfunc(exist(&lib..&name.)) %then %do;

		%put First Attempt Failed; Trying one more time;

		%DropIt(name=&name);

	%end;
%mend ;
%macro email(Slib, Sname, email);
	%put Expected dataset does not exists (&Slib..&Sname.), need to email someone;
	filename mailbox email
			TO=(&email)
			FROM=('NoReply <NOREPLY@firstsolar.com>')
			SENDER = ('NoReply <NOREPLY@firstsolar.com>')
			IMPORTANCE='HIGH'
			replyto='NOREPLY@FirstSolar.com'
	        Subject='SAS VA Dataset Loading Failed';
	  
	DATA _NULL_;
	FILE Mailbox;
	PUT "Greetings,";
	PUT "  This is a message from a SAS.";
	PUT "Expected dataset does not exists (&Slib..&Sname.)";
	PUT "may need to do something";
	RUN;
%mend;

%macro checkCASAdhocresult(Slib,Sname,Tlib,Tname,email);


options cashost="azr1sas01s110.fs.local" casport=5570;
cas mySession sessopts=(caslib=public timeout=1800 locale="en_US");
LIBNAME PUBLIC CAS CASLIB="PUBLIC";

	%PUT Source Data &Slib..&Sname.;
	%PUT Target Data &Tlib..&Tname ;
	%if %sysfunc(exist(&Slib..&Sname.)) %then %do;
		%put CheckResultMacro;

			%deleteCASdsifexists(&Tlib, &Tname);
		
			%put loadTable;
			%LoadPromoteIt(name=&Sname);
	%end;	
	%else %do;
		%email(&Slib, &Sname, &email);
	%end;


LIBNAME PUBLIC CLEAR;
%mend;