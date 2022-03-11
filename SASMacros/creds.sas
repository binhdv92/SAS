%MACRO creds();
OPTION SPOOL;
FILENAME GetCreds "/home/zsvc_sas_temp_reader/.creds";

data _NULL_;
	LENGTH USER $20 PASS $16;
	INFILE GetCreds;
	INPUT
		USER $ PASS $;
	CALL SYMPUT("SUser", USER);
	CALL SYMPUT("SPass", PASS);
RUN;
OPTION NOSPOOL;
FILENAME GetCreds CLEAR;
%MEND;