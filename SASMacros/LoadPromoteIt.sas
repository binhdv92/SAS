%macro LoadPromoteIt(LoadDataName=,PublicSaveDataName=);
	proc casutil;
		load data="&LoadDataName" outcaslib="public" promote;
		save casdata="&PublicSaveDataName" replace;
	run;

	quit;
%mend;