%macro DropIt(PublicDropTable=);
	proc casutil incaslib="public";
		droptable casdata="&PublicDropTable";
	run;

	quit;
%mend;