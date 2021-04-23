-- Declare dataset ONE;
DATA one;
	INPUT id v1 v2;
	DATALINES;
	1 10 100
	3 20 200
	2 15 150
;
proc print data =one;
Title "One raw";
run;

PROC SORT Data = one;
	BY id;
RUN;
proc print data =one;
Title "One sorted";
run;

-- Declare dataset TWO;
DATA two;
   INPUT id v3 v4;
   DATALINES;
   1 1000 10000
   2 1500 15000
   4  800 30000
   3 2000 20000
   ;
proc print data =two;
Title "Two raw";
run;

PROC SORT Data=two;
	BY id;
RUN;
proc print data =two;
Title "Two sorted";
run;

-- Merge dataset three;
DATA three;
	MERGE one two;
	by id;
run;
proc print data =three;
Title "three merged";
run;
