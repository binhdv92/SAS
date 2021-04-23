-- Declare dataset ONE;
DATA one;
	INPUT id v1 v2;
	DATALINES;
	1 10 100
	3 20 200
	2 15 150
;
PROC SORT Data = one;
	BY id;
RUN;

-- Declare dataset TWO;
DATA two;
   INPUT id v3 v4;
   DATALINES;
   1 1000 10000
   2 1500 15000
   3 2000 20000
   4  800 30000
   ;
PROC SORT Data=two;
	BY id;
RUN;

-- Merge dataset three
DATA three;
	MERGE one two;
	by id;

proc print data=three;
run;
