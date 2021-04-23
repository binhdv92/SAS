# Merger two data sets in SAS
To merge two or more data sets in SAS, you must first sort both data sets by a shared variable upon which the merging will be based, and then use the MERGE statement in your DATA statement. If you merge data sets without sorting, called one-to-one merging, the data of the merged file will overwrite the primary data set without considering whether or not two observations are the same.

```
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
```
![image](https://user-images.githubusercontent.com/16643491/115851952-bb808380-a451-11eb-8884-df8d533945c1.png)

```
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
```
![image](https://user-images.githubusercontent.com/16643491/115852024-d05d1700-a451-11eb-96d3-10fc8f8228a0.png)

```
-- Merge dataset three;
DATA three;
	MERGE one two;
	by id;
run;
proc print data =three;
Title "three merged";
run;
```
![image](https://user-images.githubusercontent.com/16643491/115852042-d6eb8e80-a451-11eb-8734-0bd1746f2218.png)

Suppose you create two data sets (one and two below), with a common variable, id. The SAS codes below show how they can be sorted and merged:

In the example above, data set three is created by merging data sets one and two. It will have five variables (id, and v1 to v4) and four cases. Where id=4, variables v1 and v2 will be missing.

## SOURCE: INDIANA UNIVERSITY
https://kb.iu.edu/d/afin
