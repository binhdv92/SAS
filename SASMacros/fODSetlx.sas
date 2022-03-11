OPTIONS VALIDVARNAME=ANY FULLSTIMER;

%macro fODSetl(Plant, TableName);
	* prepare things;
	%PUT macro executing f_ODSetl(&Plant, &TableName);
	%LET PlantTableName = &Plant._&TableName.;
	* doing thing;

	PROC SQL;
		CONNECT TO ODBC as con2
    (DATASRC="&Plant.MESODS" authdomain=SQLGRP_Temp_Reader_Auth);
		CREATE TABLE work.&PlantTableName.    AS SELECT * FROM CONNECTION TO con2 
(&SQLExtraction.);
		DISCONNECT FROM con2;
	QUIT;

%mend;