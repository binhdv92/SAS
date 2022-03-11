%macro _ftablesize();
/*The Following Lines Print the Size of Tables Created in WorkLibrary*/
proc sql NOPRINT;
create table WorkTables as
select libname, memname as TableName, nobs, crdate, modate, (filesize+nobs*obslen)/2000 as kB
from dictionary.tables
where 
	libname IN ('WORK')
	AND NOT(memname IN ('_PRODSAVAIL', 'WorkTables'))
ORDER BY FileSize Desc;
quit;

DATA _NULL_;
	SET work.Worktables;
	PUT 'NOTE: ------------------------------- Column Names ------------------------------';
	PUT 'NOTE: ' @10 'TableName' @47 'Nobs' @67 'kB';
	PUT 'NOTE: ' @10 TableName @41 nobs comma9. @61 kB comma9.;
	PUT 'NOTE: ------------------------------- Column Names ------------------------------';
RUN;

PROC SQL;
DROP TABLE Work.WorkTables;
QUIT;

%mend;