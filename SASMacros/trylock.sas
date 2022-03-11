/*https://www.lexjansen.com/pharmasug/2005/posters/po33.pdf*/

%macro trylock(member=, timeout=20, retry=4);
	%local starttime;
	%let starttime = %sysfunc(datetime());

	%do %until(&syslckrc <=0 or %sysevalf(%sysfunc(datetime()) > (&starttime + &timeout)));
/* 		%put trying open ...; */

		data _null_;
			dsid=0;

			do until (dsid > 0 or datetime() > (&starttime + &timeout));
				dsid=open("&member");

				if (dsid=0) then
					rc=sleep(&retry);
			end;

			if (dsid > 0) then
				rc=close(dsid);
		run;

/* 		%put trying lock ...; */
		lock &member;
/* 		%put syslckrc=&syslckrc; */
	%end;
%mend;
