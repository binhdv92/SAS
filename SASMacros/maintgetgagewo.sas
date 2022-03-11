%MACRO maintgetgagewo();

PROC SQL;/*Maint_S6_Meters_WOs:  Get all WOs related to Maximo S6 Meters*/
   CONNECT TO ODBC as con2
    (DATASRC="maximo_maxprod" authdomain=SQLGRP_Temp_Reader_Auth);
/*     (DATASRC="maximo_maxqa" authdomain=SQLGRP_Temp_Reader_Auth); */

CREATE TABLE work.Maint_S6_Meters_WOs    AS 
SELECT * FROM CONNECTION TO con2 
(
with thing1 AS
(
SELECT 
	a.[pointnum]
	,a.siteid
	,a.[assetnum]
	,a.[metername]
	,a.description
	,a.UpperAction
	,a.LowerAction
	,a.ulpmnum
	,a.llpmnum
	,b.pmnum as charpmnum
FROM 
	(	
	SELECT PointNum, SiteID, AssetNum
	FROM assetmeter
	WHERE 
		active = 1 
		AND SITEID IN (1006,3003,3006)
		AND NOT(Metername IN ('EPCRNSTATE','Runstate'))
	) a1
	LEFT JOIN [dbo].[measurepoint] a    on a1.siteid = a.siteid and a1.pointnum = a.pointnum
	LEFT JOIN [dbo].[charpointaction] b on a1.siteid = b.siteid and a1.pointnum = b.pointnum
WHERE 
	a.Siteid IN (1006, 3006, 3003)
	AND NOT(a.Metername in ('WATRCOND','RUNSTATE'))
) 

SELECT DISTINCT
	a.[pointnum]
	,a.UpperAction
	,a.[metername]
	,a.[assetnum]
	,b.[location]
	,b.description as AssetDescription
	,b.oeeleg
	,b.controlsid
	,a.[description]
	,a.[siteid]
	,c.[wonum]
	,[worktype]
	,c.pmnum
	,c.[status]
	,c.[statusdate]
	,c.reportedby
	,c.reportdate
	,c.actfinish
	,c.jpnum
	/*,a.**/
FROM 
	thing1 a
	/*LEFT JOIN [MAXPROD].[dbo].[measurement]  d on a.pointnum = d.pointnum*/
	LEFT JOIN dbo.Asset b on a.assetnum = b.assetnum and a.siteid = b.siteid
	LEFT JOIN [dbo].[workorder] c on 
		a.siteid = c.siteid 
		AND 
			(
			NOT(c.status IN ('COMP','CLOSE','CAN','WTOOL','3RDPTY'))
			OR ((c.status IN ('COMP','WTOOL','3RDPTY')) AND c.actfinish > DATEADD(HOUR,-2,GETDATE())  AND NOT(c.jpnum IN ('NA00001753','NA00001731','NA00001427','NA00001428','NA00001999','NA00001501','NA00001429','NA00001483')))
			OR ((c.status IN ('COMP','WTOOL','3RDPTY')) AND c.actfinish > DATEADD(HOUR,-48,GETDATE()) AND     c.jpnum IN ('NA00001753','NA00001731','NA00001427','NA00001428','NA00001999','NA00001501','NA00001429','NA00001483') )

			)
		AND a.assetnum = b.assetnum 
		and (
			a.ulpmnum = c.pmnum
			OR a.llpmnum = c.pmnum
			OR a.charpmnum = c.pmnum
			)
		/*and a.pointnum = c.pointnum*/
		and (
			a.pointnum = c.pointnum
			or
			c.pointnum is null /*had to add this to capture WO's that were auto generated*/
			)
WHERE	
	LEN(WONUM) > 5
ORDER BY 
	c.reportdate desc, b.[location], a.assetnum, b.controlsid, a.[description], c.status, c.statusdate
);
DISCONNECT FROM con2;
QUIT;


%MEND;