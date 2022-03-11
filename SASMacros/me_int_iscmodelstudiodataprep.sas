
%MACRO me_int_iscmodelstudiodataprep(SPUL=,DebugFlag=False,Purpose=All3);

/*****************************************************************************
 *                                                                           *
 * SAS Program to investigate a particular Simulation Unit - Simulation Line *
 * in order to predict Isc as part of goal to adjust Isc value.              *
 * This program prepars the simulator-level data file for subsequent use in  *
 * a SAS Model Studio project.  The SAS Model Studio project takes the data  *
 * file and uses the suspect Isc values as a target variable and creates     *
 * various models using other variables (e.g., AVGDeadZoneMeasurement) to    *
 * predict the suspect Isc values.  The predicted and actual Isc values are  *
 * used to determine an adjustment value to the Isc measurements.            *
 *                                                                           *
 * Program written by: Thomas Shields (Jedi Master)                          *
 *                     William Herald, Ph.D. (padawan)                       *
 * Version:            January 25, 2021.                                     *
 *                                                                           *
 *****************************************************************************/

/*****************************************************************************
 * SET UP DEBUG INDICATOR VARIABLE                                           *
 *    ... making sure this is visible at the beginning of the program ...    *
 *                                                                           *
 * Define indicator variable to assist with possible debugging.              *
 * If the variable is set to "True", then various tables will be produced    *
 * to allow for checking of processing throughout the program.               *
 * If the variable is set to "False", then the various tables will not be    *
 * processed, and as a result, will not be displayed.                        *
 *                                                                           *
 *****************************************************************************/

/* %LET DebugFlag = True; */

/* %PUT &SPUL &DebugFlag; */

%IF %UPCASE(&DebugFlag) = TRUE %THEN %DO;
    %PUT *** DebugFlag = &DebugFlag ***;
%END;

/*****************************************************************************
 *                                                                           *
 * USE OF PROGRAM                                                            *
 *                                                                           *
 * This program is structured in a fashion that it can be MODIFIED for use   *
 * by other SimUnitLine values.                                              *
 *                                                                           *
 * It should be strongly noted that each SimUnitLine (simulator) has the     *
 * potential for having different decisions (e.g., different variable        *
 * characteristics, rccc being one value for one simulator's data set        *
 * but having multiple values for another simulator's data set).  These      *
 * differences could impact the ultimate model selected for the simulator.   *
 * Therefore, this program should not be blindly followed.                   *
 *                                                                           *
 *****************************************************************************/

/*****************************************************************************
 *                                                                           *
 * MODIFICATION TO AND NAVIGATION THROUGH PROGRAM                            *
 *                                                                           *
 * Although the coding could have been set up to have an overall variable    *
 * identify the simulator (e.g., &SPUL.) so that changing one line of code   *
 * would ripple the changed simulator throughout the program, a simplier     *
 * approach was taken (primarily for ease of reading the program).           *
 * All that needs to be done is to copy this program with a name identifying *
 * the simulator (e.g., ME_INT_ISCAD_MODEL_DEV_DMT22B).  Then, with the new  *
 * program, simply do a global change of "&SPUL." to, for example, "DMT22B". *
 *                                                                           *
 * The beginning of each section (listing of the sections is shown below)    *
 * can be found by searching for the phrase "* Section".                     *
 *                                                                           *
 * The end of each section has a listing of major conclusions and notes for  *
 * the section.  The beginning of each section's results/notes can be found  *
 * by searching for "/*>>>>".                                                *
 *                                                                           *
 *****************************************************************************/

/*****************************************************************************
 *                                                                           *
 * OVERVIEW OF SECTIONS                                                      *
 *                                                                           *
 * For this program, the Sim Unit - Sim Line (SimUnitLine) is: &SPUL..       *
 * There are a number of steps to be followed.  The insights gained from the *
 * investigation will vary from SimUnitLine to SimUnitLine.  But, because of *
 * earlier ivestigation at a more macro level, the general approach will be  *
 * followed for each SimUnitLine analysis.  Namely, each analysis will have  *
 * the following sections:                                                   *
 *                                                                           *
 * (A) Set up structure for obtaining data files.                            *
 *                                                                           *
 * (B) Detail the within program macro(s) which may be used in this program. *
 *                                                                           *
 * (C) Obtain subset file for specific SimUnitLine (simulator) and derive    *
 *     certain variables.                                                    *
 *                                                                           *
 * (D) Keep records which have variables with certain values.                *
 *                                                                           *
 * (E) Divide file into last seven days, last three hours and then           *
 *     partition files into Train, Valid, Test.                              *
 *                                                                           *
 * (F) Merge the three files, retain needed variables, and place the unified *
 *     data file into SAS CAS storage for use with SAS Model Studio.         *
 *                                                                           *
 * The sections of the program follow ...                                    *
 *                                                                           *
 *****************************************************************************/


/*****************************************************************************
 *****************************************************************************
 *                                                                           *
 * Section A: Set up structure for obtaining data files.                     *
 *                                                                           *
 *****************************************************************************
 *****************************************************************************/

/*****************************************************************************
 * File setup code from Tom Shields to                                       *
 * Get access to file and set up times.                                      *
 *****************************************************************************/

ods results on;
options cashost="azr1sas01s110.fs.local"
    casport=5570 NONOTES NOFULLSTIMER MAUTOSOURCE;
/* cas mysession terminate; */
cas mySession
    sessopts=(caslib=casuser timeout=1800 locale="en_US"
              METRICS=TRUE/*maxTableMem=1Gb*/);
caslib _all_ assign;
options casdatalimit = 2000M;

/* proc contents data=public.Premodel_finaldata noprint out=work.cols; run; */
/* Formatting changed to cut down on line length.*/

%LET PGT     = %SYSFUNC(DATEPART(
               %SYSFUNC(DATETIME())));
%LET PGTTime = %SYSFUNC(DATETIME());
%LET PGTHour = %EVAL(
               %SYSFUNC(FLOOR(
              (%SYSFUNC(DATETIME()))/3600))*3600);
%LET UDT     = %SYSFUNC(DATEPART(
               %SYSFUNC(tzones2u(
               %SYSFUNC(DATETIME())))));
%LET UDTTime = %SYSFUNC(tzones2u(
               %SYSFUNC(DATETIME())));
%LET KMT     = %SYSFUNC(DATEPART(
               %SYSFUNC(tzoneu2s(
               %SYSFUNC(tzones2u(
               %SYSFUNC(DATETIME()))),Asia/Singapore))));
%LET KMTTime = %SYSFUNC(tzoneu2s(
               %SYSFUNC(tzones2u(
               %SYSFUNC(DATETIME()))),Asia/Singapore));
%LET KMTHour = %EVAL(
               %SYSFUNC(FLOOR(
              (%SYSFUNC(tzoneu2s(
               %SYSFUNC(tzones2u(
               %SYSFUNC(DATETIME()))),Asia/Singapore)))/3600))*3600);
%LET DMT     = %SYSFUNC(DATEPART(
               %SYSFUNC(tzoneu2s(
               %SYSFUNC(tzones2u(
               %SYSFUNC(DATETIME()))),Asia/Saigon))));
%LET DMTTime = %SYSFUNC(tzoneu2s(
               %SYSFUNC(tzones2u(%SYSFUNC(DATETIME()))),Asia/Saigon));
%LET DMTHour = %EVAL(
               %SYSFUNC(FLOOR(
              (%SYSFUNC(tzoneu2s(
               %SYSFUNC(tzones2u(
               %SYSFUNC(DATETIME()))),Asia/Saigon)))/3600))*3600);

/* %PUT NOTE: UDT Date is:     &UDT; */
/* %PUT NOTE: PGT Date is:     &PGT; */
/* %PUT NOTE: PGTHour Date is: &PGTHour; */
/* %PUT NOTE: KMT Date is:     &KMT; */
/* %PUT NOTE: KMTHour Date is: &KMTHour; */
/* %PUT NOTE: DMT Date is:     &DMT; */
/* %PUT NOTE  DMTHour Date is: &DMTHour; */

/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
 >                                                                           >
 > Conclusions gleaned from Section A:                                       >
 >                                                                           >
 > (1) The first part of the section covered setting up date-time variables. >
 >                                                                           >
 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>*/


/*****************************************************************************
 *****************************************************************************
 *                                                                           *
 * Section B: Detail the within program macro(s) which may be used in this   *
 *            program.                                                       *
 *                                                                           *
 *****************************************************************************
 *****************************************************************************/

/*****************************************************************************
 * Macro code from Tom Shields to                                            *
 * Allow for deleting a file after checking if it exists.                    *
 *****************************************************************************/

%macro deletedsifexists(lib,name);
    %if %sysfunc(exist(&lib..&name.)) %then %do;
/* %put DeletDSifExistsMacro; */ /*210302 (TWS): Removed this put*/
proc datasets library=&lib. nolist;
        delete &name.;
    quit;
%end;
%mend;

/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
 >                                                                           >
 > Conclusions gleaned from Section B:                                       >
 >                                                                           >
 > (1) This program does not use many user-defined macros.  All user-defined >
 >     macros used in this program are shown above.                          >
 >                                                                           >
 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>*/


/*****************************************************************************
 *****************************************************************************
 *                                                                           *
 * Section C: Obtain subset file for specific SimUnitLine (simulator) and    *
 *            derive certain variables.                                      *
 *                                                                           *
 *****************************************************************************
 *****************************************************************************/

/*****************************************************************************
 * Used SAS Tasks | Prepare Data | Transform Data | Filter Data to           *
 * Filter ME_INT_ISCAD2 data file to obtain subset of &SPUL. records.        *
 *****************************************************************************/

proc sql noprint;
	create table WORK.ME_INT_ISCAD2_&SPUL.
        as
        select * from PUBLIC.ME_INT_ISCAD2 
        where(put(sim, $25.) EQ "&SPUL.-SIM_HIPOT_SIM");
quit;

/*****************************************************************************
 * Derive variables whch may be needed later.                                *
 *****************************************************************************/

data WORK.ME_INT_ISCAD2_&SPUL.;
    set WORK.ME_INT_ISCAD2_&SPUL.;
        UnitValue   = 1;
        SimUnit     = substr('sim'n, 1, 5);
        SimLine     = substr('sim'n, 6, 1);
        SimUnitLine = cats(SimUnit, SimLine);
        rccc_char85 = put(rccc, 8.5);
        rccc_char83 = put(round(rccc, .001), 8.3);
        /*210407 (TWS) - PUBLIC.ME_INT_ISCAD2 has twice truncated RCCC, 	*/
		/*				adding format to force rccc to include deimal 		*/
		/*				necessary for MES integration 						*/
		format rccc F11.5; 
run;

%IF &DebugFlag = True %THEN %DO;

    /*************************************************************************
     * Used SAS Tasks | Statistics | Table Analysis to                       *
     * Verify the derivation of three new sim-related variables.             *
     *************************************************************************/

    title1   color=blue bold "************ Section C ************";
    title3   "Frequencies that verify derivation of selected variables";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL.";

    proc freq data=WORK.ME_INT_ISCAD2_&SPUL.;
        tables  (sim) *(SimUnitLine SimUnit SimLine)
            / missprint nopercent norow nocol nocum 
            plots=none;
        run;

    ods noproctitle;

    title    "Crosstab that verifies derivation of certain variables";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL.";

    proc freq data=WORK.ME_INT_ISCAD2_&SPUL.;
        tables  (SimUnit) * (SimLine)
        / missprint nopercent norow nocol nocum 
        plots=none;
    run;

    /*************************************************************************
     * Used SAS Tasks | Statistics | Summary Statistics to                   *
     * Verify the derivation of the UnitValue "variable".                    *
     *************************************************************************/

    title    "Frequencies that verify derivation of UnitValue variable";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL.";

    ods graphics / imagemap=on;

    proc means data=WORK.ME_INT_ISCAD2_&SPUL.
        chartype n nmiss min max vardef=df;
        var UnitValue;
    run;

    /*************************************************************************
     * Used SAS Tasks | Statistics | Summary Statistics and                  *
     * Used SAS Tasks | Statistics | Table Analysis to                       *
     * Verify the derivation of the rccc_char variables.                     *
     *************************************************************************/

    title    "Frequencies that verify derivation of rccc_char variables";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL.";

    ods graphics / imagemap=on;

    proc means data=WORK.ME_INT_ISCAD2_&SPUL.
        chartype n nmiss min max vardef=df;
        var rccc;
    run;

    proc means data=WORK.ME_INT_ISCAD2_&SPUL.
        chartype n nmiss min max vardef=df;
        var rccc;
        class rccc_char85;
    run;

    proc means data=WORK.ME_INT_ISCAD2_&SPUL.
        chartype n nmiss min max vardef=df;
        var rccc;
        class rccc_char83;
    run;

    proc freq data=WORK.ME_INT_ISCAD2_&SPUL.;
    	tables  (rccc_char85) * (rccc_char83)
            / missprint nopercent norow nocol nocum 
    		plots=none;
    run;

    /*************************************************************************
     * Used SAS Tasks | Visualize Data | Graph | Bar Chart to                *
     * verify that have subset of data.                                      *
     *************************************************************************/

    title    "Plot that verifies subset of data";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL.";

    ods graphics / reset width=6.4in height=4.8in imagemap;

    proc sgplot data=WORK.ME_INT_ISCAD2_&SPUL.;
        vbar sim /;
        yaxis grid;
    run;

    ods graphics / reset;

    /*************************************************************************
     * Used SAS Tasks | Prepare Data | Examine Data | List Table Att. to     *
     * Obtain meta-data attributes of variables.                             *
     *************************************************************************/

    title    "Table of meta-data for attributes of variables";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL.";

    ods select attributes variables;

    proc datasets;
        contents data=WORK.ME_INT_ISCAD2_&SPUL. order=collate;
    quit;

%END;

/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
 >                                                                           >
 > Conclusions gleaned from Section C:                                       >
 >                                                                           >
 > (1) The main (base) data file used in the analyses is:                    >
 >     PUBLIC.ME_INT_ISCAD2.                                                 >
 >                                                                           >
 > (2) This data file is updated quite frequently.                           >
 >                                                                           >
 > (3) Because of this, if this program is run several times within a short  >
 >     period of time, the contents of the data file may be different.       >
 >     The results may, therefore, be different as well.  Of course, because >
 >     of partitioning (see next section) the results may be different       >
 >     regardless.                                                           >
 >                                                                           >
 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>*/


/*****************************************************************************
 *****************************************************************************
 *                                                                           *
 * Section D: Keep records which have variables with certain values.         *
 *            Specifically, keep records with:                               *
 *            ArcModule      value  is "True";                               *
 *            AND keep records with:                                         *
 *            Name          "value" is NOT blank (missing or "") AND         *
 *            EquipmentName "value" is NOT blank (missing or "") AND         *
 *            CAL            value  is "NO".                                 *
 *                                                                           *
 *****************************************************************************
 *****************************************************************************/

/*****************************************************************************
 * SAS code to                                                               *
 * KEEP records where                                                        *
 * ----ArcModule      value  is "True"                                       *
 *     Be sure that records with the following "values" are NOT retained:    *
 *     ArcModule     "value" is blank (missing or "").      ---              *
 *****************************************************************************/

/*****************************************************************************
 * Used SAS Tasks | Prepare Data | Transform Data | Filter Data to           *
 * produce "snapshot" of data set to preserve its current status.            *
 *****************************************************************************/

proc sql noprint;
    create table WORK.ME_INT_ISCAD2_&SPUL._OLD01
    as
    select * from WORK.ME_INT_ISCAD2_&SPUL.;
quit;

/*****************************************************************************
 * Used SAS Tasks | Prepare Data | Transform Data | Filter Data to           *
 * produce "snapshot" of data set to set up revision.                        *
 *****************************************************************************/

proc sql noprint;
    create table WORK.ME_INT_ISCAD2_&SPUL._TEMP
    as
    select * from WORK.ME_INT_ISCAD2_&SPUL.;
quit;

%IF &DebugFlag = True %THEN %DO;

    /*************************************************************************
     * Used SAS Tasks | Statistics | Descriptive | Table Analysis to         *
     * Determine the number of records which should be deleted.              *
     *************************************************************************/

    title1   color=blue bold "************ Section D ************";
    title3   "Crosstab of ArcModule to check if correct records dropped - "
             "BEFORE";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL._TEMP";

    proc freq data=WORK.ME_INT_ISCAD2_&SPUL._TEMP;
        tables (ArcModule) * (UnitValue)
        / missing nopercent norow nocol 
            nocum plots=none;
    run;

%END;

/*****************************************************************************
 * SAS code to                                                               *
 * Delete data set for reproduction in next part's proc SQL code.            *
 *****************************************************************************/

proc datasets library=WORK noprint;
    delete ME_INT_ISCAD2_&SPUL.;
run;

/*****************************************************************************
 * Used SAS code to                                                          *
 * Keep the records which should be kept.                                    *
 *****************************************************************************/

 proc sql;
    create table WORK.ME_INT_ISCAD2_&SPUL.
    as
    select *
    from WORK.ME_INT_ISCAD2_&SPUL._TEMP
    where
    ArcModule = 'True';
quit;

%IF &DebugFlag = True %THEN %DO;

    /*************************************************************************
     * Used SAS Tasks | Statistics | Descriptive | Table Analysis to         *
     * Determine if correct records were dropped.                            *
     *************************************************************************/

    ods noproctitle;

    title    "Crosstab of ArcModule to check if correct records dropped - "
             "AFTER";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL.";

    proc freq data=WORK.ME_INT_ISCAD2_&SPUL.;
        tables (ArcModule) * (UnitValue)
        / missing nopercent norow nocol 
            nocum plots=none;
    run;

    /*************************************************************************
     * Used SAS Tasks | Statistics | Descriptive | Table Analysis to         *
     * Check size of revised data set.                                       *
     *************************************************************************/

    title    "Crosstab to show num. of records in revised data set";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL.";

    proc freq data=WORK.ME_INT_ISCAD2_&SPUL.;
        tables (SimUnitLine) * (UnitValue)
        / missing nopercent norow nocol 
            nocum plots=none;
    run;

%END;

/*****************************************************************************
 * SAS code to                                                               *
 * Delete temporary data set since it is no longer needed.                   *
 *****************************************************************************/

proc datasets library=WORK noprint;
    delete ME_INT_ISCAD2_&SPUL._TEMP;
run;

/*****************************************************************************
 * SAS code to                                                               *
 * Keep records where                                                        *
 *     Name          "value" is NOT blank (missing or "") AND                *
 *     EquipmentName "value" is NOT blank (missing or "") AND                *
 *     CAL            value  is "NO".                                        *
 *****************************************************************************/

/*****************************************************************************
 * Used SAS Tasks | Prepare Data | Transform Data | Filter Data to           *
 * produce "snapshot" of data set to preserve its current status.            *
 *****************************************************************************/

proc sql noprint;
    create table WORK.ME_INT_ISCAD2_&SPUL._OLD02
    as
    select * from WORK.ME_INT_ISCAD2_&SPUL.;
quit;

/*****************************************************************************
 * Used SAS Tasks | Prepare Data | Transform Data | Filter Data to           *
 * produce "snapshot" of data set to set up revision.                        *
 *****************************************************************************/

proc sql noprint;
    create table WORK.ME_INT_ISCAD2_&SPUL._TEMP
    as
    select * from WORK.ME_INT_ISCAD2_&SPUL.;
quit;

%IF &DebugFlag = True %THEN %DO;

    /*************************************************************************
     * Used SAS Tasks | Statistics | Descriptive | Table Analysis to         *
     * Determine the number of records which should be kept.                 *
     *************************************************************************/

    title    "Crosstabs of variables with values resulting in records being "
             "dropped - BEFORE";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL._TEMP";

    proc freq data=WORK.ME_INT_ISCAD2_&SPUL._TEMP;
        tables (CAL) *(Name) * (SPLEquipmentName)
        / missing nopercent norow nocol 
            nocum plots=none;
    run;

%END;

/*****************************************************************************
 * SAS code to                                                               *
 * Delete data set for reproduction in next part's proc SQL code.            *
 *****************************************************************************/

proc datasets library=WORK noprint;
    delete ME_INT_ISCAD2_&SPUL.;
run;

/*****************************************************************************
 * Used SAS code to                                                          *
 * Keep the records which should be kept.                                    *
 *****************************************************************************/

 proc sql;
    create table WORK.ME_INT_ISCAD2_&SPUL.
    as
    select *
    from WORK.ME_INT_ISCAD2_&SPUL._TEMP
    where
    'Name'n <> "" AND SPLEquipmentName <> "" AND CAL = 'NO';
quit;

%IF &DebugFlag = True %THEN %DO;

    /*************************************************************************
     * Used SAS Tasks | Statistics | Descriptive | Table Analysis to         *
     * Determine if correct records were dropped.                            *
     *************************************************************************/

    title    "Crosstabs of variables with values resulting in records being "
             "dropped - AFTER";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL.";

    proc freq data=WORK.ME_INT_ISCAD2_&SPUL.;
        tables (CAL) *(Name) * (SPLEquipmentName)
        / missing nopercent norow nocol 
            nocum plots=none;
    run;

    /*************************************************************************
     * Used SAS Tasks | Statistics | Descriptive | Table Analysis to         *
     * Check size of revised data set.                                       *
     *************************************************************************/

    title    "Crosstab to show num. of records in revised data set";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL.";

    proc freq data=WORK.ME_INT_ISCAD2_&SPUL.;
        tables (SimUnitLine) * (UnitValue)
        / missing nopercent norow nocol 
            nocum plots=none;
    run;

%END;

/*****************************************************************************
 * SAS code to                                                               *
 * Delete temporary data set since it is no longer needed.                   *
 *****************************************************************************/

proc datasets library=WORK noprint;
    delete ME_INT_ISCAD2_&SPUL._TEMP;
run;

/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
 >                                                                           >
 > Conclusions gleaned from Section D:                                       >
 >                                                                           >
 > (1) The main (base) data file is adjusted to retain certain records       >
 >     based on values of ArcModule, Name, SPLEquipmentName, and CAL.        >
 >                                                                           >
 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>*/


/*****************************************************************************
 *****************************************************************************
 *                                                                           *
 * Section E: Divide file into last seven days, last three hours and then    *
 *            and partition files into Train, Valid, Test.                   *
 *                                                                           *
 *****************************************************************************
 *****************************************************************************/

/*****************************************************************************
 * Used portion of massive, impressive code from Tom Shields to              *
 * Set up partitions: Train, Validate, Test (last three hours of data).      *
 *****************************************************************************/

options validmemname=extend;

data _null_;
    idMaxLength=max(length("1-Train"), length("0-Validate"), length("2-Test"));
	/* Put it in a macro variable for use in the real code */
    call symput('idLength', idMaxLength);
run;

/*Gets last 3 hours worth of data;
  site specific values to ensure consistent 3-hour period.*/

DATA WORK.ME_INT_ISCAD2_&SPUL._TEST;
    SET WORK.ME_INT_ISCAD2_&SPUL. 
        (where=(
                 ( Substr('sim'n, 1, 3) = 'PGT'          AND
                  (IVSweepReadTime) >= (&PGTHour-3*3600) AND
                  (IVSweepReadTime) < &PGTHour+1*3600
                 )
                 OR
                 ( Substr('sim'n, 1, 3) = 'KMT'          AND
                  (IVSweepReadTime) >= (&KMTHour-3*3600) AND
                  (IVSweepReadTime) < &KMTHour+1*3600
                 )
                 OR
                 ( Substr('sim'n, 1, 3) = 'DMT'          AND
                  (IVSweepReadTime) >= (&DMTHour-3*3600) AND
                  (IVSweepReadTime) < &DMTHour+1*3600
                 )
               )
        );
    length Partition1Tr2Va3Te $ &idLength;
    Partition1Tr2Va3Te = "2-Test";
run;

/*Gets last 7 days worth of data;
  site specific values to ensure consistent 7-day period.*/

DATA WORK.ME_INT_ISCAD2_&SPUL._Last7D;
    SET WORK.ME_INT_ISCAD2_&SPUL. 
        (where=(
                 ( Substr('sim'n, 1, 3) = 'PGT' AND
                   DATEPART(IVSweepReadTime) >= (&PGT-8)
                   /*AND DATEPART(IVSweepReadTime) < &PGT */ AND
                  (IVSweepReadTime) < (&PGTHour-3*3600)
                 )
                 OR
                 ( Substr('sim'n, 1, 3) = 'KMT'              AND
                   DATEPART(IVSweepReadTime) >= (&KMT-8)
                   /*AND DATEPART(IVSweepReadTime) < &KMT */ AND
                  (IVSweepReadTime) < (&KMTHour-3*3600)
                 )
                 OR
                 ( Substr('sim'n, 1, 3) = 'DMT'              AND
                   DATEPART(IVSweepReadTime) >= (&DMT-8)
                   /*AND DATEPART(IVSweepReadTime) < &DMT */ AND
                  (IVSweepReadTime) < (&DMTHour-3*3600)
                 )
               )
        ); 
run;

/*****************************************************************************
 * Used SAS Tasks | Prepare Data | Transform Data | Partition Data to        *
 * Obtain Test and Validate data sets.                                       *
 *****************************************************************************/

proc sql noprint;
    select count(*) into :count from WORK.ME_INT_ISCAD2_&SPUL._Last7D;
quit;

data WORK.ME_INT_ISCAD2_&SPUL._TRAIN WORK.ME_INT_ISCAD2_&SPUL._VALID;
    set WORK.ME_INT_ISCAD2_&SPUL._Last7D;
    length Partition1Tr2Va3Te $ &idLength;
    retain __tmp1-__tmp%trim(&count) __nobs__ __nobs1__ __nobs2__;
    drop _i_ __seed__ __tmp1-__tmp%trim(&count);
    drop _n1_ __nobs__ __nobs1__ __nobs2__;
    array __tmp(*) __tmp1-__tmp%trim(&count);

    if (_n_=1) then
        do;
            __seed__=123;
            __nobs__=&count;

            do _i_=1 to dim(__tmp);
                __tmp(_i_)=_i_;
            end;
            call ranperm(__seed__, of __tmp(*));
/********************************************************************/
/* 				212026(TWS):  Per SAS Track 7613289203, there is an */
/*							  issue with how Model Studio Treats the*/ 
/*							  partition variable.  until that is    */
/*							  resolved, we are changing the %age    */
/*							  from 70/30 to 50/50 					*/
/*				212026(TWS):  Original code below					*/
/********************************************************************/
            __nobs1__=round(0.7*__nobs__); 						
            __nobs2__=round(0.3*__nobs__)+__nobs1__; 			
/********************************************************************/
/*				212026(TWS):  New code below						*/
/********************************************************************/
/*             __nobs1__=round(0.5*__nobs__); 						*/
/*             __nobs2__=round(0.5*__nobs__)+__nobs1__; 			*/

/********************************************************************/
/*				210309(TWS):  Recoded Val=0, Train=1, Test=2		*/
/* 					per tech support track above.  keeping 50/50 	*/
/* 					commented out above incase it is needed. 	 	*/
/********************************************************************/
        end;
    _n1_=_n_;

    if (_n1_ <=dim(__tmp)) then
        do;

            if (__tmp(_n1_) > 0) then
                do;

                    if (__tmp(_n1_) <=__nobs1__) then
                        do;
                            Partition1Tr2Va3Te = "1-Train";
                            output WORK.ME_INT_ISCAD2_&SPUL._TRAIN;
                        end;
                    else if (__tmp(_n1_) <=__nobs2__) then
                        do;
                            Partition1Tr2Va3Te = "0-Validate";
                            output WORK.ME_INT_ISCAD2_&SPUL._VALID;
                        end;
                end;
        end;
run;

%IF &DebugFlag = True %THEN %DO;

    /*************************************************************************
     * Used SAS Tasks | Statistics | Descriptive | Table Analysis to         *
     * Show the frequencies for various partitions before further refinement.*
     *                                                                       *
     *************************************************************************/

    /* Training */

    title1   color =blue bold "************ Section E ************";
    title3   "Frequencies for Partition Variable Partition1Tr2Va3Te "
             "- TRAIN";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL._TRAIN";

    proc freq data=WORK.ME_INT_ISCAD2_&SPUL._TRAIN;
        tables (Partition1Tr2Va3Te) * (UnitValue)
        / missing nopercent norow nocum plots=none;
    run;

    ods noproctitle;

    /* Validation */

    title    "Frequencies for Partition Variable Partition1Tr2Va3Te "
             "- VALIDATION";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL._VALID";

    proc freq data=WORK.ME_INT_ISCAD2_&SPUL._VALID;
        tables (Partition1Tr2Va3Te) * (UnitValue)
        / missing nopercent norow nocum plots=none;
    run;

    /* Test */

    title    "Frequencies for Partition Variable Partition1Tr2Va3Te "
             "- TEST";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL._TEST";

    proc freq data=WORK.ME_INT_ISCAD2_&SPUL._TEST;
        tables (Partition1Tr2Va3Te) * (UnitValue)
        / missing nopercent norow nocum plots=none;
    run;

    /*************************************************************************
     * Used SAS Tasks | Statistics | Summary Statistics to                   *
     * Show the number of records and time frame for each partition file.    *
     *************************************************************************/

    title    "Various Statistics Related to Train Partition and Time Period";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL._TRAIN";

    ods graphics / imagemap=on;

    proc means data=WORK.ME_INT_ISCAD2_&SPUL._TRAIN
        chartype n nmiss min max vardef=df;
        var UnitValue IVSweepReadTime;
    run;

    title    "Various Statistics Related to Valid Partition and Time Period";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL._VALID";

    ods graphics / imagemap=on;

    proc means data=WORK.ME_INT_ISCAD2_&SPUL._VALID
        chartype n nmiss min max vardef=df;
        var UnitValue IVSweepReadTime;
    run;

    title    "Various Statistics Related to Test Partition and Time Period";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL._TEST";

    ods graphics / imagemap=on;

    proc means data=WORK.ME_INT_ISCAD2_&SPUL._TEST
        chartype n nmiss min max vardef=df;
        var UnitValue IVSweepReadTime;
    run;

    ods graphics / reset;

    /*************************************************************************
     * Used SAS Tasks | Visualize Data | Graph | Bar Chart to                *
     * See range of IVSweepReadTime values for entire &SPUL. data file.      *
     *************************************************************************/

    ods graphics / reset width=6.4in height=4.8in imagemap;

    title    "Histogram of IVSweepReadTime";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL.";

    proc sgplot data=WORK.ME_INT_ISCAD2_&SPUL.;
        histogram IVSweepReadTime /;
        yaxis grid;
    run;

    /*************************************************************************
     * Used SAS Tasks | Visualize Data | Graph | Bar Chart to                *
     * See range of IVSweepReadTime values for last seven days of data file. *
     *************************************************************************/

    ods graphics / reset width=6.4in height=4.8in imagemap;

    title    "Histogram of IVSweepReadTime for Last Seven Days";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL._Last7D";

    proc sgplot data=WORK.ME_INT_ISCAD2_&SPUL._Last7D;
        histogram IVSweepReadTime /;
        yaxis grid;
    run;

    ods graphics / reset;

    /*************************************************************************
     * Used SAS Tasks | Visualize Data | Graph | Bar Chart to                *
     * See range of IVSweepReadTime values for Training portion of data file.*
     *************************************************************************/

    ods graphics / reset width=6.4in height=4.8in imagemap;

    title    "Histogram of IVSweepReadTime for Train Partition";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL._TRAIN";

    proc sgplot data=WORK.ME_INT_ISCAD2_&SPUL._TRAIN;
        histogram IVSweepReadTime /;
        yaxis grid;
    run;

    ods graphics / reset;

    /*************************************************************************
     * Used SAS Tasks | Visualize Data | Graph | Bar Chart to                *
     * See range of IVSweepReadTime values for Valid portion of data file.   *
     *************************************************************************/

    ods graphics / reset width=6.4in height=4.8in imagemap;

    title    "Histogram of IVSweepReadTime for Valid Partition";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL._VALID";

    proc sgplot data=WORK.ME_INT_ISCAD2_&SPUL._VALID;
        histogram IVSweepReadTime /;
        yaxis grid;
    run;

    ods graphics / reset;

    /*************************************************************************
     * Used SAS Tasks | Visualize Data | Graph | Bar Chart to                *
     * See range of IVSweepReadTime values for Test (last three hours) of DF.*
     *************************************************************************/

    ods graphics / reset width=6.4in height=4.8in imagemap;

    title    "Histogram of IVSweepReadTime for Test Partition "
             "(Last Three Hours)";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL._TEST";

    proc sgplot data=WORK.ME_INT_ISCAD2_&SPUL._TEST;
        histogram IVSweepReadTime /;
        yaxis grid;
    run;

%END;

/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
 >                                                                           >
 > Conclusions gleaned from Section E:                                       >
 >                                                                           >
 > (1) The "usual" way of partitioning a data set into training, validation, >
 >     testing (e.g., randomly assigning a certain percentage of records to  >
 >     each partition, like 70%, 15%, 15%) is not done here.                 >
 >     Instead, the main data file is randomly assigned to the training and  >
 >     validation partitions.  The testing partition consists of the last    >
 >     three hours of records (modules).                                     >
 >                                                                           >
 > (2) Because of this, the number of records in the testing partition may   >
 >     not be as large as desired.                                           >
 >                                                                           >
 > (3) At this point in the program, each of the partitions is in its own    >
 >     data file.                                                            >
 >                                                                           >
 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>*/


/*****************************************************************************
 *****************************************************************************
 *                                                                           *
 * Section F: Merge the three files, retain needed variables, and place the  *
 *            unified data file into SAS CAS storage for use with SAS Model  *
 *            Studio.                                                        *
 *                                                                           *
 *****************************************************************************
 *****************************************************************************/

/*****************************************************************************
 * Used SAS Tasks | Prepare Data | Transform Data | Combine Tables to        *
 * Merge the three daa files so that the training, validatio, and testing    *
 * records are all in the same data file.                                    *
 *****************************************************************************/

data    WORK.ME_INT_ISCAD2_&SPUL._TEMP;
	set WORK.ME_INT_ISCAD2_&SPUL._TRAIN
        WORK.ME_INT_ISCAD2_&SPUL._VALID
        WORK.ME_INT_ISCAD2_&SPUL._TEST;
run;

/*****************************************************************************
 * Used SAS code to                                                          *
 * Delete (and keep) the variables which should be deleted (or kept)         *
 * AND                                                                       *
 * place the data file in the CAS library for use in constructing models.    *
 *****************************************************************************/

proc sql noprint;
    create table WORK.ME_INT_ISCAD2_&SPUL._ALL3
    as
    select
    ArcModule,                AVGDeadZoneMeasurement,
    CAL,                      ColorSpace_b_Avg,
    ColorSpace_b_Max,         ColorSpace_b_Min,
    ColorSpace_b_StDev,       COUNTDeadZoneMeasurement,
    DeltaT,                   EquipmentName,
    flash_count,              Isc,
    IVSweepReadTime,          MAXDeadZoneMeasurement,
    MDC_Cu,                   MINDeadZoneMeasurement,
    MonitorCellTemperature,   Name,
    NokAVGDZM,                NOkCount,
    NokMAXDZM,                NokMINDZM,
    NokSTDDZM,                NokSUMDZM,
    NokVARDZM,                P3ETLSource,
    Partition1Tr2Va3Te,       Pmax,
    PostBsaAccumDwellMinutes, process_cd,
    PulseWidth,               PWL,
    rccc,                     rccc_char83,
    rccc_char85,              ReflectBand1_Avg,
    ReflectBand2_Avg,         ReflectBand3_Avg,
    ReflectBand4_Avg,         RFADTemperature,
    Roc,                      Rs,
    Rsc,                      Rsh,
    SimUnitLine,              SPL,
    SPLCv,                    SPLEquipmentName,
    STDevDeadZoneMeasurement, sub_id,
    SUMDeadZoneMeasurement,   TCor,
    TemporalStability,        Tmod,
    total_eff,                UnitValue,
    v_lamp,                   VARDeadZoneMeasurement,
    VmaxP,                    Voc,
    WAR_Avg
    from WORK.ME_INT_ISCAD2_&SPUL._TEMP;
quit;

/*****************************************************************************
 * SAS code to                                                               *
 * Delete temporary data set.                                                *
 *****************************************************************************/

proc datasets library=WORK noprint;
    delete ME_INT_ISCAD2_&SPUL._TEMP;
run;

%IF &DebugFlag = True %THEN %DO;

    /*************************************************************************
     * Used SAS Tasks | Statistics | Descriptive | Table Analysis to         *
     * Show the frequencies for various partitions for comparison.           *
     *************************************************************************/

    title1   color =blue bold "************ Section F ************";
    title3   "Frequencies for Partition Variable Partition1Tr2Va3Te";
    footnote "Data Source WORK.ME_INT_ISCAD2_&SPUL._ALL3";

    proc freq data=WORK.ME_INT_ISCAD2_&SPUL._ALL3;
        tables (Partition1Tr2Va3Te) * (UnitValue)
        / missing nopercent norow nocum plots=none;
    run;

%END;

/*****************************************************************************
 * Used SAS code to                                                          *
 * Place the data file in the CAS library for use in constructing models.    *
 *                                                                           *
 * NOTE: There are two sets of code (after the libname statement).           *
 * The first set is used for development (with the second set commented out).*
 * The second set is used (with the first set commnted out) when development *
 * has progressed to the point where the file can be confidently "promoted"  *
 * to the shared CAS library.                                                *
 *                                                                           *
 *****************************************************************************/

libname _tmpcas_ cas caslib="CASUSER";

/*
proc sql noprint;
    create table PUBLIC.ME_INT_ISCAD2_&SPUL._ALL3
    as
    select *
    from WORK.ME_INT_ISCAD2_&SPUL._ALL3;
quit;
*/

/*****************************************************************************
 * Go ahead and promote the modified file to CAS.                            *
 * We're ready to roll and use the SAS Model Studio pipeline!                *
 *****************************************************************************/

/*****************************************************************************/
/* 20301 (TWS) :  Adding IF ELSE logic to generate dataset based on purpose: */
/* 	  Purpose=ALL3 for Project Retraining 									 */
/* 	  Purpose=Score for Scoring 											 */
/*****************************************************************************/

%PUT Purpose = &Purpose;
%IF %UPCASE(&Purpose)=ALL3 %THEN %DO;
	%PUT Delete ME_INT_ISCAD2_&SPUL._ALL3;
	%deletedsifexists(lib=PUBLIC, name=ME_INT_ISCAD2_&SPUL._ALL3);
	
	%put write PUBLIC.ME_INT_ISCAD2_&SPUL._ALL3;
	data  PUBLIC.ME_INT_ISCAD2_&SPUL._ALL3 (promote=YES);
	    set WORK.ME_INT_ISCAD2_&SPUL._ALL3;
	run;
%END;
%IF %UPCASE(&Purpose)=SCORE %THEN %DO;

	%deletedsifexists(lib=PUBLIC, name=ME_INT_ISCAD2_&SPUL._Score);
	PROC SQL; SELECT COUNT(*) INTO :ScoreObs FROM WORK.ME_INT_ISCAD2_&SPUL._ALL3 WHERE Partition1Tr2Va3Te = '2-Test'; QUIT;
	%PUT ScoreObs:  &ScoreObs;
	
	data  PUBLIC.ME_INT_ISCAD2_&SPUL._Score (promote=YES);
	    set WORK.ME_INT_ISCAD2_&SPUL._ALL3 (WHERE=(Partition1Tr2Va3Te = '2-Test'));
	run;
%END;

%IF &DebugFlag = True %THEN %DO;

    /*************************************************************************
     * Used SAS Tasks | Statistics | Descriptive | Table Analysis to         *
     * Show the frequencies for various partitions for comparison.           *
     *************************************************************************/

    ods noproctitle;

    title    "Frequencies for Partition Variable Partition1Tr2Va3Te";
    footnote "Data Source PUBLIC.ME_INT_ISCAD2_&SPUL._ALL3";

    proc freq data=PUBLIC.ME_INT_ISCAD2_&SPUL._ALL3;
        tables (Partition1Tr2Va3Te) * (UnitValue)
        / missing nopercent norow nocum plots=none;
    run;

    /*************************************************************************
     * Used SAS Tasks | Prepare Data | Examine Data | List Table Att. to     *
     * Obtain meta-data attributes of variables in combined data file.       *
     *************************************************************************/

    title    "Table of Meta-Data for Variable Attributes in Combined File";
    footnote "Data Source PUBLIC.ME_INT_ISCAD2_&SPUL._ALL3";

    ods select attributes variables;

    proc datasets;
        contents data=PUBLIC.ME_INT_ISCAD2_&SPUL._ALL3 order=collate;
    quit;

%END;

/*>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
 >                                                                           >
 > Conclusions gleaned from Section F:                                       >
 >                                                                           >
 > (1) Three separate partition data files were combined.                    >
 >                                                                           >
 > (2) Various variables were dropped.                                       >
 >                                                                           >
 > (3) The combined data file was stored in SAS CAS for future use within    >
 >     SAS Model Studio.                                                     >
 >                                                                           >
 > (4) In checking the work to see if the variables are as they should be,   >
 >     it turns out that they were.                                          >
 >                                                                           >
 > Now, on to SAS Model Studio. ...                                          >
 >                                                                           >
 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>*/

%MEND;

