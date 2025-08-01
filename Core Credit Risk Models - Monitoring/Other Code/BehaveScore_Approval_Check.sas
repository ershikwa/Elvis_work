/* Upload Behavescore tables Trigger */
/* Pull data from APS */
proc sql stimer;        
		connect to ODBC (dsn=mpwaps);
        create table BehaveScore_Approval as
        select * from connection to odbc 
		(
            select * from DEV_DataDistillery_General.dbo.BehaveScore_Approval
        ) ;
        disconnect from odbc;
quit;

proc sql stimer;        
		connect to ODBC (dsn=mpwaps);
        create table Behavescore_RunDate as
        select * from connection to odbc 
		(
            select top 1 RunDate, left(RunDate, 6) as RunMonth
            from PRD_DataDistillery.dbo.Behavescore
        ) ;
        disconnect from odbc;
quit;

/* Create date macro variables */
/*data _null_;*/
/*	set BehaveScore_Approval */
*	(where = (prxmatch("/eshikwambana.*/", LOWCASE(Approver)) or prxmatch("/tmphogo.*/", LOWCASE(Approver))
	or prxmatch("/kmolawa.*/", LOWCASE(Approver))));
/**/
/*	if Approval_Date = max(Approval_Date);*/
/*	call symputx('Approval_Date', max(Approval_Date));*/
/*run;*/

proc sql noprint;
	select max(Approval_Date) into :Approval_Date
	from BehaveScore_Approval
	where prxmatch("/eshikwambana.*/", LOWCASE(Approver)) or prxmatch("/tmphogo.*/", LOWCASE(Approver))
	or prxmatch("/kmolawa.*/", LOWCASE(Approver));
quit;

data _null_;
	set Behavescore_RunDate;
	call symputx('RunMonth_on_APS', RunMonth);
run;

%put &Approval_Date;
%put &RunMonth_on_APS;

data filenames;
	length fref $8 fname $200 fref2 $8 fname2 $200;
	did = filename(fref,'\\mpwsas5\G\Automation\Behavescore\Datasets');
	did = dopen(fref);
	do i = 1 to dnum(did);
	  fname = dread(did,i);
	  output;
	end;
	did = dclose(did);
	did = filename(fref);

	did2 = filename(fref2,'\\mpwsas64\Core_Credit_Risk_Models\BehaveScoreV2 Data');
	did2 = dopen(fref2);
	do i = 1 to dnum(did2);
	  fname2 = dread(did2,i);
	  output;
	end;
	did2 = dclose(did2);
	did2 = filename(fref2);
	keep fname fname2;
run;

data filenames1;
	set filenames;
	where fname like "behave_%";
	RunDate = input(substr(fname,8,8), 8.);
	RunMonth = substr(fname,8,6);
	drop fname2;
run;

data filenames2;
	set filenames;
	where fname2 like "behavev2_%";
	RunDate2 = input(substr(fname2 ,10,8), 8.);
	RunMonth2 = substr(fname2 ,10,6);
	drop fname;
run;

data _null_;
	set filenames1 (where=(RunDate=max(RunDate)));
	call symputx('Last_RunDate', RunDate);
	call symputx('Last_RunMonth', RunMonth);
run;
data _null_;
	set filenames2 (where=(RunDate2=max(RunDate2)));
	call symputx('Last_RunDateBehV2', RunDate2);
	call symputx('Last_RunMonthBehV2', RunMonth2);
run;

/* See All Dates */
%put RunMonth_on_APS = &RunMonth_on_APS;
%put Approval_Date = &Approval_Date;
%put Last_RunDate = &Last_RunDate;
%put Last_RunMonth = &Last_RunMonth;
%put Last_RunDateBehV2 = &Last_RunDateBehV2;
%put Last_RunMonthBehV2 = &Last_RunMonthBehV2;


%macro BehaveScore_Trigger;
  %if (&Last_RunMonth >= &RunMonth_on_APS) and (&Last_RunMonthBehV2 >= &RunMonth_on_APS)
  	 and (Approval_Date >= &Last_RunDate) and (Approval_Date >= &Last_RunDateBehV2)  
	%then %do;  
		%put Approval has been given, upload the latest behavescore tables to the APS.;
		Libname Path1 '\\mpwsas5\G\Automation\Behavescore\Datasets';
		Libname Path2 '\\mpwsas64\Core_Credit_Risk_Models\BehaveScoreV2 Data';

		Data Behavescore;
		set Path1.Behave_&Last_RunDate. (Keep = idno BehaveScore RunDate);
		run;

		Data BehavescoreV2;
		set Path2.BehaveV2_&Last_RunDateBehV2. (Keep = idno BehaveScore BehaveScoreV2 RunDate);
		run;

		Data Behave_&Last_RunDateBehV2.;
		set BehavescoreV2;
		run;

		%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\Upload_APS.sas";
		%Upload_APS(Set = Behavescore, Server = work, APS_ODBC = Prd_DaDi, APS_DB = PRD_DataDistillery,distribute = hash(idno));
		%Upload_APS(Set = BehavescoreV2, Server = work, APS_ODBC = Prd_DaDi, APS_DB = PRD_DataDistillery,distribute = hash(idno));

		%Upload_APS(Set = Behavescore, Server = work, APS_ODBC = Prd_DDDa, APS_DB = PRD_DataDistillery_Data,distribute = hash(idno));
		%Upload_APS(Set = BehaveScoreV2, Server = work, APS_ODBC = Prd_DDDa, APS_DB = PRD_DataDistillery_Data,distribute = hash(idno));
		%Upload_APS(Set = Behave_&Last_RunDateBehV2., Server = work, APS_ODBC = Prd_DDDa, APS_DB = PRD_DataDistillery_Data,distribute = hash(idno));
	%end; 
  %else 
	%put Either the BehaveScore tables on the APS are up to date or approval has not yet been given. Do not upload an tables.;
%mend;     
%BehaveScore_Trigger;
