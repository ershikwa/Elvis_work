/* Pull approval table from APS */
proc sql stimer;        
		connect to ODBC (dsn=mpwaps);
        create table CS_TU_Approval as
        select * from connection to odbc 
		(
            select * from DEV_DataDistillery_General.dbo.CS_TU_Approval
        ) ;
        disconnect from odbc;
quit;

data _null_;
	call symput("month", put(intnx("month", today(),-1,'end'),yymmn6.));
run;
%put &month;

proc sql stimer;        
		connect to ODBC (dsn=mpwaps);
        create table CS_TU_rundate as
        select * from connection to odbc 
		(
            select top 1 rundate from DEV_DataDistillery_General.dbo.TU_applicationbase_&month
        ) ;
        disconnect from odbc;
quit;


/* Create date macro variables */
proc sql noprint; 
	select Approval_Date, Approver into :Approval_Date, :Approver
	from CS_TU_Approval
	where prxmatch("/eshikwa.*/", LOWCASE(Approver)) or prxmatch("/tmphogo.*/", LOWCASE(Approver)) or prxmatch("/kmolawa.*/", LOWCASE(Approver))
	having Approval_Date = max(Approval_Date);
quit;
proc sql noprint; select max(rundate) into :Pt1run_date from CS_TU_rundate;quit;

/* See All Dates */
%put &Approval_Date;
%put &Approver;
%put &Pt1run_date;


%macro upload_Trigger;
  %if (&Approval_Date >= &Pt1run_date)
	%then %do;  
		%put Approval has been given. Please append the TU and CS applicationbase tables to the APS.;

		/*Append TU_applicationbase */
		proc sql; connect to odbc (dsn=MPWAPS);
		execute 
		(
/*			---- Drop Table ----*/
			IF OBJECT_ID('DEV_DataDistillery_General.dbo.TU_applicationbase', 'U') IS NOT NULL 
			DROP TABLE DEV_DataDistillery_General.dbo.TU_applicationbase;

			/*---- Append Tables ----*/
			Create table DEV_DataDistillery_General.dbo.TU_applicationbase
			with (distribution = hash(baseloanid), clustered columnstore index ) as
			select * 
			from DEV_DataDistillery_General.dbo.TU_applicationbase_BackUp
			union all
			select * 
			from DEV_DataDistillery_General.dbo.TU_applicationbase_&month;

			/*----- Drop retro monthly table ---- */
			IF OBJECT_ID('DEV_DataDistillery_General.dbo.TU_applicationbase_&month', 'U') IS NOT NULL 
			DROP TABLE DEV_DataDistillery_General.dbo.TU_applicationbase_&month;
		) by odbc;
		quit;

		/*Append CS_applicationbase */
		proc sql; connect to odbc (dsn=MPWAPS);
		execute 
		(
			/*---- Drop Table ----*/
			IF OBJECT_ID('DEV_DataDistillery_General.dbo.CS_applicationbase', 'U') IS NOT NULL 
			DROP TABLE DEV_DataDistillery_General.dbo.CS_applicationbase;

			/*---- Append Tables ----*/
			Create table DEV_DataDistillery_General.dbo.CS_applicationbase
			with (distribution = hash(baseloanid), clustered columnstore index ) as
			select * 
			from DEV_DataDistillery_General.dbo.CS_applicationbase_BackUp
			union all
			select * 
			from DEV_DataDistillery_General.dbo.CS_applicationbase_&month;

			/*----- Drop retro monthly table ---- */
			IF OBJECT_ID('DEV_DataDistillery_General.dbo.CS_applicationbase_&month', 'U') IS NOT NULL 
			DROP TABLE DEV_DataDistillery_General.dbo.CS_applicationbase_&month;
		) by odbc;
		quit;
	%end; 
  %else 
	%put Either the TU or CS applicanbase tables on the APS are up to date or approval has not yet been given. Do not upload any tables.;
%mend;     
%upload_Trigger;
