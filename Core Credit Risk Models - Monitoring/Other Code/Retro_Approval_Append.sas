/* Pull approval table from APS */
proc sql stimer;        
		connect to ODBC (dsn=mpwaps);
        create table Retro_Approval as
        select * from connection to odbc 
		(
            select * from DEV_DataDistillery_General.dbo.Retro_Approval
        ) ;
        disconnect from odbc;
quit;

data _null_;
	call symput("month", put(intnx("month", today(),-1,'end'),yymmn6.));
run;
%put &month;

proc sql stimer;        
		connect to ODBC (dsn=mpwaps);
        create table Retro_rundate as
        select * from connection to odbc 
		(
            select top 1 rundate from PRD_DataDistillery_Data.dbo.TC_v6_Retro_disb_&month
        ) ;
        disconnect from odbc;
quit;

/* Create date macro variables */
proc sql noprint; 
	select Approval_Date, Approver into :Approval_Date, :Approver
	from Retro_Approval
	where prxmatch("/eshikwa.*/", LOWCASE(Approver)) or prxmatch("/tmphogo.*/", LOWCASE(Approver))
	having Approval_Date = max(Approval_Date);
quit;
proc sql noprint; select max(rundate) into :Pt1run_date from Retro_rundate;quit;

/* See All Dates */
%put &Approval_Date;
%put &Approver;
%put &Pt1run_date;


%macro upload_retro_Trigger;
  %if (&Approval_Date >= &Pt1run_date) 
	%then %do;  
		%put Approval has been given. Please append the TC_v6_Retro_disb tables to the APS.;

		/*Append TC_v6_Retro_disb */

		proc sql; connect to odbc (dsn=MPWAPS);
		execute 
		(
			/*---- Drop Table ----*/
			IF OBJECT_ID('PRD_DataDistillery_data.dbo.TC_v6_Retro_disb', 'U') IS NOT NULL 
			DROP TABLE PRD_DataDistillery_data.dbo.TC_v6_Retro_disb;

			/*---- Append Tables ----*/
			Create table PRD_DataDistillery_data.dbo.TC_v6_Retro_disb
			with (distribution = hash(loanid), clustered columnstore index ) as
			select * 
			from PRD_DataDistillery_data.dbo.TC_v6_Retro_disb_BackUp
			union all
			select * 
			from PRD_DataDistillery_data.dbo.TC_v6_Retro_disb_&month;
) by odbc;
quit;
	%end; 
  %else 
	%put Either the TU or CS applicanbase tables on the APS are up to date or approval has not yet been given. Do not upload any tables.;
%mend;     
%upload_retro_Trigger;
