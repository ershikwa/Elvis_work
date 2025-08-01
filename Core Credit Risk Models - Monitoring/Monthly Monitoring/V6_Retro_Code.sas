OPTIONS NOSYNTAXCHECK ;
options compress = yes;
options mstored sasmstore=sasmacs; 
/*libname sasmacs "\\neptune\credit$\AA_GROUP CREDIT\Scoring\Model Macros\"; */
libname tu '\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets';

/*%include "\\neptune\credit$\AA_GROUP CREDIT\Scoring\SAS Macros\Upload_APS.sas"; */
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\Upload_APS.sas";
%let odbc = MPWAPS;

data _null_;
	call symput("month", put(intnx("month", today(),-1,'end'),yymmn6.));
	call symput("prevmonth", put(intnx("month", today(),-2,'end'),yymmn6.));
	call symput("prev2month", put(intnx("month", today(),-3,'end'),yymmn6.));
run;
%put &month;
%put &prevmonth;
%put &prev2month;

/* Create a backup of the Retro table */
/*proc sql; connect to odbc (dsn=MPWAPS);*/
/*execute (*/
/*		IF OBJECT_ID('PRD_DataDistillery_data.dbo.TC_v6_Retro_disb_BackUp', 'U') IS NOT NULL */
/*		DROP TABLE PRD_DataDistillery_data.dbo.TC_v6_Retro_disb_BackUp;*/
/*		create table PRD_DataDistillery_data.dbo.TC_v6_Retro_disb_BackUp */
/*		with (distribution = hash(loanid), clustered columnstore index ) as*/
/*			select **/
/*			from PRD_DataDistillery_data.dbo.TC_v6_Retro_disb;*/
/*		) by odbc;*/
/*quit;*/


/*Get current applications*/
proc sql stimer;
	connect to ODBC (dsn=&odbc);
	create table allapps as
	select * from connection to odbc ( 
		select	uniqueid, baseloanid, appmonth, Combine_Thin as comb_thin, concat(comp_seg,Tu_seg) as v6_seg,
				V500, V505, V530 ,V535,
				V620, V621, V622,
				V631, V632, V633, V634, V635, V636,
				V640, V641, V642, V643, V644, V645, V645_adj,
				V650_2, V651_2, V652_2, V653_2, V654_2, V655_2, V655_2_B05,
				V650, V651, V652, V653, V654, V655,
				V660, V661, V662, V663, V664, V665, V667,
				V535_finalscore,
				V622_finalriskscore as V622_finalscore,
				V635_finalriskscore as V635_finalscore,
				V636_finalriskscore as V636_finalscore, 
				V645_finalriskscore as V645_finalscore, V645_Adjfinalriskscore, 
				V655_2_finalriskscore as V655_2_finalscore, V655_2_B05_finalriskscore,
				V655_finalriskscore as V655_finalscore,
				V667_finalriskscore, 
				V535_RG,
				V622_RiskGroup as V622_RG,
				V635_RiskGroup as V635_RG,
				V636_RiskGroup as V636_RG,
				V645_RiskGroup as V645_RG, V645_AdjRG,
				V655_2_RiskGroup as V655_2_RG, V655_2_B05_RiskGroup as V655_2_B05_RG,
				V655_RiskGroup as V655_RG,
				V667_RiskGroup as V667_RG,
				transysteminstance, typecode, Institutioncode, EMPLOYERSUBGROUPCODE
		from DEV_DataDistillery_General.dbo.TU_applicationbase
		where appmonth >=&prev2month.;
	);
	disconnect from odbc ;
quit;

data final_model_test(keep = baseloanid);
	set allapps;
run;

proc sql; connect to odbc (dsn=MPWAPS);
	execute 
	(
/*---- Drop Table ----*/
		IF OBJECT_ID('PRD_DataDistillery_data.dbo.final_model_test', 'U') IS NOT NULL 
		DROP TABLE PRD_DataDistillery_data.dbo.final_model_test;
	) by odbc;
quit;
%Upload_APS(Set =final_model_test , Server =Work, APS_ODBC = DEV_DDGe, APS_DB = DEV_DataDistillery_General, distribute = HASH([baseloanid]));

proc sql stimer;
	connect to ODBC (dsn=&odbc);
	create table DisbursedBase as 
	select * from connection to odbc ( 
	select 	A.baseloanid,
					C.product,
					C.FirstDueMonth,
					B.FirstDueDate,
					B.Disbstartdate,
					B.product as product1
			from DEV_DataDistillery_General.dbo.Final_model_test A 
			inner join
			PRD_DataDistillery_data.dbo.JS_Outcome_base_final C
			on a.baseloanid = c.loanid
			left join	(Select loanid, FirstDueDate, Disbstartdate, product 
					from PRD_DataDistillery_data.dbo.Disbursement_Info
					union all
					Select loanid, cast(replace(FirstDueDate,'-','') as numeric) as FirstDueDate, 
					cast(replace(Disbstartdate,'-','') as numeric) as Disbstartdate, product
					from PRD_DataDistillery_data.dbo.Disbursement_Info_Over
				) B
			on a.baseloanid = b.loanid
	) ;
	disconnect from odbc ;
quit;

proc sql;
	create table allapps2 as
		select b.*, a.*
		from allapps a
		left join DISBURSEDBASE b
		on a.baseloanid = b.baseloanid;
quit;

proc sort data=allapps2 nodupkey;
	by BaseLoanid;
run;

data allapps3;
	set allapps2;
	if product1 = "" then product1=product;
	RealMonth = put(datepart(FirstDueMonth),yymmn6.);
	if product1 in ('Card', 'Overdraft') then SecondMonth = substr(compress(Disbstartdate),1,6);
	else if product1 = 'Loan' then SecondMonth = substr(compress(FirstDueDate),1,6);
	month = coalesce(RealMonth, SecondMonth);
	if month=. or month=0 then delete;
run;


/* Creating subset from the applications base */
data subset (drop=month1);
	set allapps3 (rename=(month = month1));
	format month $6.;
	month = month1;
	if month >= &prevmonth.;
run;

proc sql;
	connect to ODBC as Scoring7001 (dsn=mpwaps);
	create table subset_oldretro as      
	   select * from connection to Scoring7001                                                                                                        
	(
		select * from PRD_DataDistillery_data.dbo.TC_V6_Retro_disb_Backup
		where month >= &prevmonth. ;
	);
 	disconnect from Scoring7001;         
quit;

/*data subset_oldretro;*/
/*	set old_v6_Retro;*/
/*	if month >= &prevmonth.;*/
/*run;*/
/*71 752*/


/************Check*****************/
proc sql;
	create table match as
		select a.baseloanid, a.month
		from SUBSET a
		inner join subset_oldretro b
		on a.baseloanid = b.loanid;
	quit;
run;

proc sql;
	create table unmatch as
		select b.baseloanid as loanid, a.*
		from SUBSET a
		left join match b
		on a.baseloanid = b.baseloanid;
	quit;
run;
	
proc sort data=unmatch nodupkey;
	by BaseLoanid;
run;

data unmatch;
	set unmatch;
	if loanid = '';
run;



/*Get the column positions so monthly table aligns with main tables when appending*/
proc contents data= subset_oldretro noprint out=cont;run;
proc sql noprint; 
	select NAME into :retro_vars separated by " ,"
	from cont where NAME not in ('rundate', 'loanid')
	order by VARNUM;
quit;
data unmatch;
	set unmatch;
	V4 = . ;
	V4_finalscore = . ;
	V655_B05= . ;
	V655_B05_finalriskscore = . ;
    V655_B05_RG = . ;
run;
/*********Merge**************/
/* appending the latest month data in the retro */
proc sql;
	create table TC_v6_Retro_disb_&month as
	select baseloanid as loanid, &retro_vars
	from unmatch;
quit;

data TC_v6_Retro_disb_&month;
	set TC_v6_Retro_disb_&month;
	rundate = %sysfunc(today(), yymmddn8.);
run;

%Upload_APS(Set = TC_v6_Retro_disb_&month , Server =Work, APS_ODBC = PRD_DDDa, APS_DB = PRD_DataDistillery_Data , distribute = HASH([loanid]));


/* Retro Table Checks */
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\Amo\V667 monitoring\retro_checks.sas";


/* %include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Monthly Monitoring\send retro email.sas"; */

%macro sendEmail();
%let Attachment1 = \\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard Monthly Monitoring\base_tables_reports\retro_base_&m..pdf;
%let approvalPath = \\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\ ;

%IF %SYSFUNC(FILEEXIST("&Attachment1")) %THEN %DO;
	options emailport=25 emailsys =SMTP emailhost = midrandcasarray.africanbank.net;
	options emailackwait=300;
	filename output email 
	to=("AMpyatona@africanbank.co.za" "kmolawa@africanbank.co.za" "TMphogo@AfricanBank.co.za" "Eshikwambana@AfricanBank.co.za" "ryisa@africanbank.co.za")
/*	from="DataScienceAutomation@AfricanBank.co.za"*/
	to="AMpyatona@africanbank.co.za"
	from="AMpyatona@africanbank.co.za"
	content_type="text/html"
	subject="Retro Table Approval"
	attach=("&Attachment1");

	ods html file=output rs=none;

	proc odstext;
	   p "Hi everyone,";
	   p " ";
	   p " ";
	   p "Please find attached the detailed report on the retro table and approve";
	   p " ";
	   p "Elvis/Tshepo, please run the approval code 'Retro_Approval' in the path below:";
	   p "&approvalPath. ";
	   p "Kind Regards";
	   p "Data Science Automation.";
	run;

	ods html close;
%END;
%ELSE %PUT ERROR: FILE DOES NOT EXIST AND NO EMAIL WILL BE SENT.;
%mend;
%sendEmail();

