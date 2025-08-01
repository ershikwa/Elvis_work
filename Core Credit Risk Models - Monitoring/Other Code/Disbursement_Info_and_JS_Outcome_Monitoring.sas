proc sql stimer;
connect to ODBC (dsn=MPWAPS);
create table Var_Missings_DI as 
select * from connection to odbc 
(	
	select 
	max(RunDate) as RunDate, count(*) as No_OBS,
	Avg(case when IDNumber is null then 1.0 else 0.0 end) as IDNumber,
	Avg(case when LoanID is null then 1.0 else 0.0 end) as LoanID,
	Avg(case when LoanReference is null then 1.0 else 0.0 end) as LoanReference,
	Avg(case when Create_Date is null then 1.0 else 0.0 end) as Create_Date,
	Avg(case when DisbStartDate is null then 1.0 else 0.0 end) as DisbStartDate,
	Avg(case when CapitalDisbursed is null then 1.0 else 0.0 end) as CapitalDisbursed,
	Avg(case when Term is null then 1.0 else 0.0 end) as Term,
	Avg(case when Product is null then 1.0 else 0.0 end) as Product,
	Avg(case when SubGroupCode is null then 1.0 else 0.0 end) as SubGroupCode,
	Avg(case when WageType is null then 1.0 else 0.0 end) as WageType,
	Avg(case when OfferGroup is null then 1.0 else 0.0 end) as OfferGroup,
	Avg(case when FirstDueDate is null then 1.0 else 0.0 end) as FirstDueDate
	from Prd_DataDistillery.dbo.Disbursement_Info
);

create table Var_Freq_DI as 
select * from connection to odbc 
(	
	select TblRef, Product, WageType, OfferGroup, Count(*), max(RunDate) as RunDate
	from Prd_DataDistillery.dbo.Disbursement_Info
	group by TblRef, Product, WageType, OfferGroup
	order by TblRef, Product, WageType, OfferGroup
);

create table Var_Missings_JSO as 
select * from connection to odbc 
(	
	select 
	max(RunDate) as RunDate, count(*) as No_OBS,
	Avg(case when IDNumber is null then 1.0 else 0.0 end) as IDNumber,
	Avg(case when LoanID is null then 1.0 else 0.0 end) as LoanID,
	Avg(case when LoanReference is null then 1.0 else 0.0 end) as LoanReference,
	Avg(case when Capital_OR_Limit is null then 1.0 else 0.0 end) as Capital_OR_Limit,
	Avg(case when Principaldebt is null then 1.0 else 0.0 end) as Principaldebt,
	Avg(case when CONTRACTUAL_3_LE9 is null then 1.0 else 0.0 end) as CONTRACTUAL_3_LE9,
	Avg(case when Term is null then 1.0 else 0.0 end) as Term,
	Avg(case when OfferGroup is null then 1.0 else 0.0 end) as OfferGroup,
	Avg(case when Product is null then 1.0 else 0.0 end) as Product,
	Avg(case when WageType is null then 1.0 else 0.0 end) as WageType,
	Avg(case when CompanyCode is null then 1.0 else 0.0 end) as CompanyCode,
	Avg(case when Create_Date is null then 1.0 else 0.0 end) as Create_Date,
	Avg(case when FirstdueMonth is null then 1.0 else 0.0 end) as FirstdueMonth,
	Avg(case when FirstDueDate is null then 1.0 else 0.0 end) as FirstDueDate
	from Prd_DataDistillery.dbo.JS_OUTCOME_BASE_FINAL
);
disconnect from ODBC;
quit;

%let ReportPath = \\mpwsas64\Core_Credit_Risk_Models\Disbursement_Info and JS_Outcome Monitoring; 
libname Data "&ReportPath";

proc append base=Data.Var_Missings_DI data=Var_Missings_DI force;
run;
proc append base=Data.Var_Freq_DI data=Var_Freq_DI force;
run;
proc append base=Data.Var_Missings_JSO data=Var_Missings_JSO force;
run;

proc sort data = Data.Var_Missings_DI; by descending RunDate; run;
proc sort data = Data.Var_Freq_DI; by descending RunDate; run;
proc sort data = Data.Var_Missings_JSO; by descending RunDate; run;

data TF_DI_Monitoring_Missings;
set Data.Var_Missings_DI;
run;
data TF_DI_Monitoring_Freq;
set Data.Var_Freq_DI;
run;
data TF_JSO_Monitoring_Missings;
set Data.Var_Missings_JSO;
run;

%Upload_APS(Set = TF_DI_Monitoring_Missings , Server = Work, APS_ODBC = Dev_DDCr, APS_DB = DEV_DataDistillery_Credit);
%Upload_APS(Set = TF_DI_Monitoring_Freq , Server = Work, APS_ODBC = Dev_DDCr, APS_DB = DEV_DataDistillery_Credit);
%Upload_APS(Set = TF_JSO_Monitoring_Missings , Server = Work, APS_ODBC = Dev_DDCr, APS_DB = DEV_DataDistillery_Credit);

%macro runme_table(Data=, Title =, Varlist=);
	proc report data=&Data (OBS=7) style(report)={outputwidth=100%};
	title HEIGHT =.20in &Title;
	column RunDate No_OBS &Varlist;
	format No_OBS comma10.;
	%do i = 1 %to %sysfunc(countw(&Varlist));
		%let var = %scan(&Varlist, &i);
		format &var. percent10.2;
		define &var./center display;
		compute &var;
		   if &var >= 0.05 then call define(_col_,"style","style={background=#ff684c}");
		   else if &var >= 0.02 then call define(_col_,"style","style={background=#ffda66}");
		   else call define(_col_,"style","style={background=#8aca7e}"); 
		endcomp;
	%end;
	run;
%mend;

options nodate;
ods pdf file = "&ReportPath.\DI and JSO Monitoring.pdf" UNifORM;
options orientation=landscape nocenter;
%runme_table(
	Data=Data.Var_Missings_DI, 
	Title = 'Disbursement Info Monitoring (% Missing)',
	Varlist=IDNumber LoanID LoanReference Create_Date DisbStartDate CapitalDisbursed Term Product SubGroupCode WageType OfferGroup FirstDueDate);
%runme_table(
	Data=Data.Var_Missings_JSO,
	Title = 'JS Outcomes Monitoring (% Missing)',
	Varlist=IDNumber LoanID LoanReference Capital_OR_Limit Principaldebt CONTRACTUAL_3_LE9 Term OfferGroup Product WageType CompanyCode Create_Date FirstdueMonth FirstDueDate);
ods pdf close;
ods _all_ close;

%macro sendMonitorReport();
	options mprint mlogic;
	options emailport = 25;
	options emailsys = SMTP;
	options emailhost = midrandcasarray.africanbank.net;
	options emailackwait=300;

	FILENAME outbox EMAIL ("DataScienceAutomation@africanbank.co.za");
	DATA _NULL_;
		FILE outbox
		TO=("tsehlapelo1@africanbank.co.za")
		FROM=("tsehlapelo1@africanbank.co.za")
		SUBJECT=("Test Monitoring Email")
		ATTACH=("&ReportPath.\DI and JSO Monitoring.pdf");
		PUT " ";
		PUT "Good day all";
		PUT " ";
		PUT "This is a test.";
		PUT " ";
		PUT " ";
		PUT "Kind regards,";
		PUT "Scoring Team";
	RUN;
%mend;
options nomprint nosymbolgen nomlogic;
%sendMonitorReport;
