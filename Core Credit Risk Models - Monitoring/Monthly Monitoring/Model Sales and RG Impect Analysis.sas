/* Specify Month */
%let month = %sysfunc(today(), yymmn6.);
%put &month;
%let Appmonth = &month;
%let Dataset = RawData_&month;
%put &Appmonth;
%put &Dataset;

libname Path '\\mpwsas64\Core_Credit_Risk_Model_Team\Mpho_Thebeyagae\Monthly_sales_impact';

/* APS data extraction */
proc sql stimer;
	connect to ODBC (dsn=MPWAPS);
		create table Path.&Dataset. as 
		select * from connection to odbc 
		(	
			Select
			a.uniqueid,	a.tranappnumber, a.applicationdate,
			a.offer, a.SCORECARDVERSION,
			B.CapitalDisbursed, B.Product, B.Capital_OR_Limit,
			case
				when a.SCORECARDVERSION = 'V622' then V622_RiskGroup
				when a.SCORECARDVERSION = 'V636' then V636_RiskGroup
				when a.SCORECARDVERSION = 'V645' then V645_RiskGroup
			end as SCOREBAND,
			case
				when B.loanid is not null then 'DISBURSED'
				else 'Not DISBURSED'
			end as Sample_Ind
			from (select * from DEV_DataDistillery_General.dbo.TU_ApplicationBase
				where Appmonth = &Appmonth) A
			left join PRD_DataDistillery.dbo.disbursement_info B
			on A.tranappnumber = B.loanid
			where A.SCORECARDVERSION in ('V622','V636','V645')
      );
      disconnect from ODBC;
quit;

proc sort data = Path.&Dataset.;
      by tranappnumber descending uniqueid;
run;

proc sort data = Path.&Dataset. nodupkey out = Rawdata_No_Dup;
      by tranappnumber;
run;

%let Input_Data = Rawdata_No_Dup;

/* Get App, Offer and TakeUp Values */
proc freq data=&Input_Data;
tables SCORECARDVERSION / missing out = Apps_Table;
run;
proc freq data=&Input_Data;
tables SCORECARDVERSION*Offer / missing out = Offer_Table (where = (Offer = 1));
run;
proc freq data=&Input_Data;
tables SCORECARDVERSION*Sample_Ind / missing out = TakeUp_Table (where = (Sample_Ind = 'DISBURSED'));
run;

/* Sales Impect */
proc summary data = &Input_Data (where=(Sample_Ind = 'DISBURSED')) nway missing;
    class SCORECARDVERSION;
    var CapitalDisbursed;
    output out = Sales_Table (drop = _type_) sum() =;
run;

/* Summary Table */
proc sql;
	/*Query 1*/
	create table Table_1 as 
		select
			A.SCORECARDVERSION as Model, A.COUNT as Apps, B.COUNT as Offers,
			Offers/Apps as Offer_Rate, C.COUNT as TakeUps, TakeUps/Offers as TakeUp_Rate,
			CapitalDisbursed, CapitalDisbursed/TakeUps as Avg_CapitalDisbursed,
			A.PERCENT/100 as Apps_Per, CapitalDisbursed/(A.PERCENT/100) as Normalised_Sales
		from Apps_Table A
		left join Offer_Table B
		on A.SCORECARDVERSION = B.SCORECARDVERSION 
		left join TakeUp_Table C
		on A.SCORECARDVERSION = C.SCORECARDVERSION
		left join Sales_Table D
		on A.SCORECARDVERSION = D.SCORECARDVERSION;

	/*Query 2*/
	select Normalised_Sales into : Base_Normalised_Sales
		from Table_1
		where Model = 'V636';

	/*Query 3*/
	create table Table_1 as 
		select *, Normalised_Sales/&Base_Normalised_Sales - 1 as Lift_Against_V636
	from Table_1;
quit;

proc transpose data = Table_1 out = Table_2(rename=(_NAME_ = Variable) drop =_LABEL_);
	ID Model;
run;

proc sql;
	select SCORECARDVERSION into :Scorecard_list separated by ", "
	from Apps_Table;
quit;
%put &Scorecard_list;

data Table_2;
	set Table_2;
	if Variable in ('Apps', 'Apps_Per', 'Offers', 'TakeUps', 'CapitalDisbursed') then Overall = sum(&Scorecard_list);
run;

proc transpose data = Table_2 out = Table_3 (where=(_NAME_ = 'Overall') keep=_NAME_ Apps Offers TakeUps CapitalDisbursed);
ID Variable;
run;

data _null_;
	set Table_2 (where=(Variable='Normalised_Sales'));
	call symputx('V636_Norm_Sales', V636);
run;

data _null_;
	set Table_3;
	call symputx('Apps', Apps);
	call symputx('Offers', Offers);
	call symputx('TakeUps', TakeUps);
	call symputx('CapitalDisbursed', CapitalDisbursed);
run;

data Table_3;
set Table_2;
format Name $40.;
if Variable = 'Offer_Rate' then Overall = &Offers/&Apps;
if Variable = 'TakeUp_Rate' then Overall = &TakeUps/&Offers;
if Variable = 'Avg_CapitalDisbursed' then Overall = &CapitalDisbursed/&TakeUps;

if Variable = 'Apps' then Name = 'Applications';
else if Variable = 'Apps_Per' then Name = '% Applications';
else if Variable = 'Offers' then Name = 'Offers';
else if Variable = 'Offer_Rate' then Name = 'Offer Rate';
else if Variable = 'TakeUps' then Name = 'Take-Ups';
else if Variable = 'TakeUp_Rate' then Name = 'Take-Up Rate';
else if Variable = 'CapitalDisbursed' then Name = 'Capital Disbursed';
else if Variable = 'Avg_CapitalDisbursed' then Name = 'Average Capital Disbursed';
else if Variable = 'Normalised_Sales' then Name = 'Normalised Sales';
else if Variable = 'Lift_Against_V636' then Name = 'Lift Against V636';
run;

data _null_;
	set Table_3 (where=(Variable = 'Lift_Against_V636'));
	call symputx('V622_Lift', V622);
	call symputx('V645_Lift', V645);
run;

proc transpose data = Table_3 out = Temp_4 (drop=_LABEL_ Variable);
ID Name;
run;

data V645_Sales_Impacte_1(drop=_NAME_);
set Temp_4;
format Model $10.;
if _NAME_ = 'V622' then Model = 'V6.22';
else if _NAME_ = 'V636' then Model = 'V6.36';
else if _NAME_ = 'V645' then Model = 'V6.45';
else if _NAME_ = 'Overall' then Model = 'Overall';
run;

/* RG Distribyion Table */
proc sort data = &Input_Data ;
  by SCORECARDVERSION;
run;

proc freq data = &Input_Data (where=(SCOREBAND ne .));
by SCORECARDVERSION;
tables SCOREBAND / missing out=V645_Sales_Impacte_3;
run;

data V645_Sales_Impacte_3(drop=SCOREBAND);
set V645_Sales_Impacte_3;
PERCENT = PERCENT/100;
Risk_Group = SCOREBAND;
run;

proc sql;
	create table Low_RG as 
	select SCORECARDVERSION, sum(PERCENT)
	from V645_Sales_Impacte_3
	where Risk_Group <= 53
	group by SCORECARDVERSION;
quit;

proc transpose data = Low_RG out = Low_RG;
ID SCORECARDVERSION;
run;

data _null_;
	set Low_RG;
	call symputx('V622_Low_RG', V622);
	call symputx('V636_Low_RG', V636);
	call symputx('V645_Low_RG', V645);
run;

data V645_Sales_Impacte_2;
	Month = &Appmonth;
	WA_Lift = &CapitalDisbursed/&V636_Norm_Sales -1;
	V622_Low_RG = &V622_Low_RG;
	V636_Low_RG = &V636_Low_RG;
	V645_Low_RG = &V645_Low_RG;
	V622_Lift = &V622_Lift;
	V645_Lift = &V645_Lift;
run;

/* Upload Tables to APS */
libname dbo_path odbc dsn=Dev_DDCr schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

proc sql stimer; 
	connect to odbc (dsn=mpwaps);
	select * from connection to odbc (
	    drop table DEV_DataDistillery_Credit.dbo.V645_Sales_Impacte_1;
		drop table DEV_DataDistillery_Credit.dbo.V645_Sales_Impacte_2;
		drop table DEV_DataDistillery_Credit.dbo.V645_Sales_Impacte_3;
	);
	disconnect from odbc;
quit;

proc sql;
	create table  dbo_path.V645_Sales_Impacte_1(BULKLOAD=YES) as
	select * from V645_Sales_Impacte_1;

	create table  dbo_path.V645_Sales_Impacte_2(BULKLOAD=YES) as
	select * from V645_Sales_Impacte_2;

	create table  dbo_path.V645_Sales_Impacte_3(BULKLOAD=YES) as
	select * from V645_Sales_Impacte_3;
quit;