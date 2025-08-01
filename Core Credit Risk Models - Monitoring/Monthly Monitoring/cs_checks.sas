%let dset = CS_applicationbase_&month;

data _null_;
	call symput("oneyearago", input(put(intnx("month", today(),-13,'B'),yymmn6.),$6.));
	call symput("appmonth", put(intnx("month", today(),-1,'end'),yymmn6.));
	call symput("appmonth2m", put(intnx("month", today(),-2,'end'),yymmn6.));


	call symput("oneyearago1", cats("'",put(intnx('month',today(),-13,'B'),yymmddd10.),"'"));
	call symput("appmonth1", cats("'",put(intnx('month',today(),-1,'E'),yymmddd10.),"'"));
	call symput("appmonth2", cats("'",put(intnx('month',today(),-2,'E'),yymmddd10.),"'"));

run;

proc sql stimer;
connect to odbc(dsn=MPWAPS);
create table cs_appbase_checks as
select * from connection to odbc( 
	select max(replace(substring(applicationdate,1,7),'-','')) as appmonth,
	sum(case when comp_seg = 1 then 1 else 0 end) as volume_compseg1,
	sum(case when comp_seg = 2 then 1 else 0 end) as volume_compseg2,
	sum(case when comp_seg = 3 then 1 else 0 end) as volume_compseg3,
	sum(case when comp_seg = 4 then 1 else 0 end) as volume_compseg4,
	sum(case when comp_seg = 5 then 1 else 0 end) as volume_compseg5,
	min(V655_2) as min_V655_2, max(V655_2) as max_V655_2, avg(V655_2) as average_V655_2,
	stdev(V655_2) as std_V655_2, min(V655_2_finalriskscore) as min_V655_2_finalscore,
	max(V655_2_finalriskscore) as max_V655_2_finalscore, avg(V655_2_finalriskscore) as average_V655_2_finalscore,
	stdev(V655_2_finalriskscore) as std_V655_2_finalscore, count(V655_2) as V655_2_non_null, 
	min(BehavescoreV2) as min_BehavescoreV2, max(BehavescoreV2) as max_BehavescoreV2,
	avg(BehavescoreV2) as avg_BehavescoreV2,
	sum(case when V655_2 is null then 1 else 0 end)/count(*) as Perc_V655_2_null,
	sum(case when V655_2 <= 0 then 1 else 0 end) as V655_2_zero,
	sum(case when V655_2 >= 1 then 1 else 0 end) as V655_2_one,
	sum(case when V655_2 = V645 then 1 else 0 end) as V655_2eqV645,
	count(V655_2_finalriskscore) as V655_2_finalriskscore_non_null, 
	sum(case when V655_2_finalriskscore is null then 1 else 0 end) as V655_2_finalriskscore_null,
	count(V655_2_RiskGroup) as V655_2_RiskGroup_non_null, 
	sum(case when V655_2_RiskGroup is null then 1 else 0 end) as V655_2_RiskGroup_null,
	count(TU_V580_prob) as CS_V570_prob_non_null,
	sum(case when CS_V570_prob is null then 1 else 0 end) as CS_V570_prob_null,
	sum(case when comp_thin = 1 then 1 else 0 end) as comp_thin1,
	sum(case when comp_thin = 0 then 1 else 0 end) as comp_thin0,
	sum(case when comp_thin is null then 1 else 0 end) as comp_thinNull,
	sum(case when LoanID is null then 1 else 0 end)/count(*) as Perc_LoanID_null,
	sum(case when NationalID is null then 1 else 0 end)/count(*) as Perc_NationalID_null,
	sum(case when UniqueID is null then 1 else 0 end)/count(*) as Perc_UniqueID_null,
	sum(case when baseloanID is null then 1 else 0 end)/count(*) as Perc_baseloanID_null,

	/*cs vars */
	sum(case when ALL_MaxDelq1YearLT24M_W is null then 1 else 0 end)/count(*) as ALL_MaxDelq1YearLT24M_Wnull ,
	sum(case when ALL_TimeMREnq_W is null then 1 else 0 end)/count(*) as ALL_TimeMREnq_Wnull ,
	sum(case when ALL_TimeOldestEnq_W is null then 1 else 0 end)/count(*) as ALL_TimeOldestEnq_Wnull ,
	sum(case when BehavescoreV2_W is null then 1 else 0 end)/count(*) as BehavescoreV2_Wnull ,
	sum(case when CST_CustomerAge_W is null then 1 else 0 end)/count(*) as CST_CustomerAge_Wnull ,
	sum(case when INSTITUTIONCODE_W is null then 1 else 0 end)/count(*) as INSTITUTIONCODE_Wnull ,
	sum(case when GROSSINCOMEADJUSTED_W is null then 1 else 0 end)/count(*) as GROSSINCOMEADJUSTED_Wnull ,
	sum(case when MONTHSATCURRENTEMPLOYER_W is null then 1 else 0 end)/count(*) as MONTHSATCURRENTEMPLOYER_Wnull ,
	sum(case when OTH_AvgMonthsOnBook_W is null then 1 else 0 end)/count(*) as OTH_AvgMonthsOnBook_Wnull ,
	sum(case when OTH_MaxDelqEver_W is null then 1 else 0 end)/count(*) as OTH_MaxDelqEver_Wnull ,
	sum(case when OTH_PercPayments2Years_W is null then 1 else 0 end)/count(*) as OTH_PercPayments2Years_Wnull ,
	sum(case when OWN_Perc1pDelq2Years_W is null then 1 else 0 end)/count(*) as OWN_Perc1pDelq2Years_Wnull ,
	sum(case when RCG_AvgMonthsOnBook_W is null then 1 else 0 end)/count(*) as RCG_AvgMonthsOnBook_Wnull ,
	sum(case when REV_MaxDelq180DaysGE24M_W is null then 1 else 0 end)/count(*) as REV_MaxDelq180DaysGE24M_Wnull ,
	sum(case when REV_NumPayments2Years_W is null then 1 else 0 end)/count(*) as REV_NumPayments2Years_Wnull ,
	sum(case when REV_PercPayments180Days_W is null then 1 else 0 end)/count(*) as REV_PercPayments180Days_Wnull ,
	sum(case when REV_PercPayments1Year_W is null then 1 else 0 end)/count(*) as REV_PercPayments1Year_Wnull ,
	sum(case when UNN_AvgMonthsOnBook_W is null then 1 else 0 end)/count(*) as UNN_AvgMonthsOnBook_Wnull ,
	sum(case when UNS_AvgMonthsOnBook_W is null then 1 else 0 end)/count(*) as UNS_AvgMonthsOnBook_Wnull ,
	sum(case when UNS_RatioMR60DBal1YearAdj is null then 1 else 0 end)/count(*) as UNS_RatioMR60DBal1YearAdj_Wnull

	from DEV_DataDistillery_General.dbo.&dset
	where applicationdate <= &appmonth1
	);
	disconnect from odbc;
quit;

	
	
proc sql stimer;
	connect to odbc(dsn=MPWAPS);

		create table INSTITUTIONCODE as 
		select * from connection to odbc(
		select max(replace(substring(applicationdate,1,7),'-','')) as appmonth,
		(case when INSTITUTIONCODE is null then '(blank)' else INSTITUTIONCODE end) as INSTITUTIONCODE,
		count(case when INSTITUTIONCODE is null then 1 else 2 end) as vol_INSTITUTIONCODE
		from DEV_DataDistillery_General.dbo.&dset
		where applicationdate <= &appmonth1
		group by INSTITUTIONCODE
		);

		create table cstAge as 
		select * from connection to odbc(
		select max(a.appmonth) as appmonth, a.CST_CustomerAge ,
			(case when a.CST_CustomerAge = '<=30' then sum(case when a.CST_CustomerAge = '<=30' then 1 else 0 end)
				when a.CST_CustomerAge ='<=40' then sum(case when a.CST_CustomerAge= '<=40' then 1 else 0 end)
				when a.CST_CustomerAge ='<=50' then sum(case when a.CST_CustomerAge= '<=50' then 1 else 0 end)
				when a.CST_CustomerAge ='>50' then sum(case when a.CST_CustomerAge='>50' then 1 else 0 end) 
				end) as Vol_CustomerAge
		from (select replace(substring(applicationdate,1,7),'-','') as appmonth,
					(case when CST_CustomerAge <= 30 then '<=30' 
						when CST_CustomerAge <= 40 then '<=40'
						when CST_CustomerAge <= 50 then '<=50'
						else '>50' end) as CST_CustomerAge
					from DEV_DataDistillery_General.dbo.&dset
					where applicationdate <= &appmonth1) a
		group by a.CST_CustomerAge
		);

	disconnect from odbc;
quit;


proc sql; 
	create table var_Checks as
		select distinct a.appmonth, a.CST_CustomerAge , a.Vol_CustomerAge,
		b.INSTITUTIONCODE, b.vol_INSTITUTIONCODE
		from cstAge a left join INSTITUTIONCODE b
		on a.appmonth=b.appmonth;
quit;

proc sql;
	create table appbase_all as
		select a.*, b.CST_CustomerAge , b.Vol_CustomerAge, b.INSTITUTIONCODE, b.vol_INSTITUTIONCODE
		from cs_appbase_checks a left join var_Checks b
		on a.appmonth=b.appmonth
		order by appmonth;
quit;


proc sort data=appbase_all;
by appmonth CST_CustomerAge;
run;
data cstAge;
set appbase_all;
by appmonth CST_CustomerAge;
if first.appmonth then rank=0;
rank+1;

if first.CST_CustomerAge then rankcstAge=0;
rankcstAge+1;
run;
proc sort data=cstAge;
by appmonth INSTITUTIONCODE;
run;
data CS_checks;
set cstAge;
by appmonth INSTITUTIONCODE;

if first.INSTITUTIONCODE then rankINSTITUTIONCODE=0;
rankINSTITUTIONCODE+1;
run;

proc sql stimer;
	connect to odbc(dsn=MPWAPS);
	create table CS_checks_past as
	select * from connection to odbc(
		select *
		from DEV_DataDistillery_General.dbo.cs_checks_&appmonth2m
		where appmonth >= &oneyearago1 and appmonth <= &appmonth2m
	);
	disconnect from odbc;
quit;

data cs_checks_&appmonth;
	set CS_checks_past CS_checks;
	where appmonth >= "&oneyearago" and appmonth <= "&appmonth";
run;
proc sort data=cs_checks_&appmonth;
by appmonth;
run;


/*
proc sql; connect to odbc (dsn=MPWAPS);
execute (
		IF OBJECT_ID('DEV_DataDistillery_General.dbo.cs_checks_&appmonth2m', 'U') IS NOT NULL 
		DROP TABLE DEV_DataDistillery_General.dbo.cs_checks_&appmonth2m
		) by odbc;
quit;

%Upload_APS(Set =cs_checks_&appmonth , Server =Work, APS_ODBC = DEV_DDGe, APS_DB = DEV_DataDistillery_General , distribute = HASH([appmonth]));
*/


options orientation=portrait;
ods pdf file="\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard Monthly Monitoring\base_tables_reports\cs_basechecks_&month..pdf" style=seaside startpage=never;
ods graphics / width= 5in height=5in;
data summary;
	set cs_checks_&appmonth;
	keep appmonth Perc_V655_2_null Perc_LoanID_null Perc_NationalID_null Perc_UniqueID_null;
/*	volume_TuSeg1 volume_TuSeg2 volume_TuSeg3 volume_TuSeg4 volume_TuSeg5;*/
/*	where compress(appmonth) >= compress("&oneyearago") and compress(appmonth) <= compress("&appmonth");*/
run;
proc sort data=summary nodup; by descending appmonth;run;
Title "Percentage missing summary for TU_applicationbase";
proc print data= summary;
var appmonth Perc_V655_2_null Perc_LoanID_null Perc_NationalID_null Perc_UniqueID_null;
run;
proc sgplot data= cs_checks_&appmonth;
	series x=appmonth y=average_V655_2;
	Title 'Average V655_2 over time';
	yaxis label= "probability";
	yaxis grid values=(0 to 1 by 0.1);
	where rank=1;* compress(appmonth) >= compress("&oneyearago") and compress(appmonth) <= compress("&appmonth") and rank=1;
run;

Title 'V655_2 final Risk Score Distribution';
proc sgplot data= cs_checks_&appmonth;
	scatter x=appmonth y=min_V655_2_finalscore /markerattrs=(size=8 symbol=circlefilled color=crimson);
	scatter x=appmonth y=average_V655_2_finalscore /markerattrs=(size=8 symbol=circlefilled color=orange);
	scatter x=appmonth y=max_V655_2_finalscore /markerattrs=(size=8 symbol=circlefilled color=green);

	where rank=1; *compress(appmonth) >= compress("&oneyearago") and compress(appmonth) <= compress("&appmonth");
	yaxis grid values=(0 to 1000 by 100) label='Volume';
run;

Title 'BehavescoreV2 Distribution';
proc sgplot data= cs_checks_&appmonth;
	scatter x=appmonth y=min_BehavescoreV2 /markerattrs=(size=8 symbol=circlefilled color=crimson);
	scatter x=appmonth y=avg_BehavescoreV2 /markerattrs=(size=8 symbol=circlefilled color=orange);
	scatter x=appmonth y=max_BehavescoreV2 /markerattrs=(size=8 symbol=circlefilled color=green);

	where rank=1; *compress(appmonth) >= compress("&oneyearago") and compress(appmonth) <= compress("&appmonth");
	yaxis grid values=(0 to 1000 by 100) label='Volume';
run;


proc sgplot data=cs_checks_&appmonth;
	vbar appmonth / response=volume_TuSeg3 datalabel;
	vbar appmonth / response=volume_TuSeg5 datalabel;
	vbar appmonth / response=volume_TuSeg2 datalabel;
	vbar appmonth / response=volume_TuSeg4 datalabel;
	vbar appmonth / response=volume_TuSeg1 datalabel;

	where rank=1;* compress(appmonth) >= compress("&oneyearago") and compress(appmonth) <= compress("&appmonth");
	title 'CS applicationbase volumes per comp_seg';
	yaxis label='Volume';
run;
/*proc print data=summary;*/
/*var appmonth volume_TuSeg1 volume_TuSeg2 volume_TuSeg3 volume_TuSeg4 volume_TuSeg5;*/
/*run;*/

proc sgplot data=cs_checks_&appmonth;
	vbar appmonth / response=V655_2_non_null legendlabel= 'V655_2';
	vbar appmonth / response=V655_2_zero legendlabel= 'V655_2 = 0' datalabel;
	where rank=1;* compress(appmonth) >= compress("&oneyearago") and compress(appmonth) <= compress("&appmonth");
	title "V655_2 equal to 0 ";
	yaxis label='Volume';
run;
proc sgplot data=cs_checks_&appmonth;
	vbar appmonth / response=V655_2_non_null legendlabel= 'V655_2';
	vbar appmonth / response=V655_2_one legendlabel= 'V655_2 = 1' datalabel;
	where rank=1;* compress(appmonth) >= compress("&oneyearago") and compress(appmonth) <= compress("&appmonth");
	title "V655_2 equal to 1 ";
	yaxis label='Volume';
run;



proc sgplot data=cs_checks_&appmonth;

	vbar appmonth / response=comp_thin0 datalabel;
	vbar appmonth / response=comp_thin1 datalabel;
	vbar appmonth / response=comp_thinNull  ;


	where rank=1;* compress(appmonth) >= compress("&oneyearago") and compress(appmonth) <= compress("&appmonth");
	title 'Volumes per comp_thin';
	yaxis label='Volume';
run;



proc sgplot data=cs_checks_&appmonth;
	vbar appmonth / response=CS_V570_prob_non_null legendlabel= 'CS_V570_prob_not_null';
	vbar appmonth / response=CS_V570_prob_null legendlabel= 'CS_V570_prob_null' datalabel;
	where rank=1;* compress(appmonth) >= compress("&oneyearago") and compress(appmonth) <= compress("&appmonth");
	title "Volumes Over time CS_V570_prob";
	yaxis label='Volume';
run;


proc sgplot data= cs_checks_&appmonth;
	vbar appmonth / response= vol_CustomerAge group=CST_CustomerAge stat=sum missing datalabel;
	where rankcstAge=1;* compress(month) >= compress("&oneyearago") and compress(month) <= compress("&month") and rankTypeCode=1;
	Title "Volume Overtime per cstAge group";
	yaxis label='Volume';
run;

proc sgplot data= cs_checks_&appmonth;
	vbar appmonth / response= vol_INSTITUTIONCODE group=INSTITUTIONCODE stat=sum missing datalabel;
	where rankINSTITUTIONCODE=1;* compress(month) >= compress("&oneyearago") and compress(month) <= compress("&month") and rankTypeCode=1;
	Title "Volume Overtime per INSTITUTIONCODE";
	yaxis label='Volume';
run;

Title 'Compuscan Variables WOE analysis';
%macro segvars();
	%do i = 1 %to 5;
		libname seg&i "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V570 Compuscan\Segment&i.\IV";
		proc sql noprint; 
		select cats(Parameter,'null') into :segment&i separated by ' '
		from seg&i..parameter_estimate where
		upcase(Parameter) <> 'INTERCEPT';
		quit;
		
		data summary2;
			set cs_checks_&appmonth;
			keep appmonth &&segment&i ;
		run;
		proc sort data=summary2 nodup; by appmonth;run;
		proc print data=summary2;
			var appmonth &&segment&i ;
			Title "Percentage missing in Segment&i _W variables";
		run;
	%end;
%mend;
%segvars();

Title ;

ods pdf close;