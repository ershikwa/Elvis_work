%let dset = TU_applicationbase_&month;

data _null_;
	call symput("oneyearago", input(put(intnx("month", today(),-12,'B'),yymmn6.),$6.));
	call symput("appmonth", put(intnx("month", today(),-1,'end'),yymmn6.));
	call symput("appmonth2m", put(intnx("month", today(),-2,'end'),yymmn6.));


	call symput("oneyearago1", cats("'",put(intnx('month',today(),-12,'B'),yymmddd10.),"'"));
	call symput("appmonth1", cats("'",put(intnx('month',today(),-1,'E'),yymmddd10.),"'"));
	call symput("appmonth2", cats("'",put(intnx('month',today(),-2,'E'),yymmddd10.),"'"));

run;

proc sql stimer;
connect to odbc(dsn=MPWAPS);
create table tu_appbase_checks as
select * from connection to odbc( 
	select replace(substring(applicationdate,1,7),'-','') as appmonth,
	sum(case when TU_seg = 1 then 1 else 0 end) as volume_TuSeg1,
	sum(case when TU_seg = 2 then 1 else 0 end) as volume_TuSeg2,
	sum(case when TU_seg = 3 then 1 else 0 end) as volume_TuSeg3,
	sum(case when TU_seg = 4 then 1 else 0 end) as volume_TuSeg4,
	sum(case when TU_seg = 5 then 1 else 0 end) as volume_TuSeg5,
	min(V655_2) as min_V655_2, max(V655_2) as max_V655_2, avg(V655_2) as average_V655_2,
	stdev(V655_2) as std_V655_2, min(V655_2_finalriskscore) as min_V655_2_finalscore,
	max(V655_2_finalriskscore) as max_V655_2_finalscore, avg(V655_2_finalriskscore) as average_V655_2_finalscore,
	stdev(V655_2_finalriskscore) as std_V655_2_finalscore, count(V655_2) as V655_2_non_null, 
	sum(case when V655_2 is null then 1 else 0 end)/count(*) as Perc_V655_2_null,
	sum(case when V655_2 <= 0 then 1 else 0 end) as V655_2_zero,
	sum(case when V655_2 >= 1 then 1 else 0 end) as V655_2_one,
	sum(case when V655_2 = V645 then 1 else 0 end) as V655_2eqV645,
	count(V655_2_finalriskscore) as V655_2_finalriskscore_non_null, 
	sum(case when V655_2_finalriskscore is null then 1 else 0 end) as V655_2_finalriskscore_null,
	count(V655_2_RiskGroup) as V655_2_RiskGroup_non_null, 
	sum(case when V655_2_RiskGroup is null then 1 else 0 end) as V655_2_RiskGroup_null,
	count(TU_V580_prob) as TU_V580_prob_non_null,
	sum(case when TU_V580_prob is null then 1 else 0 end) as TU_V580_prob_null,
	sum(case when TU_thin = 1 then 1 else 0 end) as TU_thin1,
	sum(case when TU_thin = 0 then 1 else 0 end) as TU_thin0,
	sum(case when TU_thin is null then 1 else 0 end) as TU_thinNull,
	sum(case when Thinfileindicator = 1 then 1 else 0 end) as Thinfileindicator1,
	sum(case when Thinfileindicator = 0 then 1 else 0 end) as Thinfileindicator0,
	sum(case when Thinfileindicator is null then 1 else 0 end) as ThinfileindicatorNull,
	sum(case when LoanID is null then 1 else 0 end)/count(*) as Perc_LoanID_null,
	sum(case when NationalID is null then 1 else 0 end)/count(*) as Perc_NationalID_null,
	sum(case when UniqueID is null then 1 else 0 end)/count(*) as Perc_UniqueID_null,
	sum(case when baseloanID is null then 1 else 0 end)/count(*) as Perc_baseloanID_null,
	count(EMPLOYERSUBGROUPCODE) as EMPLOYERSUBGROUPCODE_notnull, 
	sum(case when EMPLOYERSUBGROUPCODE is null then 1 else 0 end) as EMPLOYERSUBGROUPCODE_null,
	sum(case when WAGEFREQUENCYCODE is null then 1 else 0 end) as WAGEFREQUENCYCODEnull,
	sum(case when WAGEFREQUENCYCODE = 'WAG001' then 1 else 0 end) as WAGEFREQUENCYCODE001,
	sum(case when WAGEFREQUENCYCODE = 'WAG002' then 1 else 0 end) as WAGEFREQUENCYCODE002,
	sum(case when WAGEFREQUENCYCODE = 'WAG003' then 1 else 0 end) as WAGEFREQUENCYCODE003,

	sum(case when PERSALINDICATOR is null then 1 else 0 end) as PERSALINDICATORnull,
	sum(case when PERSALINDICATOR = 1 then 1 else 0 end) as PERSALINDICATOR1,
	sum(case when PERSALINDICATOR = 0 then 1 else 0 end) as PERSALINDICATOR0,

	sum(case when Repeat is null then 1 else 0 end) as Repeatnull,
	sum(case when Repeat = 1 then 1 else 0 end) as Repeat1,
	sum(case when Repeat = 0 then 1 else 0 end) as Repeat0,
	/*tu vars */
	sum(case when MonthsAtCurrentEmployer_W is null then 1 else 0 end)/count(*) as MonthsAtCurrentEmployer_Wnull ,
	sum(case when DM0001AL_W is null then 1 else 0 end)/count(*) as DM0001AL_Wnull ,
	sum(case when BehavescoreV2_W is null then 1 else 0 end)/count(*) as BehavescoreV2_Wnull ,
	sum(case when GrossIncomeAdjusted_W is null then 1 else 0 end)/count(*) as GrossIncomeAdjusted_Wnull ,
	sum(case when EQ2012AL_W is null then 1 else 0 end)/count(*) as EQ2012AL_Wnull ,
	sum(case when INSTITUTIONCODE_W is null then 1 else 0 end)/count(*) as INSTITUTIONCODE_Wnull ,
	sum(case when PP173Adj_W is null then 1 else 0 end)/count(*) as PP173Adj_Wnull ,
	sum(case when PP0801AL_GI_RATIO_W is null then 1 else 0 end)/count(*) as PP0801AL_GI_RATIO_Wnull ,
	sum(case when PP0406AL_W is null then 1 else 0 end)/count(*) as PP0406AL_Wnull ,
	sum(case when PP0327AL_W is null then 1 else 0 end)/count(*) as PP0327AL_Wnull ,
	sum(case when PP0601AL_CU_RATIO_3_W is null then 1 else 0 end)/count(*) as PP0601AL_CU_RATIO_3_Wnull ,
	sum(case when PP0325AL_W is null then 1 else 0 end)/count(*) as PP0325AL_Wnull ,
	sum(case when PP173_W is null then 1 else 0 end)/count(*) as PP173_Wnull ,
	sum(case when PP0407AL_W is null then 1 else 0 end)/count(*) as PP0407AL_Wnull ,
	sum(case when PP0714AL_GI_RATIO_W is null then 1 else 0 end)/count(*) as PP0714AL_GI_RATIO_Wnull ,
	sum(case when PP0601AL_CU_RATIO_6_W is null then 1 else 0 end)/count(*) as PP0601AL_CU_RATIO_6_Wnull ,
	sum(case when EQ0015PL_W is null then 1 else 0 end)/count(*) as EQ0015PL_Wnull ,
	sum(case when PP0503AL_3_RATIO_12_W is null then 1 else 0 end)/count(*) as PP0503AL_3_RATIO_12_Wnull ,
	sum(case when PP149_W is null then 1 else 0 end)/count(*) as PP149_Wnull ,
	sum(case when PP0313LN_W is null then 1 else 0 end)/count(*) as PP0313LN_Wnull ,
	sum(case when PP0051CL_W is null then 1 else 0 end)/count(*) as PP0051CL_Wnull ,
	sum(case when PP0935AL_W is null then 1 else 0 end)/count(*) as PP0935AL_Wnull ,
	sum(case when PP0901AL_W is null then 1 else 0 end)/count(*) as PP0901AL_Wnull ,
	sum(case when PP0171CL_W is null then 1 else 0 end)/count(*) as PP0171CL_Wnull ,
	sum(case when PP0325AL_W is null then 1 else 0 end)/count(*) as PP0325AL_Wnull ,
	sum(case when PP0111LB_W is null then 1 else 0 end)/count(*) as PP0111LB_Wnull 

	from DEV_DataDistillery_General.dbo.&dset
	where applicationdate <= &appmonth1
	group by substring(applicationdate,1,7) 
/*	order by substring(applicationdate,1,7) desc*/
	);
	disconnect from odbc;
quit;

	
proc sql stimer;
	connect to odbc(dsn=MPWAPS);
	
		create table TypeCode as 
		select * from connection to odbc(
		select replace(substring(applicationdate,1,7),'-','') as appmonth,
		(case when TypeCode is null then '(blank)' else TypeCode end) as TypeCode,
		count(case when TypeCode is null then 1 else 2 end) as vol_TypeCode
		from DEV_DataDistillery_General.dbo.&dset
		where applicationdate <= &appmonth1
		group by substring(applicationdate,1,7), TypeCode
		);


		create table cstAge as 
		select * from connection to odbc(
		select a.appmonth, a.DM0001AL ,
			(case when a.DM0001AL = '<=30' then sum(case when a.DM0001AL = '<=30' then 1 else 0 end)
				when a.DM0001AL ='<=40' then sum(case when a.DM0001AL= '<=40' then 1 else 0 end)
				when a.DM0001AL ='<=50' then sum(case when a.DM0001AL= '<=50' then 1 else 0 end)
				when a.DM0001AL ='>50' then sum(case when a.DM0001AL='>50' then 1 else 0 end) 
				end) as Vol_DM0001AL
		from (select replace(substring(applicationdate,1,7),'-','') as appmonth,
					(case when DM0001AL <= 30 then '<=30' 
						when DM0001AL <= 40 then '<=40'
						when DM0001AL <= 50 then '<=50'
						else '>50' end) as DM0001AL
					from DEV_DataDistillery_General.dbo.&dset
					where applicationdate <= &appmonth1) a
		group by a.appmonth, a.DM0001AL
		);

		create table EMPLOYERSUBGROUPCODE as 
		select * from connection to odbc(
			select b.* from (
				select a.*, row_number() over (partition by a.appmonth order by a.Vol_EMPLOYERSUBGROUPCODE desc) as rank
				from(
					select replace(substring(applicationdate,1,7),'-','') as appmonth,
					(case when EMPLOYERSUBGROUPCODE is null then '(blank)' else EMPLOYERSUBGROUPCODE end) as EMPLOYERSUBGROUPCODE ,
					count(case when EMPLOYERSUBGROUPCODE is null then 1 else 2 end) as Vol_EMPLOYERSUBGROUPCODE
					from DEV_DataDistillery_General.dbo.&dset
					where applicationdate <= &appmonth1
					group by substring(applicationdate,1,7) , EMPLOYERSUBGROUPCODE
				) a ) b
			where b.rank <= 5
		);

	disconnect from odbc;
quit;


proc sql; 
	create table var_Checks as
		select distinct a.appmonth, a.DM0001AL as cstAge , a.Vol_DM0001AL as vol_cstAge,
		b.EMPLOYERSUBGROUPCODE, b.vol_EMPLOYERSUBGROUPCODE, c.TypeCode, c.vol_TypeCode
		from cstAge a left join EMPLOYERSUBGROUPCODE b
		on a.appmonth=b.appmonth
		left join TypeCode c
		on a.appmonth=c.appmonth;
quit;

proc sql;
	create table appbase_all as
		select a.*, b.cstAge , b.vol_cstAge, b.EMPLOYERSUBGROUPCODE, b.vol_EMPLOYERSUBGROUPCODE, b.TypeCode, b.vol_TypeCode
		from tu_appbase_checks a left join var_Checks b
		on a.appmonth=b.appmonth
		order by appmonth;
quit;

 
proc sort data=appbase_all;
by appmonth cstAge;
run;
data cstAge;
set appbase_all;
by appmonth cstAge;
if first.appmonth then rank=0;
rank+1;

if first.cstAge then rankcstAge=0;
rankcstAge+1;
run;


proc sort data=cstAge;
by appmonth EMPLOYERSUBGROUPCODE;
run;
data EMPLOYERSUBGROUPCODE;
set cstAge;
by appmonth EMPLOYERSUBGROUPCODE;

if first.EMPLOYERSUBGROUPCODE then rankEMPLOYERSUBGROUPCODE=0;
rankEMPLOYERSUBGROUPCODE+1;

run;

proc sort data=EMPLOYERSUBGROUPCODE;
by appmonth TypeCode ;
run;
data TU_checks;
set EMPLOYERSUBGROUPCODE;
by appmonth TypeCode;

if first.TypeCode then rankTypeCode=0;
rankTypeCode+1;

run;


proc sql stimer;
	connect to odbc(dsn=MPWAPS);
	create table tu_checks_past as
	select * from connection to odbc(
		select *
		from DEV_DataDistillery_General.dbo.tu_checks_&appmonth2m
		where appmonth >= &oneyearago and appmonth <= &appmonth2m
	);
	disconnect from odbc;
quit;

data tu_checks_&appmonth;
	set tu_checks_past tu_checks;
	where appmonth >= "&oneyearago" and appmonth <= "&appmonth";
run;
proc sort data=tu_checks_&appmonth;
by appmonth;
run;

/*
proc sql; connect to odbc (dsn=MPWAPS);
execute (
		IF OBJECT_ID('DEV_DataDistillery_General.dbo.tu_checks_&appmonth2m', 'U') IS NOT NULL 
		DROP TABLE DEV_DataDistillery_General.dbo.tu_checks_&appmonth2m
		) by odbc;
quit;

%Upload_APS(Set =tu_checks_&appmonth , Server =Work, APS_ODBC = DEV_DDGe, APS_DB = DEV_DataDistillery_General , distribute = HASH([appmonth]));
*/

options orientation=portrait;
ods pdf file="\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard Monthly Monitoring\base_tables_reports\tu_basechecks_&month..pdf" style=seaside startpage=never;
ods graphics / width= 5in height=5in;
data summary;
	set tu_checks_&appmonth;
	keep appmonth Perc_V655_2_null Perc_LoanID_null Perc_NationalID_null Perc_UniqueID_null;
/*	volume_TuSeg1 volume_TuSeg2 volume_TuSeg3 volume_TuSeg4 volume_TuSeg5;*/
/*	where compress(appmonth) >= compress("&oneyearago") and compress(appmonth) <= compress("&appmonth");*/
run;
proc sort data=summary nodup; by descending appmonth;run;
Title "Percentage missing summary for TU_applicationbase";
proc print data= summary;
var appmonth Perc_V655_2_null Perc_LoanID_null Perc_NationalID_null Perc_UniqueID_null;
run;
proc sgplot data= tu_checks_&appmonth;
	series x=appmonth y=average_V655_2;
	Title 'Average V655_2 over time';
	yaxis label= "probability";
	yaxis grid values=(0 to 1 by 0.1);
	where rank=1;* compress(appmonth) >= compress("&oneyearago") and compress(appmonth) <= compress("&appmonth") and rank=1;
run;

Title 'V655_2 final Risk Score Distribution';
proc sgplot data= tu_checks_&appmonth;
	scatter x=appmonth y=min_V655_2_finalscore /markerattrs=(size=8 symbol=circlefilled color=crimson);
	scatter x=appmonth y=average_V655_2_finalscore /markerattrs=(size=8 symbol=circlefilled color=orange);
	scatter x=appmonth y=max_V655_2_finalscore /markerattrs=(size=8 symbol=circlefilled color=green);

	where rank=1; *compress(appmonth) >= compress("&oneyearago") and compress(appmonth) <= compress("&appmonth");
	yaxis grid values=(0 to 1000 by 100) label='Volume';
run;

proc sgplot data=tu_checks_&appmonth;
	vbar appmonth / response=volume_TuSeg3 datalabel;
	vbar appmonth / response=volume_TuSeg5 datalabel;
	vbar appmonth / response=volume_TuSeg2 datalabel;
	vbar appmonth / response=volume_TuSeg4 datalabel;
	vbar appmonth / response=volume_TuSeg1 datalabel;

	where rank=1;* compress(appmonth) >= compress("&oneyearago") and compress(appmonth) <= compress("&appmonth");
	title 'TU applicationbase volumes per TU_seg';
	yaxis label='Volume';
run;
/*proc print data=summary;*/
/*var appmonth volume_TuSeg1 volume_TuSeg2 volume_TuSeg3 volume_TuSeg4 volume_TuSeg5;*/
/*run;*/

proc sgplot data=tu_checks_&appmonth;
	vbar appmonth / response=V655_2_non_null legendlabel= 'V655_2';
	vbar appmonth / response=V655_2_zero legendlabel= 'V655_2 = 0' datalabel;
	where rank=1;* compress(appmonth) >= compress("&oneyearago") and compress(appmonth) <= compress("&appmonth");
	title "V655_2 equal to 0 ";
	yaxis label='Volume';
run;
proc sgplot data=tu_checks_&appmonth;
	vbar appmonth / response=V655_2_non_null legendlabel= 'V655_2';
	vbar appmonth / response=V655_2_one legendlabel= 'V655_2 = 1' datalabel;
	where rank=1;* compress(appmonth) >= compress("&oneyearago") and compress(appmonth) <= compress("&appmonth");
	title "V655_2 equal to 1 ";
	yaxis label='Volume';
run;


proc sgplot data=tu_checks_&appmonth;
/*	vbar appmonth / response=ThinfileindicatorNull ;*/
/*	vbar appmonth / response=Thinfileindicator0 ;*/
/*	vbar appmonth / response=Thinfileindicator1 ;*/

	vbar appmonth / response=TU_thin0 datalabel;
	vbar appmonth / response=TU_thin1 datalabel;
	vbar appmonth / response=TU_thinNull  ;


	where rank=1;* compress(appmonth) >= compress("&oneyearago") and compress(appmonth) <= compress("&appmonth");
	title 'Volumes per TU_thin';
	yaxis label='Volume';
run;



proc sgplot data=tu_checks_&appmonth;
	vbar appmonth / response=TU_V580_prob_non_null legendlabel= 'TU_V580_prob_not_null';
	vbar appmonth / response=TU_V580_prob_null legendlabel= 'TU_V580_prob_null' datalabel;
	where rank=1;* compress(appmonth) >= compress("&oneyearago") and compress(appmonth) <= compress("&appmonth");
	title "Volumes Over time TU_V580_prob";
	yaxis label='Volume';
run;

/*%if &dset. = cs %then %do;*/
/*proc sgplot data=&dset._appbase_checks;*/
/*	vbar appmonth / response=CS_V570_prob_non_null legendlabel= 'CS_V570_prob_non_null';*/
/*	vbar appmonth / response=CS_V570_prob_null legendlabel= 'CS_V570_prob_null';*/
/*	where compress(appmonth) >= compress("&oneyearago") and compress(appmonth) <= compress("&appmonth");*/
/*	title "Volumes Over time where CS_V570_prob is null and not null";*/
/*	yaxis label='Volume';*/
/*run;*/
/*%end;*/

proc sgplot data=tu_checks_&appmonth;
	vbar appmonth / response=WAGEFREQUENCYCODE001 datalabel;
	vbar appmonth / response=WAGEFREQUENCYCODE002 datalabel;
	vbar appmonth / response=WAGEFREQUENCYCODE003 datalabel;
	vbar appmonth / response=WAGEFREQUENCYCODEnull ;

	where rank=1;* compress(appmonth) >= compress("&oneyearago") and compress(appmonth) <= compress("&appmonth");
	title 'Volumes per WAGEFREQUENCYCODE';
	yaxis label='Volume';
run;

proc sgplot data=tu_checks_&appmonth;
	vbar appmonth / response=PERSALINDICATOR0 datalabel;
	vbar appmonth / response=PERSALINDICATOR1 datalabel;
	vbar appmonth / response=PERSALINDICATORnull ;

	where rank=1;* compress(appmonth) >= compress("&oneyearago") and compress(appmonth) <= compress("&appmonth");
	title 'Volumes per PERSALINDICATOR';
	yaxis label='Volume';
run;

proc sgplot data=tu_checks_&appmonth;
	vbar appmonth / response=Repeat1 datalabel;
	vbar appmonth / response=Repeat0 datalabel;
	vbar appmonth / response=Repeatnull ;
	
	where rank=1;* compress(appmonth) >= compress("&oneyearago") and compress(appmonth) <= compress("&appmonth");
	title 'Volumes per Repeat';
	yaxis label='Volume';
run;

proc sgplot data=tu_checks_&appmonth;
	vbar appmonth / response=EMPLOYERSUBGROUPCODE_notnull legendlabel= 'EMPLOYERSUBGROUPCODE_non_null';
	vbar appmonth / response=EMPLOYERSUBGROUPCODE_null legendlabel= 'EMPLOYERSUBGROUPCODE_null' datalabel;
	where rank=1;* compress(month) >= compress("&oneyearago") and compress(month) <= compress("&month") and rank=1;
	title "Volume Over time EMPLOYERSUBGROUPCODE";
	yaxis label='Volume';
run;
proc sgplot data= tu_checks_&appmonth;
	vbar appmonth / response= Vol_EMPLOYERSUBGROUPCODE group=EMPLOYERSUBGROUPCODE stat=sum missing datalabel;
	where rankEMPLOYERSUBGROUPCODE = 1;* compress(month) >= compress("&oneyearago") and compress(month) <= compress("&month") and rankEMPLOYERSUBGROUPCODE = 1;
	Title "Top 5 EMPLOYERSUBGROUPCODE per month";
	yaxis label='Volume';
run;

proc sgplot data= tu_checks_&appmonth;
	vbar appmonth / response= Vol_TypeCode group=TypeCode stat=sum missing datalabel;
	where rankTypeCode=1;* compress(month) >= compress("&oneyearago") and compress(month) <= compress("&month") and rankTypeCode=1;
	Title "Volume Overtime per TypeCode";
	yaxis label='Volume';
run;

proc sgplot data= tu_checks_&appmonth;
	vbar appmonth / response= vol_cstAge group=cstAge stat=sum missing datalabel;
	where rankcstAge=1;* compress(month) >= compress("&oneyearago") and compress(month) <= compress("&month") and rankTypeCode=1;
	Title "Volume Overtime per cstAge group";
	yaxis label='Volume';
run;

Title 'TU Variables WOE analysis';
%macro segvars();
	%do i = 1 %to 5;
		libname seg&i "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU\Segment&i.\IV\";
		proc sql noprint; 
		select cats(Parameter,'null') into :segment&i separated by ' '
		from seg&i.._estimates_ where
		upcase(Parameter) <> 'INTERCEPT';
		quit;
		
		data summary2;
			set tu_checks_&appmonth;
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

ods pdf close;

Title ;