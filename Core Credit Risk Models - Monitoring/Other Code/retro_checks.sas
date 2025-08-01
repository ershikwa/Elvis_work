
%let retro_dset = PRD_DataDistillery_Data.dbo.TC_v6_Retro_disb_&month;
data _null_;
	call symput("oneyearago", input(put(intnx("month", today(),-12,'B'),yymmn6.),$6.));
	call symput("m2", put(intnx("month", today(),-2,'end'),yymmn6.));
	call symput("m", put(intnx("month", today(),-1,'end'),yymmn6.));

	call symput("oneyearago1", cats("'",input(put(intnx("month", today(),-12,'B'),yymmn6.),$6.),"'"));
	call symput("month1", cats("'",put(intnx("month", today(),-1,'end'),yymmn6.),"'"));

run;
%put &m;

proc sql stimer;
connect to odbc(dsn=MPWAPS); 
create table retro_appbase_checks as
select * from connection to odbc( 
	select month, count(*) as volume_per_month, min(V655_2) as min_V655_2, max(V655_2) as max_V655_2,
	avg(V655_2) as average_V655_2, stdev(V655_2) as std_v655_2, min(V655_2_finalscore) as min_V655_2_finalscore,
	max(V655_2_finalscore) as max_V655_2_finalscore, avg(V655_2_finalscore) as average_V655_2_finalscore,
	stdev(V655_2_finalscore) as std_V655_2_finalscore, count(V655_2) as V655_2_notnull, 
	sum(case when V655_2 is null then 1 else 0 end) as V655_2_null, 
	sum(case when V655_2 is null then 1 else 0 end)/count(*) as Perc_V655_2_null,
	sum(case when V655_2 <= 0.00000000 and V655_2 is not null then 1 else 0 end) as V655_2_zero,
	sum(case when V655_2 >= 1.00000000 then 1 else 0 end) as V655_2_one,
	sum(case when V655_2 = V645 then 1 else 0 end) as V655_2eqV645,
	count(LoanID) as LoanID_notnull, 
	sum(case when LoanID is null then 1 else 0 end) as LoanID_null,
	sum(case when LoanID is null then 1 else 0 end)/count(*) as Perc_LoanID_null,
	count(V655_2_finalscore) as V655_2_finalscore_notnull, 
	sum(case when V655_2_finalscore is null then 1 else 0 end) as V655_2_finalscore_null,
	count(V655_2_RG) as V655_2_RG_notnull, 
	sum(case when V655_2_RG is null then 1 else 0 end) as V655_2_RG_null,
	count(INSTITUTIONCODE) as INSTITUTIONCODE_notnull, 
	sum(case when INSTITUTIONCODE is null then 1 else 0 end) as INSTITUTIONCODE_null,
	count(EMPLOYERSUBGROUPCODE) as EMPLOYERSUBGROUPCODE_notnull, 
	sum(case when EMPLOYERSUBGROUPCODE is null then 1 else 0 end) as EMPLOYERSUBGROUPCODE_null,
	count(comb_thin) as comb_thin_notnull,
	sum(case when comb_thin is null then 1 else 0 end) as combthin_null,
	count(Product) as Product_notnull,
	sum(case when Product is null then 1 else 0 end) as Product_null,
	count(TypeCode) as TypeCode_notnull,
	sum(case when TypeCode is null then 1 else 0 end) as TypeCode_null
	from &retro_dset
	where month >= &oneyearago and month <= &m
	group by month );
/*	order by month desc); */
disconnect from odbc;
quit;
	
proc sql stimer;
	connect to odbc(dsn=MPWAPS);
	
	create table comb_thin as
	select * from connection to odbc(
		select month, (case when comb_thin is null then '(blank)' else comb_thin end) as comb_thin,
		count(case when comb_thin is null then 1 else 2 end) as vol_combthin
		from &retro_dset
		where month >= &oneyearago and month <= &m
		group by month, comb_thin );
/*		order by month); */

	create table product as 
		select * from connection to odbc(
		select month, (case when product is null then '(blank)' else product end) as product,
		count(case when product is null then 1 else 2 end) as vol_product
		from &retro_dset
		where month >= &oneyearago and month <= &m
		group by month, product );
/*		order by month); */
		
		create table INSTITUTIONCODE as 
		select * from connection to odbc(
		select month, (case when INSTITUTIONCODE is null then '(blank)' else INSTITUTIONCODE end) as INSTITUTIONCODE,
		count(case when INSTITUTIONCODE is null then 1 else 2 end) as vol_INSTITUTIONCODE
		from &retro_dset
		where month >= &oneyearago and month <= &m
		group by month, INSTITUTIONCODE );
/*		order by month ); */
		
		create table TypeCode as 
		select * from connection to odbc(
		select month, (case when TypeCode is null then '(blank)' else TypeCode end) as TypeCode,
		count(case when TypeCode is null then 1 else 2 end) as vol_TypeCode
		from &retro_dset
		where month >= &oneyearago and month <= &m
		group by month, TypeCode);
/*		order by month); */

		create table EMPLOYERSUBGROUPCODE as 
		select * from connection to odbc(
			select b.* from (
				select a.*, row_number() over (partition by month order by Vol_EMPLOYERSUBGROUPCODE desc) as rank
				from(
					select month, (case when EMPLOYERSUBGROUPCODE is null then '(blank)' else EMPLOYERSUBGROUPCODE end) as EMPLOYERSUBGROUPCODE ,
					count(case when EMPLOYERSUBGROUPCODE is null then 1 else 2 end) as Vol_EMPLOYERSUBGROUPCODE
					from &retro_dset
					where month >= &oneyearago and month <= &m
					group by month , EMPLOYERSUBGROUPCODE
				) a ) b
			where b.rank <= 5 ) ;
/*			order by b.month desc, b.rank );	*/
		
	disconnect from odbc;
quit;

proc sql; 
	create table Retro_var_Checks as
		select distinct a.month, a.comb_thin, a.vol_combthin, b.product, b.vol_product,
			c.EMPLOYERSUBGROUPCODE, c.vol_EMPLOYERSUBGROUPCODE,  
			d.TypeCode, d.vol_TypeCode,
			e.INSTITUTIONCODE, e.vol_INSTITUTIONCODE
		from comb_thin a left join product b
		on a.month=b.month
		left join EMPLOYERSUBGROUPCODE c
		on a.month=c.month 
		left join TypeCode d
		on a.month=d.month
		left join INSTITUTIONCODE e
		on a.month=e.month;
quit;

proc sql;
	create table retro_all as
		select a.*, b.*
		from retro_appbase_checks a left join Retro_var_Checks b
		on a.month=b.month;
quit;


proc sort data=retro_all;
by month comb_thin;
run;
data comb_thin;
set retro_all;
by month comb_thin;
if first.month then rank=0;
rank+1;

if first.comb_thin then rankcomb_thin=0;
rankcomb_thin+1;
run;


proc sort data=comb_thin;
by month product;
run;
data product;
set comb_thin;
by month product;

if first.product then rankproduct=0;
rankproduct+1;

run;

proc sort data=product;
by month EMPLOYERSUBGROUPCODE;
run;
data EMPLOYERSUBGROUPCODE;
set product;
by month EMPLOYERSUBGROUPCODE;

if first.EMPLOYERSUBGROUPCODE then rankEMPLOYERSUBGROUPCODE=0;
rankEMPLOYERSUBGROUPCODE+1;

run;

proc sort data=EMPLOYERSUBGROUPCODE;
by month TypeCode;
run;
data TypeCode;
set EMPLOYERSUBGROUPCODE;
by month TypeCode;

if first.TypeCode then rankTypeCode=0;
rankTypeCode+1;

run;

proc sort data=TypeCode;
by month INSTITUTIONCODE;
run;
data INSTITUTIONCODE;
set TypeCode;
by month INSTITUTIONCODE;

if first.INSTITUTIONCODE then rankINSTITUTIONCODE=0;
rankINSTITUTIONCODE+1;

run;

proc sort data=INSTITUTIONCODE;
by month TypeCode;
run;
data retro_checks ;  
set INSTITUTIONCODE;
by month TypeCode;

if first.TypeCode then rankTypeCode=0;
rankTypeCode+1;
where month = "&m";
run;


proc sql stimer;
	connect to odbc(dsn=MPWAPS);
	create table retro_checks_past as
	select * from connection to odbc(
		select * from PRD_DataDistillery_Data.dbo.retro_checks_&m2
		where month >= &oneyearago and month <= &m2 );
	disconnect from odbc;
quit;

data retro_checks_&m;
	set retro_checks  retro_checks_past ;
	where month >= "&oneyearago" and month <= "&m" ;
run;

proc sort data= retro_checks_&m;
by month;
run;

/*
proc sql; connect to odbc (dsn=MPWAPS);
execute (
		IF OBJECT_ID('PRD_DataDistillery_data.dbo.retro_checks_&m2', 'U') IS NOT NULL 
		DROP TABLE PRD_DataDistillery_data.dbo.retro_checks_&m2;
		) by odbc;
quit;
*/

%Upload_APS(Set =retro_checks_&m , Server =Work, APS_ODBC = PRD_DDDa, APS_DB = PRD_DataDistillery_Data , distribute = HASH([month]));


options orientation=portrait;
ods pdf file="\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard Monthly Monitoring\base_tables_reports\retro_base_&m..pdf" style=seaside startpage=never;
ods graphics / width= 5in height=5in;
data summary;
	set retro_checks_&m ;
	keep month Perc_V655_2_null Perc_LoanID_null;
/*	where compress(month) >= compress("&oneyearago") and compress(month) <= compress("&month");*/
run;
proc sort data=summary nodup; by descending month;run;
Title "Percentage missing summary in &month :";
proc print data= summary;
run;
proc sgplot data= retro_checks_&m ;
	series x=month y=average_V655_2;
	Title 'Average V655_2 over time';
	yaxis label= "probability";
	yaxis grid values=(0 to 1 by 0.1);
	where rank=1;* compress(month) >= compress("&oneyearago") and compress(month) <= compress("&month") and ;
run;

Title 'V655 final Risk Score Distribution';
proc sgplot data= retro_checks_&m ;
	scatter x=month y=min_V655_2_finalscore /markerattrs=(size=8 symbol=circlefilled color=crimson);
	scatter x=month y=average_V655_2_finalscore /markerattrs=(size=8 symbol=circlefilled color=orange);
	scatter x=month y=max_V655_2_finalscore /markerattrs=(size=8 symbol=circlefilled color=green);

	where rank=1;* compress(month) >= compress("&oneyearago") and compress(month) <= compress("&month") and rank=1;
	yaxis grid values=(0 to 1000 by 100) label='Volume';
run;

proc sgplot data= retro_checks_&m ;
	vbar month / response=volume_per_month;
	where rank=1;* compress(month) >= compress("&oneyearago") and compress(month) <= compress("&month") and rank=1;
	title 'Retro Table Volumes Per Month';
	yaxis label='Volume';
run;



proc sql;
 create table V655_2_0 as
 	select distinct month, V655_2_notnull,V655_2_zero
 	from retro_checks_&m;
quit;

proc transpose data = V655_2_0 name=seg label=seg out= outdata;
by month;
run;

data V655_2_0;
	set outdata;
	label seg = 'segment';
run;

proc sgplot data= V655_2_0 pctlevel=group;
	vbar month/response=COL1 group=seg stat=pct missing datalabel;
	title 'V655_2 equal to 0 ';
	yaxis label='Volume';
run;

proc sql;
 create table V655_2_1 as
 	select distinct month, V655_2_notnull,V655_2_zero
 	from retro_checks_&m;
quit;

proc transpose data = V655_2_1 name=seg label=seg out= outdata;
by month;
run;

data V655_2_1;
	set outdata;
	label seg = 'segment';
run;

proc sgplot data= V655_2_1 pctlevel=group;
	vbar month/response=COL1 group=seg stat=pct missing datalabel;
	title 'V655_2 equal to 1';
	yaxis label='Volume';
run;



proc sgplot data= retro_checks_&m pctlevel=group;
	vbar month / response= Vol_INSTITUTIONCODE group=INSTITUTIONCODE stat=pct missing datalabel;
	where rankINSTITUTIONCODE=1;* compress(month) >= compress("&oneyearago") and compress(month) <= compress("&month") and rankINSTITUTIONCODE=1;
	Title "Volume Overtime per INSTITUTIONCODE";
	yaxis label='Volume';
run;


proc sql;
 create table EMPLOY as
 	select distinct month, EMPLOYERSUBGROUPCODE_notnull,EMPLOYERSUBGROUPCODE_null
 	from retro_checks_&m;
quit;

proc transpose data = EMPLOY name=seg label=seg out= outdata;
by month;
run;

data EMPLOY;
	set outdata;
	label seg = 'segment';
run;

proc sgplot data= EMPLOY pctlevel=group;
	vbar month/response=COL1 group=seg stat=pct missing datalabel;
	title 'Volume Over time where EMPLOYERSUBGROUPCODE is null and not null';
	yaxis label='Volume';
run;

proc sgplot data= retro_checks_&m pctlevel=group;
	vbar month / response= Vol_EMPLOYERSUBGROUPCODE group=EMPLOYERSUBGROUPCODE stat=pct missing datalabel;
	where rankEMPLOYERSUBGROUPCODE = 1;* compress(month) >= compress("&oneyearago") and compress(month) <= compress("&month") and rankEMPLOYERSUBGROUPCODE = 1;
	Title "Volume for top 5 EMPLOYERSUBGROUPCODE per month";
	yaxis label='Volume';
run; 

proc sgplot data= retro_checks_&m pctlevel=group;
	vbar month / response= Vol_combthin group=comb_thin stat=pct missing datalabel;
	where rankcomb_thin=1;* compress(month) >= compress("&oneyearago") and compress(month) <= compress("&month") and rankcomb_thin=1;
	Title "Volume of thin and non-thin files per month";
	yaxis label='Volume';
run;


proc sgplot data= retro_checks_&m pctlevel=group;
	vbar month / response= Vol_Product group=Product stat=pct missing datalabel;
	where rankProduct=1;* compress(month) >= compress("&oneyearago") and compress(month) <= compress("&month") and rankProduct=1;
	Title "Volume Overtime per Product";
	yaxis label='Volume';
run;


proc sgplot data= retro_checks_&m pctlevel=group;
	vbar month / response= Vol_TypeCode group=TypeCode stat=pct missing datalabel;
	where rankTypeCode=1;* compress(month) >= compress("&oneyearago") and compress(month) <= compress("&month") and rankTypeCode=1;
	Title "Volume Overtime per TypeCode";
	yaxis label='Volume';
run;


Title ;
ods pdf close;




