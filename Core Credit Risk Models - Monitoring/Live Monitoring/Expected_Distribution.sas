%let todaysDate = %sysfunc(today(), yymmddn8.);
%put &todaysDate;
%let thismonth = %sysfunc(today(), yymmn6.);
%put &thismonth;
data _null_;
	call symput('lstmnth',cats("'",put(intnx('day',today(),-30),yymmddd10.),"'"));
run;
%put &lstmnth;

libname daily "\\Neptune\sasa$\V5\Application Scorecard\V6 Monitoring\Data\&thismonth.\&todaysDate.";
libname Prd_DDDa odbc dsn=Prd_DDDa schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
libname capri odbc dsn=Prd_Pres schema='capri' preserve_tab_names=yes connection=unique direct_sql=yes;


data all_scores_v604;
set daily.testresults;
AppTime = input(ApplicationTime,time5.);
format AppTime time8.;
AppHour = hour(AppTime);
length TypeCodeWithMiss $32;
length IncomeBins $32;
length MonthsBins $32;
length AgeBins $32;
length channelcode_Description $32;
length institution_Description $32;
length typecode_Description $32;

if declinedescription = "" then declinedescription = "UNSPECIFIED";

TypeCodeWithMiss = old_TypeCode;
IF TypeCodeWithMiss = 'EML001' then Typecode_Description = 'Full Time';
IF TypeCodeWithMiss = 'EML002' then Typecode_Description = 'Part Time';
IF TypeCodeWithMiss = 'EML003' then Typecode_Description = 'Unemployed';
IF TypeCodeWithMiss = 'EML004' then Typecode_Description = 'Self Emplyed';
IF TypeCodeWithMiss = 'EML005' then Typecode_Description = 'Temporary Employed';
IF TypeCodeWithMiss = 'EML006' then Typecode_Description = 'Contract Worker';
IF TypeCodeWithMiss = 'EML007' then Typecode_Description = 'Labour and Personnel Agents';
IF TypeCodeWithMiss = 'EML008' then Typecode_Description = 'Pure Commission Earners';
IF TypeCodeWithMiss = 'EML009' then Typecode_Description = 'State Pension';
IF TypeCodeWithMiss = 'EML010' then Typecode_Description = 'Seasonal Worker';
IF TypeCodeWithMiss = 'EML011' then Typecode_Description = 'Pensioner';
IF TypeCodeWithMiss = 'EML012' then Typecode_Description = 'Professionals/Entrepreneurs';
IF TypeCodeWithMiss = '' then TypeCodeWithMiss = 'MISS';
IF TypeCodeWithMiss = 'MISS' then Typecode_Description = 'Unspecified';

IF InstitutionCode = 'BNKABL' then institution_Description = 'African Bank';
IF InstitutionCode = 'BNKABS' then institution_Description = 'ABSA';
IF InstitutionCode = 'BNKFNB' then institution_Description = 'FNB';
IF InstitutionCode = 'BNKNED' then institution_Description = 'Nedbank';
IF InstitutionCode = 'BNKDIS' then institution_Description = 'Discovery Bank';
IF InstitutionCode = 'BNKCAP' then institution_Description = 'Capitec';
IF InstitutionCode = 'BNKINV' then institution_Description = 'Investec';
IF InstitutionCode = 'BNKSTD' then institution_Description = 'Standard Bank';
IF InstitutionCode = 'BNKTYM' then institution_Description = 'TymeBank';
IF InstitutionCode = 'BNKOTH' then institution_Description = 'Other';

if ChannelCode = 'CCC001' then channelcode_Description = 'Branch';
if ChannelCode = 'CCC003' then channelcode_Description = 'Virtual Branch';
if ChannelCode = 'CCC007' then channelcode_Description = 'Old Call Centre';
if ChannelCode = 'CCC008' then channelcode_Description = 'Transfer from Call Centre';
if ChannelCode = 'CCC013' then channelcode_Description = 'New Call Centre';
if ChannelCode = 'CCC020' then channelcode_Description = 'Omni Branch';
if ChannelCode = 'CCC021' then channelcode_Description = 'Omni Web';
if ChannelCode = 'CCC026' then channelcode_Description = 'Omni Call Centre';
if ChannelCode = 'CCC028' then channelcode_Description = 'Quick Quote';
run;

proc sql;
drop table Prd_DDDa.testresults;
quit;

proc sql;
	create table Prd_DDDa.testresults as
	select a.*, b.canoverridescoreband, b.overridescoreband
	from all_scores_v604 a
	left join capri.capri_scoring_results b
	on a.uniqueid = b.uniqueid;
run;

proc sql stimer;
connect to ODBC (dsn=MPWAPS);
create table ScoredData3 as
select * from connection to odbc
(
select b.*, a.V635 as V635_score, a.V636 as V636_score, a.*
from (select tranappnumber, uniqueid from PRD_press.capri.capri_loan_application
where (applicationdate >= &lstmnth and TRANSEQUENCE <> '005')) b
inner join PRD_Press.[Capri].[CreditRisk_SegmentProbabilityAdjTUFunction] a
on a.uniqueid = b.uniqueid
order by a.Requestid, b.uniqueid);
disconnect from ODBC;
quit;

proc sql;
create table inv as
select a.*, b.V601, b.V602, b.V603, b.V633 as V633_Score, b.V634 as V634_Score, b.V635_score, b.V636_score
from all_scores_v604 a left join ScoredData3 b
on a.uniqueid = b.uniqueid;
quit;

data all_scores_v604;
set inv;
if round(V6_prob ,0.0001) - round(input(V601,best16.),0.0001) = 0
then V601_Match = 1;
else V601_Match =0;

if round(V6_prob2 ,0.0001) - round(input(V602,best16.),0.0001) = 0
then V602_Match = 1;
else V602_Match =0;

if round(V6_prob3 ,0.0001) - round(input(V603,best16.),0.0001) = 0
then V603_Match = 1;
else V603_Match =0;

if round(V633 ,0.0001) - round(input(V633_score,best16.),0.0001) = 0
then V633_Match = 1;
else V633_Match =0;

if round(V634 ,0.0001) - round(input(V634_score,best16.),0.0001) = 0
then V634_Match = 1;
else V634_Match =0;

if round(V635 ,0.0001) - round(input(V635_score,best16.),0.0001) = 0
then V635_Match = 1;
else V635_Match =0;

if round(V636 ,0.0001) - round(input(V636_score,best16.),0.0001) = 0
then V636_Match = 1;
else V636_Match =0;

if round(input(V6_FinalScore,best16.) ,0.01) - round(V6_finalriskscore,0.01) = 0
then V6_Match = 1;
else V6_Match =0;

if round(input(V6_RiskGroup,best16.) ,0.01) - round(Riskgroup,0.01) = 0
then RG_Match = 1;
else RG_Match =0;
run;

proc sql;
	create table V6_Live_Transactions as (
	select AppHour, round(AVG(count),1) as average_transactions
	from (select AppHour, count(AppHour) as count
			from all_scores_v604
			group by ApplicationDate, AppHour) 
	group by  AppHour);
quit;

proc sql;
	drop table Prd_DDDa.V6_Live_Override;

	create table Prd_DDDa.V6_Live_Override as (
	select AppHour, round(AVG(count),1) as average_transactions,rg
	from (select AppHour, count(AppHour) as count, avg(rg_match) as rg
			from Prd_DDDa.testresults
			where canoverridescoreband = 1
			group by ApplicationDate, AppHour) 
	group by  AppHour);
quit;

proc sql;
	create table V6_Live_Transactions_Declined as (
	select AppHour, round(AVG(count_declined),1) as average_declined
	from (select AppHour, count(*) as count_declined
			from all_scores_v604
			where declinecode ne ""
			group by ApplicationDate, AppHour) 
	group by AppHour);
quit;

proc sql;
	drop table Prd_DDDa.V6_Live_Transactions;

	create table Prd_DDDa.V6_Live_Transactions as (
	select a.AppHour, average_transactions, average_declined, average_declined/average_transactions as average_decline_perc
	from V6_Live_Transactions a
	inner join V6_Live_Transactions_Declined b
	on a.AppHour = b.AppHour);
quit;

proc sql;
drop table Prd_DDDa.V6_Live_RiskGroup;

create table Prd_DDDa.V6_Live_RiskGroup as (
select V6_RiskGroup, round(AVG(count), 1) as average, 0 as Total
from (select ApplicationDate, V6_RiskGroup, count(V6_RiskGroup) as count
		from all_scores_v604
		group by ApplicationDate, V6_RiskGroup)
group by V6_RiskGroup
having count(V6_RiskGroup) > 0);

UPDATE Prd_DDDa.V6_Live_RiskGroup
SET Total = (select sum(average) from Prd_DDDa.V6_Live_RiskGroup);
quit;

proc sql;
	drop table Prd_DDDa.V6_Live_DeclineCodes;

	create table Prd_DDDa.V6_Live_DeclineCodes as (
	select Declinecode, DeclineCode, DeclineDescription, round(AVG(count), 1) as average, 0 as Total
	from(
		select ApplicationDate, Declinecode, DeclineDescription, count(DeclineCode) as count
		from all_scores_v604
		where declinecode <> ""
		group by ApplicationDate, Declinecode, DeclineDescription) a
	group by DeclineCode, DeclineDescription
	having count(a.Declinecode) > 0);

	UPDATE Prd_DDDa.V6_Live_DeclineCodes
	SET Total = (select sum(average) from Prd_DDDa.V6_Live_DeclineCodes);
quit;
proc sql;
	drop table Prd_DDDa.V6_Live_matchraterates;

	create table Prd_DDDa.V6_Live_matchraterates as (
	select b.AppHour, V633_matchrate, V634_matchrate, AVG(V635_matchrate) as V635_matchrate, AVG(V636_matchrate) as V636_matchrate, AVG(rg_matchrate) as rg_matchrate
	from (select AppHour, (sum(V633_match)/count(*)) as V633_matchrate
				from all_scores_v604
				group by AppHour) b
	inner join (select AppHour, (sum(V634_match)/count(*)) as V634_matchrate
				from all_scores_v604
				group by AppHour) c
	on b.AppHOur = c.AppHour
	inner join (select AppHour, (sum(V635_match)/count(*)) as V635_matchrate
				from all_scores_v604
				group by AppHour) d
	on c.AppHour = d.AppHour
	inner join (select AppHour, (sum(V636_match)/count(*)) as V636_matchrate
				from all_scores_v604 
				group by AppHour) e
	on d.AppHour = e.AppHour
	inner join (select AppHour, (sum(rg_match)/count(*)) as rg_matchrate
				from all_scores_v604 
				group by AppHour) f
	on e.AppHour = f.AppHour
	group by b.AppHour); 
quit;

%let missinglist = PERCANDNUMPAYMENTRANK MONTHSATCURRENTEMPLOYER GROSSINCOMEADJUSTED PERSALINDICATOR COMPUSCANVAR3275 COMPUSCANVAR6130 
	CST_CustomerAge COMPUSCANVAR187 COMPUSCANVAR7550 COMPUSCANVAR2696 COMPUSCANVAR3935 adjCOMPUSCANVAR6289 SG_CREATIONYEAR2 COMPUSCANVAR188 COMPUSCANVAR3916 COMPUSCANVAR5579 COMPUSCANVAR716 
	BehaveScore COMPUSCANVAR2312 COMPUSCANVAR2528 COMPUSCANVAR6073 COMPUSCANVAR7479 COMPUSCANVAR7547 INSTITUTIONCODE TYPECODE UNS_RatioMR60DBal1YearAdj BehaveScore COMPUSCANVAR1401 
	COMPUSCANVAR2678 COMPUSCANVAR5208 COMPUSCANVAR5826 COMPUSCANVAR733 COMPUSCANVAR7549 COMPUSCANVAR7683 ALL_RatioOrgBalLim1Year 
	DM0001AL EQ2012AL NG004 PP116 PP149 PP283 PP0001AL PP003_NP003 PP0051CL PP0111LB PP0171CL PP0313LN PP0325AL PP0327AL PP0406AL PP0407AL PP0421CL PP0503AL PP0503AL_3_RATIO_12 PP0505AL PP0515AL PP0515AL_GI_RATIO 
	PP0521LB_GI_RATIO PP0601AL PP0601AL_CU_RATIO_3 PP0601AL_CU_RATIO_6 PP0603AL PP0604AL PP0714AL PP0714AL_GI_RATIO PP0801AL PP0801AL_GI_RATIO PP0901AL PP0935AL 
	PP173 RE006_019 RE006 RE019 NP013 NP015;

proc sort data = all_scores_v604;
	by apphour;
run;

%macro getMissings();
	%do k = 1 %to %sysfunc(countw(&missinglist));
		%let vari = %scan(&missinglist, &k);
		proc freq data=all_scores_v604;
			by apphour;
			tables &vari. /out=temp missing norow nocol nopercent nocum ;
		run;

		proc contents data=temp out=details;
		run;

		proc sql;
			select TYPE into: vartype 
			from details 
			where upcase(NAME) = upcase("&vari.");
		quit;

		proc sql;
			select count(*) into: totalobs 
			from all_scores_v604;
		quit;

		%put &vari.;
		%put &vartype.;

		data temp2;
			set temp;
			Percentage = Percent/100;
			VariableName = "&vari.";
			Character = &vari.;
			%if %eval(&vartype.) = 1 %then %do;
				Num_Character = &vari.;
				where &vari. = .;
			%end;
			%else %if %eval(&vartype.) = 2 %then %do;
				Char_Character = &vari.;
				where &vari. = '';
			%end;
			drop &vari.;
		run;

		proc append base=Missing data=temp2 force;
		run;

		proc sql;
			create table missingtemp as
			select * , sum(count)/%eval(&totalobs.) as TotalDayPerc, &totalobs. as TotalObs
			from Missing
			group by VariableName,Character;
		quit;
	%end;
%mend;
%getMissings;

proc sql ;
	create table missingtemp as (SELECT AppHour, VariableName, AVG(Percentage) as Percentage
	from missingtemp
	group by AppHour, VariableName);

	drop table Prd_DDDa.V6_Live_Missing_Expected; 

	create table  Prd_DDDa.V6_Live_Missing_Expected 
		like missingtemp; 

	insert into Prd_DDDa.V6_Live_Missing_Expected (Bulkload = Yes)
		select *
		from missingtemp;
quit;
