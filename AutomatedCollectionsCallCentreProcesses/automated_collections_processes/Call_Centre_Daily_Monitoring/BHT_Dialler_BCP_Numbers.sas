%include "H:\Process_Automation\sas_autoexec\sas_autoexec.sas";

%let projectcode =H:\Process_Automation\Codes;

%let runmonth = %scan(&sysparm,1);
%let process_number = %scan(&sysparm,2);
%let process =  %scan(&sysparm,3);

%let project =pj;

libname &project "&process";

%start_program;

options mprint symbolgen mlogic;

data _null_;
     call symput("Last2Months", put(intnx("month", today(),-2),yymmn6.));
     call symput("LastMonth", put(intnx("month", today(),-1),yymmn6.));
	 call symput("ThisMonth", put(intnx("month", today(),0),yymmn6.));
     call symput("YesterdaysMonth", put(intnx("day", today(),-1),yymmn6.));
	 call symput("TomorrowsMonth", put(intnx("day", today(),1),yymmn6.));
     call symput("Yesterday", put(intnx("day", today(),-1),yymmddn8.));
     call symput("Today", put(intnx("day", today(),0),yymmddn8.));
     call symput("Tomorrow", put(intnx("day", today(),1),yymmddn8.));
run;

%put &Last2Months.;
%put &LastMonth.;
%put &ThisMonth.;
%put &YesterdaysMonth.;
%put &Yesterday.;
%put &Today.;
%put &Tomorrow.;


proc sql stimer;
     connect to ODBC (dsn = MPWAPS);
     create table ScoringTables as
	     select * from connection to odbc 
		 (
			Use PRD_Collections_Strategy
				SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES order by TABLE_NAME
	      );
	     disconnect from odbc;
quit;

options mprint mlogic symbolgen;

%macro GetLatestColl_ActvRate();
	proc sql;
		create table Actvtemp as 
		select TABLE_NAME from ScoringTables where TABLE_NAME like "Coll_ActvRate_&LastMonth.";
	quit;
	proc sql;
		create table Actvtemp2 as 
		select TABLE_NAME from ScoringTables where TABLE_NAME like "Coll_ActvRate_&Last2Months.";
	quit;

	proc sql;
		select count(*) into: Actvcount1 from Actvtemp;
	quit;
	
	%if (&Actvcount1. = 1) %then %do;  
		%put The table Coll_ActvRate_&LastMonth. exists. ;
		%global LatestColActvRate;
		%let LatestColActvRate = Coll_ActvRate_&LastMonth.;
	%end; 
	%else %do;
		%put The table Coll_ActvRate_&LastMonth. does not exists, table Coll_ActvRate_&Last2Months will be used;
		%global LatestColActvRate;
		%let LatestColActvRate = Coll_ActvRate_&Last2Months;
	%end;
%mend;

%macro GetLatestColData();
	proc sql;
		create table Coltemp as 
		select TABLE_NAME from ScoringTables where upcase(TABLE_NAME) like "COLDATA_&LastMonth.";
	quit;

	proc sql;
		create table Coltemp2 as 
		select TABLE_NAME from ScoringTables where upcase(TABLE_NAME) like "COLDATA_&Last2Months.";
	quit;

	proc sql;
		select count(*) into: Colcount1 from Coltemp;
	quit;

	%if (&Colcount1. = 1) %then %do;  
		%put The table Coldata_&LastMonth. exists. ;
		%global LatestColData;
		%let LatestColData = Coldata_&LastMonth.;
	%end; 
	%else %do;
		%put The table Coldata_&LastMonth. does not exists, table Coldata_&Last2Months. will be used;
		%global LatestColData;
		%let LatestColData = Coldata_&Last2Months.;
	%end;
%mend;


%macro GetLatestProb2Honour();
	proc sql;
		create table Probtemp as 
		select TABLE_NAME from ScoringTables where upcase(TABLE_NAME) like "KJM_PROB2HONOUR_SCORED_&LastMonth.";
	quit;

	proc sql;
		create table Probtemp2 as 
		select TABLE_NAME from ScoringTables where upcase(TABLE_NAME) like "KJM_PROB2HONOUR_SCORED_&Last2Months.";
	quit;

	proc sql;
		select count(*) into: Probcount1 from Probtemp;
	quit;

	%if (&Probcount1. = 1) %then %do;  
		%put The table KJM_PROB2HONOUR_SCORED_&LastMonth. exists. ;
		%global LatestProbData;
		%let LatestProbData = KJM_PROB2HONOUR_SCORED_&LastMonth.;
	%end; 
	%else %do;
		%put The table KJM_PROB2HONOUR_SCORED_&LastMonth. does not exists, table KJM_PROB2HONOUR_SCORED_&Last2Months. will be used;
		%global LatestProbData;
		%let LatestProbData = KJM_PROB2HONOUR_SCORED_&Last2Months.;
	%end;
%mend;

%GetLatestColl_ActvRate;
%GetLatestColData;
%GetLatestProb2Honour;

%put &LatestColActvRate;
%put &LatestColData;
%put &LatestProbData;


proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id('Prd_ContactInfo.dbo.BHT_Dialler_1') is not null DROP TABLE Prd_ContactInfo.dbo.BHT_Dialler_1;
		create table Prd_ContactInfo.dbo.BHT_Dialler_1
		with (distribution = hash(ClientNo), clustered columnstore index)
		as
		select a.ClientNo, a.IDNo, a.Loanref, a.ReasonCode, a.HomeBranch, a.EvenInstalment, a.Instalment_Orig, a.Balance, a.AvailableBalance, a.PrincipalDebt, a.LastReceiptDate, a.SalaryDay, a.RandomInd
				, a.RunMonth, a.LoanGroup, a.Bank
				, C.Final_Prob2Actv, b.Probability
		from Prd_ContactInfo.dbo.&LatestColData.							a
		/*
		left join PRD_Collections_Strategy.dbo.&LatestProbData.						b
		on a.loanref = b.loanref
		left join PRD_Collections_Strategy.dbo.&LatestColActvRate.					c
		on a.loanref = c.loanref
	)
	by APS;
quit;


proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id('Prd_ContactInfo.dbo.BHT_Dialler_1_DeD') is not null DROP TABLE Scoring.dbo.BHT_Dialler_1_DeD;
		create table Prd_ContactInfo.dbo.BHT_Dialler_1_DeD
		with(distribution = hash(ClientNo), clustered columnstore index)
		as
		select 	ClientNo,IDNo,Loanref,ReasonCode,HomeBranch,EvenInstalment,Instalment_Orig
				,Balance,AvailableBalance,PrincipalDebt,LastReceiptDate,SalaryDay,RandomInd,RunMonth,LoanGroup
				,Bank,Final_Prob2Actv,Probability
		from 	Prd_ContactInfo.dbo.BHT_Dialler_1
		group by 
					ClientNo,IDNo,Loanref,ReasonCode,HomeBranch,EvenInstalment,Instalment_Orig,Balance,AvailableBalance
					,PrincipalDebt,LastReceiptDate,SalaryDay,RandomInd,RunMonth,LoanGroup,Bank,Final_Prob2Actv,Probability
	)
	by APS;
quit;


proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id('Prd_ContactInfo.dbo.BHT_Dialler_P2A_Twentile') is not null DROP TABLE Prd_ContactInfo.dbo.BHT_Dialler_P2A_Twentile;
		create table Prd_ContactInfo.dbo.BHT_Dialler_P2A_Twentile
		with(distribution = hash(ClientNo), clustered columnstore index)
		as
		select *, ntile(10) over (order by Final_Prob2Actv)	P2A_Twentile
		from Prd_ContactInfo.dbo.BHT_Dialler_1_Ded
	)
	by APS;
quit;


proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id('Prd_ContactInfo.dbo.BHT_BCP_Decile') is not null DROP TABLE Prd_ContactInfo.dbo.BHT_BCP_Decile;				
		create table Prd_ContactInfo.dbo.BHT_BCP_Decile
		with(distribution = hash(IDNumber), clustered columnstore index)
		as
		select *, ntile(10) over (order by Score)	BCP_Decile
		from Prd_ContactInfo.dbo.BHT_BCP_Score
		where score is not null
	)
	by APS;
quit;

/*--BCP_Decile	Clients		Min_Score	Max_Score		Avg_Score
--1				125453		0.000000	0.899510		0.298419
--2				125453		0.899510	2.308721		1.501097
--3				125453		2.308751	7.383458		4.170097
--4				125453		7.383489	42.182122		19.744763
--5				125453		42.182200	117.450398		80.367327
--6				125453		117.451467	182.614232		147.530013
--7				125453		182.614833	298.408934		236.026202
--8				125453		298.408974	497.172666		386.513797
--9				125452		497.172678	935.973779		682.494820
--10			125452		935.981636	52706.048742	1741.769800

----Use the Min score for decile 10 ensure their inclusion in every cycle.*/;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id('Prd_ContactInfo.dbo.BHT_Dialler_1_ImputeP2H') is not null DROP TABLE Prd_ContactInfo.dbo.BHT_Dialler_1_ImputeP2H;				
		create table Prd_ContactInfo.dbo.BHT_Dialler_1_ImputeP2H
		with(distribution = hash(ClientNo), clustered columnstore index)
		as
		select *, case when P2A_Twentile = 1 then 0.06863145
					   when P2A_Twentile = 2 then 0.07083160
					   when P2A_Twentile = 3 then 0.07393448
					   when P2A_Twentile = 4 then 0.07524551
					   when P2A_Twentile = 5 then 0.11637667
					   when P2A_Twentile = 6 then 0.13450550
					   when P2A_Twentile = 7 then 0.138
					   when P2A_Twentile = 8 then 0.21155885
					   when P2A_Twentile = 9 then 0.31117363
					   when P2A_Twentile = 10 then 0.38131496
					   when P2A_Twentile = 11 then 0.46138714
					   when P2A_Twentile = 12 then 0.52372859
					   when P2A_Twentile = 13 then 0.55008580
					   when P2A_Twentile = 14 then 0.57697525
					   when P2A_Twentile = 15 then 0.64673098
					   when P2A_Twentile = 16 then 0.65429427
					   when P2A_Twentile = 17 then 0.66326689
					   when P2A_Twentile = 18 then 0.67359209
					   when P2A_Twentile = 19 then 0.68
					   when P2A_Twentile = 20 then 0.69
					   end as Probability_2
		from Prd_ContactInfo.dbo.BHT_Dialler_P2A_Twentile
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id('Scoring.dbo.BHT_Dialler_2') is not null DROP TABLE Prd_ContactInfo.dbo.BHT_Dialler_2;				
		create table Prd_ContactInfo.dbo.BHT_Dialler_2
		with (distribution = hash(ClientNo), clustered columnstore index)
		as
		select *,Probability_2 * EvenInstalment						Expectation_2
		from Prd_ContactInfo.dbo.BHT_Dialler_1_ImputeP2H	
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id('Prd_ContactInfo.dbo.BHT_Dialler_2_Agg') is not null DROP TABLE Prd_ContactInfo.dbo.BHT_Dialler_2_Agg;						
		create table Prd_ContactInfo.dbo.BHT_Dialler_2_Agg
		with (distribution = hash(ClientNo), clustered columnstore index)
		as
		select ClientNo	, IDNo as IDNumber
				, sum(Expectation_2)		Expectation
		from Prd_ContactInfo.dbo.BHT_Dialler_2
		group by ClientNo,IDNo 	
	)
	by APS;
quit;


/*-BCP Score included below. Previous calcs are not driving the inclusion metrics currently.*/
proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id('Prd_ContactInfo.dbo.BHT_Dialler_3') is not null DROP TABLE Prd_ContactInfo.dbo.BHT_Dialler_3;						
		declare @Max_Value float 
		set @Max_Value = 935.981636
		declare @Min_Value float 
		set @Min_Value = 0
		create table Prd_ContactInfo.dbo.BHT_Dialler_3
		with (distribution = hash(ClientNo), clustered columnstore index)
		as
		select a.*, ((Expectation - @Min_Value) / (@Max_Value - @Min_Value))		Scaled_Expectation_Org
				, ((b.Score - @Min_Value) / (@Max_Value - @Min_Value))				Scaled_Expectation
				,c.highest_score
		from Prd_ContactInfo.dbo.BHT_Dialler_2_Agg			a
		left join Prd_ContactInfo.dbo.BHT_BCP_Score			b
		on a.IDNumber = b.IDNumber
		left join Prd_ContactInfo.dbo.BHT_BNM_EQ_scored_MT		c
		on a.idnumber =  c.idnumber
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id('Prd_ContactInfo.dbo.BHT_Dialler_4') is not null DROP TABLE Prd_ContactInfo.dbo.BHT_Dialler_4;						
		declare @Recycle_Factor float 
		set @Recycle_Factor = 0.1
		create table Prd_ContactInfo.dbo.BHT_Dialler_4
		with (distribution = hash(ClientNo), clustered columnstore index)
		as
		select *, case when Scaled_Expectation < 0.20	then 0.20
				when Scaled_Expectation > 0.90	then 0.90
				else Scaled_Expectation
				end as Clipped_Expectation
				,case when Scaled_Expectation / @Recycle_Factor < 0.001 then 0.001 else Scaled_Expectation / @Recycle_Factor	end as		Inclusion_Rate
		from Prd_ContactInfo.dbo.BHT_Dialler_3
	)
	by APS;
quit;

/*proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		update Prd_ContactInfo.dbo.BHT_Dialler_4
		set Inclusion_Rate = 1
		where Inclusion_Rate > 1
	)
	by APS;
quit;*/

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id('Prd_ContactInfo.dbo.BHT_Dialler_5') is not null DROP TABLE Prd_ContactInfo.dbo.BHT_Dialler_5;						
		declare @Drop_Off_1 float 
		set @Drop_Off_1 = 1.16
		declare @Drop_Off_2 float 
		set @Drop_Off_2 = 1.1
		declare @Drop_Off_3 float 
		set @Drop_Off_3 = 1.22
		declare @Drop_Off_4 float 
		set @Drop_Off_4 = 1.4
		declare @Drop_Off_5 float 
		set @Drop_Off_5 = 1.6
		declare @Drop_Off_6 float 
		set @Drop_Off_6 = 6.22
		declare @Drop_Off_7 float 
		set @Drop_Off_7 = 2.04
		declare @Drop_Off_8 float 
		set @Drop_Off_8 = 2.40
		declare @Drop_Off_9 float 
		set @Drop_Off_9 = 3.2
		declare @Drop_Off_10 float 
		set @Drop_Off_10 = 2.6
		declare @Drop_Off_11 float 
		set @Drop_Off_11 = 3.0
		declare @Drop_Off_12 float 
		set @Drop_Off_12 = 3.0

		create table Prd_ContactInfo.dbo.BHT_Dialler_5
		with (distribution = hash(ClientNo), clustered columnstore index)
		as
		select		 *
			,case when @Drop_Off_1 * Inclusion_Rate > rand(convert(varbinary, newid()))		then 1 else 0 end as	Cycle_1_Inclusion
			,case when @Drop_Off_1 * @Drop_Off_2 * Inclusion_Rate > rand(convert(varbinary, newid()))		then 1 else 0 end as	Cycle_2_Inclusion
			,case when @Drop_Off_1 * @Drop_Off_2 * @Drop_Off_3 * Inclusion_Rate > rand(convert(varbinary, newid()))		then 1 else 0 end as	Cycle_3_Inclusion
			,case when @Drop_Off_1 * @Drop_Off_2 * @Drop_Off_3 * @Drop_Off_4 *  Inclusion_Rate > rand(convert(varbinary, newid()))		then 1 else 0 end as	Cycle_4_Inclusion
			,case when @Drop_Off_1 * @Drop_Off_2 * @Drop_Off_3 * @Drop_Off_4 * @Drop_Off_5 * Inclusion_Rate > rand(convert(varbinary, newid()))		then 1 else 0 end as	Cycle_5_Inclusion		
			,case when @Drop_Off_1 * @Drop_Off_2 * @Drop_Off_3 * @Drop_Off_4 * @Drop_Off_5 * @Drop_Off_6 * Inclusion_Rate * highest_score > rand(convert(varbinary, newid()))		then 1 else 0 end as	Cycle_6_Inclusion
			,case when @Drop_Off_1 * @Drop_Off_2 * @Drop_Off_3 * @Drop_Off_4 * @Drop_Off_5 * @Drop_Off_6 * @Drop_Off_7 * Inclusion_Rate * highest_score > rand(convert(varbinary, newid()))		then 1 else 0 end as	Cycle_7_Inclusion
			,case when @Drop_Off_1 * @Drop_Off_2 * @Drop_Off_3 * @Drop_Off_4 * @Drop_Off_5 * @Drop_Off_6 * @Drop_Off_7 * @Drop_Off_8 * Inclusion_Rate * highest_score > rand(convert(varbinary, newid()))		then 1 else 0 end as	Cycle_8_Inclusion
			,case when @Drop_Off_1 * @Drop_Off_2 * @Drop_Off_3 * @Drop_Off_4 * @Drop_Off_5 * @Drop_Off_6 * @Drop_Off_7 * @Drop_Off_8 * @Drop_Off_9 * Inclusion_Rate * highest_score > rand(convert(varbinary, newid()))		then 1 else 0 end as	Cycle_9_Inclusion
			,case when @Drop_Off_1 * @Drop_Off_2 * @Drop_Off_3 * @Drop_Off_4 * @Drop_Off_5 * @Drop_Off_6 * @Drop_Off_7 * @Drop_Off_8 * @Drop_Off_9 * @Drop_Off_10 * Inclusion_Rate * highest_score  > rand(convert(varbinary, newid()))		then 1 else 0 end as	Cycle_10_Inclusion
			,case when @Drop_Off_1 * @Drop_Off_2 * @Drop_Off_3 * @Drop_Off_4 * @Drop_Off_5 * @Drop_Off_6 * @Drop_Off_7 * @Drop_Off_8 * @Drop_Off_9 * @Drop_Off_10 * @Drop_Off_11 * Inclusion_Rate * highest_score  > rand(convert(varbinary, newid()))		then 1 else 0 end as	Cycle_11_Inclusion
			,case when @Drop_Off_1 * @Drop_Off_2 * @Drop_Off_3 * @Drop_Off_4 * @Drop_Off_5 * @Drop_Off_6 * @Drop_Off_7 * @Drop_Off_8 * @Drop_Off_9 * @Drop_Off_10 * @Drop_Off_11 * @Drop_Off_12 * Inclusion_Rate * highest_score   > rand(convert(varbinary, newid()))		then 1 else 0 end as	Cycle_12_Inclusion
		from Prd_ContactInfo.dbo.BHT_Dialler_4
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id('Prd_ContactInfo.dbo.BHT_Dialler_6') is not null DROP TABLE Prd_ContactInfo.dbo.BHT_Dialler_6;						
		create table Prd_ContactInfo.dbo.BHT_Dialler_6
		with (distribution = hash(ClientNo), clustered columnstore index)
		as
		select *, rand(convert(varbinary, newid()))		Cycle_1_Rand
		, rand(convert(varbinary, newid()))				Cycle_2_Rand
		, rand(convert(varbinary, newid()))				Cycle_3_Rand
		, rand(convert(varbinary, newid()))				Cycle_4_Rand
		, rand(convert(varbinary, newid()))				Cycle_5_Rand
		, rand(convert(varbinary, newid()))				Cycle_6_Rand
		, rand(convert(varbinary, newid()))				Cycle_7_Rand
		, rand(convert(varbinary, newid()))				Cycle_8_Rand
		, rand(convert(varbinary, newid()))				Cycle_9_Rand
		, rand(convert(varbinary, newid()))				Cycle_10_Rand
		, rand(convert(varbinary, newid()))				Cycle_11_Rand
		, rand(convert(varbinary, newid()))				Cycle_12_Rand
		from Prd_ContactInfo.dbo.BHT_Dialler_5
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id('Prd_ContactInfo.dbo.BHT_Dialler_7') is not null DROP TABLE Prd_ContactInfo.dbo.BHT_Dialler_7;						
		declare @Number_1_Intensity float = 0.85
		declare @Number_2_Intensity float = 0.97
		declare @Number_3_Intensity float = 1.00
		create table Prd_ContactInfo.dbo.BHT_Dialler_7
		with (distribution = hash(ClientNo), clustered columnstore index)
		as
		select *, case when Cycle_1_Inclusion = 1	then	(case when Cycle_1_Rand < @Number_1_Intensity	then 1
																when Cycle_1_Rand < @Number_2_Intensity		then 2
																when Cycle_1_Rand < @Number_3_Intensity		then 3
																else											 4		end)
					else 0		end as		Cycle_1_Number
				, case when Cycle_2_Inclusion = 1	then	(case when Cycle_2_Rand < @Number_1_Intensity	then 1
																when Cycle_2_Rand < @Number_2_Intensity		then 2
																when Cycle_2_Rand < @Number_3_Intensity		then 3
																else											 4		end)
					else 0		end as		Cycle_2_Number
				, case when Cycle_3_Inclusion = 1	then	(case when Cycle_3_Rand < @Number_1_Intensity	then 1
																when Cycle_3_Rand < @Number_2_Intensity		then 2
																when Cycle_3_Rand < @Number_3_Intensity		then 3
																else											 4		end)
					else 0		end as		Cycle_3_Number
				, case when Cycle_4_Inclusion = 1	then	(case when Cycle_4_Rand < @Number_1_Intensity	then 1
																when Cycle_4_Rand < @Number_2_Intensity		then 2
																when Cycle_4_Rand < @Number_3_Intensity		then 3
																else											 4		end)
					else 0		end as		Cycle_4_Number
				, case when Cycle_5_Inclusion = 1	then	(case when Cycle_5_Rand < @Number_1_Intensity	then 1
																when Cycle_5_Rand < @Number_2_Intensity		then 2
																when Cycle_5_Rand < @Number_3_Intensity		then 3
																else											 4		end)
					else 0		end as		Cycle_5_Number
				, case when Cycle_6_Inclusion = 1	then	(case when Cycle_6_Rand < @Number_1_Intensity	then 1
																when Cycle_6_Rand < @Number_2_Intensity		then 2
																when Cycle_6_Rand < @Number_3_Intensity		then 3
																else											 4		end)
					else 0		end as		Cycle_6_Number
				, case when Cycle_7_Inclusion = 1	then	(case when Cycle_7_Rand < @Number_1_Intensity	then 1
																when Cycle_7_Rand < @Number_2_Intensity		then 2
																when Cycle_7_Rand < @Number_3_Intensity		then 3
																else											 4		end)
					else 0		end as		Cycle_7_Number
				, case when Cycle_8_Inclusion = 1	then	(case when Cycle_8_Rand < @Number_1_Intensity	then 1
																when Cycle_8_Rand < @Number_2_Intensity		then 2
																when Cycle_8_Rand < @Number_3_Intensity		then 3
																else											 4		end)
					else 0		end as		Cycle_8_Number
				, case when Cycle_9_Inclusion = 1	then	(case when Cycle_9_Rand < @Number_1_Intensity	then 1
																when Cycle_9_Rand < @Number_2_Intensity		then 2
																when Cycle_9_Rand < @Number_3_Intensity		then 3
																else											 4		end)
					else 0		end as		Cycle_9_Number
				, case when Cycle_10_Inclusion = 1	then	(case when Cycle_10_Rand < @Number_1_Intensity	then 1
																when Cycle_10_Rand < @Number_2_Intensity	then 2
																when Cycle_10_Rand < @Number_3_Intensity	then 3
																else											 4		end)
					else 0		end as		Cycle_10_Number
				, case when Cycle_11_Inclusion = 1	then	(case when Cycle_11_Rand < @Number_1_Intensity	then 1
																when Cycle_11_Rand < @Number_2_Intensity	then 2
																when Cycle_11_Rand < @Number_3_Intensity	then 3
																else											 4		end)
					else 0		end as		Cycle_11_Number
				, case when Cycle_12_Inclusion = 1	then	(case when Cycle_12_Rand < @Number_1_Intensity	then 1
																when Cycle_12_Rand < @Number_2_Intensity	then 2
																when Cycle_12_Rand < @Number_3_Intensity	then 3
																else											 4		end)
					else 0		end as		Cycle_12_Number
		from Prd_ContactInfo.dbo.BHT_Dialler_6
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id('Prd_ContactInfo.[UNIZA\BTuckerAdmin].BHT_Dialler_BCP_Numbers') is not null DROP TABLE Prd_ContactInfo.[UNIZA\BTuckerAdmin].BHT_Dialler_BCP_Numbers;						
		create table Prd_ContactInfo.[UNIZA\BTuckerAdmin].BHT_Dialler_BCP_Numbers
		with(distribution = hash(ClientNo), clustered columnstore index)
		as
		select ClientNo, IDNumber
				,Cycle_1_Number
				,Cycle_2_Number
				,Cycle_3_Number
				,Cycle_4_Number
				,Cycle_5_Number
				,Cycle_6_Number
				,Cycle_7_Number
				,Cycle_8_Number
				,Cycle_9_Number
				,Cycle_10_Number
				,Cycle_11_Number
				,Cycle_12_Number
		from Prd_ContactInfo.dbo.BHT_Dialler_7
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		update Prd_ContactInfo.[UNIZA\BTuckerAdmin].BHT_Dialler_BCP_Numbers
		set Cycle_12_Number = 1
	)
	by APS;
quit;

options compress = yes;

proc sql stimer;
     connect to ODBC (dsn = MPWAPS);
     create table Temp1 as
     select * from connection to odbc 
	 (	
	    select  *  
		from Prd_ContactInfo.[UNIZA\BTuckerAdmin].BHT_Dialler_BCP_Numbers
      );
     disconnect from odbc;
quit;


data _null_;
     call symput('strategyDate',put(intnx('day',today(),1),yymmddn8.));
	 call symput('runDate',put(intnx('day',today(),0),yymmddn8.));
run;

%put &strategyDate;
%put &runDate;

data temp2;
	set temp1;
	StrategyDate = &strategyDate;
	RunDate = &runDate;
	RunTime = put(time(),time20.);
run;

%include "H:\Process_Automation\macros\createdirectory.sas";

%createdirectory(directory=H:\Call_Centre\&TomorrowsMonth.);

libname kat "H:\Call_Centre\&TomorrowsMonth";

proc append 
	base = kat.BHT_Dialler_BCP_Num_Hist_&TomorrowsMonth.
	data = temp2 force ;       
run; 

data BHT_Dialler_BCP_Num_Hist_&TomorrowsMonth.;
	set kat.BHT_Dialler_BCP_Num_Hist_&TomorrowsMonth.;
run;

/* Final table Cycle Results */
libname sphebht "H:\Call_Centre\CycleResults";
/*proc sql;
	create table sphebht.CycleResults_&Today. as 
	select count(distinct(case when Cycle_1_Number > 0 then clientno else 0 end)) -1 "Cycle_1_Distinct_Clients"
,count(distinct(case when Cycle_2_Number > 0 then clientno else 0 end)) -1 "Cycle_2_Distinct_Clients"
,count(distinct(case when Cycle_3_Number > 0 then clientno else 0 end)) -1 "Cycle_3_Distinct_Clients"
,count(distinct(case when Cycle_4_Number > 0 then clientno else 0 end)) -1 "Cycle_4_Distinct_Clients"
,count(distinct(case when Cycle_5_Number > 0 then clientno else 0 end)) -1 "Cycle_5_Distinct_Clients"
,count(distinct(case when Cycle_6_Number > 0 then clientno else 0 end)) -1 "Cycle_6_Distinct_Clients"
,count(distinct(case when Cycle_7_Number > 0 then clientno else 0 end)) -1 "Cycle_7_Distinct_Clients"
,count(distinct(case when Cycle_8_Number > 0 then clientno else 0 end)) -1 "Cycle_8_Distinct_Clients"
,count(distinct(case when Cycle_9_Number > 0 then clientno else 0 end)) -1 "Cycle_9_Distinct_Clients"
,count(distinct(case when Cycle_10_Number > 0 then clientno else 0 end)) -1 "Cycle_10_Distinct_Clients"
,count(distinct(case when Cycle_11_Number > 0 then clientno else 0 end)) -1 "Cycle_11_Distinct_Clients"
,count(distinct(case when Cycle_12_Number > 0 then clientno else 0 end)) -1 "Cycle_12_Distinct_Clients"
from BHT_Dialler_BCP_Num_Hist_&TomorrowsMonth.
where rundate = (select max(rundate) from BHT_Dialler_BCP_Num_Hist_&TomorrowsMonth.);
quit;*/

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id('Prd_ContactInfo.dbo.BHT_Dialler_BCP_Num_Hist_&TomorrowsMonth.') is not null DROP TABLE Prd_ContactInfo.dbo.BHT_Dialler_BCP_Num_Hist_&TomorrowsMonth.;						
	)
	by APS;
quit;

%Upload_APS(Set = BHT_Dialler_BCP_Num_Hist_&TomorrowsMonth., Server = work, APS_ODBC = Prd_ContactInfo, APS_DB = Prd_ContactInfo, Distribute = HASH(RunDate));

%end_program(&process_number);


