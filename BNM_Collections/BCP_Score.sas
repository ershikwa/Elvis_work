options mprint symbolgen mlogic;

data _null_;
     call symput("Last2Months", put(intnx("month", today(),-2),yymmn6.));
     call symput("LastMonth", put(intnx("month", today(),-1),yymmn6.));
	 call symput("ThisMonth", put(intnx("month", today(),0),yymmn6.));
     call symput("YesterdaysMonth", put(intnx("day", today(),-1),yymmn6.));
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
     create table Prd_CoSt as
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
		select TABLE_NAME from Prd_CoSt where TABLE_NAME like "Coll_ActvRate_&LastMonth.";
	quit;
	proc sql;
		create table Actvtemp2 as 
		select TABLE_NAME from Prd_CoSt where TABLE_NAME like "Coll_ActvRate_&Last2Months.";
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
		select TABLE_NAME from Prd_CoSt where upcase(TABLE_NAME) like "COLDATA_&LastMonth.";
	quit;

	proc sql;
		create table Coltemp2 as 
		select TABLE_NAME from Prd_CoSt where upcase(TABLE_NAME) like "COLDATA_&Last2Months.";
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

%GetLatestColl_ActvRate;
%GetLatestColData;

%put &LatestColActvRate;
%put &LatestColData;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id('Prd_Collections_Strategy.dbo.BHT_Coldata_Source') is not null drop table Prd_Collections_Strategy.dbo.BHT_Coldata_Source;	
		create table Prd_Collections_Strategy.dbo.BHT_Coldata_Source with(distribution = hash(IDNo), clustered columnstore index)
		as select a.IDNo, count(distinct a.loanref) Loan_Count, sum(a.Balance)	Total_Balance
				, sum(a.eveninstalment)							Sum_Instalments
				, sum(b.Final_Prob2Actv * a.eveninstalment)		Sum_Inst_Actv
		from Prd_Collections_Strategy.dbo.&LatestColData.			a
		left join Prd_Collections_Strategy.dbo.&LatestColActvRate.		b
		on a.loanref = b.loanref
		group by a.IDNo
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id('Prd_Collections_Strategy.dbo.BHT_Coldata_Source_ClientNo') is not null drop table Prd_Collections_Strategy.dbo.BHT_Coldata_Source_ClientNo;
		create table Prd_Collections_Strategy.dbo.BHT_Coldata_Source_ClientNo
		with(distribution = hash(IDNo), clustered columnstore index)
		as
		select IDNo, Clientno, count(Loanref)	Loan_count
		from Prd_Collections_Strategy.dbo.&LatestColData.
		group by IDNo, Clientno
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(			
		IF object_id('Prd_ContactInfo.dbo.BHT_BNM_EQ_scored_MT') is not null drop table Prd_ContactInfo.dbo.BHT_BNM_EQ_scored_MT; 	
		create table Prd_ContactInfo.dbo.BHT_BNM_EQ_scored_MT 
		with(distribution = hash(IDnumber), clustered columnstore index)
		as
		select IDnumber, count(distinct number)	Number_Count, max(score)	Highest_Score
		from Prd_ContactInfo.dbo.BNM_ranked_nums_MT
		group by IDnumber
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(		
		IF object_id('Prd_ContactInfo.dbo.BHT_BCP_Score') is not null drop table Prd_ContactInfo.dbo.BHT_BCP_Score;	
		create table Prd_ContactInfo.dbo.BHT_BCP_Score
		with(distribution = hash(IDNumber), clustered columnstore index)
		as
		select a.IDNumber, a.Highest_Score * b.Sum_Inst_Actv	Score
		from Prd_ContactInfo.dbo.BHT_BNM_EQ_scored_MT		a
		left join Prd_Collections_Strategy.dbo.BHT_Coldata_Source				b
		on a.IDnumber = b.IDNo
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(
		IF object_id('Prd_ContactInfo.dbo.BHT_BCP_Score_Transfer') is not null drop table Prd_ContactInfo.dbo.BHT_BCP_Score_Transfer;	
		create table Prd_ContactInfo.dbo.BHT_BCP_Score_Transfer
		with(distribution = hash(clientnumber), clustered columnstore index)
		as
		select b.ClientNo as	clientnumber, a.Score	as	ScoreValue, getdate()	as	UpdateDateTime
		from Prd_ContactInfo.dbo.BHT_BCP_Score										a
		left join Prd_Collections_Strategy.dbo.BHT_Coldata_Source_ClientNo					b
		on a.IDNumber = b.IDNo
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
		from Prd_ContactInfo.dbo.BHT_BCP_Score_Transfer
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

libname kat "H:\BCP_Score\";

data kat.BHT_BCP_Score_Transfer_&strategyDate.;
	set temp2;       
run; 

data BHT_BCP_Score_Transfer_&strategyDate.;
	set kat.BHT_BCP_Score_Transfer_&strategyDate.;
run;

/*filename macros1 '\\neptune\credit$\AA_GROUP CREDIT\Scoring\SAS Macros';
options sasautos = (sasautos  macros1);*/

/*%Upload_APS(Set = BHT_BCP_Score_Transfer_&strategyDate., Server = work, APS_ODBC = Scoring, APS_DB = Scoring, Distribute = HASH(RunDate));*/
%Upload_APS_A(Set = BHT_BCP_Score_Transfer_&strategyDate., Server = work, APS_ODBC = Prd_Cont, APS_DB = Prd_ContactInfo, Distribute = HASH(RunDate));

/*filename macros2 'H:\Process_Automation\macros';
options sasautos = (sasautos  macros2);*/
