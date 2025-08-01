
/***************** Loan Disbursals *****************/
proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Daily_AppBase')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_Daily_AppBase
  END;

CREATE TABLE Creditprofitability.dbo.TS_Daily_AppBase
WITH (DISTRIBUTION = HASH(ApplicationID), CLUSTERED COLUMNSTORE INDEX)
AS

SELECT x.ApplicationID
	  ,x.UniqueID
	  ,x.CreationDate
	  ,x.CreationMonth
	  ,isnull(x.ISFOREIGNERIND, 0) as ISFOREIGNERIND
FROM (
		SELECT a.Tranappnumber AS ApplicationID
			  ,a.UniqueID
			  ,CAST(a.ApplicationDate AS DATETIME) AS CreationDate
			  ,REPLACE(SUBSTRING(LEFT(CAST(a.ApplicationDate AS VARCHAR),7),1,7),'-','') AS CreationMonth
			  ,b.ISFOREIGNERIND
			  ,RANK () OVER (PARTITION BY a.Tranappnumber ORDER BY a.UniqueID DESC) AS Latest
		FROM PRD_Press.capri.capri_loan_application_2021 a 
		     left join
			 PRD_Press.capri.CAPRI_APPLICANT_2021 b
        on a.UniqueID = b.UniqueID
		WHERE DATEDIFF(MONTH, a.ApplicationDate, GETDATE()) < 12
		AND	DATEDIFF(MONTH, a.ApplicationDate, GETDATE()) >= 0
	) AS x

WHERE x.Latest = 1
			) by APS;
quit;




/***************** Loan SRA Disbursals *****************/

proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Daily_LoanSales')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_Daily_LoanSales
  END;

CREATE TABLE Creditprofitability.dbo.TS_Daily_LoanSales
WITH (DISTRIBUTION = HASH(ApplicationID), CLUSTERED COLUMNSTORE INDEX)
AS

SELECT LoanID AS ApplicationID
	  ,Capital
	  ,LastUpdateTimeStamp
	  ,LEFT(LastUpdateTimeStamp,6) AS CreationMonth
	  ,Product = 'Loan'
FROM Prd_ExactusSync.dbo.ZA31200P
WHERE LoanID IN (SELECT ApplicationID FROM Creditprofitability.dbo.TS_Daily_AppBase)

			) by APS;
quit;




/***************** Card Disbursals *****************/
proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
			use Creditprofitability

			if exists (select table_name from information_schema.tables where table_name like 'ELR_Daily_CardSales')
			begin
				drop table Creditprofitability.dbo.ELR_Daily_CardSales
			end;

			CREATE TABLE Creditprofitability.dbo.ELR_Daily_CardSales
			WITH (DISTRIBUTION = HASH(ApplicationID), CLUSTERED COLUMNSTORE INDEX)
			AS

			SELECT LoanID AS ApplicationID
				  ,Capital
				  ,CreationDate
				  ,LEFT(CreationDate,6) AS CreationMonth
				  ,Product = 'Card'
			FROM Creditprofitability.dbo.Card_Pricing_Daily
			WHERE LoanID IN (SELECT ApplicationID FROM Creditprofitability.dbo.ELR_Daily_AppBase)

			) by APS;
quit;




/***************** OD Disbursals *****************/
proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
	USE Creditprofitability
if exists(select * from information_schema.tables where table_name like 'TS_Daily_OverdraftSales_TEMP1')
begin
	drop table CreditProfitability.dbo.TS_Daily_OverdraftSales_TEMP1
end;

create table Creditprofitability.dbo.TS_Daily_OverdraftSales_TEMP1
with (distribution = hash(loanid), clustered columnstore index)
as

select applicationid as Loanid, Clientnumber, CreationDate
from
(
	select distinct ApplicationID, ClientNumber, CreationDate, 
	row_number() over(partition by ApplicationID order by CreationDate desc) rn
	from
	(
		select * from PRD_ExactusSync.dbo.ApplicationsHistory where ApplicationStatus = 'DIS' and ApplicationType in ('CRE', 'CBC')
		and left(convert(varchar, CreationDate, 112), 6) >= left(convert(varchar, dateadd(month, -22, getdate()), 112), 6)
		union 
		select * from PRD_ExactusSync.dbo.Applications where ApplicationStatus = 'DIS' and ApplicationType in ('CRE', 'CBC')
		and left(convert(varchar, CreationDate, 112), 6) >= left(convert(varchar, dateadd(month, -22, getdate()), 112), 6)
	) a
) b where rn = 1
			) by APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
			use Creditprofitability

if exists(select * from information_schema.tables where table_name like 'TS_Daily_OverdraftSales_TEMP2')
begin
	drop table CreditProfitability.dbo.TS_Daily_OverdraftSales_TEMP2
end;

create table CreditProfitability.dbo.TS_Daily_OverdraftSales_TEMP2
with (distribution = hash(applicationid), clustered columnstore index)
as

select		applicationid
			, productcategory
			, description as product_description
			, scoreband
			, channel
			, odoverdraftlimit
			, convert(varchar, lastupdatetimestamp, 112) as lastupdatedtimestamp
from		PRD_ExactusSync.dbo.APPLICATIONOFFERS
where		OFFERSSELECTEDTYPE = 'SEL' 
and			productcategory in (190, 191, 192, 193)
and			left(convert(varchar,LASTUPDATETIMESTAMP,112), 6) >= left(convert(varchar,dateadd(mm,-22,getdate()),112),6)

			) by APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
use Creditprofitability

if exists(select * from information_schema.tables where table_name like 'TS_Daily_OverdraftSales_TEMP3')
begin
	drop table Creditprofitability.dbo.TS_Daily_OverdraftSales_TEMP3
end ;

create table Creditprofitability.dbo.TS_Daily_OverdraftSales_TEMP3
with (distribution = hash(loanid), clustered columnstore index)
as 

select distinct *
from
(
select		a.Loanid, a.clientnumber, coalesce(d.Reference, c.Accountreference) as AccountReference, b.*

from		Creditprofitability.dbo.TS_Daily_OverdraftSales_TEMP1			a				

inner join	Creditprofitability.dbo.TS_Daily_OverdraftSales_TEMP2	b	
on a.loanid = b.applicationid

inner join	PRD_ExactusSync.dbo.ApplicationAccount					c	
on a.loanid = c.applicationid

inner join	PRD_ExactusSync.dbo.OC00000P							d	
on a.loanid = d.applicationid 

) z

			) by APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
			use Creditprofitability

if exists(select * from information_schema.tables where table_name like 'TS_Daily_OverdraftSales')
begin
	drop table CreditProfitability.dbo.TS_Daily_OverdraftSales
end;

create table CreditProfitability.dbo.TS_Daily_OverdraftSales
with (distribution = hash(Applicationid), clustered columnstore index)
as

select		  id.Loanid													as Applicationid
			, main.MyWorldAccountNumber									as AccountNumber
			, coalesce(id.odoverdraftlimit, main.OverdraftCurrentLimit)	as Capital
			, id.AccountReference
			, Product = 'Overdraft'

from TS_Daily_OverdraftSales_TEMP3									id

left join	PRD_ExactusSync.dbo.OverdraftMaster						main
on			id.clientnumber = main.clientnumber
and			id.AccountReference = main.overdraftaccountnumber

			) by APS;
quit;



/***************** Overall Sales and Risk *****************/

proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (

USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Overall_Daily_Sales_Temp')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_Overall_Daily_Sales_Temp
  END;

CREATE TABLE Creditprofitability.dbo.TS_Overall_Daily_Sales_Temp
WITH (DISTRIBUTION = HASH(ApplicationID), CLUSTERED COLUMNSTORE INDEX)
AS

SELECT a.ApplicationID
	  ,convert(char(8), a.CreationDate, 112) as Creationdate
	  ,a.CreationMonth
	  ,a.ISFOREIGNERIND
	  ,coalesce(b.Product, c.Product, d.Product) AS Product
	  ,coalesce(b.Capital, c.Capital, d.Capital) AS Capital

FROM Creditprofitability.dbo.TS_Daily_AppBase AS a

LEFT JOIN Creditprofitability.dbo.TS_Daily_LoanSales AS b
ON a.ApplicationID = b.ApplicationID

LEFT JOIN Creditprofitability.dbo.TS_Daily_CardSales AS c
ON a.ApplicationID = c.ApplicationID

LEFT JOIN Creditprofitability.dbo.TS_Daily_OverdraftSales AS d
ON a.ApplicationID = d.ApplicationID

			) by APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Overall_Daily_Sales')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_Overall_Daily_Sales
  END;

CREATE TABLE Creditprofitability.dbo.TS_Overall_Daily_Sales
WITH (DISTRIBUTION = HASH(ApplicationID), CLUSTERED COLUMNSTORE INDEX)
AS

select *
from Creditprofitability.dbo.TS_Overall_Daily_Sales_Temp
where Product is not null


			) by APS;
quit;



/***************** Scoring Results *****************/
proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Daily_ScoringResults')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_Daily_ScoringResults
  END;

CREATE TABLE Creditprofitability.dbo.TS_Daily_ScoringResults
WITH (DISTRIBUTION = HASH(ApplicationID), CLUSTERED COLUMNSTORE INDEX)
AS

SELECT distinct x.ApplicationID
	  , x.AdjRiskPD as Scorecard_Exp
	  , x.ScoreBand

FROM (
		SELECT requestid AS ApplicationID
			  , case when adjriskpd = '-9.90008E7' then null else cast(adjriskpd as float) end as AdjRiskPD
			  , cast(left(ScoreBand, 2) AS INT) as ScoreBand
			  , rank () OVER (PARTITION BY UniqueID ORDER BY Scoreband DESC) AS Highest
			  , rank () OVER (PARTITION BY requestid ORDER BY UniqueID DESC) AS Latest
		FROM PRD_Press.capri.Creditrisk_Riskgroup
		WHERE CAST(requestid AS VARCHAR) IN (SELECT CAST(ApplicationID AS VARCHAR) FROM Creditprofitability.dbo.TS_Daily_AppBase)

	) AS x
WHERE x.Latest = 1


			) by APS;
quit;




/***************** Testing Results *****************/

proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Daily_TestingStrategy')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_Daily_TestingStrategy
  END;

CREATE TABLE Creditprofitability.dbo.TS_Daily_TestingStrategy
WITH (DISTRIBUTION = HASH(ApplicationID), CLUSTERED COLUMNSTORE INDEX)
AS

SELECT distinct x.ApplicationID
	  , x.scorecardversion
	  , x.Strategy2RandomNumber

FROM (
		SELECT requestid AS ApplicationID
			  , scorecardversion
			  , Strategy2RandomNumber
			  , rank () OVER (PARTITION BY requestid ORDER BY UniqueID DESC) AS Latest
		FROM PRD_Press.Capri.Capri_Testing_Strategy_Results_2021
		WHERE CAST(requestid AS VARCHAR) IN (SELECT CAST(ApplicationID AS VARCHAR) FROM Creditprofitability.dbo.TS_Daily_AppBase)

	) AS x
WHERE x.Latest = 1

			) by APS;
quit;




/***************** Joins *****************/

proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Daily_Sales_Risk')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_Daily_Sales_Risk
  END;

CREATE TABLE Creditprofitability.dbo.TS_Daily_Sales_Risk
WITH (DISTRIBUTION = HASH(ApplicationID), CLUSTERED COLUMNSTORE INDEX)
AS

Select Sales.ApplicationID
, Sales.Creationdate
, Sales.Product
, Sales.Capital
, Sales.ISFOREIGNERIND
, Scoring.Scoreband as scoreband_@_App

, case	when Sales.CreationDate <= '20230615' then left(replace(Sales.CreationDate, '-',''),6)
		when Sales.CreationDate >= '20230616' and Sales.CreationDate <= '20230631' then ' Post Deploy 202306'
		when Sales.CreationDate >= '20230701' and Sales.CreationDate <= '20230731' then ' Post Deploy 202307'
		when Sales.CreationDate >= '20230801' then ' Post Deploy 202308'

  end as Creationmonth

, case	when Sales.CreationDate >= '20230501' and Sales.CreationDate <= '20230531' then ' Pre-Deploy'
		when Sales.CreationDate >= '20230616' and Sales.CreationDate <= '20230631' then ' Post Deploy 202306'
		when Sales.CreationDate >= '20230701' and Sales.CreationDate <= '20230731' then ' Post Deploy 202307'
		when Sales.CreationDate >= '20230801' then ' Post Deploy 202308'
  end as Creationmonth_grouped

, case  when Sales.CreationDate <= '20230727' then ' Pre-Deploy'
		when Sales.CreationDate >  '20230727' then ' Post Deploy 202307'
		when Sales.CreationDate >= '20230801' then ' Post Deploy 202308'
  end as Creationmonth_FOREIGNER

, case	when Sales.Product in ('Overdraft', 'Loan') then Scoring.Scorecard_Exp
		when Sales.Product in ('Card') then

						   case when Sales.CreationDate > '20201121' and Sales.CreationDate <= '20220303'
	                            then
	                        (case when Scoring.scoreBand >= 50 and Scoring.scoreBand <= 67 then (0.0256*(Scoring.scoreBand - 49) + 0.0108) * 1.07
									when Scoring.scoreBand  = 68 then 0.1222
									when Scoring.scoreBand  = 69 then 0.1788
									when Scoring.scoreBand  = 70 then 0.2287
									when Scoring.scoreBand  = 71 then 0.2789
									else 0 
								end )

							when Sales.CreationDate > '20220303' and Sales.CreationDate <= '20220824' and Testing.ScorecardVersion in ('V636', 'V4')
							then 
							(case when Scoring.scoreBand >= 50 and Scoring.scoreBand <= 67 then (0.0256*(Scoring.scoreBand - 49) + 0.0108) * 1.07
									when Scoring.scoreBand  = 68 then 0.1222
									when Scoring.scoreBand  = 69 then 0.1788
									when Scoring.scoreBand  = 70 then 0.2287
									when Scoring.scoreBand  = 71 then 0.2789
									else 0 
								end )
									 
							when Sales.CreationDate > '20220303' and Sales.CreationDate <= '20220824' and Testing.ScorecardVersion = 'V645' and Testing.Strategy2RandomNumber < 0.5
							then 
							(case when Scoring.scoreBand >= 50 and Scoring.scoreBand <= 67 then (0.0256*(Scoring.scoreBand - 49) + 0.0108) * 1.07
									when Scoring.scoreBand  = 68 then 0.1222
									when Scoring.scoreBand  = 69 then 0.1788
									when Scoring.scoreBand  = 70 then 0.2287
									when Scoring.scoreBand  = 71 then 0.2789
									else 0 
								end )

							when Sales.CreationDate > '20220303' and Sales.CreationDate <= '20220824' and Testing.ScorecardVersion = 'V645' and Testing.Strategy2RandomNumber >= 0.5
							then case when Scoring.Scorecard_Exp = 1 then null else ((1/(1+power((Scoring.Scorecard_Exp/(1-Scoring.Scorecard_Exp)),(-1*0.8181))*exp(0.8945)))*1.1) end 
										  
							when Testing.ScorecardVersion in ('V655','V5','V4') and Sales.CreationDate > '20220824' and Sales.CreationDate <= '20221124' 
							then Scoring.Scorecard_Exp
										  
							when Testing.ScorecardVersion in ('V655','V5','V4', 'V585', 'V667') and Sales.CreationDate > '20221124'
							then case when Scoring.Scorecard_Exp = 1 then null else ((1/(1+power((Scoring.Scorecard_Exp/(1-Scoring.Scorecard_Exp)),(-1*0.8231))*exp(0.5387)))) end
							end
 end as Exp@App_Buffer

, case	when scoreband = 50 then 0.069
		when scoreband = 51 then 0.076
		when scoreband = 52 then 0.101
		when scoreband = 53 then 0.128
		when scoreband = 54 then 0.149
		when scoreband = 55 then 0.166
		when scoreband = 56 then 0.179
		when scoreband = 57 then 0.187
		when scoreband = 58 then 0.202
		when scoreband = 59 then 0.217
		when scoreband = 60 then 0.241
		when scoreband = 61 then 0.274
		when scoreband = 62 then 0
		when scoreband = 63 then 0
		when scoreband = 64 then 0
		when scoreband = 65 then 0
		when scoreband = 66 then 0
		when scoreband = 67 then 0
		when scoreband = 68 then 0
		when scoreband = 69 then 0
		when scoreband = 70 then 0.274
end as Exp_PD

From Creditprofitability.dbo.TS_Overall_Daily_Sales as Sales

Left join Creditprofitability.dbo.TS_Daily_ScoringResults as Scoring
on cast(Sales.ApplicationID AS VARCHAR) = cast(Scoring.ApplicationID AS VARCHAR) 

Left join Creditprofitability.dbo.TS_Daily_TestingStrategy as Testing
on cast(Sales.ApplicationID AS VARCHAR) = cast(Testing.ApplicationID AS VARCHAR) 


			) by APS;
quit;




/***************** Channels *****************/
proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Daily_Channels')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_Daily_Channels
  END;

CREATE TABLE Creditprofitability.dbo.TS_Daily_Channels
WITH (DISTRIBUTION = HASH(ApplicationID), CLUSTERED COLUMNSTORE INDEX)
AS

SELECT x.ApplicationID
	  ,x.Channel
	  ,x.ApplicationStatus
FROM (
		SELECT ApplicationID
			  ,CASE WHEN ExternalReference IN ('OMNI_BFO','OMNI_BRANCH') THEN 'Branch'
					WHEN ExternalReference IN ('OMNI_CC','OMNI_CALL_CENT') THEN 'Call Centre'
					WHEN ExternalReference IN ('OMNI_APP') THEN 'OMNI App'
					WHEN ExternalReference IN ('OMNI_QQ') THEN 'OMNI QQ'
					WHEN ExternalReference IN ('OMNI_WEB') THEN 'Web'
					WHEN ExternalReference IN ('OMNI_PROSPECTING') THEN 'OMNI Prospecting'
					END AS Channel
			  ,ApplicationStatus
		FROM Prd_ExactusSync.dbo.Applications
		WHERE ApplicationID IN (SELECT ApplicationID FROM Creditprofitability.dbo.TS_Daily_AppBase)
	) AS x


			) by APS;
quit;



/*********** Get all offers  ************/
proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Daily_Offers_data')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_Daily_Offers_data
  END;

CREATE TABLE Creditprofitability.dbo.TS_Daily_Offers_data
WITH (DISTRIBUTION = HASH(APPLICATIONID), CLUSTERED COLUMNSTORE INDEX)
AS SELECT

DISTINCT x.ApplicationID
		,x.CreationDate
		,x.Product
		,GotOffer = 1

FROM	(SELECT	 ApplicationID
				,LASTUPDATETIMESTAMP AS CreationDate
				,CASE WHEN Description LIKE '%Loan%'				THEN 'Loan'
					  WHEN Description LIKE '%card%'				THEN 'Card'
					  WHEN Description LIKE '%Overdraft%'			THEN 'Overdraft'
					  END AS Product
		FROM PRD_ExactusSync.dbo.ApplicationOffers
		WHERE OFFERSSELECTEDTYPE = 'SEL') AS x
WHERE x.ApplicationID IN (SELECT ApplicationID FROM Creditprofitability.dbo.TS_Daily_AppBase)


			) by APS;
quit;



/*********** Add all data to the appbase  ************/
proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Daily_Apps_data')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_Daily_Apps_data
  END;

CREATE TABLE Creditprofitability.dbo.TS_Daily_Apps_data
WITH (DISTRIBUTION = HASH(ApplicationID), CLUSTERED COLUMNSTORE INDEX)
AS

SELECT BASE.ApplicationID, BASE.ISFOREIGNERIND
	  ,convert(varchar(8), BASE.Creationdate, 112) as Creationdate
	  ,BASE.CreationMonth
	  ,COALESCE(OFFERS.Product, SALES.Product) AS Product
	  ,OFFERS.GotOffer AS Offered
	  ,SALES.Capital
	  ,SCORING.ScoreBand
	  ,CHANNELS.Channel
	  ,CASE WHEN SALES.Capital > 0 THEN 1 ELSE 0 END AS Disbursed
	  ,CASE WHEN BASE.ApplicationID IN (SELECT ApplicationID FROM PRD_ExactusSync.dbo.ApplicationOffers)
			THEN 1 ELSE 0
	   END AS GotOffer	  

FROM Creditprofitability.dbo.TS_Daily_AppBase				AS BASE

LEFT JOIN Creditprofitability.dbo.TS_Daily_Offers_data			AS OFFERS
ON CAST(BASE.ApplicationID AS VARCHAR)  = CAST(OFFERS.ApplicationID AS VARCHAR) 

LEFT JOIN Creditprofitability.dbo.TS_Daily_Sales_Risk			AS SALES
ON CAST(BASE.ApplicationID AS VARCHAR)  = CAST(SALES.ApplicationID AS VARCHAR) 

LEFT JOIN Creditprofitability.dbo.TS_Daily_ScoringResults	AS SCORING
ON CAST(BASE.ApplicationID AS VARCHAR)  = CAST(SCORING.ApplicationID AS VARCHAR) 

LEFT JOIN Creditprofitability.dbo.TS_Daily_Channels		AS CHANNELS
ON CAST(BASE.ApplicationID AS VARCHAR)  = CAST(CHANNELS.ApplicationID AS VARCHAR) 

			) by APS;
quit;



/*********** Loan SRA Disbursals  ************/
proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Daily_SRA')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_Daily_SRA
  END;

Create table Creditprofitability.dbo.TS_Daily_SRA
with (distribution=hash(CreationMonth), clustered columnstore index) 
as

select a.Creationmonth
, sum(a.Exp@App_Buffer*a.Capital)/sum(cast(a.Capital as numeric)) as RW_Risk_Overall
, avg(b.RW_Risk_SRA) as RW_Risk_SRA
, avg(b.Capital)/sum(a.Capital) as SRA_Perc

from Creditprofitability.dbo.TS_Daily_Sales_Risk a

left join	(select a.CreationMonth, a.ISFOREIGNERIND, sum(a.Exp@App_Buffer*a.Capital)/sum(cast(a.Capital as numeric)) as RW_Risk_SRA, sum(a.Capital) as Capital
			 from Creditprofitability.dbo.TS_Daily_Sales_Risk a
			 left join loan_pricing_daily b
			 on a.Applicationid = b.loanid
			 where b.SRA = 'Y' and a.product = 'Loan'
			 group by a.CreationMonth, a.ISFOREIGNERIND
			) b 
on a.CreationMonth = b.CreationMonth

where a.product = 'Loan'
group by a.Creationmonth

			) by APS;
quit;



/*********** Risk per RG  ************/
proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Daily_Risk_RG')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_Daily_Risk_RG
  END;

create table Creditprofitability.dbo.TS_Daily_Risk_RG
with (distribution = hash(scoreband_@_App), clustered columnstore index)
as

select scoreband_@_App, Creationmonth_grouped, sum(Exp@App_Buffer*Capital)/sum(Capital) as RW_Exp
, avg(Exp_PD) as Exp_PD
from Creditprofitability.dbo.TS_Daily_Sales_Risk
where Creationmonth_grouped in (' Post Deploy 202308')
group by scoreband_@_App, Creationmonth_grouped

union all

select scoreband_@_App, Creationmonth_grouped, sum(Exp@App_Buffer*Capital)/sum(Capital) as RW_Exp
, avg(Exp_PD) as Exp_PD
from Creditprofitability.dbo.TS_Daily_Sales_Risk
where Creationmonth_grouped = ' Pre-Deploy'
group by scoreband_@_App, Creationmonth_grouped

			) by APS;
quit;



/*********** Calculate Overall Post Deploy Offerrate  ************/
proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
			use Creditprofitability

			if exists (select * from information_schema.tables where table_name like 'ELR_Daily_Offerrate')
			begin
				drop table Creditprofitability.dbo.ELR_Daily_Offerrate
			end;

			create table Creditprofitability.dbo.ELR_Daily_Offerrate
			with (distribution = hash(Creationmonth_grouped), clustered columnstore index)
			as 

			select 
			  case	when CreationDate >= '20230501' and CreationDate <= '20230531' then ' Pre-Deploy'
					when CreationDate >= '20230616' and CreationDate <= '20230631' then ' Post Deploy 202306'
					when CreationDate >= '20230701'  then ' Post Deploy 202307'
			  end as Creationmonth_grouped

			, sum(cast(Gotoffer as numeric))/count(*) as Offerrate
			from CreditProfitability.dbo.ELR_Daily_Apps_data 
			where ((Creationdate >= '20230501' and Creationdate <= '20230531') or (Creationdate > '20230615'))
			and Channel <> 'OMNI Prospecting'
			group by
			  case	when CreationDate >= '20230501' and CreationDate <= '20230531' then ' Pre-Deploy'
					when CreationDate >= '20230616' and CreationDate <= '20230631' then ' Post Deploy 202306'
					when CreationDate >= '20230701'  then ' Post Deploy 202307'
			  end

			) by APS;
quit;



/*** Foreigner ***/
proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Daily_Risk_RG_FN')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_Daily_Risk_RG_FN
  END;

create table Creditprofitability.dbo.TS_Daily_Risk_RG_FN
with (distribution = hash(scoreband_@_App), clustered columnstore index)
as

select scoreband_@_App, Creationmonth_grouped, sum(Exp@App_Buffer*Capital)/sum(Capital) as RW_Exp
, avg(Exp_PD) as Exp_PD
from Creditprofitability.dbo.TS_Daily_Sales_Risk
where Creationmonth_grouped in (' Post Deploy 202307') and ISFOREIGNERIND = 1
group by scoreband_@_App, Creationmonth_grouped

union all

select scoreband_@_App, Creationmonth_grouped, sum(Exp@App_Buffer*Capital)/sum(Capital) as RW_Exp
, avg(Exp_PD) as Exp_PD
from Creditprofitability.dbo.TS_Daily_Sales_Risk
where Creationmonth_grouped = ' Pre-Deploy' and ISFOREIGNERIND = 1
group by scoreband_@_App, Creationmonth_grouped
			) by APS;
quit;


/*********** Calculate Daily Post Deploy Offerrate  ************/
proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Daily_DailyOffers')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_Daily_DailyOffers
  END;

create table Creditprofitability.dbo.TS_Daily_DailyOffers
with (distribution = hash(Creationdate), clustered columnstore index)
as
select a.Creationdate
, cast(sum(cast(a.Gotoffer as float)) as decimal)/count(*) as Offerrate
, cast(sum(case when a.Capital > 0 then 1 else 0 end) as decimal)/NULLIF(cast(sum(case when a.Gotoffer > 0 then 1 else 0 end) as decimal), 0) as TakeUpRate
, count(*) as Volume
, sum(a.gotoffer) as Offers
, 0.242 as Expected_Offerrate
, 0.123 as PreDeploy_Offerrate
, 0.435 as Expected_TakeupRate

from CreditProfitability.dbo.TS_Daily_Apps_data a

where Channel <> 'OMNI Prospecting'
and a.Creationdate > '20230615'
group by a.Creationdate
having cast(sum(case when a.Capital > 0 then 1 else 0 end) as decimal)/NULLIF(cast(sum(case when a.Gotoffer > 0 then 1 else 0 end) as decimal), 0) is not null


			) by APS;
quit;



/* FN */
proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Daily_DailyOffers_FN')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_Daily_DailyOffers_FN
  END;

create table Creditprofitability.dbo.TS_Daily_DailyOffers_FN
with (distribution = hash(Creationdate), clustered columnstore index)
as
select a.Creationdate
, cast(sum(cast(a.Gotoffer as float)) as decimal)/count(*) as Offerrate
, cast(sum(case when a.Capital > 0 then 1 else 0 end) as decimal)/NULLIF(cast(sum(case when a.Gotoffer > 0 then 1 else 0 end) as decimal), 0) as TakeUpRate
, count(*) as Volume
, sum(a.gotoffer) as Offers
, 0.242 as Expected_Offerrate
, 0.123 as PreDeploy_Offerrate
, 0.435 as Expected_TakeupRate

from CreditProfitability.dbo.TS_Daily_Apps_data a

where Channel <> 'OMNI Prospecting'
and a.Creationdate > '20230615'
and a.ISFOREIGNERIND = 1
group by a.Creationdate
having cast(sum(case when a.Capital > 0 then 1 else 0 end) as decimal)/NULLIF(cast(sum(case when a.Gotoffer > 0 then 1 else 0 end) as decimal), 0) is not null


			) by APS;
quit;



/*********** Calculate Monthly Post Deploy Offerrate  ************/

proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Daily_Offerrate')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_Daily_Offerrate
  END;

create table Creditprofitability.dbo.TS_Daily_Offerrate
with (distribution = hash(Creationmonth_grouped), clustered columnstore index)
as 

select 
  case	when CreationDate >= '20230501' and CreationDate <= '20230531' then ' Pre-Deploy'
		when CreationDate >= '20230616' and CreationDate <= '20230631' then ' Post Deploy 202306'
		when CreationDate >= '20230701' and CreationDate <= '20230731' then ' Post Deploy 202307'
		when CreationDate >= '20230801'  then ' Post Deploy 202308'
  end as Creationmonth_grouped

, sum(cast(Gotoffer as numeric))/count(*) as Offerrate
from CreditProfitability.dbo.TS_Daily_Apps_data 
where ((Creationdate >= '20230501' and Creationdate <= '20230531') or (Creationdate > '20230615'))
and Channel <> 'OMNI Prospecting'
group by
  case	when CreationDate >= '20230501' and CreationDate <= '20230531' then ' Pre-Deploy'
		when CreationDate >= '20230616' and CreationDate <= '20230631' then ' Post Deploy 202306'
		when CreationDate >= '20230701' and CreationDate <= '20230731' then ' Post Deploy 202307'
		when CreationDate >= '20230801'  then ' Post Deploy 202308'
  end 

			) by APS;
quit;


proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Daily_MonthlyOffers')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_Daily_MonthlyOffers
  END;

create table Creditprofitability.dbo.TS_Daily_MonthlyOffers
with (distribution = hash(Creation_month), clustered columnstore index)
as

select 
  case	when a.CreationDate <= '20230615' then Creationmonth
		when a.CreationDate >= '20230616' and a.CreationDate <= '20230631' then ' Post Deploy 202306'
		when a.CreationDate >= '20230701' and a.CreationDate <= '20230731' then ' Post Deploy 202307'
		when a.CreationDate >= '20230801'  then ' Post Deploy 202308'
  end as Creation_month
, sum(cast(a.Gotoffer as numeric))/count(*) as Offerrate
, sum(case when a.Capital is not null then 1 else 0 end)/NULLIF(cast(sum(case when a.Gotoffer > 0 then 1 else 0 end) as decimal), 0) as TakeUpRate
, count(*) as Volume
, sum(a.gotoffer) as Offers

from CreditProfitability.dbo.TS_Daily_Apps_data a

where Channel <> 'OMNI Prospecting'
and a.Creationmonth > '202208'

group by     case	when a.CreationDate <= '20230615' then Creationmonth
		when a.CreationDate >= '20230616' and a.CreationDate <= '20230631' then ' Post Deploy 202306'
		when a.CreationDate >= '20230701' and a.CreationDate <= '20230731' then ' Post Deploy 202307'
		when a.CreationDate >= '20230801'  then ' Post Deploy 202308'
  end
having cast(sum(case when a.Capital > 0 then 1 else 0 end) as decimal)/NULLIF(cast(sum(case when a.Gotoffer > 0 then 1 else 0 end) as decimal), 0) is not null
 

			) by APS;
quit;


/*********** Calculate Scoreband Oferrate  ************/

proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Daily_Offers_RG')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_Daily_Offers_RG
  END;

create table Creditprofitability.dbo.TS_Daily_Offers_RG
with (distribution = hash(Scoreband), clustered columnstore index)
as

select Scoreband, count(*)/avg(cast(Total as numeric)) as Perc
, sum(cast(Gotoffer as numeric))/count(*) as Offerrate
, case	when a.Creationdate >= '20230501' and a.Creationdate <= '20230531' then ' Pre-Deploy'
		when a.CreationDate >= '20230616' and a.CreationDate <= '20230631' then ' Post Deploy 202306'
		when a.CreationDate >= '20230701' and a.CreationDate <= '20230731' then ' Post Deploy 202307'
		when a.CreationDate >= '20230801'  then ' Post Deploy 202308'
  end as Creationmonth_grouped

, avg(case	when scoreband = 50 then 0.697
		when scoreband = 51 then 0.656
		when scoreband = 52 then 0.611
		when scoreband = 53 then 0.60
		when scoreband = 54 then 0.525
		when scoreband = 55 then 0.502
		when scoreband = 56 then 0.427
		when scoreband = 57 then 0.537
		when scoreband = 58 then 0.385
		when scoreband = 59 then 0.345
		when scoreband = 60 then 0.343
		when scoreband = 61 then 0.39
		when scoreband = 62 then 0
		when scoreband = 63 then 0
		when scoreband = 64 then 0
		when scoreband = 65 then 0
		when scoreband = 66 then 0
		when scoreband = 67 then 0
		when scoreband = 68 then 0
		when scoreband = 69 then 0.692
		when scoreband = 70 then 0.10
end) as Exp_Offerrate

from		(select Creationdate, Scoreband, GotOffer, 1 as Ind from CreditProfitability.dbo.TS_Daily_Apps_data where Channel <> 'OMNI Prospecting' and Creationdate >= '20230801' and Creationdate <= '20230831') as a

left join	(select 1 as Ind, count(*) as Total from CreditProfitability.dbo.TS_Daily_Apps_data where Channel <> 'OMNI Prospecting' and Creationdate >= '20230801' and Creationdate <= '20230831') as b
on a.Ind = b.Ind

where		Creationdate >= '20230801' and Creationdate <= '20230831'
group by Scoreband
, case	when a.Creationdate >= '20230501' and a.Creationdate <= '20230531' then ' Pre-Deploy'
		when a.CreationDate >= '20230616' and a.CreationDate <= '20230631' then ' Post Deploy 202306'
		when a.CreationDate >= '20230701' and a.CreationDate <= '20230731' then ' Post Deploy 202307'
		when a.CreationDate >= '20230801'  then ' Post Deploy 202308'
  end

union all

select Scoreband, count(*)/avg(cast(Total as numeric)) as Perc
, sum(cast(GotOffer as numeric))/count(*) as Offerrate
, case	when a.Creationdate >= '20230501' and a.Creationdate <= '20230531' then ' Pre-Deploy'
		when a.CreationDate >= '20230616' and a.CreationDate <= '20230631' then ' Post Deploy 202306'
		when a.CreationDate >= '20230701' and a.CreationDate <= '20230731' then ' Post Deploy 202307'
		when a.CreationDate >= '20230801'  then ' Post Deploy 202308'
  end as Creationmonth_grouped

, avg(case	when scoreband = 50 then 0.697
		when scoreband = 51 then 0.656
		when scoreband = 52 then 0.611
		when scoreband = 53 then 0.60
		when scoreband = 54 then 0.525
		when scoreband = 55 then 0.502
		when scoreband = 56 then 0.427
		when scoreband = 57 then 0.537
		when scoreband = 58 then 0.385
		when scoreband = 59 then 0.345
		when scoreband = 60 then 0.343
		when scoreband = 61 then 0.39
		when scoreband = 62 then 0
		when scoreband = 63 then 0
		when scoreband = 64 then 0
		when scoreband = 65 then 0
		when scoreband = 66 then 0
		when scoreband = 67 then 0
		when scoreband = 68 then 0
		when scoreband = 69 then 0.692
		when scoreband = 70 then 0.10
	  end) as Exp_Offerrate

from		(select Creationdate, Scoreband, GotOffer, 1 as Ind from CreditProfitability.dbo.TS_Daily_Apps_data where Channel <> 'OMNI Prospecting' and Creationdate >= '20230501' and Creationdate <= '20230531') as a

left join	(select 1 as Ind, count(*) as Total from CreditProfitability.dbo.TS_Daily_Apps_data where Channel <> 'OMNI Prospecting' and Creationdate >= '20230501' and Creationdate <= '20230531') as b
on a.Ind = b.Ind

where Creationdate >= '20230501' and Creationdate <= '20230531'
group by Scoreband
, case	when a.Creationdate >= '20230501' and a.Creationdate <= '20230531' then ' Pre-Deploy'
		when a.CreationDate >= '20230616' and a.CreationDate <= '20230631' then ' Post Deploy 202306'
		when a.CreationDate >= '20230701' and a.CreationDate <= '20230731' then ' Post Deploy 202307'
		when a.CreationDate >= '20230801'  then ' Post Deploy 202308'
  end


			) by APS;
quit;

/***************** FN *****************/
proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Daily_Offers_RG_FN')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_Daily_Offers_RG_FN
  END;

create table Creditprofitability.dbo.TS_Daily_Offers_RG_FN
with (distribution = hash(Scoreband), clustered columnstore index)
as

select Scoreband, count(*)/avg(cast(Total as numeric)) as Perc
, sum(cast(Gotoffer as numeric))/count(*) as Offerrate
, case	when a.Creationdate >= '20230501' and a.Creationdate <= '20230531' then ' Pre-Deploy'
		when a.CreationDate >= '20230616' and a.CreationDate <= '20230631' then ' Post Deploy 202306'
		when a.CreationDate >= '20230701' and a.CreationDate <= '20230731' then ' Post Deploy 202307'
		when a.CreationDate >= '20230801'  then ' Post Deploy 202308'
  end as Creationmonth_grouped

, avg(case	when scoreband = 50 then 0.697
		when scoreband = 51 then 0.656
		when scoreband = 52 then 0.611
		when scoreband = 53 then 0.60
		when scoreband = 54 then 0.525
		when scoreband = 55 then 0.502
		when scoreband = 56 then 0.427
		when scoreband = 57 then 0.537
		when scoreband = 58 then 0.385
		when scoreband = 59 then 0.345
		when scoreband = 60 then 0.343
		when scoreband = 61 then 0.39
		when scoreband = 62 then 0
		when scoreband = 63 then 0
		when scoreband = 64 then 0
		when scoreband = 65 then 0
		when scoreband = 66 then 0
		when scoreband = 67 then 0
		when scoreband = 68 then 0
		when scoreband = 69 then 0.692
		when scoreband = 70 then 0.10
end) as Exp_Offerrate

from		(select Creationdate, ISFOREIGNERIND, Scoreband, GotOffer, 1 as Ind from CreditProfitability.dbo.TS_Daily_Apps_data where Channel <> 'OMNI Prospecting' and Creationdate >= '20230701' and Creationdate <= '20230831' and ISFOREIGNERIND = 1) as a

left join	(select 1 as Ind, count(*) as Total from CreditProfitability.dbo.TS_Daily_Apps_data where Channel <> 'OMNI Prospecting' and Creationdate >= '20230701' and Creationdate <= '20230831' and ISFOREIGNERIND = 1) as b
on a.Ind = b.Ind

where		Creationdate >= '20230801' and Creationdate <= '20230831' and ISFOREIGNERIND = 1
group by Scoreband
, case	when a.Creationdate >= '20230501' and a.Creationdate <= '20230531' then ' Pre-Deploy'
		when a.CreationDate >= '20230616' and a.CreationDate <= '20230631' then ' Post Deploy 202306'
		when a.CreationDate >= '20230701' and a.CreationDate <= '20230731' then ' Post Deploy 202307'
		when a.CreationDate >= '20230801'  then ' Post Deploy 202308'
  end

union all

select Scoreband, count(*)/avg(cast(Total as numeric)) as Perc
, sum(cast(GotOffer as numeric))/count(*) as Offerrate
, case	when a.Creationdate >= '20230501' and a.Creationdate <= '20230531' then ' Pre-Deploy'
		when a.CreationDate >= '20230616' and a.CreationDate <= '20230631' then ' Post Deploy 202306'
		when a.CreationDate >= '20230701' and a.CreationDate <= '20230731' then ' Post Deploy 202307'
		when a.CreationDate >= '20230801'  then ' Post Deploy 202308'
  end as Creationmonth_grouped

, avg(case	when scoreband = 50 then 0.697
		when scoreband = 51 then 0.656
		when scoreband = 52 then 0.611
		when scoreband = 53 then 0.60
		when scoreband = 54 then 0.525
		when scoreband = 55 then 0.502
		when scoreband = 56 then 0.427
		when scoreband = 57 then 0.537
		when scoreband = 58 then 0.385
		when scoreband = 59 then 0.345
		when scoreband = 60 then 0.343
		when scoreband = 61 then 0.39
		when scoreband = 62 then 0
		when scoreband = 63 then 0
		when scoreband = 64 then 0
		when scoreband = 65 then 0
		when scoreband = 66 then 0
		when scoreband = 67 then 0
		when scoreband = 68 then 0
		when scoreband = 69 then 0.692
		when scoreband = 70 then 0.10
	  end) as Exp_Offerrate

from		(select Creationdate, ISFOREIGNERIND, Scoreband, GotOffer, 1 as Ind from CreditProfitability.dbo.TS_Daily_Apps_data where Channel <> 'OMNI Prospecting' and Creationdate >= '20230501' and Creationdate <= '20230531' and ISFOREIGNERIND = 1) as a

left join	(select 1 as Ind, count(*) as Total from CreditProfitability.dbo.TS_Daily_Apps_data where Channel <> 'OMNI Prospecting' and Creationdate >= '20230501' and Creationdate <= '20230531' and ISFOREIGNERIND = 1) as b
on a.Ind = b.Ind

where Creationdate >= '20230501' and Creationdate <= '20230531' and ISFOREIGNERIND = 1
group by Scoreband
, case	when a.Creationdate >= '20230501' and a.Creationdate <= '20230531' then ' Pre-Deploy'
		when a.CreationDate >= '20230616' and a.CreationDate <= '20230631' then ' Post Deploy 202306'
		when a.CreationDate >= '20230701' and a.CreationDate <= '20230731' then ' Post Deploy 202307'
		when a.CreationDate >= '20230801'  then ' Post Deploy 202308'
  end


			) by APS;
quit;



/***************** Sales in comparison to March apps *****************/

proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Daily_Sales_Apps_1')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_Daily_Sales_Apps_1
  END;

create table Creditprofitability.dbo.TS_Daily_Sales_Apps_1
with (distribution = hash(Creationdate), clustered columnstore index)
as

select Creationdate
, count(*) as Apps
, sum(Capital) as Capital

from TS_Daily_Apps_data

where Creationdate >= '20230616' and Channel <> 'OMNI Prospecting'
group by Creationdate


			) by APS;
quit;


proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
	USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Daily_Sales_Apps_2')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_Daily_Sales_Apps_2
  END;

create table Creditprofitability.dbo.TS_Daily_Sales_Apps_2
with (distribution = hash(Creationdate), clustered columnstore index)
as

select *
, cast(Apps as numeric)/107105*576042832 as Exp_Sales
from TS_Daily_Sales_Apps_1

	) by APS;
quit;




/****** NCR Short Term Loan ******/
proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_NCR_Risk_Temp')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_NCR_Risk_Temp
  END;

CREATE TABLE Creditprofitability.dbo.TS_NCR_Risk_Temp
WITH (DISTRIBUTION = HASH(Loanid), CLUSTERED COLUMNSTORE INDEX)
AS
select a.Loanid
      ,a.Capital
	  ,a.OPD
	  ,a.ProductCategory
	  ,a.Scoreband_@_App as Scoreband_at_App
	  ,a.Loan_Exp@App_CD3_MOB9_NoBuffer as Exp_at_App_NoBuffer
	  ,a.Loan_Exp@App_CD3_MOB9_Buffer as Exp_at_App_Buffer
from CreditProfitability.dbo.loan_pricing_daily a 
where a.ProductCategory = 300

	) by APS;
quit;



proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_NCR_Risk_Temp2')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_NCR_Risk_Temp2
  END;

CREATE TABLE Creditprofitability.dbo.TS_NCR_Risk_Temp2
WITH (DISTRIBUTION = HASH(APPLICATIONID), CLUSTERED COLUMNSTORE INDEX)
AS
select a.APPLICATIONID,
	   max(a.LOANCAPITAL) as MaxOffer
from PRD_ExactusSync.dbo.ApplicationOffers a 
inner join PRD_ExactusSync.dbo.Applications b
on a.applicationid = b.applicationid
where b.creationdate >= '2023-07-27' and a.LOANCAPITAL > 0
group by a.APPLICATIONID


	) by APS;
quit;



proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_NCR_Risk_Temp3')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_NCR_Risk_Temp3
  END;

CREATE TABLE Creditprofitability.dbo.TS_NCR_Risk_Temp3
WITH (DISTRIBUTION = HASH(APPLICATIONID), CLUSTERED COLUMNSTORE INDEX)
AS
select a.APPLICATIONID,
	   a.APPLICATIONSTATUS,
	   substring(CAST(cast(a.CREATIONDATE as DATE) as varchar),1,10) AS Creation_Date,
	   substring(CAST(cast(a.CREATIONDATE as DATE) as varchar),1,7) AS Creation_Month,
	   substring(CAST(cast(a.LASTUPDATETIMESTAMP as DATE) as varchar),1,10) AS Disbursed_Date,
       a.EXTERNALREFERENCE as CHANNEL
from PRD_ExactusSync.dbo.Applications a
where a.CREATIONDATE >= '2023-07-25' and a.APPLICATIONSTATUS = 'DIS'


	) by APS;
quit;



proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_NCR_Risk_Temp4')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_NCR_Risk_Temp4
  END;

CREATE TABLE Creditprofitability.dbo.TS_NCR_Risk_Temp4
WITH (DISTRIBUTION = HASH(loanid), CLUSTERED COLUMNSTORE INDEX)
AS
select c.Creation_Date,
       c.Disbursed_Date,
	   c.Creation_Month,
	   c.CHANNEL,
	   a.*,
       case when b.MaxOffer is null then a.Capital else b.MaxOffer end as MaxOffer
from Creditprofitability.dbo.TS_NCR_Risk_Temp a left join
     Creditprofitability.dbo.TS_NCR_Risk_Temp2 b
	 on b.applicationid = a.loanid
     left join Creditprofitability.dbo.TS_NCR_Risk_Temp3 c
     on c.applicationid = a.loanid

	) by APS;
quit;



proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_NCR_Risk_Temp5')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_NCR_Risk_Temp5
  END;

CREATE TABLE Creditprofitability.dbo.TS_NCR_Risk_Temp5
WITH (DISTRIBUTION = HASH(loanid), CLUSTERED COLUMNSTORE INDEX)
AS
select *,
       CASE
    WHEN MaxOffer < 0                             THEN 'Less than 0'
    WHEN MaxOffer > 0      AND MaxOffer <= 5000   THEN '0 - 5k'
    WHEN MaxOffer > 5000   AND MaxOffer <= 10000  THEN '5k - 10k'
    WHEN MaxOffer > 10000  AND MaxOffer <= 20000  THEN '10k - 20k'
    WHEN MaxOffer > 20000  AND MaxOffer <= 30000  THEN '20k - 30k'
    WHEN MaxOffer > 30000  AND MaxOffer <= 40000  THEN '30k - 40k'
    WHEN MaxOffer > 40000  AND MaxOffer <= 50000  THEN '40k - 50k'
    WHEN MaxOffer > 50000  AND MaxOffer <= 60000  THEN '50k - 60k'
    WHEN MaxOffer > 60000  AND MaxOffer <= 70000  THEN '60k - 70k'
    WHEN MaxOffer > 70000  AND MaxOffer <= 80000  THEN '70k - 80k'
    WHEN MaxOffer > 80000  AND MaxOffer <= 90000  THEN '80k - 90k'
    WHEN MaxOffer > 90000  AND MaxOffer <= 100000 THEN '90k - 100k'
    WHEN MaxOffer > 100000 AND MaxOffer <= 150000 THEN '100k - 150k'
    WHEN MaxOffer > 150000 AND MaxOffer <= 200000 THEN '150k - 200k'
    WHEN MaxOffer > 200000 AND MaxOffer <= 250000 THEN '200k - 250k'
    WHEN MaxOffer > 250000 AND MaxOffer <= 300000 THEN '250k - 300k'
    WHEN MaxOffer > 300000 AND MaxOffer <= 350000 THEN '300k - 350k'
END AS MaxCapitalOfferGroup
from Creditprofitability.dbo.TS_NCR_Risk_Temp4


	) by APS;
quit;



proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_NCR_Risk_Temp6')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_NCR_Risk_Temp6
 END;

select top 0  A.*
into creditprofitability.dbo.TS_NCR_Risk_Temp6
FROM (SELECT top 0 *,  row_number() over (partition by loanid order by Creation_Date desc,loanid desc) as r2 
            from creditprofitability.dbo.TS_NCR_Risk_Temp5 ) AS A
WHERE r2=1;

insert into creditprofitability.dbo.TS_NCR_Risk_Temp6
select A.*
FROM (SELECT *,  row_number() over (partition by loanid order by Creation_Date desc,loanid desc) as r2 
            from creditprofitability.dbo.TS_NCR_Risk_Temp5 ) AS A
WHERE r2=1;


ALTER TABLE creditprofitability.dbo.TS_NCR_Risk_Temp6 DROP COLUMN r2

	) by APS;
quit;



proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_NCR_Disbursals')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_NCR_Disbursals
 END;

 CREATE TABLE CreditProfitability.dbo.TS_NCR_Disbursals
 WITH (DISTRIBUTION = HASH(loanid),
 	   CLUSTERED COLUMNSTORE INDEX )
	   as
 Select * from CreditProfitability.dbo.TS_NCR_Risk_Temp6

) by APS;
quit;



proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_NCR_Risk')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_NCR_Risk
  END;

CREATE TABLE Creditprofitability.dbo.TS_NCR_Risk
WITH (DISTRIBUTION = HASH(Disbursed_Month), CLUSTERED COLUMNSTORE INDEX)
AS
select left(Disbursed_Date,7) as Disbursed_Month, sum(Exp_at_App_Buffer*OPD)/sum(OPD) as RW_Risk
from Creditprofitability.dbo.TS_NCR_Disbursals
group by left(Disbursed_Date,7)

) by APS;
quit;



proc sql;
	connect to ODBC as APS (dsn=mpwaps);
	execute (
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_NCR_Risk_RG')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TS_NCR_Risk_RG
  END;

CREATE TABLE Creditprofitability.dbo.TS_NCR_Risk_RG
WITH (DISTRIBUTION = HASH(Scoreband_at_App), CLUSTERED COLUMNSTORE INDEX)
AS
select Scoreband_at_App, sum(Exp_at_App_Buffer*OPD)/sum(OPD) as RW_Risk
from Creditprofitability.dbo.TS_NCR_Disbursals
where left(Disbursed_Date,7) = '2023-08'
group by Scoreband_at_App

) by APS;
quit;