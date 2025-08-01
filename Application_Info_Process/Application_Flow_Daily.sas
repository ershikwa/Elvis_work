proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*SCR 1--------------Creates Applications Base table ---------------*/
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables_Base')
  BEGIN
    DROP TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Base
  END;

CREATE TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Base

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

As Select	Distinct CENAPP.Loanid,
			CENAPP.idnumber,
			CENAPP.uniqueid,
			CENAPP.creationdate,
			CENAPP.Branchcode,
			CENAPP.Origination_source,
			CENAPP.Status,
			CENAPP.Uniqueindicator,
			CENAPP.Finaluniqueid,
			CENAPP.Scorecard,
			CENAPP.Wagefreq,
			CENAPP.Bank,
			CENAPP.InCapri,
			CENAPP.Gazelle_wa,
			CENAPP.Transequence,
			CENAPP.scoreband,
			CENAPP.Subgroupcode,
			CENAPP.Occupationstatus,
			CENAPP.Employmenttype,
			CENAPP.Persal,
			CENAPP.Inseconds,
			CENAPP.Transysteminstance,
			CENAPP.Scoremodel,
			CENAPP.Calculatednetincome,
			CENAPP.Creationmonth,
			CENAPP.Sourcesystem,
			CENAPP.Clientnumber

 
FROM (Select Loanid,
			idnumber,
			uniqueid,
			CAST(LEFT(CreationDate, 10) AS DATETIME) AS CreationDate,
			Branchcode,
			Origination_source,
			Status,
			Uniqueindicator,
			Finaluniqueid,
			Scorecard,
			Calculatednetincome,
			Wagefreq,
			Bank,
			InCapri,
			Gazelle_wa,
			Transequence,
			Scoremodel,
			left(scoreband,2) as Scoreband,
			Subgroupcode,
			Occupationstatus,
			Employmenttype,
			Persal,
			Inseconds,
			Pilottype as Transysteminstance,
			SourceSystem,
			Clientnumber,
			concat(left(cast(cast(LEFT(creationdate,10) as date)as varchar),4),substring(cast(cast(creationdate as date)as varchar),6,2)) as Creationmonth,
			rank () over (partition by loanid order by uniqueid desc) as Latest									

from		PRD_Credit_Central.credcentral.VW_MASTER_APPLICATIONS_TABLE 
Where	(Origination_Source NOT IN ('PROSPECT','OMNI_PROSPECTING') OR Origination_Source IS NULL)
		and idnumber not in ('5512309909082',  '5512259900081','5512259970084' , '5512259930088',  '5512299999085',  '5512229999080', '5512239980088')  
		and BranchCode not in ('1999') 
		and BranchCode NOT BETWEEN 80000 and 85999 ) CENAPP
Where	CENAPP.latest = 1
AND		DATEDIFF(mm, CONVERT(DATETIME, cast(CENAPP.creationdate as varchar), 100), getdate()) < 2
AND		DATEDIFF(mm, CONVERT(DATETIME, cast(CENAPP.creationdate as varchar), 100), getdate()) >= 0
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*-------------------Creates Affordability Indicator table --------------------*/
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables_Affordabilityindicator')
 BEGIN
    DROP TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Affordabilityindicator
 END;

CREATE TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Affordabilityindicator

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)
			
As Select 	distinct LNQT.loanid
,applicationdate
, Case when b.applicationdate > '2018-07-07' and (b.maxabexposurebase <= 350  or b.maxcompliancebase <= 350 or b.InstalmentmaxRTI <= 350 or 
b.MAX7BASE <= 350) and (b.maxabexposurebase > 0  and b.maxcompliancebase > 0 and b.InstalmentmaxRTI > 0 and b.MAX7BASE > 0) then '31'
when  b.applicationdate >= '2018-06-17' and b.transysteminstance = 'gazellepilot' and (b.maxabexposurebase <= 350 or b.maxcompliancebase <= 350 or b.InstalmentmaxRTI <= 350 or 
b.MAX7BASE <= 350) and (b.maxabexposurebase > 0  and b.maxcompliancebase > 0 and b.InstalmentmaxRTI > 0 and b.MAX7BASE > 0) then '32'
when b.maxabexposurebase <= 0 or b.maxcompliancebase <= 0 or b.InstalmentmaxRTI <= 0 or b.MAX7BASE <= 0 then '1' else 0 end as failed_affordability		

	from (
		SELECT DISTINCT 
			loanid 
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT
Left join (select c.tranappnumber as loanid
				  ,c.applicationdate
				  ,c.uniqueid
				  ,c.maxabexposurebase
				  ,c.maxcompliancebase
				  ,cast (e.InstalmentmaxRTI as float) as InstalmentmaxRTI
				  ,c.MAX7BASE
				  ,d.transysteminstance
				  , rank () over (partition by c.tranappnumber order by c.uniqueid desc) as latest

			from prd_press.Capri.CAPRI_AFFORDABILITY_RESULTS c
			left join (Select * from prd_press.capri.capri_scoring_results
			where datediff(mm,applicationdate,getdate()) < 2) d 
			on c.uniqueid = d.uniqueid 
			Left join (select * from prd_press.capri.clientprofile_maxRTIinstalment_RTIfunction
			where datediff(mm,request_timestamp,getdate()) < 2) e
			on e.uniqueid = d.uniqueid
			where d.transequence = '007' and datediff(mm,c.applicationdate,getdate()) < 2) as  b
		on cast(LNQT.loanid as varchar) = right(b.uniqueid,10)
		where b.latest = 1

	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*-----------------------------Affordability indicator created*/
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables_Scoringscoreband')
 BEGIN
    DROP TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Scoringscoreband
 END;

CREATE TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Scoringscoreband

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

AS Select LNQT.Loanid,b.scoringscoreband,b.scoreband
				  ,b.scoringpd
				  ,b.adjriskpd
				  ,b.request_timestamp
				  ,b.uniqueid

from (
		SELECT DISTINCT 
			loanid 
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT
Left join (select requestid as loanid
				  ,request_timestamp
				  ,left(scoringscoreband,2) as scoringscoreband
				  ,scoringfinalriskscore
				  ,adjfinalriskscore
				  ,left(scoreband,2) as Scoreband
				  ,scoringpd
				  ,adjriskpd
				  ,uniqueid
				  ,rank () over (partition by requestid order by uniqueid desc) as latest
			from PRD_PRESS.CAPRI.CREDITRISK_RISKGROUP 
			where datediff(mm,request_timestamp,getdate()) < 2) as  b
		on cast(LNQT.loanid as varchar) = right(uniqueid,10)
		where b.latest = 1
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*
---------------------------------------------------------------------------------------
*/
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables_Scoreband')
 BEGIN
    DROP TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Scoreband
 END;

CREATE TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Scoreband

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

AS Select LNQT.Loanid,b.scoreband
				  ,b.transequence
				  ,b.transysteminstance
				  ,b.applicationdate
				  ,b.uniqueid
from (
		SELECT DISTINCT 
			loanid 
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT
Left join (Select tranappnumber as loanid
				  ,rank () over (partition by tranappnumber order by uniqueid desc) as latest
				  ,transequence
				  ,Scoreband
				  ,transysteminstance
				  ,uniqueid
				  ,applicationdate
			from PRD_PRESS.CAPRI.CAPRI_SCORING_RESULTS
			where datediff(mm,applicationdate,getdate()) < 2) as  b
		on cast(LNQT.loanid as varchar) = right(b.uniqueid,10)
		where b.latest = 1

	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*---------------------------Central Offers Temp tables------------------OFFERS */
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables_Offers')
 BEGIN
    DROP TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Offers
 END;

CREATE TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Offers

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

AS Select distinct LNQT.Loanid
				  ,b.uniqueid
				  ,b.maxcapoffer
				  ,b.Maxtermoffer
				  ,b.Maxloanoffer
				  ,b.maxcardoffer
				  ,b.creationmonth
from (
		SELECT DISTINCT 
			loanid 
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT
Left join (Select  loanid
				  ,uniqueid
				  ,rank () over (partition by loanid order by uniqueid desc) as latest
				  ,max(Totalcapital) as maxcapoffer
				  ,max(Term) as Maxtermoffer
				  ,max(Loancapital) as maxloanoffer
				  ,max(cardlimit) as Maxcardoffer
				  ,CAST(LEFT(CreationDate, 10) AS DATETIME) AS CreationDate
				  ,concat(left(cast(cast(LEFT(creationdate,10) as date)as varchar),4),substring(cast(cast(creationdate as date)as varchar),6,2)) as Creationmonth
			from PRD_Credit_Central.credcentral.VW_MASTER_OFFERS_TABLE
			Group by loanid,uniqueid,creationdate ) as  b
		on LNQT.loanid = b.loanid
		where b.latest = 1
		AND datediff(mm, b.creationdate,getdate()) < 2
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(

/*--------------------------------------------------------------------------Loan disbursals*/

USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables_Loandisbursed')
 BEGIN
    DROP TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Loandisbursed
 END;

CREATE TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Loandisbursed

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

AS Select LNQT.Loanid
				  ,b.uniqueid
				  ,b.creationdate
				  ,b.startdate
				  ,b.Startmonth
				  ,b.OPD 
				  ,b.CapitalinclSRA
				  ,b.CapitalexclSRA
				  ,b.Term
				  ,b.loanreference
				  ,b.Creationmonth
				  ,b.Productcategory
				  ,b.Productcategorydescription
from (
		SELECT DISTINCT 
			loanid 
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT
Left join (Select Loanid
				  ,Uniqueid
				  ,Rank () over (partition by loanid order by uniqueid desc) as latest
				  ,OPD 
				  ,CapitalinclSRA
				  ,CapitalexclSRA
				  ,Loanreference
				  ,Term
				  ,Productcategory
				  ,Productcategorydescription
				  ,Startdate
				  ,Left(startdate,6) as Startmonth
				  ,CAST(LEFT(CreationDate, 10) AS DATETIME) AS CreationDate
				  ,concat(left(cast(cast(LEFT(creationdate,10) as date)as varchar),4),substring(cast(cast(creationdate as date)as varchar),6,2)) as Creationmonth
			from PRD_Credit_Central.credcentral.VW_MASTER_DISBURSED_LOAN_TABLE 
			Group by loanid,uniqueid,creationdate,Startdate,Productcategory,Productcategorydescription,OPD,CapitalinclSRA,CapitalexclSRA,Term,loanreference ) as  b
		on LNQT.loanid = b.loanid
		where b.latest = 1
		and datediff(mm,b.creationdate,getdate()) < 2
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*---------------------------------------------------------------------------Card disbursals*/
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables_Carddisbursed')
 BEGIN
    DROP TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Carddisbursed
 END;

CREATE TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Carddisbursed

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

AS Select distinct LNQT.Loanid
				  ,b.Uniqueid
				  ,b.Creationdate
				  ,b.Limitincreasedate
				  ,b.Limitincreasemonth 
				  ,b.Startdate
				  ,b.Cardlimit
				  ,b.Accountnumber
				  ,b.creationmonth
				  ,b.Productcategory
				  ,b.Productcategorydescription
from (
		SELECT DISTINCT 
			loanid 
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT
Left join (Select loanid
				  ,uniqueid
				  ,rank () over (partition by loanid order by uniqueid desc) as latest
				  ,Cardlimit
				  ,Accountnumber
				  ,Limitincreasedate
				  ,Left(limitincreasedate,6) as Limitincreasemonth
				  ,CAST(LEFT(CreationDate, 10) AS DATETIME) AS CreationDate
				  ,Productcategory
				  ,Productcategorydescription
				  ,CONVERT(VARCHAR(10), Startdate, 112) as Startdate
				  ,concat(left(cast(cast(LEFT(creationdate,10) as date)as varchar),4),substring(cast(cast(creationdate as date)as varchar),6,2)) as Creationmonth
			from PRD_Credit_Central.credcentral.VW_MASTER_DISBURSED_CARD_TABLE  
			Group by Loanid,Uniqueid,Creationdate,Startdate,Productcategory,Productcategorydescription,Limitincreasedate,Cardlimit,Accountnumber ) as  b
		on LNQT.loanid = b.loanid
		where b.latest = 1
		and datediff(mm,b.creationdate,getdate()) < 2
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*------------Investigations---------DM_FLAG--------------------------------*/
USE CreditProfitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA+'.'+TABLE_NAME = 'dbo.KM_OMNI_DECLINES1_202006V2')
  BEGIN
    DROP TABLE dbo.KM_OMNI_DECLINES1_202006V2
  END;

CREATE TABLE dbo.KM_OMNI_DECLINES1_202006V2
WITH (DISTRIBUTION = HASH(APPLICATIONID), CLUSTERED COLUMNSTORE INDEX)
AS
Select Distinct applicationid
		 ,sequenceid
		 ,reasoncode
		 ,Declineaction
		 ,GROUPING 

From (Select
		 Distinct applicationid
		 ,sequenceid
		 ,reasoncode
		 ,Action as declineaction
		 ,GROUPING 
		 ,rank () over (partition by applicationid order by sequenceid asc) as latest
From Prd_exactussync.dbo.Applicationreasons
where DATEDIFF(mm, lastupdatetimestamp, getdate()) < 2
and (reasoncode is not null and reasoncode <> '') and (grouping not like 'docupl%'and reasoncode not like ('UP%')) AND  (action like 'Rej%' or action like 'DEcline%')) b
where b.latest = 1
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*---------------------------------------------------------------------------------------------
														--'Added OMNI Reasoncodes!!!!'
----------------------------------------------------------------------------------------------*/
USE CreditProfitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA+'.'+TABLE_NAME = 'dbo.KM_Appflow_TempTables_Scorecardversion')
  BEGIN
    DROP TABLE dbo.KM_Appflow_TempTables_Scorecardversion
  END;

CREATE TABLE dbo.KM_Appflow_TempTables_Scorecardversion
WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)
AS
Select Distinct LNQT.Loanid 
				,b.scorecardversion
		
From (
		SELECT DISTINCT 
			loanid 
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT
Left join 
	( Select Tranappnumber as loanid 
			,Uniqueid
			,scorecardversion
			,rank () over (partition by tranappnumber order by uniqueid desc) as latest
	from prd_press.capri.capri_testing_strategy_results
	where datediff(mm,applicationdate,getdate()) < 2) b
	On cast(LNQT.loanid as varchar) = right(uniqueid,10)
	where b.latest = 1 
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*---------------------Offer Call Number--------------------------------------------*/
USE CreditProfitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA+'.'+TABLE_NAME = 'dbo.KM_Appflow_TempTables_OfferCallNumber')
  BEGIN
    DROP TABLE dbo.KM_Appflow_TempTables_OfferCallNumber
  END;

CREATE TABLE dbo.KM_Appflow_TempTables_OfferCallNumber
WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)
AS
Select Distinct LNQT.Loanid 
				,b.Call_Number
				,b.uniqueid
		
From (
		SELECT DISTINCT 
			loanid 
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT
Left join 
	( Select Tranappnumber as loanid 
			,Callnumber as Call_Number
			,Uniqueid
			,rank () over (partition by tranappnumber order by uniqueid desc) as latest
	from prd_press.capri.capri_applicant
	where datediff(mm,applicationdate,getdate()) < 2) b
	On convert(varchar,LNQT.loanid) = convert(varchar,b.loanid)
	where b.latest = 1 
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*---------------------OMNI Routing Indicator------------------------------------------*/

USE CreditProfitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA+'.'+TABLE_NAME = 'dbo.KM_Appflow_TempTables_OMNI_RoutingIndicator')
  BEGIN
    DROP TABLE dbo.KM_Appflow_TempTables_OMNI_RoutingIndicator
  END;

CREATE TABLE dbo.KM_Appflow_TempTables_OMNI_RoutingIndicator
WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)
AS
Select Distinct LNQT.Loanid 
				,b.Indicatorvalue
				,b.Lastupdate_date
		
From (
		SELECT DISTINCT 
			loanid 
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT
Left join 
	( Select Distinct Applicationid as loanid 
			,Indicatorvalue
			,CONVERT(VARCHAR(10), Lastupdatetimestamp, 112) as Lastupdate_date
	from prd_exactussync.dbo.Applicationindicators
	where Indicatorcode = 'ROUTIND' and datediff(mm,Lastupdatetimestamp,getdate()) < 2) b
	On LNQT.loanid = b.Loanid

	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*-----------------*/

USE CreditProfitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA+'.'+TABLE_NAME = 'dbo.KM_Appflow_TempTables_Gazelle_RoutingIndicator')
  BEGIN
    DROP TABLE dbo.KM_Appflow_TempTables_Gazelle_RoutingIndicator
  END;

CREATE TABLE dbo.KM_Appflow_TempTables_Gazelle_RoutingIndicator
WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)
AS
Select Distinct LNQT.Loanid 
				,b.Routetooffer
				,b.Lastupdate_date
		
From (
		SELECT DISTINCT 
			loanid 
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT
Left join 
	( Select Distinct loanid 
			,Routetooffer
			,Lastupdatedate as Lastupdate_date
	from ARC_LOANQ_2028.dbo.loanroutingtable
	where left(CONVERT(VARCHAR, LASTUPDATEdate, 112),6) >= '202001') b 
	On LNQT.loanid = b.Loanid
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------Loanq offers (Gazelle) ------------------------------------------------------------------------------
*/
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables2')
  BEGIN
    DROP TABLE dbo.KM_Appflow_TempTables2
  END;

CREATE TABLE dbo.KM_Appflow_TempTables2

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

as select	Distinct A.Loanid,
			MAX(A.Term)       AS MaxTermOffer,
			MAX(A.Capital)    AS MaxCapOffer,     
			MAX(CASE WHEN (B.PRODUCTTYPE IS NULL AND A.PRODUCTcode NOT LIKE '%LIMIT%')
							OR B.PRODUCTTYPE IN (2,3) THEN A.CreditCardLimit ELSE 0 END)  AS MaxCard  ,
			MAX(CASE WHEN (B.PRODUCTTYPE IS NULL AND A.PRODUCTcode   LIKE '%LIMIT%') 
							OR B.PRODUCTTYPE IN (4,5) THEN A.CreditCardLimit ELSE 0 END)  AS MaxCardLimitIncrease
FROM
	(SELECT 	LOANID,
				PRODUCTcode,
				CreditCardLimit,
				Term, 
				Capital,
				ApplicationType
	from ARC_LOANQ_2028.dbo.LOANQUOTATIONOFFERSPRESENTED
	where DATEDIFF(mm, create_date, getdate()) < 2
	) as A

left join 
	(select		ProductCategory,
				PRODUCTTYPE
	from PRD_ExactusSync.dbo.ProductCategory) as B 
ON A.ApplicationType = CAST(B.ProductCategory AS VARCHAR)
GROUP BY a.LoanID 
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*
---------Product Category from Loanq Source (Gazelle)----------------------------------------------------------
*/
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables_Prod')
  BEGIN
    DROP TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Prod
  END;

CREATE TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Prod

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

as select	  distinct LNQT.loanid
			, LQA.ProductCategory as a
			, Prod.ProductCategory as b
			, case when LQA.ProductCategory is not null then LQA.ProductCategory else Prod.ProductCategory end as ProductCategory

	from (
		SELECT DISTINCT loanid 
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT

LEFT JOIN (
		SELECT LoanID
		, ProductCategory
		, ProductType
	FROM ARC_LOANQ_2028.dbo.LoanquotationAccount
	where DATEDIFF(mm, create_date, getdate()) < 2
	or DATEDIFF(mm, MODIFIED_DATE, getdate()) < 2
		) AS LQA
ON LNQT.LoanId = LQA.LoanID

left join (
	select b.ProductCategory, a.loanid, a.Max_Offer_data, a.offerselecteddataid
	from
		(select offerselecteddataid
		, ProductCategory
		from ARC_LOANQ_2028.dbo.LOANQUOTATIONOFFERSELECTED
		where DATEDIFF(mm, convert(datetime, convert(varchar ,offerdate, 112), 112), getdate()) < 2 and offerdate > 0
		) as b
	inner join
		(select rank() over (partition by loanid order by offerselecteddataid desc ) as Max_Offer_data
		, loanid
		, offerselecteddataid
		from ARC_LOANQ_2028.dbo.LoanQOfferSelecteddata
		) as a
		on b.offerselecteddataid = a.offerselecteddataid
		where 
			a.Max_Offer_data = '1'
		) as Prod
		on LNQT.LoanId = Prod.loanid
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*
--------------------------------------Decline Reasons Temp table-----------------------------------------------------------------------
*/
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables_Decline')
  BEGIN
    DROP TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Decline
  END;

CREATE TABLE Creditprofitability.dbo.KM_Appflow_TempTables_decline

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

as select	max.Loanid,
			max.Max_Reasoning,
			REA.reasoncode,
			REA.Action
from (
		select 	Loanid,
				Max(LOANQUOTATIONREASONSID) as Max_Reasoning				
		from ARC_LOANQ_2028.dbo.LoanquotationReasons 
		where DATEDIFF(mm, create_date, getdate()) < 2
		group by loanid) 
		as Max

inner join (
		select	distinct loanid,
				LOANQUOTATIONREASONSID,
				reasoncode,
				action
		from ARC_LOANQ_2028.dbo.LoanquotationReasons 
		where DATEDIFF(mm, create_date, getdate()) < 2
		) 
		as REA
		ON MAX.LOANID = REA.LOANID
		WHERE MAX.Max_Reasoning = rea.LOANQUOTATIONREASONSID

	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*
-------------------------Reasoncode1  Field (LoanqSource ------------------------------------------
*/
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables_Reasoncode1')
  BEGIN
    DROP TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Reasoncode1
  END;

CREATE TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Reasoncode1

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

as select	  distinct LNQT.loanid, LNQT.scoremodel,RSCD.DeclineCode  as ReasonCode1
	from (
		SELECT DISTINCT loanid 
			, status 
			, scoremodel
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT

LEFT JOIN (
		select b.*
		from
		(select  rank() over (partition by a.loanid order by a.LOANQUOTATIONEXCEPTIONID) as Max_ID, a.*
		from (select distinct loanid
			, LOANQUOTATIONEXCEPTIONID
			, case when reasoncode in ('LQR22','LQR17','LQR20','LQR16','LQR15','LQR01','LQR23','LQR24')
								then reasoncode end as reasoncode1_PreBureau
			, case when (reasoncode in ('ds01','ds02')) or (reasoncode like ('D%') 
														and reasoncode not like ('DS%') 
														and reasoncode not like ('DAR%') 
														and reasoncode not like ('DAW%'))
								then reasoncode end as reasoncode3_PreBureau
			, case when reasoncode in ('DS64','DS74','DS78','DS79','DS777','DS778','DS783','DS784','DS785','DS779','DS786','DS780','DS787',
'DS788','DS789','DS790','DS791','DS792','DS793','DS794','DS795','DS796','DS797','DS798','DS799','DS991','DS992','DS993','DS994','DS995','DS996') 
								then reasoncode end as reasoncode5_Score
			, case when reasoncode like ('DS%')
								and reasoncode <> 'ds64'
								and reasoncode <> 'ds01'
								and reasoncode <> 'ds02'
								and reasoncode <> 'ds74'
								and reasoncode <> 'ds78'
								and reasoncode <> 'ds79'
								and reasoncode <> 'ds777'
								and reasoncode <> 'ds778'
								and reasoncode <> 'ds780'
								and reasoncode <> 'ds783'
								and reasoncode <> 'ds784'
								and reasoncode <> 'ds785'
								and reasoncode <> 'ds786'
								and reasoncode <> 'ds787'
								and reasoncode <> 'ds788'
								and reasoncode <> 'ds789'
								and reasoncode <> 'ds790'
								and reasoncode <> 'ds791'
								and reasoncode <> 'ds792'
								and reasoncode <> 'ds793'
								and reasoncode <> 'ds794'
								and reasoncode <> 'ds795'
								and reasoncode <> 'ds796'
								and reasoncode <> 'ds797'
								and reasoncode <> 'ds797'
								and reasoncode <> 'ds798'
								and reasoncode <> 'ds799'
								and reasoncode <> 'ds991'
								and reasoncode <> 'ds992'
								and reasoncode <> 'ds993'
								and reasoncode <> 'ds994'
								and reasoncode <> 'ds995'
								and reasoncode <> 'ds996'
								then reasoncode end as reasoncode7_PostBureau
			, case when reasoncode like ('AFR%')
								and left(reasoncode,1) <> 'd'
								then reasoncode end as reasoncode9_Aford
			, case when reasoncode in ('ns01', 'ns02')
								and actioncode like ('rej%')
								then reasoncode end as reasoncode11_NS
			, case when reasoncode in ('LQR02')
								then reasoncode end as reasoncode13_WalkAway
			, case when reasoncode in ('MAN','FRR01','FRR02','FRR05','FRR12','FRS01','FRS02','FRS04','FRS05','CFR06','CRE30','CRE31','AFS02')
								OR reasoncode like ('AFF%')
								OR reasoncode like ('CE%')
								OR reasoncode like ('MR%')
								then 'PQAFR' end as reasoncode15_AffordAfterQ
			, case when reasoncode like ('OS%')
								then reasoncode end as reasoncode17_OS
		from ARC_LOANQ_2028.dbo.Loanquotationexception
		where DATEDIFF(mm, create_date, getdate()) < 2) as a
	where a.reasoncode1_PreBureau is not Null
	or a.reasoncode3_PreBureau is not Null
	or a.reasoncode5_Score is not Null
	or a.reasoncode7_PostBureau is not Null
	or a.reasoncode9_Aford is not Null
	or a.reasoncode11_NS is not Null
	or a.reasoncode13_WalkAway is not Null
	or a.reasoncode15_AffordAfterQ is not Null
	or a.reasoncode17_OS is not Null
	) as b
	where b.Max_ID = '1' 
	) as exception
	ON LNQT.LoanId = exception.loanid

left join (select TranappNumber, DeclineCode 
			from  Creditprofitability.dbo.KM_Appflow_WaterFall) as RSCD
on RSCD.TranappNumber = LNQT.LoanId
	

left join (
	select b.*
		from
		(select  rank() over (partition by a.loanid order by a.LOANQUOTATIONreasonsID) as Max_ID, a.*
		from (select distinct loanid
		, LOANQUOTATIONreasonsID
		, case when reasoncode in ('LQR22','LQR17','LQR20','LQR16','LQR15','LQR01','LQR23','LQR24')
								then reasoncode end as reasoncode2_PreBureau
		, case when (reasoncode in ('ds01','ds02')) or (reasoncode like ('D%') 
														and reasoncode not like ('DS%') 
														and reasoncode not like ('DAR%') 
														and reasoncode not like ('DAW%'))
								then reasoncode end as reasoncode4_PreBureau
		, case when reasoncode in ('DS64','DS74','DS78','DS79','DS777','DS778','DS783','DS784','DS785','DS779','DS786','DS780','DS787',
'DS788','DS789','DS790','DS791','DS792','DS793','DS991','DS992','DS993','DS994','DS995','DS996') then reasoncode end as reasoncode6_Score
		, case when reasoncode like ('DS%')
								and reasoncode <> 'ds64'
								and reasoncode <> 'ds01'
								and reasoncode <> 'ds02'
								and reasoncode <> 'ds74'
								and reasoncode <> 'ds78'
								and reasoncode <> 'ds79'
								and reasoncode <> 'ds777'
								and reasoncode <> 'ds778'
								and reasoncode <> 'ds780'
								and reasoncode <> 'ds783'
								and reasoncode <> 'ds784'
								and reasoncode <> 'ds785'
								and reasoncode <> 'ds786'
								and reasoncode <> 'ds787'
								and reasoncode <> 'ds788'
								and reasoncode <> 'ds789'
								and reasoncode <> 'ds790'
								and reasoncode <> 'ds791'
								and reasoncode <> 'ds792'
								and reasoncode <> 'ds793'
								and reasoncode <> 'ds794'
								and reasoncode <> 'ds795'
								and reasoncode <> 'ds796'
								and reasoncode <> 'ds797'
								and reasoncode <> 'ds797'
								and reasoncode <> 'ds798'
								and reasoncode <> 'ds799'
								and reasoncode <> 'ds991'
								and reasoncode <> 'ds992'
								and reasoncode <> 'ds993'
								and reasoncode <> 'ds994'
								and reasoncode <> 'ds995'
								and reasoncode <> 'ds996'
								then reasoncode end as reasoncode8_PostBureau
		, case when reasoncode like ('AFR%')
								and left(reasoncode,1) <> 'd'
								then reasoncode end as reasoncode10_Aford
		, case when reasoncode in ('ns01', 'ns02')
								and action like ('rej%')
								then reasoncode end as reasoncode12_NS
		, case when reasoncode in ('LKQR02')
								then reasoncode end as reasoncode14_WalkAway
		, case when reasoncode in ('MAN','FRR01','FRR02','FRR05','FRR12','FRS01','FRS02','FRS04','FRS05','CFR06','CRE30','CRE31','AFS02')
								OR reasoncode like ('AFF%')
								OR reasoncode like ('CE%')
								OR reasoncode like ('MR%')
								then 'PQAFR' end as reasoncode16_AffordAfterQ
		, case when reasoncode like ('OS%')
								then reasoncode end as reasoncode18_OS
	from ARC_LOANQ_2028.dbo.LoanquotationReasons
	where DATEDIFF(mm, create_date, getdate()) < 2) as a
	where a.reasoncode2_PreBureau is not Null
	or a.reasoncode4_PreBureau is not Null
	or a.reasoncode6_Score is not Null
	or a.reasoncode8_PostBureau is not Null
	or a.reasoncode10_Aford is not Null
	or a.reasoncode12_NS is not Null
	or a.reasoncode14_WalkAway is not Null
	or a.reasoncode16_AffordAfterQ is not Null
	or a.reasoncode18_OS is not Null
	) as b
	where b.Max_ID = '1'
	) as reasons	
	ON LNQT.LoanId = reasons.loanid


UPDATE a
SET reasoncode1 = case when a.scoremodel = 'PRB' then 'PRB' else a.reasoncode1 end
FROM KM_Appflow_TempTables_Reasoncode1 as a
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*
	-----------------------------------------------Qrejects -------------------------------------
*/
	USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables_Qrejects')
  BEGIN
    DROP TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Qrejects
  END;

CREATE TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Qrejects

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

as select	  distinct LNQT.loanid
			, case when LNQT.Loanid = QueRej.loanid then 'QRej' end as DeclineAction

	from (
		SELECT DISTINCT 
			loanid 
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT

inner join (
		select ApplicationID AS loanid
		from Prd_exactussync.dbo.Applicationreasons
		where DATEDIFF(mm, LEFT(LASTUPDATETIMESTAMP, 10), getdate()) < 2
			and reasoncode in ('FRR01','FRR02','FRR05','FRR12','FRR13','FRS01','FRS02','FRS04','FRS05') 
			or reasoncode in ('CFR06','CRE30','CRE31','AFS02')
			or REASONCODE IN ('D599','D850','DS64','DS80','DS81','DS83','AVSR1','AVSR2','CNQ01','REA02')
			) as QueRej
			on LNQT.Loanid = QueRej.loanid
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*
-----------Man_Rev Manager Review Queue--------------------------------------------
*/
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables_Man_Rev')
  BEGIN
    DROP TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Man_Rev
  END;

CREATE TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Man_Rev

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

as select	  distinct LNQT.loanid
			, case when LNQT.Loanid = ManRev.loanid then '1' else '0' end as Man_Review

	from (
		SELECT DISTINCT 
			loanid 
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT

left join (
		select loanid
		from ARC_LOANQ_2028.dbo.Loanquotationexception
		where DATEDIFF(mm, create_date, getdate()) < 2
		and (reasoncode like ('mr%') or reasoncode like ('man'))
			) as ManRev
			on LNQT.Loanid = ManRev.loanid
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*
----------------------Employer_Conf (Complete)------------------------
*/
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables_Emp')
  BEGIN
    DROP TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Emp
  END;

CREATE TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Emp

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

as select	  distinct LNQT.loanid
			, case when LNQT.Loanid = EMP.loanid then '1' else '0' end as Employer_Conf

	from (
		SELECT DISTINCT 
			loanid 
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT

left join (
		select loanid
		from ARC_LOANQ_2028.dbo.Loanquotationexception
		where DATEDIFF(mm, create_date, getdate()) < 2
		and reasoncode like ('EM%')
			) as EMP
			on LNQT.Loanid = EMP.loanid
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*
---------------------Cred_Ex Queue ------------------------
*/
	USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables_Cred_ex')
  BEGIN
    DROP TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Cred_ex
  END;

CREATE TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Cred_ex

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

as select	  distinct LNQT.loanid
			, case when LNQT.Loanid = Cred_ex.loanid then '1' else '0' end as Credit_ex

	from (
		SELECT DISTINCT 
			loanid 
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT

left join (
		select loanid
		from ARC_LOANQ_2028.dbo.Loanquotationexception
		where DATEDIFF(mm, create_date, getdate()) < 2
		and ((reasoncode like ('C%') and reasoncode not like ('CAQ%'))
			or reasoncode in ('avs01', 'avs02'))
			) as Cred_ex
			on LNQT.Loanid = Cred_ex.loanid
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*
--------------------Contactability Queue -------------------------------------------------------------------
*/
	USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables_Contact')
  BEGIN
    DROP TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Contact
  END;

CREATE TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Contact

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

as select	  distinct LNQT.loanid
			, case when LNQT.Loanid = Contact.loanid then '1' else '0' end as Contactability

	from (
		SELECT DISTINCT 
			loanid 
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT

left join (
		select loanid
		from ARC_LOANQ_2028.dbo.Loanquotationexception
		where DATEDIFF(mm, create_date, getdate()) < 2
		and reasoncode like ('CAQ%')
			) as Contact
			on LNQT.Loanid = Contact.loanid
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*
------------------------------------------------------------Fraud Queue -----------------------------------------------------------------
*/
	USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables_Fraud')
  BEGIN
    DROP TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Fraud
  END;

CREATE TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Fraud

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

as select	  distinct LNQT.loanid
			, case when LNQT.Loanid = Fraud.loanid then '1' else '0' end as Fraud_Settlement_ex

	from (
		SELECT DISTINCT 
			loanid 
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT

left join (
		select loanid
		from ARC_LOANQ_2028.dbo.Loanquotationexception
		where DATEDIFF(mm, create_date, getdate()) < 2
		and (reasoncode like 'F%' or reasoncode like 'se%')
			) as Fraud
			on LNQT.Loanid = Fraud.loanid
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*
--------------------------------------------AFF Affordability Queue ---------------------------------------------------------
*/
	USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables_AFF')
  BEGIN
    DROP TABLE Creditprofitability.dbo.KM_Appflow_TempTables_AFF
  END;

CREATE TABLE Creditprofitability.dbo.KM_Appflow_TempTables_AFF

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

as select	  distinct LNQT.loanid
			, case when LNQT.Loanid = AFF.loanid then '1' else '0' end as Affordability_Q

	from (
		SELECT DISTINCT 
			loanid 
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT

left join (
		select loanid
		from ARC_LOANQ_2028.dbo.Loanquotationexception
		where DATEDIFF(mm, create_date, getdate()) < 2
		and reasoncode like ('AFF%')
			) as AFF
			on LNQT.Loanid = AFF.loanid

	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*
--------------------------------------------Front Offer vs Decline pt1 -----------------------------
*/
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables_FOvD1')
  BEGIN
    DROP TABLE Creditprofitability.dbo.KM_Appflow_TempTables_FOvD1
  END;

CREATE TABLE Creditprofitability.dbo.KM_Appflow_TempTables_FOvD1

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

as select	  distinct LNQT.loanid
			, LOT.LastOfferTime
			, LDT.LastDeclineTime
			, LRT.LastDeclineTime1
			, EXP.lastAffordibilityDecline
			, REA.lastAffordibilityDecline1

	from (
		SELECT DISTINCT 
			loanid 
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT

left join (
		select a.*
	from 
		(select rank() over (partition by Loanid order by Modified_date desc) as Max_Date
		 , loanid
		 , Modified_date as LastOfferTime
		from ARC_LOANQ_2028.dbo.loanquotationOffersPresented
		where DATEDIFF(mm, CREATE_DATE, getdate()) < 2) 
	as a
	where Max_Date = '1'
			) as LOT
			on LNQT.loanid = LOT.loanid

left join (
		select a.*
	from 
		(select rank() over (partition by Loanid order by Modified_date desc) as Max_Date
		 , loanid
		 , Modified_date as LastDeclineTime
		from ARC_LOANQ_2028.dbo.Loanquotationexception
		where DATEDIFF(mm, Modified_date, getdate()) < 2 
				and reasoncode in ('LQR22','LQR17','LQR20','LQR16','LQR15','LQR01','LQR02')
				OR (reasoncode like ('D%') or reasoncode like ('DS%'))
				and reasoncode not like ('DAR%')
		)as a
	where Max_Date = '1'
			) as LDT
			on LNQT.loanid = LDT.loanid

left join (
		select a.*
	from 
		(select rank() over (partition by Loanid order by Modified_date desc) as Max_DateReasons
		 , loanid
		 , Modified_date as LastDeclineTime1
		from ARC_LOANQ_2028.dbo.LoanquotationReasons
		where DATEDIFF(mm, Modified_date, getdate()) < 2
				and reasoncode in ('LQR22','LQR17','LQR20','LQR16','LQR15','LQR01','LQR02')
				OR (reasoncode like ('D%') or reasoncode like ('DS%'))
				and reasoncode not like ('DAR%')
		)as a
	where Max_DateReasons = '1'
			) as LRT
			on LNQT.loanid = LRT.loanid

left join (
		select a.*
	from 
		(select rank() over (partition by Loanid order by Modified_date desc) as Max_Date
		 , loanid
		 , Modified_date as lastAffordibilityDecline
		from ARC_LOANQ_2028.dbo.Loanquotationexception
		where DATEDIFF(mm, Modified_date, getdate()) < 2
				and reasoncode like ('AFR%')
		)as a
	where Max_Date = '1'
			) as EXP
			on LNQT.loanid = EXP.loanid

left join (
		select a.*
	from 
		(select rank() over (partition by Loanid order by Modified_date desc) as Max_DateReasons
		 , loanid
		 , Modified_date as lastAffordibilityDecline1
		from ARC_LOANQ_2028.dbo.LoanquotationReasons
		where DATEDIFF(mm, Modified_date, getdate()) < 2
				and reasoncode like ('AFR%')
		)as a
	where Max_DateReasons = '1'
			) as REA
			on LNQT.loanid = REA.loanid

	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*
---------------------------------------------Front Offer vs Decline pt2 --------------------------------------------------------------------
*/
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables_FOvD2')
  BEGIN
    DROP TABLE Creditprofitability.dbo.KM_Appflow_TempTables_FOvD2
  END;

CREATE TABLE Creditprofitability.dbo.KM_Appflow_TempTables_FOvD2

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

as select	  distinct LNQT.loanid
			, FrontOfferVsDecline
			, LastOfferTime
			

	from (
		SELECT loanid
			, LastOfferTime					
			, LastDeclineTime				
			, LastDeclineTime1				
			, lastAffordibilityDecline		
			, lastAffordibilityDecline1		
			, case when LastOfferTime > isnull(LastDeclineTime			, '2006-06-25  11:38:22.900')
					and LastOfferTime > isnull(LastDeclineTime1			, '2006-06-25  11:38:22.900')
					and LastOfferTime > isnull(lastAffordibilityDecline	, '2006-06-25  11:38:22.900')
					and LastOfferTime > isnull(lastAffordibilityDecline1, '2006-06-25  11:38:22.900')
				then 'O' else 'D'
				end as FrontOfferVsDecline
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_FOvD1
		) AS LNQT
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*
----------------------------------------------------------User_Cancel ------------------------------------------------------------
*/
	USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables_User_Cancel')
  BEGIN
    DROP TABLE Creditprofitability.dbo.KM_Appflow_TempTables_User_Cancel
  END;

CREATE TABLE Creditprofitability.dbo.KM_Appflow_TempTables_User_Cancel 

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

as select	  distinct LNQT.loanid
			, case when LNQT.Loanid = UserCan.loanid then '1' else '0' end as User_Cancel 

	from (
		SELECT DISTINCT 
			loanid 
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT

left join (
		select loanid
		from ARC_LOANQ_2028.dbo.LoanquotationReasons
		where DATEDIFF(mm, Modified_date, getdate()) < 2
			and GROUPING in ('USER','CUST') 
			and action like 'can' 
			and reasoncode in ('RRO01','RRO02','RRO03')
		)as UserCan
on LNQT.loanid = UserCan.loanid 
/*
----------------------------------------------------PRECAPRIREASON-------------------------------------------------------------
*/
	USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables_PRECAPRIREASON')
  BEGIN
    DROP TABLE Creditprofitability.dbo.KM_Appflow_TempTables_PRECAPRIREASON
  END

CREATE TABLE Creditprofitability.dbo.KM_Appflow_TempTables_PRECAPRIREASON

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

as select	  distinct LNQT.loanid
			, CASE WHEN B.REASONCODE ='LQR15' THEN 'ADMIN'
				WHEN B.REASONCODE ='LQR16' THEN 'Sequestration'
				WHEN B.REASONCODE ='LQR17' THEN 'DEBT_COUN'
				WHEN B.REASONCODE ='LQR20' THEN 'CONSENTtoCREDIT'
				WHEN B.REASONCODE ='LQR22' THEN 'SALARYnotBANK' 
				ELSE 'UNKNOWN' END 
				as PRECAPRIREASON

	from (
		SELECT DISTINCT 
			loanid 
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT

left join (
		SELECT loanID 
				, REASONCODE
		FROM ARC_LOANQ_2028.dbo.Loanquotationexception
		WHERE DATEDIFF(mm, Modified_date, getdate()) < 2 
		AND REASONCODE IN ('LQR15', 'LQR16', 'LQR17', 'LQR22', 'LQR20')
		) as B
		ON LNQT.loanid = B.LOANID 
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*
-----------------------------------------------------------------------Rejected_Queue----------------------------------------------------------------------------
*/

USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables_Rejected_Queue')
  BEGIN
    DROP TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Rejected_Queue
  END;

CREATE TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Rejected_Queue

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

as select distinct LNQT.loanid
			, case	when b.GROUPING like 'CREDITEXCEPTION%' then 'CreditEx'
					when b.GROUPING like 'EMPLOYERCONFIRMATION' then 'EmployerConf'
					when b.GROUPING like 'CONTACTABILITY' then 'Contactability'
					when b.Grouping like 'MReview' or Grouping like 'MANAGERREVIEWQ' then 'Manager'
					when b.grouping like  'ASSESSMENTEXCEPTIONQ' then 'Fraud'
					when b.GROUPING like 'affordability' then 'Affordability'
					when b.reasoncode in ('FRR01','FRR02','FRR05','FRR12','FRR13','FRS01','FRS02','FRS04','FRS05') then 'Fraud'
					when b.reasoncode in ('CFR06','CRE30','CRE31','AFS02') then 'Other' else null end
					as Rejected_Queue 
			, b.reasoncode
			, b.latest_reason
	from (
		SELECT DISTINCT 
			loanid 
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT

left join (
		select loanid
			, reasoncode
			, grouping
			, action
			, LOANQUOTATIONREASONSID
			,rank() over (partition by Loanid order by LOANQUOTATIONREASONSID desc) as latest_reason
		from ARC_LOANQ_2028.dbo.LoanquotationReasons
		where DATEDIFF(mm, Modified_date, getdate()) < 2
		) as b
		on LNQT.loanid = B.loanid
		where b.action like ('rej%') 
		and (b.GROUPING like 'CREDITEXCEPTION%' 
			or b.GROUPING like'EMPLOYERCONFIRMATION' 
			or b.GROUPING like 'CONTACTABILITY' 
			or b.grouping like  'MREVIEW'  
			or Grouping like 'MANAGERREVIEWQ' 
			or Grouping like 'ASSESSMENTEXCEPTIONQ' 
			or b.GROUPING like 'affordability%' 
			or b.reasoncode  in ('FRR01','FRR02','FRR05','FRR12','FRS01','FRS02','FRS04','FRS05') 
			or b.reasoncode in ('CFR06','CRE30','CRE31','AFS02')) 
			and b.latest_reason = 1
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
											-------------------OFFER RESULTS TABLE -------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
*/
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_TempTables_Offer_results')
 BEGIN
    DROP TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Offer_results
 END;

CREATE TABLE Creditprofitability.dbo.KM_Appflow_TempTables_Offer_results

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

AS Select distinct LNQT.Loanid
				  ,b.uniqueid
				  ,b.maxcapoffer
				  ,b.Maxtermoffer
				  ,b.Maxloanoffer
				  ,b.maxcardoffer
				
from (
		SELECT DISTINCT 
			loanid 
		FROM Creditprofitability.dbo.KM_Appflow_TempTables_Base
		) AS LNQT
Left join (Select  tranappnumber as loanid 
				  ,uniqueid
				  ,rank () over (partition by tranappnumber order by uniqueid desc) as latest
				  ,max(Totalcapital) as maxcapoffer
				  ,max(Term) as Maxtermoffer
				  ,max(Loancapital) as maxloanoffer
				  ,max(cardlimit) as Maxcardoffer
			from Prd_press.capri.capri_offer_results
			where datediff(mm,Applicationdate,getdate()) < 2 
			Group by tranappnumber,uniqueid,Applicationdate ) as  b
		on cast(LNQT.loanid as varchar) = b.loanid 
		where b.latest = 1

	) by APS;
	DISCONNECT FROM APS;
quit;
/*The End of script 1*/



/*script 2*/
proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_CreditAppflow1')
 BEGIN
    DROP TABLE Creditprofitability.dbo.KM_CreditAppflow1
 END;

CREATE TABLE Creditprofitability.dbo.KM_CreditAppflow1

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

as 

Select 
C.*
,reasoncode.reasoncode1
,D.declineaction
,E.failed_affordability
,case when f.Scoringscoreband is null and C.scorecard = 'v5' then F.scoreband else F.scoringscoreband end as Scoringscoreband
,G.rejected_queue
,Case when (I.CallCentre_DM_Flag  = 1 or    
		   I.SMSLeads_DM_Flag  = 1  or   
		   I.DM_MailerDate_DM_Flag = 1 or 
		   I.BranchLMS_DM_Flag = 1 or 
		   I.Email_Tracker_DM_Flag = 1 ) then 1 else 0 end as DM_Flag
from (select distinct Apps.loanid,
Apps.idnumber,
Apps.Clientnumber,
Apps.uniqueid,
Apps.creationdate,
Apps.Branchcode,
Apps.origination_source,
Apps.Sourcesystem,
Apps.status,
Apps.uniqueindicator,
Apps.finaluniqueid,
Apps.scorecard,
Apps.wagefreq,
Apps.bank,
Apps.InCapri,
Apps.Gazelle_wa,
Apps.Transequence,
Apps.Scoreband,
Apps.Creationmonth,
Apps.Subgroupcode,
Apps.Occupationstatus,
Apps.Employmenttype,
Apps.Persal,
Apps.Inseconds,
Apps.Transysteminstance,
Apps.Scoremodel,
Apps.Calculatednetincome
,sum(case when OFFERS.loanid is not null then 1 else 0 end) as Gotoffer
,Sum(case when maxcardoffer > 0.00 and maxcardoffer is not null then 1 else 0 end) as Gotcardoffer
,sum(case when LOANDISB.loanid is not null then 1 else 0 end) as Loan_disbursed
,sum(case when CARDDISB.loanid is not null then 1 else 0 end) as Card_disbursed
,Offers.Maxcapoffer
,Offers.Maxtermoffer
,Offers.Maxcardoffer
,Offers.Maxloanoffer
,LOANDISB.Loanreference
,LOANDISB.OPD
,LOANDISB.CapitalinclSRA
,LOANDISB.CapitalexclSRA
,LOANDISB.Term
,LOANDISB.Startdate
,LOANDISB.Startmonth
,Case when LOANDISB.loanid is not null then LOANDISB.Productcategory 
when LOANDISB.loanid is null and CARDDISB.loanid  is not null then CARDDISB.Productcategory end as Productcategory
,Case when LOANDISB.loanid is not null then LOANDISB.Productcategorydescription 
when LOANDISB.loanid is null and CARDDISB.loanid  is not null then CARDDISB.Productcategorydescription end as Productcategorydescription
,CARDDISB.Cardlimit
,CARDDISB.Limitincreasedate
,CARDDISB.Limitincreasemonth
,CARDDISB.Accountnumber
from Creditprofitability.dbo.KM_Appflow_TempTables_Base  as Apps
Left join (Select * from Creditprofitability.dbo.KM_Appflow_TempTables_Offers) as  OFFERS
on Apps.loanid = offers.loanid
left join (Select * from Creditprofitability.dbo.KM_Appflow_TempTables_Carddisbursed) as CARDDISB
on Apps.loanid = carddisb.loanid
Left join (select * from Creditprofitability.dbo.KM_Appflow_TempTables_Loandisbursed) as  LOANDISB
on Apps.loanid = loandisb.loanid
Group by Apps.loanid
,Apps.Idnumber
,Apps.Clientnumber
,Apps.Uniqueid
,Apps.creationdate
,Apps.Branchcode
,Apps.Origination_source
,Apps.Sourcesystem
,Apps.Status
,Apps.Uniqueindicator
,Apps.Finaluniqueid
,Apps.Scorecard
,Apps.Wagefreq
,Apps.Bank
,Apps.InCapri
,Apps.Gazelle_wa
,Apps.Transequence
,Apps.Scoreband
,Apps.Creationmonth
,Apps.Subgroupcode
,Apps.Occupationstatus
,Apps.Employmenttype
,Apps.Persal
,Apps.Inseconds
,Apps.Transysteminstance
,Apps.Scoremodel
,Apps.Calculatednetincome
,Offers.Maxcapoffer
,Offers.Maxtermoffer
,Offers.Maxcardoffer
,Offers.Maxloanoffer
,LOANDISB.Loanid
,LOANDISB.Loanreference
,LOANDISB.OPD
,LOANDISB.CapitalinclSRA
,LOANDISB.CapitalexclSRA
,LOANDISB.Term
,LOANDISB.Startdate
,LOANDISB.Startmonth
,LOANDISB.Productcategory
,LOANDISB.Productcategorydescription
,CARDDISB.Loanid
,CARDDISB.Cardlimit    
,CARDDISB.Limitincreasedate
,CARDDISB.Limitincreasemonth
,CARDDISB.Accountnumber
,CARDDISB.Productcategory
,CARDDISB.Productcategorydescription)  c 
left join (Select loanid, Reasoncode1 from creditprofitability.dbo.KM_Appflow_TempTables_Reasoncode1)  ReasonCode
on c.LoanId = ReasonCode.loanid
left join Creditprofitability.dbo.KM_Appflow_TempTables_Qrejects D
on c.loanid = d.loanid
Left join Creditprofitability.dbo.KM_Appflow_TempTables_Affordabilityindicator E
on c.loanid = e.loanid
Left join Creditprofitability.dbo.KM_Appflow_TempTables_Scoringscoreband F
on c.uniqueid = F.uniqueid
Left join Creditprofitability.dbo.KM_Appflow_TempTables_Rejected_Queue G
on c.loanid = G.loanid 
Left join Creditprofitability.dbo.KM_Appflow_TempTables_DMFLAG I
on c.loanid =I.loanid
left join creditprofitability.dbo.KM_Appflow_TempTables_BehaveScore_2 H
on cast(c.loanid as varchar) = H.Tranappnumber
Group by c.loanid
,c.idnumber
,c.Clientnumber
,c.uniqueid
,c.creationdate
,c.origination_source
,C.Sourcesystem
,c.Branchcode
,c.status
,c.uniqueindicator
,c.finaluniqueid
,c.scorecard
,c.wagefreq
,c.bank
,c.InCapri
,c.Gazelle_wa
,c.Scoreband
,f.scoreband
,c.creationmonth
,c.Subgroupcode
,c.Occupationstatus
,c.Employmenttype
,c.Persal
,c.Inseconds
,c.Transysteminstance
,c.Gotoffer
,C.Gotcardoffer
,c.maxcapoffer
,c.Maxtermoffer
,C.Maxcardoffer
,C.Maxloanoffer
,Loan_disbursed
,Card_disbursed
,reasoncode1
,declineaction
,failed_affordability
,scoringscoreband
,rejected_queue
,transequence
,c.Scoremodel
,c.Calculatednetincome
,c.Loanreference
,c.OPD
,c.CapitalinclSRA
,c.CapitalexclSRA
,c.Term
,c.Cardlimit
,c.Limitincreasedate
,c.Limitincreasemonth
,c.Startdate
,c.Startmonth
,c.Accountnumber
,c.Productcategory
,c.Productcategorydescription
,I.CallCentre_DM_Flag   
,I.SMSLeads_DM_Flag
,I.DM_MailerDate_DM_Flag 
,I.BranchLMS_DM_Flag
,I.Email_Tracker_DM_Flag
,H.Repeat_Flag
;

Update Creditprofitability.dbo.KM_creditappflow1
Set Startdate = Case when Card_disbursed = 1 
and Creditprofitability.dbo.KM_creditappflow1.startdate is null and Creditprofitability.dbo.KM_creditappflow1.limitincreasedate is null 
then b.startdate else Creditprofitability.dbo.KM_creditappflow1.startdate end 
from Creditprofitability.dbo.KM_Appflow_TempTables_Carddisbursed b
where replace(Creditprofitability.dbo.KM_creditappflow1.loanid,' ','') = replace(b.loanid,' ','')
;

Update Creditprofitability.dbo.KM_creditappflow1
Set Startmonth = Case when startmonth is null then left(startdate,6) else startmonth end
;

Alter table creditprofitability.dbo.KM_CreditAppflow1
Add BRdeclines varchar (30)
,INDUSTRY varchar (100)
,SUBGROUP_PROPOSED_STRATEGY varchar(100)
,Walkawayprob decimal(38,10)
,TakeUpProb  decimal(38,10)
,Loan_offer Tinyint
,Scorecardversion Varchar (10)
,IS_SG_UNKNOWN_CAPRI tinyint
,Call_Number tinyint
,Routing_NewvsRepeat Varchar (10)
;


Update creditprofitability.dbo.KM_CreditAppflow1
Set BRDeclines = case when InCapri is null or (InCapri = 1 and (reasoncode1  in  ('LQR22','LQR17','LQR20','LQR16','LQR15'))) then 'PreCapri'
					when  incapri = 1 and ((reasoncode1 IN ( 'prb')) or (reasoncode1 LIKE ('D%') AND  reasoncode1 NOT LIKE ('DS%')	AND  reasoncode1 NOT LIKE ('DAR%')))  then 'CapriPreBureau'
					when incapri = 1 and (reasoncode1 in ('DS62','DS64','DS74','DS78','DS79','DS777','DS778','DS779', 'DS783','DS784','DS785','DS786','DS780','DS787','DS788','DS789','DS790','DS791','DS792','DS793',
					'DS794','DS795','DS796','DS797','DS798','DS799','DS991','DS992','DS993','DS994','DS995','DS996', 'DS650', 'DS651') or reasoncode1 ='pb') then 'Scoring'
					When InCapri = 1 and ((reasoncode1 like ('afr%') or reasoncode1 = 'DS501') and reasoncode1 not in ('AFR03','DS500') and (failed_affordability <> '0' and failed_affordability is not null))  then 'Affordability'
					when incapri = 1 and ((reasoncode1 LIKE ('DS%')		and reasoncode1 <> 'DS64' 
																		and reasoncode1 <> 'ds01' 
																		and reasoncode1 <> 'ds02'
																		and reasoncode1 <> 'ds62'
																		and reasoncode1 <> 'ds74'
																		and reasoncode1 <> 'ds78'
																		and reasoncode1 <> 'ds79'
																		and reasoncode1 <> 'ds777'
																		and reasoncode1 <> 'ds778'
																		and reasoncode1 <> 'ds783'
																		and reasoncode1 <> 'ds784'
																		and reasoncode1 <> 'ds785'
																		and reasoncode1 <> 'ds779'
																		and reasoncode1 <> 'ds786'
																		and reasoncode1 <> 'ds780'
																		and reasoncode1 <> 'ds787'
																		and reasoncode1 <> 'ds500'
																		and reasoncode1 <> 'ds788'
																		and reasoncode1 <> 'ds789'
																		and reasoncode1 <> 'ds790'
																		and reasoncode1 <> 'ds791'
																		and reasoncode1 <> 'ds792'
																		and reasoncode1 <> 'ds793'
																		and reasoncode1 <> 'ds794'
																		and reasoncode1 <> 'ds795'
																		and reasoncode1 <> 'ds796'
																		and reasoncode1 <> 'ds797'
																		and reasoncode1 <> 'ds798'
																		and reasoncode1 <> 'ds799'
																		and reasoncode1 <> 'ds991'
																		and reasoncode1 <> 'ds992'
																		and reasoncode1 <> 'ds993'
																		and reasoncode1 <> 'ds994'
																		and reasoncode1 <> 'ds995'
																		and reasoncode1 <> 'ds996'
																		)) then 'CapriPostBureau'
					When incapri = 1 and reasoncode1 = 'LQR01' then 'DeclinedByCredit'
					When InCapri = 1 and ((reasoncode1 not in ('LQR22','LQR17','LQR20','LQR16','LQR15','ds01','ds02', 'prb','DS64','pb','PQAFr') /*-----added DS500 inclusion to add queue applicants in the queues category*/
										  and reasoncode1 not like ('DS%') or reasoncode1 = 'ds500') or reasoncode1 is null) and 
												declineaction = 'Qrej' then 'Queue'
					When InCapri = 1 and reasoncode1 in ('NS01','NS02') then 'NSCode '
					When InCapri = 1  and  reasoncode1 = 'PQAFr' then 'PostQueueAFR'
					When reasoncode1 like ('lqr02') then 'WalkAwayBeforeOffer'
					when reasoncode1 in ('ds500','AFR03') and (declineaction <> 'Qrej' or declineaction is null)  then 'No offers could be generated'
					else null end				
;

Update creditprofitability.dbo.KM_CreditAppflow1
Set BRDeclines = 'Queue' 
WHERE declineaction = 'Qrej' AND BRDeclines = 'AFFORDABILITY' AND REJECTED_QUEUE = 'AFFORDABILITY'
;

Update creditprofitability.dbo.KM_CreditAppflow1
Set Scoringscoreband = left(scorebanD,2) 
where Scoringscoreband like'-9%' and scorecard = 'v4'
;

Update creditprofitability.dbo.KM_CreditAppflow1
Set Scoringscoreband = case when scorecard = 'V4' and scoringscoreband = '*' then scoreband
else Scoringscoreband end
;

Update creditprofitability.dbo.KM_CreditAppflow1
set INDUSTRY = b.Ab_Sector
from PRD_CRUP.ic.CRUP_Subgroup_Industry_Test b  --CreditProfitability.ic.CRUP_Subgroup_Industry b
where replace(creditprofitability.dbo.KM_CreditAppflow1.Subgroupcode,' ','') = replace(b.REFERENCE,' ','') --use your appflow table
;

Update creditprofitability.dbo.KM_CreditAppflow1
set SUBGROUP_PROPOSED_STRATEGY = b.PROPOSED_STRATEGY
from PRD_CRUP.ic.CRUP_Subgroup_Industry_Test b ---CreditProfitability.ic.CRUP_Subgroup_Industry b
where replace(creditprofitability.dbo.KM_CreditAppflow1.Subgroupcode,' ','') = replace(b.REFERENCE,' ','') --use your appflow table
;

Update creditprofitability.dbo.KM_CreditAppflow1
Set Reasoncode1 = case when loanid like '944%' and reasoncode is null then b.reasoncode else reasoncode end
from CreditProfitability.dbo.KM_OMNI_DECLINES1_202006V2 b
where replace(creditprofitability.dbo.KM_CreditAppflow1.loanid,' ','') = replace(b.applicationid,' ','')
;

Update creditprofitability.dbo.KM_CreditAppflow1
Set Declineaction =  case when loanid like '944%' 
 and creditprofitability.dbo.KM_CreditAppflow1.declineaction is null 
 and (b.grouping  like 'AVSRJ%'
 or b.grouping  like 'REREJ%'
 or b.grouping  like 'COREJ%'
 or reasoncode1 = 'LQR30') then 'QREJ'
when loanid like '944%' and creditprofitability.dbo.KM_CreditAppflow1.declineaction is null
and b.declineaction like 'DECL%' then 'DECL'
when loanid like '944%' and creditprofitability.dbo.KM_CreditAppflow1.declineaction is null
and b.declineaction Like 'rej%' 
and b.grouping  not like 'AVSRJ%'
and b.grouping not like 'REREJ%'
and b.grouping not like 'COREJ%' then 'REJ' else creditprofitability.dbo.KM_CreditAppflow1.declineaction end
from CreditProfitability.dbo.KM_OMNI_DECLINES1_202006V2 b
where replace(creditprofitability.dbo.KM_CreditAppflow1.loanid,' ','') = replace(b.applicationid,' ','')
;

Update creditprofitability.dbo.KM_CreditAppflow1
Set BRDeclines = case when InCapri is null or (InCapri = 1 and (Reasoncode1  in  ('LQR22','LQR17','LQR20','LQR16','LQR15','LQR30'))) then 'PreCapri'
					when incapri = 1 and ((Reasoncode1 IN ( 'prb')) or (Reasoncode1 LIKE ('D%') AND  Reasoncode1 NOT LIKE ('DS%') AND  Reasoncode1 NOT LIKE ('DAR%')))  then 'CapriPreBureau'
					When incapri = 1 and (Reasoncode1 in ('DS62','DS64','DS74','DS78','DS79','DS777','DS778','DS779', 'DS783','DS784','DS785','DS786','DS780','DS787','DS788','DS789','DS790','DS791','DS792','DS793',
					'DS794','DS795','DS796','DS797','DS798','DS799','DS991','DS992','DS993','DS994','DS995','DS996', 'DS650', 'DS651') or Reasoncode1 ='pb') then 'Scoring'
					When InCapri = 1 and ((Reasoncode1 like ('afr%') or Reasoncode1 in ('DS501')) and Reasoncode1 not in ('AFR03','DS500') and (failed_affordability <> '0' and failed_affordability is not null))  then 'Affordability'
					when incapri = 1 and ((Reasoncode1 LIKE ('DS%')		and Reasoncode1 <> 'DS64' 
																		and Reasoncode1 <> 'ds01' 
																		and Reasoncode1 <> 'ds02'
																		and Reasoncode1 <> 'ds62'
																		and Reasoncode1 <> 'ds74'
																		and Reasoncode1 <> 'ds78'
																		and Reasoncode1 <> 'ds79'
																		and Reasoncode1 <> 'ds777'
																		and Reasoncode1 <> 'ds778'
																		and Reasoncode1 <> 'ds783'
																		and Reasoncode1 <> 'ds784'
																		and Reasoncode1 <> 'ds785'
																		and Reasoncode1 <> 'ds779'
																		and Reasoncode1 <> 'ds786'
																		and Reasoncode1 <> 'ds780'
																		and Reasoncode1 <> 'ds787'
																		and Reasoncode1 <> 'ds500'
																		and Reasoncode1 <> 'ds788'
																		and Reasoncode1 <> 'ds789'
																		and Reasoncode1 <> 'ds790'
																		and Reasoncode1 <> 'ds791'
																		and Reasoncode1 <> 'ds792'
																		and Reasoncode1 <> 'ds793'
																		and Reasoncode1 <> 'ds794'
																		and Reasoncode1 <> 'ds795'
																		and Reasoncode1 <> 'ds796'
																		and Reasoncode1 <> 'ds797'
																		and Reasoncode1 <> 'ds798'
																		and Reasoncode1 <> 'ds799'
																		and Reasoncode1 <> 'ds991'
																		and Reasoncode1 <> 'ds992'
																		and Reasoncode1 <> 'ds993'
																		and Reasoncode1 <> 'ds994'
																		and Reasoncode1 <> 'ds995'
																		and Reasoncode1 <> 'ds996'
																		)) then 'CapriPostBureau'  				
					When incapri = 1 and Reasoncode1 = 'LQR01' then 'DeclinedByCredit'
					When InCapri=1 and ((Reasoncode1 not in ('LQR22','LQR17','LQR20','LQR16','LQR15','ds01','ds02', 'prb','DS64','pb','PQAFr') /*-----added DS500 inclusion to add queue applicants in the queues category --why must DS500 be added as part of the queues?*/
										  and Reasoncode1 not like ('DS%') or reasoncode1 = 'DS500') or Reasoncode1 is null) and 
												Declineaction ='QREJ'  then 'Queue'
					When InCapri = 1 and Reasoncode1 in ('NS01','NS02') then 'NSCode '
					When InCapri = 1 and Reasoncode1 in ('PQAFr','GAR01') then 'PostQueueAFR'
					When Reasoncode1 like ('lqr02') then 'WalkAwayBeforeOffer'
					when Reasoncode1 in ('ds500','AFR03') and (Declineaction <> 'QRej' or Declineaction is null)  then 'No offers could be generated'
					else Brdeclines end	
where Loanid like '944%' 
;

Update creditprofitability.dbo.KM_CreditAppflow1
Set BRDeclines = Case when Reasoncode1 is null and Declineaction is null then NULL else BRdeclines end
where Loanid like '944%' and BRDECLINES ='QUEUE'
;

Update creditprofitability.dbo.KM_CreditAppflow1
Set BRDeclines = Case when Reasoncode1 in ('RRO04','RRC04') 
then 'User_Cancel' else BRdeclines end
where Loanid like '944%'   
;

Update creditprofitability.dbo.KM_CreditAppflow1
Set TakeUpProb = Adj_Final_score
,Loan_offer = b.loanOffer
from dev_DataDistillery_credit.dbo.UD_TUModelScored b 
where replace(creditprofitability.dbo.KM_CreditAppflow1.loanid,' ','') = replace(b.loanid,' ','') 
;

Update creditprofitability.dbo.KM_CreditAppflow1
Set Walkawayprob = (1-Takeupprob)
;

Update Creditprofitability.dbo.KM_creditappflow1
Set Scorecardversion = b.scorecardversion
from creditprofitability.dbo.KM_Appflow_TempTables_Scorecardversion b
where replace(Creditprofitability.dbo.KM_creditappflow1.loanid, ' ','') = replace(b.loanid,' ','')
;

Update Creditprofitability.dbo.KM_creditappflow1
Set IS_SG_UNKNOWN_CAPRI = b.IS_SG_UNKNOWN_CAPRI
from PRD_CRUP.dbo.EVA_App_Subgroup_Detail b
where replace(Creditprofitability.dbo.KM_creditappflow1.loanid, ' ','') = replace(b.Applicationid,' ','')
;

Update Creditprofitability.dbo.KM_creditappflow1
Set Call_Number = b.Call_Number
from Creditprofitability.dbo.KM_Appflow_TempTables_OfferCallNumber b
where replace(Creditprofitability.dbo.KM_creditappflow1.loanid, ' ','') = replace(b.Loanid,' ','')
;

Update Creditprofitability.dbo.KM_creditappflow1
Set Routing_NewvsRepeat = case when b.Routetooffer = 'N01' then 'NEW'
							   when b.Routetooffer in ('R01','R02','R03') then 'REPEAT'
							   when b.Routetooffer = 'D01' then 'DORMANT' else Routing_NewvsRepeat end
from Creditprofitability.dbo.KM_Appflow_TempTables_Gazelle_RoutingIndicator b
Where replace(Creditprofitability.dbo.KM_creditappflow1.loanid,' ','') = replace(b.loanid,' ','')
;

Update Creditprofitability.dbo.KM_creditappflow1
Set Routing_NewvsRepeat = case when Routing_NewvsRepeat is null and b.Indicatorvalue = 'N01' then 'NEW'
							   when Routing_NewvsRepeat is null and b.Indicatorvalue in ('R01','R02','R03') then 'REPEAT'
							   when Routing_NewvsRepeat is null and b.Indicatorvalue = 'D01' then 'DORMANT' else Routing_NewvsRepeat end
from Creditprofitability.dbo.KM_Appflow_TempTables_OMNI_RoutingIndicator b
Where replace(Creditprofitability.dbo.KM_creditappflow1.loanid,' ','') = replace(b.loanid,' ','')
;

Update creditprofitability.dbo.KM_CreditAppflow1
Set BRDeclines = case when InCapri is null or (InCapri = 1 and (reasoncode1  in  ('LQR22','LQR17','LQR20','LQR16','LQR15'))) then 'PreCapri'
					when  incapri = 1 and ((reasoncode1 IN ( 'prb')) or (reasoncode1 LIKE ('D%') AND  reasoncode1 NOT LIKE ('DS%')	AND  reasoncode1 NOT LIKE ('DAR%')))  then 'CapriPreBureau'
					when incapri = 1 and (reasoncode1 in ('DS62','DS64','DS74','DS78','DS79','DS777','DS778','DS779', 'DS783','DS784','DS785','DS786','DS780','DS787','DS788','DS789','DS790','DS791','DS792','DS793',
					'DS794','DS795','DS796','DS797','DS798','DS799','DS991','DS992','DS993','DS994','DS995','DS996' ,'DS983', 'DS984', 'DS985', 'DS986', 'DS987' ,'DS988') or reasoncode1 ='pb') then 'Scoring'
					When InCapri = 1 and ((reasoncode1 like ('afr%') or reasoncode1 = 'DS501') and reasoncode1 not in ('AFR03','DS500') and (failed_affordability <> '0' and failed_affordability is not null))  then 'Affordability'
					when incapri = 1 and ((reasoncode1 LIKE ('DS%')		and reasoncode1 <> 'DS64' 
																		and reasoncode1 <> 'ds01' 
																		and reasoncode1 <> 'ds02'
																		and reasoncode1 <> 'ds62'
																		and reasoncode1 <> 'ds74'
																		and reasoncode1 <> 'ds78'
																		and reasoncode1 <> 'ds79'
																		and reasoncode1 <> 'ds777'
																		and reasoncode1 <> 'ds778'
																		and reasoncode1 <> 'ds783'
																		and reasoncode1 <> 'ds784'
																		and reasoncode1 <> 'ds785'
																		and reasoncode1 <> 'ds779'
																		and reasoncode1 <> 'ds786'
																		and reasoncode1 <> 'ds780'
																		and reasoncode1 <> 'ds787'
																		and reasoncode1 <> 'ds500'
																		and reasoncode1 <> 'ds788'
																		and reasoncode1 <> 'ds789'
																		and reasoncode1 <> 'ds790'
																		and reasoncode1 <> 'ds791'
																		and reasoncode1 <> 'ds792'
																		and reasoncode1 <> 'ds793'
																		and reasoncode1 <> 'ds794'
																		and reasoncode1 <> 'ds795'
																		and reasoncode1 <> 'ds796'
																		and reasoncode1 <> 'ds797'
																		and reasoncode1 <> 'ds798'
																		and reasoncode1 <> 'ds799'
																		and reasoncode1 <> 'ds991'
																		and reasoncode1 <> 'ds992'
																		and reasoncode1 <> 'ds993'
																		and reasoncode1 <> 'ds994'
																		and reasoncode1 <> 'ds995'
																		and reasoncode1 <> 'ds996'
																		)) then 'CapriPostBureau'
					When incapri = 1 and reasoncode1 = 'LQR01' then 'DeclinedByCredit'
					When InCapri = 1 and ((reasoncode1 not in ('LQR22','LQR17','LQR20','LQR16','LQR15','ds01','ds02', 'prb','DS64','pb','PQAFr') /*-----added DS500 inclusion to add queue applicants in the queues category*/
										  and reasoncode1 not like ('DS%') or reasoncode1 = 'ds500') or reasoncode1 is null) and 
												declineaction = 'Qrej' then 'Queue'
					When InCapri = 1 and reasoncode1 in ('NS01','NS02') then 'NSCode '
					When InCapri = 1  and  reasoncode1 = 'PQAFr' then 'PostQueueAFR'
					When reasoncode1 like ('lqr02') then 'WalkAwayBeforeOffer'
					when reasoncode1 in ('ds500','AFR03') and (declineaction <> 'Qrej' or declineaction is null)  then 'No offers could be generated'
					else null end				
;

delete from Creditprofitability.dbo.KM_creditappflow1
where startdate is null and loanid in (Select loanid from Creditprofitability.dbo.KM_creditappflow1
group by loanid having count(loanid) > 1 )
;
/*
-----------------------------------------------------------------------------------------------------------------------------------------------Uniqueindicator Fixes

-----Test the data in the table

Select top 100 * from creditprofitability.dbo.KM_CreditAppflow1
Go

Select top 35 loanid from creditprofitability.dbo.KM_CreditAppflow1
WHERE UniqueIndicator =1
group by loanid having count(loanid)>1 
Go

SELECT * FROM creditprofitability.dbo.KM_CreditAppflow1
WHERE LOANID IN ('9451799924'
,'9452011153'
,'9451820352'
,'9451801441'
)
ORDER BY LOANID
*/
Update Creditprofitability.dbo.KM_creditappflow1
Set Uniqueindicator = Case when (origination_source = ' ' and loanid like '944%') then 0 else Uniqueindicator end
;

Update Creditprofitability.dbo.KM_creditappflow1
Set Uniqueindicator = Case when (failed_affordability != '0' and LOANID IN ('9447090233')) then 0 else Uniqueindicator end
;

Update Creditprofitability.dbo.KM_creditappflow1
Set Uniqueindicator = Case when (scoringscoreband = '60' and LOANID IN ('9449532674')) then 0 else Uniqueindicator end

;

delete from creditprofitability.dbo.KM_CreditAppflow1
where LOANID IN ('9449532674') and Uniqueindicator = 0
;
/*
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
*/
DELETE FROM creditprofitability.dbo.KM_CreditAppflow1
WHERE LOANID IN (Select loanid from creditprofitability.dbo.KM_CreditAppflow1
				 group by loanid having count(loanid) > 1 )
AND STATUS = 'REJ'
AND UniqueIndicator = 1
	) by APS;
	DISCONNECT FROM APS;
quit;

/**/
/*Script 3*/
proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_CreditAppflow11')
 BEGIN
    DROP TABLE creditprofitability.dbo.KM_CreditAppflow11
 END;

CREATE TABLE creditprofitability.dbo.KM_CreditAppflow11

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

as 

Select  C.* 
From (Select A.[loanid]
,A.[idnumber]
,A.[Clientnumber]
,A.[Uniqueid]
,A.[Creationdate]
,A.[Branchcode]
,A.[Origination_source]
,A.[Status]
,A.[Uniqueindicator]
,A.[Finaluniqueid]
,A.[Scorecard]
,A.[Wagefreq]
,A.[Bank]
,A.[InCapri]
,A.[Gazelle_wa]
,A.[Transequence]
,A.[Scoreband]
,A.[creationmonth]
,A.[Subgroupcode]
,A.[Occupationstatus]
,A.[Employmenttype]
,A.[Persal]
,A.[Inseconds]
,A.[Transysteminstance]
,A.[Scoremodel]
,A.[Calculatednetincome]
,A.[Gotoffer]
,A.[Gotcardoffer]
,A.[Loan_disbursed]
,A.[Card_disbursed]
,A.[Maxcapoffer]
,A.[MaxTermoffer]
,A.[MaxCardoffer]
,A.[MaxLoanoffer]
,A.[Loanreference]
,A.[OPD]
,A.[CapitalinclSRA]
,A.[CapitalexclSRA]
,A.[Term]
,A.[Cardlimit]
,A.[Limitincreasedate]
,A.[Accountnumber]
,A.[Reasoncode1]
,A.[Declineaction]
,A.[Failed_affordability]
,A.[Scoringscoreband]
,A.[Rejected_queue]
,A.[DM_Flag]
,A.[BRdeclines]
,A.[Industry]
,A.[Subgroup_Proposed_Strategy]
,A.[IS_SG_UNKNOWN_CAPRI]
,A.[TakeupProb]
,A.[WalkawayProb]
,A.[Loan_Offer]
,A.[Scorecardversion] 
,A.[Sourcesystem]
,A.[Call_Number]
,A.[Routing_NewvsRepeat]
,A.[Startdate]
,A.[Startmonth]
,A.[Limitincreasemonth]
,A.[Productcategory]
,A.[Productcategorydescription] 
  FROM [Creditprofitability].[dbo].[Central_CreditAppflow] A 
                     LEFT JOIN Creditprofitability.dbo.KM_CreditAppflow1 A1 
					 ON A.loanid = A1.loanid 
  Where A1.loanid IS NULL 
              and A.creationdate > 0  

  union all 

  SELECT [loanid]
,[idnumber]
,[Clientnumber]
,[Uniqueid]
,[Creationdate]
,[Branchcode]
,[Origination_source]
,[Status]
,[Uniqueindicator]
,[Finaluniqueid]
,[Scorecard]
,[Wagefreq]
,[Bank]
,[InCapri]
,[Gazelle_wa]
,[Transequence]
,[Scoreband]
,[creationmonth]
,[Subgroupcode]
,[Occupationstatus]
,[Employmenttype]
,[Persal]
,[Inseconds]
,[Transysteminstance]
,[Scoremodel]
,[Calculatednetincome]
,[Gotoffer]
,[Gotcardoffer]
,[Loan_disbursed]
,[Card_disbursed]
,[Maxcapoffer]
,[MaxTermoffer]
,[MaxCardoffer]
,[MaxLoanoffer]
,[Loanreference]
,[OPD]
,[CapitalinclSRA]
,[CapitalexclSRA]
,[Term]
,[Cardlimit]
,[Limitincreasedate]
,[Accountnumber]
,[Reasoncode1]
,[Declineaction]
,[Failed_affordability]
,[Scoringscoreband]
,[Rejected_queue]
,[DM_Flag]
,[BRdeclines]
,[Industry]
,[Subgroup_Proposed_Strategy]
,[IS_SG_UNKNOWN_CAPRI] 
,[TakeupProb]
,[WalkawayProb]
,[Loan_Offer]
,[Scorecardversion]
,[Sourcesystem]
,[Call_Number]
,[Routing_NewvsRepeat]
,[Startdate]
,[Startmonth]
,[Limitincreasemonth]
,[Productcategory]
,[Productcategorydescription]
  FROM [Creditprofitability].[dbo].[KM_CreditAppflow1]
  ) AS C  

	) by APS;
	DISCONNECT FROM APS;
quit;

/*
-SCR 4----------------------------------------------------------------------------------------------------------------------
------------------------------RUN as Final step------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------
select min(creationdate), max(creationdate) from  creditprofitability.dbo.Central_CreditAppflow_old
select min(creationdate), max(creationdate) from  creditprofitability.dbo.Central_CreditAppflow
select min(creationdate), max(creationdate) from  creditprofitability.dbo.KM_CreditAppflow11

-----------------------------------------------------------------------------------------------------------------------
*/
proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'central_creditappflow_old')
 BEGIN
    DROP TABLE creditprofitability.dbo.central_creditappflow_old
 END;

CREATE TABLE creditprofitability.dbo.central_creditappflow_old

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

as 

Select *
from creditprofitability.dbo.Central_CreditAppflow
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
	/*
------------------------------------------------------------------------------------------------------------------------

select min(creationdate), max(creationdate) from  creditprofitability.dbo.central_creditappflow_old
select min(creationdate), max(creationdate) from  creditprofitability.dbo.Central_CreditAppflow
select min(creationdate), max(creationdate) from  creditprofitability.dbo.KM_CreditAppflow11

-------------------------------------------------------------------------------------------------------------------------
Select CreationMonth, count(*) 
from creditprofitability.dbo.Central_CreditAppflow
where CreationMonth > 202212 and uniqueindicator =1 and Incapri =1 
group by CreationMonth
Order by CreationMonth

Select CreationMonth, count(*) 
from creditprofitability.dbo.KM_CreditAppflow11
where CreationMonth > 202212 and Uniqueindicator =1 and Incapri =1
group by CreationMonth
Order by CreationMonth
Go
--------------------------------------------------------------------------------------------------------------------------
*/
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'Central_CreditAppflow')
 BEGIN
    DROP TABLE creditprofitability.dbo.Central_CreditAppflow
 END;

CREATE TABLE creditprofitability.dbo.Central_CreditAppflow

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

as 

Select *
from creditprofitability.dbo.KM_CreditAppflow11
;

/*-------------------------------------------------------------------------------------------------------------------------*/

Drop table creditprofitability.dbo.KM_CreditAppflow11

/*--PRINT	'***ALL COMPLETE!!! RENAME APPFlOW11 to APPFLOW!!!!!!!!!!!!!!!!!!!!!!!!!!!'*/
	) by APS;
	DISCONNECT FROM APS;
quit;

/*Scropt 5*/
proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'Central_CreditAppflow_Pivotdata')
 BEGIN
    DROP TABLE Creditprofitability.dbo.Central_CreditAppflow_Pivotdata
 END;

CREATE TABLE Creditprofitability.dbo.Central_CreditAppflow_Pivotdata

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

as 

Select distinct loanid
,idnumber
,Uniqueid
,CONVERT(VARCHAR(10), creationdate, 112) as Creationdate
,Origination_source
,Status
,Uniqueindicator
,Finaluniqueid
,Scorecard
,Wagefreq
,Bank
,InCapri
,Transequence
,Scoreband
,creationmonth
,Gotoffer
,Gotcardoffer
,Loan_disbursed
,Card_disbursed
,Maxcapoffer
,Maxloanoffer
,a.Loanreference
,Accountnumber
,OPD
,CapitalinclSRA
,CapitalexclSRA
,Cardlimit
,Reasoncode1
,Failed_affordability
,Scoringscoreband
,BRdeclines
,DM_flag
,Scorecardversion
,Sourcesystem
,Takeupprob
,Walkawayprob
,Loan_offer
,Routing_NewvsRepeat
,Call_Number
,IS_SG_UNKNOWN_CAPRI
from Creditprofitability.dbo.Central_CreditAppflow a
Where DATEDIFF(mm, CONVERT(DATETIME, cast(A.creationdate as varchar), 100), getdate()) < 15
and DATEDIFF(mm, CONVERT(DATETIME, cast(A.creationdate as varchar), 100), getdate()) >= 0
;

Alter table Creditprofitability.dbo.Central_CreditAppflow_Pivotdata
Add Scoreband_grouping varchar (20),
Offer_grouping varchar (35),
Loanoffer_grouping Varchar (35),
LoanexclSRA_grouping varchar (35),
Disbursed int,
Offerscoreband int,
LoaninclSRA_grouping varchar (35)
;

Update Creditprofitability.dbo.Central_CreditAppflow_Pivotdata
Set Scoreband_grouping = case when BRdeclines = 'Scoring' then '999' when brdeclines = 'capriprebureau' then '998' else Scoringscoreband end,
Offer_grouping = case when MaxCapOffer <=  5000 then 'Up to 5k'
when MaxCapOffer > 5000 and MaxCapOffer <= 10000 then  'Greater than 5k and up to 10k'
when MaxCapOffer > 10000 and MaxCapOffer <= 15000 then  'Greater than 10k and up to 15k'
when MaxCapOffer > 15000 and MaxCapOffer <= 25000 then  'Greater than 15k and up to 25k'
when MaxCapOffer > 25000 and MaxCapOffer <= 35000 then  'Greater than 25k and up to 35k'
when MaxCapOffer > 35000 and MaxCapOffer <= 60000 then 'Greater than 35k and up to 60k'
when MaxCapOffer > 60000 and MaxCapOffer <= 90000 then 'Greater than 60k and up to 90k'
when MaxCapOffer > 90000 and MaxCapOffer <=  120000 then 'Greater than 90k up 120k' 
when MaxCapOffer > 120000 and MaxCapOffer <=  150000 then  'Greater than 120k up 150k' 
when MaxCapOffer > 150000 and MaxCapOffer <=  200000 then 'Greater than 150k up 200k'
when MaxCapOffer > 200000  then 'Greater than 200k' end
;

Update Creditprofitability.dbo.Central_CreditAppflow_Pivotdata
Set LoanOffer_grouping = case when MaxLoanOffer <=  5000 then 'Up to 5k'
when MaxLoanOffer > 5000 and MaxLoanOffer <= 10000 then  'Greater than 5k and up to 10k'
when MaxLoanOffer > 10000 and MaxLoanOffer <= 15000 then  'Greater than 10k and up to 15k'
when MaxLoanOffer > 15000 and MaxLoanOffer <= 25000 then  'Greater than 15k and up to 25k'
when MaxLoanOffer > 25000 and MaxLoanOffer <= 35000 then  'Greater than 25k and up to 35k'
when MaxLoanOffer > 35000 and MaxLoanOffer <= 60000 then 'Greater than 35k and up to 60k'
when MaxLoanOffer > 60000 and MaxLoanOffer <= 90000 then 'Greater than 60k and up to 90k'
when MaxLoanOffer > 90000 and MaxLoanOffer <= 120000 then 'Greater than 90k up 120k' 
when MaxLoanOffer > 120000 and MaxLoanOffer <= 150000 then  'Greater than 120k up 150k' 
when MaxLoanOffer > 150000 and MaxLoanOffer <= 200000 then 'Greater than 150k up 200k'
when MaxLoanOffer > 200000  then 'Greater than 200k' end
;

Update Creditprofitability.dbo.Central_CreditAppflow_Pivotdata
Set LoanexclSRA_grouping = case when CapitalexclSRA <=  5000 then 'Up to 5k'
when CapitalexclSRA > 5000 and CapitalexclSRA <= 10000 then 'Greater than 5k and up to 10k'
when CapitalexclSRA > 10000 and CapitalexclSRA <= 15000 then 'Greater than 10k and up to 15k'
when CapitalexclSRA > 15000 and CapitalexclSRA <= 25000 then 'Greater than 15k and up to 25k'
when CapitalexclSRA > 25000 and CapitalexclSRA <= 35000 then 'Greater than 25k and up to 35k'
when CapitalexclSRA > 35000 and CapitalexclSRA <= 60000 then 'Greater than 35k and up to 60k'
when CapitalexclSRA > 60000 and CapitalexclSRA <= 90000 then 'Greater than 60k and up to 90k'
when CapitalexclSRA > 90000 and CapitalexclSRA <= 120000 then 'Greater than 90k and up to 120k'
when CapitalexclSRA > 120000 and CapitalexclSRA <= 150000 then 'Greater than 120k and up to 150k'
when CapitalexclSRA > 150000 and CapitalexclSRA <= 200000 then 'Greater than 150k and up to 200k'
when CapitalexclSRA > 200000 then 'Greater than 200k' end
;

Update creditprofitability.dbo.Central_CreditAppflow_Pivotdata
Set Disbursed = case when card_disbursed > 0 or loan_disbursed > 0 then 1 else 0 end
;

Update creditprofitability.dbo.Central_CreditAppflow_Pivotdata
Set Offerscoreband = scoreband
;

Update creditprofitability.dbo.Central_CreditAppflow_Pivotdata
Set LoaninclSRA_grouping = case when CapitalinclSRA <= 5000 then 'Up to 5k'
when CapitalinclSRA > 5000 and CapitalinclSRA <= 10000 then 'Greater than 5k and up to 10k'
when CapitalinclSRA > 10000 and CapitalinclSRA <= 15000 then 'Greater than 10k and up to 15k'
when CapitalinclSRA > 15000 and CapitalinclSRA <= 25000 then 'Greater than 15k and up to 25k'
when CapitalinclSRA > 25000 and CapitalinclSRA <= 35000 then 'Greater than 25k and up to 35k'
when CapitalinclSRA > 35000 and CapitalinclSRA <= 60000 then 'Greater than 35k and up to 60k'
when CapitalinclSRA > 60000 and CapitalinclSRA <= 90000 then 'Greater than 60k and up to 90k'
when CapitalinclSRA > 90000 and CapitalinclSRA <= 120000 then 'Greater than 90k and up to 120k'
when CapitalinclSRA > 120000 and CapitalinclSRA <= 150000 then 'Greater than 120k and up to 150k'
when CapitalinclSRA > 150000 and CapitalinclSRA <= 200000 then 'Greater than 150k and up to 200k'
when CapitalinclSRA > 200000 then 'Greater than 200k' end
	) by APS;
	DISCONNECT FROM APS;
quit;

/*script 6*/
proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*------------------------------------------ CREATE BEHAVE SCORE TABLE ------------------------------------------*/
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_BehaveScore')
  BEGIN
    DROP TABLE creditprofitability.dbo.KM_BehaveScore
  END;

CREATE TABLE creditprofitability.dbo.KM_BehaveScore
WITH (DISTRIBUTION = HASH(Uniqueid), CLUSTERED COLUMNSTORE INDEX)
AS
SELECT  A.Caprikey 
      , A.Uniqueid 
      , A.ApplicationDate 
      , A.Score as BehaveScore 
      , B.Tranappnumber
from PRD_PRESS.Capri.Capri_behavioural_Score AS A
INNER JOIN PRD_PRess.Capri.Capri_Loan_Application AS B 
ON A.UniqueID = B.UniqueID
WHERE Classification = 'AB'
AND isnull(B.channelcode,'') <>'ccc006' and B.TRANSEQUENCE <> '005' 
AND DATEDIFF(DAY, a.ApplicationDate, GETDATE()) < 500
AND	DATEDIFF(DAY, a.ApplicationDate, GETDATE()) >= 0
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(

/*------------------------------------------ UPDATE BEHAVE SCORE TABLE ------------------------------------------*/
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_BehaveScore_2')
  BEGIN
    DROP TABLE creditprofitability.dbo.KM_BehaveScore_2
  END;

CREATE TABLE creditprofitability.dbo.KM_BehaveScore_2
		WITH (DISTRIBUTION = HASH(uniqueid), CLUSTERED COLUMNSTORE INDEX)
AS
SELECT	Caprikey 
      , Uniqueid 
      , ApplicationDate 
      , BehaveScore 
      , Tranappnumber
	  , Repeat_Flag
FROM
	(SELECT
			CASE WHEN BehaveScore > 0 THEN 1
			ELSE 0
			END AS Repeat_Flag
		  , Caprikey 
		  , Uniqueid 
		  , ApplicationDate 
		  , BehaveScore 
		  , Tranappnumber
	FROM creditprofitability.dbo.KM_BehaveScore)x
	GROUP BY Repeat_Flag
		  , Caprikey 
		  , Uniqueid 
		  , ApplicationDate 
		  , BehaveScore 
		  , Tranappnumber
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*------------------------------------------ CREATE NEW VS REPEAT PIVOT ------------------------------------------*/
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_AppFlow_BehaveScore')
  BEGIN
    DROP TABLE creditprofitability.dbo.KM_AppFlow_BehaveScore
  END;

CREATE TABLE creditprofitability.dbo.KM_AppFlow_BehaveScore
		WITH (DISTRIBUTION = HASH(uniqueid), CLUSTERED COLUMNSTORE INDEX)
AS
SELECT DISTINCT a.Uniqueid
				, b.Repeat_Flag
				, a.loanid
				, a.Origination_source
				, a.Uniqueindicator
				, a.InCapri
				, a.creationmonth
				, a.Gotoffer
FROM Creditprofitability.dbo.Central_CreditAppflow_Pivotdata AS a
LEFT JOIN creditprofitability.dbo.KM_BehaveScore_2 AS b
ON a.uniqueid = b.uniqueid
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*------------------------------------------ CREATE WATERFALL PIVOT ------------------------------------------*/

USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Waterfall_Pivots')
 BEGIN
    DROP TABLE CreditProfitability.dbo.KM_Waterfall_Pivots
 END;

CREATE TABLE CreditProfitability.dbo.KM_Waterfall_Pivots

WITH (DISTRIBUTION = HASH(LOANID), CLUSTERED COLUMNSTORE INDEX)

as 

Select * from Creditprofitability.dbo.Central_CreditAppflow_Pivotdata
Where creationmonth = (SELECT max(creationmonth) FROM Creditprofitability.dbo.Central_CreditAppflow_Pivotdata)
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*------------------------------------------ CREATE OVERDRAFT TABLE ------------------------------------------*/

USE creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Overdraft')
 BEGIN
    DROP TABLE creditprofitability.dbo.KM_Overdraft
 END;

CREATE TABLE creditprofitability.dbo.KM_Overdraft

WITH (DISTRIBUTION = HASH(APPLICATIONID), CLUSTERED COLUMNSTORE INDEX)
AS       
SELECT  
		a.APPLICATIONID    
		,a.APPLICATIONTYPE    
		,b.Type    
		,a.CLIENTNUMBER 
        ,APPLICATIONSTATUS    
        ,WORFLOWSTATUS    
        ,COMPANY    
        ,CHANNEL    
        ,ORIGINATIONBRANCH    
        ,BRANCH    
        ,EXTERNALREFERENCE    
        ,CONCLUDEDBY    
        ,CREATEDBY    
        ,CREATIONDATE    
        ,LASTUPDATEDBY    
        ,LASTUPDATETIMESTAMP     
 FROM PRD_ExactusSync.dbo.applications a 
 JOIN PRD_ExactusSync.dbo.OC00000P b    
 ON a.APPLICATIONID = b.ApplicationID    
 WHERE a.CreationDate >= '2021-05-01'
 and Type IN ('OVER', 'LOAN', 'CARD', 'LIMI', 'OVLI')
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*------------------------------------------------- JOIN OVERDRAFT TO APPFLOW -------------------------------------------------*/

USE creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Overdraft_Appflow')
 BEGIN
    DROP TABLE creditprofitability.dbo.KM_Overdraft_Appflow
 END;

CREATE TABLE creditprofitability.dbo.KM_Overdraft_Appflow

WITH (DISTRIBUTION = HASH(loanid), CLUSTERED COLUMNSTORE INDEX)
AS    
SELECT DISTINCT b.Applicationid
				,a.loanid
				,a.Uniqueid
				,a.Origination_source
				,a.Status
				,a.Uniqueindicator
				,a.Finaluniqueid
				,a.Scorecard
				,a.Wagefreq
				,a.Bank
				,a.InCapri
				,a.Transequence
				,a.Scoreband
				,a.creationmonth
				,a.Gotoffer
				,a.Gotcardoffer
				,a.Loan_disbursed
				,a.Card_disbursed
				,a.disbursed
				,a.Maxcapoffer
				,a.Maxloanoffer
				,a.Loanreference
				,a.Accountnumber
				,a.OPD
				,a.CapitalinclSRA
				,a.CapitalexclSRA
				,a.Cardlimit
				,a.Reasoncode1
				,a.Failed_affordability
				,a.Scoringscoreband
				,a.BRdeclines
				,a.DM_flag
				,a.Scorecardversion
				,a.Sourcesystem
				,a.Takeupprob
				,a.Walkawayprob
				,a.Loan_offer
				,a.Routing_NewvsRepeat
				,a.Call_Number
				,a.IS_SG_UNKNOWN_CAPRI
				,b.type AS Product
				,b.worflowstatus AS WorkFlowStatus
FROM Creditprofitability.dbo.Central_CreditAppflow_Pivotdata AS a
LEFT JOIN creditprofitability.dbo.KM_Overdraft AS b
ON a.loanid = b.applicationid
WHERE a.creationmonth >= '202105'
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*------------------------------------------------- CREATE PRODIUCT PIVOT FOR POWERBI -------------------------------------------------*/

USE creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_Products')
 BEGIN
    DROP TABLE creditprofitability.dbo.KM_Appflow_Products
 END;

CREATE TABLE creditprofitability.dbo.KM_Appflow_Products

WITH (DISTRIBUTION = HASH(loanid), CLUSTERED COLUMNSTORE INDEX)
AS    
SELECT loanid
		,origination_source
		,Uniqueindicator
		,InCapri
		,Creationmonth
		,Product
		FROM creditprofitability.dbo.KM_Overdraft_Appflow
		WHERE creationmonth is not null

Update creditprofitability.dbo.KM_Appflow_Products
Set product = Case	When product IN ('LIMI', 'CARD', ' ') THEN 'Card'
					When product IN ('OVER','OVLI') THEN 'Overdraft'
					When product IN ('LOAN') Then 'Loan'
					Else 'Unknown'
					End
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*------------------------------------------------------ Create Appflow Waterfall Temp Table ------------------------------------------------------- */
USE CREDITPROFITABILITY
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_WaterFall_temp')
   BEGIN
DROP TABLE creditprofitability.dbo.KM_Appflow_WaterFall_temp
   END;
CREATE TABLE creditprofitability.dbo.KM_Appflow_WaterFall_temp
WITH (CLUSTERED COLUMNSTORE INDEX ,DISTRIBUTION = HASH(loanid))
AS
select	loanid
		,uniqueid
		,creationdate
		,creationmonth
		,ReasonCode1
		,gotoffer
		,Status
from Creditprofitability.dbo.Central_CreditAppflow_Pivotdata
where uniqueindicator = 1
and reasoncode1 is not null
and incapri = 1
and GotOffer = 0
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*------------------------------------------------------ Create Appflow Waterfall Table ------------------------------------------------------- */

USE CREDITPROFITABILITY
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_Appflow_WaterFall')
   BEGIN
DROP TABLE CreditProfitability.dbo.KM_Appflow_WaterFall
   END;
CREATE TABLE CreditProfitability.dbo.KM_Appflow_WaterFall
		WITH (DISTRIBUTION = HASH(uniqueid), CLUSTERED COLUMNSTORE INDEX)
AS  
SELECT	a.uniqueid
		,c.Tranappnumber
		,a.creationmonth
		,CONVERT(FLOAT,c.maximuminstalment2) AS maxinstalment2
		,CONVERT(FLOAT,c.maximuminstalment4) AS maxinstalment4
		,CONVERT(FLOAT,c.maximuminstalment5) AS maxinstalment5
		,ROUND(CONVERT(FLOAT,c.maximuminstalment6),0) AS maxinstalment6
		,CONVERT(FLOAT,c.maximuminstalment7) AS maxinstalment7
		,CASE 
			WHEN behavescore IS NOT NULL OR CAST(behavescore AS INT) > 0 THEN 1 
			ELSE 0 
		END AS New
		,term12 = 12
		,term7 = 7
		,term24 = 24
		,a.Status
		,c.MonthlyGrossIncome
		,c.UniqueClientApp
		,c.CallNumber
		,IntRate = 0.275
		,InsRate = 0.0540
		,a.Reasoncode1 as DeclineCode
 FROM  creditprofitability.dbo.KM_Appflow_WaterFall_temp as a
 LEFT JOIN CreditProfitability..AppInfo c
 on a.uniqueid = c.uniqueid
;
/*------------------------------------------------------ Alter Table ------------------------------------------------------- */

Alter table CreditProfitability.dbo.KM_Appflow_WaterFall
Add	DeclineCategory varchar(30)
	,DeclineSubCategory varchar(30)
	,Min_Afford BIGINT
	,Income_bucket varchar (40)
	,Afford_bucket varchar (40)

;
update CreditProfitability.dbo.KM_Appflow_WaterFall
set DeclineCategory = case	when DeclineCode in (select distinct declineCode 
							from CreditProfitability..BusinessRulesCategorization
							where maincategory = 'Credit Rules') Then 'Credit Rules'

							when DeclineCode in (select distinct declineCode 
							from CreditProfitability..BusinessRulesCategorization
							where maincategory = 'Compliance Internal') Then 'Compliance Internal'

							when DeclineCode in (select distinct declineCode 
							from CreditProfitability..BusinessRulesCategorization
							where maincategory = 'Compliance External') Then 'Compliance External'

							when DeclineCode in (select distinct declineCode 
							from CreditProfitability..BusinessRulesCategorization
							where maincategory = 'Collections Internal') Then 'Collections Internal'
							
							when DeclineCode in ('DS01','DS17','DS66','DS40','DS38','DS103','DS601','DS602','DS603','DS605','DS607'
												,'DS610','DS611','DS612','DS42','DS021','DS169','DS991','DS650','DS651','DS990','DS991'
												,'DS992','DS31','DS62','DS64','DS74','DS777','DS778','DS788','DS789','DS790','DS791'
												,'DS792','DS793','DS794','DS795','DS796','DS797','DS798','DS799','DS779','DS993','DS994'
												,'DS995','DS996','DS81','DS83','DS997','DS998','DS608','DS294','DS261','DS251','DS48'
												,'DS93','DS142','DS143')
							THEN 'Credit Rules'

							ELSE 'Compliance Internal'
							END
;
update CreditProfitability.dbo.KM_Appflow_WaterFall
set DeclineSubCategory = case	when DeclineCode in ('DS01', 'DS17', 'DS66')
								THEN 'Age'

								when DeclineCode in ('DS40')
								THEN '40 days since last disbursal'

								when DeclineCode in ('DS38','DS103','DS601','DS602','DS603','DS605','DS607','DS610','DS611','DS612'
													,'DS42','DS021','DS169')
								Then 'Employment'

								when DeclineCode in ('DS991','DS650','DS651','DS990','DS991','DS992','DS31','DS62','DS64','DS74','DS777'
													,'DS778','DS788','DS789','DS790','DS791','DS792','DS793','DS794','DS795','DS796','DS797'
													,'DS798','DS799','DS779', 'DS993', 'DS994', 'DS995', 'DS996',  'DS983', 'DS984', 'DS985', 'DS986', 'DS987' ,'DS988')
								Then 'Scoring'

								when DeclineCode in ('DS81','DS83','DS997','DS998')
								THEN 'CD/RECENCY'

								when DeclineCode in ('DS608')
								THEN 'High Risk >2 loans'

								when DeclineCode in ('DS500')
								THEN 'No Offer Could Be Generated'

								when DeclineCode in ('DS294','DS261','DS251','DS48','DS93','DS142','DS143')
								THEN 'Bureau'
								
								ELSE 'Unkown' END
;
update CreditProfitability.dbo.KM_Appflow_WaterFall
set DeclineCategory = CASE WHEN DeclineSubCategory IN ('Age', '40 days since last disbursal','Employment','Scoring'
													  ,'High Risk >2 loans','No Offer Could Be Generated','Bureau')
													  THEN 'Credit Rules'
													  ELSE DeclineCategory
													  END


;
Update CreditProfitability.dbo.KM_Appflow_WaterFall
Set Min_Afford = CASE	WHEN CAST(MaxInstalment5 AS BIGINT) <= CAST(Maxinstalment2 AS BIGINT)
						AND CAST(MaxInstalment5 AS BIGINT) <= CAST(maxinstalment4 AS BIGINT)
						AND CAST(MaxInstalment5 AS BIGINT) <= CAST(maxinstalment6 AS BIGINT)
						AND CAST(MaxInstalment5 AS BIGINT) <= CAST(maxinstalment7 AS BIGINT)
						THEN CAST(MaxInstalment5 AS BIGINT)

						WHEN  CAST(maxinstalment6 AS BIGINT) <= CAST(maxinstalment2 AS BIGINT)
						AND   CAST(maxinstalment6 AS BIGINT) <= CAST(Maxinstalment4 AS BIGINT) 
						AND   CAST(maxinstalment6 AS BIGINT) <= CAST(MaxInstalment5 AS BIGINT)
						AND   CAST(maxinstalment6 AS BIGINT) <= CAST(maxinstalment7 AS BIGINT)
						THEN  CAST(maxinstalment6 AS BIGINT)

						WHEN CAST(maxinstalment4 AS BIGINT) <= CAST(maxinstalment2 AS BIGINT)
						AND  CAST(maxinstalment4 AS BIGINT) <= CAST(MaxInstalment5 AS BIGINT)
						AND  CAST(maxinstalment4 AS BIGINT) <= CAST(maxinstalment6 AS BIGINT) 
						AND  CAST(maxinstalment4 AS BIGINT) <= CAST(maxinstalment7 AS BIGINT)
						THEN CAST(maxinstalment4 AS BIGINT)

						WHEN  CAST(maxinstalment7 AS BIGINT) <= CAST(maxinstalment2 AS BIGINT)
						AND   CAST(maxinstalment7 AS BIGINT) <= CAST(Maxinstalment4 AS BIGINT)
						AND   CAST(maxinstalment7 AS BIGINT) <= CAST(MaxInstalment5 AS BIGINT)
						AND   CAST(maxinstalment7 AS BIGINT) <= CAST(maxinstalment6 AS BIGINT)
						THEN  CAST(maxinstalment7 AS BIGINT)

						when  CAST(Maxinstalment2 AS BIGINT) <= CAST(maxinstalment4 AS BIGINT)
						AND   CAST(Maxinstalment2 AS BIGINT) <= CAST(MaxInstalment5 AS BIGINT)
						AND   CAST(Maxinstalment2 AS BIGINT) <= CAST(maxinstalment6 AS BIGINT)
						AND   CAST(Maxinstalment2 AS BIGINT) <= CAST(maxinstalment7 AS BIGINT)
						THEN  CAST(Maxinstalment2 AS BIGINT)
						END;

Update CreditProfitability.dbo.KM_Appflow_WaterFall
Set Income_bucket = CASE	WHEN monthlygrossincome >= 0 AND monthlygrossincome <= 2999 THEN 'Above Zero But Below R3000'
							WHEN monthlygrossincome >= 3000 THEN 'R3000 & Above'
							ELSE 'Below Zero'
							END
;
Update CreditProfitability.dbo.KM_Appflow_WaterFall
Set Afford_bucket = CASE	WHEN Min_Afford >= 0 AND  Min_Afford <= 249 THEN 'Above Zero But Below R350'
							WHEN Min_Afford > 350 THEN 'R350 And Above'
							ELSE 'Below Zero'
							END

;
UPDATE CreditProfitability.dbo.KM_Appflow_WaterFall
SET DeclineCategory = CASE	WHEN Afford_bucket = 'Below Zero' AND DeclineCode IN ('DS500') THEN 'Compliance Internal'
							WHEN Afford_bucket = 'Above Zero But Below R350' AND DeclineCode IN ('DS500') THEN 'Compliance Internal'
							ELSE DeclineCategory
							END
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*------------------------------------------------- BASE SCORE ----------------------------------------------------------------*/
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'KM_V6AppScore')
  BEGIN
    DROP TABLE creditprofitability.dbo.KM_V6AppScore
  END;

CREATE TABLE creditprofitability.dbo.KM_V6AppScore
WITH (DISTRIBUTION = HASH(Uniqueid), CLUSTERED COLUMNSTORE INDEX)
AS

SELECT DISTINCT  a.loanid
				,a.UniqueId
				,a.CreationMonth
				,a.UniqueIndicator
				,a.InCapri
				,a.Origination_source
				,b.V6@App			
FROM Creditprofitability.dbo.Central_CreditAppflow_Pivotdata AS a
LEFT JOIN (SELECT UniqueId
                 ,case when scorecardversion = 'V645' then 1000 - (V640*1000)
					   when scorecardversion in ('V636','V635') then 1000 - (V630*1000)
					   when scorecardversion = 'V622' then 1000 - (V620*1000)
					   else 0 end as V6@App
				 ,rank () over (partition by uniqueid order by applicationtime desc) as Latest
			FROM DEV_DataDistillery_General.dbo.TU_applicationbase) AS b
ON a.UniqueID = b.UniqueID
WHERE b.latest = 1
	) by APS;
	DISCONNECT FROM APS;
quit;

/*

SELECT TOP 1 * FROM DEV_DataDistillery_General.dbo.TU_applicationbase

SELECT TOP 5 * FROM Creditprofitability.dbo.Central_CreditAppflow_Pivotdata

SELECT * FROM creditprofitability.dbo.KM_V6AppScore
WHERE LoanId in ('9450155243'
,'9450098866'
,'9450047222'
,'9450132672'
,'9450267534'
,'9448475625'
,'9450158714'
,'9450181996'
,'9449972975'
,'9448517531'
,'9448666012'
,'9450173126'
,'9448994843'
,'9448786133'
,'9450182356'
,'9449043261'
,'9450240483'
,'9448473475'
,'9449070647'
,'9450248888'
,'9450104849'
,'9450246932'
,'9448662824'
,'9450178629'
,'9450183594'
,'9449014748'
,'9450148208'
,'9450104669'
,'9450152990'
,'9448438261'
,'9450129331'
,'9450170081'
,'9450142400'
,'9448498112'
,'9449029083')
order by LoanId

Select TOP 35 loanid from creditprofitability.dbo.KM_V6AppScore
group by loanid having count(loanid)>1 


SELECT TOP 5 * FROM DEV_DataDistillery_General.dbo.TU_applicationbase
ORDER BY UNIQUEID

*/






