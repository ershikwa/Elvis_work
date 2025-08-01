/* Final Afforadability Table Backup */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'SZ_Daily_Affordability_Table_BackUp')
 BEGIN
 DROP TABLE Creditprofitability.dbo.SZ_Daily_Affordability_Table_BackUp
 END;

CREATE TABLE CreditProfitability.dbo.SZ_Daily_Affordability_Table_BackUp
WITH 
(DISTRIBUTION = HASH(loanid))
AS
SELECT * FROM creditprofitability.dbo.SZ_Daily_Affordability_Table)
 
by APS;
DISCONNECT FROM APS;
quit;


/* All Apps */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Apps')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_Apps
 END;

CREATE TABLE CreditProfitability.dbo.TS_Apps
WITH (
      DISTRIBUTION = HASH(Loanid),
	   CLUSTERED COLUMNSTORE INDEX )
	   AS
Select distinct
NationalID,
loanid,
LOANREFERENCE,
Creationmonth,
Creationdate,
ApplicationStatus,
Branch,
Origination_source,
Clientnumber,
Accountnumber,
Scorecard,
Scoremodel,
Subgroupcode,
Persal,
SCORECARDVERSION ,
Uniqueindicator,
Incapri,
Transequence
FROM (Select IDNUMBER AS NationalID, Loanid
,LOANREFERENCE
     ,Origination_source
     ,Uniqueindicator
     ,InCapri
	 ,Status as ApplicationStatus
	 ,Branchcode as Branch
	 ,Clientnumber
	 ,Accountnumber
	 ,Scorecard
	 ,Scoremodel
	 ,Subgroupcode
	 ,Persal
	 ,SCORECARDVERSION 
	 ,Transequence
     ,rank () over (partition by loanid order by uniqueid desc) as Latest
	 ,concat(left(cast(cast(creationdate as date)as varchar),4),substring(cast(cast(creationdate as date)as varchar),6,2)) as creationmonth
	,concat(left(cast(cast(creationdate as date)as varchar),4),substring(cast(cast(creationdate as date)as varchar),6,2),substring(cast(cast(creationdate as date)as varchar),9,2)) as creationdate
from          PRD_Credit_Central.credcentral.VW_MASTER_APPLICATIONS_TABLE 
Where    	  datediff(month,creationdate,getdate()) <= 2
              and (Origination_Source NOT IN ('PROSPECT') OR Origination_Source IS NULL)
              and idnumber not in ('5512309909082',  '5512259900081','5512259970084' , '5512259930088',  '5512299999085',  '5512229999080', '5512239980088')  
                       and BranchCode not in ('1999')   
              and BranchCode NOT BETWEEN 80000 and 85999 ) A
Where  A.latest =1) 
by APS;
DISCONNECT FROM APS;
quit;


/* Eliminating Dupllicates */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Apps_2')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_Apps_2
 END;

select top 0  A.*
into creditprofitability.dbo.TS_Apps_2
FROM 
(SELECT top 0 *,  
row_number() over (partition by Loanid order by Creationdate desc,Loanid desc) as r2 
from creditprofitability.dbo.TS_Apps ) AS A
WHERE r2 =1;

insert into creditprofitability.dbo.TS_Apps_2
select A.*
FROM (SELECT *,  row_number() over (partition by Loanid order by Creationdate desc,Loanid desc) as r2 
            from creditprofitability.dbo.TS_Apps ) AS A
WHERE r2 =1
) 
by APS;
DISCONNECT FROM APS;
quit;


/* Capri Affordability */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Capri_Affordability')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_Capri_Affordability
 END;

CREATE TABLE CreditProfitability.dbo.TS_Capri_Affordability
WITH (
      DISTRIBUTION = HASH(Tranappnumber),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
 TRANAPPNUMBER
,uniqueid
,ABLoansOnPayslip
,TotalPayrolLoans
,JointHomeLoanInstalment
,Bonusses
,Overtime
,Comissiom
,Nonrecurringincome
,TotalExactusInstalment
,Bureaudebtinclusions
,Payslipadjustment
,MonthlyNLRInstalmentOther2
,MonthlyCCAInstalmentOther2
,PayslipTaxPaid
,MonthlyGrossIncome
FROM (Select 
TRANAPPNUMBER
 ,uniqueid
,ABLoansOnPayslip
,TotalPayrolLoans
,JointHomeLoanInstalment
,Bonusses
,Overtime
,Comissiom
,Nonrecurringincome
,TotalExactusInstalment
,Bureaudebtinclusions
,Payslipadjustment
,MonthlyNLRInstalmentOther2
,MonthlyCCAInstalmentOther2
,PayslipTaxPaid
,MonthlyGrossIncome
 ,rank () over (partition by TRANAPPNUMBER order by uniqueid desc) as Latest  
from       prd_Press.Capri.Capri_Affordability_2021 as K
where datediff(month,applicationdate,getdate()) <= 2) A
Where  A.latest =1) 

by APS;
DISCONNECT FROM APS;
quit;


/* Affordability Measures */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_AffordabilityMeasures')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_AffordabilityMeasures
 END;

CREATE TABLE CreditProfitability.dbo.TS_AffordabilityMeasures
WITH (
      DISTRIBUTION = HASH(Tranappnumber),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
 TRANAPPNUMBER
,uniqueid
,CreationDate
,MaximumInstalment2
,MaximumInstalment4
,MaximumInstalment5
,MaximumInstalment7
FROM (Select 
TRANAPPNUMBER
 ,uniqueid
 ,left(applicationdate,10) as Creationdate
,MaxNaedoBase as MaximumInstalment2
,MaxABExposureBase as MaximumInstalment4
,Maxcompliancebase as MaximumInstalment5
,Max7base as MaximumInstalment7
 ,rank () over (partition by TRANAPPNUMBER order by uniqueid desc) as Latest  
from       prd_Press.capri.capri_affordability_results as K
where datediff(month,applicationdate,getdate()) <= 2) A
Where  A.latest =1
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Risk and Calculated Net Income */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_ClientIncome')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_ClientIncome
 END;

CREATE TABLE CreditProfitability.dbo.TS_ClientIncome
WITH (
      DISTRIBUTION = HASH(RequestID),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
CreationDate
,RequestID
,uniqueid
,CalculatedNetIncome
,RiskNetIncome
,AdditionalIncome
 FROM (Select 
 convert(char(8),Request_timestamp,112) as CreationDate
,RequestID
,uniqueid
,NetIncome as CalculatedNetIncome
,RiskNetIncome
,AdditionalIncome
,rank () over (partition by RequestID order by uniqueid desc) as Latest  
from       prd_Press.capri.clientprofile_income as K
where datediff(month,Request_timestamp,getdate()) <= 2) A
Where  A.latest =1
) 

by APS;
DISCONNECT FROM APS;
quit;


/* CCA and NLR Other 1 */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_InstalmentBase')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_InstalmentBase
 END;

CREATE TABLE CreditProfitability.dbo.TS_InstalmentBase
WITH (
	    DISTRIBUTION = HASH(RequestID),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
RequestID
 ,uniqueid
,BureauDebtExclusions
,JHLAddback
,TotalCCAInstalmentOther1
,TotalNLRInstalmentOther1
,WageModifier
FROM (Select 
RequestID
 ,uniqueid
,BureauDebtExclusions
,JHLAddback
,TotalCCAInstalmentOther1
,TotalNLRInstalmentOther1
,WageModifier
 ,rank () over (partition by RequestID order by uniqueid desc) as Latest  
from   prd_press.Capri.ClientProfile_BaseAffordability_MaxAffordabilityInstalmentBaseFunction as K
where datediff(month,Request_timestamp,getdate()) <= 2) A
Where  A.latest =1
) 

by APS;
DISCONNECT FROM APS;
quit;


/* CCA and NLR Other 2 */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_NLRandCCA')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_NLRandCCA
 END;

CREATE TABLE CreditProfitability.dbo.TS_NLRandCCA
WITH (
      DISTRIBUTION = HASH(RequestID),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
RequestID,
uniqueid,
totalNLRInstalmentOther2,
totalCCAInstalmentOther2
FROM (Select 
RequestID,
uniqueid,
totalNLRInstalmentOther2,
totalCCAInstalmentOther2,
 rank () over (partition by RequestID order by uniqueid desc) as Latest  
from   PRD_Press.capri.ClientProfile_BaseAffordability_MaxComplianceInstalmentBaseFunction as K
where datediff(month,Request_timestamp,getdate()) <= 2) A
Where  A.latest =1
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Offers */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Offers')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_Offers
 END;

CREATE TABLE CreditProfitability.dbo.TS_Offers
WITH (
      DISTRIBUTION = HASH(Tranappnumber),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
Tranappnumber
,Applicationdate
,Uniqueid
,OFFERINDICATOR
,Offer_MaxOVERDRAFT
,Offer_MaxCapCard
,AvailableClientFacility
,Offer_MaxCapital
,Offer_MaxTerm
,PRODUCTCATEGORY
from (select
Tranappnumber
,Applicationdate
,Uniqueid
,OFFERINDICATOR
,AvailableClientFacility
,LoanCapital as Offer_MaxCapital
,CardLimit as Offer_MaxCapCard
,OVERDRAFTLIMIT as Offer_MaxOVERDRAFT
,Term as Offer_MaxTerm
,PRODUCTCATEGORY
 ,row_number() over (partition by tranappnumber order by applicationdate desc,LoanCapital desc) as Latest  
from prd_Press.capri.capri_offer_results_2021
where datediff(month,applicationdate,getdate()) <= 2) as A
Where A.latest =1
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Disbursed Loan */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_DisbursedLoan')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_DisbursedLoan
 END;

CREATE TABLE CreditProfitability.dbo.TS_DisbursedLoan
WITH (
      DISTRIBUTION = HASH(Loanid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
Loanid
,loanreference
,Applicationdate
,Clientnumber
,dis_caploanonly
,dis_termloanonly
from (select
Loanid
,loanreference
,left(Startdate,6) as Applicationdate
,Clientnumber
,Principaldebt as dis_caploanonly
,Term as dis_termloanonly
 ,row_number() over (partition by loanid order by LastUpdateTime desc) as Latest  
from prd_exactussync.dbo.ZA31200P
where Startdate > 0 and 
      datediff(month,cast((cast(Startdate as varchar)) as datetime),getdate()) <= 2) as A
Where A.latest =1
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Disbursed Card */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_DisbursedCard')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_DisbursedCard
 END;

CREATE TABLE CreditProfitability.dbo.TS_DisbursedCard
WITH (
      DISTRIBUTION = HASH(Clientnumber),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
Applicationdate
,Accountreference
,Clientnumber
,dis_capcardonly
from (select
left(Startdate,6) as Applicationdate
,Accountreference
,Clientnumber
,AccountLimit as dis_capcardonly
 ,row_number() over (partition by Clientnumber order by Timestamp desc) as Latest  
from prd_exactussync.dbo.Ca20000P
where Startdate > 0 and 
      datediff(month,cast((cast(Startdate as varchar)) as datetime),getdate()) <= 2) as A
Where A.latest =1
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Loan Instalment */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_LoanInstalment')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_LoanInstalment
 END;

CREATE TABLE CreditProfitability.dbo.TS_LoanInstalment
WITH (
      DISTRIBUTION = HASH(loanref),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
loanref
,Eveninstalment as Instalment
from Provisions.dbo.Payments_Table
where stmt_nr = '0'
and product = 'bank'
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Card Instalment */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_CardInstalment')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_CardInstalment
 END;

CREATE TABLE CreditProfitability.dbo.TS_CardInstalment
WITH (
      DISTRIBUTION = HASH(loanref),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
loanref
,Eveninstalment as Instalment
from Provisions.dbo.Payments_Table
where stmt_nr = '0'
and product = 'Card'

) 

by APS;
DISCONNECT FROM APS;
quit;


/* Disbursals */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Disbursals')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_Disbursals
 END;

CREATE TABLE CreditProfitability.dbo.TS_Disbursals
WITH (
      DISTRIBUTION = HASH(Loanid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select 
 A.Loanid
,dis_caploanonly
,dis_termloanonly
,e.Instalment as Card_Instalment
,dis_capcardonly
,d.Instalment as Loan_Instalment
,a.loanreference
,C.Accountreference
,OD.CurrentLimit as dis_capODonly
,dis_insODonly = OD.CurrentLimit * (OD.RepaymentPercent/100)
,Case when d.Instalment >=0 then 'Loan_Only'
	  when OD.CurrentLimit >=0 then 'Overdraft_Only'
	  when e.Instalment >=0 then 'Card_Only'
	  when d.Instalment >=0  and  e.Instalment >=0  then 'Combo' 
	  else null end as Product_Type,
	 row_number() over (partition by a.loanid order by a.loanid) as L  
from  creditprofitability.dbo.TS_Apps_2 as A
left join creditprofitability.dbo.TS_DisbursedLoan as B
on A.Loanid = B.loanid
left join creditprofitability.dbo.TS_DisbursedCard as C
on A.ClientNumber = C.Clientnumber
left join creditprofitability.dbo.TS_LoanInstalment as D
on B.loanreference = D.Loanref
left join creditprofitability.dbo.TS_CardInstalment as E
on C.Accountreference = E.loanref
left join creditprofitability.dbo.overdraft_pricing as OD
on a.loanid = OD.loanid
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Scoring */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Scoreband')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_Scoreband
 END;

CREATE TABLE CreditProfitability.dbo.TS_Scoreband
WITH (
      DISTRIBUTION = HASH(uniqueid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
Tranappnumber
,Uniqueid
,Scoreband
,Scorecard
from (select
Tranappnumber
,Uniqueid
,Scoreband
,Scorecard
,rank () over (partition by Tranappnumber order by Uniqueid desc) as Latest  
from  prd_press.Capri.CAPRI_SCORING_RESULTS_2021
where datediff(month,applicationdate,getdate()) <= 2) as A
Where A.latest =1
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Scoring PD */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_ScoringPD')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_ScoringPD
 END;

CREATE TABLE CreditProfitability.dbo.TS_ScoringPD
WITH (
      DISTRIBUTION = HASH(Tranappnumber),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Tranappnumber, scoringPD
From
(select RequestID as Tranappnumber,
        Uniqueid, 
        scoringPD,
		rank () over (partition by RequestID order by Uniqueid desc) as Latest
from PRD_press.[Capri].[creditrisk_RiskGroup]
where datediff(month,Request_timestamp,getdate()) <= 2) as A
Where A.latest =1
) 

by APS;
DISCONNECT FROM APS;
quit;


/* RTI */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_RTICaps')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_RTICaps
 END;

CREATE TABLE CreditProfitability.dbo.TS_RTICaps
WITH (
      DISTRIBUTION = HASH(uniqueid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
RequestID
,Request_timestamp
,Uniqueid
,finalRTICap
,OptimalRTI
,MaxRTICap
,AffordabilityTestingStrategy
,UnsecuredAndRevolvingDebt
from (select
RequestID
,Request_timestamp
,Uniqueid
,MinRTI_Cap as finalRTICap
,Final_GS_RTI as OptimalRTI
,RTI_Cap as MaxRTICap
,AffordabilityTestingStrategy
,UnsecuredAndRevolvingDebt
,row_number() over (partition by RequestID order by Request_timestamp desc,Uniqueid desc) as Latest  
from  prd_press.CAPRI.ClientProfile_MaxRTIInstalment_PDStrategyFunction
where datediff(month,Request_timestamp,getdate()) <= 2) as A
Where A.latest =1
) 

by APS;
DISCONNECT FROM APS;
quit;


/* PD Capital */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_PDCapital')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_PDCapital
 END;

CREATE TABLE CreditProfitability.dbo.TS_PDCapital
WITH (
      DISTRIBUTION = HASH(uniqueid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
RequestID
,Uniqueid
,PDCapital
,AffordabilityTestingStrategy
from (select
RequestID
,Uniqueid
,pDBaseRiskMaxLoanCapitalAdj as PDCapital
,AffordabilityTestingStrategy
 ,rank () over (partition by RequestID order by Uniqueid desc) as Latest  
from  prd_press. CAPRI.ClientProfile_MaxRTIInstalment_PDStrategyFunction
where datediff(month,Request_timestamp,getdate()) <= 2) as A
Where A.latest =1
and AffordabilityTestingStrategy = 'RTI80'
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Max 6 */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Max6')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_Max6
 END;

CREATE TABLE CreditProfitability.dbo.TS_Max6
WITH (
      DISTRIBUTION = HASH(uniqueid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
Creationmonth
,RequestID
,Uniqueid
,MaximumInstalment6
 from (select
 convert(char(8),Request_timestamp,112) as Creationmonth
,RequestID
,Uniqueid
,instalmentMaxRTI as MaximumInstalment6
,row_number() over (partition by RequestID order by Request_timestamp desc,Uniqueid desc) as Latest  
from  prd_press.[Capri].[ClientProfile_MaxRTIInstalment_RTIFunction]
where datediff(month,Request_timestamp,getdate()) <= 2) as A
Where A.latest =1
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Max 7 variables */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Max7')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_Max7
 END;

CREATE TABLE CreditProfitability.dbo.TS_Max7

WITH (
      DISTRIBUTION = HASH(uniqueid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
RequestID
,Uniqueid
,Request_timestamp
,TaxBack2
,TaxBackPerc
,MaximumInstalment7	= isnull(Max7Final,Max7InstalmentBase)
,Max7RTICapPerc 
,Max7InstalmentBase
,Max7RTICapVal 
,Max7Test
,Max7Final
,MinComplianceExpenses
,TotalExpenses
, case when WageModifier = '1.0' then 'M'
		when WageModifier = '2.1667' then 'F'
		when WageModifier = '4.333'  then 'W'
		else null end as WageType
,monthlyNetIncome
,WageModifier
, case when WageModifier = '1.0' then monthlyNetIncome
		when WageModifier = '2.1667' then monthlyNetIncome*2.1667
		when WageModifier = '4.333' then monthlyNetIncome*4.333
		else monthlyNetIncome end as NetSalaryMonthly 
from (select
RequestID
,K.Uniqueid
,K.Request_timestamp
,TaxBack2
,TaxBackPerc
,Max7RTICapPerc 
,Max7InstalmentBase
,Max7RTICapVal 
,Max7Test
,Max7Final
,MinComplianceExpenses
,TotalExpenses
,convert(float,monthlyNetIncome) as monthlyNetIncome
,WageModifier
, row_number() over (partition by RequestID order by K.Request_timestamp desc,K.Uniqueid desc)
  as Latest  
from  prd_press.capri.ClientProfile_Max7 as K
left join creditprofitability.dbo.SZ_BaseData_Firstdeploy_WageType As L
on  convert(varchar,K.Uniqueid) = convert(varchar,L.Uniqueid)
where datediff(month,K.Request_timestamp,getdate()) <= 2) as A
Where A.latest =1
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Other Income */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_IncomeSupport')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_IncomeSupport
 END;

CREATE TABLE CreditProfitability.dbo.TS_IncomeSupport
WITH (
      DISTRIBUTION = HASH(uniqueid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
RequestID
,Uniqueid
,Commission1
,Commission2
,Overtime1
,Overtime2  
,TaxBackValue
from (select
RequestID
,Uniqueid
,Commission1
,Commission2
,Overtime1
,Overtime2  
,TaxBackValue
 ,rank () over (partition by RequestID order by Uniqueid desc) as Latest  
from  prd_press.Capri.ClientProfile_IncomeSupport
where datediff(month,Request_timestamp,getdate()) <= 2) as A
Where A.latest =1
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Converting Affordability Measures to Float */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_MaxInstalments')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_MaxInstalments
 END;

CREATE TABLE CreditProfitability.dbo.TS_MaxInstalments
WITH (
      DISTRIBUTION = HASH(Tranappnumber),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select 
Tranappnumber
,A.CreationDate
,convert(float,maximuminstalment2) as maximuminstalment2
,convert(float,maximuminstalment4) as maximuminstalment4
,convert(float,maximuminstalment5) as maximuminstalment5
,round(convert(float,maximuminstalment6),0) as maximuminstalment6
,convert(float,maximuminstalment7) as maximuminstalment7
from creditprofitability.dbo.TS_AffordabilityMeasures as A
left join creditprofitability.dbo.TS_Max6 as b
on a.Tranappnumber = B.RequestID
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Minimum Affordability */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_MinAfford')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_MinAfford
 END;

CREATE TABLE CreditProfitability.dbo.TS_MinAfford
WITH (
      DISTRIBUTION = HASH(Tranappnumber),
	   CLUSTERED COLUMNSTORE INDEX )
as 
select Tranappnumber,
case	when MaximumInstalment5 <= Maximuminstalment2 
						and  MaximumInstalment5 <=  maximuminstalment4 
						and  MaximumInstalment5 <=  maximuminstalment6 
						and  MaximumInstalment5 <=  maximuminstalment7 
						THEN MaximumInstalment5

						when  maximuminstalment6 <= maximuminstalment2 
						and   maximuminstalment6 <= Maximuminstalment4 
						and   maximuminstalment6 <=  MaximumInstalment5 
						and   maximuminstalment6 <=  maximuminstalment7 
						THEN  maximuminstalment6

						when maximuminstalment4 <= maximuminstalment2 
						and  maximuminstalment4 <= MaximumInstalment5
						and  maximuminstalment4 <=  maximuminstalment6 
						and  maximuminstalment4 <=  maximuminstalment7  
						THEN maximuminstalment4

						when  maximuminstalment7 <= maximuminstalment2
						and   maximuminstalment7 <= Maximuminstalment4 
						and   maximuminstalment7 <=  MaximumInstalment5 
						and   maximuminstalment7 <=  maximuminstalment6 
						THEN  maximuminstalment7

						when  Maximuminstalment2 <= maximuminstalment4
						and   Maximuminstalment2 <= MaximumInstalment5
						and   Maximuminstalment2 <=  maximuminstalment6 
						and   Maximuminstalment2 <=  maximuminstalment7 
						THEN  Maximuminstalment2
				        else 0 end as Min_Afford
from creditprofitability.dbo.TS_MaxInstalments as a
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Average Gross Income */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_AvgGross')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_AvgGross
 END;

CREATE TABLE CreditProfitability.dbo.TS_AvgGross
WITH (
      DISTRIBUTION = HASH(uniqueid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
RequestID
,Uniqueid
,avgGrossIncome
,CreationMonth
from (select
RequestID
,Uniqueid
,avgGrossIncome
,convert(char(6),Request_timestamp,112) as Creationmonth
 ,rank () over (partition by RequestID order by Uniqueid desc) as Latest  
from  prd_press.capri.CreditRisk_AverageGrossIncome
where datediff(month,Request_timestamp,getdate()) <= 2) as A
Where A.latest =1
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Additional Income */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_AdditionalIncome')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_AdditionalIncome
 END;

CREATE TABLE CreditProfitability.dbo.TS_AdditionalIncome
WITH (
      DISTRIBUTION = HASH(uniqueid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
RequestID
,Uniqueid
,AvgAdditionalIncome
,AvgNetIncome
from (select
RequestID
,Uniqueid
,AvgAdditionalIncome
,AvgNetIncome
 ,rank () over (partition by RequestID order by Uniqueid desc) as Latest  
from prd_press.capri.ClientProfile_Income
where datediff(month,Request_timestamp,getdate()) <= 2) as A
Where A.latest =1
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Max4 Percent */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Max4Perc')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_Max4Perc
 END;

CREATE TABLE CreditProfitability.dbo.TS_Max4Perc
WITH (
      DISTRIBUTION = HASH(uniqueid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
RequestID
,Uniqueid
, Max4Percentage
from (select
RequestID
,Uniqueid
,CappedABexpinst as Max4Percentage
 ,rank () over (partition by RequestID order by Uniqueid desc) as Latest  
from prd_press.Capri.ClientProfile_BaseAffordability_ABExpInstCappedDecisionTable
where datediff(month,Request_timestamp,getdate()) <= 2) as A
Where A.latest =1
) 

by APS;
DISCONNECT FROM APS;
quit;


/* RTI Offered */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_RTIOffered')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_RTIOffered
 END;

CREATE TABLE CreditProfitability.dbo.TS_RTIOffered
WITH (
      DISTRIBUTION = HASH(Tranappnumber),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
Tranappnumber
,Min_afford
,RiskNetIncome
,UnsecuredAndRevolvingDebt
,case when risknetincome>0 then (((isnull(a.Min_afford,0)
					+isnull(a.UnsecuredAndRevolvingDebt,0))
					/a.Risknetincome)*100) else null end as RTI_Offered
from (select
A.Tranappnumber
,A.Min_afford
,convert(float,B.RiskNetIncome) as RiskNetIncome
,convert(float,C.UnsecuredAndRevolvingDebt) as UnsecuredAndRevolvingDebt
from creditprofitability.dbo.TS_MinAfford as A
left join creditprofitability.dbo.TS_RTICaps as C
on A.Tranappnumber = C.RequestID
left join creditprofitability.dbo.TS_ClientIncome as B
on A.Tranappnumber = B.RequestID) as A
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Loan Product Limits */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Join')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_Join
 END;

CREATE TABLE CreditProfitability.dbo.TS_Join
WITH (
      DISTRIBUTION = HASH(loanid),
       CLUSTERED COLUMNSTORE INDEX )
as 
Select 
loanid,
A.scorecardversion,
creationdate,
K.scoreband,
M.scoringPD,
B.WageModifier,
R.STRATEGY2RANDOMNUMBER
from creditprofitability.dbo.TS_Apps_2 as A
left join  creditprofitability.dbo.TS_Scoreband as K
on convert(varchar,A.Loanid) = convert(varchar,K.Tranappnumber)
left join  Creditprofitability.dbo.TS_ScoringPD as M
on convert(varchar,A.Loanid) = convert(varchar,M.Tranappnumber)
left join  creditprofitability.dbo.TS_InstalmentBase as b
on convert(varchar,A.Loanid) = convert(varchar,B.Requestid)
left join PRD_Press.CAPRI.CAPRI_TESTING_STRATEGY_RESULTS R
on convert(varchar,A.Loanid) = convert(varchar,R.Tranappnumber)
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Loan Product Limits V635 V636 */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Loan_Product_Limits_V635_V636')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_Loan_Product_Limits_V635_V636
 END;

CREATE TABLE CreditProfitability.dbo.TS_Loan_Product_Limits_V635_V636
WITH (
      DISTRIBUTION = HASH(Loanid),
       CLUSTERED COLUMNSTORE INDEX )
as 
select Loanid,
concat(left(cast(cast(creationdate as date)as varchar),4),substring(cast(cast(creationdate as date)as varchar),6,2)) as Creationmonth,
WageModifier,
scorecardversion,
scoreband,
 case when STRATEGY2RANDOMNUMBER >= 0.5 then
(case 
 when scoreband = 50 and WageModifier =   '1.0'     and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585')  then 350000
 when scoreband = 51 and WageModifier =   '1.0'     and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585')  then 250000
 when scoreband = 52 and WageModifier =   '1.0'     and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585')  then 180000
 when scoreband = 53 and WageModifier =   '1.0'     and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585')  then  20000
 when scoreband = 50 and WageModifier in ('2.1667') and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585')  then 350000
 when scoreband = 51 and WageModifier in ('2.1667') and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585')  then 250000
 when scoreband = 52 and WageModifier in ('2.1667') and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585')  then 180000
 when scoreband = 53 and WageModifier in ('2.1667') and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585')  then  20000
 when scoreband = 50 and WageModifier in ('4.333')  and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585')  then 350000
 when scoreband = 51 and WageModifier in ('4.333')  and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585')  then 250000
 when scoreband = 52 and WageModifier in ('4.333')  and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585')  then 180000
 when scoreband = 53 and WageModifier in ('4.333')  and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585')  then  18000
                    else Null end)  
 else					
(case 
 when scoreband = 50 and WageModifier =   '1.0'     and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585','V575')  then 250000
 when scoreband = 51 and WageModifier =   '1.0'     and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585','V575')  then 250000
 when scoreband = 52 and WageModifier =   '1.0'     and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585','V575')  then 180000
 when scoreband = 53 and WageModifier =   '1.0'     and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585','V575')  then  20000
 when scoreband = 50 and WageModifier in ('2.1667') and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585','V575')  then 250000
 when scoreband = 51 and WageModifier in ('2.1667') and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585','V575')  then 250000
 when scoreband = 52 and WageModifier in ('2.1667') and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585','V575')  then 180000
 when scoreband = 53 and WageModifier in ('2.1667') and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585','V575')  then  20000
 when scoreband = 50 and WageModifier in ('4.333')  and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585','V575')  then 250000
 when scoreband = 51 and WageModifier in ('4.333')  and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585','V575')  then 250000
 when scoreband = 52 and WageModifier in ('4.333')  and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585','V575')  then 180000
 when scoreband = 53 and WageModifier in ('4.333')  and SCORECARDVERSION in ('V635','V636','V622','V645','V655','V585','V575')  then  18000
                    else Null end) end as Loan_Product_Limits__V635_V636
                from creditprofitability.dbo.TS_Join as A
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Loan Product Limits */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Loan_Product_Limits')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_Loan_Product_Limits
 END;

CREATE TABLE CreditProfitability.dbo.TS_Loan_Product_Limits
WITH (
      DISTRIBUTION = HASH(Loanid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
select
 A.Loanid,
concat(left(cast(cast(A.creationdate as date)as varchar),4),substring(cast(cast(A.creationdate as date)as varchar),6,2)) as Creationmonth,
A.WageModifier,
A.scorecardversion,
A.scoreband,
Loan_Product_Limits__V635_V636 as Loan_Product_Limits
from creditprofitability.dbo.TS_Join as A
left join creditprofitability.dbo.TS_Loan_Product_Limits_V635_V636 as B
on A.loanid = B.loanid
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Internally and Externally settled loans */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_SRA_And_CL')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_SRA_And_CL
 END;

CREATE TABLE CreditProfitability.dbo.TS_SRA_And_CL
WITH (
      DISTRIBUTION = HASH(Applicationid),
	   CLUSTERED COLUMNSTORE INDEX)
AS 
select ApplicationID
,Internalsettledloans = sum(case when TradeType = 'Int' then isnull(Instalment,0) else 0 end)
,IntBalSettled = sum(case when TradeType = 'Int' then isnull(SettlementAmount,0) else 0 end)
,IntCount = sum(case when TradeType = 'Int' then 1 else 0 end)
,Externalsettledloans = sum(case when TradeType = 'Ext' then isnull(Instalment,0) else 0 end)
,ExtBalSettled = sum(case when TradeType = 'Ext' then isnull(SettlementAmount,0) else 0 end)
,ExtCount = sum(case when TradeType = 'Ext' then 1 else 0 end)
from (select distinct
a.ApplicationID, a.OfferID, a.UniqueID, b.TradeSequenceID,
c.TradeType, c.SubscriberName, c.Instalment, c.OutstandingBalance, c.SettlementAmount
from PRD_ExactusSync.dbo.ApplicationOffers a
left join PRD_ExactusSync.dbo.APPLICATIONOFFERTRADEMAP b
on a.ApplicationID = b.ApplicationID and a.UniqueID = b.UNQUEID and a.OfferID = b.OfferID
left join PRD_ExactusSync.dbo.APPLICATIONTRADES c
on b.TradeSequenceID = c.SEQUENCEID
where a.OFFERSSELECTEDTYPE = 'SEL') as SQ
group by SQ.ApplicationID
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Internally and Externally settled loans - 1 */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_SRA_CL')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_SRA_CL
 END;

CREATE TABLE CreditProfitability.dbo.TS_SRA_CL
WITH (
      DISTRIBUTION = HASH(Loanid),
	   CLUSTERED COLUMNSTORE INDEX)
AS 
 select distinct
A.Loanid,
sum(B.internalsettledloans) as internalsettledloans,
sum(B.externalsettledloans) as externalsettledloans
from creditprofitability.dbo.TS_Apps_2 as A
left join Creditprofitability.dbo.TS_SRA_And_CL as B
on A.Loanid = B.Applicationid
group by loanid

) 

by APS;
DISCONNECT FROM APS;
quit;


/* Internally and Externally settled loans - 2 */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_SRA_CL_2')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_SRA_CL_2
 END;

CREATE TABLE CreditProfitability.dbo.TS_SRA_CL_2
WITH (
      DISTRIBUTION = HASH(Loanid),
	   CLUSTERED COLUMNSTORE INDEX)
AS 
 select distinct
Loanid,
internalsettledloans,
externalsettledloans,
Case when isnull(Internalsettledloans,0) > 0 then 'SRA' else 'Not_SRA' end as SRA_Definition,
Case when isnull(Externalsettledloans,0) > 0 then 'CL'  else 'Not_CL'  end as CL_Definition 
from Creditprofitability.dbo.TS_SRA_CL

) 

by APS;
DISCONNECT FROM APS;
quit;


/* Adjusted Affordability Measures */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Adj_Affordabilitymeasures')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_Adj_Affordabilitymeasures
 END;

CREATE TABLE CreditProfitability.dbo.TS_Adj_Affordabilitymeasures
WITH (
      DISTRIBUTION = HASH(Tranappnumber),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
Tranappnumber
,CreationDate
,Maximuminstalment2
,maximuminstalment4
,maximuminstalment5
,maximuminstalment6
,maximuminstalment7
,isnull(MaximumInstalment4,0)+isnull(internalsettledloans,0)as ADJ_MAX4 
,isnull(MaximumInstalment5,0)+isnull(internalsettledloans,0)+isnull(externalsettledloans,0) as ADJ_MAX5 
,isnull(MaximumInstalment6,0)+isnull(internalsettledloans,0)+isnull(externalsettledloans,0) as ADJ_MAX6 
,isnull(MaximumInstalment7,0)+isnull(internalsettledloans,0)+isnull(externalsettledloans,0) as ADJ_MAX7 
from (select
A.Tranappnumber
,A.CreationDate
,A.Maximuminstalment2
,A.maximuminstalment4
,A.maximuminstalment5
,A.maximuminstalment6
,A.maximuminstalment7
,B.externalsettledloans
,B.internalsettledloans
from creditprofitability.dbo.TS_MaxInstalments as A
left join Creditprofitability.dbo.TS_SRA_CL_2 as B
on convert(varchar,A.Tranappnumber) = convert(varchar,B.Loanid)) as A

) 

by APS;
DISCONNECT FROM APS;
quit;


/* Max Afford */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_MaxAfford')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_MaxAfford
 END;

CREATE TABLE CreditProfitability.dbo.TS_MaxAfford
WITH (
      DISTRIBUTION = HASH(Tranappnumber),
	   CLUSTERED COLUMNSTORE INDEX )
as 
select Tranappnumber,
 case when Maximuminstalment2 <= isnull(ADJ_MAX4, maximuminstalment4) and Maximuminstalment2 <= isnull(ADJ_MAX5, maximuminstalment5) and Maximuminstalment2 <= isnull(ADJ_MAX6, maximuminstalment6) and Maximuminstalment2 <= isnull(ADJ_MAX7, maximuminstalment7) THEN maximuminstalment2
						when isnull(ADJ_MAX4, maximuminstalment4) <= Maximuminstalment2 and isnull(ADJ_MAX4, maximuminstalment4) <= isnull(ADJ_MAX5, maximuminstalment5) and isnull(ADJ_MAX4, maximuminstalment4) <= isnull(ADJ_MAX6, maximuminstalment6) and isnull(ADJ_MAX4, maximuminstalment4) <= isnull(ADJ_MAX7, maximuminstalment7) THEN isnull(ADJ_MAX4, maximuminstalment4)
						when isnull(ADJ_MAX5, maximuminstalment5) <= Maximuminstalment2 and isnull(ADJ_MAX5, maximuminstalment5) <= isnull(ADJ_MAX4, maximuminstalment4) and isnull(ADJ_MAX5, maximuminstalment5) <= isnull(ADJ_MAX6, maximuminstalment6) and isnull(ADJ_MAX5, maximuminstalment5) <= isnull(ADJ_MAX7, maximuminstalment7) THEN isnull(ADJ_MAX5, maximuminstalment5)
						when isnull(ADJ_MAX6, maximuminstalment6) <= Maximuminstalment2 and isnull(ADJ_MAX6, maximuminstalment6) <= isnull(ADJ_MAX4, maximuminstalment4) and isnull(ADJ_MAX6, maximuminstalment6) <= isnull(ADJ_MAX5, maximuminstalment5) and isnull(ADJ_MAX6, maximuminstalment6) <= isnull(ADJ_MAX7, maximuminstalment7) THEN isnull(ADJ_MAX6, maximuminstalment6)
						when isnull(ADJ_MAX7, maximuminstalment7) <= Maximuminstalment2 and isnull(ADJ_MAX7, maximuminstalment7) <= isnull(ADJ_MAX4, maximuminstalment4) and isnull(ADJ_MAX7, maximuminstalment7) <= isnull(ADJ_MAX5, maximuminstalment5) and isnull(ADJ_MAX7, maximuminstalment7) <= isnull(ADJ_MAX7, maximuminstalment7) THEN isnull(ADJ_MAX7, maximuminstalment7)
				else 0 end as Max_Afford
from creditprofitability.dbo.TS_Adj_Affordabilitymeasures
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Exactus Card */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_ExactusCard')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_ExactusCard
 END;

CREATE TABLE CreditProfitability.dbo.TS_ExactusCard
WITH (
      DISTRIBUTION = HASH(uniqueid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
Tranappnumber
,Uniqueid
,ActiveLoans
,TotalExactusCard
,TotalExactusLoan
from (select
K.Tranappnumber
,K.Uniqueid
,L.ActiveLoans
,L.TotalExactusCard
,L.TotalExactusLoan
 ,rank () over (partition by Tranappnumber order by K.Uniqueid desc) as Latest 
from creditprofitability.dbo.TS_Capri_Affordability as K
left join prd_press.capri.CAPRI_ACCOUNT_SUMMARY_2021 as L
on K.Uniqueid = L.Uniqueid
where datediff(month,Applicationdate,getdate()) <= 2) as A
Where A.latest =1
) 

by APS;
DISCONNECT FROM APS;
quit;


/* New Card */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_NewCard')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_NewCard
 END;

CREATE TABLE CreditProfitability.dbo.TS_NewCard
WITH (
      DISTRIBUTION = HASH(Loanid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select
 Loanid
,TotalExactusCard
,case when TotalExactusCard = 0 and k.Instalment > 0 then 1 else 0 end as New_Card_Disbursed
from (select
 A.Loanid
,A.Card_Instalment as Instalment
,B.TotalExactusCard
from creditprofitability.dbo.TS_Disbursals as A
left join creditprofitability.dbo.TS_ExactusCard as B
on convert(varchar,A.Loanid) = convert(varchar,B.Tranappnumber)) AS k
) 

by APS;
DISCONNECT FROM APS;
quit;


/* RTI Post Disbursal */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_RTIPostDisb_Daily')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_RTIPostDisb_Daily
 END;

CREATE TABLE CreditProfitability.dbo.TS_RTIPostDisb_Daily
WITH (
      DISTRIBUTION = HASH(Loanid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
Loanid
,case when risknetincome>0 
then ((isnull(loan_instalment,0 )+isnull(card_instalment, 0)+isnull(a.dis_insODOnly, 0)
					+isnull(UnsecuredAndRevolvingDebt,0)
					-ISNULL(externalsettledloans,0)
					-isnull(internalsettledloans,0)
					-(case when New_Card_Disbursed = 0 and ( card_instalment > 0  ) then totalExactusCard else 0 end))
					/Risknetincome)*100 else null end as RTI_PostDisb
from (select
 A.Loanid
,A.dis_insODOnly
,A.Product_Type
,convert(float,B.RiskNetIncome) as RiskNetIncome
,convert(float,C.UnsecuredAndRevolvingDebt) as UnsecuredAndRevolvingDebt
,D.externalsettledloans
,D.internalsettledloans
,E.totalExactusCard
,E.New_Card_Disbursed
,f.instalment as loan_instalment
,f.ProductDescription as Loan_ProductDescription
,ff.instalment as card_instalment
,ff.productdescription as card_productdescription
from creditprofitability.dbo.TS_Disbursals as A
left join creditprofitability.dbo.TS_RTICaps as C
on convert(varchar,A.Loanid) = convert(varchar,C.RequestID)
left join creditprofitability.dbo.TS_ClientIncome as B
on convert(varchar,A.Loanid) = convert(varchar,B.RequestID)
left join Creditprofitability.dbo.TS_SRA_CL_2 as D
on convert(varchar,A.Loanid) = convert(varchar,D.Loanid)
left join creditprofitability.dbo.TS_NewCard as E
on convert(varchar,A.Loanid) = convert(varchar,E.Loanid)
left join creditprofitability.dbo.loan_pricing_daily f
on convert(varchar,A.Loanid) = convert(varchar,f.Loanid) 
left join creditprofitability.dbo.card_pricing_daily ff
on convert(varchar,A.Loanid)  = convert(varchar,ff.Loanid) 
 ) as A
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Secured and Unsecured debt */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_GazelleExpenses_Final')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_GazelleExpenses_Final
 END;

CREATE TABLE CreditProfitability.dbo.TS_GazelleExpenses_Final
WITH (
      DISTRIBUTION = HASH(loanid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
loanid,
case when BONDREPAYMENTS >0 or VEHICLEREPAYMENT >0 then 'Secured_Debt'
else 'unsecured_debt' end as Debt
from ARC_LOANQ_2028.DBO.LOANQUOTATIONCLIENTEXPENSES
where left(CREATE_DATE,10) >= '2020-01-01' 
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Secured and Unsecured debt - 1 */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_OmniExpenses_Final')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_OmniExpenses_Final
 END;

CREATE TABLE CreditProfitability.dbo.TS_OmniExpenses_Final
WITH (
      DISTRIBUTION = HASH(applicationid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
applicationid,
case when expensetype in ('HOMELOAN','JHLOAN','vehicle') then 'Secured_Debt'
else null end as Debt
from PRD_EXACTUSSYNC.DBO.APPLICATIONEXPENSES
where datediff(month,LASTUPDATETIMESTAMP,getdate()) <= 2
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Secured and Unsecured debt - 2 */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_OmniExpenses_Final2')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_OmniExpenses_Final2
 END;

CREATE TABLE CreditProfitability.dbo.TS_OmniExpenses_Final2
WITH (
      DISTRIBUTION = HASH(applicationid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
applicationid,
Debt
from creditprofitability.dbo.TS_OmniExpenses_Final
where debt is not null
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Secured and Unsecured debt - 3 */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Secured_Unsecured_Expenses_Final')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_Secured_Unsecured_Expenses_Final
 END;

CREATE TABLE CreditProfitability.dbo.TS_Secured_Unsecured_Expenses_Final
WITH (
      DISTRIBUTION = HASH(loanid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
a.loanid,
coalesce(b.Debt,c.debt) as Debt
from creditprofitability.dbo.TS_Apps_2 as a
left join creditprofitability.dbo.TS_GazelleExpenses_Final as b
on a.loanid = b.loanid 
left join creditprofitability.dbo.TS_OmniExpenses_Final2 as c
on a.loanid = c.applicationid
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Industry */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Industry_loan_Final')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_Industry_loan_Final
 END;

CREATE TABLE CreditProfitability.dbo.TS_Industry_loan_Final
WITH (
      DISTRIBUTION = HASH(loanreference),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
a.LOANREFERENCE,
a.loanid,
Industry,
Supersector,
sector,
subsector,
AB_Sector
from creditprofitability.dbo.TS_Apps_2  as a
left join prd_exactussync.dbo.ZH31100P  as b
on a.loanreference = b.loanreference
and a.clientnumber = b.clientnumber
left join Provisions.dbo.Payments_Table as c
on b.loanreference = c.loanref
left join PRD_CRUP.ic.CRUP_Subgroup_Industry as d
on a.Subgroupcode = d.REFERENCE_CONCATENATED
where stmt_nr = '0'
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Industry - 2 */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Industry_card_Final')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_Industry_card_Final
 END;

CREATE TABLE CreditProfitability.dbo.TS_Industry_card_Final
WITH (
      DISTRIBUTION = HASH(loanreference),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
a.LOANREFERENCE,
a.loanid,
Industry,
Supersector,
sector,
subsector,
AB_Sector
from creditprofitability.dbo.TS_Apps_2 as a
left join prd_exactussync.dbo.CH20000P  as b
on a.loanreference = b.ACCOUNTREFERENCE
and a.clientnumber = b.clientnumber
left join Provisions.dbo.Payments_Table as c
on b.ACCOUNTREFERENCE = c.loanref
left join PRD_CRUP.ic.CRUP_Subgroup_Industry as d
on a.Subgroupcode = d.REFERENCE_CONCATENATED
where stmt_nr = '0'
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Industry - Final */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Industry_Final')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_Industry_Final
 END;

CREATE TABLE CreditProfitability.dbo.TS_Industry_Final
WITH (
      DISTRIBUTION = HASH(loanid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
select 
indf.loanid,
indf.Industry,
indf.Supersector,
indf.sector,
indf.subsector,
indf.AB_Sector

from (
Select distinct
a.loanid,
d.Industry,
coalesce (b.Supersector,c.Supersector) as Supersector,
coalesce (b.sector,c.sector) as sector,
coalesce (b.subsector,c.subsector) as subsector,
coalesce (b.AB_Sector,c.AB_Sector) as AB_Sector,
row_number() over (partition by a.Loanid order by Creationdate desc,a.Loanid desc) as r2
from creditprofitability.dbo.TS_Apps_2 as a
left join creditprofitability.dbo.TS_Industry_loan_Final as b
on a.loanid = b.loanid
left join creditprofitability.dbo.TS_Industry_card_Final as c
on a.loanid = c.loanid
left join PRD_CRUP.ic.CRUP_Subgroup_Industry as d
on a.Subgroupcode = d.REFERENCE_CONCATENATED ) as indf
where indf.r2=1
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Final Affordability Table */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_FinalAffordabilityDetail_3Months_Daily')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_FinalAffordabilityDetail_3Months_Daily
 END;

CREATE TABLE CreditProfitability.dbo.TS_FinalAffordabilityDetail_3Months_Daily
WITH (
      DISTRIBUTION = HASH(Uniqueid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select distinct
 A.NationalID
,A.loanid
,A.Creationmonth
,A.Creationdate
,A.ApplicationStatus
,A.Branch
,A.Origination_source
,A.Clientnumber
,A.Accountnumber
,A.Scoremodel
,A.Subgroupcode
,A.Persal
,A.Uniqueindicator
,A.Incapri
,A.SCORECARDVERSION 
,A.Transequence
,C.Uniqueid
,C.ABLoansOnPayslip
,C.TotalPayrolLoans
,C.JointHomeLoanInstalment
,C.Bonusses
,C.Overtime
,C.Comissiom
,C.Nonrecurringincome
,C.TotalExactusInstalment
,C.Bureaudebtinclusions
,C.Payslipadjustment
,C.MonthlyNLRInstalmentOther2
,C.MonthlyCCAInstalmentOther2
,C.PayslipTaxPaid
,C.MonthlyGrossIncome
,Case when C.MonthlyGrossIncome > 20000 then 'Greater than 20k'
when C.MonthlyGrossIncome > 15000 then 'up to 15k'
when C.MonthlyGrossIncome > 10000 then 'up to 10k'
when C.MonthlyGrossIncome > 8000 then 'Up to 10k'
when C.MonthlyGrossIncome > 6000 then 'Up to 8k'
when C.MonthlyGrossIncome > 4000 then 'Up to 6k'
when C.MonthlyGrossIncome > 2000 then 'Up to 4K'
when C.MonthlyGrossIncome > 0 then 'Up to 2k'
when C.MonthlyGrossIncome = -99000800  then '-99000800'
when C.MonthlyGrossIncome  = 0 then 'Equal to 0'
else Null end as MonthlyGrossIncomeGroup 
,round(D.MaximumInstalment2,1) as MaximumInstalment2
,round(D.MaximumInstalment4,1) as MaximumInstalment4
,round(D.MaximumInstalment5,1) as MaximumInstalment5
,round(M.MaximumInstalment6,1) as MaximumInstalment6
,round(D.MaximumInstalment7,1) as MaximumInstalment7
,round(P.Min_Afford,1) as Min_Afford
,Case when round(P.Min_Afford,1)> 20000 then 'Greater than 20k'
when round(P.Min_Afford,1) > 15000 then 'up to 15k'
when round(P.Min_Afford,1) > 10000 then 'up to 10k'
when round(P.Min_Afford,1) > 8000 then 'Up to 10k'
when round(P.Min_Afford,1) > 6000 then 'Up to 8k'
when round(P.Min_Afford,1) > 4000 then 'Up to 6k'
when round(P.Min_Afford,1) > 2000 then 'Up to 4K'
when round(P.Min_Afford,1) > 0 then 'Up to 2k'
when round(P.Min_Afford,1) = -99000800  then '-99000800'
when round(P.Min_Afford,1)  = 0 then 'Equal to 0'
else Null end as Min_AffordGroup 
,round(E.CalculatedNetIncome,1) as CalculatedNetIncome
,round(E.RiskNetIncome,1) as RiskNetIncome
,Case when round(E.RiskNetIncome,1)> 20000 then 'Greater than 20k'
when round(E.RiskNetIncome,1)> 15000 then 'up to 15k'
when round(E.RiskNetIncome,1)> 10000 then 'up to 10k'
when round(E.RiskNetIncome,1)> 8000 then 'Up to 10k'
when round(E.RiskNetIncome,1)> 6000 then 'Up to 8k'
when round(E.RiskNetIncome,1)> 4000 then 'Up to 6k'
when round(E.RiskNetIncome,1)> 2000 then 'Up to 4K'
when round(E.RiskNetIncome,1)> 0 then 'Up to 2k'
when round(E.RiskNetIncome,1)= -99000800  then '-99000800'
when round(E.RiskNetIncome,1) = 0 then 'Equal to 0'
else Null end as RiskNetIncomeGroup 
,E.AdditionalIncome
,F.BureauDebtExclusions
,F.JHLAddback
,F.TotalCCAInstalmentOther1
,F.TotalNLRInstalmentOther1
,G.AvailableClientFacility
,G.Offer_MaxCapital
,G.Offer_MaxCapCard
,G.Offer_MaxTerm
,G.OFFERINDICATOR
,G.Offer_MaxOVERDRAFT
,round(H.dis_caploanonly,1) as dis_caploanonly
,H.dis_termloanonly
,H.Loan_Instalment
,H.dis_capcardonly
,H.Card_Instalment
,H.dis_capODonly
,H.dis_insODonly
,(H.Loan_Instalment+H.Card_Instalment) as Instalment 
,case when dis_caploanonly is not null then 'Loan_disbursed'
 when dis_capcardonly is not null then  'Card_disbursed'
 when dis_capODonly is not null then  'Overdraft_disbursed'
 else null end as Product
,round(I.ADJ_MAX4,1) as ADJ_MAX4
,round(I.ADJ_MAX5,1) as ADJ_MAX5
,round(I.ADJ_MAX6,1) as ADJ_MAX6
,round(I.ADJ_MAX7,1) as ADJ_MAX7
,left(K.Scoreband,2)as Scoreband
,SP.scoringPD
,K.Scorecard
,L.finalRTICap
,L.OptimalRTI
,L.MaxRTICap 
,L.AffordabilityTestingStrategy
,round(L.UnsecuredAndRevolvingDebt,1) as UnsecuredAndRevolvingDebt
,Case when round(L.UnsecuredAndRevolvingDebt,1)  > 20000 then 'Greater than 20k'
when round(L.UnsecuredAndRevolvingDebt,1)  > 15000 then 'up to 15k'
when round(L.UnsecuredAndRevolvingDebt,1) > 10000 then 'up to 10k'
when round(L.UnsecuredAndRevolvingDebt,1)  > 8000 then 'Up to 10k'
when round(L.UnsecuredAndRevolvingDebt,1)  > 6000 then 'Up to 8k'
when round(L.UnsecuredAndRevolvingDebt,1)  > 4000 then 'Up to 6k'
when round(L.UnsecuredAndRevolvingDebt,1)  > 2000 then 'Up to 4K'
when round(L.UnsecuredAndRevolvingDebt,1)  > 0 then 'Up to 2k'
when round(L.UnsecuredAndRevolvingDebt,1)  = -99000800  then '-99000800'
when round(L.UnsecuredAndRevolvingDebt,1)   = 0 then 'Equal to 0'
else Null end as UnsecuredAndRevolvingDebtGroup 
,TaxBack2
,N.TaxBackPerc
,N.Max7RTICapPerc 
,N.max7RTICapVal
,Max7InstalmentBase 
,Max7Final 
,round(N.MinComplianceExpenses,1) as MinComplianceExpenses
,convert(float,N.TotalExpenses) as TotalExpenses
,N.WageType
,N.monthlyNetIncome
,N.WageModifier
,N.NetSalaryMonthly 
,Case when NetSalaryMonthly > 20000 then 'Greater than 20k'
when NetSalaryMonthly > 15000 then 'up to 15k'
when NetSalaryMonthly > 10000 then 'up to 10k'
when NetSalaryMonthly > 8000 then 'Up to 10k'
when NetSalaryMonthly > 6000 then 'Up to 8k'
when NetSalaryMonthly > 4000 then 'Up to 6k'
when NetSalaryMonthly > 2000 then 'Up to 4K'
when NetSalaryMonthly > 0 then 'Up to 2k'
when NetSalaryMonthly = -99000800  then '-99000800'
when NetSalaryMonthly  = 0 then 'Equal to 0'
else Null end as NetSalaryMonthlyGroup 
,O.Commission1
,O.Commission2
,O.Overtime1
,O.Overtime2  
,round(O.TaxBackValue,1) as TaxBackValue
,round(Q.avgGrossIncome,1) as avgGrossIncome
,case when convert(float,Q.avgGrossIncome) <7500 then 1 else 0 end as Debtrelief
,R.AvgAdditionalIncome
,round(R.AvgNetIncome,1) as AvgNetIncome
,S.Max4Percentage
,round(T.RTI_Offered,1) as RTI_Offered
,Case when RTI_OFFERED >=80  then	'80'
when RTI_OFFERED >60		 then	'Up to 80'
when RTI_OFFERED >40	     then	'Up to 60'
when RTI_OFFERED >20		 then	'Up to 40'
when RTI_OFFERED >0		 then	'Up to 20'
when RTI_OFFERED <=0		 then	'FinalRTI = 0'
else null end as RTI_OFFERED_Group
,U.Loan_Product_Limits
,V.Internalsettledloans
,V.Externalsettledloans
,V.SRA_Definition
,V.CL_Definition 
,W.TotalExactusCard
,W.New_Card_Disbursed
,case when A.Applicationstatus = 'dis' then round(X.RTI_PostDisb,1) else null end as RTI_PostDisb
,Case when A.Applicationstatus = 'dis'  and RTI_POSTDISB =80  then	'80'
when A.Applicationstatus = 'dis'  and RTI_POSTDISB >60		 then	'Up to 80'
when A.Applicationstatus = 'dis'  and RTI_POSTDISB >40	     then	'Up to 60'
when A.Applicationstatus = 'dis'  and RTI_POSTDISB >20		 then	'Up to 40'
when A.Applicationstatus = 'dis'  and RTI_POSTDISB >0		 then	'Up to 20'
when A.Applicationstatus = 'dis'  and RTI_POSTDISB <=0		 then	'FinalRTI = 0'
else null end as RTI_POSTDISB_Group
,ZB.PDCapital
,round(ZE.totalCCAInstalmentOther2,1) as totalCCAInstalmentOther2
,Case when round(ZE.totalCCAInstalmentOther2,1)> 20000 then 'Greater than 20k'
when round(ZE.totalCCAInstalmentOther2,1) > 15000 then 'up to 15k'
when round(ZE.totalCCAInstalmentOther2,1) > 10000 then 'up to 10k'
when round(ZE.totalCCAInstalmentOther2,1) > 8000 then 'Up to 10k'
when round(ZE.totalCCAInstalmentOther2,1) > 6000 then 'Up to 8k'
when round(ZE.totalCCAInstalmentOther2,1) > 4000 then 'Up to 6k'
when round(ZE.totalCCAInstalmentOther2,1)> 2000 then 'Up to 4K'
when round(ZE.totalCCAInstalmentOther2,1) > 0 then 'Up to 2k'
when round(ZE.totalCCAInstalmentOther2,1) = -99000800  then '-99000800'
when round(ZE.totalCCAInstalmentOther2,1)  = 0 then 'Equal to 0'
else Null end as CCAInstalmentOther2Group 
,round(ZE.totalNLRInstalmentOther2,1) as totalNLRInstalmentOther2
,Case when round(ZE.totalNLRInstalmentOther2,1)> 20000 then 'Greater than 20k'
when round(ZE.totalNLRInstalmentOther2,1) > 15000 then 'up to 15k'
when round(ZE.totalNLRInstalmentOther2,1) > 10000 then 'up to 10k'
when round(ZE.totalNLRInstalmentOther2,1) > 8000 then 'Up to 10k'
when round(ZE.totalNLRInstalmentOther2,1)> 6000 then 'Up to 8k'
when round(ZE.totalNLRInstalmentOther2,1) > 4000 then 'Up to 6k'
when round(ZE.totalNLRInstalmentOther2,1)> 2000 then 'Up to 4K'
when round(ZE.totalNLRInstalmentOther2,1) > 0 then 'Up to 2k'
when round(ZE.totalNLRInstalmentOther2,1) = -99000800  then '-99000800'
when round(ZE.totalNLRInstalmentOther2,1)  = 0 then 'Equal to 0'
else Null end as NLRInstalmentOther2Group 
,ZC.Max_Afford
,Case when round(ZC.Max_Afford,1)> 20000 then 'Greater than 20k'
when round(ZC.Max_Afford,1) > 15000 then 'up to 15k'
when round(ZC.Max_Afford,1) > 10000 then 'up to 10k'
when round(ZC.Max_Afford,1) > 8000 then 'Up to 10k'
when round(ZC.Max_Afford,1) > 6000 then 'Up to 8k'
when round(ZC.Max_Afford,1) > 4000 then 'Up to 6k'
when round(ZC.Max_Afford,1) > 2000 then 'Up to 4K'
when round(ZC.Max_Afford,1) > 0 then 'Up to 2k'
when round(ZC.Max_Afford,1) = -99000800  then '-99000800'
when round(ZC.Max_Afford,1)  = 0 then 'Equal to 0'
else Null end as Max_AffordGroup 
,SZ.Debt
,SM.Industry
,case when 
     SM.Industry in  ('CONSUMER DISCRETIONARY','GOVERNMENT AND SOE','CONSUMER STAPLES','OTHER MINOR INDUSTRIES','FINANCIALS','TELECOMMUNICATIONS','REAL ESTATE',
 'HEALTH CARE','BASIC MATERIALS','INDUSTRIALS','TECHNOLOGY') then SM.Industry
 when SM.Industry in ('ENERGY','UTILITIES') then 'ENERGY & UTILITIES'
 else 'Other'
end as byindustry
from creditprofitability.dbo.TS_Apps_2 as A
left join  creditprofitability.dbo.TS_Capri_Affordability as C
on convert(varchar,A.Loanid)= C.Tranappnumber
left join creditprofitability.dbo.TS_AffordabilityMeasures as D
on convert(varchar,A.Loanid)= D.TRANAPPNUMBER
left join creditprofitability.dbo.TS_ClientIncome as E
on convert(varchar,A.Loanid)= E.RequestID
left join creditprofitability.dbo.TS_InstalmentBase as F
on convert(varchar,A.Loanid)= F.RequestID
left join creditprofitability.dbo.TS_Offers as G
on convert(varchar,A.Loanid)= G.Tranappnumber
left join creditprofitability.dbo.TS_Disbursals as H
on convert(varchar,A.Loanid)= H.Loanid
left join creditprofitability.dbo.TS_Adj_Affordabilitymeasures as I
on convert(varchar,A.Loanid)= I.Tranappnumber
left join creditprofitability.dbo.TS_Scoreband as K
on convert(varchar,A.Loanid)= K.Tranappnumber
left join  Creditprofitability.dbo.TS_ScoringPD as SP
on convert(varchar,A.Loanid)= SP.Tranappnumber
left join  creditprofitability.dbo.TS_RTICaps as L
on convert(varchar,A.Loanid)= L.RequestID
left join creditprofitability.dbo.TS_Max6 as M
on convert(varchar,A.Loanid)= M.RequestID
left join creditprofitability.dbo.TS_Max7 as N
on convert(varchar,A.Loanid)= N.RequestID
left join creditprofitability.dbo.TS_IncomeSupport as O
on convert(varchar,A.Loanid)= O.RequestID
left join creditprofitability.dbo.TS_MinAfford as P
on convert(varchar,A.Loanid)= P.Tranappnumber
left join creditprofitability.dbo.TS_AvgGross as Q
on convert(varchar,A.Loanid)= Q.Requestid
left join creditprofitability.dbo.TS_AdditionalIncome as R
on convert(varchar,A.Loanid)= R.RequestID
left join creditprofitability.dbo.TS_Max4Perc as S
on convert(varchar,A.Loanid)= S.Requestid
left join creditprofitability.dbo.TS_RTIOffered as T
on convert(varchar,A.Loanid)= T.Tranappnumber
left join creditprofitability.dbo.TS_Loan_Product_Limits as U
on convert(varchar,A.Loanid)= U.Loanid
left join Creditprofitability.dbo.TS_SRA_CL_2 as V
on convert(varchar,A.Loanid)= V.Loanid
left join creditprofitability.dbo.TS_NewCard as W
on convert(varchar,A.Loanid)= W.Loanid
left join creditprofitability.dbo.TS_RTIPostDisb_daily as X
on convert(varchar,A.Loanid)= X.Loanid
left join creditprofitability.dbo.TS_PDCapital as ZB
on convert(varchar,A.Loanid)= ZB.RequestID
left join prd_press.capri.CAPRI_EMPLOYMENT as ZD
on C.uniqueid = ZD.uniqueid
left join creditprofitability.dbo.TS_NLRandCCA as ZE
on convert(varchar,A.Loanid)= ZE.Requestid
left join creditprofitability.dbo.TS_MaxAfford as ZC
on convert(varchar,A.Loanid)= ZC.Tranappnumber
left join creditprofitability.dbo.TS_Secured_Unsecured_Expenses_Final as SZ
on convert(varchar,A.Loanid)= SZ.loanid
left join creditprofitability.dbo.TS_Industry_Final as SM
on convert(varchar,A.Loanid)= SM.Loanid
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Max Cap Offer */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_MaxOffers_Saturday')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_MaxOffers_Saturday
 END;

CREATE TABLE CreditProfitability.dbo.TS_MaxOffers_Saturday
WITH (
      DISTRIBUTION = HASH(Tranappnumber),
	   CLUSTERED COLUMNSTORE INDEX)
AS
select
Tranappnumber,
MAX(Term)  AS MaxTerm , 
MAX(TOTALCAPITAL)    AS MaxCapital,
Applicationdate
from prd_Press.capri.capri_offer_results_2021 as K
where datediff(month,applicationdate,getdate()) <= 2
Group by Tranappnumber,Applicationdate
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Max Cap Offer - 1 */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_MaxOffers_Saturday2')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_MaxOffers_Saturday2
 END;

CREATE TABLE CreditProfitability.dbo.TS_MaxOffers_Saturday2
WITH (
      DISTRIBUTION = HASH(Tranappnumber),
	   CLUSTERED COLUMNSTORE INDEX)
AS
select
Tranappnumber,
MAX(MaxCapital) AS MaximumOfferCapital
from creditprofitability.dbo.TS_MaxOffers_Saturday
Group by Tranappnumber
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Max Cap Offer - 2 */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_MaxOffers_Saturday3')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_MaxOffers_Saturday3
 END;

CREATE TABLE CreditProfitability.dbo.TS_MaxOffers_Saturday3
WITH (
      DISTRIBUTION = HASH(loanid),
	   CLUSTERED COLUMNSTORE INDEX)
AS
select
a.Loanid,
MaximumOfferCapital,
creationmonth,
Origination_source,
Case when MaximumOfferCapital =350000 then '350k'
when MaximumOfferCapital >250000 then 'Up to 350k'
when MaximumOfferCapital =250000 then '250k'
when MaximumOfferCapital >200000 then 'Up to 250k'
when MaximumOfferCapital > 150000 then 'up to 200k'
when MaximumOfferCapital > 120000 then 'Up to 150k'
when MaximumOfferCapital > 90000 then  'Up to 120k'
when MaximumOfferCapital > 60000 then 'Up to 90k'
when MaximumOfferCapital > 35000 then 'Up to 60k'
when MaximumOfferCapital > 25000 then 'Up to 35k'
when MaximumOfferCapital > 15000 then  'Up to 25k'
when MaximumOfferCapital > 10000 then 'Up to 15k'
when MaximumOfferCapital > 5000 then 'Up to 10K'
when MaximumOfferCapital > 0 then 'Up to 5K' 
else null 
end as MaxCapitalOfferGroup
from creditprofitability.dbo.TS_Apps_2  as A
left join creditprofitability.dbo.TS_MaxOffers_Saturday2 as b
on cast(a.loanid as varchar)= b.Tranappnumber
where uniqueindicator = '1'
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Max Term Offer */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_MaxOffers_Term')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_MaxOffers_Term
 END;

CREATE TABLE CreditProfitability.dbo.TS_MaxOffers_Term
WITH (
      DISTRIBUTION = HASH(Tranappnumber),
	   CLUSTERED COLUMNSTORE INDEX)
AS
select
Tranappnumber,
MAX(MaxTerm)    AS MaximumOfferTerm
from creditprofitability.dbo.TS_MaxOffers_Saturday
Group by Tranappnumber
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Max Term Offer - 1 */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_MaxOffers_Term2')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_MaxOffers_Term2
 END;

CREATE TABLE CreditProfitability.dbo.TS_MaxOffers_Term2
WITH (
      DISTRIBUTION = HASH(loanid),
	   CLUSTERED COLUMNSTORE INDEX)
AS
select
a.Loanid,
MaximumOfferTerm,
creationmonth,
Origination_source,
Case when MaximumOfferTerm= '72' then '72'
when MaximumOfferTerm= '60' then '60'
when MaximumOfferTerm = '54' then '54'
when MaximumOfferTerm = '48' then '48'
when MaximumOfferTerm = '42' then '42'
when MaximumOfferTerm = '36' then '36'
when MaximumOfferTerm = '30' then '30'
when MaximumOfferTerm = '24' then '24'
when MaximumOfferTerm = '18' then '18'
when MaximumOfferTerm = '12' then  '12'
when MaximumOfferTerm = '9' then '9'
when MaximumOfferTerm = '8' then '8'
when MaximumOfferTerm = '7' then '7'
when MaximumOfferTerm <= '6' then '<=6'
else NULL 
end as MaximumOfferTermOfferGroup 
from creditprofitability.dbo.TS_Apps_2 as A
left join creditprofitability.dbo.TS_MaxOffers_Term as b
on cast(a.loanid as varchar) = b.Tranappnumber
where uniqueindicator = '1'
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Failing Applicants' */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_Failures_2')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_Failures_2
 END;

CREATE TABLE CreditProfitability.dbo.TS_Failures_2
WITH (
      DISTRIBUTION = HASH(loanid),
	   CLUSTERED COLUMNSTORE INDEX)
AS 
SELECT
k.CreationMonth
,k.Loanid
,k.origination_source
,k.applicationstatus
,K.Uniqueindicator
, case when loanid in (
select applicationid from prd_ExactusSync.dbo.OC00000P
where type = 'OVER' 
) then (	case when Adj_Max5 <=150	then 'Overdraft-MAX5'
					 when Adj_Max7 <=150	then 'Overdraft-MAX7' 
					when Adj_max6 <=150		then 'Overdraft-RTI'
					when Adj_max4 <=150		then 'Overdraft-MAX4' end)
else(
case when Adj_Max5 <=350	then 'MAX5'
 when Adj_Max7 <=350		then 'MAX7' 
when Adj_max6 <=350			then 'RTI'
when Adj_max4 <=350			then 'MAX4'
 end) end as FailedBy
 from  creditprofitability.dbo.TS_FinalAffordabilityDetail_3Months_Daily  as k
where  
Adj_Max5 <=350
or Adj_max6 <=350
or Adj_max4 <=350
or Adj_Max7 <=350	
and MaximumInstalment2 > -99000800.00
and MaximumInstalment4 > -99000800.00
and MaximumInstalment5 > -99000800.00
and MaximumInstalment6 > -99000800.00
) 

by APS;
DISCONNECT FROM APS;
quit;


 /* Capping Offers */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_GotOffer')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_GotOffer
 END;

CREATE TABLE CreditProfitability.dbo.TS_GotOffer
WITH (
      DISTRIBUTION = HASH(loanid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select Distinct 
loanid,
creationmonth, 
uniqueindicator,
incapri,
sum(case when B.Tranappnumber is not null then 1 else 0 end) as Gotoffer
from creditprofitability.dbo.TS_Apps_2 as A
left join creditprofitability.dbo.TS_Offers as B
on cast(A.loanid as varchar) = B.Tranappnumber
where datediff(month,creationdate,getdate()) <= 2
group by loanid,creationmonth,uniqueindicator,incapri
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Capping Offers - 1 */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_CappingOffers_final')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_CappingOffers_final
 END;

CREATE TABLE CreditProfitability.dbo.TS_CappingOffers_final
WITH (
      DISTRIBUTION = HASH(loanid),
	   CLUSTERED COLUMNSTORE INDEX)
AS 
select  distinct
Creationmonth
,loanid
,SCORECARDVERSION
,scoreband
,LimitedBY
,Gotoffer
,Uniqueindicator
from 
(select K.loanid
,K.Creationmonth
,k.SCORECARDVERSION
,k.scoreband
,l.gotoffer
,k.uniqueindicator
 ,case when k.Offer_MaxCapital >= k.Loan_Product_limits then 'Product Limited'
when k.Offer_MaxCapital >= cast(k.PDCapital as float) then 'PD Product limits'
else
(case when k.creationmonth >'201901' then
(case when MaximumInstalment5 <= MaximumInstalment2 
and MaximumInstalment5 <= MaximumInstalment4
and (MaximumInstalment5 <= MaximumInstalment6 or  (((MaximumInstalment5) - (MaximumInstalment6)) between -2 and 2 and MaximumInstalment6 <> 0))
and (MaximumInstalment5 <= MaximumInstalment7 or  (((MaximumInstalment5) - (MaximumInstalment7)) between -2 and 2 and MaximumInstalment7 <> 0))
                                                then 'MAX5'
             when  MaximumInstalment6 <= MaximumInstalment2
				and MaximumInstalment6 <= MaximumInstalment4
               and MaximumInstalment6 <= MaximumInstalment5
               and (MaximumInstalment6 <= MaximumInstalment7 or (((MaximumInstalment6) - (MaximumInstalment7)) between -2 and 2 and MaximumInstalment7 <> 0))
                                                then 'RTI'
            when MaximumInstalment4 <= MaximumInstalment2
               and MaximumInstalment4 <= MaximumInstalment5
               and MaximumInstalment4 <= MaximumInstalment6
               and MaximumInstalment4 <= MaximumInstalment7
                                                then 'MAX4'
           when MaximumInstalment7 <= MaximumInstalment2
             and MaximumInstalment7 <= MaximumInstalment4
         and MaximumInstalment7 <= MaximumInstalment5
        and (MaximumInstalment7 <= MaximumInstalment6 or (((MaximumInstalment7) - (MaximumInstalment6)) between -2 and 2 and MaximumInstalment6 <> 0))
                                                then 'MAX7'
         when MaximumInstalment2 <= MaximumInstalment4
          and MaximumInstalment2 <= MaximumInstalment5
          and MaximumInstalment2 <= MaximumInstalment7
                                                then 'MAX2' end)end ) end as Limitedby
 from  creditprofitability.dbo.TS_FinalAffordabilityDetail_3Months_Daily as K
 left join creditprofitability.dbo.TS_GotOffer as l
 on cast(K.Loanid as varchar)  = l.loanid
where gotoffer = '1'
and k.creationmonth >= '202212'
       and MaximumInstalment2 > -99000800.00
       and MaximumInstalment4 > -99000800.00
       and MaximumInstalment5 > -99000800.00
       and MaximumInstalment6 > -99000800.00
	   ) a
) 

by APS;
DISCONNECT FROM APS;
quit;


 /* Final Affordability Table with credit pack variables */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_FinalAffordabilityDetail_3Months_2_Daily')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_FinalAffordabilityDetail_3Months_2_Daily
 END;

CREATE TABLE CreditProfitability.dbo.TS_FinalAffordabilityDetail_3Months_2_Daily
WITH (
      DISTRIBUTION = HASH(Uniqueid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select distinct
A.*,
case when A.applicationstatus = 'dis'  and RTI_postdisb >=81	then	'Greater than 80'
 when A.applicationstatus = 'dis'  and RTI_postdisb >=70	and RTI_postdisb <81 then	'70-80'
when A.applicationstatus = 'dis'  and RTI_postdisb >=60	and RTI_postdisb <70 then	'60-70'
when A.applicationstatus = 'dis'  and RTI_postdisb >=50	and RTI_postdisb <60 then	'50-60'
when A.applicationstatus = 'dis'  and RTI_postdisb >=40	and RTI_postdisb <50 then	'40-50'
when A.applicationstatus = 'dis'  and RTI_postdisb >=30	and RTI_postdisb <40 then	'30-40'
when A.applicationstatus = 'dis'  and RTI_postdisb >=20	and RTI_postdisb <30 then	'20-30'
when A.applicationstatus = 'dis'  and RTI_postdisb >=10	and RTI_postdisb <20 then	'10-20'
when A.applicationstatus = 'dis'  and RTI_postdisb >=0	and RTI_postdisb <10 then	'0-10'
when A.applicationstatus = 'dis'  and RTI_postdisb <0 then	'Negative RTI'
else null end as  RTIPostdisb_grouping,

case when F.gotoffer in ('1', '2')  and RTI_Offered >=81	then	'Greater than 80'
 when F.gotoffer in ('1', '2')  and RTI_Offered >=70	and RTI_Offered <81 then	'70-80'
when F.gotoffer in ('1', '2')  and RTI_Offered >=60	and RTI_Offered <70 then	'60-70'
when F.gotoffer in ('1', '2')  and RTI_Offered >=50	and RTI_Offered <60 then	'50-60'
when F.gotoffer in ('1', '2')  and RTI_Offered >=40	and RTI_Offered <50 then	'40-50'
when F.gotoffer in ('1', '2')  and RTI_Offered >=30	and RTI_Offered <40 then	'30-40'
when F.gotoffer in ('1', '2')  and RTI_Offered >=20	and RTI_Offered <30 then	'20-30'
when F.gotoffer in ('1', '2')  and RTI_Offered >=10	and RTI_Offered <20 then	'10-20'
when F.gotoffer in ('1', '2')  and RTI_Offered >=0	and RTI_Offered <10 then	'0-10'
when F.gotoffer in ('1', '2')  and RTI_Offered <0 then	'Negative RTI'
else null end as  Group_RTIOffered,

case when A.Scoreband in ('50','51','52') then '50-52'
when A.Scoreband in ('53','54','55','56') then '53-56'
when A.Scoreband in ('57','58','59','60') then '57-60' 
when A.Scoreband in ('61','62','63') then '61-63'
when A.Scoreband in ('68','69','70','71') then 'Thinfile'
else Null end as Scoreband_group,

B.MaximumOfferCapital,
B.MaxCapitalOfferGroup,
C.MaximumOfferTerm,
C.MaximumOfferTermOfferGroup ,
D.FailedBy,
E.LimitedBY,
F.GotOffer
from  creditprofitability.dbo.TS_FinalAffordabilityDetail_3Months_Daily as A
left join creditprofitability.dbo.TS_MaxOffers_Saturday3 as B
on convert(varchar,A.Loanid) = b.loanid
left join creditprofitability.dbo.TS_MaxOffers_Term2 as C
on convert(varchar,A.Loanid) = c.loanid
left join Creditprofitability.dbo.TS_Failures_2 as D
on convert(varchar,A.Loanid) = d.loanid
left join Creditprofitability.dbo.TS_CappingOffers_final as E
on convert(varchar,A.Loanid) = E.loanid
left join creditprofitability.dbo.TS_GotOffer as F
on convert(varchar,A.Loanid) = F.loanid
) 

by APS;
DISCONNECT FROM APS;
quit;


/* History Affordabilitydetail Table */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_FinalAffordabilityDetail_II_Daily')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_FinalAffordabilityDetail_II_Daily
 END;

CREATE TABLE CreditProfitability.dbo.TS_FinalAffordabilityDetail_II_Daily
WITH (
      DISTRIBUTION = HASH(Loanid),
	   CLUSTERED COLUMNSTORE INDEX )
as 
Select  C.* 
From (Select
 A.NationalID
,A.loanid
,A.Creationmonth
,A.Creationdate
,A.ApplicationStatus
,A.Branch
,A.Origination_source
,A.Clientnumber
,A.Accountnumber
,A.Scoremodel
,A.Subgroupcode
,A.Persal
,A.Uniqueindicator
,A.Incapri
,A.SCORECARDVERSION 
,A.Uniqueid
,A.ABLoansOnPayslip
,A.TotalPayrolLoans
,A.JointHomeLoanInstalment
,A.Bonusses
,A.Overtime
,A.Comissiom
,A.Nonrecurringincome
,A.TotalExactusInstalment
,A.Bureaudebtinclusions
,A.Payslipadjustment
,A.MonthlyNLRInstalmentOther2
,A.MonthlyCCAInstalmentOther2
,A.PayslipTaxPaid
,A.MonthlyGrossIncome
,A.MonthlyGrossIncomeGroup 
,A.MaximumInstalment2
,A.MaximumInstalment4
,A.MaximumInstalment5
,A.MaximumInstalment6
,A.MaximumInstalment7
,A.Min_Afford
,A.Min_AffordGroup 
,A.CalculatedNetIncome
,A.RiskNetIncome
,A.RiskNetIncomeGroup 
,A.AdditionalIncome
,A.BureauDebtExclusions
,A.JHLAddback
,A.TotalCCAInstalmentOther1
,A.TotalNLRInstalmentOther1
,A.AvailableClientFacility
,A.Offer_MaxCapital
,A.Offer_MaxTerm
,A1.Offer_MaxCapCard
,A.OFFERINDICATOR
,A.Offer_MaxOVERDRAFT
,A.dis_caploanonly
,A.dis_termloanonly
,A1.Loan_Instalment
,A.dis_capcardonly
,A1.Card_Instalment
,A1.dis_capODonly
,A1.dis_insODonly
,A1.Instalment
,A.Product
,A.ADJ_MAX4
,A.ADJ_MAX5
,A.ADJ_MAX6
,A.ADJ_MAX7
,A.Scoreband
,A.Scorecard
,A.scoringPD
,A.finalRTICap
,A.OptimalRTI
,A.MaxRTICap 
,A.AffordabilityTestingStrategy
,A.UnsecuredAndRevolvingDebt
,A.UnsecuredAndRevolvingDebtGroup 
,A.TaxBack2
,A.TaxBackPerc
,A.Max7RTICapPerc 
,A.max7RTICapVal
,A.Max7InstalmentBase 
,A.Max7Final 
,A.MinComplianceExpenses
,A.TotalExpenses
,A.WageType
,A.monthlyNetIncome
,A.WageModifier
,A.NetSalaryMonthly 
,A.NetSalaryMonthlyGroup 
,A.Commission1
,A.Commission2
,A.Overtime1
,A.Overtime2  
,A.TaxBackValue
,A.avgGrossIncome
,A.Debtrelief
,A.AvgAdditionalIncome
,A.AvgNetIncome
,A.Max4Percentage
,A.RTI_Offered
,A.RTI_OFFERED_Group
,A.Loan_Product_Limits
,A.Internalsettledloans
,A.Externalsettledloans
,A.SRA_Definition
,A.CL_Definition 
,A.TotalExactusCard
,A.New_Card_Disbursed
,A.RTI_PostDisb
,A.RTI_POSTDISB_Group
,A.PDCapital
,A.totalCCAInstalmentOther2
,A.CCAInstalmentOther2Group 
,A.totalNLRInstalmentOther2
,A.NLRInstalmentOther2Group 
,A.Max_Afford
,A.Debt
,A.Industry
,A.byindustry
,A.MaximumOfferCapital
,A.MaxCapitalOfferGroup
,A.MaximumOfferTerm
,A.MaximumOfferTermOfferGroup 
,A.FailedBy
,A.LimitedBY
,A.GotOffer
,A.Transequence
,A.Group_RTIOffered
,A.Scoreband_group
,A.RTIPostdisb_grouping
,A.Max_AffordGroup
FROM  creditprofitability.dbo.SZ_Daily_Affordability_Table A 
LEFT JOIN creditprofitability.dbo.TS_FinalAffordabilityDetail_3Months_2_Daily A1 
ON convert(varchar,A.Loanid) = convert(varchar,A1.Loanid)
Where A1.loanid IS NULL and A.Creationmonth > 0

union all 

 SELECT 
 NationalID
,loanid
,Creationmonth
,Creationdate
,ApplicationStatus
,Branch
,Origination_source
,Clientnumber
,Accountnumber
,Scoremodel
,Subgroupcode
,Persal
,Uniqueindicator
,Incapri
,SCORECARDVERSION 
,Uniqueid
,ABLoansOnPayslip
,TotalPayrolLoans
,JointHomeLoanInstalment
,Bonusses
,Overtime
,Comissiom
,Nonrecurringincome
,TotalExactusInstalment
,Bureaudebtinclusions
,Payslipadjustment
,MonthlyNLRInstalmentOther2
,MonthlyCCAInstalmentOther2
,PayslipTaxPaid
,MonthlyGrossIncome
,MonthlyGrossIncomeGroup 
,MaximumInstalment2
,MaximumInstalment4
,MaximumInstalment5
,MaximumInstalment6
,MaximumInstalment7
,Min_Afford
,Min_AffordGroup 
,CalculatedNetIncome
,RiskNetIncome
,RiskNetIncomeGroup 
,AdditionalIncome
,BureauDebtExclusions
,JHLAddback
,TotalCCAInstalmentOther1
,TotalNLRInstalmentOther1
,AvailableClientFacility
,Offer_MaxCapital
,Offer_MaxTerm
,Offer_MaxCapCard
,OFFERINDICATOR
,Offer_MaxOVERDRAFT
,dis_caploanonly
,dis_termloanonly
,Loan_Instalment
,dis_capcardonly
,Card_Instalment
,dis_capODonly
,dis_insODonly
,Instalment
,Product
,ADJ_MAX4
,ADJ_MAX5
,ADJ_MAX6
,ADJ_MAX7
,Scoreband
,Scorecard
,scoringPD
,finalRTICap
,OptimalRTI
,MaxRTICap 
,AffordabilityTestingStrategy
,UnsecuredAndRevolvingDebt
,UnsecuredAndRevolvingDebtGroup 
,TaxBack2
,TaxBackPerc
,Max7RTICapPerc 
,max7RTICapVal
,Max7InstalmentBase 
,Max7Final 
,MinComplianceExpenses
,TotalExpenses
,WageType
,monthlyNetIncome
,WageModifier
,NetSalaryMonthly 
,NetSalaryMonthlyGroup 
,Commission1
,Commission2
,Overtime1
,Overtime2  
,TaxBackValue
,avgGrossIncome
,Debtrelief
,AvgAdditionalIncome
,AvgNetIncome
,Max4Percentage
,RTI_Offered
,RTI_OFFERED_Group
,Loan_Product_Limits
,Internalsettledloans
,Externalsettledloans
,SRA_Definition
,CL_Definition 
,TotalExactusCard
,New_Card_Disbursed
,RTI_PostDisb
,RTI_POSTDISB_Group
,PDCapital
,totalCCAInstalmentOther2
,CCAInstalmentOther2Group 
,totalNLRInstalmentOther2
,NLRInstalmentOther2Group 
,Max_Afford
,Debt
,Industry
,byindustry
,MaximumOfferCapital
,MaxCapitalOfferGroup
,MaximumOfferTerm
,MaximumOfferTermOfferGroup 
,FailedBy
,LimitedBY
,GotOffer
,Transequence 
,Group_RTIOffered
,Scoreband_group
,RTIPostdisb_grouping
,Max_AffordGroup
 FROM creditprofitability.dbo.TS_FinalAffordabilityDetail_3Months_2_Daily
  ) AS C
) 

by APS;
DISCONNECT FROM APS;
quit;
  

  
/* Remove Duplicates */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_FinalAffordabilityDetail4_Daily')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_FinalAffordabilityDetail4_Daily
 END;

select top 0  A.*
into creditprofitability.dbo.TS_FinalAffordabilityDetail4_Daily
FROM (SELECT top 0 *,  row_number() over (partition by Loanid order by Creationdate desc,Loanid desc) as r2 
            from creditprofitability.dbo.TS_FinalAffordabilityDetail_II_Daily ) AS A
WHERE r2 =1;

insert into creditprofitability.dbo.TS_FinalAffordabilityDetail4_Daily
select A.*
FROM (SELECT *,  row_number() over (partition by Loanid order by Creationdate desc,Loanid desc) as r2 
            from creditprofitability.dbo.TS_FinalAffordabilityDetail_II_Daily ) AS A
WHERE r2 =1;

ALTER TABLE creditprofitability.dbo.TS_FinalAffordabilityDetail4_Daily DROP COLUMN r2
)
 
by APS;
DISCONNECT FROM APS;
quit;


/* Final Daily Affordabilitydetail Table */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'FinalAffordabilityDetail_Daily')
 BEGIN
 DROP TABLE Creditprofitability.dbo.FinalAffordabilityDetail_Daily
 END;

select top 0 *
into creditprofitability.dbo.FinalAffordabilityDetail_Daily 
from creditprofitability.dbo.TS_FinalAffordabilityDetail4_Daily;


insert
into creditprofitability.dbo.FinalAffordabilityDetail_Daily 
select *
from creditprofitability.dbo.TS_FinalAffordabilityDetail4_Daily
) 

by APS;
DISCONNECT FROM APS;
quit;



 /* Daily Final Afforability Table - Extended */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'FinalAffordabilityDetail_Daily_Ext')
 BEGIN
 DROP TABLE Creditprofitability.dbo.FinalAffordabilityDetail_Daily_Ext
 END;

CREATE TABLE CreditProfitability.dbo.FinalAffordabilityDetail_Daily_Ext
WITH (
      DISTRIBUTION = HASH(loanid))
AS
SELECT 
a.*
,case when a.MaximumInstalment4 <= 0 then '<=0'
when a.MaximumInstalment4 <= 1000 then '<=1000'
when a.MaximumInstalment4 <= 2000 then '<=2000'
when a.MaximumInstalment4 <= 3000 then '<=3000'
when a.MaximumInstalment4 <= 4000 then '<=4000'
when a.MaximumInstalment4 <= 5000 then '<=5000'
when a.MaximumInstalment4 <= 6000 then '<=6000'
when a.MaximumInstalment4 <= 7000 then '<=7000'
when a.MaximumInstalment4 <= 8000 then '<=8000'
when a.MaximumInstalment4 <= 9000 then '<=9000'
when a.MaximumInstalment4 > 9000 then '>9000'
  else '' end as max4_bucket,

case when a.MaximumInstalment5 <= 0 then '<=0'
when a.MaximumInstalment5 <= 1000 then '<=1000'
when a.MaximumInstalment5 <= 2000 then '<=2000'
when a.MaximumInstalment5 <= 3000 then '<=3000'
when a.MaximumInstalment5 <= 4000 then '<=4000'
when a.MaximumInstalment5 <= 5000 then '<=5000'
when a.MaximumInstalment5 <= 6000 then '<=6000'
when a.MaximumInstalment5 <= 7000 then '<=7000'
when a.MaximumInstalment5 <= 8000 then '<=8000'
when a.MaximumInstalment5 <= 9000 then '<=9000'
when a.MaximumInstalment5 > 9000 then '>9000'
  else '' end as max5_bucket,

case when a.MaximumInstalment6 <= 0 then '<=0'
when a.MaximumInstalment6 <= 1000 then '<=1000'
when a.MaximumInstalment6 <= 2000 then '<=2000'
when a.MaximumInstalment6 <= 3000 then '<=3000'
when a.MaximumInstalment6 <= 4000 then '<=4000'
when a.MaximumInstalment6 <= 5000 then '<=5000'
when a.MaximumInstalment6 <= 6000 then '<=6000'
when a.MaximumInstalment6 <= 7000 then '<=7000'
when a.MaximumInstalment6 <= 8000 then '<=8000'
when a.MaximumInstalment6 <= 9000 then '<=9000'
when a.MaximumInstalment6 > 9000 then '>9000'
  else '' end as max6_bucket,

case when a.MaximumInstalment7 <= 0 then '<=0'
when a.MaximumInstalment7 <= 1000 then '<=1000'
when a.MaximumInstalment7 <= 2000 then '<=2000'
when a.MaximumInstalment7 <= 3000 then '<=3000'
when a.MaximumInstalment7 <= 4000 then '<=4000'
when a.MaximumInstalment7 <= 5000 then '<=5000'
when a.MaximumInstalment7 <= 6000 then '<=6000'
when a.MaximumInstalment7 <= 7000 then '<=7000'
when a.MaximumInstalment7 <= 8000 then '<=8000'
when a.MaximumInstalment7 <= 9000 then '<=9000'
when a.MaximumInstalment7 > 9000 then '>9000'
else '' end as max7_bucket,

case 
when a.ADJ_MAX4 - isnull(b.INSTALLMENT,0) <= 0 then '<=0'
when a.ADJ_MAX4 - isnull(b.INSTALLMENT,0) <= 1000 then '<=1000'
when a.ADJ_MAX4 - isnull(b.INSTALLMENT,0) <= 2000 then '<=2000'
when a.ADJ_MAX4 - isnull(b.INSTALLMENT,0) <= 3000 then '<=3000'
when a.ADJ_MAX4 - isnull(b.INSTALLMENT,0) <= 4000 then '<=4000'
when a.ADJ_MAX4 - isnull(b.INSTALLMENT,0) <= 5000 then '<=5000'
when a.ADJ_MAX4 - isnull(b.INSTALLMENT,0) <= 6000 then '<=6000'
when a.ADJ_MAX4 - isnull(b.INSTALLMENT,0) <= 7000 then '<=7000'
when a.ADJ_MAX4 - isnull(b.INSTALLMENT,0) <= 8000 then '<=8000'
when a.ADJ_MAX4 - isnull(b.INSTALLMENT,0) <= 9000 then '<=9000'
when a.ADJ_MAX4 - isnull(b.INSTALLMENT,0)  > 9000 then '>9000'
  else '' end as max4_postdisb_bucket,

case 
when a.ADJ_MAX5 - isnull(b.INSTALLMENT,0) <= 0 then '<=0'
when a.ADJ_MAX5 - isnull(b.INSTALLMENT,0) <= 1000 then '<=1000'
when a.ADJ_MAX5 - isnull(b.INSTALLMENT,0) <= 2000 then '<=2000'
when a.ADJ_MAX5 - isnull(b.INSTALLMENT,0) <= 3000 then '<=3000'
when a.ADJ_MAX5 - isnull(b.INSTALLMENT,0) <= 4000 then '<=4000'
when a.ADJ_MAX5 - isnull(b.INSTALLMENT,0) <= 5000 then '<=5000'
when a.ADJ_MAX5 - isnull(b.INSTALLMENT,0) <= 6000 then '<=6000'
when a.ADJ_MAX5 - isnull(b.INSTALLMENT,0) <= 7000 then '<=7000'
when a.ADJ_MAX5 - isnull(b.INSTALLMENT,0) <= 8000 then '<=8000'
when a.ADJ_MAX5 - isnull(b.INSTALLMENT,0) <= 9000 then '<=9000'
when a.ADJ_MAX5 - isnull(b.INSTALLMENT,0)  > 9000 then '>9000'
  else '' end as max5_postdisb_bucket,

case 
when a.ADJ_MAX6 - isnull(b.INSTALLMENT,0) <= 0 then '<=0'
when a.ADJ_MAX6 - isnull(b.INSTALLMENT,0) <= 1000 then '<=1000'
when a.ADJ_MAX6 - isnull(b.INSTALLMENT,0) <= 2000 then '<=2000'
when a.ADJ_MAX6 - isnull(b.INSTALLMENT,0) <= 3000 then '<=3000'
when a.ADJ_MAX6 - isnull(b.INSTALLMENT,0) <= 4000 then '<=4000'
when a.ADJ_MAX6 - isnull(b.INSTALLMENT,0) <= 5000 then '<=5000'
when a.ADJ_MAX6 - isnull(b.INSTALLMENT,0) <= 6000 then '<=6000'
when a.ADJ_MAX6 - isnull(b.INSTALLMENT,0) <= 7000 then '<=7000'
when a.ADJ_MAX6 - isnull(b.INSTALLMENT,0) <= 8000 then '<=8000'
when a.ADJ_MAX6 - isnull(b.INSTALLMENT,0) <= 9000 then '<=9000'
when a.ADJ_MAX6 - isnull(b.INSTALLMENT,0)  > 9000 then '>9000'
  else '' end as max6_postdisb_bucket,

case 
when a.ADJ_MAX7 - isnull(b.INSTALLMENT,0) <= 0 then '<=0'
when a.ADJ_MAX7 - isnull(b.INSTALLMENT,0) <= 1000 then '<=1000'
when a.ADJ_MAX7 - isnull(b.INSTALLMENT,0) <= 2000 then '<=2000'
when a.ADJ_MAX7 - isnull(b.INSTALLMENT,0) <= 3000 then '<=3000'
when a.ADJ_MAX7 - isnull(b.INSTALLMENT,0) <= 4000 then '<=4000'
when a.ADJ_MAX7 - isnull(b.INSTALLMENT,0) <= 5000 then '<=5000'
when a.ADJ_MAX7 - isnull(b.INSTALLMENT,0) <= 6000 then '<=6000'
when a.ADJ_MAX7 - isnull(b.INSTALLMENT,0) <= 7000 then '<=7000'
when a.ADJ_MAX7 - isnull(b.INSTALLMENT,0) <= 8000 then '<=8000'
when a.ADJ_MAX7 - isnull(b.INSTALLMENT,0) <= 9000 then '<=9000'
when a.ADJ_MAX7 - isnull(b.INSTALLMENT,0)  > 9000 then '>9000'
else '' end as max7_postdisb_bucket,

case
when a.Scoreband < 50                then 'No Scoreband'
when a.Scoreband in (50,51,52,53,54) then 'Low Risk'
when a.Scoreband in (55,56,57,58)    then 'Medium Risk'
when a.Scoreband in (59,60,61,62,63) then 'High Risk'
when a.Scoreband in (64,65,66,67)    then 'Rejected'
when a.Scoreband in (68,69,70,71,72) then 'Thin File'
else '' end as Risk_Group

from creditprofitability.dbo.FinalAffordabilityDetail_Daily a 
left join prd_ExactusSync.dbo.AC00100P b
on a.loanid = b.loanid 
where a.creationmonth >= 202201
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Remove Duplicates */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_FinalAffordabilityDetail6_Daily')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_FinalAffordabilityDetail6_Daily
 END;

select top 0  A.*
into creditprofitability.dbo.TS_FinalAffordabilityDetail6_Daily
FROM (SELECT top 0 *,  row_number() over (partition by Loanid order by Creationdate desc,Loanid desc) as r2 
      from creditprofitability.dbo.FinalAffordabilityDetail_Daily_Ext ) AS A
WHERE r2 =1;


insert into creditprofitability.dbo.TS_FinalAffordabilityDetail6_Daily
select A.*
FROM (SELECT *,  row_number() over (partition by Loanid order by Creationdate desc,Loanid desc) as r2 
      from creditprofitability.dbo.FinalAffordabilityDetail_Daily_Ext ) AS A
WHERE r2 =1;


ALTER TABLE creditprofitability.dbo.TS_FinalAffordabilityDetail6_Daily DROP COLUMN r2
) 

by APS;
DISCONNECT FROM APS;
quit;


                       /* Compuscan and TransUnion scores */

/* Compuscan Scores */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_CS_Bureau_Score_Final')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_CS_Bureau_Score_Final
 END;

CREATE TABLE CreditProfitability.dbo.TS_CS_Bureau_Score_Final
WITH (DISTRIBUTION = HASH(tranappnumber), CLUSTERED COLUMNSTORE INDEX)
AS Select a.* 
From 
(Select ROW_NUMBER() over (partition by right(uniqueid,9) order by uniqueid desc) as latest,
right(uniqueid,9) as tranappnumber,
left(replace(applicationdate,'-', ''),6) as applicationmonth,
uniqueid, 
prismscoremi,
Bureau_Score_Grouping =         
case when prismscoremi >=0 and prismscoremi <= 479 then'Thin File (0-479)'                                           
     when prismscoremi >=480 and prismscoremi <=605 then'Very High Risk (480-605)'                                               
     when prismscoremi >=606 and prismscoremi <=621 then'High Risk (606-621)'
     when prismscoremi >=622 and prismscoremi <=641 then'Medium Risk (622-641)'                                                 
     when prismscoremi >=642 and prismscoremi <=667 then'Low Risk (642-667)'                                               
     when prismscoremi >=668 then 'Minimum Risk (>668)'
else 'No Bureau Score'
end 

from prd_press.capri.capri_bur_profile_pinpoint_2021 
where applicationdate >= '2022-01-01') a
Where a.latest =1
) 

by APS;
DISCONNECT FROM APS;
quit;


 /* Transunion Scores */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TS_TU_Bureau_Score_Final')
 BEGIN
 DROP TABLE Creditprofitability.dbo.TS_TU_Bureau_Score_Final
 END;

CREATE TABLE CreditProfitability.dbo.TS_TU_Bureau_Score_Final
WITH 
(DISTRIBUTION = HASH(tranappnumber))
AS 
Select a.* 
From
 (Select ROW_NUMBER() over (partition by right(uniqueid,9) order by uniqueid desc) as latest ,
 right(uniqueid,9) as tranappnumber,
 left(replace(applicationdate,'-', ''),6) as applicationmonth, 
 uniqueid,
 convert(float,Lng_Scr) as Lng_Scr,Bureau_Score_Grouping =         
 case when Lng_Scr <= 10 then'Thin File (<= 10)'                                           
 when Lng_Scr >=11 and Lng_Scr <=579 then'Very High Risk (11-579)'                                               
 when Lng_Scr >=580 and Lng_Scr <=614 then'High Risk (580-614)'
 when Lng_Scr >=615 and Lng_Scr <=629 then'Medium Risk (615-629)'                                                 
 when Lng_Scr >=630 and Lng_Scr <=659 then'Low Risk (630-659)'                                               
 when Lng_Scr >=660 then 'Minimum Risk (>660)'
 else 'No Bureau Score'
 end 
from prd_press.[capri].[CAPRI_BUR_PROFILE_TRANSUNION_PLSCORECARD_2021] 
where  applicationdate >= '2022-01-01') a
Where a.latest =1
) 

by APS;
DISCONNECT FROM APS;
quit;


/* Final Afforadability Table with bureau scores */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'Daily_Affordability_Table')
 BEGIN
 DROP TABLE Creditprofitability.dbo.Daily_Affordability_Table
 END;

CREATE TABLE CreditProfitability.dbo.Daily_Affordability_Table
WITH
(DISTRIBUTION = HASH(loanid))
AS
SELECT 
A.*,
B.Lng_Scr,
B.Bureau_Score_Grouping as TU_Bureau_Score_Grouping,
C.prismscoremi,
C.Bureau_Score_Grouping as CS_Bureau_Score_Grouping,
isnull(D.OPD,0) as OPD, 
isnull(E.Incr_Cap_Disb,0) as Incr_Cap_Disb, 
isnull(D.OPD,0) + isnull(E.Incr_Cap_Disb,0) as Overall_OPD
from creditprofitability.dbo.TS_FinalAffordabilityDetail6_Daily  as A
left join  CreditProfitability.dbo.TS_TU_Bureau_Score_Final as B
on A.uniqueid = B.uniqueid
left join  CreditProfitability.dbo.TS_CS_Bureau_Score_Final as C
on A.uniqueid = C.uniqueid
left join CreditProfitability.dbo.loan_pricing_daily D
on a.loanid = D.loanid
left join CreditProfitability.dbo.card_pricing_daily E
on a.loanid = E.loanid 
where a.creationmonth >= 202201 
) 

by APS;
DISCONNECT FROM APS;
quit;


 /* SZ - Final Afforadability Table with bureau scores */
proc sql;
connect to ODBC as APS (dsn=MPWAPS);
execute(
USE Creditprofitability
IF EXISTS 
(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'SZ_Daily_Affordability_Table')
 BEGIN
 DROP TABLE Creditprofitability.dbo.SZ_Daily_Affordability_Table
 END;

CREATE TABLE CreditProfitability.dbo.SZ_Daily_Affordability_Table
WITH 
(DISTRIBUTION = HASH(loanid))
AS
SELECT * from creditprofitability.dbo.Daily_Affordability_Table
) 

by APS;
DISCONNECT FROM APS;
quit;