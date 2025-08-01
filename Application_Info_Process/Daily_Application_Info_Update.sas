proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/*	-------------------------------------------1 Max_UniqID --------------------------------------------*/
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'Max_UniqID')
  BEGIN
    DROP TABLE Creditprofitability.dbo.Max_UniqID
  END;

CREATE TABLE Creditprofitability.dbo.Max_UniqID
with (DISTRIBUTION = HASH (uniqueid), CLUSTERED COLUMNSTORE INDEX) as
select		a.tranappnumber, max(a.UniqueID) as uniqueid, min(a.UniqueID) as FirstContact
from		PRD_Press.capri.capri_loan_application a
where		datediff(day,ApplicationDate,getdate()) <= 100 /*Run only for the apps in the last x days*/
and			a.TRANSEQUENCE <> '005'   
group by	a.tranappnumber
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'FirstStrikeInfo')
  BEGIN
    DROP TABLE Creditprofitability.dbo.FirstStrikeInfo
  END;

CREATE TABLE Creditprofitability.dbo.FirstStrikeInfo
with (DISTRIBUTION = HASH (LoanRef), CLUSTERED COLUMNSTORE INDEX) as
select	distinct LoanReference as LoanRef, PlatformID,121 AS DDD
from	PRD_EXACTUSsync..ST20100p a
join	(select LoanReference as LoanRef, min(StrikeDate) as StrikeDate
		 from PRD_EXACTUSsync..ST20100p
		 where StrikeType = 'N'
		 group by LoanReference) b
on		a.LoanReference = b.LoanRef
and		a.StrikeDate = b.StrikeDate
where	a.StrikeType = 'N'
	) by APS;
	DISCONNECT FROM APS;
quit;
/*   
---------------------------------------------------------------------------------------
---- 2 Max Affordabilty Indicators ------------------------------------------------------
---------------------------------------------------------------------------------------
*/
proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'App_Afford')
  BEGIN
    DROP TABLE Creditprofitability.dbo.App_Afford
  END;

CREATE TABLE Creditprofitability.dbo.App_Afford
with (DISTRIBUTION = HASH (Uniqueid), CLUSTERED COLUMNSTORE INDEX) as
select		x.UniqueID
			, min(a.MaxNaedoBase) as MaximumInstalment2
			, min(a.MaxABExposureBase) as MaximumInstalment4
			, min(a.Maxcompliancebase) as MaximumInstalment5
			, min(a.Max7base) as MaximumInstalment7
			, min(b.instalmentMaxRTI) as MaximumInstalment6
from		Creditprofitability.dbo.Max_UniqID x
left join	PRD_PRESS.capri.capri_affordability_results a
on			x.UniqueID = a.UniqueID
left join	PRD_PRESS.capri.ClientProfile_MaxRTIInstalment_RTIFunction b
on			x.UniqueID = b.UniqueID
group by	x.UniqueID
	) by APS;
	DISCONNECT FROM APS;
quit;

/*
---------------------------------------------------------------------------------------
----3 Rebuild table to create decline type flags ---------------------------------------
---------------------------------------------------------------------------------------
*/
proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'App_Offers_Removed')
  BEGIN
    DROP TABLE Creditprofitability.dbo.App_Offers_Removed
  END;

CREATE TABLE Creditprofitability.dbo.App_Offers_Removed
with (DISTRIBUTION = HASH (Uniqueid), CLUSTERED COLUMNSTORE INDEX) as
select		x.UniqueID
from		Creditprofitability.dbo.Max_UniqID x
join		PRD_Press.capri.OFFERSTRATEGY_OFFERKNOCKOUT_RULES a
on			x.UniqueID = a.UniqueID
where		a.OFFERREMOVERULE = 'OA132'
group by	x.UniqueID
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'App_Declines')
  BEGIN
    DROP TABLE Creditprofitability.dbo.App_Declines
  END;

CREATE TABLE Creditprofitability.dbo.App_Declines
with (DISTRIBUTION = HASH (uniqueid), CLUSTERED COLUMNSTORE INDEX) as
select		a.uniqueid, a.DeclineCode
			, isnull(b.MainCategory,'Unmapped') as MainCategory
			, isnull(b.SubCategory,'Unmapped') as SubCategory
			, case when c.UniqueID is null then 0 else 1 end as Offers_Removed
from		Creditprofitability.dbo.Max_UniqID x
join		PRD_Press.capri.capri_application_decline a
on			x.UniqueID = a.UniqueID
left join	CreditProfitability..BusinessRulesCategorization b
on			a.DeclineCode = b.DeclineCode
left join	CreditProfitability..App_Offers_Removed c
on			x.UniqueID = c.UniqueID
	) by APS;
	DISCONNECT FROM APS;
quit;
proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'App_Declines_Uniqt')
  BEGIN
    DROP TABLE Creditprofitability.dbo.App_Declines_Uniqt
  END;

CREATE TABLE Creditprofitability.dbo.App_Declines_Uniqt
with (DISTRIBUTION = HASH (uniqueid), CLUSTERED COLUMNSTORE INDEX) as
select		a.UniqueID
			/* Main Category */
			, max(case when MainCategory = 'Compliance External' then 1 else 0 end) as Compliance_External
			/*--, max(case when MainCategory = 'Compliance Internal' then 1 else 0 end) as Compliance_Internal*/
			, max(case when MainCategory = 'Credit Rules' then 1 else 0 end) as Credit_Rules
			, max(case when MainCategory = 'Unmapped' then 1 else 0 end) as Unmapped_MC

			/* Sub Category */
			, max(case when MainCategory = 'Compliance External' and SubCategory = 'DC' then 1 else 0 end) as Ext_DC
			, max(case when MainCategory = 'Compliance External' and SubCategory = 'Judgement' then 1 else 0 end) as Ext_Judgement

			, max(case when MainCategory = 'Compliance Internal' and SubCategory = 'Admin' then 1 else 0 end) as Int_Admin
			, max(case when MainCategory = 'Compliance Internal' and SubCategory = 'DC' then 1 else 0 end) as Int_DC
			, max(case when MainCategory = 'Compliance Internal' and SubCategory = 'Min Income' then 1 else 0 end) as Int_Min_Income
			, max(case when MainCategory = 'Compliance Internal' and SubCategory = 'Bureau' then 1 else 0 end) as Int_Bureau

			, max(case when MainCategory = 'Credit Rules' and SubCategory = '40 days since last disbursal' then 1 else 0 end) as Cred_40D
			, max(case when MainCategory = 'Credit Rules' and SubCategory = 'Affordability' then 1 else 0 end) as Cred_Afford
			, max(case when MainCategory = 'Credit Rules' and SubCategory = 'Age' then 1 else 0 end) as Cred_Age
			, max(case when MainCategory = 'Credit Rules' and SubCategory = 'Bureau' then 1 else 0 end) as Cred_Bureau
			, max(case when MainCategory = 'Credit Rules' and SubCategory = 'CD/Recency' then 1 else 0 end) as Cred_CD_REC
			, max(case when MainCategory = 'Credit Rules' and SubCategory = 'Employment' then 1 else 0 end) as Cred_Emp
			, max(case when MainCategory = 'Credit Rules' and SubCategory = 'High Risk >2 loans' then 1 else 0 end) as Cred_2HRL
			, max(case when MainCategory = 'Credit Rules' and SubCategory = 'Scoring' then 1 else 0 end) as Cred_Scoring

			, max(case when SubCategory = 'Unmapped' then 1 else 0 end) as Unmapped_SC

			, max(Offers_Removed) as Offers_Removed

from		Creditprofitability.dbo.App_Declines a
group by	a.UniqueID
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'App_Declines_Uniq')
  BEGIN
    DROP TABLE Creditprofitability.dbo.App_Declines_Uniq
  END;

CREATE TABLE Creditprofitability.dbo.App_Declines_Uniq
with (DISTRIBUTION = HASH (uniqueid), CLUSTERED COLUMNSTORE INDEX) as
select	a.*
		, Compliance_Internal = case
									/*when Cred_Afford = 1 and Offers_Removed = 1 then 1 --Min affordability*/
									when Int_Admin + Int_DC + Int_Min_Income + Int_Bureau > 0 then 1
									else 0 end
		, Int_Min_Aff = case when /*Cred_Afford = 1 and */Offers_Removed = 1 then 1 else 0 end
from	Creditprofitability.dbo.App_Declines_Uniqt a
	) by APS;
	DISCONNECT FROM APS;
quit;

/*
---------------------------------------------------------------------------------------
4---------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------
*/
proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'App_Offer')
  BEGIN
    DROP TABLE Creditprofitability.dbo.App_Offer
  END;

CREATE TABLE Creditprofitability.dbo.App_Offer
with (DISTRIBUTION = HASH (TranAppNumber), CLUSTERED COLUMNSTORE INDEX) as
select		a.TranAppNumber
			, max(case when a.UniqueID is not null then 1 else 0 end) as Offer
			, max(case when LoanCapital > 0 then 1 else 0 end) as Loan_Offer
			, max(case when CardLimit > 0 then 1 else 0 end) as Card_Offer
			, max(Term) as Term_Offered
			, max(case when LoanCapital > CardLimit then LoanCapital else CardLimit end) as Max_Offer
			, max(LoanCapital) as Max_Loan_Offer
			, max(CardLimit) as Max_Card_Offer
from		Creditprofitability.dbo.Max_UniqID x
join		PRD_Press.capri.capri_offer_results a
on			x.TranAppNumber = a.TranAppNumber
group by	a.TranAppNumber
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'App_BhvSc')
  BEGIN
    DROP TABLE Creditprofitability.dbo.App_BhvSc
  END;

CREATE TABLE Creditprofitability.dbo.App_BhvSc
with (DISTRIBUTION = HASH (uniqueid), CLUSTERED COLUMNSTORE INDEX) as
select		a.uniqueid, a.score as behavescore
from		Max_UniqID x
left join	PRD_Press.capri.capri_behavioural_score a
on			x.UniqueID = a.UniqueID
where		classification = 'AB'
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'App_ScoreR')
  BEGIN
    DROP TABLE Creditprofitability.dbo.App_ScoreR
  END;

CREATE TABLE Creditprofitability.dbo.App_ScoreR
with (DISTRIBUTION = HASH (uniqueid), CLUSTERED COLUMNSTORE INDEX) as
select		a.uniqueid
			, max(a.Scorecard) as ScoreCard
			, max(a.ScoreBand) as ScoreBand
			, max(1-a.FINALRISKSCORE/1000) as Scorecard_Exp
			, max(b.prismscoremi) as prismscoremi
			, max(c.Lng_Scr) as Lng_Scr
			, max(isnull(a.OverRideScoreBand,-99)) as OverRideScoreBand
from		Creditprofitability.dbo.Max_UniqID x
left join	PRD_Press.capri.capri_scoring_results a
on			x.UniqueID = a.UniqueID
left join	prd_press.capri.capri_bur_profile_pinpoint b
on			x.UniqueID = b.UniqueID
left join	prd_press.capri.CAPRI_BUR_PROFILE_TRANSUNION_PLSCORECARD c
on			x.UniqueID = c.UniqueID
group by	a.UniqueID
	) by APS;
	DISCONNECT FROM APS;
quit;
proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'App_Income')
  BEGIN
    DROP TABLE Creditprofitability.dbo.App_Income
  END;

CREATE TABLE Creditprofitability.dbo.App_Income
with (DISTRIBUTION = HASH (uniqueid), CLUSTERED COLUMNSTORE INDEX) as
select		a.uniqueid
			, min(a.avgGrossIncome) as GrossIncome
from		Creditprofitability.dbo.Max_UniqID x
left join	PRD_Press.Capri.CreditRisk_AverageGrossIncome a
on			x.UniqueID = a.UniqueID
group by	a.UniqueID
	) by APS;
	DISCONNECT FROM APS;
quit;
proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TEMP1')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TEMP1
  END;

CREATE TABLE Creditprofitability.dbo.TEMP1
with (DISTRIBUTION = HASH (UniqueID), CLUSTERED COLUMNSTORE INDEX) as
Select			a.uniqueid, a.tranappnumber
				, a.transysteminstance, a.branchcode ,a.channelcode
				, e.channelcode as First_ChannelCode
				, e.ApplicationDate as FirstHitCapriOn
				, isnull(a.RequestedProduct,'N/A') as RequestedProduct
from			CreditProfitability..Max_UniqID x
left join		PRD_Press.capri.capri_loan_application a					 on x.uniqueid = a.uniqueid
left join		PRD_Press.capri.capri_loan_application e					 on x.FirstContact = e.uniqueid
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TEMP1_1')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TEMP1_1
  END;

CREATE TABLE Creditprofitability.dbo.TEMP1_1
with (DISTRIBUTION = HASH (UniqueID), CLUSTERED COLUMNSTORE INDEX) as
select			x.*
				, b.ApplicationDate, b.ApplicationTime, b.caprikey, b.nationalid, b.CallNumber
				, c.TypeCode, c.CURRENTEMPLOYMENTSTARTDATE,c.EMPLOYERGROUPCODE, c.employersubgroupcode, c.MEMBER
				, c.ACCREDITEDINDICATOR, c.SUBGROUPCREATEDATE,c.WAGEFREQUENCYCODE
				, c.IsEmployerSubGroup, c.IsSubgroupLimitation, c.IsSubGroupDecline
				, c.StaffIndicator, c.PersalClient
from			Creditprofitability.dbo.TEMP1 x
left join		PRD_Press.capri.CAPRI_APPLICANT b							 on x.uniqueid = b.uniqueid
left join		PRD_Press.capri.CAPRI_EMPLOYMENT c							 on x.uniqueid = c.uniqueid
	) by APS;
	DISCONNECT FROM APS;
quit;
proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TEMP1_2')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TEMP1_2
  END;

CREATE TABLE Creditprofitability.dbo.TEMP1_2
with (DISTRIBUTION = HASH (UniqueID), CLUSTERED COLUMNSTORE INDEX) as
select			x.*
				, d.MONTHLYGROSSINCOME,d.MONTHLYNETTINCOME, d.CALCULATEDNETTINCOME
				, f.IsExistentIndicator
				, g.UnsecuredAndRevolvingDebt
from			Creditprofitability.dbo.TEMP1_1 x
left join		PRD_Press.capri.CAPRI_AFFORDABILITY d						 on x.uniqueid = d.uniqueid
left join		PRD_Press.capri.capri_Account_Summary f						 on x.uniqueid = f.uniqueid
left join		PRD_Press.capri.ClientProfile_MaxRTIInstalment_RTIFunction g on x.UniqueID = g.uniqueid
	) by APS;
	DISCONNECT FROM APS;
quit;
proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'TEMP2')
  BEGIN
    DROP TABLE Creditprofitability.dbo.TEMP2
  END;

CREATE TABLE Creditprofitability.dbo.TEMP2
	with (DISTRIBUTION = HASH (UniqueID), CLUSTERED COLUMNSTORE INDEX) as
	select			x.*
					, e.INSTITUTIONCODE
					, f.Scorecard, f.ScoreBand, f.Scorecard_Exp, f.prismscoremi, f.Lng_Scr, f.OverRideScoreBand
					, h.behavescore
					, m.SCORECARDVERSION, m.AFFORDABILITYTESTINGSTRATEGY, m.SCORINGTESTINGSTRATEGY, m.RandomNumber
					, Offer = isnull(j.Offer,0)
					, Loan_Offer = isnull(Loan_Offer,0)
					, Card_Offer = isnull(Card_Offer,0)
					, Term_Offered = isnull(Term_Offered,-99)
					, Max_Offer = isnull(Max_Offer,0)
					, Max_Loan_Offer = isnull(Max_Loan_Offer,0)
					, Max_Card_Offer = isnull(Max_Card_Offer,0)
					, Compliance_External = isnull(k.Compliance_External,0)
					, Compliance_Internal = isnull(k.Compliance_Internal,0)
					, Credit_Rules		  = isnull(k.Credit_Rules,0)
					, Unmapped_MC		  = isnull(k.Unmapped_MC,0)
					, Ext_DC			  = isnull(k.Ext_DC,0)
					, Ext_Judgement		  = isnull(k.Ext_Judgement,0)
					, Int_Admin			  = isnull(k.Int_Admin,0)
					, Int_DC			  = isnull(k.Int_DC,0)
					, Int_Min_Income	  = isnull(k.Int_Min_Income,0)
					, Int_Min_Aff		  = isnull(k.Int_Min_Aff,0)
					, Int_Bureau		  = isnull(k.Int_Bureau,0)
					, Cred_40D			  = isnull(k.Cred_40D,0)
					, Cred_Afford		  = isnull(k.Cred_Afford,0)
					, Cred_Age			  = isnull(k.Cred_Age,0)
					, Cred_Bureau		  = isnull(k.Cred_Bureau,0)
					, Cred_CD_REC		  = isnull(k.Cred_CD_REC,0)
					, Cred_Emp			  = isnull(k.Cred_Emp,0)
					, Cred_2HRL		      = isnull(k.Cred_2HRL,0)
					, Cred_Scoring		  = isnull(k.Cred_Scoring,0)
					, Unmapped_SC		  = isnull(k.Unmapped_SC,0)
					, Offers_Removed	  = isnull(k.Offers_Removed,0)
					, MaximumInstalment2  = isnull(g.MaximumInstalment2, -10000)
					, MaximumInstalment4  = isnull(g.MaximumInstalment4, -10000)
					, MaximumInstalment5  = isnull(g.MaximumInstalment5, -10000)
					, MaximumInstalment6  = isnull(g.MaximumInstalment6, -10000)
					, MaximumInstalment7  = isnull(g.MaximumInstalment7, -10000)
					, GrossIncome		  = isnull(i.GrossIncome,0)
	from			Creditprofitability.dbo.TEMP1_2 x
	left join		PRD_Press.capri.CAPRI_BANKING e								on x.uniqueid = e.uniqueid
	left join		PRD_Press.capri.CAPRI_TESTING_STRATEGY_RESULTS m			on x.uniqueid = m.uniqueid
	left join		CreditProfitability..App_Declines_Uniq k					on x.uniqueid = k.uniqueid
	left join		CreditProfitability..App_Offer j							on x.TranAppNumber = j.TranAppNumber
	left join		CreditProfitability..App_BhvSc h							on x.uniqueid = h.uniqueid
	left join		CreditProfitability..App_ScoreR f							on x.uniqueid = f.uniqueid
	left join		CreditProfitability..App_Afford g							on x.uniqueID = g.uniqueid
	left join		CreditProfitability..App_Income i							on x.uniqueID = i.uniqueid
			
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(

USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'AppsLastXDays')
  BEGIN
    DROP TABLE Creditprofitability.dbo.AppsLastXDays
  END;

CREATE TABLE Creditprofitability.dbo.AppsLastXDays
with (DISTRIBUTION = HASH (TranAppNumber), CLUSTERED COLUMNSTORE INDEX) as
select		a.TranAppNumber
			
			, sum(case when  b.ApplicationDate >= a.ApplicationDate and b.ApplicationDate <= dateadd(day,35,a.ApplicationDate)
				 then 1 else 0 end) as AppsPerClient_35d
			
from		Creditprofitability.dbo.TEMP2 a
left join	Creditprofitability.dbo.TEMP2 b on a.NationalID = b.NationalID
where		b.ApplicationDate >= a.ApplicationDate and b.ApplicationDate <= dateadd(day,60,a.ApplicationDate)
			and a.TranAppNumber <> b.TranAppNumber 
			and b.UniqueID > a.UniqueID /* implies that B happend after A to cater for same day applications */ 
group by	a.TranAppNumber
			
	) by APS;
	DISCONNECT FROM APS;
quit;

proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'DTemp1')
  BEGIN
    DROP TABLE Creditprofitability.dbo.DTemp1
  END;

CREATE TABLE Creditprofitability.dbo.DTemp1
with (DISTRIBUTION = HASH (ApplicationID), CLUSTERED COLUMNSTORE INDEX) as
select distinct a.ApplicationID, a.OfferID, a.UniqueID
			, b.InitiationFeeExcludingVAT, b.InitiationFeeVATPortion
			, b.InterestRate, b.InsuranceRate 
			, b.MonthlyFeeExcludingVAT, b.MonthlyFeeVATPortion
			, b.InstalmentFinalPRD as Instalment, b.PrincipalDebt, b.FirstDueDate
from		PRD_ExactusSync..ApplicationOffers a
left join	PRD_ExactusSync..ApplicationOfferingPricing  b
on			a.ApplicationID = b.ApplicationID and a.UniqueID = b.UniqueID and a.OfferID = b.OfferID
where		a.OFFERSSELECTEDTYPE = 'SEL' and isnull(b.PrincipalDebt,0) > 0
	union
select distinct a.ApplicationID, a.OfferID, a.UniqueID
			, InitiationFeeExcludingVAT = b.INITIATIONFEE - b.VATONINITIATIONFEE
			, InitiationFeeVATPortion = b.VATONINITIATIONFEE
			, InterestRate = b.AnnualRateDR
			, b.InsuranceRate 
			, MonthlyFeeExcludingVAT = b.MonthlyFee - b.VATONMONTHLYFEES
			, VATONMONTHLYFEES = b.VATonMonthlyFees
			, Instalment = case when b.INSTALMENTONOUTBAL > 0 then b.INSTALMENTONOUTBAL else b.INSTALMENTONLIMIT end
			, b.Limit as PrincipalDebt, b.FirstDueDate
from		PRD_ExactusSync..ApplicationOffers a
left join	PRD_ExactusSync..ApplicationOfferCreditCard  b
on			a.ApplicationID = b.ApplicationID and a.UniqueID = b.UniqueID and a.OfferID = b.OfferID
where		a.OFFERSSELECTEDTYPE = 'SEL' and isnull(b.Limit,0) > 0
	) by APS;
	DISCONNECT FROM APS;
quit;
proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'DTemp2')
  BEGIN
    DROP TABLE Creditprofitability.dbo.DTemp2
  END;

CREATE TABLE Creditprofitability.dbo.DTemp2
with (DISTRIBUTION = HASH (ApplicationID), CLUSTERED COLUMNSTORE INDEX) as
select		isnull(b.LimitIncreaseDate,c.StartDate) as StartDate
			, isnull(b.LoanID,c.LoanID) as ApplicationID, isnull(b.AccountReference,c.Loanreference) as Loanreference
			, Product = case when b.LoanID is null then 'Loan' else 'Card' end
			, isnull(b.Capital,c.Capital) as Capital
			, isnull(b.OrigPrinciple,c.OPD) as OPD
			, case when b.ProductDescription = 'Migration / Limit increase'
				then isnull(b.Incr_Cap_Disb,0) else 0 end as Limit_Increase
			, isnull(c.CapitalDisbursed,0) as CapitalDisbursed
			, isnull(c.Term,999) as Term
from			creditprofitability.dbo.Card_Pricing_Daily b
full outer join	creditprofitability.dbo.Loan_Pricing_Daily c
on				b.LoanID = c.LoanID
	) by APS;
	DISCONNECT FROM APS;
quit;
proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'Disbursement_Table')
  BEGIN
    DROP TABLE Creditprofitability.dbo.Disbursement_Table
  END;

CREATE TABLE Creditprofitability.dbo.Disbursement_Table
with (DISTRIBUTION = HASH (ApplicationID), CLUSTERED COLUMNSTORE INDEX) as
select		a.*, b.Instalment, b.FirstDueDate
from		Creditprofitability.dbo.DTemp2 a
left join	Creditprofitability.dbo.DTemp1 b
on			a.ApplicationID = b.ApplicationID
	) by APS;
	DISCONNECT FROM APS;
quit;
proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'HasNonQQ')
  BEGIN
    DROP TABLE Creditprofitability.dbo.HasNonQQ
  END;

CREATE TABLE Creditprofitability.dbo.HasNonQQ
with (DISTRIBUTION = HASH (TranAppNumber), CLUSTERED COLUMNSTORE INDEX) as
select		x.TranAppNumber, HasNonQQ = max(case when a.ChannelCode not in('CCC028') then 1 else 0 end)
from		Creditprofitability.dbo.Max_UniqID x
left join	PRD_Press.capri.capri_loan_application a
on			x.TranAppNumber = a.TranAppNumber
group by	x.TranAppNumber
	) by APS;
	DISCONNECT FROM APS;
quit;
proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'Settled_Loans')
  BEGIN
    DROP TABLE Creditprofitability.dbo.Settled_Loans
  END;

CREATE TABLE Creditprofitability.dbo.Settled_Loans
with (DISTRIBUTION = HASH (ApplicationID), CLUSTERED COLUMNSTORE INDEX) as
select		ApplicationID
            , Internalsettledloans = sum(case when TradeType = 'Int' then isnull(Instalment,0) else 0 end)
            , IntBalSettled = sum(case when TradeType = 'Int' then isnull(SettlementAmount,0) else 0 end)
			, IntCount = sum(case when TradeType = 'Int' then 1 else 0 end)
            , Externalsettledloans = sum(case when TradeType = 'Ext' then isnull(Instalment,0) else 0 end)
            , ExtBalSettled = sum(case when TradeType = 'Ext' then isnull(SettlementAmount,0) else 0 end)
			, ExtCount = sum(case when TradeType = 'Ext' then 1 else 0 end)
from (
	select distinct	a.ApplicationID, a.OfferID, a.UniqueID, b.TradeSequenceID
				, c.TradeType, c.SubscriberName, c.Instalment, c.OutstandingBalance, c.SettlementAmount
	from		PRD_ExactusSync..ApplicationOffers a
	left join	PRD_ExactusSync..APPLICATIONOFFERTRADEMAP  b
	on			a.ApplicationID = b.ApplicationID and a.UniqueID = b.UnqueID and a.OfferID = b.OfferID
	left join	PRD_ExactusSync..APPLICATIONTRADES c
	on			b.TradeSequenceID = c.SEQUENCEID
	where		a.OFFERSSELECTEDTYPE = 'SEL'
	) as SQ
group by SQ.ApplicationID
	) by APS;
	DISCONNECT FROM APS;
quit;
proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
/* Replace "APPINFO_BACKUP" with a union of previous "APPINFO_BACKUP" that excludes the recent dates, and a temp table that only has the recent dates */
	/* First create the temp table */
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'AppInfo_Temp')
  BEGIN
    DROP TABLE Creditprofitability.dbo.AppInfo_Temp
  END;

CREATE TABLE Creditprofitability.dbo.AppInfo_Temp
	with (DISTRIBUTION = HASH (TranAppNumber), CLUSTERED COLUMNSTORE INDEX) as
	select distinct a.*
			, case
				when a.IsEmployerSubGroup = 'FALSE' then 'No Subgroup Impact'
				when a.IsSubGroupLimitation = 'TRUE' then 'Subgroup Limitation'
				when a.IsSubGroupDecline = 'TRUE' then 'Subgroup Decline'
				else 'N/A'
			  end as SubGroupLimitation
			, Org_Source = case
					when a.ChannelCode in('CCC001','CCC020') then 'Branch'
					when a.ChannelCode in('CCC003') then 'Virtual Branch'
					when a.ChannelCode in('CCC004') then 'USD'
					when a.ChannelCode in('CCC006','CCC023') then 'Prospecting'
					when a.ChannelCode in('CCC007','CCC026') then 'Call Centre'
					when a.ChannelCode in('CCC008') then 'Transf. CC to Branch'
					when a.ChannelCode in('CCC013') then 'Direct Fulfilment'
					when a.ChannelCode in('CCC015') then 'Metropolitan'
					when a.ChannelCode in('CCC016') then 'Momentum'
					when a.ChannelCode in('CCC017') then 'Vuka'
					when a.ChannelCode in('CCC021') then 'Credit on Web (COW)'
					when a.ChannelCode in('CCC028') then 'Quick Quote'
					when a.ChannelCode in('CCC030') then 'Open API'
				end
			, First_Contact = case
					when a.First_ChannelCode in('CCC001','CCC020') then 'Branch'
					when a.First_ChannelCode in('CCC003') then 'Virtual Branch'
					when a.First_ChannelCode in('CCC004') then 'USD'
					when a.First_ChannelCode in('CCC006','CCC023') then 'Prospecting'
					when a.First_ChannelCode in('CCC007','CCC026') then 'Call Centre'
					when a.First_ChannelCode in('CCC008') then 'Transf. CC to Branch'
					when a.First_ChannelCode in('CCC013') then 'Direct Fulfilment'
					when a.First_ChannelCode in('CCC015') then 'Metropolitan'
					when a.First_ChannelCode in('CCC016') then 'Momentum'
					when a.First_ChannelCode in('CCC017') then 'Vuka'
					when a.First_ChannelCode in('CCC021') then 'Credit on Web (COW)'
					when a.First_ChannelCode in('CCC028') then 'Quick Quote'
					when a.First_ChannelCode in('CCC030') then 'Open API'
				end
			, case 
					when IsNull(Lng_Scr,0) <= 100 then 0
					when IsNull(Lng_Scr,0) <= 577 then 1	
					when IsNull(Lng_Scr,0) <= 590 then 2	
					when IsNull(Lng_Scr,0) <= 600 then 3	
					when IsNull(Lng_Scr,0) <= 612 then 4	
					when IsNull(Lng_Scr,0) <= 623 then 5	
					when IsNull(Lng_Scr,0) <= 634 then 6	
					when IsNull(Lng_Scr,0) <= 646 then 7		
					when IsNull(Lng_Scr,0) <= 658 then 8		
					when IsNull(Lng_Scr,0) <= 671 then 9		
					else 10								
				end as Lng_Scr_Grp
				, PrismScoreGroup = 
				case
					when IsNull(PrismscoreMI,0) <= 100 then 0
					when IsNull(PrismscoreMI,0) <= 604 then 1	
					when IsNull(PrismscoreMI,0) <= 614 then 2	
					when IsNull(PrismscoreMI,0) <= 624 then 3	
					when IsNull(PrismscoreMI,0) <= 640 then 4		
					else 5													
				end
			, b.Product 
			, b.OPD
			, b.Capital
			, b.Limit_Increase
			, b.CapitalDisbursed
			, b.LoanReference
			, case when b.Limit_Increase > 0 then b.Limit_Increase
				   when b.Product = 'Card' then b.Capital
					else b.CapitalDisbursed end as CreditValue
			, isnull(C.Final_score_1,d.Final_score_1) as Final_score_1
			, isnull(C.FirstDueMonth,d.FirstDueMonth)  as FirstDueMonth
			, b.FirstDueDate
			, b.StartDate
			, isnull(cast(f.ApplicationStatus as varchar(8)),'Not OMNI') as ApplicationStatus
			, case when e.AppsPerClient_35d > 0 then 0 else 1 end as UniqueClientApp
			
			, isnull(Term,0) as Term
			, isnull(Internalsettledloans,0)  as Internalsettledloans
			, isnull(IntBalSettled,0) as IntBalSettled
			, isnull(Externalsettledloans,0) as Externalsettledloans
			, isnull(ExtBalSettled,0) as ExtBalSettled
			, isnull(Instalment,0) as Instalment
			, i.HasNonQQ
			
	from		CreditProfitability..TEMP2 a
	left join	Creditprofitability.dbo.Disbursement_Table b
	on			a.tranappnumber = cast(b.ApplicationID as varchar(15))
	left join	(select LoanID, Final_score_1, FirstDueMonth from CREDITPROFITABILITY.dbo.ELR_LOANESTIMATES_3_9_CALIB where Final_score_1 is not null) as c
	on			a.tranappnumber = cast(c.loanid as varchar(15))
	left join	(select LoanID, Final_score_1, FirstDueMonth from CREDITPROFITABILITY.dbo.ELR_CARDESTIMATES_3_9_CALIB where Final_score_1 is not null) as d
	on			a.tranappnumber = cast(d.loanid as varchar(15))
	left join	Creditprofitability.dbo.AppsLastXDays e
	on			a.TranAppNumber = e.TranAppNumber
	left join	Prd_ExactusSync.dbo.Applications f
	on			a.TranAppNumber = cast(f.ApplicationID as varchar(15))
	left join	Creditprofitability.dbo.Settled_Loans h
	on			a.TranAppNumber = cast(h.ApplicationID as varchar(15))
	left join	HasNonQQ i
	on			a.TranAppNumber = i.TranAppNumber
	) by APS;
	DISCONNECT FROM APS;
quit;
proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'AppInfo_Old')
  BEGIN
    DROP TABLE Creditprofitability.dbo.AppInfo_Old
  END;

CREATE TABLE Creditprofitability.dbo.AppInfo_Old
	with (DISTRIBUTION = HASH (TranAppNumber), CLUSTERED COLUMNSTORE INDEX) as
	select * from Creditprofitability.dbo.APPINFO_BACKUP where datediff(day,ApplicationDate,getdate()) > 100/* -- Old data excluding recent apps*/
	) by APS;
	DISCONNECT FROM APS;
quit;
proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'APPINFO_BACKUP')
  BEGIN
    DROP TABLE Creditprofitability.dbo.APPINFO_BACKUP
  END;

CREATE TABLE Creditprofitability.dbo.APPINFO_BACKUP
	with (DISTRIBUTION = HASH (TranAppNumber), CLUSTERED COLUMNSTORE INDEX) as
	select distinct * from Creditprofitability.dbo.AppInfo_Temp
		union all
	select distinct * from Creditprofitability.dbo.AppInfo_Old 
	) by APS;
	DISCONNECT FROM APS;
quit;
proc sql;
	connect to ODBC as APS (dsn=MPWAPS);
	execute
	(
USE Creditprofitability
IF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'AppInfo_Summary')
  BEGIN
    DROP TABLE Creditprofitability.dbo.AppInfo_Summary
  END;

CREATE TABLE Creditprofitability.dbo.AppInfo_Summary
with (DISTRIBUTION = HASH (AppMonth), CLUSTERED COLUMNSTORE INDEX) as
select		Org_Source, First_Contact, isnull(Product,'No Product') as Product, RequestedProduct, CallNumber, HasNonQQ
			, GrossIncome_7500 = case when cast(left(GrossIncome,charindex('.',GrossIncome)) + '00' as decimal) > 7500 then 'GT7500' else 'LE7500' end
			, WageFrequencyCode, InstitutionCode, BehaveScore, ScorecardVersion, ScoreBand
			, Offer as Offer_ind
			, case when OPD > 0 then 1 else 0 end as TakeUp_ind
			, FirstDueMonth
			, year(ApplicationDate) * 100 + month(ApplicationDate) as AppMonth
			, InLast7Days = case when datediff(day,ApplicationDate,getdate()) <= 7 then 1 else 0 end
			, Offers = sum(isnull(Offer,0))
			, sum(isnull(Loan_Offer,0)) as Loan_Offer
			, sum(isnull(Card_Offer,0)) as Card_Offer
			, sum(isnull(Term_Offered,0)) as Term_Offered
			, sum(isnull(Max_Loan_Offer,0)) as Loan_Offer_Size
			, sum(isnull(Max_Card_Offer,0)) as Card_Offer_Size
			, sum(isnull(Max_Offer,0)) as Max_Offer_Size
			, TakeUps = sum(case when OPD > 0 then 1 else 0 end)
			, TakeUps_Loan = sum(case when Product = 'Loan' and OPD > 0 then 1 else 0 end)
			, TakeUps_Card = sum(case when Product = 'Card' and OPD > 0 then 1 else 0 end)
			, OPD = sum(case when Product = 'Card' then Limit_Increase else OPD end)
			, Limit = sum(case when Product = 'Card' then Limit_Increase else 0 end)
			, LoanOPD = sum(case when Product = 'Loan' then OPD else 0 end)
			, SC_Exp_Risk = sum(Scorecard_Exp*OPD) 
			, SC_Exp_Risk_Inc_Buff = sum(case when ScorecardVersion = 'V622' then 1.1 else 1 end * Scorecard_Exp*OPD) 
			, Act_Risk = sum(Final_score_1*OPD)
			, count(*) as NumApp
			, sum(UniqueClientApp) as UniqueClientApp
			, sum(case when RequestedProduct in('CARD','LIMI') then 1 else 0 end) as CardApp
			, sum(case when RequestedProduct = 'LOAN' then 1 else 0 end) as LoanApp
			, sum(case when RequestedProduct = 'OVER' then 1 else 0 end) as OverDApp
from		Creditprofitability.dbo.APPINFO_BACKUP
group by	Org_Source, First_Contact, isnull(Product,'No Product'), RequestedProduct, CallNumber, HasNonQQ
			, case when cast(left(GrossIncome,charindex('.',GrossIncome)) + '00' as decimal) > 7500 then 'GT7500' else 'LE7500' end
			, WageFrequencyCode, InstitutionCode, BehaveScore, ScorecardVersion, ScoreBand
			, Offer
			, case when OPD > 0 then 1 else 0 end
			, FirstDueMonth, year(ApplicationDate) * 100 + month(ApplicationDate)
			, case when datediff(day,ApplicationDate,getdate()) <= 7 then 1 else 0 end 
			
	) by APS;
	DISCONNECT FROM APS;
quit;