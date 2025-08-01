options compress=yes;


proc sql; connect to odbc (dsn=MPWAPS);
select * from connection to odbc(
	IF OBJECT_ID('prd_DataDistillery_data.dbo.Disbursement_Info_Backup', 'U') IS NOT NULL 
	DROP TABLE prd_DataDistillery_data.dbo.Disbursement_Info_Backup;
	create table prd_DataDistillery_data.dbo.Disbursement_Info_Backup 
	with (distribution = hash(loanid), clustered columnstore index ) as
	select *
	from prd_DataDistillery_data.dbo.Disbursement_Info
); disconnect from odbc;
quit;

proc sql noprint stimer;
connect to ODBC (dsn=Cre_Scor);
create table JS_EHL_ProductTypes  as 
select * from connection to odbc 
	(Select * from EHL_ProductTypes);
disconnect from odbc ;
quit;

%Upload_APS(Set=Js_ehl_producttypes, Server = work, APS_ODBC = Prd_DDDa, APS_DB = PRD_DataDistillery_Data, Distribute=hash(ProductCode));

proc sql; connect to odbc (dsn=MPWAPS);
execute (
	/*---- Step0: Get LoanIDs and AccountStatus and create_date*/
	create table #S0_LIDBase with (distribution = hash(LoanID), clustered columnstore index) as 
	select distinct *
	from
	(
	select LoanID, Status, 'Gazelle' as TblRef 
		from ARC_LOANQ_2028.dbo.Loanquotation 
		where Status='DIS' /*--Gazelle Apps*/
	union all
	select ApplicationID as LoanID, ApplicationStatus as Status, 'Omni' as TblRef 
		from prd_ExactusSync.dbo.Applications 
		where ApplicationStatus='DIS' and ApplicationType in ('CRE','CBC') /*-- OMNI Apps*/
	union all
	select ApplicationID as LoanID, ApplicationStatus as Status, 'Omni' as TblRef 
		from prd_ExactusSync.dbo.ApplicationsHistory 
		where ApplicationStatus='DIS' and ApplicationType in ('CRE','CBC') /*-- OMNI Apps*/
	) X

	/*---- Step0b: Get create_date*/
	create table #TmpCrDte with (distribution = hash(LoanID), clustered columnstore index ) as 
	(
	select a.LoanID, b.Loanreference, case when b.TblRef='Loan' then a.create_date else b.TimeStamp end as create_date
	from (
	select ApplicationID as LoanID, creationdate as create_date, 'Omni' as TblRef 
		from prd_ExactusSync.dbo.Applications 
		where ApplicationStatus='DIS' and ApplicationType in ('CRE','CBC') /*-- OMNI Apps*/
	union all
	select ApplicationID as LoanID, creationdate as create_date, 'Omni' as TblRef 
		from prd_ExactusSync.dbo.ApplicationsHistory 
		where ApplicationStatus='DIS' and ApplicationType in ('CRE','CBC')) as a /*-- OMNI Apps*/
	left join
	(select ApplicationID as loanid, ltrim(rtrim(AccountReference)) as LoanReference, 'Card' as TblRef, 'Omni Card' as TblRef0, TimeStamp 
		from prd_exactusSync.dbo.CA22100P (nolock)
	union all
	select loanid, ltrim(rtrim(LoanReference)) as LoanReference, 'Loan' as TblRef, 'Omni+Gazelle Loan' as TblRef0, NULL as TimeStamp 
		from prd_exactusSync.dbo.ZA31200P (nolock)  ) as b
	on a.LoanID = b.LoanID
	)

	/*---- #S0b_LIDBase*/
	create table #S0b_LIDBase with (distribution = hash(LoanID), clustered columnstore index ) as 
	select distinct *
	from
	(
	select LoanID, LoanReference, create_date, 'Gazelle-Loan' as TblRef 
		from ARC_LOANQ_2028.dbo.LoanquotationAccount 
		where isnull(Capital,0)>0 /*--Gazelle loan*/
	union all
	select LoanID, AccountNumber as LoanReference, create_date, 'Gazelle-Card' as TblRef 
		from ARC_LOANQ_2028.dbo.LOANQUOTATIONCARDDETAILS /*--Gazelle card*/
	union all
	select LoanID, LoanReference, create_date, 'OMNI' as TblRef
		from #TmpCrDte
	) X

	/*-- we created an extra table for omni card;*/
	create table #S0b_LIDBase_v2 with (distribution = hash(LoanID), clustered columnstore index ) as 
	select distinct *
	from
	(
	select LoanID, LoanReference,create_date, 'Gazelle-Loan' as TblRef 
		from ARC_LOANQ_2028.dbo.LoanquotationAccount 
		where isnull(Capital,0)>0 /*--Gazelle loan*/
	union all
	select LoanID, AccountNumber as LoanReference,create_date, 'Gazelle-Card' as TblRef 
		from ARC_LOANQ_2028.dbo.LOANQUOTATIONCARDDETAILS /*--Gazelle card*/
	union all
	select ApplicationID as loanid, ltrim(rtrim(AccountReference)) as LoanReference,TimeStamp as create_date ,'OMNI-Card' as TblRef
		from prd_exactusSync.dbo.CA22100P (nolock)
	) X

	/*---- Step1: Get LoanReferences*/
	create table #S1_LRefBase with (distribution = hash(LoanID), clustered columnstore index ) as 
	select loanid, Max(LoanReference) as LoanReference, TblRef from
	(
	select ApplicationID as loanid, ltrim(rtrim(AccountReference)) as LoanReference, 'Card' as TblRef, 'Omni Card' as TblRef0 
		from prd_exactusSync.dbo.CA22100P (nolock)
	union all
	select loanid, ltrim(rtrim(LoanReference)) as LoanReference, 'Loan' as TblRef, 'Omni+Gazelle Loan' as TblRef0 
		from prd_exactusSync.dbo.ZA31200P (nolock) 
	union all
	select loanid, ltrim(rtrim(LoanReference)) as LoanReference, 'Loan' as TblRef, 'Gazelle Loan' as TblRef0 
		from ARC_LOANQ_2028.dbo.LoanQuotationAccount (nolock) 
	union all
	select loanid, AccountNumber as LoanReference, 'Card' as TblRef, 'Gazelle Card' as TblRef0 
		from ARC_LOANQ_2028.dbo.LOANQUOTATIONCARDDETAILS
	) X group by loanid, TblRef

	/*---- Step2: Get Subgroup & Disbursal Data*/
	create table #S2_LRefsubGrpDisbBase with (distribution = hash(LoanReference), clustered columnstore index ) as 
	(
	select LoanReference, EMPLOYERSUBGROUPCODE as Subgroup, CompanyCode, Capital as Capital_OR_Limit, startdate, ProductCode, Term, EvenInstalment as Instalment
		from prd_exactusSync.dbo.ZA31200P
	union all
	select AccountReference as LoanReference, Subgroup, Company as CompanyCode, AccountLimit as Capital_OR_Limit, startdate, ProductCode, NULL as Term,
	case
		when SubGroup='AFRICAN BANK ST' then 0.045*AccountLimit
		when AccountLimit<=5000 then 0.125*AccountLimit
		when AccountLimit<=7500 then 0.10*AccountLimit
		when AccountLimit<=10000 then 0.08*AccountLimit
		when AccountLimit<=15000 then 0.06*AccountLimit
		else 0.05*AccountLimit 
	end as Instalment
	from prd_exactusSync.dbo.CA20000P
	)

	create table #S2a_LIDsubGrpOld with (distribution = hash(LoanID), clustered columnstore index ) as 
	select loanid, SubGroupCode from ARC_LOANQ_2028..LoanQuotation

	/* This is to get Card Limits (across both gazelle and omni) */
	create table #S2b0_LIDLrefCapOrLmt with (distribution = hash(LoanID), clustered columnstore index ) as 
	select a.LoanID, a.LoanReference, a.Create_date, a.TblRef, b.NewLimit, b.AutoManualIndicator, 
	convert(datetime, cast(b.LimitIncreaseDate as varchar)) as LimitDate,b.LimitIncreaseDate,
	case
		when b.AccountReference is not null then 1 
		else 0 
	end as LIMIT_INCREASE
		from (select * from #S0b_LIDBase_v2 where TblRef like '%card%') as a
	left join (select * from prd_exactusSync.dbo.CA26100P where isnull(AutoManualIndicator,'') not in ('A') and isnull(NewLimit,0)>0) as b
	on a.LoanReference = b.AccountReference
	and abs(datediff(day, a.create_date, convert(datetime, cast(b.LimitIncreaseDate as varchar)))) < 3

	create table #S2b_LIDLrefCapOrLmt with (distribution = hash(LoanID), clustered columnstore index ) as 
	select *, 
	row_number() over( partition by LoanID, LoanReference order by LimitDate desc) as RN
	from #S2b0_LIDLrefCapOrLmt
	drop table #S2b0_LIDLrefCapOrLmt
	delete from #S2b_LIDLrefCapOrLmt where RN > 1

	/*---- Step3: Get IDNumber and ScoreModel*/
	create table #S3_LIDIDNumBase with (distribution = hash(LoanID), clustered columnstore index ) as 
	(
	select coalesce(a.loanid, b.loanid) as loanid, ltrim(rtrim(coalesce(a.IDNumber, b.IDNumber))) as IDNumber, b.ScoreModel
		from (select convert(numeric, replace(tranappnumber, 'E', '')) as LoanID, NationalID as IDNumber 
		from prd_press.capri.capri_loan_application (nolock)
		where isnull(TRANSEQUENCE,'') <> '005' and isnumeric(tranappnumber) = 1
		group by tranappnumber, NationalID
	) as a
	full join ARC_LOANQ_2028..LoanQuotation as b
	on a.loanid = b.loanid
	)

	/*-- delete few duplicates on loanid that are in the table*/
	delete from #S3_LIDIDNumBase where loanid in (select loanid from #S3_LIDIDNumBase group by loanid having count(*) > 1)

	/*---- Step4: Get WageType*/
	create table #S4_LIDWageType with (distribution = hash(loanid), clustered columnstore index ) as 
	select distinct * from
	(
	select loanid, WageType, row_number() over( partition by loanid order by create_date desc) as RN 
		from ARC_LOANQ_2028..LOANQUOTATIONEMPLOYMENTDETAILS (nolock) 
		where ltrim(rtrim(isnull(WageType,''))) not in ('') /*--Gazelle (Card+loan)*/
	union all
	select ApplicationID as loanid, WageType, 1 as RN 
		from prd_exactusSync.dbo.APPLICATIONEMPLOYMENT (nolock) 
		where ltrim(rtrim(isnull(WageType,''))) not in ('') /*--omni */
	) X

	delete from #S4_LIDWageType where RN > 1

	/*---- Step5: Get RepaymentMethod*/
	create table #S5_LRefRepay with (distribution = hash(LoanReference), clustered columnstore index ) as 
	(
	select LoanReference, RepaymentMethod 
		from prd_exactusSync.dbo.ZA31200P (nolock)  /*--Loan (omni + Gazelle)*/
	union all
	select AccountReference as LoanReference, RepaymentMethod 
		from prd_Exactussync.dbo.CA20500P (nolock) /*--Card (omni + Gazelle) */
	)

	/*---- Step6: Get OfferGroup*/
	create table #S6a_temp with (distribution = hash(ApplicationID), clustered columnstore index ) as 
	select ApplicationID, scoreband, OfferID, LastUpdateTimeStamp,
	case 
		when PRODUCTCLASSIFICATION like 'Staff%' then 'Staff' 
		else 'Non Staff' 
	end as Staff_Ind
	from prd_exactusSync..ApplicationOffers where offersselectedtype = 'SEL'
	union all
	select ApplicationID, scoreband, OfferID, LastUpdateTimeStamp,
	case 
		when PRODUCTCLASSIFICATION like 'Staff%' then 'Staff' 
		else 'Non Staff'
	end as Staff_Ind
	from prd_exactusSync..ApplicationOffersHistory where offersselectedtype = 'SEL';

	create table #S6_LIDScband with (distribution = hash(LoanID), clustered columnstore index ) as 
	select distinct LoanID, Scoreband, Staff_Ind
	from 
	(select aa1.ApplicationID as LoanID, aa1.Scoreband, aa1.Staff_Ind
		from #S6a_temp as aa1
		inner join (
		select ApplicationID, max(LastUpdateTimeStamp) as MID 
		from #S6a_temp
		group by ApplicationID
		) as aa2
		on aa1.ApplicationID = aa2.ApplicationID and aa1.LastUpdateTimeStamp=aa2.MID 
		union all
		select loanid as ApplicationID, convert(numeric, nullif(ltrim(rtrim(EXPERIANSCOREBAND1)),'')) as Scoreband, '' as Staff_Ind
		from ARC_LOANQ_2028.dbo.loanquotation
		where convert(numeric, nullif(ltrim(rtrim(EXPERIANSCOREBAND1)),'')) is not NULL
	) X

	/*---- Step7: Get Card Instalment (prior to 2015) */
	create table #S7_LIDOrgInstal with (distribution = hash(LoanID), clustered columnstore index ) as 
	(
	select LoanID, InstalmentLimit as Instalment 
	from ARC_LOANQ_2028.dbo.LoanQuotationCardDetails
	)

	Create table #Jn_Step1  
	with (distribution = hash(LoanID), clustered columnstore index ) as
	select distinct 
	  a.LoanID
	, a.Status
	, b0.create_date
	, b.LoanReference
	, a.TblRef+'-'+b.TblRef as TblRef
	, b.TblRef as Product
	from #S0_LIDBase as a
	left join #S1_LRefBase as b
	on a.LoanID = b.LoanID
	left join #S0b_LIDBase as b0
	on b.LoanReference = b0.Loanreference and b.loanid = b0.loanid

	Create table #Jn_Step2 
	with (distribution = hash(LoanReference), clustered columnstore index ) as
	select distinct 
	  a.LoanID
	, cast(a.Status as char(3)) as Status
	, a.create_date
	, a.LoanReference
	, a.TblRef
	, a.Product
	, 
	case 
		when a.create_date >= '2015-01-01' then c.SubGroup 
		else c0.subgroupcode 
	end as SubGroupCode  /*--ensure older history matches disbursement_info*/
	, 
	case 
		when a.Product ='Card' then isnull(cc.NewLimit,c.Capital_OR_Limit) 
		else c.Capital_OR_Limit
	end as Capital_OR_Limit
	, 
	case 
		when cc.LIMIT_INCREASE = 1 then cc.LimitIncreaseDate 
		else c.startdate
	end as DisbStartDate
	, cast(d.IDNumber as varchar(15)) as IDNumber
	, d.ScoreModel
	, cast(e.ScoreBand as char(4)) as OfferGroup
	, e.Staff_Ind
	, c.Instalment as cInstalment
	, 
	case 
		when a.Product = 'Loan' then c.Instalment
		else
		   case 
				when (case when a.create_date>='2015-01-01' then c.SubGroup else c0.subgroupcode end)='AFRICAN BANK ST' then 0.045*cc.NewLimit
				when cc.NewLimit<=5000 then 0.125*cc.NewLimit
				when cc.NewLimit<=7500 then 0.10*cc.NewLimit
				when cc.NewLimit<=10000 then 0.08*cc.NewLimit
				when cc.NewLimit<=15000 then 0.06*cc.NewLimit
				else 0.05*cc.NewLimit 
		   end 
	end as c1Instalment
	, cc.NewLimit
	, cc.LIMIT_INCREASE
	from #Jn_Step1 as a
	left join #S2_LRefsubGrpDisbBase as c
	on a.LoanReference = c.LoanReference
	left join #S2b_LIDLrefCapOrLmt as cc
	on a.LoanID = cc.LoanID and a.LoanReference = cc.Loanreference
	left join #S2a_LIDsubGrpOld as c0
	on a.LoanID = c0.LoanID
	left join #S3_LIDIDNumBase as d
	on a.loanid = d.loanid
	left join #S6_LIDScband (nolock) as e
	on a.loanid = e.LoanID

	drop table #Jn_Step1

	Create table #Jn_Step3
	with (distribution = hash(LoanReference), clustered columnstore index ) as
	select distinct 
	  a.LoanID
	, a.Status
	, a.create_date
	, a.LoanReference
	, a.TblRef
	, a.Product
	, a.SubGroupCode
	, a.Capital_OR_Limit
	, a.DisbStartDate
	, a.IDNumber
	, a.ScoreModel
	, a.OfferGroup
	, a.Staff_Ind
	, a.LIMIT_INCREASE
	, isnull(a.c1Instalment, a.cInstalment) as c1Instalment
	, f.CompanyCode
	, f.ProductCode
	, f.Term
	, h.ACCOUNTLIMIT
	, g.principaldebt
	, case when f.CapitalDisbursed =0 or f.CapitalDisbursed is null then coalesce(g.capital, a.Capital_OR_Limit) else f.CapitalDisbursed end as CapitalDisbursed
    from #Jn_Step2 as a
    left join edwdw.dbo.tbSDSales (nolock) as f
    on a.LoanReference = f.LoanReference
    left join PRD_ExactusSync.dbo.ZA31200P (nolock) as g
    on a.LoanReference = g.LoanReference
    left join PRD_ExactusSync.dbo.CA20000P(nolock) as h
	on a.LoanReference = h.AccountReference
	drop table #Jn_Step2

	/*---- Write intermediary table to permanent table - all joins done, still some processing required*/
	IF OBJECT_ID('Prd_DataDistillery.dbo.Disbursement_Info0', 'U') IS NOT NULL 
	DROP TABLE Prd_DataDistillery.dbo.Disbursement_Info0;
	Create table Prd_DataDistillery.dbo.Disbursement_Info0
	with (distribution = hash(LoanID), clustered columnstore index ) as
	select distinct 
	  a.LoanID
	, a.Status
	, a.create_date
	, a.LoanReference
	, a.SubGroupCode
	, a.Capital_OR_Limit
	, a.CompanyCode
	, a.DisbStartDate
	, a.ProductCode
	, 
	case 
		when a.Product = 'Card' then 12
		else a.Term 
	end as Term
	, 
	case 
		when a.LIMIT_INCREASE is null then 0 
		else a.LIMIT_INCREASE 
	end as LIMIT_INCREASE
	, 
	case 
		when a.Product='Loan' then a.c1Instalment
		when a.Product='Card' and a.create_date>='2015-12-01' then a.c1Instalment /*-- history was a couple of rands off in some cases, keep history as was in base table*/
		else j.Instalment
	end as Instalment
	, a.IDNumber
	, a.ScoreModel
	, a.OfferGroup
	, a.Staff_Ind
	, a.CapitalDisbursed
	, g.[1stduedate] as FirstDueDate
	, h.WageType
	, i.RepaymentMethod
	, a.TblRef
	, a.Product
	, a.principaldebt
	, a.accountlimit
	from #Jn_Step3 as a
	left join EDWDW..Abildata as g
	on a.LoanReference = g.LoanRef
	left join #S4_LIDWageType as h
	on a.loanid = h.loanid
	left join #S5_LRefRepay as i
	on a.LoanReference = i.LoanReference
	left join #S7_LIDOrgInstal as j
	on a.loanid = j.loanid
) by odbc;
quit;


proc sql; connect to odbc (dsn=MPWAPS);
execute (
	/*-- Delete cases where CapitalDisbursed is null - many of the other fields also null in this case*/
	delete from Prd_DataDistillery.dbo.Disbursement_Info0 where CapitalDisbursed is null
	/*-- Delete Gazelle loan applications with status='DIS' but isnull(Capital,0)=0 (as with current process)*/
	delete from Prd_DataDistillery.dbo.Disbursement_Info0 where create_date is null and TblRef like '%Gazelle%'
	/*-- Delete Gazelle cases with blank or null WageType (as with current process)*/
	delete from Prd_DataDistillery.dbo.Disbursement_Info0 where isnull(WageType,'')='' and TblRef like '%Gazelle%'
	/*-- Delete OMNI card applications with status='DIS' but disbursal date more than 2 months before create_date */
	delete from Prd_DataDistillery.dbo.Disbursement_Info0 where TblRef like '%OMNI%' and TblRef like '%Card%' and datediff(month, create_date, convert(datetime,cast(nullif(DisbStartDate,0) as varchar))) < -2 
	/*-- Delete OMNI loan applications with status='DIS' but isnull(Capital,0)=0 (as with current process)*/
	delete from Prd_DataDistillery.dbo.Disbursement_Info0 where TblRef like '%OMNI%' and TblRef like '%Loan%' and isnull(CapitalDisbursed,0)=0

	Create table #ProductType  
	with (distribution = hash(LoanReference), clustered columnstore index ) as
	select a.LoanReference,
	case    
		when lower(b.ProductType) like '%capri campaign%' or lower(b.ProductType) like '%capri debit order%' then 'DO Furn'
		when lower(b.ProductType) like '%capri cash%' or lower(b.ProductType) like '%capri%affidavit%' then 'CSH Furn'
		else b.ProductType
	end as EHL_ProductType
	from PRD_DataDistillery.dbo.Disbursement_Info0 a
	join PRD_DataDistillery_Data.dbo.JS_EHL_ProductTypes b
	on a.ProductCode = b.ProductCode
	where a.CompanyCode = '005'
	and a.LoanReference is not null
	and a.[Status] = 'DIS';

	Create table #tempLI 
	with (distribution = hash(LoanReference), clustered columnstore index ) as
	select 
	a.LoanID
	, a.LoanReference
	, a.Create_Date
	, a.Capital_OR_Limit
	, a.Instalment
	, a.accountlimit
	, a.principaldebt
	,
	case
		when row_number() over (partition by LoanReference order by Create_Date) = 1 then 0
		else 1
	end as CC_Limit_Inc,
	row_number() over (partition by LoanReference order by Create_Date) as Disb_Number
	from Prd_DataDistillery.dbo.Disbursement_Info0 a
	where a.TblRef like '%Card'; /*--a.Product = 'Card';*/

	Create table #LimitIncrease 
	with (distribution = hash(LoanReference), clustered columnstore index ) as
	select a.*, 
	case
		when b.Capital_OR_Limit is null then null
		else a.Capital_OR_Limit - b.Capital_OR_Limit
	end as Incremental_Limit_Increase
	from #tempLI a
	left join #tempLI b
	on a.LoanReference = b.LoanReference
	and a.Disb_Number = b.Disb_Number + 1;

	Create table #IncrLimitIncr
	with (distribution = hash(LoanID), clustered columnstore index ) as
	select a.*
	,isnull(c.Disb_Number,1) as Disb_Number
	,isnull(c.Incremental_Limit_Increase,0) as Incremental_Limit_Increase
	,isnull(c.CC_Limit_Inc,0) as CC_Limit_Inc
	from Prd_DataDistillery.dbo.Disbursement_Info0 as a
	left join #LimitIncrease as c
	on a.LoanReference = c.LoanReference and a.LoanID = c.LoanID;

	Create table #LastDis
	with (distribution = hash(LoanReference), clustered columnstore index ) as
	select LoanReference, max(Disb_Number) as Last_Disb_Number
	from #IncrLimitIncr
	group by LoanReference;

	Create table #Disb_Info
	with (distribution = hash(LoanReference), clustered columnstore index  ) as
	select a.*,
	case
		when b.LoanReference is null then 0
		else 1
	end as Last_Dis
	from #IncrLimitIncr a
	left join #LastDis b
	on a.LoanReference = b.LoanReference
	and a.Disb_Number = b.Last_Disb_Number;

	IF OBJECT_ID('Prd_DataDistillery.dbo.Disbursement_Info', 'U') IS NOT NULL 
	DROP TABLE Prd_DataDistillery.dbo.Disbursement_Info;
	Create table Prd_DataDistillery.dbo.Disbursement_Info 
	with (distribution = hash(loanid), clustered columnstore index ) as
	select distinct 
	  a.IDNumber
	, a.LoanID
	, a.LoanReference
	, a.Create_Date
	, a.CompanyCode
	, a.Status
	, a.DisbStartDate
	, a.CapitalDisbursed
	, a.Term
	, a.ProductCode
	, a.Product
	, a.SubGroupCode
	, a.WageType
	, a.OfferGroup
	, a.ScoreModel
	, a.FirstDueDate
	, c.EHL_ProductType
	, a.CC_Limit_Inc
	, a.LIMIT_INCREASE
	, a.Capital_OR_Limit
	, a.Instalment
	, a.Disb_Number
	, a.Incremental_Limit_Increase
	, a.Last_Dis
	, a.RepaymentMethod
	, a.TblRef
	, a.accountlimit
	, a.Staff_Ind
	, a.principaldebt
	, convert(varchar(10), getdate(), 23) as RunDate
	from #Disb_Info a
	left join #ProductType as c
	on a.loanreference = c.loanreference
	where a.Capital_OR_Limit > 0;

	truncate table #S0_LIDBase; drop table #S0_LIDBase;
	truncate table #S0b_LIDBase; drop table #S0b_LIDBase;
	truncate table #S1_LRefBase; drop table #S1_LRefBase;
	truncate table #S2_LRefsubGrpDisbBase; drop table #S2_LRefsubGrpDisbBase;
	truncate table #S2b_LIDLrefCapOrLmt; drop table #S2b_LIDLrefCapOrLmt;
	truncate table #S3_LIDIDNumBase; drop table #S3_LIDIDNumBase;
	truncate table #S4_LIDWageType; drop table #S4_LIDWageType;
	truncate table #S5_LRefRepay; drop table #S5_LRefRepay;
	truncate table #S6_LIDScband; drop table #S6_LIDScband;
) by odbc;
quit;

proc sql; connect to odbc (dsn=MPWAPS);
execute (
	---- Clean up ----
	IF OBJECT_ID('prd_DataDistillery_data.dbo.Disbursement_Info', 'U') IS NOT NULL 
	DROP TABLE prd_DataDistillery_data.dbo.Disbursement_Info;
	Create table prd_DataDistillery_data.dbo.Disbursement_Info
	with (distribution = hash(loanid), clustered columnstore index ) as
	select * from prd_DataDistillery.dbo.Disbursement_Info;
) by odbc;
quit;




