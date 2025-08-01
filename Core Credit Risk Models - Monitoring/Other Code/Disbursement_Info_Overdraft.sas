options compress=yes;

proc sql; connect to odbc (dsn=MPWAPS);
select * from connection to odbc(
	IF OBJECT_ID('prd_DataDistillery_data.dbo.Disbursement_Info_Over_Backup', 'U') IS NOT NULL 
	DROP TABLE prd_DataDistillery_data.dbo.Disbursement_Info_Over_Backup;
	create table prd_DataDistillery_data.dbo.Disbursement_Info_Over_Backup 
	with (distribution = hash(loanid), clustered columnstore index ) as
	select *
	from Prd_DataDistillery_data.dbo.Disbursement_Info_Over
); disconnect from odbc;
quit;

proc sql; connect to odbc (dsn=MPWAPS);
execute (
	/*---- Step0: Get LoanIDs, AccountStatus and create_date*/
	create table #S0_LIDBase with (distribution = hash(LoanID), clustered columnstore index ) as 
	select distinct *
	from
	(
		select ApplicationID as LoanID, ClientNumber, ApplicationStatus as Status, creationdate as create_date, 'Omni' as TblRef
		from prd_ExactusSync.dbo.Applications 
		where ApplicationStatus = 'DIS' and ApplicationType in ('CRE','CBC')
	) X;

	/* 	---- Step1: Get LoanReferences */
	create table #S1_LRefBase with (distribution = hash(LoanID), clustered columnstore index ) as 
	select loanid, Max(LoanReference) as LoanReference, TblRef from
	(
		Select ApplicationID as Loanid, ltrim(rtrim(AccountReference)) as LoanReference, 'Overdraft' as TblRef, 'Omni Overdraft' as TblRef0 
		from prd_ExactusSync.dbo.ApplicationAccount (nolock) 
		where applicationid in (select applicationid 
								from prd_ExactusSync.dbo.OC00000P 
								where type in ('OVER','OVLI') )
	) X group by loanid, TblRef ;

	/* 	---- Step2a: Get IDNumber */
	create table #S2a_LIDIDNumBase with (distribution = hash(LoanID), clustered columnstore index ) as 
	(
		select loanid, ltrim(rtrim(IDNumber)) as IDNumber, uniqueid
		from (
				select convert(numeric, tranappnumber) as LoanID, NationalID as IDNumber,uniqueid
				from prd_press.capri.capri_loan_application (nolock)
			  	where isnull(TRANSEQUENCE,'') <> '005' and tranappnumber in (select cast(LoanID as varchar) from #S0_LIDBase)
				) X
	) ;
		
	/* 	---- Step2b: Get Subgroupcode */
	create table #S2b_UIDSBGBase with (distribution = hash(uniqueid), clustered columnstore index ) as 
	(
		select uniqueid, Subgroupcode
		from (
				select Uniqueid,EmployerSubgroupcode as Subgroupcode,ApplicationDate,ApplicationTime
				from prd_press.capri.capri_employment (nolock)
			  	where ApplicationDate >= '2021-05-01'
			    ) X
	) ;

	/* 	---- Step2c: Get RepaymentMethod */
	create table #S2c_UIDREPAY with (distribution = hash(uniqueid), clustered columnstore index ) as 
	(
		select uniqueid,RepaymentMethod
		from (
				select Uniqueid,RepaymentMethod
				from prd_press.capri.Capri_Account (nolock)
			  	where ApplicationDate >= '2021-05-01'
			    ) X
	) ;
		
	/* 	---- Step3: Get WageType */
	create table #S3_LIDWageType with (distribution = hash(loanid), clustered columnstore index ) as 
	select distinct * from
	(
		select ApplicationID as loanid, WageType 
		from prd_exactusSync.dbo.APPLICATIONEMPLOYMENT (nolock) 
		where ltrim(rtrim(isnull(WageType,''))) not in ('') 
	) X;

	/* 	---- Step4: Get Disbursal Information */
	create table #S4_LIDDISB with (distribution = hash(Loanid), clustered columnstore index ) as 
		select ApplicationID as Loanid, scoreband, ProductCategory as ProductCode, ProductClassification, Description, CompanyCode,
				ODOVERDRAFTLIMIT AS CapitalDisbursed, ODOverDraftRepayment as Instalment,
				ODOverDraftTerm as Term, convert(Varchar,LastUpdateTimeStamp,23) as DisbStartDate,
				case when Description like '%Limit%' then 1 else 0 end as CC_Limit_Increase,
				case when PRODUCTCLASSIFICATION like 'Staff%' then 'Staff' else 'Non Staff' end as Staff_Ind
		from prd_exactusSync.dbo.ApplicationOffers
		Where ProductClassification like '%Over%' and OffersSelectedType = 'SEL';

	/* 	---- Step5: Get ProductCode */
	create table #S5_CNPRCODE with (distribution = hash(ClientNumber), clustered columnstore index ) as 
		select ClientNumber, OverDraftProductCode as ProductCode
		from prd_exactusSync.dbo.OverDraftMaster;

	/* 	Join LoanIDs and Loanreference*/
	Create table #Jn_Step1  
	with (distribution = hash(LoanID), clustered columnstore index ) as
	select distinct a.LoanID,
					a.Clientnumber,
					a.Status,
					a.create_date,
					b.LoanReference,
					a.TblRef+'-'+b.TblRef as TblRef,
					b.TblRef as Product
	from #S0_LIDBase as a
	left join #S1_LRefBase as b
	on a.LoanID = b.LoanID
	where b.TblRef = 'Overdraft'

	Create table #Jn_Step1b
	with (distribution = hash(LoanID), clustered columnstore index ) as
	select a.LoanID,
			cast(b.IDNumber as varchar(15)) as IDNumber,
			c.Subgroupcode,
			d.RepaymentMethod,
			row_number() over(partition by a.Loanid order by b.Uniqueid desc) as RowNum
	from #Jn_Step1 as a
	left join (select LoanID, IDNumber, max(uniqueid) as uniqueid from  #S2a_LIDIDNumBase group by LoanID, IDNumber) as b 
	on a.LoanID = b.LoanID
	left join (select distinct uniqueid, Subgroupcode from #S2b_UIDSBGBase) as c
	on b.uniqueid = c.uniqueid
	left join (select distinct uniqueid, RepaymentMethod from #S2c_UIDREPAY) as d
	on c.uniqueid = d.uniqueid;

	delete from #Jn_Step1b where RowNum <> 1;

	Create table #Jn_Step2 
	with (distribution = hash(LoanID), clustered columnstore index ) as
	select distinct a.LoanID,
					cast(a.Status as char(3)) as Status,
					a.create_date,
					cast(a.LoanReference as varchar(11)) as LoanReference,
					a.TblRef,
					a.Product,
					b.IDNumber, 
					b.SubGroupCode,
					b.RepaymentMethod,
					c.instalment,
					c.CapitalDisbursed,	
					cast(c.ScoreBand as char(4)) as OfferGroup,
					cast(replace(c.Disbstartdate,'-','') as numeric) as Disbstartdate,
					cast(replace(c.Disbstartdate,'-','') as numeric) as FirstDueDate,
					c.Term,
					c.CompanyCode,
					c.Staff_Ind,
					c.CC_limit_increase,
					c.CapitalDisbursed as Capital_Or_Limit,
					d.WageType,
					e.ProductCode
	from #Jn_Step1 as a
	left join #Jn_Step1b as b
	on a.LoanID = b.loanid
	left join #S4_LIDDISB as c 
	on a.LoanID = c.LoanID
	left join #S3_LIDWageType as d 
	on a.LoanID = d.LoanID
	left join #S5_CNPRCODE as e
	on a.clientnumber = e.clientnumber;

	drop table #Jn_Step1;
	drop table #Jn_Step1b;

	IF OBJECT_ID('Prd_DataDistillery.dbo.Disbursement_Info_Over0', 'U') IS NOT NULL DROP TABLE Prd_DataDistillery.dbo.Disbursement_Info_Over0;
	Create table Prd_DataDistillery.dbo.Disbursement_Info_Over0
	with (distribution = hash(LoanID), clustered columnstore index ) as
		select distinct a.LoanID,
						a.IDNumber,
						a.LoanReference,
						a.Create_Date,
						a.CompanyCode,
						a.Status,
						a.DisbStartDate,
						a.CapitalDisbursed,
						a.Term,
						a.ProductCode,
						a.Staff_Ind,
						a.Product,
						a.SubGroupCode,
						a.WageType,
						a.OfferGroup,
						'' as ScoreModel,
						a.FirstDueDate,
						'' as EHL_ProductType,
						a.CC_limit_increase as CC_Limit_Inc,
						a.Capital_OR_Limit,
						a.Instalment,
						a.RepaymentMethod,
						a.TblRef,
						convert(varchar(10), getdate(), 23) as RunDate
	from #Jn_Step2 as a;

	delete from Prd_DataDistillery.dbo.Disbursement_Info_Over0 where CapitalDisbursed is null;

	Create table #tempLI 
	with (distribution = hash(LoanReference), clustered columnstore index ) as
	select a.LoanID,
			a.LoanReference,
			a.Create_Date,
			a.Capital_OR_Limit,
			a.Instalment,
			a.CC_Limit_Inc,
			row_number() over (partition by LoanReference order by Create_Date) as Disb_Number
	from Prd_DataDistillery.dbo.Disbursement_Info_Over0 a; 

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
	select a.*,
			isnull(c.Disb_Number,1) as Disb_Number,
			isnull(c.Incremental_Limit_Increase,0) as Incremental_Limit_Increase
	from Prd_DataDistillery.dbo.Disbursement_Info_Over0 as a
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
	on a.LoanReference = b.LoanReference and a.Disb_Number = b.Last_Disb_Number;

	/*---- Create the final table, maintaining ordering of variables in original table */
	IF OBJECT_ID('Prd_DataDistillery.dbo.Disbursement_Info_Over', 'U') IS NOT NULL DROP TABLE Prd_DataDistillery.dbo.Disbursement_Info_Over;
	Create table Prd_DataDistillery.dbo.Disbursement_Info_Over
	with (distribution = hash(loanid), clustered columnstore index ) as
	select distinct a.IDNumber,
					a.LoanID,
					a.LoanReference,
					a.Create_Date,
					a.CompanyCode,
					a.Status,
					a.DisbStartDate,
					a.CapitalDisbursed,
					a.Term,
					a.ProductCode,
					a.Product,
					a.Staff_Ind,
					a.SubGroupCode,
					a.WageType,
					a.OfferGroup,
					a.ScoreModel,
					a.FirstDueDate,
					Null as EHL_ProductType,
					a.CC_Limit_Inc,
					a.Capital_OR_Limit,
					a.Instalment,
					a.Disb_Number,
					a.Incremental_Limit_Increase,
					a.Last_Dis,
					a.RepaymentMethod,
					a.TblRef,
					convert(varchar(10), getdate(), 23) as RunDate
	from #Disb_Info a
	where a.Capital_OR_Limit > 0;

	truncate table #S0_LIDBase; drop table #S0_LIDBase;
	truncate table #S1_LRefBase; drop table #S1_LRefBase;
	truncate table #S2a_LIDIDNumBase; drop table #S2a_LIDIDNumBase;
	truncate table #S2b_UIDSBGBase; drop table #S2b_UIDSBGBase;
	truncate table #S2c_UIDREPAY; drop table #S2c_UIDREPAY;
	truncate table #S3_LIDWageType; drop table #S3_LIDWageType;
	truncate table #S4_LIDDISB; drop table #S4_LIDDISB;
	truncate table #S5_CNPRCODE; drop table #S5_CNPRCODE;

	IF OBJECT_ID('prd_DataDistillery_data.dbo.Disbursement_Info_Over', 'U') IS NOT NULL 
	DROP TABLE prd_DataDistillery_data.dbo.Disbursement_Info_Over;
	Create table prd_DataDistillery_data.dbo.Disbursement_Info_Over
	with (distribution = hash(loanid), clustered columnstore index ) as
	select * from prd_DataDistillery.dbo.Disbursement_Info_Over;
) by odbc;
quit;
