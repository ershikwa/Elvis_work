%include "\\neptune\credit$\AA_GROUP CREDIT\Scoring\SAS Macros\upload_aps_A.sas";
data _null_;
     call symput('tday_',put(intnx('day',today(),0),yymmddn8.));
run;

%put &tday_;

Proc Sql;
     Connect to ODBC (dsn = ABC_Col_Dialer);
     Create Table Coll_BNM_Deleted_Numbers_All as
           Select * from connection to odbc 
                (
                Select IDnumber,loanrefno,ContactNumber,DeleteDateTime
                from ABC_Dialer.dbo.AB_Col_Delete_History
                /*where convert(date,convert(varchar(50), DeleteDateTime))  >= dateadd(day,-60,cast(getdate() as date))*/
                Union
                Select IDnumber,loanrefno,ContactNumber,DeleteDateTime
                from ABC_Dialer.dbo.AB_Col_Delete_History_Archive
                /*where convert(date,convert(varchar(50), DeleteDateTime))  >= dateadd(day,-60,cast(getdate() as date))*/
                
                );
     disconnect from odbc;
Quit;

%Upload_APS_A(Set = Coll_BNM_Deleted_Numbers_All, Server = WORK, APS_ODBC = dev_cont, APS_DB = dev_contactinfo,Distribute = HASH(IDnumber));

/*Catch all all Xi107 numbers*/
proc sql stimer feedback;
     connect to ODBC (dsn = mpwetlp1);
     create table MD_XI10700P_All_Col as
     select * from connection to odbc (
Select distinct clientnumber,replace(concat(areacode,telephonenumber), ' ', '') as number 
from PRD_Restricted_Telnos.dbo.XI10700P  where telephonenumber != '' /*and 
lastupdatetimestamp >= '20221201'*/
);
disconnect from odbc;
quit;
%Upload_APS_A(Set = MD_XI10700P_All_Col, Server = WORK, APS_ODBC = dev_cont, APS_DB = dev_contactinfo,Distribute = HASH(clientnumber));


/*Create base*/
proc sql stimer feedback;
connect to ODBC as APS (dsn=mpwaps);
execute (
CREATE TABLE #bnmbase
WITH
(
DISTRIBUTION = HASH(clientnumber),
CLUSTERED COLUMNSTORE INDEX
)
AS
select distinct clientnumber,loanreference
from prd_collections_strategy.dbo.GCOL_MONTHLY_NEWCOL_UAT
where runmonth = (select max(runmonth) as runmonth
 from prd_collections_strategy.dbo.GCOL_MONTHLY_NEWCOL_UAT)


CREATE TABLE #bnmbase_SAID
WITH
(
DISTRIBUTION = HASH(clientnumber),
CLUSTERED COLUMNSTORE INDEX
)
AS
select b.idnumber, a.clientnumber
from #bnmbase a
left join (select distinct idnumber, clientnumber
from prd_collections_strategy.dbo.GCOL_MONTHLY_NEWCOL_UAT where idnumber not in ('','0') and len(idnumber) = 13) b
on a.clientnumber = b.clientnumber


CREATE TABLE #bnmbase_passports
WITH
(
DISTRIBUTION = HASH(passportno),
CLUSTERED COLUMNSTORE INDEX
)
AS
select passportno, max(datefrom) as max_date
from EDWDW.dbo.vwtbclientlatest
group by passportno

CREATE TABLE #bnmbase_passports2
WITH
(
DISTRIBUTION = HASH(passportno),
CLUSTERED COLUMNSTORE INDEX
)
AS
select b.identificationnumber, a.passportno, clientnumber, datefrom
from #bnmbase_passports a
inner join EDWDW.dbo.vwtbclientlatest b
on a.passportno = b.passportno and a.max_date = b.datefrom
where a.passportno !='' and identificationnumber =''

/*add passports to clientnumbers with null idnumbers*/
CREATE TABLE #base
WITH
(
DISTRIBUTION = HASH(idnumber),
CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT case when a.idnumber is not null then a.idnumber else passportno end as idnumber, a.clientnumber, identificationnumber, passportno, datefrom
from #bnmbase_SAID a
left join #bnmbase_passports2 b
on a.clientnumber = b.clientnumber

CREATE TABLE #bnmbase_missing_ids
WITH
(
DISTRIBUTION = HASH(identificationnumber),
CLUSTERED COLUMNSTORE INDEX
)
AS
select identificationnumber, max(datefrom) as max_date
from EDWDW.dbo.vwtbclientlatest
group by identificationnumber


CREATE TABLE #bnmbase_missing_ids2
WITH
(
DISTRIBUTION = HASH(identificationnumber),
CLUSTERED COLUMNSTORE INDEX
)
AS
select a.identificationnumber,b.clientnumber,b.passportno, datefrom
from #bnmbase_missing_ids a
inner join EDWDW.dbo.vwtbclientlatest b
on a.identificationnumber = b.identificationnumber and a.max_date = b.datefrom
where a.identificationnumber !=''
and clientnumber in (select clientnumber from #base where idnumber is null)


CREATE TABLE #base2
WITH
(
DISTRIBUTION = HASH(idnumber),
CLUSTERED COLUMNSTORE INDEX
)
AS
select a.idnumber, a.clientnumber, b.identificationnumber, b.passportno, b.datefrom
from #base a
left join #bnmbase_missing_ids2 b
on a.clientnumber = b.clientnumber

CREATE TABLE #base3
WITH
(
DISTRIBUTION = HASH(idnumber),
CLUSTERED COLUMNSTORE INDEX
)
AS
select case when idnumber is null and identificationnumber is not null then identificationnumber else idnumber end as idnumber,
 clientnumber, identificationnumber, passportno, datefrom
from #base2

CREATE TABLE #dupids
WITH
(
DISTRIBUTION = HASH(idnumber),
CLUSTERED COLUMNSTORE INDEX
)
AS
select idnumber, count(distinct clientnumber) cn
from #base3
group by idnumber
having count(distinct clientnumber) > 1

CREATE TABLE #base4
WITH
(
DISTRIBUTION = HASH(idnumber),
CLUSTERED COLUMNSTORE INDEX
)
AS
select distinct case when b.cn is null then a.idnumber else null end as idnumber,clientnumber
from #base3 a
left join #dupids b
on a.idnumber = b.idnumber

CREATE TABLE #bnmbase1
WITH
(
DISTRIBUTION = HASH(idnumber),
CLUSTERED COLUMNSTORE INDEX
)
AS
SELECT a.idnumber, a.clientnumber, b.number
from #base4 a
left join dev_contactinfo.dbo.bnm_fin_nums b
on a.idnumber = b.idnumber

/**********************************Xi*******************************************************/

create table dev_datadistillery_optimisation.dbo.#clean1 with(clustered columnstore index, distribution=hash(clientnumber)) as
select  clientnumber, replace(replace(lower(number), ' ', '') , '+', '') as number
from dev_contactinfo.dbo.MD_XI10700P_All_Col where clientnumber in (select clientnumber from #bnmbase1)

create table dev_datadistillery_optimisation.dbo.#clean2 with(clustered columnstore index, distribution=hash(clientnumber)) as
select distinct clientnumber,
case when len(number) = 11 and substring(number,1,2) = '27' 
then concat('0', substring(number,3,9)) else 
number end as number
from dev_datadistillery_optimisation.dbo.#clean1
        
        
create table dev_datadistillery_optimisation.dbo.#MD_XI10700P_Coll with(clustered columnstore index,distribution=hash(clientnumber)) as
select  a.clientnumber,a.number
from (select distinct clientnumber,number 
from dev_datadistillery_optimisation.dbo.#clean2
where len(number) = 10 and number like '0%' and substring(number,1,4) < '0862'
and number not like '00%' and number not like '09%' 
and number not like '01000000%' and number not like '9999%'
and number not like '080%' and number not like '01100000%') a
		

create table dev_datadistillery_optimisation.dbo.#md_xi_nums1 with (clustered columnstore index, distribution=hash(idnumber)) as
	          select a.idnumber, a.clientnumber, b.number  from
             (select distinct idnumber, clientnumber from #bnmbase1) a
          inner join (select clientnumber,number
             from dev_datadistillery_optimisation.dbo.#MD_XI10700P_Coll)b
             on a.clientnumber = b.clientnumber
             
create table dev_datadistillery_optimisation.dbo.#md_xi_nums with (clustered columnstore index, distribution=hash(idnumber)) as
	         select a.idnumber, a.clientnumber, a.number
	         from #md_xi_nums1 a
             left join dev_contactinfo.dbo.Coll_BNM_Deleted_Numbers_All b
on a.idnumber = b.idnumber and a.number = b.contactnumber
where b.contactnumber is null

    CREATE TABLE dev_contactinfo.dbo.#XI_Numbers_Check WITH(clustered columnstore index, distribution=hash(import_date)) as
    
    select cast(getdate() as date) as import_date, count(*) new_numbers
    from (
    select idnumber, number from #md_xi_nums
    except
    select idnumber, number from dev_contactinfo.dbo.numbers_rank_score_&tday_
    )a
    union
    select import_date, new_numbers
    from dev_contactinfo.dbo.XI_Numbers_Check

	    IF OBJECT_ID('dev_contactinfo.dbo.XI_Numbers_Check') IS NOT NULL
    DROP TABLE dev_contactinfo.dbo.XI_Numbers_Check
    CREATE TABLE dev_contactinfo.dbo.XI_Numbers_Check WITH(clustered columnstore index, distribution=hash(import_date)) as
    select import_date, new_numbers
    from #XI_Numbers_Check

/*****************************************Comp*****************************************************/

create table dev_contactinfo.dbo.#MD_BNM_Comp_AB with(distribution=hash(idnumber)) as
select idnumber,clientnumber,max(info_date) as info_date
from (select distinct idnumber,clientnumber 
from #bnmbase1 where 
clientnumber is not null) a
inner join PRD_BUR.DBO.COMP_TELEPHONE b
on a.idnumber = b.id_no
group by idnumber,clientnumber

create table dev_contactinfo.dbo.#MD_BNM_Comp_AB1 with(distribution=hash(idnumber)) as
select distinct a.idnumber,clientnumber, b.tel_no as number,tel_qlty,rec_dttm
from #MD_BNM_Comp_AB a
inner join PRD_BUR.DBO.COMP_TELEPHONE b
on a.idnumber = b.id_no  and a.info_date = b.info_date

create table dev_contactinfo.dbo.#clean1_comp with(clustered 
	columnstore index, distribution=hash(idnumber)) as select distinct idnumber, 
	clientnumber, replace(replace(lower(number), ' ', '') , '+', '') as number 
	from dev_contactinfo.dbo.#MD_BNM_Comp_AB1

create table dev_contactinfo.dbo.#clean2_comp with(clustered columnstore index, distribution=hash(idnumber)) as
select distinct idnumber,clientnumber,case when len(number) = 11 and substring(number,1,2) = '27' 
        then concat('0', substring(number,3,9)) else 
        number end as number
from dev_contactinfo.dbo.#clean1_comp

IF OBJECT_ID('dev_contactinfo.dbo.MD_Clean_Compuscan_Col ') IS NOT NULL
DROP TABLE dev_contactinfo.dbo.MD_Clean_Compuscan_Col
create table dev_contactinfo.dbo.MD_Clean_Compuscan_Col with(clustered columnstore index, distribution=hash(idnumber)) as
select distinct a.idnumber,a.clientnumber,a.number
from dev_contactinfo.dbo.#clean2_comp a
left join dev_contactinfo.dbo.Coll_BNM_Deleted_Numbers_All b
on a.idnumber = b.idnumber and a.number = b.contactnumber
where b.contactnumber is null and len(number) = 10 and number like '0%' and substring(number,1,4) < '0862'
		and number not like '00%' and number not like '09%' 
		and number not like '01000000%' and number not like '9999%'
		and number not like '080%' and number not like '01100000%'

    CREATE TABLE dev_contactinfo.dbo.#Compuscan_Numbers_Check WITH(clustered columnstore index, distribution=hash(import_date)) as
    
    select cast(getdate() as date) as import_date, count(*) new_numbers
    from (
    select idnumber, number from dev_contactinfo.dbo.MD_Clean_Compuscan_Col
    except
    select idnumber, number from dev_contactinfo.dbo.numbers_rank_score_&tday_
    )a
    union
    select import_date, new_numbers
    from dev_contactinfo.dbo.Compuscan_Numbers_Check 

	    IF OBJECT_ID('dev_contactinfo.dbo.Compuscan_Numbers_Check ') IS NOT NULL
    DROP TABLE dev_contactinfo.dbo.Compuscan_Numbers_Check 
    CREATE TABLE dev_contactinfo.dbo.Compuscan_Numbers_Check  WITH(clustered columnstore index, distribution=hash(import_date)) as
    select import_date, new_numbers
    from #Compuscan_Numbers_Check
/***********************************************************TU********************************************/

create table #tu1 with(distribution=hash(idnumber)) as
select a.idnumber,clientnumber, max(info_date) as max_info_date
from (select distinct idnumber, clientnumber from #bnmbase1 where 
clientnumber is not null) a
inner join prd_bur.bureau.chd_cons_contact b
on a.idnumber = b.idnumber
group by a.idnumber,clientnumber	
	
create table dev_contactinfo.dbo.#MD_BNM_TU_AB with (clustered columnstore index, distribution = hash(idnumber)) as
select a.idnumber,a.clientnumber,work_tel_1,
work_tel_2,
work_tel_3,
home_tel_1,
home_tel_2,
home_tel_3,
 cell_1_no,
 cell_2_no,
 cell_3_no,
 a.max_info_date as info_date
from #tu1 a

inner join (select idnumber,
CONCAT(work_tel_1_code,work_tel_1_no) as work_tel_1,
CONCAT(work_tel_2_code,work_tel_2_no) as work_tel_2,
CONCAT(work_tel_3_code,work_tel_3_no) as work_tel_3,
CONCAT(home_tel_1_code,home_tel_1_no) as home_tel_1,
CONCAT(home_tel_2_code,home_tel_2_no) as home_tel_2,
CONCAT(home_tel_3_code,home_tel_3_no) as home_tel_3,
 cell_1_no,
 cell_2_no,
 cell_3_no,
 info_date from prd_bur.bureau.chd_cons_contact
  where load_type = 'MONTHLY_AB') b
  on a.idnumber = b.idnumber and a.max_info_date = b.info_date

CREATE TABLE dev_contactinfo.dbo.#MD_BNM_TU_AB1 with (clustered columnstore index, distribution = hash(idnumber))
	AS

	SELECT idnumber, clientnumber,number
	FROM
	(
	SELECT idnumber, clientnumber,cast(work_tel_1 as varchar(50)) as work_tel_1, 
	cast(work_tel_2 as varchar(50)) work_tel_2, cast(work_tel_3 as varchar(50)) work_tel_3,
	cast(home_tel_1 as varchar(50)) home_tel_1, cast(home_tel_2 as varchar(50))home_tel_2, 
	cast(home_tel_3 as varchar(50)) home_tel_3,cast(cell_1_no as varchar(50)) as cell_1_no, 
	cast(cell_2_no as varchar(50)) cell_2_no, cast(cell_3_no as varchar(50)) cell_3_no
	FROM #MD_BNM_TU_AB
	)r

	UNPIVOT
	(
		
		number FOR numbers in (work_tel_1, work_tel_2, work_tel_3,home_tel_1, home_tel_2, home_tel_3,
		cell_1_no, cell_2_no, cell_3_no)
	)n



create table dev_contactinfo.dbo.#cleantu with(clustered columnstore index, distribution=hash(idnumber)) as
select distinct idnumber, clientnumber, replace(replace(lower(number), ' ', '') , '+', '') as number
from dev_contactinfo.dbo.#MD_BNM_TU_AB1

create table dev_contactinfo.dbo.#clean2tu with(clustered columnstore index, distribution=hash(idnumber)) as
select distinct idnumber, clientnumber,case when len(number) = 11 and substring(number,1,2) = '27' 
        then concat('0', substring(number,3,9)) else 
        number end as number
from dev_contactinfo.dbo.#cleantu

IF OBJECT_ID('dev_contactinfo.dbo.MD_Clean_TU ') IS NOT NULL
DROP TABLE dev_contactinfo.dbo.MD_Clean_TU 
create table dev_contactinfo.dbo.MD_Clean_TU with(clustered columnstore index, distribution=hash(idnumber)) as
select distinct a.idnumber, a.clientnumber,a.number
from dev_contactinfo.dbo.#clean2tu a
left join dev_contactinfo.dbo.Coll_BNM_Deleted_Numbers_All b
on a.idnumber = b.idnumber and a.number = b.contactnumber
where b.contactnumber is null 
and len(number) = 10 and number like '0%' and substring(number,1,4) < '0862'
		and number not like '00%' and number not like '09%' 
		and number not like '01000000%' and number not like '9999%'
		and number not like '080%' and number not like '01100000%'
		
    CREATE TABLE dev_contactinfo.dbo.#TU_Numbers_Check WITH(clustered columnstore index, distribution=hash(import_date)) as
    
    select cast(getdate() as date) as import_date, count(*) new_numbers
    from (
    select idnumber, number from dev_contactinfo.dbo.MD_Clean_TU 
    except
    select idnumber, number from dev_contactinfo.dbo.numbers_rank_score_&tday_
    )a
    union
    select import_date, new_numbers
    from dev_contactinfo.dbo.TU_Numbers_Check 

	    IF OBJECT_ID('dev_contactinfo.dbo.TU_Numbers_Check ') IS NOT NULL
    DROP TABLE dev_contactinfo.dbo.TU_Numbers_Check 
    CREATE TABLE dev_contactinfo.dbo.TU_Numbers_Check  WITH(clustered columnstore index, distribution=hash(import_date)) as
    select import_date, new_numbers
    from #TU_Numbers_Check		

/********************************************************************************/
CREATE TABLE dev_contactinfo.dbo.#MD_XI_Bureau_EDC_Latest WITH(clustered columnstore index, distribution=hash(idnumber)) as
select idnumber, clientnumber,number
from #bnmbase1 where number is not null
union
select idnumber, clientnumber, number
from #md_xi_nums where number is not null
union
select idnumber, clientnumber, number
from dev_contactinfo.dbo.MD_Clean_Compuscan_Col where number is not null
union
select idnumber, clientnumber, number
from dev_contactinfo.dbo.MD_Clean_TU where number is not null



IF OBJECT_ID('dev_contactinfo.dbo.MD_XI_Bureau_Latest') IS NOT NULL
DROP TABLE dev_contactinfo.dbo.MD_XI_Bureau_Latest
CREATE TABLE dev_contactinfo.dbo.MD_XI_Bureau_Latest WITH(clustered columnstore index, distribution=hash(idnumber)) as
select idnumber, clientnumber, number
from #MD_XI_Bureau_EDC_Latest

IF OBJECT_ID('dev_contactinfo.dbo.bnm_fin_nums') IS NOT NULL
DROP TABLE dev_contactinfo.dbo.bnm_fin_nums
CREATE TABLE dev_contactinfo.dbo.bnm_fin_nums WITH(clustered columnstore index, distribution=hash(idnumber)) as
select idnumber, clientnumber, number
from dev_contactinfo.dbo.MD_XI_Bureau_Latest
) by APS;
quit;