/* Change to demo commit, push and merge request */

%include "H:\Process_Automation\sas_autoexec\sas_autoexec.sas";

%let projectcode =H:\Process_Automation\Codes;

%let runmonth = %scan(&sysparm,1);
%let process_number = %scan(&sysparm,2);
%let process =  %scan(&sysparm,3);

%let project =pj;

libname &project "&process";

%start_program;

options compress=yes;

%let DisbTblRef= Prd_DataDistillery_data.[dbo].Disbursement_Info;
%Let OutcomeTableName= Prd_DataDistillery_data.[dbo].JS_OUTCOME_BASE_FINAL;
* Todays day for comparison report;
%Let Today= %sysfunc(today(),yymmddn8.); *&sysdate;
%PUT NOTE: &Today;

/************************PULL DEFAULT FLAG ******************************************* ;*/

/*--%SQLDEL_APS(Scoring.dbo.JS_disbursement1_info_duplicates);*/

proc sql noprint noerrorstop;
	connect to ODBC as APS (dsn=mpwaps);
	execute( 


/*-- drop table #disbursement1_info_duplicates*/
create table #disbursement1_info_duplicates
with (
          DISTRIBUTION = hash(loanreference),
          clustered columnstore index 
         )
as
SELECT C.* 
FROM &DisbTblRef  C 
inner join 
(SELECT distinct A.loanreference as Loanreference FROM 
(
SELECT loanreference , disb_number  
FROM  &DisbTblRef 
GROUP BY loanreference  ,disb_number
HAVING count(*) > 1 ) as A) as B 
ON C.loanreference = B.loanreference ;



/*-- drop table #Disbursement1_MissingFirstDueDate*/
create table #disbursement1_MissingFirstDueDate
with (
          DISTRIBUTION = hash(LOANID),
          clustered columnstore index 
         )
as
SELECT * 
FROM &DisbTblRef A
WHERE 
A.Loanreference not in (Select distinct loanreference from  #disbursement1_info_duplicates)
and len(cast(isnull(Firstduedate,0) as varchar)) = 1;

create table #JStemp2
with (
          DISTRIBUTION = hash(LOANID),
          clustered columnstore index 
         )
as
SELECT distinct LOANID ,
       LoanReference ,
	   Create_date,
	   CompanyCode,
	   DisbStartDate,
	   Term,
	   Product,
	   WageType,
	   case when len(OfferGroup)=1 then '0'+ OfferGroup else OfferGroup end as Offergroup,
	   FirstDueDate,
	   EHL_ProductType,
	   CC_Limit_Inc,
	   Capital_OR_Limit,
	   Disb_Number,
	   RunDate,
	          (case 
			  when Disb_Number > 1 and  day(cast(A.Create_Date as date)) < 9 then cast(cast(Create_date as date) as datetime) 
	          when Disb_Number > 1 and day(cast(A.Create_Date as date)) >= 9 then cast(DATEADD(month,1,cast(Create_date as date)) as datetime) 
			 else cast(cast(FirstDueDate as varchar(8)) as datetime) 
			end) as FirstdueMonth  
FROM &DisbTblRef A
WHERE len(cast(isnull(Firstduedate,0) as varchar)) > 1 and 
(A.Loanreference not in (Select  loanreference from  #disbursement1_info_duplicates UNION  
Select  loanreference from #Disbursement1_MissingFirstDueDate));

create table #JStemp3 
with (
          DISTRIBUTION = hash(Loanreference),
          clustered columnstore index 
         )
as

select distinct A.*, B.Product_Detail , B.ORG_TERM  
from #JStemp2 A  
left join 
Provisions.dbo.PAYMENTS_TABLE B 
on A.Loanreference = B.loanref and B.STMT_NR = 0 
where A.FirstDueMonth >=  '2010-01-01 00:00:00.000';


create table #Temp_JS_MAXSTASARBS  
with (
          DISTRIBUTION = hash(loanreference),
          clustered columnstore index 
         )
as
select distinct A.loanreference , A.FirstdueMonth , A.Disb_Number , 
       max(case when CURED_CD > 3 and STMT_NR <= 12 then 1 else 0 end) as CURED_4LE12,
	   max(WO_IND) as WO_IND
from #JStemp3 A   
inner join Provisions.dbo.PAYMENTS_TABLE  B 
on A.loanreference = B.loanref 
group by A.loanreference , A.FirstdueMonth , A.Disb_Number;


create table #Temp_CONCTRACTUALBAL_CD 
with (
          DISTRIBUTION = hash(loanreference),
          clustered columnstore index 
         )
as
select A.loanreference , A.FirstdueMonth , A.Disb_Number 

  ,max(case when B.CONTRACTUALCD > 0 and
        (DATEDIFF(MONTH, A.Firstduemonth , B.runmonth) + 1) between 0 and  1 
          then 1 else 0 end) as CONTRACTUAL_1_LE1

  ,max(case when B.CONTRACTUALCD > 1 and
         ( DATEDIFF(MONTH, A.Firstduemonth , B.runmonth) + 1)  between 0 and  2
          then 1 else 0 end) as CONTRACTUAL_2_LE2

  ,max(case when B.CONTRACTUALCD > 2 and
          (  DATEDIFF(MONTH, A.Firstduemonth , B.runmonth) + 1)  between 0 and  3
          then 1 else 0 end) as CONTRACTUAL_3_LE3


  ,max(case when B.CONTRACTUALCD > 1 and
          (  DATEDIFF(MONTH, A.Firstduemonth , B.runmonth) + 1)  between 0 and  3
          then 1 else 0 end) as CONTRACTUAL_2_LE3

  ,max(case when B.CONTRACTUALCD > 1 and
          (  DATEDIFF(MONTH, A.Firstduemonth , B.runmonth) + 1)  between 0 and  4
          then 1 else 0 end) as CONTRACTUAL_2_LE4

  ,max(case when B.CONTRACTUALCD > 1 and
         (DATEDIFF(MONTH, A.Firstduemonth , B.runmonth) + 1)  between 0 and  5
          then 1 else 0 end) as CONTRACTUAL_2_LE5
,max(case when B.CONTRACTUALCD > 1 and
         (DATEDIFF(MONTH, A.Firstduemonth , B.runmonth) + 1) between 0 and  6
          then 1 else 0 end) as CONTRACTUAL_2_LE6
  ,max(case when B.CONTRACTUALCD > 1 and
         (  DATEDIFF(MONTH, A.Firstduemonth , B.runmonth) + 1)  between 0 and  7
          then 1 else 0 end) as CONTRACTUAL_2_LE7

 ,max(case when B.CONTRACTUALCD > 1 and
         (   DATEDIFF(MONTH, A.Firstduemonth , B.runmonth) + 1)  between 0 and  8
          then 1 else 0 end) as CONTRACTUAL_2_LE8
  , max(case when B.CONTRACTUALCD > 2 and
         (  DATEDIFF(MONTH, A.Firstduemonth , B.runmonth) + 1 )  between 0 and  9
          then 1 else 0 end) as CONTRACTUAL_3_LE9
            , max(case when B.CONTRACTUALCD > 2 and
          ( DATEDIFF(MONTH, A.Firstduemonth , B.runmonth) + 1)   between 0 and  12
          then 1 else 0 end) as CONTRACTUAL_3_LE12

          , max(case when B.CONTRACTUALCD > 3 and
         (  DATEDIFF(MONTH, A.Firstduemonth , B.runmonth) + 1 )  between 0 and  12
          then 1 else 0 end) as CONTRACTUAL_4_LE12

              , max(case when B.CONTRACTUALCD > 2 and
         (  DATEDIFF(MONTH, A.Firstduemonth , B.runmonth) + 1 )   between 0 and  18
          then 1 else 0 end) as CONTRACTUAL_3_LE18

         ,sum(case when (DATEDIFF(MONTH, A.Firstduemonth , B.runmonth) + 1 = 24)
           then  B.CONTRACTUALCD  else 0  end ) as STATUS_MONTH24

              ,max(case when  (  (DATEDIFF(MONTH, A.Firstduemonth , B.runmonth) + 1)  between 0 and  18)
               then B.CONTRACTUALCD else 0 end ) as MAX18

               ,max(case when (   (  DATEDIFF(MONTH, A.Firstduemonth , B.runmonth) + 1 )  between 0 and  24)
               then B.CONTRACTUALCD else 0 end ) as MAX24

               ,max(case when  ( (  DATEDIFF(MONTH, A.Firstduemonth , B.runmonth) + 1 )  between 0 and  36)
               then B.CONTRACTUALCD else 0 end ) as MAX36

               ,max(B.CONTRACTUALCD) as MAXEVER 

              ,max( DATEDIFF(MONTH, A.Firstduemonth , B.runmonth) + 1) as MSFD      
FROM #Temp_JS_MAXSTASARBS A   
inner join Provisions.dbo.CONTRACTUALCD  B 
on A.loanreference = B.loanref 
group by A.loanreference , A.FirstdueMonth , A.Disb_Number;


create table #JStemp4 
with (
          DISTRIBUTION = hash(loanreference),
          clustered columnstore index 
         )
as
 select distinct A.* ,
        B.CONTRACTUAL_1_LE1,
        B.CONTRACTUAL_2_LE2,
        B.CONTRACTUAL_3_LE3,
        B.CONTRACTUAL_2_LE3,
        B.CONTRACTUAL_2_LE4,
        B.CONTRACTUAL_2_LE5,
        B.CONTRACTUAL_2_LE6,
        B.CONTRACTUAL_2_LE7,
        B.CONTRACTUAL_2_LE8,
        B.CONTRACTUAL_3_LE9,
		B.CONTRACTUAL_3_LE12,
		B.CONTRACTUAL_4_LE12,
		B.CONTRACTUAL_3_LE18,
		B.STATUS_MONTH24,
		B.MAX18,
		B.MAX24,
		B.MAX36,
		B.MAXEVER,
		B.MSFD
 from #JStemp3  A 
 inner join   #Temp_CONCTRACTUALBAL_CD   B 
 on A.loanreference = B.Loanreference  and a.FirstdueMonth = b.FirstdueMonth and a.disb_number = b.disb_number ;

create table #JStemp5 
with (
          DISTRIBUTION = hash(Loanreference),
          clustered columnstore index 
         )
as

select distinct A.* ,
       B.CURED_4LE12 as CURED_4_LE12 , WO_IND 
FROM #JStemp4  A 
 left join  #Temp_JS_MAXSTASARBS  B 
 on A.loanreference = B.Loanreference  and a.FirstdueMonth = b.FirstdueMonth and a.disb_number = b.disb_number 
;

create table #JStemp6 
with (
          DISTRIBUTION = hash(loanid),
          clustered columnstore index 
         )
as
 select distinct 
 A.* ,
 case when len(C.OfferGroup)=1 then '0'+ C.OfferGroup else C.OfferGroup end as EXPERIANSCOREBAND1
 from #JStemp5  A 
 left join &DisbTblRef as C
		/*EDWDW.DBO.LOANQUOTATION C*/
 on cast(A.loanid as varchar(15)) = C.loanid and C.[STATUS] = 'DIS' ;

create table #JStempCombo  
with (
          DISTRIBUTION = hash(loanid),
          clustered columnstore index 
         )
as
  select loanid , count(*) as NumLoanid ,
sum(case when Product = 'Loan' then 1 else 0 end) as Loan , 
sum(case when Product = 'Card' then 1 else 0 end) as [Card]  
from #JStemp6  
group by loanid 
having count(*) > 1;

create table #JStemp7 
with (
          DISTRIBUTION = hash(loanid),
          clustered columnstore index 
         )
as

select A.* , (case when B.loanid is not null then 1 else 0 end) as ComboFlag 
from  #JStemp6 A 
left join  #JStempCombo B 
on A.loanid = B.loanid ;

 create table #JStemp8 
  with (
          DISTRIBUTION = hash(loanid),
          clustered columnstore index 
         )
as
 select * from #JStemp7 
 where ComboFlag = 0 or Product = 'Loan';

create table #JStemp9  
with (
          DISTRIBUTION = hash(loanid),
          clustered columnstore index 
         )
as
select * from #JStemp8   
where CHARINDEX('E',LOANREFERENCE) = 0 
and isnull(Product_detail,'') not like ('%EHL%');

create table #JStemp10  
with (
          DISTRIBUTION = hash(loanid),
          clustered columnstore index 
         )
as  
select *  from #JStemp9 
where   isnull(Product_detail,'') not like ('THOR%');

create table #JStemp11  
with (
          DISTRIBUTION = hash(loanid),
          clustered columnstore index 
         )
as  
select *  from #JStemp10 
where   isnull(Product_detail,'') not like ('PAYROLL%');

create table #JStemp12  
with (
           DISTRIBUTION = hash(loanid),
          clustered columnstore index 
         )
as  
select B.idnumber, A.* from #JStemp11 A 
inner join &DisbTblRef B
on A.loanreference = B.loanreference and A.loanid = B.loanid and A.disb_number = B.disb_number 
where idnumber is not null;
/**/
 Truncate table  &OutcomeTableName; 
 drop table &OutcomeTableName ;
create table &OutcomeTableName  
with (
          DISTRIBUTION = hash(loanid),
          clustered columnstore index 
         )
as  
select   A.* , coalesce(B.Principaldebt, C.Principaldebt) as Principaldebt from 
#JStemp12  A  
left join prd_exactusSync.dbo.ZA31200P B	/*-- this was pulled on Loans Only */
on A.LoanReference = B.LoanReference 
left join ARC_LOANQ_2028.dbo.loanquotationaccount as C
on A.loanid = c.loanid;

) by APS;
quit;


proc sql; connect to odbc (dsn=MPWAPS);
execute (
/*---- Clean up ----*/

	 Truncate table  prd_DataDistillery.[dbo].JS_OUTCOME_BASE_FINAL; 
 drop table prd_DataDistillery.[dbo].JS_OUTCOME_BASE_FINAL ;
	/*---- Clean up ----*/
	Create table prd_DataDistillery.[dbo].JS_OUTCOME_BASE_FINAL
	with (distribution = hash(loanid), clustered columnstore index ) as
	select * from prd_DataDistillery_data.[dbo].JS_OUTCOME_BASE_FINAL
	/* truncate table &TableName.0;*/
	/* drop table &TableName.0;*/

) by odbc;
quit;



/*filename macros2 'H:\Process_Automation\macros';*/
/*options sasautos = (sasautos  macros2);*/

%end_program(&process_number);
