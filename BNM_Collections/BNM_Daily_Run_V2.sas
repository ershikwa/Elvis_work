/*start time
data _null_;

Call Symput ("Date", put(today(),date9.));
Call symput("Start_time",Put(time(),tod5.));
Call symput("Complete_time",Put(time(),tod5.));

run;

%let date = %sysfunc(symget(Date));
%let start_time = %sysfunc(symget(Start_time));
%let complete_time = %sysfunc(symget(Complete_time));

data MD_COl_BNM_Runtime;
infile datalines;
input Date :date9. start_time :time. Completetime :datetime.;
runtime = intck('HOUR',start_time, Completetime,'DT');
format Date date9. start_time time. Completetime datetime. runtime time8.;

Call Symput ("Date", put(Date,date9.)));
Call symput("Start_time",Put(Start_time,tod5.));
Call symput("Complete_time",Put(Complete_time,datetime.));

runtime = intck('minute',start_time, Completetime,'DT');
datalines;
&date &start_time &Completetime
;

run;
*/

options compress=yes;

/*libname bnm '\\mpwsas9\Collections Strategy\BNM Collections';*/
libname bnmdata '\\MPWSAS65\BNM\BNM_Data';
Libname Monitor '\\MPWSAS65\BNM\New BNM Monitoring\Data';
libname PRD_COST odbc dsn=PRD_COST 	schema='dbo' preserve_tab_names=yes connection=unique; 
libname edwdw odbc dsn=edwdw schema="DBO" preserve_tab_names=yes CONNECTION=UNIQUE direct_sql=yes;
libname CredScr odbc dsn=Cred_Scoring schema="DBO" preserve_tab_names=yes CONNECTION=UNIQUE direct_sql=yes;
libname Dev_DDOp odbc dsn=Dev_DDOp schema="DBO" preserve_tab_names=yes CONNECTION=UNIQUE direct_sql=yes;
libname dev_cont odbc dsn=dev_cont schema="DBO" preserve_tab_names=yes CONNECTION=UNIQUE direct_sql=yes;
libname Prd_Cont odbc dsn=Prd_Cont schema="DBO" preserve_tab_names=yes CONNECTION=UNIQUE direct_sql=yes;


%include "\\neptune\credit$\AA_GROUP CREDIT\Scoring\SAS Macros\upload_aps_A.sas";

%let odbc = MPWAPS;

data _null_;
     call symput('tday',cats("'",put(intnx('day',today(),-1),yymmddn8.),"'"));
     call symput('tday_',put(intnx('day',today(),0),yymmddn8.));
     call symput('today',dhms(intnx('day',today(),-1),00,00,00));
	 Call symput('Month_Call',put(intnx('day',today(),-1),yymmn6.));
     call symput('month1',put(intnx('month',today(),0),yymmn6.));
     call symput('month2',put(intnx('month',today(),-1),yymmn6.));
     call symput('month3',put(intnx('month',today(),-2),yymmn6.));
     call symput('month4',put(intnx('month',today(),-3),yymmn6.));
     call symput('month5',put(intnx('month',today(),-4),yymmn6.));
     call symput('monthtu1',cats("'",put(intnx('month',today(),0),yymmd7.),"%'"));
     call symput('monthtu2',cats("'",put(intnx('month',today(),-1),yymmd7.),"%'"));
     Call Symput('DDay',-8);
	 Call symput('Run',cats("'",put(intnx('Day',today(),0),yymmddn.),"'"));
	 Call symput('Run1',cats("'",put(intnx('Day',today(),-3),yymmddn.),"'"));
	 call symput('tdayf',put(intnx('day',today(),0),yymmddn8.));
run;

     
%put &tdayf;
%put &tday;
%put &tday_;
%put &today;
%Put &Month_Call;
%put &month1;
%put &month2;
%put &month3;
%put &month4;
%put &month5;

%put &monthtu1;
%put &monthtu2;

/*New Variable*/
%Put &DDay;
%Put &Run;
%Put &Run1;
/*delete old data*/
proc delete data= dev_cont.vars_&month5;
run;

/*set the base*/
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
select distinct clientnumber
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

if object_id('dev_contactinfo.dbo.bnm_fin_nums') is not null
drop table dev_contactinfo.dbo.bnm_fin_nums
create table dev_contactinfo.dbo.bnm_fin_nums with (clustered columnstore index,distribution=hash(idnumber)) as
select idnumber, clientnumber, number
from #bnmbase1

) by APS;
quit;

/*get numbers to delete*/
Proc Sql;
     Connect to ODBC (dsn = ABC_Col_Dialer);
     Create Table Coll_BNM_Deleted_Numbers as
           Select * from connection to odbc 
                (
                Select IDnumber,loanrefno,ContactNumber,DeleteDateTime
                from ABC_Dialer.dbo.AB_Col_Delete_History
                where convert(date,convert(varchar(50), DeleteDateTime))  >= dateadd(day,-2,cast(getdate() as date))
                Union
                Select IDnumber,loanrefno,ContactNumber,DeleteDateTime
                from ABC_Dialer.dbo.AB_Col_Delete_History_Archive
                where convert(date,convert(varchar(50), DeleteDateTime))  >= dateadd(day,-2,cast(getdate() as date))
                
                );
     disconnect from odbc;
Quit;

%Upload_APS_A(Set = Coll_BNM_Deleted_Numbers, Server = WORK, APS_ODBC = dev_cont, APS_DB = dev_contactinfo,Distribute = HASH(IDnumber));


/*get the latest xi107 numbers*/

proc sql stimer feedback;
     connect to ODBC (dsn = mpwetlp1);
     create table MD_XI10700P_Coll as
     select * from connection to odbc (
Select clientnumber, telephonetype,relationship,replace(concat(areacode,telephonenumber), ' ', '') as telephonenumber,lastupdatetimestamp 
from PRD_Restricted_Telnos.dbo.XI10700P  where telephonenumber != '' and 
convert(date,convert(varchar(10), lastupdatetimestamp)) >= dateadd(day,-1,cast(getdate() as date))
);
disconnect from odbc;
quit;

%Upload_APS_A(Set = MD_XI10700P_Coll, Server = WORK, APS_ODBC = dev_cont, APS_DB = dev_contactinfo,Distribute = HASH(clientnumber));

proc sql stimer feedback;
connect to ODBC as APS (dsn=mpwaps);
execute (
		create table dev_datadistillery_optimisation.dbo.#clean1 with(clustered columnstore index, distribution=hash(clientnumber)) as
        select  clientnumber, replace(replace(lower(telephonenumber), ' ', '') , '+', '') as telephonenumber
        from dev_contactinfo.dbo.MD_XI10700P_Coll
        where clientnumber in (select distinct clientnumber from dev_contactinfo.dbo.bnm_fin_nums )

        create table dev_datadistillery_optimisation.dbo.#clean2 with(clustered columnstore index, distribution=hash(clientnumber)) as
        select distinct clientnumber,
        case when len(telephonenumber) = 11 and substring(telephonenumber,1,2) = '27' 
        then concat('0', substring(telephonenumber,3,9)) else 
        telephonenumber end as telephonenumber
        from dev_datadistillery_optimisation.dbo.#clean1
        
        
		create table dev_datadistillery_optimisation.dbo.#MD_XI10700P_Coll with(clustered columnstore index,distribution=hash(clientnumber)) as
		select  a.clientnumber,a.telephonenumber
        from (select distinct clientnumber,telephonenumber 
		from dev_datadistillery_optimisation.dbo.#clean2
        where len(telephonenumber) = 10 and telephonenumber like '0%' and substring(telephonenumber,1,4) < '0862'
		and telephonenumber not like '00%' and telephonenumber not like '09%' 
		and telephonenumber not like '01000000%' and telephonenumber not like '9999%'
		and telephonenumber not like '080%' and telephonenumber not like '01100000%') a
		
/*get latest numbers and delete some before champion split*/
if object_id('dev_contactinfo.dbo.base_latest_coll') is not null
drop table dev_contactinfo.dbo.base_latest_coll
create table dev_contactinfo.dbo.base_latest_coll with (clustered columnstore index, distribution=hash(idnumber)) as
	          select a.idnumber, a.clientnumber, b.number  from
             (select distinct idnumber, clientnumber from dev_contactinfo.dbo.bnm_fin_nums) a
          inner join (select clientnumber,telephonenumber as number
             from dev_datadistillery_optimisation.dbo.#MD_XI10700P_Coll)b
             on a.clientnumber = b.clientnumber
	 where  substring(number,1,4) < '0862'

)by APS;
quit;

/*@Mpho : */
data champion challenge1 challenge2;
     set dev_cont.base_latest_coll;
     z=uniform(111);
     if z <= 0.1 then output challenge1;
     else if z <= 0.9 then output challenge2;
     else output champion;
run;

data finchallenger1;
     set dev_cont.finchallenger1 challenge1(keep=idnumber);
run;

proc sort data=finchallenger1 nodupkey;
     by _all_;
run;

data finchallenger2;
     set dev_cont.finchallenger2 challenge2(keep=idnumber);
run;

proc sort data=finchallenger2 nodupkey;
     by _all_;
run;

data finchampion;
     set dev_cont.finchampion champion(keep=idnumber);
run;

proc sort data=finchampion nodupkey;
     by _all_;
run;

%Upload_APS_A(Set = finchampion, Server = WORK, APS_ODBC = dev_cont, APS_DB = dev_contactinfo,Distribute = HASH(IDnumber));
%Upload_APS_A(Set = finchallenger1, Server = WORK, APS_ODBC = dev_cont, APS_DB = dev_contactinfo,Distribute = HASH(IDnumber));
%Upload_APS_A(Set = finchallenger2, Server = WORK, APS_ODBC = dev_cont, APS_DB = dev_contactinfo,Distribute = HASH(IDnumber));

/*add new xi107 numbers and delete optouts*/
 proc sql stimer feedback;
connect to ODBC as APS (dsn=mpwaps);
execute (

create table dev_contactinfo.dbo.#bnm_fin_nums with (clustered columnstore index, distribution=hash(idnumber)) as
select idnumber, clientnumber, number
from dev_contactinfo.dbo.bnm_fin_nums
union
select idnumber, clientnumber, number
from dev_contactinfo.dbo.base_latest_coll


create table dev_contactinfo.dbo.#remove_dups with (clustered columnstore index, distribution=hash(idnumber)) as
select idnumber, max(clientnumber) as clientnumber
from #bnm_fin_nums
group by idnumber

create table dev_contactinfo.dbo.#bnm_fin_nums2 with (clustered columnstore index, distribution=hash(idnumber)) as
select a.idnumber,a.clientnumber, b.number,case when number is not null then dateadd(day,-1,cast(getdate() as date)) end as startdate
from #remove_dups a
left join #bnm_fin_nums b
on a.idnumber = b.idnumber
where number is not null and a.idnumber not in ('TRF1234', '', '0')

create table dev_contactinfo.dbo.#bnm_fin_nums3 with (clustered columnstore index, distribution=hash(idnumber)) as
     Select a.idnumber,clientnumber, number,startdate
     from #bnm_fin_nums2 as A
     Left Join dev_contactinfo.dbo.Coll_BNM_Deleted_Numbers  as B
     On a.IDNumber = b.IDNumber and a.Number = b.ContactNumber
	 where b.ContactNumber is null

if object_id('dev_contactinfo.dbo.bnm_fin_nums') is not null
drop table dev_contactinfo.dbo.bnm_fin_nums
create table dev_contactinfo.dbo.bnm_fin_nums with (clustered columnstore index, distribution=hash(idnumber)) as
select distinct *
from dev_contactinfo.dbo.#bnm_fin_nums3
          
)by APS;
quit;
/*
data bnmdata.bnm_fin_nums;
     set dev_cont.bnm_fin_nums(keep=idnumber clientnumber number);
run;*/

/*getting the tu data*/
proc sql stimer;
     connect to ODBC (dsn = mpwaps);
     create table TU_Bur_Base as
     select * from connection to odbc (
           select idnumber, 
           home_tel_1_date, home_tel_1_code, home_tel_1_no,
           home_tel_2_date, home_tel_2_code, home_tel_2_no,
           home_tel_3_date, home_tel_3_code, home_tel_3_no,
           load_date, info_date, load_type
           from prd_bur.bureau.chd_cons_contact
           where load_type = 'MONTHLY_AB' and load_date like case when load_date like &monthtu1 then &monthtu1 else &monthtu2 end
     );
     disconnect from odbc;
quit;

/*we concatenate the work and home numbers to have the code and number in the same column*/

data tu_data_clean;
     set tu_bur_base;
     home_tel1 = cats(home_tel_1_code, home_tel_1_no);
     home_tel2 = cats(home_tel_2_code, home_tel_2_no);
     home_tel3 = cats(home_tel_3_code, home_tel_3_no);
run;

data tu_data_clean2;
     set tu_data_clean;
     drop
     home_tel_1_code home_tel_1_no
     home_tel_2_code home_tel_2_no
     home_tel_3_code home_tel_3_no;
run;

data tu_data_rename;
     set tu_data_clean2;
     rename
                home_tel_1_date = homedate1
                home_tel_2_date = homedate2
                home_tel_3_date = homedate3;
run;

proc sort data=tu_data_rename out=tu_data_sorted;
     by idnumber descending load_date info_date load_type;
run;

proc sort data=tu_data_sorted nodupkey out=tu_data_sorted_;
     by idnumber;
run;

DATA tu_data_flip (KEEP=idnumber load_date info_date load_type homedate home);
SET tu_data_sorted_;
     ARRAY homedates{3} homedate1-homedate3;
     ARRAY homes{3} home_tel1-home_tel3; 

     do i = 1 to 3 ;
     homedate = homedates{i}    ;
     home = homes{i};
     OUTPUT;
     end;

RUN;

data tu_data_flipped;
     set tu_data_flip;
     format datehome yymmddd10.;
     datehome = input(homedate, b8601da.);
     where length(home)=10 and idnumber ne '' and home ne '';
run;

proc sort data=tu_data_flipped nodupkey out=tu_data_clean;
     by idnumber descending datehome;
run;

data tu_data_ranked;
     set tu_data_clean;
     by idnumber descending info_date;
     if first.info_date then rn =1;
     else rn +1;
run;

%Upload_APS_A(Set = tu_data_ranked, Server = WORK, APS_ODBC = dev_cont, APS_DB = dev_contactinfo,Distribute = HASH(IDnumber));

/****VARS EGINEERING************************/
 proc sql stimer feedback;
connect to ODBC as APS (dsn=mpwaps);
execute (

if object_id(%str(%'dev_contactinfo.dbo.vars_&month1%'), 'u') is not null
drop table dev_contactinfo.dbo.vars_&month1
CREATE TABLE dev_contactinfo.dbo.vars_&month1 WITH (CLUSTERED COLUMNSTORE INDEX,DISTRIBUTION=HASH(idnumber))
AS
   select idnumber, cast(phonenumber as varchar(10)) as phonenumber, initiateddate, terminateddate, duration, lineduration, cast(finishcode as varchar(50)) as finishcode,
    rpc, &Month_Call as source_month  
   from Prd_ContactInfo.dbo.[MD_RPC_Activities_&Month_Call]
   where isnull(idnumber,'') <> '' and isnull(phonenumber,'') <> '' and len(phonenumber) = 10 and calldirection ='Outbound'

if object_id('dev_contactinfo.dbo.all_vars_clean') is not null
	drop table dev_contactinfo.dbo.all_vars_clean
	CREATE TABLE dev_contactinfo.dbo.all_vars_clean WITH (CLUSTERED COLUMNSTORE INDEX,DISTRIBUTION=HASH(idnumber))
AS
select idnumber,phonenumber, initiateddate, terminateddate, duration, lineduration,
 cast(finishcode as varchar(50)) as finishcode,rpc,source_month
 from dev_contactinfo.dbo.vars_&month1
 union
 select idnumber,phonenumber, initiateddate, terminateddate, duration, lineduration,
 cast(finishcode as varchar(50)) as finishcode,rpc,source_month
 from dev_contactinfo.dbo.vars_&month2
 union
 select idnumber,phonenumber, initiateddate, terminateddate, duration, lineduration,
 cast(finishcode as varchar(50)) as finishcode,rpc,source_month
 from dev_contactinfo.dbo.vars_&month3
 union
 select idnumber,phonenumber, initiateddate, terminateddate, duration, lineduration,
 cast(finishcode as varchar(50)) as finishcode,rpc,source_month
 from dev_contactinfo.dbo.vars_&month4
)by APS;
quit;

 proc sql stimer feedback;
connect to ODBC as APS (dsn=mpwaps);
execute (
create table dev_contactinfo.dbo.#ids_vars_clean with (clustered columnstore index, distribution=hash(idnumber)) as
     select a.idnumber, a.clientnumber, a.number, a.startdate,
	  b.initiateddate, b.finishcode, b.rpc, b.lineduration, b.duration
     from dev_contactinfo.dbo.bnm_fin_nums a
     left join dev_contactinfo.dbo.all_vars_clean b
     on a.idnumber=b.idnumber and a.number = b.phonenumber;


	 create table dev_contactinfo.dbo.#all_vars_ with (clustered columnstore index, distribution=hash(idnumber)) as
     select distinct a.idnumber, a.clientnumber, a.number,a.startdate,a.initiateddate,a.rpc,a.lineduration, a.duration,
	 case when fcode in ('FC37','FC12','FC32','FC33','FC34','FC35','FC22','FC4','FC7','FC8','FC9','-9','FC5','FC20','FC11','FC1','FC19','FC21','FC18','FC14','FC28','FC6') then 'FC37'/*bucketing into the same score*/
	 when fcode in ('FC16','FC13','FC26','FC27','FC25','FC10','FC31','FC24','FC15','FC17','FC2','FC23','FC29','FC30','FC36') then 'FC16'
	 else 'FC37'end as recentfcode/*for numbers with no past fcode*/,

	 case when fcode = 'FC37' or fcode = 'FC12' and rpc = 0 then 1 else 0 end as istpc,
     case when fcode = 'FC9' or fcode = 'FC32' or fcode = 'FC33' or fcode = 'FC34' or fcode = 'FC35' and rpc = 0 then 1 else 0 end as plscallback,
     case when fcode = 'FC14' or fcode = 'FC15' or fcode = 'FC20' then 1 else 0 end as isnoanswer,
     case when fcode = 'FC7' and rpc = 0 then 1 else 0 end as iscalldropped,
     case when fcode = 'FC6' then 1 else 0 end as isbusy,
     /*case when fcode = 'FC11' and rpc = 0 then 1 else 0 end as leftmessage,*/
     /*case when fcode = 'FC19' then 1 else 0 end as engaged,*/
     /*case when fcode = 'FC22' or fcode = 'FC23' and rpc = 0 then 1 else 0 end as isdisconnect,*/
     case when fcode = 'FC18' or fcode = 'FC24' or fcode = 'FC25' or fcode = 'FC26' or fcode = 'FC27' 
	 or fcode = 'FC28' or fcode = 'FC29' or fcode = 'FC30' or fcode = 'FC31' then 1 else 0 end as isfailed,
	 a.finishcode

	 from  #ids_vars_clean a
     left join dev_contactinfo.dbo.finishcode_lookup b
     on a.finishcode = b.finishcode;


	 create table dev_datadistillery_optimisation.dbo.#variables_calc with (clustered columnstore index, distribution=hash(idnumber)) as
     select a.idnumber, a.number, cast(a.startdate as date) as calldate, b.initiateddate, b.finishcode,

     case when cast(b.initiateddate as date) >= dateadd(day,-7, a.startdate) and b.rpc = 1 then 1 else 0 end as RPC_7days,
     case when cast(b.initiateddate as date) >= dateadd(day,-30,a.startdate) and b.rpc = 1 then 1 else 0 end as RPC_30days,
     case when cast(b.initiateddate as date) >= dateadd(day,-90,a.startdate) and b.rpc = 1 then 1 else 0 end as RPC_90days,

     case when cast(b.initiateddate as date) >= dateadd(day,-45,a.startdate) and b.finishcode in ('DIARA','SNCT', 'SNCTB','SNCTH', 'SNCTO') and b.rpc = 0 then 1 else 0 end as callback_45days,

     case when cast(b.initiateddate as date) >= dateadd(day,-90,a.startdate) and b.finishcode='CALLD' and b.rpc = 0 then 1 else 0 end as calldropped_90days,

     case when cast(b.initiateddate as date) >= dateadd(day,-30,a.startdate) and b.finishcode in ('Not Reached','SIT Callable - Disconnect before Analysis','SIT Callable - Ineffective Other','SIT Callable - No Circuit','SIT Callable - Reorder','SIT Callable - Temporary Failure','SIT Uncallable - Bad Number','SIT Uncallable - Unknown Tone','SIT Uncallable - Vacant Code') and b.rpc = 0 then 1 else 0 end as failed_30days,
     case when cast(b.initiateddate as date) >= dateadd(day,-45,a.startdate) and b.finishcode in ('Not Reached','SIT Callable - Disconnect before Analysis','SIT Callable - Ineffective Other','SIT Callable - No Circuit','SIT Callable - Reorder','SIT Callable - Temporary Failure','SIT Uncallable - Bad Number',
	 'SIT Uncallable - Unknown Tone','SIT Uncallable - Vacant Code') and b.rpc = 0 then 1 else 0 end as failed_45days,

     case when cast(b.initiateddate as date) >= dateadd(day,-30,a.startdate) and b.finishcode in ('TPC', 'LMTPC') and b.rpc = 0 then 1 else 0 end as TPC_30days,
     
     case when cast(b.initiateddate as date) >= dateadd(day,-90,a.startdate) and b.finishcode in ('Not Reached','SIT Callable - Disconnect before Analysis','SIT Callable - Ineffective Other','SIT Callable - No Circuit','SIT Callable - Reorder','SIT Callable - Temporary Failure','SIT Uncallable - Bad Number',
	 'SIT Uncallable - Unknown Tone','SIT Uncallable - Vacant Code') and b.rpc = 0 then 1 else 0 end as failed_90days,
     
     case when cast(b.initiateddate as date) >= dateadd(day,-60,a.startdate) and b.finishcode in ('No Answer','No Answer - Timeout','PNR') and b.rpc = 0 then 1 else 0 end as noanswer_60days,
     
     case when cast(b.initiateddate as date) >= dateadd(day,-7,a.startdate) and b.finishcode in ('DIARA','SNCT', 'SNCTB','SNCTH', 'SNCTO') and b.rpc = 0 then 1 else 0 end as callback_7days,
	 case when cast(b.initiateddate as date) >= dateadd(day,-90,a.startdate) and istpc = 1 then 1 else 0 end as tpc,
	 case when cast(b.initiateddate as date) >= dateadd(day,-90,a.startdate) and isbusy = 1 then 1 else 0 end as busy,
	 case when cast(b.initiateddate as date) >= dateadd(day,-90,a.startdate) and isfailed = 1 then 1 else 0 end as isfailed,
	 case when cast(b.initiateddate as date) >= dateadd(day,-90,a.startdate) then lineduration else null end as lineduration,
	 case when cast(b.initiateddate as date) >= dateadd(day,-90,a.startdate) then duration else null end as duration

     from (select idnumber,clientnumber, number,startdate from dev_contactinfo.dbo.bnm_fin_nums) a
     left join (select idnumber, number, initiateddate, finishcode,istpc,isbusy,isfailed,lineduration,duration, rpc from #all_vars_) b
     on a.idnumber=b.idnumber and a.number=b.number


	 create table dev_datadistillery_optimisation.dbo.#some_vars with (clustered columnstore index, distribution=hash(idnumber)) as
     select idnumber, number, calldate,

     sum(tpc_30days) as tot_tpc30, 

     max(rpc_7days) as isrpc7,
     max(rpc_30days) as isrpc30, 
     max(RPC_90days) as isrpc90,

     sum(callback_7days) as tot_callback7,
     sum(callback_45days) as tot_callback45, 

     sum(calldropped_90days) as tot_calldropped90,

     max(failed_30days) as failed30, 
     sum(failed_45days)   as   tot_failed45,
     sum(failed_90days)   as   tot_failed90,

     sum(noanswer_60days) as    tot_noanswer60,

	 max(tpc) as tpc,
	 max(busy) as busy,
	 max(isfailed) as isfailed,
	 avg(lineduration) as avg_lineduration,
	 avg(duration) as avg_callduration,
	 sum(duration) as tot_callduration

     from #variables_calc
     group by idnumber, number, calldate;


	create table dev_datadistillery_optimisation.dbo.#rec_fcode with (clustered columnstore index, distribution=hash(idnumber)) as
     select idnumber, number, startdate,
	 min(recentfcode) as recentfcode/*choosing the bucket with less weight for TPC*/
     from #all_vars_
	 group by idnumber, number, startdate;


	create table dev_datadistillery_optimisation.dbo.#call_all_ with (clustered columnstore index, distribution=hash(idnumber)) as
     select a.*, recentfcode
     from #some_vars a
     left join #rec_fcode b
     on a.idnumber = b.idnumber and a.number = b.number and a.calldate = b.startdate;

	 if object_id('dev_contactinfo.dbo.call_all_base') is not null
	drop table dev_contactinfo.dbo.call_all_base
	 create table dev_contactinfo.dbo.call_all_base with (clustered columnstore index, distribution=hash(idnumber)) as
     select idnumber, number, calldate, tot_tpc30, isrpc7,isrpc30,isrpc90,tot_callback7,tot_callback45,
	 tot_calldropped90,failed30,tot_failed45,tot_failed90,tot_noanswer60,tpc,busy,isfailed,avg_lineduration,
	 avg_callduration, tot_callduration,recentfcode
	 from #call_all_


	 create table dev_contactinfo.dbo.#istpc with (clustered columnstore index, distribution=hash(idnumber)) as
     select a.*, b.initiateddate, 
     datediff(day,cast(b.initiateddate as date),cast(a.startdate as date)) as tpcdaydiff
     from (select idnumber,clientnumber, number,startdate from dev_contactinfo.dbo.bnm_fin_nums) a
     inner join 
     (select idnumber, number, istpc, initiateddate
     from #all_vars_
     where istpc = 1) b
     on a.idnumber = b.idnumber and a.number = b.number;  


	 create table dev_contactinfo.dbo.#tpcdaydiff with (clustered columnstore index, distribution=hash(idnumber)) as
     select idnumber, clientnumber, number, startdate, min(tpcdaydiff) as tpcdaydiff
	 from #istpc
	 group by  idnumber, clientnumber, number, startdate


	 create table dev_contactinfo.dbo.#isfailed with (clustered columnstore index, distribution=hash(idnumber)) as
     select a.*, b.initiateddate, 
     datediff(day,cast(b.initiateddate as date),cast(a.startdate as date)) as faileddaydiff
     from (select idnumber,clientnumber, number,startdate from dev_contactinfo.dbo.bnm_fin_nums) a
     inner join 
     (select idnumber, number, isfailed, initiateddate
     from #all_vars_
     where isfailed = 1) b
     on a.idnumber = b.idnumber and a.number = b.number


	create table dev_contactinfo.dbo.#isfailed2 with (clustered columnstore index, distribution=hash(idnumber)) as
     select idnumber, clientnumber, number, startdate, min(faileddaydiff) as faileddaydiff
	 from #isfailed
	 group by  idnumber, clientnumber, number, startdate

	create table dev_contactinfo.dbo.#call_all_base2 with (clustered columnstore index, distribution=hash(idnumber)) as
     select distinct a.*,
     	 case when tpcdaydiff is null then -999 when tpcdaydiff = 0 then -9 else tpcdaydiff end as tpcdaydiff,
	 case when faileddaydiff is null then -999 when faileddaydiff = 0 then -9 else faileddaydiff end as faileddaydiff
     from dev_contactinfo.dbo.call_all_base a
     	 left join dev_contactinfo.dbo.#tpcdaydiff b
	 on a.idnumber = b.idnumber and a.number = b.number
	 left join dev_contactinfo.dbo.#isfailed2 c
	 on a.idnumber = c.idnumber and a.number = c.number


	 if object_id('dev_contactinfo.dbo.final_col_rpc_vars') is not null
	drop table dev_contactinfo.dbo.final_col_rpc_vars
	 create table dev_contactinfo.dbo.final_col_rpc_vars with (clustered columnstore index, distribution=hash(idnumber)) as
	 select a.idnumber, a.number, a.calldate, isrpc7,isrpc30, isrpc90,
	 tot_callback45, tot_calldropped90, failed30, tot_failed45,
	 faileddaydiff, isnull(avg_lineduration,-999) as avg_lineduration ,
	 isnull(tot_callduration,-999)  as tot_callduration
	 ,case when number is not null then 1 end as istbclilent
	 from dev_contactinfo.dbo.#call_all_base2 a


	if object_id('dev_contactinfo.dbo.final_col_tpc_vars') is not null
	drop table dev_contactinfo.dbo.final_col_tpc_vars
	 create table dev_contactinfo.dbo.final_col_tpc_vars with (clustered columnstore index, distribution=hash(idnumber)) as
	 select distinct a.idnumber, a.number, a.calldate,tot_tpc30
	 , tot_calldropped90, tot_failed90, tot_noanswer60,tot_callback7,
	 tpcdaydiff, isnull(avg_lineduration,-999) as avg_lineduration ,
	 isnull(avg_callduration, -999) as avg_callduration,
	isnull(rn,-9) as TU_Home_Rank,
     case when a.number is not null then 373 end as tbcelldaydiff ,case when tpc is null then -9 else tpc end as tpc,
     case when busy is null then -9 else busy end as  busy,isnull(recentfcode,'-9') as recentfcode
	 from dev_contactinfo.dbo.#call_all_base2 a
	 left join (select idnumber,home,max(rn) as rn
	 from  dev_contactinfo.dbo.tu_data_ranked group by idnumber, home)b
	 on a.idnumber = b.idnumber and a.number = b.home
)by APS;
quit;

/*Add EDC RPC flags*/
proc sql stimer feedback;
connect to ODBC as APS (dsn=mpwaps);
execute (
CREATE TABLE #MD_EDC_Base
WITH
(
DISTRIBUTION = HASH(idnumber),
CLUSTERED COLUMNSTORE INDEX
)
AS
select idnumber, clientnumber, number, EOMONTH(cast(concat(performance_month,'01') as DATE)) as performance_month
from dev_contactinfo.dbo.MD_EDC_Base where performance_month != ''

create table #variables_calc_EDC with (clustered columnstore index, distribution=hash(idnumber)) as
select a.idnumber, a.number, cast(a.startdate as date) as calldate, b.performance_month,

case when cast(b.performance_month as date) >= dateadd(day,-7, a.startdate) and b.edc_rpc = 1 then 1 else 0 end as RPC_7days,
case when cast(b.performance_month as date) >= dateadd(day,-30,a.startdate) and b.edc_rpc = 1 then 1 else 0 end as RPC_30days,
case when cast(b.performance_month as date) >= dateadd(day,-90,a.startdate) and b.edc_rpc = 1 then 1 else 0 end as RPC_90days
from (select idnumber,clientnumber, number,startdate from dev_contactinfo.dbo.bnm_fin_nums) a
INNER JOIN (select idnumber, number, performance_month, case when number is not null then 1 else 0 end as edc_rpc from #MD_EDC_Base) b
on a.idnumber=b.idnumber and a.number=b.number

create table #max_EDC with (clustered columnstore index, distribution=hash(idnumber)) as
select idnumber, number, calldate,

max(rpc_7days) as isrpc7,
max(rpc_30days) as isrpc30, 
max(RPC_90days) as isrpc90
from #variables_calc_EDC
group by idnumber, number, calldate

if object_id(%str(%'dev_contactinfo.dbo.final_col_rpc_&tday_%'),'u') is not null
drop table dev_contactinfo.dbo.final_col_rpc_&tday_
create table dev_contactinfo.dbo.final_col_rpc_&tday_ with (clustered columnstore index, distribution=hash(idnumber)) as 
select a.idnumber, a.number, a.calldate,a.isrpc7,
case when a.isrpc30 >= b.isrpc30 or b.isrpc30 is null then a.isrpc30 else  b.isrpc30 end as isrpc30 ,
case when a.isrpc90 >= b.isrpc90 or b.isrpc90 is null then a.isrpc90 else  b.isrpc90 end as isrpc90 ,
a.isrpc30 as cic_isrpc30, b.isrpc30 as edc_isrpc30, a.isrpc90 as cic_isrpc90,b.isrpc90 as edc_isrpc90,
	 tot_callback45, tot_calldropped90, failed30, tot_failed45,
	 faileddaydiff,avg_lineduration ,tot_callduration,istbclilent
from dev_contactinfo.dbo.final_col_rpc_vars a
left join #max_EDC b
on a.idnumber = b.idnumber and a.number = b.number


 )by APS;
 quit;


/*joining or base data to the tu_data*/
/*Get assistence with calculating the days difference for the TU_Data*/

**********************************************************************************************************
***** Applying both our models                                                                                            ******
**********************************************************************************************************;

/* scoring with the bnm*/
/*data score_base;
set dev_cont.final_col_rpc_vars;
run;*/

%macro scoreinputdata(inputdataset=,outputdataset=);
     %let i = 1;
     libname iv&i "\\mpwsas9\Collections Strategy\BNM Collections\Models\bnm5";
     %global  segment_&i._list ;
     proc sql; 
           select reverse(substr(reverse(compress(Parameter)),3)) into : segment_&i._list separated by ' ' 
           from iv&i..parameter_estimate where upcase(Parameter) ne 'INTERCEPT'; 
     quit;

     %put &&segment_&i._list ;

     data /*segment_1*/&outputdataset;
           set &inputdataset;

          %do m = 1 %to %sysfunc(countw(&&segment_&i._list));
               %let var = %scan(&&segment_&i._list, &m);
               %include "\\mpwsas9\Collections Strategy\BNM Collections\Models\bnm5\&var._if_statement_.sas";
               %include "\\mpwsas9\Collections Strategy\BNM Collections\Models\bnm5\&var._woe_if_statement_.sas"; 
         %end;
         *****************************************;
         ** sas scoring code for proc hplogistic;
         *****************************************;
         %include "\\mpwsas9\Collections Strategy\BNM Collections\Models\bnm5\creditlogisticcode2.sas";
           drop _temp;
     run;

%mend;

%scoreinputdata(inputdataset=dev_cont.final_col_rpc_&tday_,outputdataset=data_scored_bnm_&tday_);


%Upload_APS_A(Set = data_scored_bnm_&tday_, Server = WORK, APS_ODBC = dev_cont, APS_DB = dev_contactinfo,Distribute = HASH(IDnumber));


/*scoring the tpc model*/
/*data score_base2;
set dev_cont.final_col_tpc_vars;
run;*/

%macro scoreinputdata(inputdataset=,outputdataset=);
     %let i = 1;
     libname iv&i "\\mpwsas9\Collections Strategy\TPC Model\Tpc5";
     %global  segment_&i._list ;
     proc sql; 
           select reverse(substr(reverse(compress(Parameter)),3)) into : segment_&i._list separated by ' ' 
           from iv&i..parameter_estimate where upcase(Parameter) ne 'INTERCEPT'; 
     quit;

     %put &&segment_&i._list ;

     data /*segment_1*/&outputdataset;
           set &inputdataset;

          %do m = 1 %to %sysfunc(countw(&&segment_&i._list));
               %let var = %scan(&&segment_&i._list, &m);
               %include "\\mpwsas9\Collections Strategy\TPC Model\Tpc5\&var._if_statement_.sas";
               %include "\\mpwsas9\Collections Strategy\TPC Model\Tpc5\&var._woe_if_statement_.sas"; 
         %end;
        *****************************************;
         ** sas scoring code for proc hplogistic;
         *****************************************;
         %include "\\mpwsas9\Collections Strategy\TPC Model\Tpc5\creditlogisticcode2.sas";
           drop _temp;
     run;


%mend;

%scoreinputdata(inputdataset=dev_cont.final_col_tpc_vars,outputdataset=data_scored_tpc_&tday_);


%Upload_APS_A(Set = data_scored_tpc_&tday_, Server = WORK, APS_ODBC = dev_cont, APS_DB = dev_contactinfo,Distribute = HASH(IDnumber));


/*******************************RANKING*********************************/
 proc sql stimer feedback;
connect to ODBC as APS (dsn=mpwaps);
execute (

if object_id(%str(%'dev_contactinfo.dbo.numbers_rank_score_&tday_%'),'u') is not null
	drop table dev_contactinfo.dbo.numbers_rank_score_&tday_
	 create table dev_contactinfo.dbo.numbers_rank_score_&tday_ with (clustered columnstore index, distribution=hash(idnumber)) as
	
     select a.idnumber,a.number, a.calldate, a.p_target1 as bnm_prob, b.p_target1 as tpc_prob, 1000*(a.p_target1/(a.p_target1+b.p_target1)) as rank_score
     from (select idnumber, number, calldate, p_target1 from dev_contactinfo.dbo.data_scored_bnm_&tday_) a
     inner join (select idnumber, number, calldate, p_target1 from dev_contactinfo.dbo.data_scored_tpc_&tday_) b
     on a.idnumber = b.idnumber and a.number = b.number

	 /*defining who to apply TPC rule to*/
	 create table dev_contactinfo.dbo.#numbers_ranked_fin with (clustered columnstore index, distribution=hash(idnumber)) as
	  select a.*, 
     case when a.idnumber = b.idnumber and 5*bnm_prob >= tpc_prob then 0
     when a.idnumber = b.idnumber and 5*bnm_prob < tpc_prob then 1
     else 0 end as TPC_Check 
     from dev_contactinfo.dbo.numbers_rank_score_&tday_ a
     left join dev_contactinfo.dbo.finchallenger2 b
     on a.idnumber = b.idnumber;

	 	 CREATE TABLE dev_contactinfo.dbo.#numbers_ranked_fin_2 WITH (CLUSTERED COLUMNSTORE INDEX,DISTRIBUTION=HASH(idnumber))
	AS
Select IDnumber,min(TPC_Check) as TPC_Rule/*tells us that at least 1 number will be available*/
from dev_contactinfo.dbo.#numbers_ranked_fin/*num_ranked_&tday_*/
Group By IDNumber;

/*people with all numbers passing TPC rule, where numbers would all be deleted*/
	 CREATE TABLE dev_contactinfo.dbo.#numbers_ranked_fin_3 WITH (CLUSTERED COLUMNSTORE INDEX,DISTRIBUTION=HASH(idnumber))
	AS
Select Idnumber
from #numbers_ranked_fin_2
where TPC_Rule = 1;

	 CREATE TABLE dev_contactinfo.dbo.#numbers_ranked_fin_4 WITH (CLUSTERED COLUMNSTORE INDEX,DISTRIBUTION=HASH(idnumber))
	AS
	select idnumber, case when idnumber = idnumber and 5*bnm_prob >= tpc_prob then number
     when idnumber = idnumber and 5*bnm_prob < tpc_prob then ''
     else number end as number,calldate,bnm_prob,tpc_prob,rank_score
	
 from dev_contactinfo.dbo.numbers_rank_score_&tday_
 Where IDnumber not in (Select IDnumber From #numbers_ranked_fin_3)

 /*people who we couldnt apply the TPC rule to because we will lose all their numbers*/
 	 CREATE TABLE dev_contactinfo.dbo.#TPC_Fix WITH (CLUSTERED COLUMNSTORE INDEX,DISTRIBUTION=HASH(idnumber))
	AS
Select idnumber,number,calldate,bnm_prob,tpc_prob,rank_score
from dev_contactinfo.dbo.numbers_rank_score_&tday_
Where IDnumber in (Select IDnumber From #numbers_ranked_fin_3);

/*RESTORING People who would otherwise be left with no numbers*/
 	 CREATE TABLE dev_contactinfo.dbo.#final_numbers WITH (CLUSTERED COLUMNSTORE INDEX,DISTRIBUTION=HASH(idnumber))
	AS
	SELECT idnumber,number,calldate,bnm_prob,tpc_prob,rank_score
	FROM #numbers_ranked_fin_4 WHERE number !=''
	UNION
	SELECT idnumber,number,calldate,bnm_prob,tpc_prob,rank_score
	FROM #TPC_Fix
	
 if object_id('dev_contactinfo.dbo.ranked_nums') is not null
	drop table dev_contactinfo.dbo.ranked_nums
	 CREATE TABLE dev_contactinfo.dbo.ranked_nums WITH (CLUSTERED COLUMNSTORE INDEX,DISTRIBUTION=HASH(idnumber))
	AS	
		SELECT
			idnumber, number, bnm_prob,  tpc_prob,rank_score,
			 ROW_NUMBER() OVER(PARTITION BY idnumber ORDER BY (rank_score )DESC , (number)DESC) AS RN
		FROM (select idnumber, number, bnm_prob,  tpc_prob,rank_score
		from dev_contactinfo.dbo.#final_numbers where number !='')a


	 CREATE TABLE dev_contactinfo.dbo.#mobi WITH (CLUSTERED COLUMNSTORE INDEX,DISTRIBUTION=HASH(idnumber))
	AS
		SELECT
			idnumber, number, rank_score, RN
			FROM dev_contactinfo.dbo.ranked_nums
			WHERE number between  '0603%' and  '085%'

CREATE TABLE dev_contactinfo.dbo.#top_rn_mob WITH (CLUSTERED COLUMNSTORE INDEX,DISTRIBUTION=HASH(idnumber))
	AS
		SELECT
			idnumber, min(RN) as top_rank
			FROM dev_contactinfo.dbo.#mobi
			GROUP BY idnumber

CREATE TABLE dev_contactinfo.dbo.#top_mobi WITH (CLUSTERED COLUMNSTORE INDEX,DISTRIBUTION=HASH(idnumber))
	AS
		SELECT
			a.idnumber, a.number, a.rank_score, a.RN
			FROM #mobi a
			inner join  #top_rn_mob b
			on a.idnumber = b.idnumber and a.RN = b.top_rank


CREATE TABLE dev_contactinfo.dbo.#piv_nums WITH (CLUSTERED COLUMNSTORE INDEX,DISTRIBUTION=HASH(idnumber))
	AS
	SELECT idnumber, Number_1, Number_2, Number_3, Number_4, Number_5,
	Number_6, Number_7, Number_8, Number_9,Number_10, Number_11, Number_12, Number_13,
	Number_14, Number_15, Number_16, Number_17
	FROM
	(
	SELECT idnumber, number,concat('Number_' , cast(RN AS VARCHAR)) as number_rank
	FROM dev_contactinfo.dbo.ranked_nums
	)r
	PIVOT
	(
		MAX(number)
		FOR number_rank in (Number_1, Number_2,Number_3, Number_4, Number_5,
	Number_6, Number_7, Number_8, Number_9,Number_10, Number_11, Number_12, Number_13,
	Number_14, Number_15, Number_16, Number_17)
	)piv


	CREATE TABLE dev_contactinfo.dbo.#piv_scores WITH (CLUSTERED COLUMNSTORE INDEX,DISTRIBUTION=HASH(idnumber))
	AS
	SELECT idnumber,RankScore_1, RankScore_2,RankScore_3, RankScore_4, RankScore_5,
	RankScore_6,RankScore_7, RankScore_8, RankScore_9,RankScore_10,RankScore_11, RankScore_12, RankScore_13,
	RankScore_14,RankScore_15, RankScore_16, RankScore_17
	FROM
	(
	SELECT idnumber, rank_score,
	 concat('RankScore_' , cast(RN AS VARCHAR)) as score_rank
	FROM dev_contactinfo.dbo.ranked_nums
	)r
	PIVOT
	(
		MAX(rank_score)
		FOR score_rank in (RankScore_1,RankScore_2,RankScore_3, RankScore_4,RankScore_5,
	RankScore_6,RankScore_7, RankScore_8, RankScore_9,RankScore_10,RankScore_11, RankScore_12, RankScore_13,
	RankScore_14,RankScore_15, RankScore_16, RankScore_17)
	)piv
	
	if object_id(%str(%'Prd_ContactInfo.dbo.BNM_New_Best_Numbers_&tday_%'),'u') is not null
	drop table Prd_ContactInfo.dbo.BNM_New_Best_Numbers_&tday_
	CREATE TABLE Prd_ContactInfo.dbo.BNM_New_Best_Numbers_&tday_ WITH (CLUSTERED COLUMNSTORE INDEX,DISTRIBUTION=HASH(idnumber))
AS
	SELECT 
		a.idnumber, RankScore_1, Number_1,
		RankScore_2, Number_2, RankScore_3, Number_3,
		RankScore_4, Number_4, RankScore_5, Number_5,RankScore_6, Number_6,
		RankScore_7, Number_7,RankScore_8, Number_8,RankScore_9, Number_9,
		RankScore_10, Number_10,RankScore_11, Number_11,RankScore_12, Number_12
		, c.number as Best_Mobile

	FROM #piv_nums a
	LEFT JOIN #piv_scores b
	on a.idnumber = b.idnumber 
	left join #top_mobi c
	on a.idnumber = c.idnumber


)by APS;
quit;


proc sql stimer feedback;
connect to ODBC as APS (dsn=mpwaps);
execute (

	if object_id(%str(%'dev_contactinfo.dbo.BNM_New_Best_Numbers_&tday_._3%'),'u') is not null
	drop table dev_contactinfo.dbo.BNM_New_Best_Numbers_&tday_._3
	CREATE TABLE dev_contactinfo.dbo.BNM_New_Best_Numbers_&tday_._3 WITH (CLUSTERED COLUMNSTORE INDEX,DISTRIBUTION=HASH(idnumber))
AS
Select a.IDNumber,b.ClientNumber,
a.RankScore_1,a.Number_1,a.RankScore_2,a.Number_2,a.RankScore_3,a.Number_3,a.RankScore_4,a.Number_4,a.RankScore_5,a.Number_5,a.RankScore_6,a.Number_6,a.RankScore_7,a.Number_7,a.RankScore_8,a.Number_8,a.RankScore_9,a.Number_9,a.RankScore_10,a.Number_10,a.RankScore_11,a.Number_11,a.RankScore_12,a.Number_12,a.Best_Mobile
from Prd_ContactInfo.dbo.BNM_New_Best_Numbers_&tday_ as A
Left Join  (select distinct idnumber, clientnumber from dev_contactinfo.dbo.bnm_fin_nums) as B
On a.IDNumber = b.IDNumber
Where a.Idnumber is not null;


 )by APS;
 quit;


/*these would be ids from BNM_Best_Numbers which arent in todays scores @Mpho*/
Proc Sql;
Create Table cred_scoring_best_OM as 
Select * from CredScr.BNM_Best_Numbers_OM
Where IDNumber not in ( Select IDnumber from dev_cont.BNM_New_Best_Numbers_&tday_._3);
Quit;

/*why are we appending everything back in? old plus current base? @Mpho*/

Proc Append Base=cred_scoring_best_OM data=dev_cont.BNM_New_Best_Numbers_&tday_._3 force;
Run;


/************************************AB*************************************/

proc sql stimer feedback;
     connect to ODBC (dsn=ETLScratch);
     execute (
           truncate table cred_scoring.dbo.BNM_Best_Numbers_V2 

     ) by odbc;
quit;

proc sql;  
	
     insert into CredScr.BNM_Best_Numbers_V2 (bulkload=yes)
     select IDNumber,ClientNumber,
RankScore_1,Number_1,RankScore_2,Number_2,RankScore_3,
Number_3,RankScore_4,Number_4,RankScore_5,Number_5,
RankScore_6,Number_6,RankScore_7,Number_7,RankScore_8,
Number_8,RankScore_9,Number_9,RankScore_10,Number_10,
RankScore_11,Number_11,RankScore_12,Number_12,Best_Mobile
     from dev_cont.BNM_New_Best_Numbers_&tday_._3;

quit;


proc sql stimer feedback;
     connect to ODBC (dsn=ETLScratch);
     execute (
           truncate table cred_scoring.dbo.BNM_Best_Numbers_OM 

     ) by odbc;
quit;

proc sql;  

     insert into CredScr.BNM_Best_Numbers_OM (bulkload=yes)
     select *
     from cred_scoring_best_OM;

quit;



/*Adding the simulation componenet to it*/
proc sql stimer feedback;
connect to ODBC as APS (dsn=mpwaps);
execute (
	if object_id('Prd_ContactInfo.dbo.BNM_ranked_nums_MT') is not null
	drop table Prd_ContactInfo.dbo.BNM_ranked_nums_MT
	CREATE TABLE Prd_ContactInfo.dbo.BNM_ranked_nums_MT WITH (CLUSTERED COLUMNSTORE INDEX,DISTRIBUTION=HASH(idnumber))
AS
select *, rank_score/1000 as score
from dev_contactinfo.dbo.numbers_rank_score_&tday_
)by APS;
quit;

/*end time*/
data _NULL_;

    Call symput("Complete_time",Put(time(),tod5.));

run;
%put &Complete_time;
