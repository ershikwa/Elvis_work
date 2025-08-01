%include "H:\Process_Automation\sas_autoexec\sas_autoexec.sas";

%let projectcode =H:\Process_Automation\Codes;

%let runmonth = %scan(&sysparm,1);
%let process_number = %scan(&sysparm,2);
%let process =  %scan(&sysparm,3);

%let project =pj;

libname &project "&process";

%start_program;

options compress =  yes ;
libname bnmdata '\\MPWSAS65\BNM\BNM_Data';

proc sql stimer;
    connect to ODBC (dsn=APS);
    create table BHT_Dialler_BCP_Numbers as 
    select * from connection to odbc (select ClientNo as clientnumber     ,IDNumber,
     Cycle_1_Number as Cycle1,
     Cycle_2_Number  as Cycle2,
     Cycle_3_Number as Cycle3,
     Cycle_4_Number as Cycle4,
     Cycle_5_Number  as Cycle5,
     Cycle_6_Number as Cycle6,
     Cycle_7_Number as Cycle7,
     Cycle_8_Number as Cycle8, 
     Cycle_9_Number as Cycle9,  
     Cycle_10_Number as Cycle10,
     Cycle_11_Number as Cycle11,
     Cycle_12_Number  as Cycle12 
     from [UNIZA\BTuckerAdmin].BHT_Dialler_BCP_Numbers ) ;
     disconnect from odbc ;
quit;
/**/
/*proc sql stimer;*/
/*    connect to ODBC (dsn=ETLSCRATCH);*/
/*      create table BNM_Best_Numbers as */
/*    select * from connection to odbc ( select * from Cred_Scoring.[dbo].[BNM_Best_Numbers]  ) ;*/
/*disconnect from odbc ;*/
/*quit;*/

/*proc sql stimer;*/
/*    connect to ODBC (dsn=MPWAPS);*/
/*      create table BNM_Best_Numbers as */
/*    select * from connection to odbc ( select * from scoring.dbo.bnm_pilot_best_num_20200429  ) ;*/
/*disconnect from odbc ;*/
/*quit;*/

/*We tranpose the BNM Best numebr table so that we can rerank the numbers*/
/**/
/*DATA bnm_data_flip (KEEP=idnumber clientnumber Number RankScore Best_Mobile);*/
/*SET BNM_Best_Numbers;*/
/*     ARRAY Numbers{12} Number_1-Number_12;*/
/*     ARRAY RankScores{12} RankScore_1-RankScore_12; */
/**/
/*     do i = 1 to 12 ;*/
/*     Number = Numbers{i}    ;*/
/*     RankScore = RankScores{i};*/
/*     OUTPUT;*/
/*     end;*/
/*RUN;*/
/**/
/*proc sql;*/
/*     create table tb_flag as*/
/*     select a.*, case when a.clientnumber = b.clientnumber and a.Number = b.Businesstelephone then 1 else 0 end as flag*/
/*     from bnm_data_flip a*/
/*     left join bnmdata.work_numbers_delete b*/
/*     on a.clientnumber = b.clientnumber*/
/*     where a.number <> '';*/
/*quit;*/
/**/
/*data bnm_nums_no_work(drop= flag);*/
/*     set tb_flag;*/
/*     if flag = 1 then do;*/
/*     number = '';*/
/*     rankscore = .;*/
/*     end;*/
/*     else do;*/
/*     number = number;*/
/*     rankscore= rankscore;*/
/*     end;*/
/*run;*/
/**/
/*proc sort data=bnm_nums_no_work;*/
/*     by idnumber clientnumber descending rankscore descending number best_mobile;*/
/*run;*/
/**/
/*data ranked_nums;*/
/*     set bnm_nums_no_work;*/
/*     by idnumber clientnumber descending rankscore descending number best_mobile;*/
/*     if first.clientnumber then rn =1;*/
/*     else rn +1;*/
/*     if rn <= 12;*/
/*run;*/

/*Flipping our data again to get our best numbers table*/
/**/
/*proc transpose data=ranked_nums  prefix= Number_ out=test(drop= _name_ _label_);*/
/*     by idnumber clientnumber best_mobile;*/
/*     id rn;*/
/*     idlabel rn;*/
/*     var number ;*/
/*run;*/
/**/
/*proc transpose data=ranked_nums  prefix= RankScore_ out=test2_(drop= _name_ _label_);*/
/*     by idnumber ClientNumber best_mobile;*/
/*     id rn;*/
/*     idlabel rn;*/
/*     var rankscore ;*/
/*run;*/
/**/
/*data fin_bnm_table;*/
/*     merge test test2_;*/
/*     by idnumber clientnumber best_mobile;*/
/*run;*/

proc sql stimer;
    connect to ODBC (dsn=APS);
      create table Col as 
    select * from connection to odbc ( select *  from scoring.dbo.bnm_all_ids  ) ;
disconnect from odbc ;
quit;

/*proc sort data=fin_bnm_table nodupkey ;*/
/*by   IDNumber clientnumber ;*/
/*run;*/

proc sort data=BHT_Dialler_BCP_Numbers nodupkey ;
by    idnumber Clientnumber;
run;

proc sort data=Col nodupkey ;
by   IDNumber Clientnumber;
run;
/**/
/*data BNM_Best_Numbers2 ;*/
/*merge fin_bnm_table (in=a)  BHT_Dialler_BCP_Numbers (in=b) col (in=c) ;*/
/*z1 = int(1 +11*uniform(7));*/
/*by    idnumber  ;*/
/*if a  ;*/
/**/
/*if a = 1 and b=1 then do ;*/
/**/
/*NewStrategy = 1 ;*/
/**/
/*Num1 = Number_1 ;*/
/*Num2 = Number_2;*/
/*Num3 = Number_3;*/
/*RS1 =Rankscore_1;*/
/*RS2 =Rankscore_2;*/
/*RS3 =Rankscore_3;*/
/*ARRAY Cycle{12} Cycle1 - Cycle12; */
/*ARRAY Number{12} Number_1-Number_12; */
/*ARRAY Rank{12} Rankscore_1-Rankscore_12; */
/*Array Check{12} Check1 - Check12 ;*/
/**/
/*do i = 1 to 11 ;*/
/*if cycle(i) = 1 then do ;*/
/*Number(i) = Num1 ;*/
/*Rank(i) = RS1;*/
/*end;*/
/*else if cycle(i) = 2 then do ;*/
/*Number(i) = Num2 ;*/
/*Rank(i) = RS2;*/
/*end;*/
/*else if cycle(i) = 3 then do ;*/
/*Number(i) = Num3 ;*/
/*Rank(i) = RS3;*/
/*end;*/
/**/
/*else if cycle(i) = 0 then do ;*/
/*Number(i) = '' ;*/
/*Rank(i) = .;*/
/*end;*/
/**/
/*end;*/
/*Number(12) = Num1;*/
/*Rank(12) = RS1 ; */
/**/
/*do i = 1 to 12 ;*/
/*if Number(i) = num1 then Check(i) = 1 ;*/
/*else if Number(i) = num2 then Check(i) = 2 ;*/
/*else if Number(i) = num3 then Check(i) = 3 ;*/
/*else Check(i) = 0 ;*/
/*end;*/
/*end;*/
/*else NewStrategy = 0 ;*/
/**/
/*count = 1 ;*/
/**/
/*if b = 0 and c = 1 then do ;*/
/*temp = 1 ;*/
/*do i = 1 to 11 ;*/
/*if cycle(i) = z1 then do ;*/
/*Number(i) = Num1 ;*/
/*Rank(i) = RS1;*/
/*end;*/
/*else do;*/
/*Number(i) = '';*/
/*Rank(i) = . ;*/
/*end;*/
/*end;*/
/*end;*/
/**/
/*run;*/
/**/
/*data BNM_Best_Numbers2 ;*/
/*set BNM_Best_Numbers2 ;*/
/*keep IDNumber*/
/*ClientNumber*/
/*RankScore_1*/
/*Number_1*/
/*RankScore_2*/
/*Number_2*/
/*RankScore_3*/
/*Number_3*/
/*RankScore_4*/
/*Number_4*/
/*RankScore_5*/
/*Number_5*/
/*RankScore_6*/
/*Number_6*/
/*RankScore_7*/
/*Number_7*/
/*RankScore_8*/
/*Number_8*/
/*RankScore_9*/
/*Number_9*/
/*RankScore_10*/
/*Number_10*/
/*RankScore_11*/
/*Number_11*/
/*RankScore_12*/
/*Number_12*/
/*Best_Mobile ;*/
/*run;*/
/**/
/*proc sql;*/
/*     create table Best_nums_all as*/
/*     select a.*,*/
/*     case when a.number_12 <> '' then b.number_1 else b.number_1 end as number12,*/
/*     case when a.rankscore_12 <> . then b.rankscore_1 else b.rankscore_1 end as rankscore12,*/
/*     case when a.number_11 <> '' then b.number_2 else b.number_2 end as number11,*/
/*     case when a.rankscore_11 <> . then b.rankscore_2 else b.rankscore_2 end as rankscore11,*/
/*     case when a.number_10 <> '' then b.number_3 else b.number_3 end as number10,*/
/*     case when a.rankscore_10 <> . then b.rankscore_3 else b.rankscore_3 end as rankscore10*/
/*     from BNM_Best_Numbers2 a*/
/*     left join bnmdata.fin_bnm_table   b*/
/*     on a.clientnumber = b.clientnumber and a.idnumber = b.idnumber;*/
/*quit;*/
/**/
/*data cred_scoring_best;*/
/*     set Best_nums_all;*/
/*     drop number_12 rankscore_12 number_11 rankscore_11 number_10 rankscore_10;*/
/*     rename number12 = Number_12;*/
/*     rename number11 = Number_11;*/
/*     rename number10 = Number_10;*/
/*     rename rankscore12 = RankScore_12;*/
/*     rename rankscore11 = RankScore_11;*/
/*     rename rankscore10 = RankScore_10;*/
/*run;*/


/*Table to be uploaded to the server to replace the BNM Table */

/*Find Source
/*filename macros1 '\\neptune\credit$\AA_GROUP CREDIT\Scoring\SAS Macros';
options sasautos = (sasautos  macros1);*/

libname credscor odbc dsn=cre_scor schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

data _null_;
     call symput('tday_',put(intnx('day',today(),1),yymmddn8.));
run;

%put &tday_;
/**/
/*proc sql stimer feedback;*/
/*     connect to ODBC (dsn=ETLScratch);*/
/*     execute (*/
/*           truncate table cred_scoring.dbo.BNM_Best_Numbers */
/**/
/*     ) by odbc;*/
/*quit;*/
/**/
/*proc sql stimer;*/
/*    connect to ODBC (dsn=ETLSCRATCH);*/
/*      create table BNM_Best_Nums as */
/*    select * from connection to odbc ( select * from cred_scoring.dbo.BNM_Best_Numbers ) ;*/
/*disconnect from odbc ;*/
/*quit;*/
/**/
/*proc append base=BNM_Best_Nums data=cred_scoring_best force; run;*/
/**/
/*data BNM_Best_Numbers_&tday_;*/
/*     set BNM_Best_Nums;*/
/*run;*/
/**/
/*proc sql;  */
/**/
/*     insert into credscor.BNM_Best_Numbers (bulkload=yes)*/
/*     select **/
/*     from BNM_Best_Numbers_&tday_;*/
/*quit;*/

/*Proc Sql;*/
/*Connect to ODBC(DSN=Cred_Scoring);*/
/*Execute(CREATE CLUSTERED INDEX [ClusteredIndex_IDNumber] ON [dbo].[BNM_Best_Numbers]([IDNumber] ASC)) by ODBC;*/
/*Disconnect from ODBC;*/
/*Quit;*/


/**/
/*data BNM_Best_Numbers;*/
/*     set BNM_Best_Numbers_&tday_;*/
/*run;*/
/**/
/*%SQLDEL_APS(PRD_ContactInfo.dbo.BNM_Best_Numbers_&tday_);*/
/**/
/*%SQLDEL_APS(PRD_ContactInfo.dbo.BNM_Best_Numbers);*/
/**/
/*%Upload_APS(Set = BNM_Best_Numbers_&tday_, Server = work, APS_ODBC = PRD_ContactInfo, APS_DB = PRD_ContactInfo, Distribute = HASH(Idnumber));*/
/**/
/*%Upload_APS(Set = BNM_Best_Numbers, Server = work, APS_ODBC = PRD_ContactInfo, APS_DB = PRD_ContactInfo, Distribute = HASH(Idnumber));*/


/**/
/*proc sql stimer;*/
/*    connect to ODBC (dsn=MPWAPS);*/
/*   create table BNM_Best_Numbers_20200407 as */
/*   select * from connection to odbc ( select * from PRD_ContactInfo.dbo.BNM_Best_Numbers_20200407  ) ;*/
/*   disconnect from odbc ;*/
/*quit;*/
/**/
/*proc sql;  */
/**/
/*     create table credscor.BNM_Best_Numbers_20200407 (bulkload=yes) as*/
/*     select **/
/*     from BNM_Best_Numbers_20200407;*/
/**/
/*quit;*/

/*Pulling the BNM CIC Table data*/

proc sql stimer;
    connect to ODBC (dsn=ETLSCRATCH);
      create table BNM_Best_Numbers_CIC as 
    select * from connection to odbc ( select * from Cred_Scoring.[dbo].[BNM_Best_Numbers_CIC] ) ;
disconnect from odbc ;
quit;

/*proc sql stimer;*/
/*    connect to ODBC (dsn=MPWAPS);*/
/*      create table BNM_Best_Numbers_CIC as */
/*    select * from connection to odbc ( select * from scoring.dbo.BNM_Best_Numbers_20200508 ) ;*/
/*disconnect from odbc ;*/
/*quit;*/

/*We tranpose the BNM Best numebr table so that we can rerank the numbers*/

DATA bnm_data_flip_cic (KEEP=idnumber clientnumber Number RankScore Best_Mobile);
SET BNM_Best_Numbers_CIC;
     ARRAY Numbers{12} Number_1-Number_12;
     ARRAY RankScores{12} RankScore_1-RankScore_12; 

     do i = 1 to 12 ;
     Number = Numbers{i}    ;
     RankScore = RankScores{i};
     OUTPUT;
     end;
RUN;

proc sql;
     create table tb_flag_cic as
     select a.*, case when a.clientnumber = b.clientnumber and a.Number = b.Businesstelephone then 1 else 0 end as flag
     from bnm_data_flip_cic a
     left join bnmdata.work_numbers_delete b
     on a.clientnumber = b.clientnumber
     where a.number <> '';
quit;

data bnm_nums_no_work_cic(drop= flag);
     set tb_flag_cic;
     if flag = 1 then do;
     number = '';
     rankscore = .;
     end;
     else do;
     number = number;
     rankscore= rankscore;
     end;
run;

proc sort data=bnm_nums_no_work_cic;
     by idnumber clientnumber descending rankscore descending number best_mobile;
run;

data ranked_nums_cic;
     set bnm_nums_no_work_cic;
     by idnumber clientnumber descending rankscore descending number best_mobile;
     if first.clientnumber then rn =1;
     else rn +1;
     if rn <= 12;
run;

/* Code to delete the number that is duplicated in best_mobile */
/*data RANKED_NUMS_CIC;*/
/*	set RANKED_NUMS_CIC;*/
/*	if best_mobile = '0609009335' then delete;*/
/*run;*/

/*Flipping our data again to get our best numbers table*/


proc transpose data=ranked_nums_cic  prefix= Number_ out=test_cic(drop= _name_ _label_);
     by idnumber clientnumber best_mobile;
     id rn;
     idlabel rn;
     var number ;
run;

proc transpose data=ranked_nums_cic  prefix= RankScore_ out=test2_cic(drop= _name_ _label_);
     by idnumber ClientNumber best_mobile;
     id rn;
     idlabel rn;
     var rankscore ;
run;

data fin_bnm_table_cic;
     merge test_cic test2_cic;
     by idnumber clientnumber best_mobile;
run;

proc sort data=fin_bnm_table_cic nodupkey ;
by   IDNumber clientnumber ;
run;

data BNM_Best_Numbers2_cic ;
merge fin_bnm_table_cic (in=a)  BHT_Dialler_BCP_Numbers (in=b) col (in=c) ;
z1 = int(1 +11*uniform(7));
by    idnumber clientnumber;
if a  ;

if a = 1 and b=1 then do ;

NewStrategy = 1 ;

Num1 = Number_1 ;
Num2 = Number_2;
Num3 = Number_3;
RS1 =Rankscore_1;
RS2 =Rankscore_2;
RS3 =Rankscore_3;
ARRAY Cycle{12} Cycle1 - Cycle12; 
ARRAY Number{12} Number_1-Number_12; 
ARRAY Rank{12} Rankscore_1-Rankscore_12; 
Array Check{12} Check1 - Check12 ;

do i = 1 to 11 ;
if cycle(i) = 1 then do ;
Number(i) = Num1 ;
Rank(i) = RS1;
end;
else if cycle(i) = 2 then do ;
Number(i) = Num2 ;
Rank(i) = RS2;
end;
else if cycle(i) = 3 then do ;
Number(i) = Num3 ;
Rank(i) = RS3;
end;

else if cycle(i) = 0 then do ;
Number(i) = '' ;
Rank(i) = .;
end;

end;
Number(12) = Num1;
Rank(12) = RS1 ; 

do i = 1 to 12 ;
if Number(i) = num1 then Check(i) = 1 ;
else if Number(i) = num2 then Check(i) = 2 ;
else if Number(i) = num3 then Check(i) = 3 ;
else Check(i) = 0 ;
end;
end;
else NewStrategy = 0 ;

count = 1 ;

if b = 0 and c = 1 then do ;
temp = 1 ;
do i = 1 to 11 ;
if cycle(i) = z1 then do ;
Number(i) = Num1 ;
Rank(i) = RS1;
end;
else do;
Number(i) = '';
Rank(i) = . ;
end;
end;
end;

run;

data BNM_Best_Numbers2_cic ;
set BNM_Best_Numbers2_cic ;
keep IDNumber
ClientNumber
RankScore_1
Number_1
RankScore_2
Number_2
RankScore_3
Number_3
RankScore_4
Number_4
RankScore_5
Number_5
RankScore_6
Number_6
RankScore_7
Number_7
RankScore_8
Number_8
RankScore_9
Number_9
RankScore_10
Number_10
RankScore_11
Number_11
RankScore_12
Number_12
Best_Mobile ;
run;

proc sql;
     create table Best_nums_cic as
     select a.*,
     case when a.number_12 <> '' then b.number_1 else b.number_1 end as number12,
     case when a.rankscore_12 <> . then b.rankscore_1 else b.rankscore_1 end as rankscore12,
     case when a.number_11 <> '' then b.number_2 else b.number_2 end as number11,
     case when a.rankscore_11 <> . then b.rankscore_2 else b.rankscore_2 end as rankscore11,
     case when a.number_10 <> '' then b.number_3 else b.number_3 end as number10,
     case when a.rankscore_10 <> . then b.rankscore_3 else b.rankscore_3 end as rankscore10
     from BNM_Best_Numbers2_cic a
     left join bnmdata.fin_bnm_table_cic   b
     on a.clientnumber = b.clientnumber and a.idnumber = b.idnumber;
quit;

data cred_scoring_best_cic;
     set Best_nums_cic;
     drop number_12 rankscore_12 number_11 rankscore_11 number_10 rankscore_10;
     rename number12 = Number_12;
     rename number11 = Number_11;
     rename number10 = Number_10;
     rename rankscore12 = RankScore_12;
     rename rankscore11 = RankScore_11;
     rename rankscore10 = RankScore_10;
run;



/*Table to be uploaded to the server to replace the BNM CIC Table */

proc sql stimer feedback;
     connect to ODBC (dsn=ETLScratch);
     execute (
           truncate table cred_scoring.dbo.BNM_Best_Numbers_CIC 

     ) by odbc;
quit;

proc sql stimer;
    connect to ODBC (dsn=ETLSCRATCH);
      create table BNM_Best_Nums_cic as 
    select * from connection to odbc ( select * from cred_scoring.dbo.BNM_Best_Numbers_CIC ) ;
disconnect from odbc ;
quit;

proc append base=BNM_Best_Nums_cic data=cred_scoring_best_cic force; run;

data BNM_Best_Numbers_CIC_&tday_;
     set BNM_Best_Nums_cic;
run;

proc sql;  

     insert into credscor.BNM_Best_Numbers_CIC (bulkload=yes)
     select *
     from BNM_Best_Numbers_CIC_&tday_;

quit;

data BNM_Best_Numbers_CIC;
     set BNM_Best_Numbers_CIC_&tday_;
run;


%SQLDEL_APS(PRD_ContactInfo.dbo.BNM_Best_Numbers_CIC_&tday_);

%SQLDEL_APS(PRD_ContactInfo.dbo.BNM_Best_Numbers_CIC);

%Upload_APS_A(Set = BNM_Best_Numbers_CIC_&tday_, Server = work, APS_ODBC = PRD_ContactInfo, APS_DB = PRD_ContactInfo, Distribute = HASH(Idnumber));

%Upload_APS_A(Set = BNM_Best_Numbers_CIC, Server = work, APS_ODBC = PRD_ContactInfo, APS_DB = PRD_ContactInfo, Distribute = HASH(Idnumber));

/* Insert into BNM_Best_Numbers_CIC_Restore table (for Restore Strategy) */
proc sql stimer feedback;
     connect to ODBC (dsn=ETLScratch);
     execute (
           truncate table cred_scoring.dbo.BNM_Best_Numbers_CIC_Restore 
     ) by odbc;
quit;

proc sql stimer feedback;
     connect to ODBC (dsn=ETLScratch);
     execute (
           Insert Into cred_scoring.dbo.BNM_Best_Numbers_CIC_Restore
		   Select * from cred_scoring.dbo.BNM_Best_Numbers_CIC
     ) by odbc;
quit;

filename macros2 'H:\Process_Automation\macros';
options sasautos = (sasautos  macros2);

%end_program(&process_number);
