
options compress =  yes ;
libname bnmdata '\\MPWSAS65\BNM\BNM_Data';

*Sleep for 1 hour while letting Dialler Split Strategy code run;
data _null_;
	time_slept=sleep(3600,1);
run;

/*	Check if Cred_Scoring.dbo.KC5_Dialler_Plan_Split exits.
	If it does, use that table. Else, use Prd_ContactInfo.dbo.BHT_Dialler_BCP_Numbers.	*/
proc sql stimer;
	connect to ODBC (dsn = ETLScratch);
	create table CrdScr as
		select * from connection to odbc 
		(
		Use Cred_Scoring
			SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'KC5_Dialler_Plan_Split'
		);
	disconnect from odbc;
quit;

proc sql;
	select count(*) into: TblCntKCDPS from CrdScr;
quit;

%if (&TblCntKCDPS. = 1) %then %do;
	proc sql stimer;
	    connect to ODBC (dsn=ETLScratch);
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
	     from Cred_Scoring.dbo.KC5_Dialler_Plan_Split) ;
	     disconnect from odbc ;
	quit;	
%end;	
%else %do;
	proc sql stimer;
	    connect to ODBC (dsn=MPWAPS);
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
	     from Prd_ContactInfo.dbo.BHT_Dialler_BCP_Numbers ) ;
	     disconnect from odbc ;
	quit;	
%end;	


proc sql stimer;
    connect to ODBC (dsn=MPWAPS);
      create table Col as 
    select * from connection to odbc ( select *  from PRD_ContactInfo.dbo.bnm_all_ids  ) ;
disconnect from odbc ;
quit;
/*source???
Sphe Notes: scoring.dbo.bnm_all_ids 
moved to PRD_ContactInfo (19/07/2021) */

/*proc sort data=fin_bnm_table nodupkey ;*/
/*by   IDNumber clientnumber ;*/
/*run;*/

proc sort data=BHT_Dialler_BCP_Numbers nodupkey ;
by    idnumber Clientnumber;
run;

proc sort data=Col nodupkey ;
by   IDNumber Clientnumber;
run;


/*Table to be uploaded to the server to replace the BNM Table */

/*Find Source
/*filename macros1 '\\neptune\credit$\AA_GROUP CREDIT\Scoring\SAS Macros';
options sasautos = (sasautos  macros1);*/

libname credscor odbc dsn=cre_scor schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

data _null_;
     call symput('tday_',put(intnx('day',today(),1),yymmddn8.));
run;

%put &tday_;

/*Pulling the BNM CIC Table data*/

proc sql stimer;
    connect to ODBC (dsn=ETLSCRATCH);
      create table BNM_Best_Numbers_V2 as 
    select * from connection to odbc ( select * from Cred_Scoring.[dbo].[BNM_Best_Numbers_V2] ) ;
disconnect from odbc ;
quit;

proc sort data=BNM_Best_Numbers_V2 nodupkey ;
by  _all_;
run;


data BNM_Best_Numbers2_cic ;
merge BNM_Best_Numbers_V2 (in=a)  BHT_Dialler_BCP_Numbers (in=b) col (in=c) ;
z1 = int(1 +11*uniform(7));
by    idnumber clientnumber;
if a  ;

if a = 1 and b=1 then do ;

NewStrategy = 1 ;

Num1 = Number_1 ;
Num2 = Number_2;
Num3 = Number_3;
Num4 = Number_4 ;
Num5 = Number_5;
Num6 = Number_6;
Num7 = Number_7 ;
Num8 = Number_8;
Num9 = Number_9;
Num10 = Number_10 ;
Num11 = Number_11;
Num12 = Number_12;
RS1 =Rankscore_1;
RS2 =Rankscore_2;
RS3 =Rankscore_3;
RS4 =Rankscore_4;
RS5 =Rankscore_5;
RS6 =Rankscore_6;
RS7 =Rankscore_7;
RS8 =Rankscore_8;
RS9 =Rankscore_9;
RS10 =Rankscore_10;
RS11 =Rankscore_11;
RS12 =Rankscore_12;
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

if cycle(i) = 4 then do ;
Number(i) = Num4 ;
Rank(i) = RS4;
end;
else if cycle(i) = 5 then do ;
Number(i) = Num5 ;
Rank(i) = RS5;
end;
else if cycle(i) = 6 then do ;
Number(i) = Num6 ;
Rank(i) = RS6;
end;

if cycle(i) = 7 then do ;
Number(i) = Num7 ;
Rank(i) = RS7;
end;
else if cycle(i) = 8 then do ;
Number(i) = Num8;
Rank(i) = RS8;
end;
else if cycle(i) = 9 then do ;
Number(i) = Num9 ;
Rank(i) = RS9;
end;

if cycle(i) = 10 then do ;
Number(i) = Num10 ;
Rank(i) = RS10;
end;
else if cycle(i) = 11 then do ;
Number(i) = Num11 ;
Rank(i) = RS11;
end;
else if cycle(i) = 12 then do ;
Number(i) = Num12 ;
Rank(i) = RS12;
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
     case when a.best_mobile <> '' then b.number_3 else b.number_3 end as best_mobile2
     /*case when a.number_10 <> '' then b.number_3 else b.number_3 end as number10,
     case when a.rankscore_10 <> . then b.rankscore_3 else b.rankscore_3 end as rankscore10*/
     from BNM_Best_Numbers2_cic a
     left join BNM_Best_Numbers_V2   b
     on a.clientnumber = b.clientnumber and a.idnumber = b.idnumber;
quit;

data cred_scoring_best_cic;
     set Best_nums_cic;
     drop number_12 rankscore_12 number_11 rankscore_11 best_mobile/*number_10 rankscore_10*/;
     rename number12 = Number_12;
     rename number11 = Number_11;
     /*rename number10 = Number_10;*/
     rename rankscore12 = RankScore_12;
     rename rankscore11 = RankScore_11;
     /*rename rankscore10 = RankScore_10;*/
     rename best_mobile2 = best_mobile;
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


%SQLDEL_APS(Prd_Cont.dbo.BNM_Best_Numbers_CIC_&tday_);

%SQLDEL_APS(Prd_Cont.dbo.BNM_Best_Numbers_CIC);

%Upload_APS_A(Set = BNM_Best_Numbers_CIC_&tday_, Server = work, APS_ODBC = Prd_Cont, APS_DB = Prd_ContactInfo, Distribute = HASH(Idnumber));

%Upload_APS_A(Set = BNM_Best_Numbers_CIC, Server = work, APS_ODBC = Prd_Cont, APS_DB = Prd_ContactInfo, Distribute = HASH(Idnumber));

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

