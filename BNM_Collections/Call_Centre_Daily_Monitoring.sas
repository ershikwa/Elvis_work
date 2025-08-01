/*12/04/2023*/
/*******************************************************************/
/* Process Name:    Call_Centre_monitoring                         */
/* BusinessPurpose  :This code is used to create the tables for    */
/*               monitoring the call center collections performance*/
/* Developer        :Kelebogile Mbedzi                             */
/*                                                                 */
/* Owner            :Bevan Tucker
/*                  :Siphephelo Gcabashe  
/*                  :Dustin Marcus                                    */
/*                  :Phumudzo Neluheni   
                        */
/* Created on:      2021-07-14                                     */
/*                                                                  */
/*------------------------------------------------------------------*/
/* Dependencies   : Coldata, Coll_ActvRate                          */
/* Inputs Dataset : indataset                                        */
/*                                                                   */
/* Column name    : columnlist=                                      */
/*                                                                   */
/* Outputs:       : KM1_CIC_PTP_&month._Reduced_Fields_DeDuped       */
/*                                                                   */
/********************************************************************/


*Change location - new libname needed for 2nd DB;
libname Prd_Cont odbc dsn = Prd_Cont schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
libname Prd_CoSt odbc dsn = Prd_CoSt schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
Options compress=yes;
 
/*data _null_;*/
/*     call symput("two_month", put(intnx("month", today(),-2,'end'),yymmn6.));*/
/*     call symput("previous", put(intnx("month", today(),-1,'end'),yymmn6.));*/
/*     call symput("month", put(intnx("day", today(),-2),yymmn6.));*/
/*     call symput("month_year", cats("'",put(intnx("month", today(),-0,'end'),YYMMD8.),"'"));*/
/*      call symput("month_years", cats("'",put(intnx("month", today(),-2,'end'),YYMMD8.),"'"));*/
/*     call symput("Day", put(intnx("day", today(),-1,'end'),yymmdd10.));*/
/*     call symput("month_yr", cats("'",put(intnx("month", today(),-0,'end'),YYMMS8.),"'"));*/
/*run;*/
 
data _null_;
	call symput("two_month", put(intnx("month", today(),-2,'end'),yymmn6.));
	call symput("previous", put(intnx("month", today(),-1,'end'),yymmn6.));
	call symput("month", put(intnx("day", today(),-1),yymmn6.));
	call symput("month_year", cats("'",put(intnx("day", today(),-1,'end'),YYMMD8.),"'"));
	/* call symput("month_years", cats("'",put(intnx("month", today(),-8,'end'),YYMMD8.),"'")); */
	call symput("Day", put(intnx("day", today(),-1,'end'),yymmdd10.));
	call symput("Today", put(intnx("day", today(),0),yymmddn8.));
	call symput("month_yr", cats("'",put(intnx("month", today(),-0,'end'),YYMMS8.),"'"));
run;


%put &previous.;
%put &month_year.;
/* Proc Sql;
    Connect to ODBC(DSN=MPWAPS);
    Create Table Max_Agent_Date as 
    Select * from connection to ODBC
    (
		Select max(Month) as Month from Prd_ContactInfo.dbo.BHT_CCAgentList
    );
    Disconnect from ODBC;
Quit;
    
proc sql Noprint;
	select Month as Month
		into :Month_Years separated by ","
			from Max_Agent_Date;
quit;
 
%Put &Month_Years; */
 /*
%macro findit;                                 
    %if %sysfunc(exist(Prd_Cont.KMD_CIC_&month.)) %then %do;  
        %put The file KMD_CIC_&month. exists. ;
        %global currentfile;
        %let currentfile = KMD_CIC_&month.;
    %end; 
    %else %do;
        %put The file KMD_CIC_&month. does not exist.;
        %global currentfile;
        %let currentfile = KMD_CIC_&previous.;
    %end;
%mend;     
 
%findit;
 
%put &currentfile;
 
%macro char(month_year,number);
    %put month_year&number "&month_year";
%mend;
%macro char2(month_yr,number);
    %put month_yr&number "&month_yr";
%mend;
 */
%put &month;
%put &previous;
%put &two_month;
%put &month_year;
%put &month_yr;
%put &Day;
 
*%include "\\neptune\credit$\AA_GROUP CREDIT\Scoring\SAS Macros\SQLDEL_APS.sas";
/*%include "\\neptune\credit$\AA_GROUP CREDIT\Scoring\SAS Macros\Upload_APS_A.sas";*/
*%include "\\neptune\credit$\AA_GROUP CREDIT\Scoring\SAS Macros\Upload_APS_A.sas";
/*libname APS odbc dsn = Scoring schema=dbo connection=unique direct_sql=yes preserve_tab_names=yes;*/

 /*
%macro remove(inputdatase);
     proc delete data=Prd_Cont.&inputdatase.;
%mend;
%remove(inputdatase= KMD_CIC_&month.);
%remove(inputdatase= KMD_CIC_&month._DeDupe);
%remove(inputdatase= KMD_CIC_&month._Scores_Activities);
%remove(inputdatase= KMD_PTP_Monitoring_Table_&month.);
%remove(inputdatase= KMD_CIC_PTP_&month.);
%remove(inputdatase= KMD_CIC_PTP_&month._Reduced_Fiel);
 
options compress = yes;*/

/* Prd_ContactInfo.dbo.[KMD_CIC_&month.] */
PROC SQL;
     connect to ODBC as APS (dsn=mpwaps);
     execute (
         create table Prd_ContactInfo.dbo.#KMD_CIC_ with (clustered columnstore index,distribution=hash(CallID_Detail)) as 
                   select  distinct   a.CallID_Detail
                        ,a.InitiatedDate
                        ,a.ConnectedDate
                        ,a.TerminatedDate
                        ,case when ConnectedDate > InitiatedDate             then  datediff(second,ConnectedDate,TerminatedDate)              
                        else 0 end as                                                                                                                   Duration
                        ,datepart(year,a.initiateddate)                            Year
                        ,datepart(month, a.InitiatedDate)                         Month
                        ,datepart(day,a.InitiatedDate)                             Day
                        ,datepart(hour,a.InitiatedDate)                            Hour
                        ,datediff(second, a.InitiatedDate, a.TerminatedDate)                                                            LineDuration
                        ,a.StationId
                        ,a.LocalUserId
                        ,a.LocalNumber
                        ,a.LocalName
                        ,a.RemoteNumber
                        ,a.RemoteNumberFmt
                        ,a.RemoteNumberCallId
                        ,a.RemoteName
                        ,a.CallDurationSeconds
                        ,a.LineDurationSeconds
                        ,a.I3TimeStampGMT
                        ,a.WrapUpCode
                        ,a.CallDirection
                        ,b.I3_RowId
                        ,b.Finishcode
                        ,b.Agentid
                        ,b.Callid
                        ,b.Callidkey
                        ,b.Phonenumber
                        ,b.Callplacedtime
                        ,b.Callconnectedtime
                        ,b.Calldisconnectedtime
                        ,b.Callingmode
                        ,b.LoadDate_History
                        ,c.*                                                                       
                        ,d.Source
                        ,d.RPC as RPC_FinishCode
                from (             
                        select            CallId CallID_Detail, InitiatedDate, ConnectedDate, TerminatedDate
                                    ,case when ConnectedDate > InitiatedDate then  datediff(second,ConnectedDate,TerminatedDate) else 0 end as Duration
                                    ,datepart(year,initiateddate) Year 
                                    ,datepart(month, InitiatedDate) Month
                                    ,datepart(day,InitiatedDate) Day
                                    ,datepart(hour,InitiatedDate) Hour
                                    ,datediff(second, InitiatedDate, TerminatedDate) LineDuration
                                    ,StationId, LocalUserId, LocalNumber , LocalName , RemoteNumber
                                    ,RemoteNumberFmt, RemoteNumberCallId , RemoteName , CallDurationSeconds
                                    ,LineDurationSeconds ,I3TimeStampGMT , WrapUpCode , CallDirection
                                     from edwdw.dbo.CIC_CallDetail
                                      where cast(InitiatedDate as date) = cast((GETDATE()-1) as date) and calldirection = 'Outbound'
                  ) a
 
                left join ( select  I3_RowId, Finishcode, Agentid, Callid, Callidkey, Phonenumber,Callplacedtime, Callconnectedtime, 
                                    Calldisconnectedtime, Callingmode,I3_Identity,Loaddate LoadDate_History     
                                    from edwdw.dbo.CIC_CallHistory 
                                    where cast(callplacedtime as date) = cast((GETDATE()-1) as date) and i3_identity is not null
									and i3_identity !='0'
                 ) b
                 on a.CallID_Detail = b.callidkey
 
                left join ( select *
                            from edwdw.dbo.CIC_ContactList
                            where cast(date_imported as date) = cast((GETDATE()-1) as date) and campaign is not null
                 ) c
                 on b.I3_Identity = c.I3_Identity
 
                left join edwdw.dbo.CIC_Disposition_Definition d
                 on b.FinishCode = d.FinishCode
 

 create table Prd_ContactInfo.dbo.#max_run with (clustered columnstore index,distribution=hash(max_runmonth)) as 
           select max(runmonth) as max_runmonth
		   from prd_collections_strategy.dbo.GCOL_MONTHLY_NEWCOL_UAT 

create table Prd_ContactInfo.dbo.#md_new_col_pop with (clustered columnstore index,distribution=hash(clientno)) as 
	 select clientnumber as clientno,loanref 
	 from prd_collections_strategy.dbo.GCOL_MONTHLY_NEWCOL_UAT a
	 inner join #max_run b
	on a.runmonth = b.max_runmonth

IF OBJECT_ID('Prd_ContactInfo.dbo.MD_RPC_Activities') IS NOT NULL
DROP TABLE Prd_ContactInfo.dbo.MD_RPC_Activities
	create table Prd_ContactInfo.dbo.MD_RPC_Activities with (clustered columnstore index,distribution=hash(callid_Detail)) as 
    select CallID_Detail
     ,InitiatedDate
     ,ConnectedDate
     ,TerminatedDate
     ,Duration
     ,LineDuration
     ,LocalUserId
     ,LocalNumber
     ,LocalName
     ,CallDirection
     ,Finishcode
     ,Agentid
     ,Callid
     ,Callidkey
     ,Phonenumber
     ,CUSTOMER_ID
     ,CLIENTNUMBER
     ,IDNUMBER
     ,a.LOANREFNO
     ,NewNumber
     ,CampaignType
     ,Campaign
     ,LoadDate
     ,NUMBER_1
     ,Source
     ,RPC_FinishCode,ID
           ,ActivityDate
           ,AbsActivityDate
           ,ActivityMonth
           ,UserID
           ,LoginName
           ,UserName
           ,Activity
           ,c.ThirdParty
           ,case when c.Activity = 'Right Party Contact' and duration > 0             then 1
           else 0          end as RPC

                from Prd_ContactInfo.dbo.#KMD_CIC_ a
 
                left join (select * from PRD_COLLECTIONS_STRATEGY.dbo.jc_callcentreagentlist) b                             
                on a.LocalUserId = b.[agent name]
                left join ( select * from PRD_COLLECTIONS_STRATEGY.dbo.useractivity where activitymonth =&month_year and activity = 'Right Party Contact') c
			
                on (select clientno from #md_new_col_pop where loanref = a.loanrefno) = (select clientno from #md_new_col_pop where loanref = c.loanrefno)
                and a.InitiatedDate < c.ActivityDate
                and c.ActivityDate < dateadd(minute,5,a.TerminatedDate)
                and b.Agent = c.loginname
            ;)
           by APS;
     quit;
     

proc sql stimer;
     connect to ODBC (dsn = MPWAPS);
     create table CntInfoTables as
	     select * from connection to odbc 
		 (
			Use Prd_ContactInfo
				SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES order by TABLE_NAME
	      );
	     disconnect from odbc;
quit;

proc sql;
	create table Deduped_Exists as 
	select TABLE_NAME from CntInfoTables where TABLE_NAME like "MD_RPC_Activities_&month";
quit;
	
data _NULL_;
	if 0 then set Deduped_Exists nobs=n;
	call symputx('totobs',n);
	stop;
run;
%put no. of observations in Deduped_Exists = &totobs;

%macro Append(inputDataSet, outputDataSet);
	%if &totobs = 0 %then %do;
		Proc sql stimer;     
		   connect to ODBC as APS (dsn=mpwaps);
		   execute(;  
		        CREATE TABLE Prd_ContactInfo.dbo.&outputDataSet. 
				with (clustered columnstore index,distribution=hash(callid_Detail)) 
				AS
				SELECT *
				FROM Prd_ContactInfo.dbo.&inputDataSet.
		   ) BY APS;
		QUIT;
	%end;
	%else %do;
		Proc sql stimer;     
		   connect to ODBC as APS (dsn=mpwaps);
		   execute(;  
		        insert into Prd_ContactInfo.dbo.&outputDataSet.
		        select *
		        from Prd_ContactInfo.dbo.&inputDataSet.;
		   ) BY APS;
		QUIT;
	%end;
%Mend;

%Append(inputDataSet=MD_RPC_Activities, outputDataSet=MD_RPC_Activities_&month);

