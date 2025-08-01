/*******************************************************************/
/* Process Name:    Call_Centre_monitoring                         */
/* BusinessPurpose  :This code is used to create the tables for    */
/*               monitoring the call center collections performance*/
/* Developer        :Kelebogile Mbedzi                             */
/*                                                                 */
/* Owner            :Bevan Tucker                                  */
/*                  :Phumudzo Neluheni                             */
/*                  :Siphe Gcabashe                                */
/*                  :Dustin Marcus                                 */

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

/*DM "log; clear; ";*/
%include "H:\Process_Automation\sas_autoexec\sas_autoexec.sas";

%let projectcode =H:\Process_Automation\Codes;

%let runmonth = %scan(&sysparm,1);
%let process_number = %scan(&sysparm,2);
%let process =  %scan(&sysparm,3);

%let project =pj;

libname &project "&process";

%start_program;

/*Change location - new libname needed for 2nd DB
libname PRD_ContactInfo odbc dsn = PRD_ContactInfo schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
libname PRD_Collections_Strategy odbc dsn = PRD_Collections_Strategy schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

 
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
	call symput("month_year", cats("'",put(intnx("month", today(),-0,'end'),YYMMD8.),"'"));
	call symput("month_years", cats("'",put(intnx("month", today(),-5,'end'),YYMMD8.),"'"));
	call symput("Day", put(intnx("day", today(),-1,'end'),yymmdd10.));
	call symput("Today", put(intnx("day", today(),0),yymmddn8.));
	call symput("month_yr", cats("'",put(intnx("month", today(),-0,'end'),YYMMS8.),"'"));
run;
 
 /
%put &month.;
 
%macro findit;                                 
    %if %sysfunc(exist(PRD_ContactInfo.KMD_CIC_&month.)) %then %do;  
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
Options compress=yes;
 
%macro remove(inputdatase);
     proc delete data=PRD_ContactInfo.&inputdatase.;
%mend;
%remove(inputdatase= KMD_CIC_&month.);
%remove(inputdatase= KMD_CIC_&month._DeDupe);
%remove(inputdatase= KMD_CIC_&month._Scores_Activities);
%remove(inputdatase= KMD_PTP_Monitoring_Table_&month.);
%remove(inputdatase= KMD_CIC_PTP_&month.);
%remove(inputdatase= KMD_CIC_PTP_&month._Reduced_Fiel);
 
options compress = yes;
PROC SQL;
     connect to ODBC as PRD_ContactInfo (dsn=mpwaps);
     execute (
                   create table PRD_ContactInfo.dbo.[KMD_CIC_&month.] with (clustered columnstore index,distribution=hash(CallID_Detail)) as 
                   select     a.CallID_Detail
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
                        ,d.RPC                                                                                     RPC_FinishCode
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
                                      where       datepart(YEAR, InitiatedDate) = YEAR(GETDATE()-0)
                                      and   datepart(month, InitiatedDate) = MONTH(DATEADD(mm, -0, GETDATE())) 
                                    and         datepart(Day, InitiatedDate) = DAY(GETDATE()-1)
                  ) a
 
                left join ( select  I3_RowId, Finishcode, Agentid, Callid, Callidkey, Phonenumber,Callplacedtime, Callconnectedtime, 
                                    Calldisconnectedtime, Callingmode,I3_Identity,Loaddate LoadDate_History     
                                    from edwdw.dbo.CIC_CallHistory 
                                    where datepart(month, callplacedtime) = MONTH(DATEADD(mm, -0, GETDATE()))   
                                    and datepart(Day, callplacedtime) = DAY(GETDATE()-1)
                 ) b
                 on a.CallID_Detail = b.callidkey
 
                left join ( select *
                            from edwdw.dbo.CIC_ContactList
                            where datepart(month, date_imported) = MONTH(DATEADD(mm, -0, GETDATE()))   
                            and datepart(Day, date_imported) = DAY(GETDATE()-1)
                 ) c
                 on b.I3_Identity = c.I3_Identity
 
                left join edwdw.dbo.CIC_Disposition_Definition d
                 on b.FinishCode = d.FinishCode
 
     ;)
     by PRD_ContactInfo;
quit;
 
PROC SQL;
     connect to ODBC as PRD_ContactInfo (dsn=mpwaps);
     execute (
     create table PRD_ContactInfo.dbo.[KMD_CIC_&month._DeDupe] with (clustered columnstore index,distribution=hash(CallID_Detail)) as 
     select CallID_Detail
     ,InitiatedDate
     ,ConnectedDate
     ,TerminatedDate
     ,Duration
     ,Year
     ,Month
     ,Day
     ,Hour
     ,LineDuration
     ,StationId
     ,LocalUserId
     ,LocalNumber
     ,LocalName
     ,RemoteNumber
     ,RemoteNumberFmt
     ,RemoteNumberCallId
     ,RemoteName
     ,CallDurationSeconds
     ,LineDurationSeconds
     ,I3TimeStampGMT
     ,WrapUpCode
     ,CallDirection
     ,I3_RowId
     ,Finishcode
     ,Agentid
     ,Callid
     ,Callidkey
     ,Phonenumber
     ,Callplacedtime
     ,Callconnectedtime
     ,Calldisconnectedtime
     ,Callingmode
     ,LoadDate_History
     ,I3_IDENTITY
     ,ZONE
    ,ATTEMPTS
     ,I3_ATTEMPTSREMOTEHANGUP
     ,I3_ATTEMPTSSYSTEMHANGUP
     ,I3_ATTEMPTSABANDONED
     ,I3_ATTEMPTSBUSY
     ,I3_ATTEMPTSFAX
     ,I3_ATTEMPTSNOANSWER
     ,I3_ATTEMPTSMACHINE
     ,I3_ATTEMPTSRESCHEDULED
     ,I3_ATTEMPTSSITCALLABLE
     ,I3_SITEID
     ,I3_ACTIVEWORKFLOWID
     ,STATUS
     ,CUSTOMER_ID
     ,ACCOUNT_NO
     ,CLIENTNUMBER
     ,REPAYMENTMETHOD
     ,TITLE
     ,FIRSTNAME1
     ,FIRSTNAME2
     ,SURNAME
     ,SALARYDAY
     ,IDNUMBER
     ,CURRENTCONTRACTDELLOAN
     ,EMPLOYER
     ,HOMETEL
     ,WORKTEL
     ,CELLNO
     ,LOANREFNO
     ,REASONCODE
     ,OVERDUEAMOUNT
     ,WORKLIST
     ,LASTPTP
     ,LASTRPC
     ,LASTACTIONDATE
     ,TOTAL_SYSTEM_WRAPS_CURR_MONTH
     ,CALL_TYPE_INDICATOR_HR_QTY
     ,BTTCINDICATOR
     ,AGENT_WRAP_COUNT_LAST_THREE_MONTHS
     ,CLIENTBALANCE
     ,CLIENTARREARS
     ,AGENT_WRAP_COUNT_CURRENT_MONTH
     ,ELLERINES_BRAND_NAME
     ,INSTALMENT
     ,BRANCH_INSTORE
     ,CELLNO2
     ,CELLNO3
     ,CELLNO4
     ,HOMETEL2
     ,HOMETEL3
     ,HOMETEL4
     ,WORKTEL2
     ,WORKTEL3
     ,WORKTEL4
     ,IVRLASTPHONE
     ,BESTPHONENUMBER
     ,PTP_PRPOENSITY_SCOREGROUP
     ,TRANSFER_SCOREGROUP
     ,EARLY_CALLCENTRE_SCOREGROUP
     ,PRODUCTCODE
     ,PERSAL_NO_PERSALINDICATOR
     ,PRELEGAL_INDICATOR
     ,DEFERRED_PRODUCT_IND
     ,DISTRESSED_SCORE_IND
     ,GEOGRAPHIC_INDICATOR
     ,BRANCH_OF_ORIGIN
     ,TERMINAL_FLD_CODE
     ,LOANTYPE_INDICATOR
     ,VALUE_SCORECARD_IND
     ,GENDER
     ,PREF_LANG1
     ,PREF_LANG2
     ,EMPLOYEENO
     ,DEPARTMENT
     ,NOOFLOANS
     ,PTPSTATUS
     ,NOOFBROKENPROMISES
     ,REASONCODEDATE
     ,RANDOMIND
     ,PAYINGPERCENTAGE
     ,DATEOFBIRTH
     ,LASTPAYMENTDATE
     ,NewNumber
     ,Date_Imported
     ,CampaignType
     ,Campaign
     ,Abandoned
     ,LoadDate
     ,NUMBER_1
     ,WC
     ,QUALITY
     ,PROB_1
     ,COMB_1
     ,PROB_2
     ,COMB_2
     ,PROB_3
     ,COMB_3
     ,PROB_4
     ,COMB_4
     ,PROB_5
     ,COMB_5
     ,Source
     ,RPC_FinishCode
     ,count(*)                         Duplicates
     from PRD_ContactInfo.dbo.KMD_CIC_&month.
     group by     CallID_Detail ,InitiatedDate ,ConnectedDate ,TerminatedDate ,Duration ,Year ,Month ,Day ,Hour ,LineDuration ,StationId ,LocalUserId ,LocalNumber
                 ,LocalName, RemoteNumber, RemoteNumberFmt ,RemoteNumberCallId ,RemoteName ,CallDurationSeconds ,LineDurationSeconds ,I3TimeStampGMT ,WrapUpCode
                 ,CallDirection, I3_RowId, Finishcode, I3_ATTEMPTSREMOTEHANGUP, I3_ATTEMPTSSYSTEMHANGUP
     ,I3_ATTEMPTSABANDONED
     ,I3_ATTEMPTSBUSY
     ,I3_ATTEMPTSFAX
     ,I3_ATTEMPTSNOANSWER
     ,I3_ATTEMPTSMACHINE
     ,I3_ATTEMPTSRESCHEDULED
     ,I3_ATTEMPTSSITCALLABLE
     ,I3_SITEID
     ,Agentid
     ,Callid
     ,Callidkey
     ,Phonenumber
     ,Callplacedtime
     ,Callconnectedtime
     ,Calldisconnectedtime
     ,Callingmode
     ,LoadDate_History
     ,I3_IDENTITY
     ,ZONE
     ,ATTEMPTS
    ,I3_ACTIVEWORKFLOWID
     ,STATUS
     ,CUSTOMER_ID
     ,ACCOUNT_NO
     ,CLIENTNUMBER
     ,REPAYMENTMETHOD
     ,TITLE
     ,FIRSTNAME1
     ,FIRSTNAME2
     ,SURNAME
     ,SALARYDAY
     ,IDNUMBER
     ,CURRENTCONTRACTDELLOAN
     ,EMPLOYER
     ,HOMETEL
     ,WORKTEL
     ,CELLNO
     ,LOANREFNO
     ,REASONCODE
     ,OVERDUEAMOUNT
     ,WORKLIST
     ,LASTPTP
     ,LASTRPC
     ,LASTACTIONDATE
     ,TOTAL_SYSTEM_WRAPS_CURR_MONTH
     ,CALL_TYPE_INDICATOR_HR_QTY
     ,BTTCINDICATOR
     ,AGENT_WRAP_COUNT_LAST_THREE_MONTHS
     ,CLIENTBALANCE
     ,CLIENTARREARS
     ,AGENT_WRAP_COUNT_CURRENT_MONTH
     ,ELLERINES_BRAND_NAME
     ,INSTALMENT
     ,BRANCH_INSTORE
     ,CELLNO2
     ,CELLNO3
     ,CELLNO4
     ,HOMETEL2
     ,HOMETEL3
     ,HOMETEL4
     ,WORKTEL2
     ,WORKTEL3
     ,WORKTEL4
     ,IVRLASTPHONE
     ,BESTPHONENUMBER
     ,PTP_PRPOENSITY_SCOREGROUP
     ,TRANSFER_SCOREGROUP
     ,EARLY_CALLCENTRE_SCOREGROUP
     ,PRODUCTCODE
     ,PERSAL_NO_PERSALINDICATOR
     ,PRELEGAL_INDICATOR
     ,DEFERRED_PRODUCT_IND
     ,DISTRESSED_SCORE_IND
     ,GEOGRAPHIC_INDICATOR
     ,BRANCH_OF_ORIGIN
     ,TERMINAL_FLD_CODE
     ,LOANTYPE_INDICATOR
     ,VALUE_SCORECARD_IND
     ,GENDER
     ,PREF_LANG1
     ,PREF_LANG2
     ,EMPLOYEENO
    ,DEPARTMENT
     ,NOOFLOANS
     ,PTPSTATUS
     ,NOOFBROKENPROMISES
     ,REASONCODEDATE
     ,RANDOMIND
     ,PAYINGPERCENTAGE
     ,DATEOFBIRTH
     ,LASTPAYMENTDATE
     ,NewNumber
     ,Date_Imported
     ,CampaignType
     ,Campaign
     ,Abandoned
     ,LoadDate
     ,NUMBER_1
     ,WC
     ,QUALITY
     ,PROB_1
     ,COMB_1
     ,PROB_2
     ,COMB_2
     ,PROB_3
     ,COMB_3
     ,PROB_4
     ,COMB_4
     ,PROB_5
     ,COMB_5
     ,Source
     ,RPC_FinishCode;)
     by PRD_ContactInfo;
quit;
 
options compress = yes;
PROC SQL;
     connect to ODBC as PRD_ContactInfo (dsn=mpwaps);
     execute (
     create table PRD_ContactInfo.dbo.[KMD_CIC_&month._Scores_Activities] with (clustered columnstore index,distribution=hash(callid_Detail)) as 
           select a.*, c.ID
           ,c.ActivityDate
           ,c.AbsActivityDate
           ,c.ActivityMonth
           ,c.UserID
           ,c.LoginName
           ,c.UserName
           ,c.TemplateName
           ,c.TeamName
           ,c.Activity
           ,c.Summary
           ,c.ReasonCode                        ReasonCode_Activity
           ,c.LoanCode1               
           ,c.LoanCode2
           ,c.ProductIndicator
           ,c.Book
           ,c.Segment
           ,c.Route
           ,c.WorkList                                WorkList_Activity
           ,c.ThirdParty
           ,c.AttorneyRegion
           ,case when c.Activity = 'Right Party Contact' and duration > 0             then 1
           else 0          end as RPC
           ,case when ConnectedDate > '2000-01-01 02:00:00.000' and duration >= 10      then 1
           else 0          end as Contact
           ,case when PhoneNumber = HOMETEL      then 'HOMETEL'
                     when PhoneNumber = WORKTEL      then 'WORKTEL'
                     when PhoneNumber = CELLNO       then 'CELLNO'
                     when PhoneNumber = CELLNO2      then 'CELLNO2'
                     when PhoneNumber = CELLNO3      then 'CELLNO3'
                     when PhoneNumber = CELLNO4      then 'CELLNO4'
                     when PhoneNumber = HOMETEL2          then 'HOMETEL2'
                     when PhoneNumber = HOMETEL3          then 'HOMETEL3'
                     when PhoneNumber = HOMETEL4          then 'HOMETEL4'
                     when PhoneNumber = WORKTEL2          then 'WORKTEL2'
                     when PhoneNumber = WORKTEL3          then 'WORKTEL3'
                     when PhoneNumber = WORKTEL4          then 'WORKTEL4'
                     when PhoneNumber = NewNumber    then 'NewNumber'
                     when PhoneNumber = cast(NUMBER_1     as varchar(25))      then 'NUMBER_1'
                end as NumberCalled_SourceField
                from PRD_ContactInfo.dbo.KMD_CIC_&month._DeDupe a
 
/*SOurce???*/
                left join (select * from PRD_ContactInfo.[UNIZA\BTuckerAdmin].BHT_CCAgentList where Month = &month_years) b                             
                on a.LocalUserId = b.[agent name]
 
/*Source???*/
                left join ( select * from scoring.dbo.useractivity where activitymonth = &month_year and activity = 'Right Party Contact') c
				/*Source
                on (select clientno from scoring.dbo.KM1_Coldata_LoanList where loanref = a.loanrefno) = (select clientno from scoring.dbo.KM1_Coldata_LoanList where loanref = c.loanrefno)
                and a.InitiatedDate < c.ActivityDate
                and c.ActivityDate < dateadd(minute,5,a.TerminatedDate)
                and b.Agent = c.loginname
            ;)
           by scoring;
     quit;
 
     
Options compress=yes;
PROC SQL;
     connect to ODBC as PRD_ContactInfo (dsn=mpwaps);
     execute (
             create table PRD_ContactInfo.dbo.[KMD_PTP_Monitoring_Table_&month.] with (clustered columnstore index,distribution=hash(ACTIVITYID)) as 
             select     datepart(day,a.PtpMadeDate)                                                     DayNum
                   ,datediff(day, a.PTPMadeDate, a.PTPDueDate)                                DateDiff_PTP_Due
                   ,cast(PTPMadeDate    as Date)                                              PTPDate
                   ,case when a.amountpaid >= a.Instalment         then 1     
                             else 0                                     end as                          GE_Instalment
                        ,a.PTPMadeDate
                   ,a.PTPMadeMonth
                   ,a.AbsPTPMadeDate
                   ,a.LoanRefNo
                   ,a.Qty
                   ,a.Product
                   ,a.ReasonCode                                                                   ReasonCode_PTP
                   ,a.ChildID
                   ,a.LoginName                                                                    LoginName_PTP
                   ,a.Shift
                   ,a.Summary
                   ,a.Balance                                                                      Balance_PTP
                   ,a.Instalment
                   ,a.InstalmentAgreed
                   ,a.Book                                                                              Book_PTP
                   ,a.Segment                                                                      Segment_PTP
                   ,a.Route                                                                        Route_PTP
                   ,a.WorkList                                                                          WorkList_PTP
                   ,a.ThirdParty                                                                   ThirdParty_PTP
                   ,a.AttorneyRegion                                                               AttorneyRegion_PTP
                   ,a.RepayMethod
                   ,a.PTPCounter
                   ,a.PTPAmount
                   ,a.PTPDueDate
                   ,a.PTPDueMonth
                   ,a.PTPPercInstal
                   ,a.PaidBeforeDate
                   ,a.PaidBeforePTPMade
                   ,a.PaidDate
                   ,a.QtyPaid
                   ,a.QtyPayments
                   ,a.AmountPaid
                   ,a.NewCash
                   ,a.PaidPercentage
                   ,a.Status                                                                       Status_PTP
                   ,a.LastPaymentDate
                   ,a.ACTIVITYID
                   ,b.Loanref                                                                      Loanref_ColData
                   ,b.ClientNo
                   ,b.IDNo
                   ,b.Reasoncode                                                                   ReasonCode_ColData
                   ,b.HomeBranch_Final                                                             HomeBranch_Final_ColData
                   ,b.Homebranch
                   ,b.ReasonCodeDate
                   ,b.GroupCode
                   ,b.STATUS
                   ,b.EvenInstalment
                   ,b.Balance
                   ,b.term
                   ,b.Firstperenddate
                   ,b.LastReceiptDate
                   ,b.startdate
                   ,b.Repaymentmethod
                   ,b.AvailableBalance
                   ,b.PTPAMOUNT                                                         PTPAMOUNT_ColData                                                               
                   ,b.PTPAmount_Made
                   ,b.New_Client
                   ,b.RepaymentMethod_Group
                  ,b.Settled
                   ,b.Brand
                   ,b.NextStrikedate
                   ,b.LastStrikeDate
                   ,b.WageType
                   ,b.SalaryDay
                   ,b.MATURITYDATE
                   ,b.PrincipalDebt
                   ,b.subgroupcode
                   ,b.randomind                                                         randomind_ColData
                   ,b.BookedScoreBand
                   ,b.Failed_Strikes
                   ,b.CNI
                   ,b.LOANID
                   ,b.Runmonth
                   ,b.Age_Of_Account
                   ,b.LoanGroup
                   ,b.Transfer_HomeBranch_Final
                   ,b.Time_In_Transfer_HB_Final
                   ,b.Time_In_HomeBranch_Final
                   ,b.First_Strike_Date_Final
                   ,b.Bank
                   ,c.Loanref
                   ,c.IDNo                                                                                         IDNo_CollActv
                   ,c.HomeBranch_Final
                   ,c.GB_BB_IND
                   ,c.Observation_date
                   ,c.Exclusion_Reason
                   ,c.Excl_Ind
                   ,c.Final_Score
                   ,c.BNM_RankScore_1
                   ,c.Model_Applied
                   ,c.Final_Prob2Actv
                   ,c.Missing
                   ,c.Final_Odds_Score
                   ,c.Scores_Balance
                   ,d.[AGENT NAME]
                   ,d.OPSManager
                   ,d.SNRManager
                   ,d.SupervisorTallymanLogin
                   ,d.OpsManagerTallymanLogin
                   ,d.ConsultantExactusLogin
                   ,d.SupervisorName
                   ,d.Afrikaans
                   ,d.English
                   ,d.Ndebele
                   ,d.Nothern_Sotho
                   ,d.Sesotho
                   ,d.Swazi
                   ,d.Tsonga
                   ,d.Tswana
                   ,d.Venda
                   ,d.Xhosa
                   ,d.Zulu
                   ,d.Ind_CS_CC
                   ,e.ID
                   ,e.ActivityDate
                   ,e.AbsActivityDate
                   ,e.ActivityMonth
                   ,e.UserID
                   ,e.LoginName
                   ,e.UserName
                   ,e.TemplateName
                   ,e.TeamName
                   ,e.LoanRefNo                                                               LoanrefNo_Activity
                   ,e.Activity
                   ,e.Summary                                                                 Summary_Activity
                   ,e.ReasonCode
                   ,e.ProductIndicator
                   ,e.Book
                   ,e.Segment
                   ,e.Route
                   ,e.WorkList
                   ,e.ThirdParty
                   ,e.AttorneyRegion
                   ,case when a.PTPAmount <= 50                               then 'PTPAmount .<= 50'
                        when a.PTPAmount <= 200                              then 'PTPAmount .<= 200'
                        when a.PTPAmount/nullif(Instalment,0) < 0.2      then 'PTPAmount/Instalment < 0.2'
                        when a.PTPAmount/nullif(Instalment,0) < 0.4      then 'PTPAmount/Instalment < 0.4'
                        when a.PTPAmount/nullif(Instalment,0) < 0.6      then 'PTPAmount/Instalment < 0.6'
                        when a.PTPAmount/nullif(Instalment,0) < 0.8      then 'PTPAmount/Instalment < 0.8'
                        when a.PTPAmount/nullif(Instalment,0) < 1             then 'PTPAmount/Instalment < 1'
                        when a.PTPAmount/nullif(Instalment,0) < 1.2      then 'PTPAmount/Instalment < 1.2'
                        when a.PTPAmount/nullif(Instalment,0) < 1.4      then 'PTPAmount/Instalment < 1.4'
                        when a.PTPAmount/nullif(Instalment,0) < 1.6      then 'PTPAmount/Instalment < 1.6'
                        when a.PTPAmount/nullif(Instalment,0) < 1.8      then 'PTPAmount/Instalment < 1.8'
                        when a.PTPAmount/nullif(Instalment,0) < 2             then 'PTPAmount/Instalment < 2'
                        when a.PTPAmount/nullif(Instalment,0) < 2.2      then 'PTPAmount/Instalment < 2.2'
                        when a.PTPAmount/nullif(Instalment,0) < 2.4      then 'PTPAmount/Instalment < 2.4'
                        when a.PTPAmount/nullif(Instalment,0) < 2.6      then 'PTPAmount/Instalment < 2.6'
                        when a.PTPAmount/nullif(Instalment,0) < 2.8      then 'PTPAmount/Instalment < 2.8'
                        when a.PTPAmount/nullif(Instalment,0) < 3.0      then 'PTPAmount/Instalment < 3.0'
                        when a.PTPAmount/nullif(Instalment,0) > 3.0      then 'PTPAmount/Instalment > 3.0'
                        end as          PTPAmount_Bucket
                   ,case when a.PTPAmount <= 50                               then 1
                        when a.PTPAmount <= 200                              then 2
                        when a.PTPAmount/nullif(Instalment,0) < 0.2      then 3
                        when a.PTPAmount/nullif(Instalment,0) < 0.4      then 4
                        when a.PTPAmount/nullif(Instalment,0) < 0.6      then 5
                        when a.PTPAmount/nullif(Instalment,0) < 0.8      then 6
                        when a.PTPAmount/nullif(Instalment,0) < 1             then 7
                        when a.PTPAmount/nullif(Instalment,0) < 1.2      then 8
                        when a.PTPAmount/nullif(Instalment,0) < 1.4      then 9
                        when a.PTPAmount/nullif(Instalment,0) < 1.6      then 10
                        when a.PTPAmount/nullif(Instalment,0) < 1.8      then 11
                        when a.PTPAmount/nullif(Instalment,0) < 2             then 12
                        when a.PTPAmount/nullif(Instalment,0) < 2.2      then 13
                        when a.PTPAmount/nullif(Instalment,0) < 2.4      then 14
                        when a.PTPAmount/nullif(Instalment,0) < 2.6      then 15
                        when a.PTPAmount/nullif(Instalment,0) < 2.8      then 16
                        when a.PTPAmount/nullif(Instalment,0) < 3.0      then 17
                        when a.PTPAmount/nullif(Instalment,0) > 3.0      then 18
                        end as          PTPAmount_Bucket_Numeric
 
 /*Source*/
            from (select * from scoring.dbo.PTPReport_Detail_Update where PTPMadeMonth = &month_yr.) a
             inner join PRD_Collections_Strategy.dbo.coldata             b
             on cast(a.loanrefno as varchar) = cast(b.loanref as varchar)
             inner join PRD_Collections_Strategy.dbo.Coll_ActvRate       c
             on a.loanrefno = c.loanref
			/*Source*/
             inner join ( select * from scoring.[UNIZA\BTuckerAdmin].BHT_CCAgentList where Month = &month_years.) d
             on a.loginname = d.agent
             inner join (select * from scoring.dbo.useractivity where ActivityMonth = &month_year.)           e
             on a.activityid = e.id ;                               
        )
  by scoring;
quit;
 

PROC SQL;
     connect to ODBC as PRD_ContactInfo (dsn=mpwaps);
     execute (
           create table PRD_ContactInfo.dbo.[KMD_CIC_PTP_&month.] with (clustered columnstore index,distribution=hash(CallID_Detail)) as 
           select a.CallID_Detail
           ,a.InitiatedDate
           ,a.ConnectedDate
           ,a.TerminatedDate
           ,a.Duration
           ,a.Month
           ,a.Day
           ,a.Hour
           ,a.CallDirection
           ,a.LineDuration
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
           ,a.I3_IDENTITY
           ,a.Campaign
           ,a.CampaignType
           ,a.ATTEMPTS
           ,a.CUSTOMER_ID
           ,a.ACCOUNT_NO
           ,a.CLIENTNUMBER
           ,a.REPAYMENTMETHOD   RepaymentMethod_CIC
           ,a.IDNUMBER
           ,a.INSTALMENT        Instalment_CIC
           ,a.LoadDate
           ,a.PREF_LANG2
           ,a.NOOFLOANS
           ,a.RANDOMIND
           ,a.DATEOFBIRTH
           ,a.ClientBalance
           ,a.Prob_2
           ,a.Prob_3
           ,a.Prob_4
           ,a.HOMETEL
           ,a.WORKTEL
           ,a.CELLNO
           ,a.CELLNO2
           ,a.CELLNO3
           ,a.CELLNO4
           ,a.HOMETEL2
           ,a.HOMETEL3
           ,a.HOMETEL4
           ,a.WORKTEL2
           ,a.WORKTEL3
           ,a.WORKTEL4
           ,a.NewNumber
           ,a.Number_1
           ,a.I3_RowId
           ,a.Finishcode
           ,a.Agentid
           ,a.Callid
           ,a.Callidkey
           ,a.Phonenumber
           ,a.Callplacedtime
           ,a.Callconnectedtime
           ,a.Calldisconnectedtime
           ,a.Callingmode
           ,a.LoadDate_History
           ,a.Source
           ,a.RPC_FinishCode
           ,a.Duplicates
           ,a.ID                      ID_Activity_CIC
           ,a.ActivityDate            ActivityDate_Activity_CIC
           ,a.AbsActivityDate         AbsActivityDate_Activity_CIC
           ,a.ActivityMonth           ActivityMonth_Activity_CIC
           ,a.UserID                  UserId_Activity_CIC
           ,a.LoginName               LoginName_Activity_CIC
           ,a.UserName                     Username_Activity_CIC
           ,a.TemplateName            TemplateName_Activity_CIC
           ,a.TeamName                     Teamname_Activity_CIC
           ,a.Activity                     Activity_Activity_CIC
           ,a.Summary                 Summary_CIC
           ,a.ReasonCode              ReasonCode_Activity_CIC
           ,a.LoanCode1
           ,a.LoanCode2
           ,a.ProductIndicator        ProductIndicator_Activity_CIC
           ,a.Book                         Book_Activity_CIC
           ,a.Segment                 Segment_Activity_CIC
           ,a.Route                   Route_Activity_CIC
           ,a.WorkList                     WorkList_Activity_CIC
           ,a.ThirdParty              ThirdParty_Activity_CIC
           ,a.AttorneyRegion          AttorneyRegion_Activity_CIC
           ,a.RPC
           ,a.Contact
           ,a.NumberCalled_SourceField
           ,b.*
           from scoring.dbo.KMD_CIC_&month._Scores_Activities                         a 
           left join scoring.dbo.KMD_PTP_Monitoring_Table_&month.                     b 
           on a.LocalUserId = b.[Agent Name]
           and a.CLIENTNUMBER = b.ClientNo
           and a.InitiatedDate < b.activitydate
           and b.Activitydate < dateadd(minute,5,a.TerminatedDate)
           and a.Duration > 30;)
     by PRD_ContactInfo;
quit;
 

PROC SQL;
     connect to ODBC as PRD_ContactInfo (dsn=mpwaps);
     execute (
    create table PRD_ContactInfo.dbo.[KMD_CIC_PTP_&month._Reduced_Fiel] with (clustered columnstore index,distribution=hash(callid_Detail)) as 
                select CallID_Detail
                ,InitiatedDate
                ,ConnectedDate
                ,TerminatedDate
                ,Duration
                ,LineDuration
                ,Month
                ,Day
                ,Hour
                ,CallDirection
                ,LocalUserId
                ,LocalNumber
                ,LocalName
                ,RemoteNumber
                ,RemoteNumberFmt
                ,RemoteNumberCallId
                ,RemoteName
                ,Product
                ,PTPCounter
                ,I3TimeStampGMT
                ,Campaign
                ,CampaignType
                ,CUSTOMER_ID
                ,ACCOUNT_NO
                ,CLIENTNUMBER
                ,IDNUMBER
                ,Instalment_CIC
                ,LoadDate
                ,PREF_LANG2
                ,NOOFLOANS
                ,RANDOMIND
                ,DATEOFBIRTH
                ,ClientBalance
                ,HOMETEL
                ,WORKTEL
                ,CELLNO
                ,CELLNO2
                ,CELLNO3
                ,CELLNO4
                ,HOMETEL2
                ,HOMETEL3
                ,HOMETEL4
                ,WORKTEL2
                ,WORKTEL3
                ,WORKTEL4
                ,NewNumber
                ,Number_1
                ,I3_RowId
                ,Finishcode
                ,Agentid
                ,Callid
                ,Callidkey
                ,Phonenumber
                ,Callingmode
                ,LoadDate_History
                ,Source
                ,RPC_FinishCode
                ,ActivityMonth_Activity_CIC
                ,UserId_Activity_CIC
                ,LoginName_Activity_CIC
                ,Username_Activity_CIC
                ,Activity_Activity_CIC
                ,Summary_CIC
                ,ReasonCode_Activity_CIC
                ,ProductIndicator_Activity_CIC
                ,Book_Activity_CIC
                ,Segment_Activity_CIC
                ,Route_Activity_CIC
                ,ThirdParty_Activity_CIC
                ,AttorneyRegion_Activity_CIC
               ,RPC
                ,NumberCalled_SourceField
                ,GE_Instalment
                ,PTPMadeDate
                ,PTPMadeMonth
                ,AbsPTPMadeDate
                ,LoanRefNo
                ,ReasonCode_PTP
                ,LoginName_PTP
                ,Instalment
                ,Book_PTP
                ,Segment_PTP
                ,Route_PTP
                ,WorkList_PTP
                ,ThirdParty_PTP
                ,AttorneyRegion_PTP
                ,RepayMethod
                ,PTPAmount
                ,PTPDueDate
                ,PaidBeforeDate
                ,PaidBeforePTPMade
                ,PaidDate
                ,AmountPaid
                ,NewCash
                ,Status_PTP
                ,LastPaymentDate
                ,ACTIVITYID
                ,Loanref_ColData
                ,ClientNo
                ,IDNo
                ,ReasonCode_ColData
                ,HomeBranch_Final_ColData
                ,Homebranch
                ,ReasonCodeDate
                ,GroupCode
                ,STATUS
                ,Balance
                ,term
                ,Firstperenddate
                ,AvailableBalance
                ,RepaymentMethod_Group
                ,LastStrikeDate
                ,WageType
                ,SalaryDay
                ,MATURITYDATE
                ,PrincipalDebt
                ,subgroupcode
                ,randomind_ColData
                ,BookedScoreBand
                ,CNI
                ,LOANID
                ,Runmonth
                ,Age_Of_Account
                ,LoanGroup
                ,Transfer_HomeBranch_Final
                ,Time_In_Transfer_HB_Final
                ,Time_In_HomeBranch_Final
                ,Bank
                ,IDNo_CollActv
                ,HomeBranch_Final
                ,Excl_Ind
                ,Model_Applied
                ,Final_Prob2Actv
                ,Missing
                ,Final_Odds_Score
                ,Scores_Balance
                ,[AGENT NAME]
               ,OPSManager
                ,SNRManager
                ,SupervisorTallymanLogin
                ,ConsultantExactusLogin
                ,SupervisorName
                ,Afrikaans
                ,English
                ,Ndebele
                ,Nothern_Sotho
                ,Sesotho
                ,Swazi
                ,Tswana
                ,Venda
                ,Xhosa
                ,Zulu
                ,Ind_CS_CC
                ,ID
                ,ActivityDate
                ,AbsActivityDate
                ,ActivityMonth
                ,UserID
                ,LoginName
                ,UserName
                ,TemplateName
                ,TeamName
                ,LoanrefNo_Activity
                ,Activity
                ,Summary_Activity
                ,ReasonCode
                ,ProductIndicator
                ,Book
                ,Segment
                ,Route
                ,WorkList
                ,ThirdParty
                ,AttorneyRegion
                ,count(*)       Join_Duplicates
           from scoring.dbo.KMD_CIC_PTP_&month.
                group by CallID_Detail
                ,InitiatedDate
                ,ConnectedDate
                ,TerminatedDate
                ,Duration
               ,LineDuration
                ,Month
                ,Day
                ,Hour
                ,CallDirection
                ,LocalUserId
                ,LocalNumber
                ,LocalName
                ,RemoteNumber
                ,RemoteNumberFmt
                ,RemoteNumberCallId
                ,RemoteName
                ,Product
                ,PTPCounter
                ,I3TimeStampGMT
                ,Campaign
                ,CampaignType
                ,CUSTOMER_ID
                ,ACCOUNT_NO
                ,CLIENTNUMBER
                ,IDNUMBER
                ,Instalment_CIC
                ,LoadDate
                ,PREF_LANG2
                ,NOOFLOANS
                ,RANDOMIND
                ,DATEOFBIRTH
                ,ClientBalance
                ,HOMETEL
                ,WORKTEL
                ,CELLNO
                ,CELLNO2
                ,CELLNO3
                ,CELLNO4
                ,HOMETEL2
                ,HOMETEL3
                ,HOMETEL4
                ,WORKTEL2
                ,WORKTEL3
                ,WORKTEL4
                ,NewNumber
                ,Number_1
                ,I3_RowId
                ,Finishcode
                ,Agentid
                ,Callid
                ,Callidkey
                ,Phonenumber
                ,Callingmode
                ,LoadDate_History
                ,Source
                ,RPC_FinishCode
                ,ActivityMonth_Activity_CIC
                ,UserId_Activity_CIC
                ,LoginName_Activity_CIC
                ,Username_Activity_CIC
                ,Activity_Activity_CIC
                ,Summary_CIC
                ,ReasonCode_Activity_CIC
                ,ProductIndicator_Activity_CIC
                ,Book_Activity_CIC
                ,Segment_Activity_CIC
                ,Route_Activity_CIC
                ,ThirdParty_Activity_CIC
                ,AttorneyRegion_Activity_CIC
                ,RPC
                ,NumberCalled_SourceField
                ,GE_Instalment
                ,PTPMadeDate
                ,PTPMadeMonth
                ,AbsPTPMadeDate
                ,LoanRefNo
                ,ReasonCode_PTP
                ,LoginName_PTP
                ,Instalment
                ,Book_PTP
                ,Segment_PTP
                ,Route_PTP
                ,WorkList_PTP
                ,ThirdParty_PTP
                ,AttorneyRegion_PTP
                ,RepayMethod
                ,PTPAmount
                ,PTPDueDate
                ,PaidBeforeDate
                ,PaidBeforePTPMade
                ,PaidDate
                ,AmountPaid
                ,NewCash
                ,Status_PTP
                ,LastPaymentDate
                ,ACTIVITYID
                ,Loanref_ColData
                ,ClientNo
                ,IDNo
                ,ReasonCode_ColData
                ,HomeBranch_Final_ColData
                ,Homebranch
                ,ReasonCodeDate
                ,GroupCode
                ,STATUS
                ,Balance
                ,term
                ,Firstperenddate
                ,AvailableBalance
                ,RepaymentMethod_Group
                ,LastStrikeDate
                ,WageType
                ,SalaryDay
                ,MATURITYDATE
                ,PrincipalDebt
                ,subgroupcode
               ,randomind_ColData
                ,BookedScoreBand
                ,CNI
                ,LOANID
                ,Runmonth
                ,Age_Of_Account
                ,LoanGroup
                ,Transfer_HomeBranch_Final
                ,Time_In_Transfer_HB_Final
                ,Time_In_HomeBranch_Final
                ,Bank
                ,IDNo_CollActv
                ,HomeBranch_Final
                ,Excl_Ind
                ,Model_Applied
                ,Final_Prob2Actv
                ,Missing
                ,Final_Odds_Score
                ,Scores_Balance
                ,[AGENT NAME]
                ,OPSManager
                ,SNRManager
                ,SupervisorTallymanLogin
                ,ConsultantExactusLogin
                ,SupervisorName
                ,Afrikaans
                ,English
                ,Ndebele
                ,Nothern_Sotho
                ,Sesotho
                ,Swazi
                ,Tswana
                ,Venda
                ,Xhosa
                ,Zulu
                ,Ind_CS_CC
                ,ID
                ,ActivityDate
                ,AbsActivityDate
                ,ActivityMonth
                ,UserID
                ,LoginName
                ,UserName
                ,TemplateName
                ,TeamName
                ,LoanrefNo_Activity
                ,Activity
                ,Summary_Activity
                ,ReasonCode
                ,ProductIndicator
                ,Book
                ,Segment
                ,Route
                ,WorkList
                ,ThirdParty
                ,AttorneyRegion)
     by PRD_ContactInfo;
quit;
/*TODO (Sphe Notes): Optimise code to only retrieve last day's call data and append to existing table.
/*???


	At the beginning of the month, the code can do a SELECT * */
/* Check if Deduped Table exists in database before Appending */ 
proc sql stimer;
     connect to ODBC (dsn = MPWAPS);
     create table ScoringTables as
	     select * from connection to odbc 
		 (
			Use Scoring
				SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES order by TABLE_NAME
	      );
	     disconnect from odbc;
quit;

proc sql;
	create table Deduped_Exists as 
	select TABLE_NAME from ScoringTables where TABLE_NAME like "KM1_CIC_PTP_&month._Reduced_Fields_DeDuped";
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
		   connect to ODBC as PRD_ContactInfo (dsn=mpwaps);
		   execute(;  
		        CREATE TABLE PRD_ContactInfo.dbo.&outputDataSet. 
				with (clustered columnstore index,distribution=hash(callid_Detail)) 
				AS
				SELECT *
				FROM PRD_ContactInfo.dbo.&inputDataSet.
		   ) BY PRD_ContactInfo;
		QUIT;
	%end;
	%else %do;
		Proc sql stimer;     
		   connect to ODBC as PRD_ContactInfo (dsn=mpwaps);
		   execute(;  
		        insert into PRD_ContactInfo.dbo.&outputDataSet.
		        select *
		        from PRD_ContactInfo.dbo.&inputDataSet.;
		   ) BY PRD_ContactInfo;
		QUIT;
	%end;
%Mend;
%Append(inputDataSet=KMD_CIC_PTP_&month._Reduced_Fiel, outputDataSet=KM1_CIC_PTP_&month._Reduced_Fields_DeDuped);

%end_program(&process_number);
