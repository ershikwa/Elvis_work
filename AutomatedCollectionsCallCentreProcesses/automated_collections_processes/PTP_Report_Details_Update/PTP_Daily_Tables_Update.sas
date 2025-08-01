%include "H:\Process_Automation\sas_autoexec\sas_autoexec.sas";

%let projectcode =H:\Process_Automation\Codes;

%let runmonth = %scan(&sysparm,1);
%let process_number = %scan(&sysparm,2);
%let process =  %scan(&sysparm,3);

%let project =pj;

libname &project "&process";

%start_program;

data _null_;
     call symput("two_month", put(intnx("month", today(),-2,'end'),yymmn6.));
     call symput("previous", put(intnx("month", today(),-1,'end'),yymmn6.));
     call symput("month", put(intnx("month", today(),-1,'end'),yymmn6.));
     call symput("month_year", put(intnx("month", today(),-0,'end'),YYMMD8.));
	 call symput("Day", put(intnx("day", today(),-1,'end'),yymmdd10.));
     call symput("month_yr", put(intnx("month", today(),-0,'end'),YYMMS8.));
run;
%put &month;
%put &previous;
%put &two_month;

%include "\\neptune\credit$\AA_GROUP CREDIT\Scoring\SAS Macros\SQLDEL_APS.sas";
/*%include "\\neptune\credit$\AA_GROUP CREDIT\Scoring\SAS Macros\Upload_APS_A.sas";*/
%include "\\neptune\credit$\AA_GROUP CREDIT\Scoring\SAS Macros\Upload_APS_A.sas";
libname APS odbc dsn = Scoring schema=dbo connection=unique direct_sql=yes preserve_tab_names=yes;
Options compress=yes;

%macro remove(inputdatase);
     proc delete data=aps.&inputdatase.;
%mend;
%remove(inputdatase=KMD_PTP_&month._Red_PTPAggregated);
%remove(inputdatase= KMD1_PTP_&month.);
%remove(inputdatase= KMD_PTP_&month._Temp);

options compress = yes;
PROC SQL;
     connect to ODBC as PRD_ContactInfo (dsn=mpwaps);
     execute (
     create table PRD_ContactInfo.dbo.KMD_PTP_&month._Red_PTPAggreghated with (clustered columnstore index,distribution=hash(ActivityID)) as 
		select CallDirection, LocalUserID
		,Product
		,PTPCounter
		, Campaign, CampaignType, Customer_ID, Account_No
		,ClientNumber, ClientNo, IDNumber, IDNo, IDNo_CollActv, Instalment_CIC, LoadDate, Pref_Lang2, NoOfLoans, RandomInd, DateOfBirth
		,ClientBalance, AgentID, LoginName_PTP, Instalment, Segment_PTP
		,WorkList_PTP
		,ThirdParty_PTP
		,AttorneyRegion_PTP
		,RepayMethod	
		,Status_PTP
		,ActivityID
		,Loanref_ColData
		,ReasonCode_ColData
		,ReasonCodeDate
		,HomeBranch_Final_ColData
		,HomeBranch
		,GroupCode
		,Status
		,Balance
		,Term
		,FirstPerenddate
		,AvailableBalance
		,RepaymentMethod_Group
		,LastStrikeDate
		,WageType
		,SalaryDay
		,MaturityDate
		,PrincipalDebt
		,SubgroupCode
		,RandomIND_ColData
		,BookedScoreBand
		,CNI
		,LoanID
		,RunMonth
		,Age_Of_Account
		,LoanGroup
		,Transfer_HomeBranch_Final
		,Time_In_Transfer_HB_Final
		,Time_In_HomeBranch_Final
		,Bank
		,HomeBranch_Final
		,Model_Applied
		,Final_Prob2Actv
		,Missing
		,Final_Odds_Score
		,[Agent Name]			Agent_Name
		,OpsManager
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
		,ID						Activity_ID
		,ActivityDate
		,AbsActivityDate
		,ActivityMonth
		,UserID
		,LoginName
		,UserName
		,LoanrefNo_Activity
		,Activity
		,ReasonCode				ReasonCode_Activity
		,ProductIndicator
		,max(Loanrefno)			Loanrefno
		,min(Loanrefno)			Loanrefno_Check
		,max(RPC)				RPC
		,max(PTPMadeDate)		PTPMadeDate
		,max(PTPDueDate)		PTPDueDatez
		,max(PTPMadeMonth)		PTPMadeMonth
		,max(AbsPTPMadeDate)	AbsPTPMadeDate
		,max(ReasonCode_PTP)	ReasonCode_PTP
		,max(PTPAmount)			PTPAmount
		,min(PTPAmount)			PTPAmount_Check
		,max(PaidBeforeDate)	PaidBeforeDate
		,max(PaidDate)			PaidDate
		,max(AmountPaid)		AmountPaid
		,min(AmountPaid)		AmountPaid_Check
		,max(NewCash)			NewCash
		,min(NewCash)			NewCash_Check
		,count(*)				PTP_Aggregation_Duplicates
	from PRD_ContactInfo.dbo.KMD_CIC_PTP_&month._Reduced_Fiel
	group by CallDirection, LocalUserID
		,Product
		,PTPCounter
		, Campaign, CampaignType, Customer_ID, Account_No
		,ClientNumber, ClientNo, IDNumber, IDNo, IDNo_CollActv, Instalment_CIC, LoadDate, Pref_Lang2, NoOfLoans, RandomInd, DateOfBirth
		,ClientBalance, AgentID, LoginName_PTP, Instalment, Segment_PTP
		,WorkList_PTP
		,ThirdParty_PTP
		,AttorneyRegion_PTP
		,RepayMethod	
		,Status_PTP
		,ActivityID
		,Loanref_ColData
		,ReasonCode_ColData
		,ReasonCodeDate
		,HomeBranch_Final_ColData
		,HomeBranch
		,GroupCode
		,Status
		,Balance
		,Term
		,FirstPerenddate
		,AvailableBalance
		,RepaymentMethod_Group
		,LastStrikeDate
		,WageType
		,SalaryDay
		,MaturityDate
		,PrincipalDebt
		,SubgroupCode
		,RandomIND_ColData
		,BookedScoreBand
		,CNI
		,LoanID
		,RunMonth
		,Age_Of_Account
		,LoanGroup
		,Transfer_HomeBranch_Final
		,Time_In_Transfer_HB_Final
		,Time_In_HomeBranch_Final
		,Bank
		,HomeBranch_Final
		,Model_Applied
		,Final_Prob2Actv
		,Missing
		,Final_Odds_Score
		,[Agent Name]		
		,OpsManager
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
		,LoanrefNo_Activity
		,Activity
		,ReasonCode
		,ProductIndicator
     ;)
     by PRD_ContactInfo;
quit;

PROC SQL;
     connect to ODBC as PRD_ContactInfo (dsn=mpwaps); 
     execute (
    	create table PRD_ContactInfo.dbo.[KMD_PTP_&month._Temp] with (clustered columnstore index,distribution=hash(ActivityID)) as 
		select *,case when (AmountPaid >= PTPAmount and PTPAmount > 0)	then	1
					else 0				end as				Honoured
		from PRD_ContactInfo.dbo.KMD_PTP_&month._Red_PTPAggregated
		where PTPMadeDate is not null
     ;)
     by PRD_ContactInfo;
quit;

PROC SQL;
     connect to ODBC as PRD_ContactInfo (dsn=mpwaps);
     execute (
    	create table PRD_ContactInfo.dbo.[KMD1_PTP_&month.] with (clustered columnstore index,distribution=hash(ActivityID)) as 
		select 
		LocalUserID
		,Product
		,PTPCounter
		,Campaign
		,CampaignType
		,Customer_ID
		,Account_No
		,ClientNumber
		,ClientNo
		,IDNumber
		,IDNo
		,IDNo_CollActv
		,Instalment_CIC
		,LoadDate
		,Pref_Lang2
		,NoOfLoans
		,RandomInd
		,DateOfBirth
		,ClientBalance
		,LoginName_PTP
		,Instalment
		,Segment_PTP
		,WorkList_PTP
		,ThirdParty_PTP
		,AttorneyRegion_PTP
		,RepayMethod
		,ActivityID
		,Loanref_ColData
		,ReasonCode_ColData
		,ReasonCodeDate
		,HomeBranch_Final_ColData
		,HomeBranch
		,GroupCode
		,Status
		,Balance
		,Term
		,FirstPerenddate
		,AvailableBalance
		,RepaymentMethod_Group
		,LastStrikeDate
		,WageType
		,SalaryDay
		,MaturityDate
		,PrincipalDebt
		,SubgroupCode
		,RandomIND_ColData
		,BookedScoreBand
		,CNI
		,LoanID
		,RunMonth
		,Age_Of_Account
		,LoanGroup
		,Transfer_HomeBranch_Final
		,Time_In_Transfer_HB_Final
		,Time_In_HomeBranch_Final
		,Bank
		,HomeBranch_Final
		,Model_Applied
		,Final_Prob2Actv
		,Missing
		,Final_Odds_Score
		,Agent_Name
		,OpsManager
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
		,Activity_ID
		,ActivityDate
		,AbsActivityDate
		,ActivityMonth
		,UserID
		,LoginName
		,UserName
		,LoanrefNo_Activity
		,Activity
		,ReasonCode_Activity
		,ProductIndicator
		,Loanrefno
		,Loanrefno_Check
		,PTPMadeDate
		,PTPDueDate
		,PTPMadeMonth
		,AbsPTPMadeDate
		,ReasonCode_PTP
		,max(RPC)				RPC
		,max(PTPAmount)			PTPAmount
		,max(PaidBeforeDate)	PaidBeforeDate
		,max(PaidDate)		PaidDate
		,max(AmountPaid)	AmountPaid
		,max(NewCash)		NewCash
		,max(Honoured)		Honoured
	from PRD_ContactInfo.dbo.KMD_PTP_&month._Temp
	group by 
		LocalUserID
		,Product
		,PTPCounter
		,Campaign
		,CampaignType
		,Customer_ID
		,Account_No
		,ClientNumber
		,ClientNo
		,IDNumber
		,IDNo
		,IDNo_CollActv
		,Instalment_CIC
		,LoadDate
		,Pref_Lang2
		,NoOfLoans
		,RandomInd
		,DateOfBirth
		,ClientBalance
		,LoginName_PTP
		,Instalment
		,Segment_PTP
		,WorkList_PTP
		,ThirdParty_PTP
		,AttorneyRegion_PTP
		,RepayMethod
		,ActivityID
		,Loanref_ColData
		,ReasonCode_ColData
		,ReasonCodeDate
		,HomeBranch_Final_ColData
		,HomeBranch
		,GroupCode
		,Status
		,Balance
		,Term
		,FirstPerenddate
		,AvailableBalance
		,RepaymentMethod_Group
		,LastStrikeDate
		,WageType
		,SalaryDay
		,MaturityDate
		,PrincipalDebt
		,SubgroupCode
		,RandomIND_ColData
		,BookedScoreBand
		,CNI
		,LoanID
		,RunMonth
		,Age_Of_Account
		,LoanGroup
		,Transfer_HomeBranch_Final
		,Time_In_Transfer_HB_Final
		,Time_In_HomeBranch_Final
		,Bank
		,HomeBranch_Final
		,Model_Applied
		,Final_Prob2Actv
		,Missing
		,Final_Odds_Score
		,Agent_Name
		,OpsManager
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
		,Activity_ID
		,ActivityDate
		,AbsActivityDate
		,ActivityMonth
		,UserID
		,LoginName
		,UserName
		,LoanrefNo_Activity
		,Activity
		,ReasonCode_Activity
		,ProductIndicator
		,Loanrefno
		,Loanrefno_Check
		,PTPMadeDate
		,PTPDueDate
		,PTPMadeMonth
		,AbsPTPMadeDate
		,ReasonCode_PTP
     ;)
     by PRD_ContactInfo;
quit;

/*TODO (Sphe Notes): Optimise code to only retrieve last day's call data and append to existing table.
	At the beginning of the month, the code can do a SELECT * */
/* Check if Deduped Table exists in database before Appending */ 
proc sql stimer;
     connect to ODBC (dsn = MPWAPS);
     create table ScoringTables as
	     select * from connection to odbc 
		 (
			Use PRD_ContactInfo
				SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES order by TABLE_NAME
	      );
	     disconnect from odbc;
quit;

proc sql;
	create table Deduped_Exists as 
	select TABLE_NAME from ScoringTables where TABLE_NAME like "KM1_PTP_&month.";
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
				with (clustered columnstore index,distribution=hash(ActivityID))
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
%Append(inputDataSet=KMD1_PTP_&month., outputDataSet=KM1_PTP_&month.);

%end_program(&process_number);
