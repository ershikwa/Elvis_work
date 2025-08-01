%include "H:\Process_Automation\sas_autoexec\sas_autoexec.sas";

%let projectcode =H:\Process_Automation\Codes;

%let runmonth = %scan(&sysparm,1);
%let process_number = %scan(&sysparm,2);
%let process =  %scan(&sysparm,3);

%let project =pj;

libname &project "&process";

%start_program;

%let thismonth = %sysfunc(today(), yymmn6.);
%put &thismonth;

options mprint symbolgen mlogic;

data _null_;
     call symput("two_month", put(intnx("month", today(),-2,'end'),yymmn6.));
     call symput("previous", put(intnx("month", today(),-1,'end'),yymmn6.));
     call symput("month", put(intnx("month", today(),-0,'end'),yymmn6.));
	 call symput("lastmonth", put(intnx("day", today(),-1),yymmn6.));
     call symput("thismonth", put(intnx("day", today(),-1),yymmn6.));
     call symput("month_year", cats("'",put(intnx("month", today(),-0,'end'),YYMMD8.),"'"));
	 call symput("Day", put(intnx("day", today(),-1,'end'),yymmdd10.));
     call symput("month_yr", cats("'",put(intnx("month", today(),-0,'end'),YYMMS8.),"'"));
run;

/*

libname scoring odbc dsn=Scoring schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

%macro findit;                                 
	%if %sysfunc(exist(scoring.KM1_CIC_PTP_&month._Reduced_Fields_DeDuped)) %then %do;  
		%put The file KM1_CIC_PTP_&month._Reduced_Fields_DeDuped exists. ;
		%global lastmonth;
		%let lastmonth = &month.;
	%end; 
	%else %do;
		%put The file KM1_CIC_PTP_&month._Reduced_Fields_DeDuped does not exists. ;
		%global lastmonth;
		%let lastmonth = &previous.;
	%end;
%mend;     

%findit;*/

%put &lastmonth;

options mprint mlogic symbolgen;

data _null_;
	call symput("Call_Mapping",cat("'scoring.[UNIZA\BTuckerAdmin].BHT_CIC_",&thismonth.,"_Call_Mapping'"));
	call symput("Call_Mapping_2",cat("'scoring.[UNIZA\BTuckerAdmin].BHT_CIC_",&thismonth.,"_Call_Mapping_2'"));
	call symput("Call_Mapping_3",cat("'scoring.[UNIZA\BTuckerAdmin].BHT_CIC_",&thismonth.,"_Call_Mapping_3'"));
	call symput("Call_Mapping_4",cat("'scoring.[UNIZA\BTuckerAdmin].BHT_CIC_",&thismonth.,"_Call_Mapping_4'"));
	call symput("Call_Mapping_5",cat("'scoring.[UNIZA\BTuckerAdmin].BHT_CIC_",&thismonth.,"_Call_Mapping_5'"));
	call symput("Call_Mapping_6",cat("'scoring.[UNIZA\BTuckerAdmin].BHT_CIC_",&thismonth.,"_Call_Mapping_6'"));
	call symput("Call_Mapping_7",cat("'scoring.[UNIZA\BTuckerAdmin].BHT_CIC_",&thismonth.,"_Call_Mapping_7'"));
	call symput("Call_Mapping_8",cat("'scoring.[UNIZA\BTuckerAdmin].BHT_CIC_",&thismonth.,"_Call_Mapping_8'"));
	call symput("Call_Mapping_9",cat("'scoring.[UNIZA\BTuckerAdmin].BHT_CIC_",&thismonth.,"_Call_Mapping_9'"));
	call symput("Call_Mapping_10",cat("'scoring.[UNIZA\BTuckerAdmin].BHT_CIC_",&thismonth.,"_Call_Mapping_10'"));
	call symput("Call_Mapping_11",cat("'scoring.[UNIZA\BTuckerAdmin].BHT_CIC_",&thismonth.,"_Call_Mapping_11'"));
	call symput("Call_Mapping_12",cat("'scoring.[UNIZA\BTuckerAdmin].BHT_CIC_",&thismonth.,"_Call_Mapping_12'"));
	call symput("Call_Mapping_7_Duplicated_Calls",cat("'scoring.[UNIZA\BTuckerAdmin].BHT_CIC_",&thismonth.,"_Call_Mapping_7_Duplicated_Calls'"));
run;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id(&Call_Mapping.) is not null DROP TABLE scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping;
		create table scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping
		with(distribution = hash(callid_detail), clustered columnstore index)
		as
		select * from Scoring.dbo.KM1_CIC_PTP_&lastmonth._Reduced_Fields_DeDuped
		where campaign is not null
		and calldirection = 'Outbound'
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id(&Call_Mapping_2.) is not null DROP TABLE scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_2;			
		create table scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_2
		with(distribution = hash(callid_detail), clustered columnstore index)
		as
		select callid_detail, InitiatedDate, ConnectedDate, TerminatedDate, Duration, LineDuration
				,hour, LocalUserID, LocalNumber, PhoneNumber, ClientNumber, IDNumber, Instalment_CIC, callingmode
				,FinishCode, Source, RPC_FinishCode, ActivityID, loanrefno, concat(cast(InitiatedDate as date), ' 06:45:00.000')	as Day_Start_Time
				,RPC, campaign
		from scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id(&Call_Mapping_3.) is not null DROP TABLE scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_3;
		create table scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_3
		with(distribution = hash(callid_detail), clustered columnstore index)
		as
		select callid_detail, InitiatedDate, ConnectedDate, TerminatedDate, Duration, LineDuration
				, hour, LocalUserID, LocalNumber, PhoneNumber, ClientNumber, IDNumber, Instalment_CIC, callingmode
				, FinishCode, Source, RPC_FinishCode, ActivityID, Loanrefno, Day_Start_Time, RPC, campaign, count(*)	Dupes
		from scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_2
		group by callid_detail, InitiatedDate, ConnectedDate, TerminatedDate, Duration, LineDuration
				, hour, LocalUserID, LocalNumber, PhoneNumber, ClientNumber, IDNumber, Instalment_CIC, callingmode
				, FinishCode, Source, RPC_FinishCode, ActivityID, Loanrefno, Day_Start_Time, RPC, campaign
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id(&Call_Mapping_4.) is not null DROP TABLE scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_4;
		create table scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_4
		with(distribution = hash(callid_detail), clustered columnstore index)
		as
		select callid_detail, InitiatedDate, ConnectedDate, TerminatedDate, Duration, LineDuration
				, hour, LocalUserID, LocalNumber, PhoneNumber, ClientNumber, IDNumber, Instalment_CIC, callingmode
				, FinishCode, Source, RPC_FinishCode, Day_Start_Time, campaign, sum(RPC)	 as RPC
				,count(distinct activityID)	PTP_Count
		from scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_3
		group by callid_detail, InitiatedDate, ConnectedDate, TerminatedDate, Duration, LineDuration
				, hour, LocalUserID, LocalNumber, PhoneNumber, ClientNumber, IDNumber, Instalment_CIC, callingmode
				, FinishCode, Source, RPC_FinishCode, Day_Start_Time, campaign
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		update scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_4
		set RPC = 1
		where rpc > 1
	)
	by APS;
quit;


proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id(&Call_Mapping_5.) is not null DROP TABLE scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_5;
		create table scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_5
		with(distribution = hash(callid_detail), clustered columnstore index)
		as
		select *, datediff(second, Day_Start_Time, Initiateddate)										Initiated_Second
				, case when duration > 0	then datediff(second, Day_Start_Time, Connecteddate)	
					else -1			end as	Connected_Second
				, case when duration > 0	then datediff(second, InitiatedDate, Connecteddate)	
					else						 datediff(second, InitiatedDate, Terminateddate)	
					end as Dial_Duration
				, datediff(second, Initiateddate, TerminatedDate)										Total_Duration
		from scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_4
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	

		IF object_id(&Call_Mapping_6.) is not null DROP TABLE scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_6;
		create table scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_6
		with(distribution = hash(callid_detail), clustered columnstore index)
		as
		select callid_detail, InitiatedDate, Duration, Dial_Duration, Total_Duration, hour, LocalUserID, PhoneNumber, ClientNumber
				,FinishCode, Source, RPC, PTP_Count, Initiated_Second, Connected_Second, campaign, callingmode
		from scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_5
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id(&Call_Mapping_7.) is not null DROP TABLE scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_7;
		create table scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_7
		with(distribution = hash(callid_detail), clustered columnstore index)
		as
		select *, datepart(day, Initiateddate)	Day from scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_6
		where callid_detail not in (select callid_detail from scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_6
		group by callid_detail
		having count(*) > 1)
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id(&Call_Mapping_7_Duplicated_Calls.) is not null DROP TABLE scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_7_Duplicated_Calls;
		create table scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_7_Duplicated_Calls
		with(distribution = hash(callid_detail), clustered columnstore index)
		as
		select *, datepart(day, Initiateddate)	Day from scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_6
		where callid_detail in (select callid_detail from scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_6
		group by callid_detail
		having count(*) > 1)
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		update scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_7
		set localuserid = '-'
		where localuserid = 'SWITCHOVER'
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id(&Call_Mapping_8.) is not null DROP TABLE scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_8;
		create table scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_8
		with(distribution = hash(localuserid), clustered columnstore index)
		as
		select day, localuserid, count(*)	Calls_Assigned
		from scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_7
		group by day, localuserid
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id(&Call_Mapping_9.) is not null DROP TABLE scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_9;
		create table scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_9
		with(distribution = hash(localuserid), clustered columnstore index)
		as
		select *, (row_number() over (partition by day order by localuserid asc))-2 as AgentNum
		from scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_8
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id(&Call_Mapping_10.) is not null DROP TABLE scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_10;
		create table scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_10
		with(distribution = hash(callid_detail), clustered columnstore index)
		as
		select a.*, b.AgentNum
		from scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_7		a
		inner join scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_9		b
		on a.localuserid = b.localuserid
		and a.day = b.day
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id(&Call_Mapping_11.) is not null DROP TABLE scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_11;
		create table scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_11
		with(distribution = hash(day), clustered columnstore index)
		as
		select day, count(*)	Call_Volume, count(distinct clientNumber)	Distinct_Clients
		, (count(distinct AgentNum) - 1)	Active_Agents
		from scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_10
		group by day
	)
	by APS;
quit;

proc sql stimer;
	connect to ODBC as APS (dsn=MPWAPS); 
	execute	
	(	
		IF object_id(&Call_Mapping_12.) is not null DROP TABLE scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_12;
		create table scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_12
		with(distribution = hash(clientnumber), clustered columnstore index)
		as
		select a.*
		,(row_number() over (partition by day order by a.initiateddate asc))  		Call_Num
		,datepart(minute, a.Initiateddate)	Minute
		from scoring.[UNIZA\BTuckerAdmin].BHT_CIC_&thismonth._Call_Mapping_10		a
	)
	by APS;
quit;

%end_program(&process_number);


