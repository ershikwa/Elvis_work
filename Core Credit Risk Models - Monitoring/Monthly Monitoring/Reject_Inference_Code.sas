OPTIONS NOSYNTAXCHECK ;
options compress = yes;
/*options mstored sasmstore=sasmacs; */
/*libname sasmacs "\\neptune\credit$\AA_GROUP CREDIT\Scoring\Model Macros\";*/

libname V5 '\\neptune\sasa$\V5\V5 Monitoring\Performance Monitoring\data';
libname decile '\\neptune\sasa$\V5\V5 Monitoring\Performance Monitoring\data';
libname lookup '\\neptune\sasa$\V5\Segmentation Models For Compuscan\lookup';
libname reject "\\MPWSAS5\projects3\Compuscan\Reject Inference data";
libname data "\\mpwsas5\data";

/*%include '\\neptune\credit$\AA_GROUP CREDIT\Scoring\SAS Macros\Upload_APS.sas';*/

data _null_;
	call symput('currentdate', put(intnx('day',today(),0,'same'),date9.));
run;
%put &currentdate;

data _null_;
	call symput("month10",put(intnx("month","&currentdate"d,-10,"begin"),yymmddn8.));
	call symput("runmonth",put(intnx("month","&currentdate"d,-1,"end"),yymmn6.));
	call symput("month",put(intnx("month","&currentdate"d,-1,"end"),yymmddn8.));
	call symput("prevmonth",put(intnx("month","&currentdate"d,-2,"end"),yymmn6.));
	call symput("startoutcome",put(intnx("month","&currentdate"d,-11,"end"),yymmn6.));
	call symput("endoutcome",put(intnx("month","&currentdate"d,-6,"end"),yymmn6.));
	call symput("pinpointmonth",put(intnx("month","&currentdate"d,-1,"end"),yymmddn8.));
	call symput("statement9",put(intnx("month","&currentdate"d,-9,"end"),yymmn6.));
	call symput("statement8",put(intnx("month","&currentdate"d,-8,"end"),yymmn6.));
	call symput("statement7",put(intnx("month","&currentdate"d,-7,"end"),yymmn6.));
	call symput("statement6",put(intnx("month","&currentdate"d,-6,"end"),yymmn6.));
run;
%put  &month ;

options compress = binary;

data Comp_pinpoint_sv_&month;
	set data.Comp_pinpoint_sv_&month(keep=	AIL_Num1pDelq90Days         AIL_NumPayments2Years         AIL_PercPayments90Days
											AIL_TimeMRTrade         ALL_MaxDelq1YearLT24M         ALL_Num1pDelq90Days
											ALL_NumTrades180Days AUL_MaxDelq1YearLT12M         AUL_RatioCurBal30Days
											CST_DebtReviewGranted         UNN_AvgMonthsOnBook         UNS_DaysSinceMRPayment
											UNS_PercSatisfToOpenTrades         VAP_PrismScore_MI         id_no
									);
run;

data reject.Comp_pinpoint_sv_&month;
	set Comp_pinpoint_sv_&month;
run; 

proc sort data =reject.Comp_pinpoint_sv_&month nodupkey ;
	by id_no ;
run;

%macro reject_scoringcode(inputdata=);
	%do i = 6 %to 9;
		libname model_&i "&path\Model_&i\Buckets";

		proc sql;
			select tranwrd(parameter,"_W","") into : varlist separated by " " from model_&i..parameter_estimate
			where upcase(parameter) ne "INTERCEPT";
		quit;

		proc sql;
			select parameter into : varlist_woe separated by " " from model_&i..parameter_estimate
			where upcase(parameter) ne "INTERCEPT";
		quit;
		proc sql;
			select tranwrd(parameter,"_W","_B") into : varlist_buckets separated by " " from model_&i..parameter_estimate
			where upcase(parameter) ne "INTERCEPT";
		quit;

		data &inputdata(drop = &varlist_woe &varlist_buckets) ;
			set &inputdata;

			%do d = 1 %to %sysfunc(countw(&varlist));
				%let var = %scan(&varlist,&d);
			    %include "&path\Model_&i\Buckets\&var._if_statement_.sas";
			    %include "&path\Model_&i\Buckets\&var._WOE_if_statement_.sas"; 
			%end;
			*****************************************;
			** SAS Scoring Code for PROC Hplogistic;
			*****************************************;
			%include "&path\Model_&i\Buckets\creditlogisticcode2.sas";
			RI&i._Probability = P_Target1;
			drop P_Target1;
		run;
	%end;

	libname model_&i "&path\Thinfile Model";

	proc sql;
		select tranwrd(parameter,"_W","") into : varlist separated by " " from model_&i..parameter_estimate
		where upcase(parameter) ne "INTERCEPT";
	quit;

	proc sql;
		select parameter into : varlist_woe separated by " " from model_&i..parameter_estimate
		where upcase(parameter) ne "INTERCEPT";
	quit;
	proc sql;
		select tranwrd(parameter,"_W","_B") into : varlist_buckets separated by " " from model_&i..parameter_estimate
		where upcase(parameter) ne "INTERCEPT";
	quit;

	data &inputdata(drop = &varlist_woe &varlist_buckets) ;
		set &inputdata;

		%do d = 1 %to %sysfunc(countw(&varlist));
			%let var = %scan(&varlist,&d);
		    %include "&path\Thinfile Model\&var._if_statement_.sas";
		    %include "&path\Thinfile Model\&var._WOE_if_statement_.sas"; 
		%end;
		*****************************************;
		** SAS Scoring Code for PROC Hplogistic;
		*****************************************;
		%include "&path\Thinfile Model\creditlogisticcode2.sas";
		TF_Probability = P_Target1;
		drop P_Target1;
	run;
%mend;
%let path = \\neptune\sasa$\V5\Application Scorecard\Reject_Inference_Models;

%reject_scoringcode(inputdata=reject.Comp_pinpoint_sv_&month);

/*data reject.Comp_pinpoint_sv_&month;
	set Comp_pinpoint_sv_&month;
run;*/

proc sql stimer;
	connect to ODBC (dsn=credit);
	create table reject.BR_Base as select * from connection to odbc (
	    select 	A.loanid, 
			    A.creationdate, 
			    substring(cast(A.creationdate as varchar),1,6) as Month ,
			    A.status,
			    A.Origination_source,
			    A.BRDeclines,
			    A.ReasonCode1,
			    A.WalkAwayFlag,
			    A.failed_affordability,
			    B.RTIUNSECUREDREVOLVINGTOTALINSTALMENT,
			    B.RTI_PREDISB
	    from credit.dbo.nr_card_appflow A 
	    left join credit.dbo.NR_AffordabilityDetail B 
	    on A.loanid = B.loanid
	    where A.creationdate >= &month10
	);disconnect from ODBC;
quit;

proc freq data=reject.BR_Base;
	tables month*failed_affordability;
run;
%put &month10;

%macro getapplicationbase();
	%if %sysfunc(exist(V5.Applicationbase_&runmonth.)) %then %do;
		Proc SQL;
			create table Applicationbase_&runmonth as 
			    select a.*, b.status,b.creationdate,b.Month,b.BRDeclines,b.RTIUNSECUREDREVOLVINGTOTALINSTAL,b.loanid,b.failed_affordability
			    from V5.Applicationbase_&runmonth. a 
			    left join Reject.BR_BASE b
			    on  input(a.tranappnumber,best15.) = b.loanid;
		quit;
	%end;
	%else %do;
		Proc SQL;
			create table Applicationbase_&runmonth as 
			    select a.*, b.status,b.creationdate,b.Month,b.BRDeclines,b.RTIUNSECUREDREVOLVINGTOTALINSTAL,b.loanid,b.failed_affordability
			    from V5.Applicationbase_&prevmonth. a 
			    left join Reject.BR_BASE b
			    on  input(a.tranappnumber,best15.) = b.loanid;
		quit;
	%end;
%mend;
%getapplicationbase();

/*Proc SQL;*/
/*	create table Applicationbase_&runmonth as */
/*	    select a.*, b.status,b.creationdate,b.Month,b.BRDeclines,b.RTIUNSECUREDREVOLVINGTOTALINSTAL,b.loanid,b.failed_affordability*/
/*	    from V5.Applicationbase_&runmonth. a */
/*	    left join Reject.BR_BASE b*/
/*	    on  input(a.tranappnumber,best15.) = b.loanid;*/
/*quit;*/

proc sort data=Applicationbase_&runmonth. nodupkey dupout=Dups;
	by Tranappnumber;
run;

/*Source DS Codes*/
Data NG_Applicationbase;
	set Applicationbase_&runmonth;
	keep loanid;
run;
%put &runmonth;

%Upload_APS(Set =NG_Applicationbase , Server = Work, APS_ODBC = Prd_DDDa, APS_DB = PRD_DataDistillery_data, Distribute = HASH (loanid));

proc sql stimer;
	connect to ODBC (dsn=MPWAPS);
	create table New_Appbase_&runmonth as select * from connection to odbc (

	    select a.loanid,c.uniqueid,c.nationalid as idnumber,b.declinecode,b.declinedescription,
	                max(case when b.declinecode like '%D%' and b.declinecode in ('DS62','DS64','DS74','DS78','DS79','DS777','DS778','DS779',
																					'DS788','DS789','DS790','DS791','DS792','DS793','DS794','DS795','DS796','DS797','DS798','DS799','DS294' , 'DS276','DS608',
																					 'DS991','DS992','DS993','DS994','DS995','DS996') 
				then 0 else 1 end) as DS_Flag2
	    from PRD_DataDistillery_data.dbo.NG_Applicationbase a
	    left join PRD_Press.capri.capri_application_decline b
	    on cast(a.loanid as varchar) = b.tranappnumber
	    inner join prd_press.capri.capri_Loan_application c
	    on cast(a.loanid as varchar) = c.TRANAPPNUMBER  
	    group by a.loanid,c.uniqueid,c.nationalid,b.declinecode,b.declinedescription);
	disconnect from ODBC;
quit;

Proc sql;
	create table New_Appbase_06_DS as 
	    select a.*,b.declinecode,b.declinedescription,b.DS_Flag2
	    from Applicationbase_&runmonth a
	    left join New_Appbase_&runmonth b
	    on input(a.tranappnumber,best15.)=b.loanid;
quit;

proc sort data=New_Appbase_06_DS nodupkey dupout=Dups;
	by Tranappnumber;
run;

Data Passed_DS;
	set New_Appbase_06_DS;
	where DS_Flag2=0;
run;

proc freq data=New_Appbase_06_DS;
	tables month*DS_Flag2;
run;
/*NOTE: There were 786925 observations read from the data set WORK.NEW_APPBASE_06_DS.*/
/*      WHERE DS_Flag2=0;*/
/*NOTE: The data set WORK.PASSED_DS has 786925 observations and 197 variables.*/

Data New_Rej_Incl;
	set Passed_DS;
	where BRDeclines = 'Scoring';
run;
/*NOTE: There were 110785 observations read from the data set WORK.PASSED_DS.*/
/*      WHERE BRDeclines='Scoring';*/
/*NOTE: The data set WORK.NEW_REJ_INCL has 110785 observations and 197 variables.*/

Data Scoring_Declines;
	set New_Rej_Incl;
	where Status ='REJ';
run;
/*NOTE: There were 110771 observations read from the data set WORK.NEW_REJ_INCL.*/
/*      WHERE Status='REJ';*/
/*NOTE: The data set WORK.SCORING_DECLINES has 110771 observations and 197 variables.*/

Data Scoring_Declines2;
	set Scoring_Declines;

	if CALCULATEDNETTINCOME = 0.00  then NETINCOME = MONTHLYNETTINCOME ;
	else if CALCULATEDNETTINCOME < MONTHLYNETTINCOME then NETINCOME = MONTHLYNETTINCOME ;
	else NETINCOME = CALCULATEDNETTINCOME ;

	if WAGEFREQUENCYCODE = 'WAG001' then do ;
		if MONTHLYGROSSINCOME = 0.00 then  GROSSINCOME = NETINCOME ;
		else GROSSINCOME = MONTHLYGROSSINCOME ;
	end;

	if WAGEFREQUENCYCODE = 'WAG002' then do ;
		if MONTHLYGROSSINCOME = 0.00 then  GROSSINCOME = NETINCOME ;
		else if MONTHLYGROSSINCOME < NETINCOME then GROSSINCOME =MONTHLYGROSSINCOME * 52/12  ;
		else GROSSINCOME = MONTHLYGROSSINCOME ; 
	end;

	if WAGEFREQUENCYCODE = 'WAG003' then do ;
		if MONTHLYGROSSINCOME = 0.00 then  GROSSINCOME = NETINCOME ;
		else if MONTHLYGROSSINCOME < NETINCOME then GROSSINCOME =MONTHLYGROSSINCOME * 26/12  ;
		else GROSSINCOME = MONTHLYGROSSINCOME ; 
	end;

	if GROSSINCOME in (.,0) then RTI_Ratio=-1;
	else if RTIUNSECUREDREVOLVINGTOTALINSTAL= -99000800 then RTI_Ratio=-2;
	else if RTIUNSECUREDREVOLVINGTOTALINSTAL = . then RTI_Ratio=-2;
	else RTI_Ratio = RTIUNSECUREDREVOLVINGTOTALINSTAL/GROSSINCOME;
run;

data Scoring_Declines2;
	set Scoring_Declines2;
	format Appdayless date9.;
	Appdayless = intnx('day',input(Applicationdate,yymmdd10.) ,-1);
run;

data Scoring_Declines2;
	set Scoring_Declines2;
	format Est_Firstduedate date9.;
	Est_Firstduedate = intnx('day',Appdayless ,38);
run;

%let varlist1=RTI_Ratio;
%let path= \\neptune\sasa$\V5\Application Scorecard\Reject_Inference_Models\RTI\Buckets;

%macro ApplyScoring(Varlist1,Path) ;
	%do i = 1 %to %sysfunc(countw(&Varlist1));
	%let var = %scan(&Varlist1, &i);
	    %include "&Path.\&var._if_statement_.sas";
	    %include "&Path.\&var._WOE_if_statement_.sas"; 
	%end;
	*****************************************;
	** SAS Scoring Code for PROC Hplogistic;
	*****************************************;
	%include "&Path.\creditlogisticcode_3.sas";
%mend;

data Scoring_Declines_test;
	set Scoring_Declines2;
	%applyScoring(varlist1=&varlist1, Path=&Path);
	ProbDisb= P_Target1;
	ProbDisb_1 = 0.75;
run;

Data Scoring_Declines_test;
      set Scoring_Declines_test;
      RANDOMNUMBERGENARATOR= (ranuni(11)); 
      if DS_FLAG2=1 THEN DISBURSED=0; 
      else if RANDOMNUMBERGENARATOR <= ProbDisb_1 then DISBURSED=1;
      else DISBURSED=0;
run;

Data Rej_Include_test;
      set Scoring_Declines_test;
      where DISBURSED=1;
run;
/*NOTE: There were 82865 observations read from the data set WORK.SCORING_DECLINES_TEST.*/
/*      WHERE DISBURSED=1;*/
/*NOTE: The data set WORK.REJ_INCLUDE_TEST has 82865 observations and 208 variables.*/

Data Rejects;
      set Rej_Include_test;
      format Est_Firstduedate yymmdd8.;
      month2 = put(Est_Firstduedate,yymmn6.);
run;

Data New_Rejects;
      set Rejects;
      where failed_Affordability = 0;
run;
/*NOTE: There were 31342 observations read from the data set WORK.REJECTS.*/
/*      WHERE failed_Affordability=0;*/
/*NOTE: The data set WORK.NEW_REJECTS has 31342 observations and 209 variables.*/

%put  if month2 >= "&startoutcome" and  month2<="&endoutcome"; 
Data Monitoring_Rejects;
      set New_Rejects;
      if month2 >= "&startoutcome" and  month2<="&endoutcome"; 
run;
/*NOTE: The data set WORK.MONITORING_REJECTS has 17774 observations and 209 variables.*/
/*NOTE: Compressing data set WORK.MONITORING_REJECTS decreased size by 91.20 percent.*/

/*Score our rejects*/
Proc Sql;
	create table Rej_Base_June as 
	    select a.*,b.RI6_probability as RI6_probability_&runmonth,
                b.RI7_probability as RI7_probability_&runmonth , b.RI8_probability as RI8_probability_&runmonth, b.RI9_probability as RI9_probability_&runmonth,
                b.TF_probability,b.VAP_PrismScore_MI as VAP_PrismScore_MI_Cur
	    from Monitoring_Rejects a
	    left join Reject.Comp_pinpoint_sv_&month b
	    on a.nationalid=b.id_no;
quit;

proc freq data=New_Rejects;
	tables month2;
run;

proc sort data= Rej_Base_June nodupkey dupout=Dups;
      by tranappnumber;
run;

/*201808-201901*/
Data Rej_Base_June_2;
      set Rej_Base_June;
      if VAP_PrismScore_MI_Cur=. then Missing_Bur=1;
      else Missing_Bur=0;

      if  VAP_PrismScore_MI_Cur<=10 then Rej_Prob=0.45;
      else if thinfileindicator=1 and VAP_PrismScore_MI_Cur> 10 then Rej_Prob=TF_probability;
      else if month2 >= "&startoutcome" and month2 <= "&statement9." then Rej_Prob=RI9_probability_&runmonth;
      else if month2 = "&statement8." then Rej_Prob=RI8_probability_&runmonth;
      else if month2 = "&statement7." then Rej_Prob=RI7_probability_&runmonth;
      else if month2 = "&statement6." then Rej_Prob=RI6_probability_&runmonth;
run;

Data Rej_Base_June_2;
      set Rej_Base_June_2;
      Sample_Ind = 'REJECTS';
run;

Data Rej_Base_June_2;
	set Rej_Base_June_2;
	Disb_Target = Target;
	RANDOMNUMBERGENARATOR1= (ranuni(11));  
	if RANDOMNUMBERGENARATOR1 <= Rej_Prob then RI_Target=1;
	else RI_Target=0;
	if Sample_Ind ='DISBURSED' then Target=Disb_Target;
	else if Sample_Ind= 'REJECTS' then Target=RI_Target;
run;

Data New_Monitoring_Rejects;
      set Rej_Base_June_2;
run;

Data Rej_Base_June_3;
      set Rej_Base_June_2;
      drop Month;
run;

Data Rej_Base_June_3;
      set Rej_Base_June_3;
      rename Month2 = Month;
run;

Data reject.outcome_Rejects_&runmonth;
set Rej_Base_June_3;
run;

Data reject.Application_Rejects_v2_&runmonth;
set New_Rejects;
run;