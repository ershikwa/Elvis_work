OPTIONS NOSYNTAXCHECK ;
options compress = yes;
options mstored sasmstore=sasmacs; 

*libname sasmacs "\\neptune\credit$\AA_GROUP CREDIT\Scoring\Model Macros\"; 

*%include "\\neptune\sasa$\SAS_Automation\SAS_Autoexec\autoexec2.sas";

%include "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\macros\Calc_Gini.sas";
%include "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\macros\CreateMonthlyGini.sas";
%include "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\macros\giniovertime.sas";
%include "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\macros\checkifcolumnsexist.sas";

libname decile "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\SAS Decile Tables\Compuscan";
libname comp "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring";
libname V5 '\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\data';
libname lookup '\\mpwsas64\Core_Credit_Risk_Models\V5\Segmentation Models For Compuscan\lookup';
Libname Data1 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\App Seg1 Model\IV";
Libname Data2 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\App Seg2 Model\IV";
Libname Data3 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\App Seg3 Model\IV";
Libname Data4 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\App Seg4 Model\IV";
Libname Data5 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\App Seg5 Model\IV";
Libname seg1 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\App Seg1 Model\Data";
Libname seg2 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\App Seg2 Model\Data";
Libname seg3 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\App Seg3 Model\Data";
Libname seg4 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\App Seg4 Model\Data";
Libname seg5 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\App Seg5 Model\Data";
Libname Rejects "\\MPWSAS5\projects3\Compuscan\Reject Inference data";
Libname Rejects2 "\\Neptune\SASA$\V5\New_Rejects";
Libname Kat "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\TU Models\v570\Calibration";
Libname tu '\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets';


%let odbc = MPWAPS;

data _null_;
     call symput("enddate",cats("'",put(intnx('month',today(),-1,'end'),yymmddd10.),"'"));
     call symput("startdate",cats("'",put(intnx('month',today(),-13,'end'),yymmddd10.),"'"));
     call symput("actual_date", put(intnx("month", today(),-9,'end'),date9.));
     call symput("month", put(intnx("month", today(),-1,'end'),yymmn6.));
     call symput("prevmonth", put(intnx("month", today(),-2,'end'),yymmn6.));
	 call symput("prevprevmonth", put(intnx("month", today(),-3,'end'),yymmn6.));
	 call symput("reject_month", put(intnx("month", today(),-7,'end'),yymmn6.));
	 call symput("build_start", put(intnx("month", today(),-12,'end'),yymmn6.));
	 call symput("build_end", put(intnx("month", today(),-7,'end'),yymmn6.));
run;

/*** Check dates ***/
%put &startdate; 		*Monitoring month minus a year;
%put &enddate; 			*Monitoring month (last day of month);
%put &actual_date; 		*Monitoring month minus 9 months;
%put &month; 			*Monitoring month (month);
%put &prevmonth; 		*Monitoring month minus a month;
%put &prevprevmonth; 	*Monitoring month minus 2 months;
%put &reject_month; 	*Monitoring month minus 7 months;
%put &build_start; 		*Monitoring month minus 12 months;
%put &build_end; 		*Monitoring month minus 7 months;

/*** Someone need to prepare the application data ***/
data V5.Applicationbase_&month;
	set V5.Applicationbase_&month;
	Score = 1000-(CS_V560_Prob*1000);   *Use this if the score variable is not correct or not provided;
run;

proc format cntlin =decile._decile1_ fmtlib ;run; 
proc format cntlin =decile._decile2_ fmtlib ;run;
proc format cntlin =decile._decile3_ fmtlib ;run;
proc format cntlin =decile._decile4_ fmtlib ;run;
proc format cntlin =decile._decile5_ fmtlib ;run;

data Applicationbase_&month;
	set V5.Applicationbase_&month;
	seg = comp_seg;
	thinfileindicator = comp_thin;
run;

data Applicationbase_&month;    
     set Applicationbase_&month; 
     if seg = 1 then decile = put(Score,s1core.);
     else if seg = 2 then decile = put(Score,s2core.);
     else if seg = 3 then decile = put(Score,s3core.);
     else if seg = 4 then decile = put(Score,s4core.);
     else if seg = 5 then decile = put(Score,s5core.);
     decile_b = put(input(decile,8.)+1,z2.);
     decile_w =input(decile,8.)+1;
     decile_s = decile_w;
run;

proc freq data=Applicationbase_&month; tables decile / missing; run;


/***************************************************************************************/
/******************************** Getting DisbursedData ********************************/
/***************************************************************************************/

/*** Run this part if you have access to credit scoring ***/ 
/*** If you don't have access, you have to ask someone to run it for ***/ 
proc sql stimer; 
	connect to odbc (dsn=mpwaps);
	select * from connection to odbc (
	    drop table PRD_DataDistillery_data.dbo.final_model_Apps
	);
	disconnect from odbc;
quit;
data apps;

data final_model_Apps;
	set Applicationbase_&month (keep = tranappnumber uniqueid);
	if input(tranappnumber, best.) =. then delete;
run;
%Upload_APS(Set = final_model_apps , Server = Work, APS_ODBC = Prd_DDDa, APS_DB = PRD_DataDistillery_data , distribute = HASH([tranappnumber]));

proc sql stimer;        
		connect to ODBC (dsn=&odbc);
        create table comp.DisbursedBase as
        select * from connection to odbc (
            select A.tranappnumber,
                    B.Principaldebt ,
                    B.product ,
                    B.Contractual_3_LE9 ,
                    B.FirstDueMonth,
                    E.FirstDueDate,
                    E.product as product1,
                    cast(d.LNG_SCR as int) as TU_Score,
                    C.Final_score_1, C.FirstDueMonth as PredictorMonth
            from PRD_DataDistillery_data.dbo.final_model_Apps A
            inner join PRD_DataDistillery_data.dbo.Disbursement_Info E
            on a.tranappnumber = e.loanid
            left join PRD_DataDistillery_data.dbo.JS_Outcome_base_final B
            on a.tranappnumber = B.loanid
            left join CREDITPROFITABILITY.dbo.ELR_LOANESTIMATES_3_9_CALIB C
            on b.loanid = c.loanid
            left join PRD_PRESS.[capri].[CAPRI_BUR_PROFILE_TRANSUNION_PLSCORECARD] d
            on a.uniqueid = d.uniqueid
        ) ;
        disconnect from odbc ;
quit;


data comp.DisbursedBase; 
 set comp.DisbursedBase;
 if product ="" then product=product1;
run;
/***************************************************************************************/


/***************************************************************************************/
/***************************** Creating final DisbursedBase ****************************/
/***************************************************************************************/

proc sort data = Comp.DisbursedBase out=disb nodupkey;
	by tranappnumber;
run;

proc sql;
	create table comp.Disbursed_only2 as
		select *
		from Applicationbase_&month a inner join disb b	
		on a.tranappnumber = b.tranappnumber
		where seg ne .;
quit;

proc sort data = comp.Disbursed_only2 nodupkey;
	by tranappnumber;
run;

data Disbursed_only3;
set comp.Disbursed_only2;
RealMonth = datepart(FirstDueMonth);
if product = 'Card' then SecondMonth = datepart(Disbstartdate);
else if product = 'Loan' then SecondMonth = datepart(FirstDueMonth);
month2 = coalesce(RealMonth,SecondMonth);
month= put(month2,yymmn6.);
if month=. or month=0 then delete;
format month2 MONYY5.;
run;

/******************************/

 

data Disbursed_only4 ;
set Disbursed_only3 ;
if Month2 >= intnx('month',"&actual_date"d,-2,'begin') and Month2 <= intnx('month',"&actual_date"d,3,'end');
count=1;
target = Contractual_3_LE9 ;
randomnum = uniform(12) ; *12 is the seed, and the random does not change;
if Month2 <= "&actual_date"d then HaveActuals = 1 ;
else HaveActuals = 0 ;
if HaveActuals = 1 then Target = CONTRACTUAL_3_LE9 ;
else if (HaveActuals = 0 and randomnum <= Final_score_1) then Target = 1 ;
else if (HaveActuals = 0 and randomnum > Final_score_1) then Target = 0;
run;

data reject_data (keep=uniqueid tranappnumber);
	set Rejects2.REJECTS_SNAPSHOT_&month;
run;

proc sql stimer;
	connect to odbc (dsn=mpwaps);
	select * from connection to odbc (
		drop table PRD_DataDistillery_data.dbo.reject_data
	);
	disconnect from odbc;
quit;
%Upload_APS(Set =reject_data , Server =Work, APS_ODBC = Prd_DDDa, APS_DB = PRD_DataDistillery_data , distribute = HASH([tranappnumber]));

proc sql stimer;
	connect to ODBC (dsn=&odbc);
	create table rejectbase as 
	select * from connection to odbc ( 
		select A.tranappnumber,
			   cast(d.LNG_SCR as int) as TU_Score
		from PRD_DataDistillery_data.dbo.reject_data A 
		left join PRD_PRESS.[capri].[CAPRI_BUR_PROFILE_TRANSUNION_PLSCORECARD] d
		on a.uniqueid = d.uniqueid
	) ;
	disconnect from odbc ;
quit;

proc sort data=rejectbase nodupkey;
	by tranappnumber;
run;

proc sql;
	create table rejectfinal as
	    select b.TU_Score, a.*
	    from Rejects2.REJECTS_SNAPSHOT_&month a
		left join rejectbase b
	    on a.tranappnumber = b.tranappnumber;
quit;

proc sql;
	create table outcome_reject as
	    select b.target as target, b.month, b.Est_Firstduedate as FirstDueMonth, b.Sample_Ind, b.TU_Score,a.*
	    from Applicationbase_&month a inner join rejectfinal b
	    on a.tranappnumber = b.tranappnumber;
quit;

data outcome_reject;
	set outcome_reject;
	FirstDueMonth=dhms(FirstDueMonth,0,0,0);
	format FirstDueMonth datetime22.3;
run;

data Disbursed_only4;
	set Disbursed_only4;
	Sample_Ind = "DISBURSED";
run;

Data DisbursedBase_without_Rej;
 set Disbursed_only4;
if  datepart(FirstDueMonth) >= intnx('month',"&actual_date"d,-2,'begin') and datepart(FirstDueMonth) <= intnx('month',"&actual_date"d,3,'end');
run;
/*------------------------------------------------------------------*/
/* Change Control: */
/*------------------------------------------------------------------*/
/* Developer: Lindokuhle Masina */
/* Date: 30 June 2021*/
/*
Changes Made:
1.Removing the rejets
*/
/* Reason: Our reject inference model has broken due to us not receiving information on the OM. 
	In the mean time can we change our monitoring to only be on Disbursals and not with rejects included*/
/*------------------------------------------------------------------*/	

/*Data DisbursedBase_with_Rej;*/
/*set Disbursed_only4 outcome_reject; */
/*if  datepart(FirstDueMonth) >= intnx('month',"&actual_date"d,-2,'begin') and datepart(FirstDueMonth) <= intnx('month',"&actual_date"d,3,'end');*/
/*run;*/


proc freq data = DisbursedBase_without_Rej; tables month; run;
proc freq data = DisbursedBase_without_Rej; tables target; run;

data comp.Disbursedbase_&month;
	set DisbursedBase_without_Rej;
run;

proc sql;
    create table comp.Disbursedbase_&month as
        select *, ((Principaldebt/sum(Principaldebt)) * count(tranappnumber)) as weight
        from comp.Disbursedbase_&month;
quit;

%macro Loopthroughcal(Dset,name=,prob=, weight=);
	proc nlmixed data=&Dset ; parms a=1 c=0;
		x=1/(1+((&prob/(1-&prob))**(-a))*exp(c) );
		MODEL Target ~ BINARY(x);
		_ll = _ll*&weight;
		ODS OUTPUT ParameterEstimates= parameters (keep = Parameter Estimate);
	run;

	proc transpose data = parameters out = parameters ;
		var Estimate ;
		id Parameter ;
	run;

	data parameters (drop = _NAME_ ) ;
		set parameters ;
		Model = "&Name";
		Count = 1;
	run;

	proc append base = Parameters_&name data = parameters force ;
	quit;
%mend;
%Loopthroughcal(Dset=comp.Disbursedbase_&month, name=V637_Calib, prob=V635, weight=weight);

proc sql;
	create table comp.Disbursedbase_&month
		as select a.*,
		1/(1+(V635/(1-V635))**(-1*(c.a))*exp(c.c)) as V637
		from comp.Disbursedbase_&month a
		left join parameters_V637_calib c
		on a.count=c.count;
quit;

/*** Check TU and Comp disbursedbases if they match ***/
/*proc freq data=Comp.Disbursedbase_&month;*/
/*	tables month;*/
/*run;*/
/**/
/*proc freq data=tu.Disbursedbase4reblt_&month.;*/
/*    tables month / missing;*/
/*run;*/
/******************************************************/

data disb1 disb2 disb3 disb4 disb5;
      set comp.Disbursedbase_&month;
	  if seg = 1 then output disb1;
      else if seg = 2 then output disb2;
      else if seg = 3 then output disb3;
      else if seg = 4 then output disb4;
      else if seg = 5 then output disb5;
	  where maxSCORECARDVERSION <> 'V4';
run;

proc freq data=comp.Disbursedbase_&month; tables month/missing; run;

%create_scorecard_variables_list(numberofsegments=1, modeltype=C);
%create_scorecard_variables_list(numberofsegments=2, modeltype=C);
%create_scorecard_variables_list(numberofsegments=3, modeltype=C);
%create_scorecard_variables_list(numberofsegments=4, modeltype=C);
%create_scorecard_variables_list(numberofsegments=5, modeltype=C);


/***************************************************************************************/
/*********************************** Data preparation **********************************/
/***************************************************************************************/
proc sql;
	create table Applications_Rejects as
	    select b.I_target as I_target_rej, a.*
	    from Applicationbase_&month a inner join rejects2.Application_Rejects_&month b
	    on a.tranappnumber = b.tranappnumber;
quit;

proc freq data=rejects2.Application_Rejects_&month; tables month; run;

Data comp.Applications_Rejects;
set Applications_Rejects;
Contractual_3_LE9 = I_target_rej*1;
run;

Data NewAppbase;
set comp.Disbursed_only2;
month = put(input(ApplicationDate,yymmdd10.),yymmn6.);
target = Contractual_3_LE9;
run;
/*------------------------------------------------------------------*/
/* Change Control: */
/*------------------------------------------------------------------*/
/* Developer: Lindokuhle Masina */
/* Date: 30 June 2021*/
/*
Changes Made:
1.Removing the rejets
*/
/* Reason: Our reject inference model has broken due to us not receiving information on the OM. 
	In the mean time can we change our monitoring to only be on Disbursals and not with rejects included*/
/*------------------------------------------------------------------*/


/*Data NewAppbase;*/
/*set comp.Disbursed_only2 comp.Applications_Rejects;*/
/*month = put(input(ApplicationDate,yymmdd10.),yymmn6.);*/
/*target = Contractual_3_LE9;*/
/*run;*/

Data comp.NewAppbase_&month;
set NewAppbase;
if ApplicationDate > &startdate;
run;

data comp.Build_6Months; 
set comp.Newappbase_&month;
if appmonth >= &build_start and appmonth <= &build_end; 
run;

proc freq data=comp.Build_6Months; tables appMonth; run;

data build1 build2 build3 build4 build5;
      set comp.Build_6Months;
      if seg = 1 then output build1;
      else if seg = 2 then output build2;
      else if seg = 3 then output build3;
      else if seg = 4 then output build4;
      else if seg = 5 then output build5;
run;

%macro datapreparation(applicationbase=,numberofsegment=);
	%do seg = 1 %to &numberofsegment;
	    proc sql;
			create table _estimates_ as
			    select a.*, b.estimate 
			    from _estimates_&seg. a left join data&seg.._estimates_ b
			    on UPCASE(a.parameter) = UPCASE(tranwrd(b.parameter,"_W",""));
	    quit;
	    filename _temp_ temp;
	    data _null_;
			set  _estimates_;
			where upcase(parameter) not in ("DECILE","INTERCEPT");
			file _temp_;
			formula = cats(parameter,'_S = ',parameter,'_W*',Estimate,';');
			put formula;
	    run;
	    data segment_&seg.;
			set &applicationbase(where= (seg=&seg));
			month = put(input(ApplicationDate,yymmdd10.),yymmn6.);
			if seg = 1 then decile = put(Score,s1core.);
			else if seg = 2 then decile = put(Score,s2core.);
			else if seg = 3 then decile = put(Score,s3core.);
			else if seg = 4 then decile = put(Score,s4core.);
			else if seg = 5 then decile = put(Score,s5core.);
			decile_b = put(input(decile,8.)+1,z2.);
			decile_w =input(decile,8.)+1;
			decile_s = decile_w;
			count=1;
			segment = seg;
			%include _temp_;
	    run;
	    data build_&seg.;
			set build&seg(where= (seg=&seg) drop= decile);
			if seg = 1 then decile = put(Score,s1core.);
			else if seg = 2 then decile = put(Score,s2core.);
			else if seg = 3 then decile = put(Score,s3core.);
			else if seg = 4 then decile = put(Score,s4core.);
			else if seg = 5 then decile = put(Score,s5core.);
			decile_b = put(input(decile,8.)+1,z2.);
			decile_w =input(decile,8.)+1;
			decile_s = decile_w;
			Month = put(datepart(ApplicationDate),yymmn6.);

			count=1;
			%include _temp_;
	    run;
	    data disb&seg.;
			set disb&seg.;
			month2 = put(input(ApplicationDate,yymmdd10.),yymmn6.);
			Month = put(datepart(FirstDueMonth),yymmn6.);
			decile_b = put(input(decile,8.)+1,z2.);
			decile_w =input(decile,8.)+1;
			decile_s = decile_w;
			count=1;
			%include _temp_;
	    run;
	%end;
%mend;
%datapreparation(applicationbase=comp.NewAppbase_&month, numberofsegment=5);

data build;     
     set build_1 build_2 build_3 build_4 build_5;
run;


/***************************************************************************************/
/*********************************** PSI Calculations **********************************/
/***************************************************************************************/

%looppervariables_monitoring(segment=1,base=segment_1, build1=build_1,variable_list=&segment_1_list decile, outdataset=summarytable);
%looppervariables_monitoring(segment=2,base=segment_2, build1=build_2,variable_list=&segment_2_list decile,outdataset=summarytable);
%looppervariables_monitoring(segment=3,base=segment_3, build1=build_3,variable_list=&segment_3_list decile,outdataset=summarytable);
%looppervariables_monitoring(segment=4,base=segment_4, build1=build_4,variable_list=&segment_4_list decile,outdataset=summarytable);
%looppervariables_monitoring(segment=5,base=segment_5, build1=build_5,variable_list=&segment_5_list decile,outdataset=summarytable);

%psi_calculation_monitoring(build=build, base=comp.NewAppbase_&month,period=month,var=seg,psi_var=seg, outputdataset=comp.summarytable_&month);

%Macro PSIRenaming (seg);
	proc delete data = all_summarytable ; run;
	proc delete data = segments; run;
	%do i= 1 %to &seg;
		Proc sql;
		    create table PSI_&i. as 
				select a.* ,b.scorecardvariable
				from  summarytable_&i. a 
				left join _estimates_&i. b
				on upcase(a.variablename) = upcase(b.Parameter);
		quit;
		proc append base = all_summarytable data = psi_&i force; run;
		proc append base = segments data = segment_&i force ; run;
	%end;
%mend;
%PSIRenaming(5);

%let segment_0_list = DECILE;
%let segment_0_list_woe = DECILE_W;
%let segment_0_list_buckets = DECILE_B;
%let segment_0_list_S = DECILE_S;

%put &segment_0_list;


%macro looppervariables(segment=0,base=,build1=,variable_list=,outdataset=);
	data segment;
		set &base.;
	run;
	proc delete data = &outdataset._&segment; run;

	%do i = 1 %to %sysfunc(countw(&variable_list));
		%let vari = %scan(&variable_list, &i.);
		%psi_calculation_monitoring(build=&build1, base=segment,period=month,var=&vari,psi_var=&vari._S, outputdataset=&outdataset._&segment);
		data &outdataset._&segment;
			set &outdataset._&segment;
			seg = &segment;
		run;
	%end;
%mend;
%looppervariables(segment=0,base=segments, build1=build,variable_list=&segment_0_list decile,outdataset=summarytable);

%macro appendPSI(outdataset=);
	proc delete data = &outdataset; run;
	%do i = 0 %to 5;
		%if &i= 0 %then %do;
			data _tempPSI_;
				set summarytable_&i;
				length variable_name $32.;
				variable_name ="DECILE";
				segment = &i.;
			run;
		%end;
		%else %do;
			proc sql;
				create table _tempPSI_ as
				   select &i as segment, b.scorecardvariable as variable_name, a.*
				   from (select * from summarytable_&i) a left join _estimates_&i b
				   on  upcase(a.variablename) = (b.Parameter);
			quit;
		%end;
		proc append base = &outdataset  data=_tempPSI_ force; run;
	%end;
%mend;
%appendPSI(outdataset =Comp.Variables_Distribution_&month.); 


/*** Creating CSI table ***/
proc sql;
	create table max_psi as
		select  seg,variablename, max(psi) as psi
		from Comp.Variables_Distribution_&month.
		where variablename ne 'DECILE'
		group by seg, variablename;
quit;

data max_psi;
     set max_psi;
     month = "max csi";
run;

proc sql;
	create table last3month as 
		select distinct month
		from Comp.Variables_Distribution_&month.
		where month ne " BUILD"
		order by month desc;
quit;
data last3month;  set last3month(obs=3); run;
           
proc sql;
	create table last3monthdata as
		select  seg,a.month,variablename, psi
		from (select * from Comp.Variables_Distribution_&month. where variablename ne 'DECILE' )
		a inner join  last3month b
		on a.month = b.month;
quit;

data CSI_Distribution_&month.;
	length month $8.;
	set last3monthdata max_psi;
run;

proc sql;
    create table comp.CSI_Distribution_&month. as
		select b.NAME as variable_name, a.*
		from CSI_Distribution_&month. a 
		left join Lookup.Compscanvar_lookup b
		on upcase(a.variablename) = upcase(b.newcolumn)
		order by a.seg, a.month desc;
quit;

Data comp.CSI_Distribution_&month.;
	set comp.CSI_Distribution_&month.;
	if variable_name = " " then variable_name = variablename;
run;


/*** Creating variables_stability table ***/
proc sort data = Comp.variables_distribution_&month(keep = Month) nodupkey out = last_2_month;
     by descending Month;
run;

data last_2_month;
     set last_2_month(obs = 2);
run;

proc sql;
	create table variable_distribution as
	   select a.month, a.segment, cats('Segment ',a.segment) as Population, 
	             case when a.psi >=0.25 then 'Unstable %'
	                   when a.psi >=0.1 then 'Marginally Unstable %'
	                   else ' Stable %'
	             end as Reason 
	           ,count(distinct variablename) as numberofvariables
	           ,avg(psi) as avg_psi
	   from (select  * from Comp.Variables_distribution_&month where upcase(variablename) ne 'DECILE')  a, last_2_month b
	   where a.month = b.month
	   group by a.month, a.segment,3,4;
quit;

proc sql;
	create table reason_month as
	   select distinct a.month, 
	             case when a.psi >=0.25 then 'Unstable %'
	                   when a.psi >=0.1 then 'Marginally Unstable %'
	                   else ' Stable %'
	             end as Reason 
	   from (select * from Comp.Variables_distribution_&month where upcase(variablename) ne 'DECILE')  a, last_2_month b
	   where a.month = b.month
	   and segment ne 0
	   group by a.month, 2;
quit;

data reason_month2;
	set last_2_month;
	Reason = 'Unstable %';
run;

data reason_month3;
    set last_2_month;
    Reason = 'Marginally Unstable %';
run;

data reason_month4;
    set last_2_month;
    Reason = ' Stable %';
run;

proc append base=reason_month data= reason_month2; run;
proc append base=reason_month data= reason_month3; run;
proc append base=reason_month data= reason_month4; run;
proc sort data=reason_month noduprecs; by month Reason; run;

proc sql;
	create table segments as
	   select segment, count(distinct variablename) as total_variables 
	   from Comp.Variables_distribution_&month
	   where segment ne 0 and upcase(variablename) ne 'DECILE'
	   group by segment;
quit;

proc sql;
	create table reason2 as
	   select *
	   from reason_month a, segments b;
quit;

proc sort data = reason2;by month segment reason;run;
proc sort data = variable_distribution;	by month segment reason;run;

data comp.variables_stability_&month.;
     merge variable_distribution(in = a)
             reason2(in = b );
     by month segment reason;
     if b;
     if b and not a then numberofvariables=0;
     Population = cats('Segment ',segment);
     Stable_percentage = numberofvariables / total_variables;
run;


/***************************************************************************************/
/**************************** Trend Over Time Calculations *****************************/
/***************************************************************************************/

%PercentageSloping_Monitoring(inputdataset=disb1,listofvars=&segment_1_list_S,target=target, period=month, trendreporttable=Variables_trendreport_seg1,slopingreporttable=alltrendtable_seg1,segment=1);
%PercentageSloping_Monitoring(inputdataset=disb2,listofvars=&segment_2_list_S,target=target, period=month, trendreporttable=Variables_trendreport_seg2,slopingreporttable=alltrendtable_seg2,segment=2);
%PercentageSloping_Monitoring(inputdataset=disb3,listofvars=&segment_3_list_S,target=target, period=month, trendreporttable=Variables_trendreport_seg3,slopingreporttable=alltrendtable_seg3,segment=3);
%PercentageSloping_Monitoring(inputdataset=disb4,listofvars=&segment_4_list_S,target=target, period=month, trendreporttable=Variables_trendreport_seg4,slopingreporttable=alltrendtable_seg4,segment=4);
%PercentageSloping_Monitoring(inputdataset=disb5,listofvars=&segment_5_list_S,target=target, period=month, trendreporttable=Variables_trendreport_seg5,slopingreporttable=alltrendtable_seg5,segment=5);

%put &segment_1_list_S;
%macro AppendingSlopingTable (segment=,outdataset=);
	proc sql;
		create table _temptrendtable_ as
			select b.scorecardvariable as variable_name, a.*
			from Alltrendtable_seg&segment a left join _estimates_&segment b
			on upcase(tranwrd(a.variable_name,"_S"," ")) = upcase(b.Parameter);
	quit;
	data trendtables_&segment (keep =variable_name segment slope_rate Recommended_Action);
		set _temptrendtable_;
		if upcase(variable_name) = "" then delete;
		format Recommended_Action $500.;
		segment = &segment.;
		overall_threshold=overall_threshold/100;

		label variable_name ="Variable Name" overall_threshold = "Slope Rate" Recommended_Action ="Recommended Action" ;
		if overall_threshold >=0.8 then do;
		   Recommended_Action = cats("No Action ");
		end;
		else if overall_threshold <0.8 then do;
		   Recommended_Action =cats("Investigate if rebucketing or collapsing of bucket is required and Investigate if variable should be removed");
		end;
		rename overall_threshold = slope_rate;
	run;

	proc sql;
	   	create table Variables_trendreport_seg&segment as
			select b.scorecardvariable as variable_name, a.*
			from Variables_trendreport_seg&segment a left join _estimates_&segment b
			on upcase(tranwrd(a.VarName,"_S"," ")) = upcase(b.Parameter);
	quit;

	data Variables_trendreport_seg&segment;
		set Variables_trendreport_seg&segment;
		if upcase(VarName) = "DECILE_S" then delete;
		if variable_name = " " then variable_name = upcase(tranwrd(VarName,"_S"," "));
	run;
	proc append base = comp.Variables_trendreport_&month data = Variables_trendreport_seg&segment force; run;
	proc append base = &outdataset data = trendtables_&segment force; run;
%Mend;
%AppendingSlopingTable (segment=1,outdataset=comp.v5_sloperate_&month);
%AppendingSlopingTable (segment=2,outdataset=comp.v5_sloperate_&month);
%AppendingSlopingTable (segment=3,outdataset=comp.v5_sloperate_&month);
%AppendingSlopingTable (segment=4,outdataset=comp.v5_sloperate_&month);
%AppendingSlopingTable (segment=5,outdataset=comp.v5_sloperate_&month);


/***************************************************************************************/
/****************************** Contribution Calculations ******************************/
/***************************************************************************************/

%Macro T ();
	proc delete data = contrib; run;
	%do seg = 1 %to 5;
		proc delete data =VARIABLECONTRIBUTIONS VAR SEG&seg._CONTRIB; run;
		%ContribCalc_Montoring(Data&seg..Parameter_estimate, DISB&seg, SEG&seg._CONTRIB);
		data Comp.Seg&seg._contrib;
			set work.seg&seg._contrib; 
			segment = &seg;
		run;
		proc append base = contrib data=comp.seg&seg._contrib force; run;
	%end;
	proc sql;
		create table contrib2 as
			select b.name, a.*
			from contrib a 
			left join Lookup.Compscanvar_lookup b
			on upcase(a.Variable) = cats(upcase(b.newcolumn),"_W");
	quit;
	Data contrib2;
	     set contrib2;
	     New_Var = tranwrd((variable),"_W"," ");
	     if name = " " then NAME = New_Var;
	run;
%Mend;
%T ();

%Macro T (); 
	proc delete data=corr_percent; run;
	%do seg = 1 %to 5 ;
		proc sql; select Parameter into : modvars separated by ' ' from data&seg..Parameter_estimate where  estimate ne . and upcase(parameter) ne 'INTERCEPT'; quit;
		%Corr_Monitoring (disb&seg, &modvars ,segcorr&seg, Summary1, 101, -101) ;
		%let vcount = %sysfunc(countw(&modvars));
		data corr_percent&seg;
			set work.segcorr&seg;
			array vars [&vcount] &modvars;
			flag = 0;
			do i = 1 to &vcount;
			    if vars[i] ne 1 and vars[i] > 0.6 then do;
			         flag = flag+1;
			    end;
			end;
			corr_percent=1-flag/(&vcount-1);
		run;
		data Comp.corr_percent&seg;
			set work.corr_percent&seg;
			segment = &seg;
		run;
		proc append base=corr_percent data= comp.corr_percent&seg force; run;
	%end;
	data corr_percent (keep= _name_ segment corr_percent);
		set corr_percent;
	run;
	proc sql;
	    create table corr_percent2 as
			select b.name, a.*
			from corr_percent a 
			left join Lookup.Compscanvar_lookup b
			on upcase(a._name_) = cats(upcase(b.newcolumn),"_W")
			order by a.segment;
	quit;
	
	Data corr_percent2;
		set corr_percent2;
		New_Var = tranwrd((_name_),"_W"," ");
		if NAME = " " then NAME = New_Var;
	run;
	proc sort data = CORR_PERCENT2;
		by segment;
	run;
%Mend;
%T ();


/***************************************************************************************/
/****************************** Variables VIF Calculations *****************************/
/***************************************************************************************/

%macro VIF_Monitoring(Scoreset=,Vars=,NameOutput=,outdataset=) ; 
	proc delete data= VIFREPORT; run;
	data in ;
		set &Scoreset ;
		y = probit(uniform(1)); 
		keep &Vars y ;
	run;

	Proc reg data = in outvif OUTEST = WORK.VIF RIDGE=0 noprint ;
		model y = &Vars /vif collin;
	quit;

	data VIF ;
		set VIF ;
		format population $100. ;
		population = "&type";
		if _type_ = 'RIDGEVIF' ;
		keep Population &Vars;
	run;
	proc transpose data = VIF out = VIF (rename = ( col1 = &NameOutput))  Name = VARIABLE ;
	run;

	proc append base =  VIFREPORT data = VIF force ;
	quit;
	data VIFREPORT ;
		set VIFREPORT ;
		label Variable = "VAR";
	run;
%mend;


%Macro T (); 
	proc delete data = VIFALLSEG; run;
	%do seg = 1 %to 5;
		proc sql; select Parameter into : modvars separated by ' ' from data&seg..Parameter_estimate where  estimate ne . and upcase(parameter) ne 'INTERCEPT'; quit;
		%let type = VIF;
		%VIF_Monitoring(Scoreset=disb&seg,Vars=&modvars,NameOutput=vif,outdataset=vif_seg&seg); 
		data Comp.vif_seg&seg;
			set work.VIFREPORT;	
			segment = &seg;
		run;
		proc append base = VIFALLSEG data = Comp.vif_seg&seg force ; run;
	%end;

	proc sql;
	    create table VIF2 as
			select b.name, a.*
			from VIFALLSEG a 
			left join Lookup.Compscanvar_lookup b
			on upcase(a.variable) = cats(upcase(b.newcolumn),"_W");
	quit;
	data VIF2;
		set VIF2;
		new_var = tranwrd((variable),"_W"," ");
		if name = " " then name = new_var;
	run;
%Mend;
%T ();


/***************************************************************************************/
/********************************** Confidence Bands ***********************************/
/***************************************************************************************/

%macro plotconfidencebands_report(inputdata=, segment=);
	proc delete data=seg_summary&segment; run;
	proc sql noprint;
		select cats(parameter,'_W') into : confidvar separated by ' ' 
		from _estimates_&segment
		;
	quit;
	proc sql noprint;
		select scorecardvariable into : descriptionvar separated by ' ' 
		from _estimates_&segment;
	quit;

	%do u = 1 %to %sysfunc(countw(&confidvar));
		%let var = %scan(&confidvar, &u);
		%let dvar = %scan(&descriptionvar,&u);
		
		proc sql noprint;
			create table allout as
				select &var,count(*)
				from &inputdata
				group by &var;
		quit;
		%do i = 1 %to %obscnt(allout);

			data onescore;
				set allout;
				if _n_ = &i;
			run;

			proc sql noprint; select cats("&dvar woe : ",&var) into : score  from onescore ; quit;

			proc sql noprint;
				create table Alloutx as 
					select * from &inputdata
					where &var in (select &var from onescore );
			quit;

		    %let position = %sysfunc( mod(&i, 4) );
		    %if &position = 1 %then %position_report(0,0);
		    %else %if &position = 2 %then %position_report(0,4);
		    %else %if &position = 3 %then %position_report(4.5,0);
		    %else %if &position = 0 %then %position_report(4.5,4.5);

		    %let width = 4;
		    %let height = 4;
		    %if &position = 1 %then %do;
		          ods pdf startpage = now;
		          ods layout start;
		    %end;

		    ods region y=&y.in x=&x.in width=&width.in height=&height.in;
		    %VarCheckConfidenceBand1_Report(Alloutx, month, v637, target, Principaldebt ,0,0,4,4, &score ) ;

			data summary1;
                    set summary1;
                    length variablename $40 bin 8.;
                    if actual >= lowerbound and actual <= upperbound then flag = 0;
                    else flag = 1;
                    bin=&i;
                    variablename ="&var" ;
					count = 1;
            run;

            proc append base= seg_summary&segment data=summary1 force ; run;

			%let obs = %obscnt(allout);
			%let count = %sysfunc( mod(&obs, 4) );

		    %if &position = 0  %then %do;
		          ods layout end ;
		    %end;

			%if (%obscnt(allout)= &i.) %then %do;
		          ods layout end ;
		    %end;
		%end;
	%end;
%mend;


options mprint mlogic;
%Macro T (); 
	proc delete data = confidence; run;
	%do seg = 1 %to 5;
		%plotconfidencebands_report(inputdata=disb&seg, segment=&seg);
		data seg_summary&seg;
		  set seg_summary&seg;
		  segment = &seg;
		run;
		proc sql;
		  	create table Comp.SEG&seg._Confidence  as
		        select distinct(variablename), segment, sum(flag)/sum(count) as No_of_buckets_outside_CB
				from seg_summary&seg
		        group by variablename;
		quit;
		proc append base = confidence data =comp.seg&seg._confidence;
	%end;
	proc sql;
	    create table confidence2 as
			select b.name, a.*
			from confidence a 
			left join lookup.Compscanvar_lookup b
			on upcase(a.variablename) = cats(upcase(b.newcolumn),"_W");
	quit;

	Data confidence2;
		set confidence2;
		New_Var = tranwrd((variablename),"_W"," ");
		if NAME = " " then NAME = New_Var;
	run;
%Mend;
%T ();

proc sql;
	create table comp.v5_sloperate_&month as
		select a.*, b.corr_percent as correlation, c.contribution, d.vif, e.No_of_buckets_outside_CB
		from comp.v5_sloperate_&month a
		left join CORR_PERCENT2 b
		on upcase(a.variable_name) = upcase(b.NAME)
		and a.segment = b.segment
		left join CONTRIB2 c
		on  upcase(a.variable_name) = upcase(c.NAME)
		and a.segment = c.segment
		left join VIF2 d
		on  upcase(a.variable_name) = upcase(d.NAME)
		and a.segment = d.segment
		left join CONFIDENCE2 e
		on  upcase(a.variable_name) = upcase(e.NAME)
		and a.segment = e.segment;
quit;


/***************************************************************************************/
/********************* Calculate the month Ginis for the variables *********************/
/***************************************************************************************/

options mprint mlogic;
%GiniPerVariable_Monitoring(segment=1,inputdata=disb1,FinalScoreField=CS_V560_Prob,period=month,target=target,outputdata=Ginis1 );
%GiniPerVariable_Monitoring(segment=2,inputdata=disb2,FinalScoreField=CS_V560_Prob,period=month,target=target,outputdata=Ginis2 );
%GiniPerVariable_Monitoring(segment=3,inputdata=disb3,FinalScoreField=CS_V560_Prob,period=month,target=target,outputdata=Ginis3 );
%GiniPerVariable_Monitoring(segment=4,inputdata=disb4,FinalScoreField=CS_V560_Prob,period=month,target=target,outputdata=Ginis4 );
%GiniPerVariable_Monitoring(segment=5,inputdata=disb5,FinalScoreField=CS_V560_Prob,period=month,target=target,outputdata=Ginis5 );

%macro appendginis(outdataset=);
     %do i = 1 %to 5;
		proc sql;
			create table _tempgini_ as
				select b.scorecardvariable as variable_name,&i as segment, a.*
				from (select * from Ginis&i ) a left join _estimates_&i b
				on  upcase(a.VarName) = (b.Parameter);
		quit;
		proc append base = &outdataset data=_tempgini_; run;
     %end;

     Data &outdataset;
	     set &outdataset;
	     New_Var = tranwrd((varname),"_W"," ");
	     if variable_name = " " then variable_name = New_Var;
	run; 
%mend;
%appendginis(outdataset=comp.variables_gini_summary_&month.);


/***************************************************************************************/
/*********************** Calculate overall gini for each segments **********************/
/***************************************************************************************/

%macro calcginimetrics(segment =0,indataset=, period=,target=, listofvargini=);
     %checkifcolumnsexist(indataset=&indataset,outdataset=missingvariables,columnlist=&period &target &listofvargini);
     %if %obscnt(missingvariables) = 0 %then %do;
           data score_table;
                set &indataset(keep = &target &period &listofvargini);
           run;
           proc delete data = _temp_; run;
           proc delete data = ginipersegment_seg&segment; run;

           %do v = 1 %to %sysfunc(countw(&listofvargini));
                %let scorevar = %scan(&listofvargini,&v.);
                proc delete data = &scorevar.; run;
                proc delete data = _temp1_; run;
                %CreateMonthlyGini(Dset =score_table , TargetVar = &target  , Score = &scorevar ,   Measurement = &period, outdataset =&scorevar.  ) ;
                data _temp1_(keep = segment &period Score_type gini);
                     set &scorevar.;
                     length Score_type $32.;
                     Score_type ="&scorevar.";
                     segment = &segment;
                run;
                proc append base = ginipersegment_seg&segment data = _temp1_ force;
                RUN;            
           %end;

           proc delete data = overallgini_seg&segment;
           RUN;
           %do v = 1 %to %sysfunc(countw(&listofvargini));
                %let scorevar = %scan(&listofvargini,&v.); 
                proc delete data = _temp_; run;
                proc delete data = &scorevar; run;
                %Calc_Gini (Predicted_col=&scorevar, Results_table = score_table , Target_Variable = &target , Gini_output = &scorevar );
                data _temp_(keep = segment score_type gini);
                     set &scorevar;
                     length Score_type $32.;
                     Score_type ="&scorevar.";
                     segment = &segment;
                run;
                proc append base = overallgini_seg&segment data =_temp_ force; 
                run;
           %end;
           Proc sort data=ginipersegment_seg&segment;
                by Month;
           run;
           proc transpose data=ginipersegment_seg&segment out=Gini_trans(drop=_name_);
                by Month;
                id score_type;
                var gini;
           run;;
           data ginipersegment_seg&segment;
                set Gini_trans;
                segment = &segment;
           run;
           proc append base =comp.ginipersegment_summary_&month data = ginipersegment_seg&segment force; run;
           proc append base = comp.overallgini_summary_&month data = overallgini_seg&segment force; run;
     %end;
     %else %do;
           %put One column supplied does not exist ;
     %end;      
%mend;

Data Disbursedbase5;
     set comp.Disbursedbase_&month;
     Compuscan_Generic = prismscoremi;
     Tu_Generic = Tu_Score;
     Compuscan_Prob = CS_V560_Prob;
	  where maxSCORECARDVERSION <> 'V4';
run;

data segment_1_gini segment_2_gini segment_3_gini segment_4_gini segment_5_gini;
     set Disbursedbase5;
     if seg=1 then output segment_1_gini ;
     else if seg=2 then output segment_2_gini ;
     else if seg=3 then output segment_3_gini ;
     else if seg=4 then output segment_4_gini ;
     else if seg=5 then output segment_5_gini ;
run;

option mprint mlogic symbolgen;
%CalcGinimetrics(segment =1,indataset=segment_1_gini, period=month,target=target, listofvargini=Compuscan_Prob  Compuscan_Generic Tu_Generic TU_V570_prob V6_Prob3 V635 V636);
%CalcGinimetrics(segment =2,indataset=segment_2_gini, period=month,target=target, listofvargini=Compuscan_Prob  Compuscan_Generic Tu_Generic TU_V570_prob V6_Prob3 V635 V636);
%CalcGinimetrics(segment =3,indataset=segment_3_gini, period=month,target=target, listofvargini=Compuscan_Prob  Compuscan_Generic Tu_Generic TU_V570_prob V6_Prob3 V635 V636);
%CalcGinimetrics(segment =4,indataset=segment_4_gini, period=month,target=target, listofvargini=Compuscan_Prob  Compuscan_Generic Tu_Generic TU_V570_prob V6_Prob3 V635 V636);
%CalcGinimetrics(segment =5,indataset=segment_5_gini, period=month,target=target, listofvargini=Compuscan_Prob  Compuscan_Generic Tu_Generic TU_V570_prob V6_Prob3 V635 V636);
%CalcGinimetrics(segment =0,indataset=Disbursedbase5, period=month,target=target, listofvargini=Compuscan_Prob  Compuscan_Generic Tu_Generic TU_V570_prob V6_Prob3 V635 V636);

data Comp.Ginipersegment_summary_&month;
     set  GINIPERSEGMENT_SEG1 GINIPERSEGMENT_SEG2 GINIPERSEGMENT_SEG3 GINIPERSEGMENT_SEG4
          GINIPERSEGMENT_SEG5 GINIPERSEGMENT_SEG0;
     rename Compuscan_Prob=V560_Comp_Prob Compuscan_Generic=Comp_Generic_Score Tu_Generic=TU_Generic_Score 
      	 TU_V570_prob=V570_TU_V570_prob V6_Prob3=V622_Prob V635=V635_Prob V636=V636_Prob month=First_Due_Month;
run;



/***************************************************************************************/
/**************************** Calibrations on V560 and V570 ****************************/
/***************************************************************************************/

/*** Start of calibration - Creation of a and c for segment split ***/
%macro Loopthroughcal(Dset,seg=,name=,prob=, Segmentation=, weight=);
      proc nlmixed data=&Dset (where = (&Segmentation = &seg))  ; 
            parms a=1 c=0;
            x=1/(1+((&prob/(1-&prob))**(-a))*exp(c) );
            MODEL Target ~ BINARY(x);
            _ll = _ll*&weight; 
            ODS OUTPUT ParameterEstimates= parameters (keep = Parameter Estimate);
      run;
      proc transpose data =  parameters out = parameters ;
            var Estimate ;
            id Parameter ;
      run;
      data parameters (drop = _NAME_ ) ;
            set parameters ; 
            Model = "&Name";
            &Name = &seg ;
      run;
      proc append base  = Parameters_&name data = parameters force ;
      quit;
%mend; 

/*** Check which banks are in the data - use for bank calibration ***/
proc freq data=comp.disbursedbase_&month;
	tables institutioncode;
run;

%macro calibration(Input=,Output=);
    proc sql;
        create table subset as
            select *, ((Principaldebt/sum(Principaldebt))*count(tranappnumber)) as weight
            from &Input;
    quit;     

	/*** Segment Calibration ***/
	%Loopthroughcal(Dset=subset, seg=1, name=V561_seg, prob=CS_V560_Prob, Segmentation=seg, weight=weight);
    %Loopthroughcal(Dset=subset, seg=2, name=V561_Seg, prob=CS_V560_Prob, Segmentation=seg, weight=weight);
    %Loopthroughcal(Dset=subset, seg=3, name=V561_Seg, prob=CS_V560_Prob, Segmentation=seg, weight=weight);
    %Loopthroughcal(Dset=subset, seg=4, name=V561_Seg, prob=CS_V560_Prob, Segmentation=seg, weight=weight);
    %Loopthroughcal(Dset=subset, seg=5, name=V561_Seg, prob=CS_V560_Prob, Segmentation=seg, weight=weight);     

	%Loopthroughcal(Dset=subset, seg=1, name=V571_seg, prob=TU_V570_prob, Segmentation=Tu_seg, weight=weight);
    %Loopthroughcal(Dset=subset, seg=2, name=V571_Seg, prob=TU_V570_prob, Segmentation=Tu_seg, weight=weight);
    %Loopthroughcal(Dset=subset, seg=3, name=V571_Seg, prob=TU_V570_prob, Segmentation=Tu_seg, weight=weight);
    %Loopthroughcal(Dset=subset, seg=4, name=V571_Seg, prob=TU_V570_prob, Segmentation=Tu_seg, weight=weight);
    %Loopthroughcal(Dset=subset, seg=5, name=V571_Seg, prob=TU_V570_prob, Segmentation=Tu_seg, weight=weight);     

	/*** Apply calibration ***/
	proc sql;
          create table Segment_calib
          as select a.*,
          1/(1+(CS_V560_Prob/(1-CS_V560_Prob))**(-1*(b.a))*exp(b.c)) as V561,
          1/(1+(TU_V570_prob/(1-TU_V570_prob))**(-1*(c.a))*exp(c.c)) as V571
          from subset a
          left join parameters_v561_seg b
          on a.seg = b.v561_Seg
          left join parameters_V571_seg c
          on a.seg = c.v571_Seg;
    quit;     	

	/*** Bank Calibration ***/
	proc sort data=Segment_calib nodupkey out=predata;
        by tranappnumber;
    run;     

	/*** Rename new institutioncodes or empty cases ***/
	data predata;
          set predata;
          if compress(INSTITUTIONCODE) not in ('BNKABL', 'BNKABS', 'BNKCAP', 'BNKFNB', 'BNKNED', 'BNKSTD', 'BNKOTH') then INSTITUTIONCODE ='BNKOTH';
    run;     

	%Loopthroughcal(Dset=predata, seg='BNKABL', prob=V561, name=V562_INSTITUTIONCODE, Segmentation=INSTITUTIONCODE, weight=weight);
    %Loopthroughcal(Dset=predata, seg='BNKABS', prob=V561, name=V562_INSTITUTIONCODE, Segmentation=INSTITUTIONCODE, weight=weight);
    %Loopthroughcal(Dset=predata, seg='BNKCAP', prob=V561, name=V562_INSTITUTIONCODE, Segmentation=INSTITUTIONCODE, weight=weight);
    %Loopthroughcal(Dset=predata, seg='BNKFNB', prob=V561, name=V562_INSTITUTIONCODE, Segmentation=INSTITUTIONCODE, weight=weight);
    %Loopthroughcal(Dset=predata, seg='BNKNED', prob=V561, name=V562_INSTITUTIONCODE, Segmentation=INSTITUTIONCODE, weight=weight);
    %Loopthroughcal(Dset=predata, seg='BNKSTD', prob=V561, name=V562_INSTITUTIONCODE, Segmentation=INSTITUTIONCODE, weight=weight);
    %Loopthroughcal(Dset=predata, seg='BNKOTH', prob=V561, name=V562_INSTITUTIONCODE, Segmentation=INSTITUTIONCODE, weight=weight);     
*	%Loopthroughcal(Dset=predata, seg='BNKINV', prob=V561, name=V562_INSTITUTIONCODE, Segmentation=INSTITUTIONCODE, weight=weight);

	%Loopthroughcal(Dset=predata, seg='BNKABL', prob=V571, name=V572_INSTITUTIONCODE, Segmentation=INSTITUTIONCODE, weight=weight);
    %Loopthroughcal(Dset=predata, seg='BNKABS', prob=V571, name=V572_INSTITUTIONCODE, Segmentation=INSTITUTIONCODE, weight=weight);
    %Loopthroughcal(Dset=predata, seg='BNKCAP', prob=V571, name=V572_INSTITUTIONCODE, Segmentation=INSTITUTIONCODE, weight=weight);
    %Loopthroughcal(Dset=predata, seg='BNKFNB', prob=V571, name=V572_INSTITUTIONCODE, Segmentation=INSTITUTIONCODE, weight=weight);
    %Loopthroughcal(Dset=predata, seg='BNKNED', prob=V571, name=V572_INSTITUTIONCODE, Segmentation=INSTITUTIONCODE, weight=weight);
    %Loopthroughcal(Dset=predata, seg='BNKSTD', prob=V571, name=V572_INSTITUTIONCODE, Segmentation=INSTITUTIONCODE, weight=weight);
    %Loopthroughcal(Dset=predata, seg='BNKOTH', prob=V571, name=V572_INSTITUTIONCODE, Segmentation=INSTITUTIONCODE, weight=weight);     
*   %Loopthroughcal(Dset=predata, seg='BNKINV', prob=V571, name=V572_INSTITUTIONCODE, Segmentation=INSTITUTIONCODE, weight=weight);     

	/*** Apply calibration ***/
	proc sql;
          create table Bank_Calibration
          as select a.*,
          1/(1+(V561/(1-V561))**(-1*(b.a))*exp(b.c)) as V562,
          1/(1+(V571/(1-V571))**(-1*(c.a))*exp(c.c)) as V572
          from predata a
          left join PARAMETERS_V562_INSTITUTIONCODE b
          on a.Institutioncode = b.V562_Institutioncode
          left join PARAMETERS_V572_INSTITUTIONCODE c
          on a.Institutioncode = c.V572_Institutioncode;
    quit;     

	proc sort data=Bank_Calibration nodupkey out=&Output;
        by tranappnumber;
    run;
%mend;

%calibration(Input=comp.Disbursedbase_&month, Output=comp.Calibrated_data);



/**********************************************************************/
/***************** Ginis for Calibration Data on V560 *****************/
/**********************************************************************/

/****************************** Overall *******************************/
%Calc_Gini (V562, comp.Calibrated_data, target, GiniTable_V562);
%Calc_Gini (V572, comp.Calibrated_data, target, GiniTable_V572);

data GiniTable_V562 (keep= Gini Score_type segment);
set GiniTable_V562;
segment = 0;
Score_type = "V562 Comp Prob";
run;

data GiniTable_V572 (keep= Gini Score_type segment);
set GiniTable_V572;
segment = 0;
Score_type = "V572 TU Prob";
run;


/**************************** Per Segment *****************************/
%macro Create_gini_tables(Predicted_col, Modelname, Dset, numberofsegment, Target_Variable, Gini_output);
	%do seg = 1 %to &numberofsegment;
		data &Dset._seg&seg;
			set &Dset;
			if seg = &seg;
		run;

		%Calc_Gini(&Predicted_col, &Dset._seg&seg, &Target_Variable, &Gini_output._seg&seg);

		data &Gini_output._seg&seg (keep= Gini Score_type segment);
			set &Gini_output._seg&seg;
			segment = &seg;
			Score_type = "&Predicted_col. &Modelname Prob";
		run;

		proc append base=&Gini_output data=&Gini_output._seg&seg force; run;
	%end;
%mend;

%Create_gini_tables(V562, Comp, comp.Calibrated_data, 5, target, GiniTable_V562)
%Create_gini_tables(V572, TU, comp.Calibrated_data, 5, target, GiniTable_V572)


/************************** Ginis over time ***************************/
%macro Create_ginipermonth(numberofsegment, indataset, period, target, vari);
	%do seg = 0 %to &numberofsegment;

	    %if &seg = 0 %then %do;
			data score_table;
				set &indataset;
			run;
	    %end;

		%else %do;
			data score_table;
				set &indataset;
				if seg = &seg;
			run;
		%end;

		data score_table ;
			set score_table (keep = &target &period &vari);
		run;

		proc delete data = calib_giniperseg_seg&seg; run;
		proc delete data = &vari.; run;
		proc delete data = _temp1_; run;

		%CreateMonthlyGini(Dset=score_table, TargetVar=&target, Score=&vari, Measurement=&period, outdataset=&vari.) ;

		data _temp1_(keep = segment &period Score_type gini);
		     set &vari;
		     length Score_type $32.;
		     Score_type ="&vari.";
		     Segment = &seg;
		run;

		proc append base=calib_giniperseg_seg&seg data=_temp1_ force;
		run; 

		Proc sort data=calib_giniperseg_seg&seg;
		    by &period;
		run;

		proc transpose data=calib_giniperseg_seg&seg out=Gini_trans(drop=_name_);
		    by Month;
		    id Score_type;
		    var gini;
		run;

		data calib_giniperseg_seg&seg;
		    set Gini_trans;
		    segment = &seg;
		run;

		proc append base=giniperseg_sum_&vari data=calib_giniperseg_seg&seg force; run;

	%end;
%mend;

%Create_ginipermonth(numberofsegment=5, indataset=comp.Calibrated_data, period=month, target=target, vari=V562);
%Create_ginipermonth(numberofsegment=5, indataset=comp.Calibrated_data, period=month, target=target, vari=V572);

/**********************************************************************/


/*** Create ginipersegment_summary table ***/
proc sql;
	create table comp.ginipersegment_summary_&month as
		select a.First_Due_Month, a.V560_Comp_Prob, a.Comp_Generic_Score, a.TU_Generic_Score, 
			a.V570_TU_V570_prob, a.V622_Prob, a.V635_Prob, a.V636_Prob, b.V562 as V562_Comp_Prob, c.V572 as V572_TU_V570_prob, a.segment
			from Comp.Ginipersegment_summary_&month a left join giniperseg_sum_V562 b
				on a.First_Due_Month = b.month and a.segment = b.segment
			left join giniperseg_sum_V572 c
				on b.month = c.month and b.segment = c.segment;
quit;


/*** Create currentmodel_benchmark table ***/
data Comp.overallgini_summary_&month;
set Comp.overallgini_summary_&month GiniTable_V562 GiniTable_V572 comp.GiniTable_Build;
run;

proc sql;
	create table Comp.currentmodel_benchmark_&month as
		select distinct  a.*,b.gini as Current_gini, (b.gini-a.gini)/a.gini as Relative_lift
		from Comp.overallgini_summary_&month a,
			(select segment, gini from Comp.overallgini_summary_&month
			where score_type='Compuscan_Prob') b
		where a.segment=b.segment;
quit;

data Comp.currentmodel_benchmark_&month;
	set Comp.currentmodel_benchmark_&month;
    format Recommended_Action $500.;
   
	if Relative_lift >=-0.10 then do;
	   	Recommended_Action = cats("No Action ");
	end;
	else if Relative_lift <-0.10 and Relative_lift >-0.15 then do;
		Recommended_Action =cats("Check Additional Metrics and Establish if a score to Risk Calibration is required");
	end;
	else if Relative_lift <-0.15 then do;
	   Recommended_Action =cats("Check Additional Metrics and Establish if a score to Risk Calibration is required");
	end;
run;

data Comp.currentmodel_benchmark_&month.;
     set Comp.currentmodel_benchmark_&month.;
     if score_type='Compuscan_Generic' then score_type ='Comp Generic Score';
     else if score_type='Tu_Generic' then score_type ='TU Generic Score';
     else if score_type='Compuscan_Prob' then score_type ='V560 Comp Prob';
	 else if score_type='TU_V570_prob' then score_type ='V570 TU Prob';
	 else if score_type='V6_Prob3' then score_type ='V622 Prob';
	 else if score_type='V635' then score_type ='V635 Prob';
	 else if score_type='V636' then score_type ='V636 Prob';
run;



/**********************************************************************/
/************* Save datasets on cred_scoring for Power BI *************/
/**********************************************************************/

/*** Only run this part if you have write access to cred_scoring ***/
libname cred_scr odbc dsn=cred_scoring schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.v5_Gini_relative_xml; run;
proc sql;
     create table  cred_scr.v5_Gini_relative_xml(BULKLOAD=YES) as
           select  distinct * 
           from Comp.currentmodel_benchmark_&month.;
quit;

proc delete data =cred_scr.Gini_summary_xml; run;
proc sql;
     create table  cred_scr.Gini_summary_xml(BULKLOAD=YES) as
           select  distinct * 
           from Comp.ginipersegment_summary_&month;
quit;

proc delete data =cred_scr.v5_Segment_distribution_xml; run;
proc sql;
     create table  cred_scr.v5_Segment_distribution_xml(BULKLOAD=YES) as
           select  distinct * 
           from Comp.Summarytable_&month;
quit;

Data Variables_Distribution_&month.;
set Comp.Variables_Distribution_&month.;
label month = App_month;
run;

proc delete data =cred_scr.distribution_summary_xml; run;
proc sql;
     create table  cred_scr.distribution_summary_xml(BULKLOAD=YES) as
           select  distinct * 
           from Variables_Distribution_&month.;
quit;

proc delete data =cred_scr.V5_Variables_stability_xml; run;
proc sql;
     create table  cred_scr.V5_Variables_stability_xml(BULKLOAD=YES) as
           select  distinct * 
           from Comp.variables_stability_&month.;
quit;

proc delete data =cred_scr.v5_sloperate_xml; run;
proc sql;
     create table  cred_scr.v5_sloperate_xml(BULKLOAD=YES) as
           select  distinct * 
           from Comp.V5_sloperate_&month.;
quit;

proc delete data =cred_scr.CSI_Distribution_xml; run;
proc sql;
     create table  cred_scr.CSI_Distribution_xml(BULKLOAD=YES) as
           select  distinct * 
           from Comp.CSI_Distribution_&month.;
quit;

proc delete data =cred_scr.v5_Segment_distribution_reblt; run;
proc sql;
                create table  cred_scr.v5_Segment_distribution_reblt(BULKLOAD=YES) as
                                select  distinct * 
                                from Comp.Summarytable_&month;
quit;
/*** Refresh Power BI ***/


/**********************************************************************/
/************************** Creating reports **************************/
/**********************************************************************/
options dlcreatedir;
libname reports "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\Reports\&month.";
%let reports = \\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\Reports\&month.;

/*** Create V5 Comp monitoring for segments reports ***/
%macro buildreport(seg);
     options nodate nonumber;
     Title ; 
     TITLE1; TITLE2;
     Footnote;
     Footnote1;
     Footnote2;
     options orientation=landscape;
     ods pdf body = "&reports\V5 Comp monitoring for segment &seg. &month..pdf" ;
	%macro plotpsi(var=,title1=,dset=,xlbl=);
		Title "&title1";
		proc sgplot data=&dset ;
			where upcase(VariableName) in ("&var")  ;
			vbar month / response = percent stat = mean group = scores  NOSTATLABEL  BARWIDTH = 0.8 ;  
			vline month / response = psi y2axis stat = mean group = scores ;
			vline month / response = marginal_stable y2axis stat = mean group = scores ;
			vline month / response = Unstable y2axis stat = mean group = scores ;
			yaxis label = 'Percentage'; 
			xaxis label = "&xlbl";
			y2axis label = 'PSI' min = 0 max = 1 ;
		run;
		Title;
	%mend;

	proc sql;
		create table _estimates_ as
			select tranwrd(a.parameter,"_W"," ") as parameter,  b.NAME as scorecardvariable
			from data&seg.._estimates_ a left outer join Lookup.Compscanvar_lookup b
			on  upcase(a.Parameter) = cats(upcase(b.newcolumn),"_W");
    quit;

	Data _estimates_;
		set _estimates_;
		if parameter = "Intercept" then delete;
		if scorecardvariable = " " then scorecardvariable = parameter;
	run;

     data _null_ ;
         set _estimates_  ;
         rownum= _n_;
         call symput (compress("X"||rownum),upcase(Parameter));
         call symput (compress("Y"||rownum),upcase(scorecardvariable));
         call symput ('NumVars',compress(rownum));
     run;
     %do i = 1 %to &NumVars;
           %let var = &&X&i..;
           ods pdf startpage = now;
           ods layout start;
           ods region x = 1in y = 0in;
           ods text = "Performance Monitoring Segment&seg.: &&Y&i.." ;

           ods region y=0.5in x=0in width=4.5in height=4in;

           ods graphics / reset attrpriority=color;
           Title "Gini over Time";
           footnote "&&Y&i..";
           proc sort data=Ginis&seg;
                 by Month;
           run;
           data ginitabletemp;;
                 set Ginis&seg;
                 where upcase(VarName) in ("OVERALLSEGMENT","&var"); 
                 varname = tranwrd(varname,upcase("&var"),upcase("&&Y&i.."));
           run;
           proc sgplot data= ginitabletemp;
                 series x=Month y=Gini / group= Varname  lineattrs= (pattern=solid Thickness = 2  ) ;
                 yaxis min = 0  grid offsetmin=.05 offsetmax=.05;
                 xaxis  grid  ;
                 keylegend / Title = '' ;
           run;
           Title;
           footnote;

           ods region y=0.5in x=5in width=4.5in height=4in;

           Title "Bad Rate Slope";
           footnote "Latest FirstDueMonth with full outcome"; 
           proc sgplot data= Variables_trendreport_seg&seg;
                 where  upcase(VarName) in ("&var._S") and  segment = &seg ;
                 vbar Bin / response = volume stat = percent  NOSTATLABEL    FILLATTRS=(color = VLIGB ) ;
                 vline Bin / response=badrate y2axis stat = mean NOSTATLABEL lineattrs= (pattern=solid Thickness = 2 color = gray)   ;
                 y2axis min = 0  grid;
                 yaxis min = 0  grid ;
           run;
           Title;
           footnote;

           ods region y=4.5in x=0in width=4.5in height=4in;

           %plotpsi(var=&var,title1=Application Distribution,dset=summarytable_&seg) ;

           Data Variables_trendreport_seg&seg;
              set Variables_trendreport_seg&seg;
              if compress(month) = '.' then delete;
           run;

           ods region y=4.5in x=5in width=4.5in height=4in;

           ods graphics / reset attrpriority=color;
           Title "Trends Over Time";
           proc sort data=Variables_trendreport_seg&seg;
                 by Month;
           run;
           proc sgplot data= Variables_trendreport_seg&seg;
                 where upcase(VarName) in ("&var._S") and segment = &seg ; 
                 series x=Month y=BadRate / group= BIN  lineattrs= (pattern=solid Thickness = 2  );
                 xaxis grid ;
                 yaxis min = 0  grid offsetmin=.05 offsetmax=.05;
           run;
           Title;
           ods LAYOUT END ;
     %end;
     ods pdf close;
%mend;

%buildreport(1);
%buildreport(2);
%buildreport(3);
%buildreport(4);
%buildreport(5);


/*** Macros for plotting Confidence bands for all the segments and Institution Codes ***/
%macro VarCheckConfidenceBand1(Dset, Var, PredProb , Outcome , LSize ,y,x,width,height, Heading ) ;
	data final ;
		set &Dset ;
		RRisk = &Outcome * &LSize ;
		LoanSize = &LSize;
		LSizeDSquared =  &LSize *  &LSize ;
		PredProb = input(&PredProb,20.);
		RPredProb = PredProb * Loansize ;
		format bucket $500. ;
		bucket = compress(&var) ;
	run;

	proc summary data = final nway missing ;
		class bucket ;
		var RRisk RPredProb LoanSize LSizeDSquared ;
		output out = Summary (drop = _type_) sum() =  ;
	run;

	data Summary1  ( keep = bucket Predicted LowerBound  UpperBound Actual ) ;
		retain bucket Predicted Actual;
		set Summary ;
		Predicted = RPredProb / Loansize ;
		Actual =   RRisk/ Loansize ;
		SE = SQRT(((Predicted)*(1-Predicted)*LSizeDSquared)/(Loansize*Loansize));
		factor = 3 ;
		UpperBound = Predicted + factor*SE ;
		LowerBound = Predicted - factor*SE ;
		if LowerBound <= 0 then LowerBound = 0 ;
		if UpperBound >= 1 then UpperBound = 1 ;
	run;

	data Summary1 ;
		set Summary1 ;
		Max = max(Predicted ,LowerBound,  UpperBound, Actual) ;
		if Max > 1 then Max = 1 ;
	run;

	Data Summary1;
		set Summary1;
		rename Predicted = Expected;
	run;

	proc sql noprint ;
		select sum(round(max(max),0.1),0.1) into : UpperRange separated by ''
		from summary1 ;
	quit;

	goptions reset = all ;
	pattern1 v=s c=white;
	pattern2 v=s c=grayee;
	pattern3 v=s c=graybb;
	pattern4 v=s c=graybb;
	pattern5 v=s c=grayee;

	symbol1 value=none interpol=join color=pink;
	symbol2 value=none interpol=join color=red width=2;

	axis1 label=("&var")
	offset=(0,0);                                                                                                                  
	axis2 label=(angle=90 'Rand Weighted Risk') order = 0 to &UpperRange by 0.1 
	offset=(0,0);

	symbol3 height=1.5 value=dot;

	/* Define legend characteristics */
	legend1 order=('Expected') label=none frame;
	legend2 order=('Actual') label=none frame;
	Title "&Heading";
	proc gplot data=Summary1;   
		plot
			LowerBound*bucket=1
			LowerBound*bucket=1
			Expected*bucket=1
			UpperBound*bucket=1
			UpperBound*bucket=1
			Expected*bucket=2
			/ overlay areas=5 vaxis = axis2 haxis = axis1 legend = legend1 ;
		plot2 Actual*bucket / vaxis = axis2 haxis = axis1  legend = legend2;  
	run;
	quit;
	Title ;
%mend;

%macro position(y1,x1);
     %global x y;
     %let x = &x1;
     %let y = &y1;
%mend ;

/*** Macro for Confidence Bands plot per variables ***/ 
%macro plotconfidencebands(inputdata=, segment=);
     proc sql noprint;
           select cats(parameter,'_W') into : confidvar separated by ' ' 
           from _estimates_&segment;
     quit;
     proc sql noprint;
           select parameter into : descriptionvar separated by ' ' 
           from _estimates_&segment;
     quit;
     %do u = 1 %to %sysfunc(countw(&confidvar));
           %let var = %scan(&confidvar, &u);
           %let dvar = %scan(&descriptionvar,&u);
           
           proc sql noprint;
                create table allout as
                     select &var,count(*)
                     from &inputdata
                     group by &var;
           quit;
           %do i = 1 %to %obscnt(allout);
                data onescore;
					set allout;
					if _n_ = &i;
                run;
                proc sql noprint; select cats("&dvar woe : ",&var) into : score  from onescore ; quit;
                proc sql noprint;
                    create table Alloutx as 
						select * from &inputdata
						where &var in (select &var from onescore );
                quit;

               %let position = %sysfunc( mod(&i, 4) );
               %if &position = 1 %then %position(0,0);
               %else %if &position = 2 %then %position(0,4);
               %else %if &position = 3 %then %position(4.5,0);
               %else %if &position = 0 %then %position(4.5,4.5);

               %let width = 4;
               %let height = 4;
               %if &position = 1 %then %do;
                     ods pdf startpage = now;
                     ods layout start;
               %end;

               ods region y=&y.in x=&x.in width=&width.in height=&height.in;
               %VarCheckConfidenceBand1(Alloutx, month, v637, target, Principaldebt, 0, 0, 4, 4, &score);

                %let obs = %obscnt(allout);
                %let count = %sysfunc( mod(&obs, 4) );

               %if &position = 0  %then %do;
                     ods layout end ;
               %end;

                %if (%obscnt(allout)= &i.) %then %do;
                     ods layout end ;
               %end;
           %end;
     %end;
%mend;


/*** Macro for creating V5 monitoring Full Pack report ***/
%macro buildreport(seg);
     %macro plotpsi(var=,title1=,dset=,xlbl=);
           Title "&title1";
           proc sgplot data=&dset ;
                where upcase(VariableName) in ("&var")  ;
                  vbar month / response = percent stat = mean group = scores  NOSTATLABEL  BARWIDTH = 0.8 ;  
                   vline month / response = psi y2axis stat = mean group = scores ;
                   vline month / response = marginal_stable y2axis stat = mean group = scores ;
                   vline month / response = Unstable y2axis stat = mean group = scores ;
                yaxis label = 'Percentage'; 
                xaxis label = "&xlbl";
                y2axis label = 'PSI' min = 0 max = 1 ;
           run;
           Title;
     %mend;

     proc sql;
        create table _estimates_ as
			select tranwrd(a.parameter,"_W"," ") as parameter,  b.NAME as scorecardvariable
			from data&seg.._estimates_ a left outer join Lookup.Compscanvar_lookup b
			on  upcase(a.Parameter) = cats(upcase(b.newcolumn),"_W");
    quit;

	Data _estimates_;
		set _estimates_;
		if parameter = "Intercept" then delete;
		if scorecardvariable = " " then scorecardvariable = parameter;
	run;

	data _null_ ;
		set _estimates_  ;
		rownum= _n_;
		call symput (compress("X"||rownum),upcase(Parameter));
		call symput (compress("Y"||rownum),upcase(scorecardvariable));
		call symput ('NumVars',compress(rownum));
	run;

	%do i = 1 %to &NumVars;
	   %let var = &&X&i..;
	   ods pdf startpage = now;
	   ods layout start;
	   ods region x = 1in y = 0in;
	   ods text = "Performance Monitoring Segment&seg.: &&Y&i.." ;

	   ods region y=0.5in x=0in width=4.5in height=4in;
	   ods graphics / reset attrpriority=color;
	   Title "Gini over Time";
	   footnote "&&Y&i..";
	   proc sort data=Ginis&seg;
	         by Month;
	   run;
	   data ginitabletemp;;
	         set Ginis&seg;
	         where upcase(VarName) in ("OVERALLSEGMENT","&var"); 
	         varname = tranwrd(varname,upcase("&var"),upcase("&&Y&i.."));
	   run;
	   proc sgplot data= ginitabletemp;
	         series x=Month y=Gini / group= Varname  lineattrs= (pattern=solid Thickness = 2  ) ;
	         yaxis min = 0  grid offsetmin=.05 offsetmax=.05;
	         xaxis  grid  ;
	         keylegend / Title = '' ;
	   run;
	   Title;
	   footnote;

	   ods region y=0.5in x=5in width=4.5in height=4in;

	   Title "Bad Rate Slope";
	   footnote "Latest FirstDueMonth with full outcome"; 
	   proc sgplot data= Variables_trendreport_seg&seg;
	         where  upcase(VarName) in ("&var._S") and  segment = &seg ;
	         vbar Bin / response = volume stat = percent  NOSTATLABEL    FILLATTRS=(color = VLIGB ) ;
	         vline Bin / response=badrate y2axis stat = mean NOSTATLABEL lineattrs= (pattern=solid Thickness = 2 color = gray)   ;
	         y2axis min = 0  grid;
	         yaxis min = 0  grid ;
	   run;
	   Title;
	   footnote;

	   ods region y=4.5in x=0in width=4.5in height=4in;
	   %plotpsi(var=&var,title1=Application Distribution,dset=summarytable_&seg) ;

	   ods region y=4.5in x=5in width=4.5in height=4in;
	   ods graphics / reset attrpriority=color;
	   Title "Trends Over Time";
	   proc sort data=Variables_trendreport_seg&seg;
	         by Month;
	   run;
	   proc sgplot data= Variables_trendreport_seg&seg;
	         where upcase(VarName) in ("&var._S") and segment = &seg; 
	         series x=Month y=BadRate / group= BIN  lineattrs= (pattern=solid Thickness = 2  );
	         xaxis grid ;
	         yaxis min = 0  grid offsetmin=.05 offsetmax=.05;
	   run;
	   Title;
	   ods LAYOUT END ;
	%end;
%mend;

data disb1 disb2 disb3 disb4 disb5;
      set Disbursedbase5;
      if seg = 1 then output disb1;
      else if seg = 2 then output disb2;
      else if seg = 3 then output disb3;
      else if seg = 4 then output disb4;
      else if seg = 5 then output disb5;
run;

/*** INSTITUTION CODE BASES ***/
data BNKABS BNKCAP BNKFNB BNKNED BNKOTH BNKSTD;
     SET Disbursedbase5;
     if INSTITUTIONCODE = 'BNKABS' then output BNKABS;
     else if INSTITUTIONCODE = 'BNKCAP' then output BNKCAP;
     else if INSTITUTIONCODE = 'BNKFNB' then output BNKFNB;
     else if INSTITUTIONCODE = 'BNKNED' then output BNKNED;
     else if INSTITUTIONCODE = 'BNKOTH' then output BNKOTH;
     else if INSTITUTIONCODE = 'BNKSTD' then output BNKSTD;
run;


/*** Create V5 monitoring Full Pack ***/
options nodate nonumber;
Title ; 
TITLE1; TITLE2;
Footnote;
Footnote1;
Footnote2;
options orientation=landscape;
ods pdf body = "&reports\V5 monitoring Full Pack &month..pdf"  ;
     ods pdf startpage = now;
     ods layout start;
     ods region y=0in x=0in width=4in height=4in;
     %VarCheckConfidenceBand1(Disbursedbase5, month, v637 , target , Principaldebt ,0,0,4,4, Overall Model ) ;
     ods region y=0in x=4.5in width=4in height=4in;
     %VarCheckConfidenceBand1(Disbursedbase5, seg, v637 , target , Principaldebt ,0,0,4,4, Segments ) ;
     ods region y=4.5in x=0in width=4in height=4in;
     %VarCheckConfidenceBand1(Disbursedbase5,Decile , v637 , target , Principaldebt ,0,0,4,4, Overall Decile ) ;
     ods region y=4.5in x=4.5in width=4in height=4in;
     %VarCheckConfidenceBand1(Disbursedbase5, RG6T, v637 , target , Principaldebt ,0,0,4,4, Risk Group ) ;
     ods layout end ;
     ods pdf startpage = now;
     ods layout start;
     ods region y=0in x=0in width=4in height=4in;
     %VarCheckConfidenceBand1(Disbursedbase5, INSTITUTIONCODE , v637 , target , Principaldebt ,0,0,4,4, INSTITUTION CODE) ;
     ods region y=0in x=4.5in width=4in height=4in;
     %VarCheckConfidenceBand1(BNKSTD, month, v637 , target , Principaldebt ,0,4.5,4,4, BNKSTD ) ;
     ods region y=4.5in x=0in width=4in height=4in;
     %VarCheckConfidenceBand1(BNKFNB, month, v637 , target , Principaldebt ,4.5,0,4,4, BNKFNB ) ;
     ods region y=4.5in x=4.5in width=4in height=4in;
     %VarCheckConfidenceBand1(BNKNED, month, v637 , target , Principaldebt ,4.5,4.5,4,4, BNKNED ) ;
     ods layout end ;
     ods pdf startpage = now;
     ods layout start;
     ods region y=0in x=0in width=4in height=4in;
     %VarCheckConfidenceBand1(BNKABS, month , v637 , target , Principaldebt ,0,0,4,4, BNKABS) ;
     ods region y=0in x=4.5in width=4in height=4in;
     %VarCheckConfidenceBand1(BNKCAP, month, v637 , target , Principaldebt ,0,4.5,4,4, BNKCAP ) ;
     ods region y=4.5in x=0in width=4in height=4in;
     %VarCheckConfidenceBand1(BNKOTH, month, v637 , target , Principaldebt ,0,0,4,4, BNKOTH ) ;
     ods layout end ;

     ods pdf startpage = now;
     ods layout start;
     ods region y=0in x=0in width=4in height=4in;
     %VarCheckConfidenceBand1(disb1, month , v637 , target , Principaldebt ,0,0,4,4, SEGMENT 1 ) ;
     ods region y=0in x=4.5in width=4in height=4in;
     %VarCheckConfidenceBand1(disb2, month, v637 , target , Principaldebt ,0,4.5,4,4, SEGMENT 2 ) ;
     ods region y=4.5in x=0in width=4in height=4in;
     %VarCheckConfidenceBand1(disb3, month, v637 , target , Principaldebt ,4.5,0,4,4, SEGMENT 3) ;
     ods region y=4.5in x=4.5in width=4in height=4in;
     %VarCheckConfidenceBand1(disb4, month, v637 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 4 ) ;
     ods layout end ;
     ods pdf startpage = now;
     ods layout start;
     ods region y=0in x=0in width=4in height=4in;
     %VarCheckConfidenceBand1(disb5, month, v637 , target , Principaldebt ,0,0,4,4, SEGMENT 5 ) ;
     ods region y=0in x=4.5in width=4in height=4in;
     %VarCheckConfidenceBand1(disb1, decile_b , v637 , target , Principaldebt ,0,0,4,4, SEGMENT 1: Decile ) ;
     ods region y=4.5in x=0in width=4in height=4in;
     %VarCheckConfidenceBand1(disb2, decile_b, v637 , target , Principaldebt ,0,4.5,4,4, SEGMENT 2: Decile ) ;
     ods region y=4.5in x=4.5in width=4in height=4in;
     %VarCheckConfidenceBand1(disb3, decile_b, v637 , target , Principaldebt ,4.5,0,4,4, SEGMENT 3: Decile) ;
     ods layout end ;
     ods pdf startpage = now;
     ods layout start;
     ods region y=0in x=0in width=4in height=4in;
     %VarCheckConfidenceBand1(disb4, decile_b, v637 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 4: Decile ) ;
     ods region y=0in x=4.5in width=4in height=4in;
     %VarCheckConfidenceBand1(disb5, decile_b, v637 , target , Principaldebt ,0,0,4,4, SEGMENT 5: Decile ) ;
     ods layout end ;
     %buildreport(1);
     %plotconfidencebands(inputdata=disb1, segment=1);
     %buildreport(2);
     %plotconfidencebands(inputdata=disb2, segment=2);
     %buildreport(3);
     %plotconfidencebands(inputdata=disb3, segment=3);
     %buildreport(4);
     %plotconfidencebands(inputdata=disb4, segment=4);
     %buildreport(5);
     %plotconfidencebands(inputdata=disb5, segment=5);
ods pdf close;