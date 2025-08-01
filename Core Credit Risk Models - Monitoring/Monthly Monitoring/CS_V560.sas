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


proc sql stimer;
	connect to ODBC (dsn=&odbc);
	create table Applicationbase_&month as 
	select * from connection to odbc ( 
		select *
				
		from 	DEV_DataDistillery_General.dbo.CS_applicationbase
		where appmonth>=&startdate.
	) ;
	disconnect from odbc ;
quit;

proc sql stimer;
	connect to ODBC (dsn=&odbc);
	create table disbursedbase_&month as 
	select * from connection to odbc ( 
		select *
				
		from 	DEV_DataDistillery_General.dbo.disbursedbase_&month
		
	) ;
	disconnect from odbc ;
quit;

proc format cntlin =decile._decile1_ fmtlib ;run; 
proc format cntlin =decile._decile2_ fmtlib ;run;
proc format cntlin =decile._decile3_ fmtlib ;run;
proc format cntlin =decile._decile4_ fmtlib ;run;
proc format cntlin =decile._decile5_ fmtlib ;run;

data Applicationbase_&month;
	set Applicationbase_&month;
	seg = comp_seg;
	thinfileindicator = comp_thin;

	Score = 1000-(CS_V570_prob*1000);  

	if comp_seg = 1 then decile = put(Score,s1core.);
	else if comp_seg = 2 then decile = put(Score,s2core.);
	else if comp_seg = 3 then decile = put(Score,s3core.);
	else if comp_seg = 4 then decile = put(Score,s4core.);
	else if comp_seg = 5 then decile = put(Score,s5core.);
	decile_b = put(input(decile,8.)+1,z2.);
	decile_w =input(decile,8.)+1;
	decile_s = decile_w;
run;
proc freq data=Applicationbase_&month;
table decile;
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
	    drop table DEV_DataDistillery_General.dbo.final_model_Apps
	);
	disconnect from odbc;
quit;

/*TO remove special characters in tranappnumber 	*/
data final_model_Apps;
	set Applicationbase_&month (keep = tranappnumber uniqueid);
	if input(tranappnumber, best.) =. then delete;

run;
%Upload_APS(Set = final_model_apps , Server = Work, APS_ODBC = Dev_DDGe, APS_DB = DEV_DataDistillery_General , distribute = HASH([tranappnumber]));

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
            from (select tranappnumber, uniqueid from PRD_DataDistillery_data.dbo.final_model_Apps where(isnumeric(tranappnumber) =1)) A
            inner join  PRD_DataDistillery_data.dbo.Disbursement_Info E
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

data Disbursedbase_&month;
	set DisbursedBase_without_Rej;
run;

proc sql;
    create table Disbursedbase_&month as
        select *, ((Principaldebt/sum(Principaldebt)) * count(tranappnumber)) as weight
        from Disbursedbase_&month;
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
%Loopthroughcal(Dset=Disbursedbase_&month, name=V637_Calib, prob=V635, weight=weight);

proc sql;
	create table Disbursedbase_&month
		as select a.*,
		1/(1+(V635/(1-V635))**(-1*(c.a))*exp(c.c)) as V637
		from Disbursedbase_&month a
		left join parameters_V637_calib c
		on a.count=c.count;
quit;
proc freq data=Applicationbase_&month; tables decile_s / missing; run;


data disb1 disb2 disb3 disb4 disb5;
      set Disbursedbase_&month;
	  if comp_seg = 1 then output disb1;
      else if comp_seg = 2 then output disb2;
      else if comp_seg = 3 then output disb3;
      else if comp_seg = 4 then output disb4;
      else if comp_seg = 5 then output disb5;
	  where maxSCORECARDVERSION <> 'V4';
run;


*/;
%create_scorecard_variables_list(numberofsegments=1, modeltype=C);
%create_scorecard_variables_list(numberofsegments=2, modeltype=C);
%create_scorecard_variables_list(numberofsegments=3, modeltype=C);
%create_scorecard_variables_list(numberofsegments=4, modeltype=C);
%create_scorecard_variables_list(numberofsegments=5, modeltype=C);
/**/
/**/
/*/***************************************************************************************/*/
/*/*********************************** Data preparation **********************************/*/
/*/***************************************************************************************/*/
/*proc sql;*/
/*	create table Applications_Rejects as*/
/*	    select b.I_target as I_target_rej, a.**/
/*	    from Applicationbase_&month a inner join rejects2.Application_Rejects_&month b*/
/*	    on a.tranappnumber = b.tranappnumber;*/
/*quit;*/
/**/
/*proc freq data=rejects2.Application_Rejects_&month; *tables month; *run;*/
/**/
/*Data comp.Applications_Rejects;*/
/*set Applications_Rejects;*/
/*Contractual_3_LE9 = I_target_rej*1;*/
/*run;*/
/**/;
/*Data NewAppbase;*/
/*set comp.Disbursed_only2;*/
/*month = put(input(ApplicationDate,yymmdd10.),yymmn6.);*/
/*target = Contractual_3_LE9;*/
/*run;*/
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
/*------------------------------------------------------------------*/;


Data NewAppbase;
set Applicationbase_&month ;*comp.Applications_Rejects;
month = put(input(ApplicationDate,yymmdd10.),yymmn6.);
target = Contractual_3_LE9;
run;

Data comp.NewAppbase_&month;
set NewAppbase;
if ApplicationDate > &startdate;
run;


data comp.Build_6Months; 
set comp.Newappbase_&month;
if appmonth >= &build_start and appmonth <= &build_end; 
run;



data build1 build2 build3 build4 build5;
      set comp.Build_6Months; 
      if comp_seg = 1 then output build1;
      else if comp_seg = 2 then output build2;
      else if comp_seg = 3 then output build3;
      else if comp_seg = 4 then output build4;
      else if comp_seg = 5 then output build5;
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
			set &applicationbase(where= (comp_seg=&seg));
			month = put(input(ApplicationDate,yymmdd10.),yymmn6.);
			if comp_seg = 1 then decile = put(CS_V570_Score,s1core.);
			else if comp_seg = 2 then decile = put(CS_V570_Score,s2core.);
			else if comp_seg = 3 then decile = put(CS_V570_Score,s3core.);
			else if comp_seg = 4 then decile = put(CS_V570_Score,s4core.);
			else if comp_seg = 5 then decile = put(CS_V570_Score,s5core.);
			decile_b = put(input(decile,8.)+1,z2.);
			decile_w =input(decile,8.)+1;
			decile_s = decile_w;
			count=1;
			segment = seg;
			%include _temp_;
	    run;
	    data build_&seg.;
			set build&seg(where= (comp_seg=&seg) drop= decile);
			if comp_seg = 1 then decile = put(CS_V570_Score,s1core.);
			else if comp_seg = 2 then decile = put(CS_V570_Score,s2core.);
			else if comp_seg = 3 then decile = put(CS_V570_Score,s3core.);
			else if comp_seg = 4 then decile = put(CS_V570_Score,s4core.);
			else if comp_seg = 5 then decile = put(CS_V570_Score,s5core.);
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

%looppervariables_monitoring(segment=1,base=segment_1, build1=build_1,variable_list=&segment_1_list decile,outdataset=summarytable);
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
