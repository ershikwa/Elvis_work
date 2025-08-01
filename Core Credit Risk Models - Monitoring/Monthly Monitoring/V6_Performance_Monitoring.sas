
%include "\\neptune\sasa$\SAS_Automation\SAS_Autoexec\autoexec6.sas";
/*\\mpwsas64\SAS_Automation\SAS_Autoexec\autoexec2.sas */

OPTIONS NOSYNTAXCHECK ;
options compress = yes;
options mstored sasmstore=sasmacs; 
/*libname sasmacs "\\neptune\credit$\AA_GROUP CREDIT\Scoring\Model Macros\"; */

/* %include "\\neptune\credit$\AA_GROUP CREDIT\Scoring\SAS Macros\Upload_APS.sas"; */
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\Upload_APS.sas";

/*%include "\\neptune\sasa$\V5\V5 Monitoring\Performance Monitoring\macros\Calc_Gini.sas";
%include "\\neptune\sasa$\V5\V5 Monitoring\Performance Monitoring\macros\calcTrends.sas";
%include "\\neptune\sasa$\V5\V5 Monitoring\Performance Monitoring\macros\CreateMonthlyGini.sas";
%include "\\neptune\sasa$\V5\V5 Monitoring\Performance Monitoring\macros\giniovertime.sas";
%include "\\neptune\sasa$\V5\V5 Monitoring\Performance Monitoring\macros\percentage_sloping.sas";
%include "\\neptune\sasa$\V5\V5 Monitoring\Performance Monitoring\macros\psi_calculation.sas";
%include "\\neptune\sasa$\V5\V5 Monitoring\Performance Monitoring\macros\checkifcolumnsexist.sas";
%include "\\neptune\sasa$\V5\V5 Monitoring\Performance Monitoring\macros\VarExist.sas"; */

%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\Calc_Gini.sas";
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\calcTrends.sas";
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\CreateMonthlyGini.sas";
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\giniovertime.sas";
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\percentage_sloping.sas";
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\psi_calculation.sas";
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\checkifcolumnsexist.sas";
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\VarExist.sas";

libname CS_data "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring";
libname tu '\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets';
libname CS_data2 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\V570";
libname tu2 '\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets\V580';
libname V6 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\V6 dataset";
libname board "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\Board";
*libname decile "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\SAS Decile Tables\V645";
libname decile "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\SAS Decile Tables\V635";

%macro ODSOff(); 
%mend;

%macro ODSOn(); 
%mend;

%let odbc = MPWAPS;
data _null_;
	call symput("month", put(intnx("month", today(),-1,'end'),yymmn6.));
	call symput("month12", put(intnx("month", today(),-12,'end'),yymmn6.));
	call symput("appdation12",cats("'",put(intnx("month", today(),-12,'end'),yymmddd10.),"'"));
run;
%put &month;
%put &month12;
%put &appdation12;

proc sql stimer;
	connect to ODBC (dsn=&odbc);
	create table disbursedbase4reblt_&month as 
	select * from connection to odbc ( 
		select *
		from DEV_DataDistillery_General.dbo.disbursedbase4reblt_&month
	) ;
	disconnect from odbc ;
quit;

proc sql stimer;
	connect to ODBC (dsn=&odbc);
	create table disbursedbase_&month as 
	select * from connection to odbc ( 
		select *
		from DEV_DataDistillery_General.dbo.disbursedbase_&month
	) ;
	disconnect from odbc ;
quit;

proc sql stimer;
	connect to ODBC (dsn=&odbc);
	create table APPLICATIONBASE_&month as 
	select * from connection to odbc ( 
		select v6_seg, comp_seg, tu_seg, appmonth
		from DEV_DataDistillery_General.dbo.TU_applicationbase
		where applicationdate >= &appdation12
	) ;
	disconnect from odbc ;
quit;

proc format cntlin =decile._decile11_ fmtlib ;run;
proc format cntlin =decile._decile12_ fmtlib ;run;
proc format cntlin =decile._decile13_ fmtlib ;run;
proc format cntlin =decile._decile14_ fmtlib ;run;
proc format cntlin =decile._decile15_ fmtlib ;run;
proc format cntlin =decile._decile21_ fmtlib ;run;
proc format cntlin =decile._decile22_ fmtlib ;run;
proc format cntlin =decile._decile23_ fmtlib ;run;
proc format cntlin =decile._decile24_ fmtlib ;run;
proc format cntlin =decile._decile25_ fmtlib ;run;
proc format cntlin =decile._decile31_ fmtlib ;run;
proc format cntlin =decile._decile32_ fmtlib ;run;
proc format cntlin =decile._decile33_ fmtlib ;run;
proc format cntlin =decile._decile34_ fmtlib ;run;
proc format cntlin =decile._decile35_ fmtlib ;run;
proc format cntlin =decile._decile41_ fmtlib ;run;
proc format cntlin =decile._decile42_ fmtlib ;run;
proc format cntlin =decile._decile43_ fmtlib ;run;
proc format cntlin =decile._decile44_ fmtlib ;run;
proc format cntlin =decile._decile45_ fmtlib ;run;
proc format cntlin =decile._decile51_ fmtlib ;run;
proc format cntlin =decile._decile52_ fmtlib ;run;
proc format cntlin =decile._decile53_ fmtlib ;run;
proc format cntlin =decile._decile54_ fmtlib ;run;
proc format cntlin =decile._decile55_ fmtlib ;run;

data disbursedbase4reblt_&month;
	set disbursedbase4reblt_&month;
	if v6_seg = 11 then v6_decile = put(V645_finalscore,s11core.);
	else if v6_seg = 12 then v6_decile = put(V645_finalriskscore ,s12core.);
	else if v6_seg = 13 then v6_decile = put(V645_finalriskscore ,s13core.);
	else if v6_seg = 14 then v6_decile = put(V645_finalriskscore ,s14core.);
	else if v6_seg = 15 then v6_decile = put(V645_finalriskscore ,s15core.);
	else if v6_seg = 21 then v6_decile = put(V645_finalriskscore ,s21core.);
	else if v6_seg = 22 then v6_decile = put(V645_finalriskscore ,s22core.);
	else if v6_seg = 23 then v6_decile = put(V645_finalriskscore ,s23core.);
	else if v6_seg = 24 then v6_decile = put(V645_finalriskscore ,s24core.);
	else if v6_seg = 25 then v6_decile = put(V645_finalriskscore ,s25core.);
	else if v6_seg = 31 then v6_decile = put(V645_finalriskscore ,s31core.);
	else if v6_seg = 32 then v6_decile = put(V645_finalriskscore ,s32core.);
	else if v6_seg = 33 then v6_decile = put(V645_finalriskscore ,s33core.);
	else if v6_seg = 34 then v6_decile = put(V645_finalriskscore ,s34core.);
	else if v6_seg = 35 then v6_decile = put(V645_finalriskscore ,s35core.);
	else if v6_seg = 41 then v6_decile = put(V645_finalriskscore ,s41core.);
	else if v6_seg = 42 then v6_decile = put(V645_finalriskscore ,s42core.);
	else if v6_seg = 43 then v6_decile = put(V645_finalriskscore ,s43core.);
	else if v6_seg = 44 then v6_decile = put(V645_finalriskscore ,s44core.);
	else if v6_seg = 45 then v6_decile = put(V645_finalriskscore ,s45core.);
	else if v6_seg = 51 then v6_decile = put(V645_finalriskscore ,s51core.);
	else if v6_seg = 52 then v6_decile = put(V645_finalriskscore ,s52core.);
	else if v6_seg = 53 then v6_decile = put(V645_finalriskscore ,s53core.);
	else if v6_seg = 54 then v6_decile = put(V645_finalriskscore ,s54core.);
	else if v6_seg = 55 then v6_decile = put(V645_finalriskscore ,s55core.);
	v6_decile_b = put(input(v6_decile,8.)+1,z2.);
	v6_decile_w = input(v6_decile,8.)+1;
	v6_decile_s = v6_decile_w;
	Decile_b=v6_decile_b;
run;

proc sql;
	create table disbursedbase4reblt_&month as
	select B.RTI, B.ORM, B.ORM_2, A.* 
	from disbursedbase4reblt_&month A
	left join tu2.RTI_ORM B
	on A.tranappnumber = B.tranappnumber;
quit;

proc sort data=disbursedbase4reblt_&month nodupkey;
	by tranappnumber;
run;

/********************************************************************************************************************************************************/
/* 												Trend Over Time calculation 																			*/
/********************************************************************************************************************************************************/

proc sql;
	create table V6.v6_slope_rate_&month as
		select 'V580 TU Model' as variable_name, a.* 
		from (	select avg(slope_rate) as Slope_rate
				from TU2.v5_sloperate_reblt_&month
				where upcase(variable_name) ne 'DECILE') a
		union 
		select 'V570 Compuscan Model' as variable_name, b.* 
		from (	select avg(slope_rate) as Slope_rate
				from cs_data2.v5_sloperate_&month
				where upcase(variable_name) ne 'DECILE') b;
quit;

libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.v6_slope_rate; run;
proc sql;
	create table cred_scr.v6_slope_rate(BULKLOAD=YES) as
		select   * 
		from v6.v6_slope_rate_&month;
quit;

/**********************PSI Summary for last 2 months******************************/

proc sql;
	create table x as
		select month, scorecard, stability, count(distinct variable_name) as total
		from (
				select  month , variable_name, "V580 TU Model" as Scorecard
						,case when psi  >=0.25 then  'Unstable %'
							when psi  >=0.1 then 'Marginally Unstable %'
							else ' Stable %'
						 end as stability
				from tu2.csi_distribution_reblt_&month
			)
		group by month, scorecard, stability 

		union 

		select month,scorecard, stability, count(distinct variable_name) as total
		from (
				select  month,variable_name, "V570 CS Model" as Scorecard
						,case when psi  >=0.25 then  'Unstable %'
							when psi  >=0.1 then 'Marginally Unstable %'
							else ' Stable %'
						 end as stability
				from cs_data2.csi_distribution_&month
			)
		group by month, scorecard, stability;
quit;

proc sql;
	create table last_3_months as
		select distinct month 
		from x 
		where month ne 'max csi'
		order by  month desc;
quit;
data last_3_months; set last_3_months(obs =2); run;
data scorecards ;
	length scorecard1 scorecard population $50.;
	scorecard1='V570 CS Model'  ;	scorecard =  'V570 Compuscan Model';	population ='V570CompuscanMo';	output;
	scorecard1='V580 TU Model' ;	scorecard =  'V580 TU Model';	population ='V580TUModel';	output;
run;
data psi_reasons ;
	length stability $32.;
	stability ='Unstable %';	output;
	stability='Marginally Unstable %';	output;
	stability=' Stable %';	output;
run;

proc sql;
	create table cross_tables as
		select *
		from last_3_months, scorecards, psi_reasons;
quit;

proc sql;
	create table total as
		select Scorecard, month, sum( total) as Totalofvariables
		from X
		group by Scorecard, month;
quit;

proc sql;
	create table stable as
		select a.Scorecard, a.month, stability,total as numberofvariables, b.Totalofvariables, total/b.Totalofvariables as Stable_percentage	   
		from x a left join total b
		on a.month = b.month 
		and a.scorecard = b.scorecard;
quit;

proc sql;
	create table v6_variables_stability as
		select case when Stable_percentage = . then 0 else Stable_percentage end as Stable_percentage, a.*,b.*
		from cross_tables a left join  stable b
		on a.stability = b.stability
		and a.month = b.month
		and a.scorecard1 = b.scorecard;
quit;

data V6.v6_variables_stability_&month;
	set v6_variables_stability(drop=numberofvariables Totalofvariables);
run;

libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.v6_variables_stability; run;
proc sql;
	create table  cred_scr.v6_variables_stability(BULKLOAD=YES) as
		select   * 
		from V6.v6_variables_stability_&month;
quit;

/********************************************************************************************************************************************************/
/* 												Calculate the Ginis																						*/
/********************************************************************************************************************************************************/
data V6.Ginipersegment_summary_&month;
	set tu2.giniperseg_summary_reblt_&month;
run;

proc sort data = tu2.overallgini_summary_reblt_&month nodupkey out=overallgini_summary_&month;
	by score_type segment;
run;

data overallgini_summary_&month;
	set overallgini_summary_&month;
	if score_type in ('Build TU Prob','Rebuild_TU','Refit_TU','Resegment TU Prob') then delete;
	if score_type = 'Compuscan_Generic' then score_type ='Comp Generic Score';
	else if score_type = 'Tu_Generic' then score_type ='TU Generic Score';
	else if score_type = 'V560_Comp_Prob' then score_type ='V560 Comp Prob';
	else if score_type = 'V570_Comp_Prob' then score_type ='V570 Comp Prob';
	else if score_type = 'V622' then score_type ='V622 Prob';
	else if score_type = 'V623_Prob' then score_type ='V623 Prob';
	else if score_type = 'TU_Prob' then score_type ='V570 TU Prob';
	else if score_type = 'V572' then score_type ='V572 TU Prob';
	else if score_type = 'V562' then score_type ='V562 Comp Prob';
	else if score_type = 'V582' then score_type ='V582 TU Prob';
	else if score_type = 'V572' then score_type ='V572 Comp Prob';
	else if score_type = 'V635' then score_type ='V635 Prob';
	else if score_type = 'V636' then score_type ='V636 Prob';
	else if score_type = 'V645' then score_type ='V645 Prob';
	else if score_type = 'V655_2' then score_type ='V655_2 Prob';
	else if score_type = 'V667' then score_type ='V667 Prob';
	else if score_type = 'ORM_2' then score_type ='ORM.2';
	else if score_type = 'ORM3_2' then score_type ='ORM3.2';
	else if score_type = 'V645_adj' then score_type ='V645_adj Prob';
run;

proc sql;
	create table V6.v6_currentmodel_benchmark_&month as
		select distinct a.*,b.gini as Benchmark_gini, (b.gini-a.gini)/a.gini as Relative_change
		from overallgini_summary_&month a,
		(select segment, gini from overallgini_summary_&month
		where score_type='V655_2 Prob') b
		where a.segment=b.segment;
quit;

data V6.v6_currentmodel_benchmark_&month;
	set V6.v6_currentmodel_benchmark_&month;
    format Recommended_Action $500.;
	where segment = 0;
  
    if relative_change >=-0.10 then do;
   		Recommended_Action = cats("No Action ");
    end;
    else if relative_change <-0.10 and relative_change >-0.15 then do;
    	Recommended_Action =cats("Check Additional Metrics and Establish if a score to Risk Calibration is required");
    end;
	else if relative_change <-0.15 then do;
		Recommended_Action =cats("Check Additional Metrics and Establish if a score to Risk Calibration is required");
	end;
run;

libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.v6_relative_gini; run;
proc sql;
	create table cred_scr.v6_relative_gini(BULKLOAD=YES) as
		select   * 
		from V6.v6_currentmodel_benchmark_&month;
quit;

libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.V6_Gini_summary; run;
proc sql;
	create table cred_scr.V6_Gini_summary(BULKLOAD=YES) as
		select   * 
		from V6.Ginipersegment_summary_&month;
quit;

/********************************************************************************************************************************************************/
/* 												Scorecard Distribution																					*/
/********************************************************************************************************************************************************/

proc sql stimer;
	connect to ODBC (dsn=&odbc);
	create table alloutc as 
	select * from connection to odbc ( 
		select appmonth, maxscorecardversion, count(*) as count
		from DEV_DataDistillery_General.dbo.TU_applicationbase
		where applicationdate >= &appdation12.
		group by appmonth, maxscorecardversion
		order by appmonth
	) ;
	disconnect from odbc ;
quit;

data combined2(rename=(count1=count));;
	length appmonth $10. maxSCORECARDVERSION $10. ;
	appmonth = ' STRATEGY';
	/*maxSCORECARDVERSION = 'V645';
	count1 =0;
	output; */
	maxSCORECARDVERSION = 'V655_2';
	count1 =0.2;
	output;

	maxSCORECARDVERSION = 'V667';
	count1 =0.8;
	output;
run;

data V6.scorecarddist_&month;;
	format count 8.2;
	set alloutc combined2;
	if maxSCORECARDVERSION = 'V655' then
	maxSCORECARDVERSION = 'V655_2'; else maxSCORECARDVERSION = maxSCORECARDVERSION;
run;

libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.scorecarddist; run;
proc sql;
	create table  cred_scr.scorecarddist(BULKLOAD=YES) as
		select   * 
		from V6.scorecarddist_&month;
quit;

/*data scorecarddist;*/
/*	set V6.scorecarddist_&month;*/
/*run;*/
/*%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\Upload_APS.sas";*/
/*%Upload_APS(Set = scorecarddist, Server = work, APS_ODBC = DEV_DDGE, APS_DB = DEV_DataDistillery_General,distribute = hash(appmonth));*/


/********************************************************************************************************************************************************/
/* 												Segment Distribution Plot																				*/
/********************************************************************************************************************************************************/
%macro psi_calculation(build=, base=,period=month,var=,psi_var=, outputdataset=);
/*    %if %VarExist(&build, &psi_var)=1 and %VarExist(&base, &psi_var)=1 and %VarExist(&base, &period)=1 %then %do;*/
      proc freq data = &base;
            tables &period*&psi_var / missing outpct out=basetable(keep =&period &psi_var pct_row rename =(pct_row = percent));
      run;
      proc freq data = &build;
            tables &psi_var /missing out=buildtable(keep=&psi_var percent);
      run;
      data buildtable;
            set buildtable;
            binnumber=_n_;
      run;
      proc sql;
            create table basetable2 as
                  select distinct  *
                  from basetable a full join  buildtable(keep = &psi_var binnumber)  b
                  on a.&psi_var = b.&psi_var
                  ;
      quit;
      proc sort data = basetable2;
            by &period &psi_var;
      run;
      proc transpose data = basetable2 out = psitrans prefix = bin ;    
            by &period;
            id binnumber;
            var percent;
      run;
      proc transpose data = buildtable out = buildtrans prefix = build;
            var percent;
      run;
      proc sql; select count(distinct &psi_var) into : numBuckets separated by "" from buildtable; quit;
      proc sql;
            create table all_psi as
               select *
               from psitrans , buildtrans;
      quit;
      data all_psi_results(keep = Variablename &period psi marginal_stable unstable);
            set all_Psi;
            length variablename $32.;
            array pred [&numBuckets] bin1 - bin&numBuckets;
            array build [&numBuckets] build1 - build&numBuckets;
            item = 0;
            do p = 1 to &numBuckets;
              item = sum(item,(pred[p]-build[p])*(log(pred[p]/build[p])));
            end;
            psi = item/100;
            marginal_stable = 0.1;
            unstable=0.25;
            variablename = tranwrd(upcase("&var."),"_W","");
      run;
      data buildset ;
            set buildtable(rename=(&psi_var =scores));
            length variablename $32.;
            &period =" BUILD";
            psi=.;
            marginal_stable=.;
            unstable=.;
            variablename=tranwrd(upcase("&var."),"_W","");
      run;
      proc sql;
            create table summarytable(rename=(&psi_var=scores)) as
                  select *
                  from basetable(keep = &period &psi_var percent) a inner join all_psi_results b
                  on a.&period = b.&period;
      quit;
      proc append base = &outputdataset data = summarytable force; run;
      proc append base = &outputdataset data = buildset force; run;
      proc datasets lib = work;
        delete buildset all_psi_results summarytable basetable basetable2 all_psi psitrans buildtrans buildtable ;
      run;quit;
%mend;

data v6_seg_applicationset;
	set applicationbase_&month;
	v6_seg = comp_seg*10+tu_seg;
run;

/* New development data base on v655.2 */
libname data2 '\\mpwsas64\core_Credit_Risk_Model_Team\Scorecard\V655.2\Data';

data new_build;
	set data2.V6_calibration_volume_new;
	v6_seg = comp_seg*10+tu_seg;
run;

%macro looppervariables(segment=0,base=,build1=,variable_list=,outdataset=);
      proc delete data = &outdataset._&segment; run;
      %do i = 1 %to %sysfunc(countw(&variable_list));
            %let vari = %scan(&variable_list, &i.);
            %psi_calculation(build=&build1, base=&base,period=appmonth,var=&vari,psi_var=&vari, outputdataset=&outdataset._&segment);
            data &outdataset._&segment;
                  set &outdataset._&segment;
                  seg = &segment;
            run;
      %end;
%mend;
%looppervariables(segment=0,base=v6_seg_applicationset,build1=new_build,variable_list=V6_Seg,outdataset=V6SegDist_&month);

data v6.V6SegDist_&month ;
	set V6SegDist_&month._0;;
run;

libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.V6SegDist; run;
proc sql;
	create table  cred_scr.V6SegDist(BULKLOAD=YES) as
		select   * 
		from V6.V6SegDist_&month;
quit;


/* New development data base on v667 */
libname data2a '\\mpwsas64\core_Credit_Risk_Model_Team\Scorecard\V665\Data';

data new_buildv667;
	set data2a.V6_calibration_volume_new;
	v6_seg = comp_seg*10+tu_seg;
run;

%macro looppervariables(segment=0,base=,build1=,variable_list=,outdataset=);
      proc delete data = &outdataset._&segment; run;
      %do i = 1 %to %sysfunc(countw(&variable_list));
            %let vari = %scan(&variable_list, &i.);
            %psi_calculation(build=&build1, base=&base,period=appmonth,var=&vari,psi_var=&vari, outputdataset=&outdataset._&segment);
            data &outdataset._&segment;
                  set &outdataset._&segment;
                  seg = &segment;
            run;
      %end;
%mend;
%looppervariables(segment=0,base=v6_seg_applicationset,build1=new_buildv667,variable_list=V6_Seg,outdataset=V667SegDist_&month);

data v6.V667SegDist_&month ;
	set V667SegDist_&month._0;;
run;

libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.V667SegDist; run;
proc sql;
	create table  cred_scr.V667SegDist(BULKLOAD=YES) as
		select   * 
		from V6.V667SegDist_&month;
quit;

/********************************************************************************************************************************************************/
/* 												Plotting Confidence bands for all the  segments and Institution Codes 																*/
/********************************************************************************************************************************************************/
%macro VarCheckConfidenceBand1(Dset, Var, PredProb , Outcome , LSize ,y,x,width,height, Heading ) ;
	data _null_;
		call symput("actual_date", put(intnx("month", today(),-9,'end'),yymmn6.));
	run;
	%put &actual_date;

	data final ;
		set &Dset ;
		RRisk = &Outcome * &LSize ;
		LoanSize = &LSize;
		LSizeDSquared =  &LSize *  &LSize ;
		PredProb = input(&PredProb,20.);
		RPredProb = PredProb * Loansize ;
		format bucket $500. ;
		bucket = compress(&var) ;
		if PredProb ne . ;
	run;

	proc summary data = final nway missing ;
		class bucket ;
		var RRisk RPredProb LoanSize LSizeDSquared ;
		output out = Summary (drop = _type_) sum() =  ;
	run;

	%if &Var = month %then %do;
		data Summary1 (keep = bucket Predicted LowerBound  UpperBound Actual ) ;
			retain bucket Predicted Actual;
			set Summary ;
			Predicted = RPredProb / Loansize ;
			Actual =   RRisk/ Loansize ;
			SE = SQRT(((Predicted)*(1-Predicted)*LSizeDSquared)/(Loansize*Loansize));
			if bucket <= &actual_date then factor = 3; else factor = 5;
			UpperBound = Predicted + factor*SE ;
			LowerBound = Predicted - factor*SE ;
			if LowerBound <= 0 then LowerBound = 0 ;
			if UpperBound >= 1 then UpperBound = 1 ;
		run;
	%end;
	%else %if &Var ne month %then %do;
		data Summary1 (keep = bucket Predicted LowerBound  UpperBound Actual ) ;
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
	%end;

	data Summary1 ;
	set Summary1 ;
	Max = max(Predicted ,LowerBound,  UpperBound, Actual) ;
	if Max > 1 then Max = 1 ;
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

	%ODSOff();
	/* Define legend characteristics */
	legend1 order=('Predicted') label=none frame;
	legend2 order=('Actual') label=none frame;
	Title "&Heading";
	proc gplot data=Summary1;   
		plot
		LowerBound*bucket=1
		LowerBound*bucket=1
		Predicted*bucket=1
		UpperBound*bucket=1
		UpperBound*bucket=1
		Predicted*bucket=2
		/ overlay areas=5 vaxis = axis2 haxis = axis1 legend = legend1 ;
		plot2 Actual*bucket / vaxis = axis2 haxis = axis1  legend = legend2;  
	run;
	quit;
	Title ;
	%ODSOn();
%mend;

/* Confidence Band Data Prep */
proc sql;
	create table Confidence_Band_Data as
	select uniqueid, input(Tranappnumber, 15.) as Tranappnumber, 
	V655_2, V655_2_riskgroup, V667, V667_riskgroup, RTI, ORM, ORM_2, ORM3_2, Month, target, Principaldebt
	from DISBURSEDBASE4REBLT_&month
	union all
	select uniqueid, Tranappnumber, 
	V655_2, V655_2_riskgroup, V667, V667_RiskGroup, RTI, ORM, ORM_2, ORM3_2, Month, target, Principaldebt
	from tu2.RTI_ORM_Early_Predictors;
quit;
proc sort data=Confidence_Band_Data nodupkey;
	by tranappnumber descending uniqueid;
run;

/* Confidence Band Data Prep for Cards and Loans */
proc sql stimer;
	connect to odbc(dsn=MPWAPS);
	create table disb as
	select * from connection to odbc(
	Select loanid, FirstDueDate, product 
		from PRD_DataDistillery_data.dbo.Disbursement_Info
		union all
		Select loanid, cast(replace(FirstDueDate,'-','') as numeric) as FirstDueDate, product
		from PRD_DataDistillery_data.dbo.Disbursement_Info_Over);
	disconnect from odbc;
quit;

proc sql;
	create table Confidence_Band_Data_Product as
	select a.*, b.Product
	from Confidence_Band_Data a left join disb b
	on a.tranappnumber=b.loanid;
quit;

data Confidence_Band_Data_Loan;
	set Confidence_Band_Data_Product;
	where Product = 'Loan';
run;

data Confidence_Band_Data_Card;
	set Confidence_Band_Data_Product;
	where Product = 'Card';
run;



/* V655.2 Confidence Band by Month */
proc delete data =summary1; run;
%VarCheckConfidenceBand1(Confidence_Band_Data, month, V655_2 , target , Principaldebt ,0,0,4,4, V655_2 Overall Model ) ;
data V6.V655_2_ConfidenceBands_&month; set summary1; run;

libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.V655_2_ConfidenceBands; run;
proc sql;
	create table cred_scr.V655_2_ConfidenceBands(BULKLOAD=YES) as
		select * 
		from V6.V655_2_ConfidenceBands_&month;
quit;

/* V655.2 Confidence Band by Riskgroup */
proc delete data =summary1; run;
%VarCheckConfidenceBand1(Confidence_Band_Data, V655_2_riskgroup, V655_2  , target , Principaldebt ,0,0,4,4, V655_2 Riskgroup Overall Model ) ;
data V6.V655_2_ConfidenceBands_RG_&month; set summary1; run;

libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.V655_2_ConfidenceBands_RG; run;
proc sql;
	create table  cred_scr.V655_2_ConfidenceBands_RG (BULKLOAD=YES) as
		select * 
		from V6.V655_2_ConfidenceBands_RG_&month;
quit;

/* V655.2 Confidence Band by Riskgroup Cards and Loans */
proc delete data =summary1; run;
%VarCheckConfidenceBand1(Confidence_Band_Data_Card, V655_2_riskgroup, V655_2  , target , Principaldebt ,0,0,4,4, V655_2 Riskgroup(50-55) Overall Model for Cards ) ;
data V6.V655_2_ConfBands_RGCards_&month;
	set summary1;
	where bucket <= '55';
run;

libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.V655_2_ConfBands_RGCards; run;
proc sql;
	create table  cred_scr.V655_2_ConfBands_RGCards (BULKLOAD=YES) as
		select * 
		from V6.V655_2_ConfBands_RGCards_&month;
quit;


proc delete data =summary1; run;
%VarCheckConfidenceBand1(Confidence_Band_Data_Loan, V655_2_riskgroup, V655_2  , target , Principaldebt ,0,0,4,4, V655_2 Riskgroup(50-53) Overall Model for Loans) ;
data V6.V655_2_ConfBands_RGLoan_&month;
	set summary1;
	where bucket <= '53';
run;

libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.V655_2_ConfBands_RGLoan; run;
proc sql;
	create table  cred_scr.V655_2_ConfBands_RGLoan (BULKLOAD=YES) as
		select * 
		from V6.V655_2_ConfBands_RGLoan_&month;
quit;


/* RTI, ORM and ORM.2 Confidence Band Tables by Month */
/* RTI */
proc delete data =summary1; run;
%VarCheckConfidenceBand1(Confidence_Band_Data , month, RTI , target , Principaldebt ,0,0,4,4, RTI Overall Model ) ;

data V6.RTI_ConfidenceBands_&month; set summary1; run;

libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.RTI_ConfidenceBands; run;
proc sql;
	create table cred_scr.RTI_ConfidenceBands (BULKLOAD=YES) as
	select * from V6.RTI_ConfidenceBands_&month;
quit;


/* ORM */
proc delete data =summary1; run;
%VarCheckConfidenceBand1(Confidence_Band_Data , month, ORM , target , Principaldebt ,0,0,4,4, ORM Overall Model ) ;

data V6.ORM_ConfidenceBands_&month; set summary1; run;

libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.ORM_ConfidenceBands; run;
proc sql;
	create table cred_scr.ORM_ConfidenceBands (BULKLOAD=YES) as
	select * from V6.ORM_ConfidenceBands_&month;
quit;

/* ORM.2 */
proc delete data =summary1; run;
%VarCheckConfidenceBand1(Confidence_Band_Data , month, ORM_2 , target , Principaldebt ,0,0,4,4, ORM.2 Overall Model ) ;

data V6.ORM_2_ConfidenceBands_&month; set summary1; run;

libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.ORM_2_ConfidenceBands; run;
proc sql;
	create table cred_scr.ORM_2_ConfidenceBands (BULKLOAD=YES) as
	select * from V6.ORM_2_ConfidenceBands_&month;
quit;

/*RTI ORM vs V655_2 */
libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.RTI_ORM_V655_2_confidencebands; run;
proc sql;
	create table RTI_ORM_V655_2_confidencebands as
	select a.bucket, a.Actual, a.Predicted as V655_2_Predicted,
	a.LowerBound as V655_2_LowerBound, a.UpperBound as V655_2_UpperBound,
	b.Predicted as RTI_Predicted,
	b.LowerBound as RTI_LowerBound, b.UpperBound as RTI_UpperBound,
	c.Predicted as ORM_Predicted,
	c.LowerBound as ORM_LowerBound, c.UpperBound as ORM_UpperBound,
	d.Predicted as ORM_2_Predicted,
	d.LowerBound as ORM_2_LowerBound, d.UpperBound as ORM_2_UpperBound
	from V6.V655_2_CONFIDENCEBANDS_&month a left join V6.RTI_CONFIDENCEBANDS_&month b
	on a.bucket = b.bucket
	left join V6.ORM_CONFIDENCEBANDS_&month c
	on a.bucket = c.bucket
	left join V6.ORM_2_CONFIDENCEBANDS_&month d
	on a.bucket = d.bucket;
quit;

proc sql;
	create table cred_scr.RTI_ORM_V655_2_confidencebands (BULKLOAD=YES) as
	select * from RTI_ORM_V655_2_confidencebands;
quit;

/* V667 Confidence Band by Month */
proc delete data =summary1; run;
%VarCheckConfidenceBand1(Confidence_Band_Data, month, V667 , target , Principaldebt ,0,0,4,4, V667 Overall Model ) ;
data V6.V667_ConfidenceBands_&month; set summary1; run;

libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.V667_ConfidenceBands; run;
proc sql;
	create table cred_scr.V667_ConfidenceBands(BULKLOAD=YES) as
		select * 
		from V6.V667_ConfidenceBands_&month;
quit;

/* ORM3.2 */
proc delete data =summary1; run;
%VarCheckConfidenceBand1(Confidence_Band_Data , month, ORM3_2 , target , Principaldebt ,0,0,4,4, ORM3_2 Overall Model ) ;
data V6.ORM3_2_ConfidenceBands_&month; set summary1; run;

libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.ORM3_2_ConfidenceBands; run;
proc sql;
	create table cred_scr.ORM3_2_ConfidenceBands (BULKLOAD=YES) as
	select * from V6.ORM3_2_ConfidenceBands_&month;
quit;

/*V667 ORM3.2 Confidence Bands*/
libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.ORM3_2_V667_confidencebands; run;
proc sql;
	create table ORM3_2_V667_confidencebands as
	select a.bucket, a.Actual, a.Predicted as V667_Predicted,
	a.LowerBound as V667_LowerBound, a.UpperBound as V667_UpperBound,
	b.Predicted as ORM3_2_Predicted,
	b.LowerBound as ORM3_2_LowerBound, b.UpperBound as ORM3_2_UpperBound
	from V6.V667_ConfidenceBands_&month a left join V6.ORM3_2_ConfidenceBands_&month b
	on a.bucket = b.bucket;
quit;

proc sql;
	create table cred_scr.ORM3_2_V667_confidencebands (BULKLOAD=YES) as
	select * from ORM3_2_V667_confidencebands;
quit;

/*V667 confidence band by product*/


proc delete data =summary1; run;
%VarCheckConfidenceBand1(Confidence_Band_Data_Card, V667_riskgroup, V667  , target , Principaldebt ,0,0,4,4, V667 Riskgroup(50-55) Overall Model for Cards ) ;
data V6.V667_ConfBands_RGCards_&month;
	set summary1;
	where bucket <= '55';
run;

libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.V667_ConfBands_RGCards; run;
proc sql;
	create table  cred_scr.V667_ConfBands_RGCards (BULKLOAD=YES) as
		select * 
		from V6.V667_ConfBands_RGCards_&month;
quit;


proc delete data =summary1; run;
%VarCheckConfidenceBand1(Confidence_Band_Data_Loan, V667_riskgroup, V667  , target , Principaldebt ,0,0,4,4, V667 Riskgroup(50-53) Overall Model for Loans) ;
data V6.V667_ConfBands_RGLoan_&month;
	set summary1;
	where bucket <= '53';
run;

libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.V667_ConfBands_RGLoan; run;
proc sql;
	create table  cred_scr.V667_ConfBands_RGLoan (BULKLOAD=YES) as
		select * 
		from V6.V667_ConfBands_RGLoan_&month;
quit;



%macro disb(in=);
	%do i=1 %to 5;
		%do j=1 %to 5;
			data disb&i.&j.;
				set &in.;
				if v6_seg = &i.&j. then do;
					output disb&i.&j.;
				end;
			run;
		%end;
	%end;
%mend;
%disb(in=DISBURSEDBASE4REBLT_&month.);

data BNKABS BNKCAP BNKFNB BNKNED BNKOTH BNKSTD BNKABL BNKINV;
	set DISBURSEDBASE4REBLT_&month.;
	if INSTITUTIONCODE = 'BNKABS' then output BNKABS;
	else if INSTITUTIONCODE = 'BNKCAP' then output BNKCAP;
	else if INSTITUTIONCODE = 'BNKFNB' then output BNKFNB;
	else if INSTITUTIONCODE = 'BNKNED' then output BNKNED;
	else if INSTITUTIONCODE = 'BNKOTH' then output BNKOTH;
	else if INSTITUTIONCODE = 'BNKSTD' then output BNKSTD;
	else if INSTITUTIONCODE = 'BNKABL' then output BNKABL;
	else if INSTITUTIONCODE = 'BNKINV' then output BNKINV;
run;

/*******************************************************************V622************************************************************/
ods pdf  body = "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\TU Performance Monitoring\reports\V6\Confident Band for V622 Model  &month..pdf";
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, month, V622 , target, Principaldebt ,0,0,4,4, V622 Overall Model ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, Decile_b, V622, target, Principaldebt ,0,0,4,4, V622 Overall Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, v6_seg, V622, target, Principaldebt ,0,0,4,4, Segments ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, V622_RiskGroup, V622, target, Principaldebt ,0,0,4,4, Risk Group ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, INSTITUTIONCODE, V622, target , Principaldebt ,0,0,4,4, INSTITUTION CODE) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(BNKSTD, month, V622 , target, Principaldebt, 0,4.5,4,4, BNKSTD ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(BNKFNB, month, V622 , target, Principaldebt ,4.5,0,4,4, BNKFNB ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(BNKNED, month, V622 , target, Principaldebt ,4.5,4.5,4,4, BNKNED ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(BNKABS, month , V622 , target , Principaldebt ,0,0,4,4, BNKABS) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(BNKCAP, month, V622 , target , Principaldebt ,0,4.5,4,4, BNKCAP ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(BNKABL, month, V622 , target , Principaldebt ,0,0,4,4, BNKABL ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(BNKINV, month, V622 , target , Principaldebt ,0,0,4,4, BNKINV ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(BNKOTH, month, V622 , target , Principaldebt ,0,0,4,4, BNKOTH ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb11, month , V622 , target , Principaldebt ,0,0,4,4, SEGMENT 11 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb12, month, V622 , target , Principaldebt ,0,4.5,4,4, SEGMENT 12 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb13, month, V622 , target , Principaldebt ,4.5,0,4,4, SEGMENT 13) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb14, month, V622 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 14 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb15, month, V622 , target , Principaldebt ,0,0,4,4, SEGMENT 15 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb21, month, V622 , target , Principaldebt ,0,4.5,4,4, SEGMENT 21 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb22, month, V622 , target , Principaldebt ,4.5,0,4,4, SEGMENT 22) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb23, month, V622 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 23 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb24, month, V622 , target , Principaldebt ,0,0,4,4, SEGMENT 24 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb25, month, V622 , target , Principaldebt ,0,4.5,4,4, SEGMENT 25 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb31, month, V622 , target , Principaldebt ,4.5,0,4,4, SEGMENT 31) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb32, month, V622 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 32 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb33, month, V622 , target , Principaldebt ,0,0,4,4, SEGMENT 33 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb34, month, V622 , target , Principaldebt ,0,4.5,4,4, SEGMENT 34 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb35, month, V622 , target , Principaldebt ,4.5,0,4,4, SEGMENT 35) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb41, month, V622 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 41 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb42, month, V622 , target , Principaldebt ,0,0,4,4, SEGMENT 42 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb43, month, V622 , target , Principaldebt ,0,4.5,4,4, SEGMENT 43 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb44, month, V622 , target , Principaldebt ,4.5,0,4,4, SEGMENT 44) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb45, month, V622 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 45 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb51, month, V622 , target , Principaldebt ,0,0,4,4, SEGMENT 51 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb52, month, V622 , target , Principaldebt ,0,4.5,4,4, SEGMENT 52 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb53, month, V622 , target , Principaldebt ,4.5,0,4,4, SEGMENT 53) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb54, month, V622 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 54 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb55, month, V622 , target , Principaldebt ,0,0,4,4, SEGMENT 55 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb11, decile_b , V622 , target , Principaldebt ,0,0,4,4, SEGMENT 11: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb12, decile_b, V622 , target , Principaldebt ,0,4.5,4,4, SEGMENT 12: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb13, decile_b, V622 , target , Principaldebt ,4.5,0,4,4, SEGMENT 13: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb14, decile_b, V622 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 14: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb15, decile_b, V622 , target , Principaldebt ,0,0,4,4, SEGMENT 15: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb21, decile_b, V622 , target , Principaldebt ,0,4.5,4,4, SEGMENT 21: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb22, decile_b, V622 , target , Principaldebt ,4.5,0,4,4, SEGMENT 22: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb23, decile_b, V622 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 23: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb24, decile_b, V622 , target , Principaldebt ,0,0,4,4, SEGMENT 24: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb25, decile_b, V622 , target , Principaldebt ,0,4.5,4,4, SEGMENT 25: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb31, decile_b, V622 , target , Principaldebt ,4.5,0,4,4, SEGMENT 31: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb32, decile_b, V622 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 32: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb33, decile_b, V622 , target , Principaldebt ,0,0,4,4, SEGMENT 33: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb34, decile_b, V622 , target , Principaldebt ,0,4.5,4,4, SEGMENT 34: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb35, decile_b, V622 , target , Principaldebt ,4.5,0,4,4, SEGMENT 35: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb41, decile_b, V622 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 41: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb42, decile_b, V622 , target , Principaldebt ,0,0,4,4, SEGMENT 42: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb43, decile_b, V622 , target , Principaldebt ,0,4.5,4,4, SEGMENT 43: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb44, decile_b, V622 , target , Principaldebt ,4.5,0,4,4, SEGMENT 44: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb45, decile_b, V622 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 45: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb51, decile_b, V622 , target , Principaldebt ,0,0,4,4, SEGMENT 51: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb52, decile_b, V622 , target , Principaldebt ,0,4.5,4,4, SEGMENT 52: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb53, decile_b, V622 , target , Principaldebt ,4.5,0,4,4, SEGMENT 53: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb54, decile_b, V622 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 54: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb55, decile_b, V622 , target , Principaldebt ,0,0,4,4, SEGMENT 55: Decile ) ;
ods layout end ;
ods pdf close ;
ods layout end ;
ods pdf close ;

/*******************************************************************V645************************************************************/
ods pdf  body = "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\TU Performance Monitoring\reports\V6\Confident Band for V645 Model  &month..pdf";
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, month, V645 , target, Principaldebt ,0,0,4,4, V645 Overall Model ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, Decile_b, V645, target, Principaldebt ,0,0,4,4, V645 Overall Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, v6_seg, V645, target, Principaldebt ,0,0,4,4, Segments ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, V645_RiskGroup, V645, target, Principaldebt ,0,0,4,4, Risk Group ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, INSTITUTIONCODE, V645, target , Principaldebt ,0,0,4,4, INSTITUTION CODE) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(BNKSTD, month, V645 , target, Principaldebt, 0,4.5,4,4, BNKSTD ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(BNKFNB, month, V645 , target, Principaldebt ,4.5,0,4,4, BNKFNB ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(BNKNED, month, V645 , target, Principaldebt ,4.5,4.5,4,4, BNKNED ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(BNKABS, month , V645 , target , Principaldebt ,0,0,4,4, BNKABS) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(BNKCAP, month, V645 , target , Principaldebt ,0,4.5,4,4, BNKCAP ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(BNKABL, month, V645 , target , Principaldebt ,0,0,4,4, BNKABL ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(BNKINV, month, V645 , target , Principaldebt ,0,0,4,4, BNKINV ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(BNKOTH, month, V645 , target , Principaldebt ,0,0,4,4, BNKOTH ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb11, month , V645 , target , Principaldebt ,0,0,4,4, SEGMENT 11 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb12, month, V645 , target , Principaldebt ,0,4.5,4,4, SEGMENT 12 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb13, month, V645 , target , Principaldebt ,4.5,0,4,4, SEGMENT 13) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb14, month, V645 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 14 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb15, month, V645 , target , Principaldebt ,0,0,4,4, SEGMENT 15 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb21, month, V645 , target , Principaldebt ,0,4.5,4,4, SEGMENT 21 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb22, month, V645 , target , Principaldebt ,4.5,0,4,4, SEGMENT 22) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb23, month, V645 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 23 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb24, month, V645 , target , Principaldebt ,0,0,4,4, SEGMENT 24 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb25, month, V645 , target , Principaldebt ,0,4.5,4,4, SEGMENT 25 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb31, month, V645 , target , Principaldebt ,4.5,0,4,4, SEGMENT 31) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb32, month, V645 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 32 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb33, month, V645 , target , Principaldebt ,0,0,4,4, SEGMENT 33 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb34, month, V645 , target , Principaldebt ,0,4.5,4,4, SEGMENT 34 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb35, month, V645 , target , Principaldebt ,4.5,0,4,4, SEGMENT 35) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb41, month, V645 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 41 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb42, month, V645 , target , Principaldebt ,0,0,4,4, SEGMENT 42 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb43, month, V645 , target , Principaldebt ,0,4.5,4,4, SEGMENT 43 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb44, month, V645 , target , Principaldebt ,4.5,0,4,4, SEGMENT 44) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb45, month, V645 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 45 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb51, month, V645 , target , Principaldebt ,0,0,4,4, SEGMENT 51 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb52, month, V645 , target , Principaldebt ,0,4.5,4,4, SEGMENT 52 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb53, month, V645 , target , Principaldebt ,4.5,0,4,4, SEGMENT 53) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb54, month, V645 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 54 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb55, month, V645 , target , Principaldebt ,0,0,4,4, SEGMENT 55 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb11, decile_b , V645 , target , Principaldebt ,0,0,4,4, SEGMENT 11: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb12, decile_b, V645 , target , Principaldebt ,0,4.5,4,4, SEGMENT 12: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb13, decile_b, V645 , target , Principaldebt ,4.5,0,4,4, SEGMENT 13: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb14, decile_b, V645 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 14: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb15, decile_b, V645 , target , Principaldebt ,0,0,4,4, SEGMENT 15: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb21, decile_b, V645 , target , Principaldebt ,0,4.5,4,4, SEGMENT 21: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb22, decile_b, V645 , target , Principaldebt ,4.5,0,4,4, SEGMENT 22: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb23, decile_b, V645 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 23: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb24, decile_b, V645 , target , Principaldebt ,0,0,4,4, SEGMENT 24: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb25, decile_b, V645 , target , Principaldebt ,0,4.5,4,4, SEGMENT 25: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb31, decile_b, V645 , target , Principaldebt ,4.5,0,4,4, SEGMENT 31: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb32, decile_b, V645 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 32: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb33, decile_b, V645 , target , Principaldebt ,0,0,4,4, SEGMENT 33: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb34, decile_b, V645 , target , Principaldebt ,0,4.5,4,4, SEGMENT 34: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb35, decile_b, V645 , target , Principaldebt ,4.5,0,4,4, SEGMENT 35: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb41, decile_b, V645 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 41: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb42, decile_b, V645 , target , Principaldebt ,0,0,4,4, SEGMENT 42: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb43, decile_b, V645 , target , Principaldebt ,0,4.5,4,4, SEGMENT 43: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb44, decile_b, V645 , target , Principaldebt ,4.5,0,4,4, SEGMENT 44: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb45, decile_b, V645 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 45: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb51, decile_b, V645 , target , Principaldebt ,0,0,4,4, SEGMENT 51: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb52, decile_b, V645 , target , Principaldebt ,0,4.5,4,4, SEGMENT 52: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb53, decile_b, V645 , target , Principaldebt ,4.5,0,4,4, SEGMENT 53: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb54, decile_b, V645 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 54: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb55, decile_b, V645 , target , Principaldebt ,0,0,4,4, SEGMENT 55: Decile ) ;
ods layout end ;
ods pdf close ;
ods layout end ;
ods pdf close ;

/*******************************************************************V655************************************************************/
ods pdf  body = "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\TU Performance Monitoring\reports\V6\Confident Band for V645 Model  &month..pdf";
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, month, V645 , target, Principaldebt ,0,0,4,4, V645 Overall Model ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, Decile_b, V645, target, Principaldebt ,0,0,4,4, V645 Overall Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, v6_seg, V645, target, Principaldebt ,0,0,4,4, Segments ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, V645_RiskGroup, V645, target, Principaldebt ,0,0,4,4, Risk Group ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, INSTITUTIONCODE, V645, target , Principaldebt ,0,0,4,4, INSTITUTION CODE) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(BNKSTD, month, V645 , target, Principaldebt, 0,4.5,4,4, BNKSTD ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(BNKFNB, month, V645 , target, Principaldebt ,4.5,0,4,4, BNKFNB ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(BNKNED, month, V645 , target, Principaldebt ,4.5,4.5,4,4, BNKNED ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(BNKABS, month , V655_2 , target , Principaldebt ,0,0,4,4, BNKABS) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(BNKCAP, month, V655_2 , target , Principaldebt ,0,4.5,4,4, BNKCAP ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(BNKABL, month, V655_2 , target , Principaldebt ,0,0,4,4, BNKABL ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(BNKINV, month, V655_2 , target , Principaldebt ,0,0,4,4, BNKINV ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(BNKOTH, month, V655_2 , target , Principaldebt ,0,0,4,4, BNKOTH ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb11, month , V655_2 , target , Principaldebt ,0,0,4,4, SEGMENT 11 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb12, month, V655_2 , target , Principaldebt ,0,4.5,4,4, SEGMENT 12 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb13, month, V655_2 , target , Principaldebt ,4.5,0,4,4, SEGMENT 13) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb14, month, V655_2 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 14 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb15, month, V655_2 , target , Principaldebt ,0,0,4,4, SEGMENT 15 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb21, month, V655_2 , target , Principaldebt ,0,4.5,4,4, SEGMENT 21 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb22, month, V655_2 , target , Principaldebt ,4.5,0,4,4, SEGMENT 22) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb23, month, V655_2 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 23 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb24, month, V655_2 , target , Principaldebt ,0,0,4,4, SEGMENT 24 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb25, month, V655_2 , target , Principaldebt ,0,4.5,4,4, SEGMENT 25 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb31, month, V655_2 , target , Principaldebt ,4.5,0,4,4, SEGMENT 31) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb32, month, V655_2 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 32 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb33, month, V655_2 , target , Principaldebt ,0,0,4,4, SEGMENT 33 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb34, month, V655_2 , target , Principaldebt ,0,4.5,4,4, SEGMENT 34 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb35, month, V655_2 , target , Principaldebt ,4.5,0,4,4, SEGMENT 35) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb41, month, V655_2 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 41 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb42, month, V655_2 , target , Principaldebt ,0,0,4,4, SEGMENT 42 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb43, month, V655_2 , target , Principaldebt ,0,4.5,4,4, SEGMENT 43 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb44, month, V655_2 , target , Principaldebt ,4.5,0,4,4, SEGMENT 44) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb45, month, V655_2 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 45 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb51, month, V655_2 , target , Principaldebt ,0,0,4,4, SEGMENT 51 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb52, month, V655_2 , target , Principaldebt ,0,4.5,4,4, SEGMENT 52 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb53, month, V655_2 , target , Principaldebt ,4.5,0,4,4, SEGMENT 53) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb54, month, V655_2 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 54 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb55, month, V655_2 , target , Principaldebt ,0,0,4,4, SEGMENT 55 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb11, decile_b , V655_2 , target , Principaldebt ,0,0,4,4, SEGMENT 11: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb12, decile_b, V655_2 , target , Principaldebt ,0,4.5,4,4, SEGMENT 12: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb13, decile_b, V655_2 , target , Principaldebt ,4.5,0,4,4, SEGMENT 13: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb14, decile_b, V655_2 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 14: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb15, decile_b, V655_2 , target , Principaldebt ,0,0,4,4, SEGMENT 15: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb21, decile_b, V655_2 , target , Principaldebt ,0,4.5,4,4, SEGMENT 21: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb22, decile_b, V655_2 , target , Principaldebt ,4.5,0,4,4, SEGMENT 22: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb23, decile_b, V655_2 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 23: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb24, decile_b, V655_2 , target , Principaldebt ,0,0,4,4, SEGMENT 24: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb25, decile_b, V655_2 , target , Principaldebt ,0,4.5,4,4, SEGMENT 25: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb31, decile_b, V655_2 , target , Principaldebt ,4.5,0,4,4, SEGMENT 31: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb32, decile_b, V655_2 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 32: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb33, decile_b, V655_2 , target , Principaldebt ,0,0,4,4, SEGMENT 33: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb34, decile_b, V655_2 , target , Principaldebt ,0,4.5,4,4, SEGMENT 34: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb35, decile_b, V655_2 , target , Principaldebt ,4.5,0,4,4, SEGMENT 35: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb41, decile_b, V655_2 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 41: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb42, decile_b, V655_2 , target , Principaldebt ,0,0,4,4, SEGMENT 42: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb43, decile_b, V655_2 , target , Principaldebt ,0,4.5,4,4, SEGMENT 43: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb44, decile_b, V655_2 , target , Principaldebt ,4.5,0,4,4, SEGMENT 44: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb45, decile_b, V655_2 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 45: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb51, decile_b, V655_2 , target , Principaldebt ,0,0,4,4, SEGMENT 51: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb52, decile_b, V655_2 , target , Principaldebt ,0,4.5,4,4, SEGMENT 52: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb53, decile_b, V655_2 , target , Principaldebt ,4.5,0,4,4, SEGMENT 53: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb54, decile_b, V655_2 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 54: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb55, decile_b, V655_2 , target , Principaldebt ,0,0,4,4, SEGMENT 55: Decile ) ;
ods layout end ;
ods pdf close ;
ods layout end ;

ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(BNKABS, month , V667 , target , Principaldebt ,0,0,4,4, BNKABS) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(BNKCAP, month, V667 , target , Principaldebt ,0,4.5,4,4, BNKCAP ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(BNKABL, month, V667 , target , Principaldebt ,0,0,4,4, BNKABL ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(BNKINV, month, V667 , target , Principaldebt ,0,0,4,4, BNKINV ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(BNKOTH, month, V667 , target , Principaldebt ,0,0,4,4, BNKOTH ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb11, month , V667 , target , Principaldebt ,0,0,4,4, SEGMENT 11 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb12, month, V667 , target , Principaldebt ,0,4.5,4,4, SEGMENT 12 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb13, month, V667 , target , Principaldebt ,4.5,0,4,4, SEGMENT 13) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb14, month, V667 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 14 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb15, month, V667 , target , Principaldebt ,0,0,4,4, SEGMENT 15 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb21, month, V667 , target , Principaldebt ,0,4.5,4,4, SEGMENT 21 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb22, month, V667 , target , Principaldebt ,4.5,0,4,4, SEGMENT 22) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb23, month, V667 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 23 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb24, month, V667 , target , Principaldebt ,0,0,4,4, SEGMENT 24 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb25, month, V667 , target , Principaldebt ,0,4.5,4,4, SEGMENT 25 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb31, month, V667 , target , Principaldebt ,4.5,0,4,4, SEGMENT 31) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb32, month, V667 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 32 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb33, month, V667 , target , Principaldebt ,0,0,4,4, SEGMENT 33 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb34, month, V667 , target , Principaldebt ,0,4.5,4,4, SEGMENT 34 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb35, month, V667 , target , Principaldebt ,4.5,0,4,4, SEGMENT 35) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb41, month, V667 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 41 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb42, month, V667 , target , Principaldebt ,0,0,4,4, SEGMENT 42 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb43, month, V667 , target , Principaldebt ,0,4.5,4,4, SEGMENT 43 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb44, month, V667 , target , Principaldebt ,4.5,0,4,4, SEGMENT 44) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb45, month, V667 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 45 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb51, month, V667 , target , Principaldebt ,0,0,4,4, SEGMENT 51 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb52, month, V667 , target , Principaldebt ,0,4.5,4,4, SEGMENT 52 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb53, month, V667 , target , Principaldebt ,4.5,0,4,4, SEGMENT 53) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb54, month, V667 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 54 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb55, month, V667 , target , Principaldebt ,0,0,4,4, SEGMENT 55 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb11, decile_b , V667 , target , Principaldebt ,0,0,4,4, SEGMENT 11: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb12, decile_b, V667 , target , Principaldebt ,0,4.5,4,4, SEGMENT 12: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb13, decile_b, V667 , target , Principaldebt ,4.5,0,4,4, SEGMENT 13: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb14, decile_b, V667 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 14: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb15, decile_b, V667 , target , Principaldebt ,0,0,4,4, SEGMENT 15: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb21, decile_b, V667 , target , Principaldebt ,0,4.5,4,4, SEGMENT 21: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb22, decile_b, V667 , target , Principaldebt ,4.5,0,4,4, SEGMENT 22: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb23, decile_b, V667 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 23: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb24, decile_b, V667 , target , Principaldebt ,0,0,4,4, SEGMENT 24: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb25, decile_b, V667 , target , Principaldebt ,0,4.5,4,4, SEGMENT 25: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb31, decile_b, V667 , target , Principaldebt ,4.5,0,4,4, SEGMENT 31: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb32, decile_b, V667 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 32: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb33, decile_b, V667 , target , Principaldebt ,0,0,4,4, SEGMENT 33: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb34, decile_b, V667 , target , Principaldebt ,0,4.5,4,4, SEGMENT 34: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb35, decile_b, V667 , target , Principaldebt ,4.5,0,4,4, SEGMENT 35: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb41, decile_b, V667 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 41: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb42, decile_b, V667 , target , Principaldebt ,0,0,4,4, SEGMENT 42: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb43, decile_b, V667 , target , Principaldebt ,0,4.5,4,4, SEGMENT 43: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb44, decile_b, V667 , target , Principaldebt ,4.5,0,4,4, SEGMENT 44: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb45, decile_b, V667 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 45: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb51, decile_b, V667 , target , Principaldebt ,0,0,4,4, SEGMENT 51: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb52, decile_b, V667 , target , Principaldebt ,0,4.5,4,4, SEGMENT 52: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb53, decile_b, V667 , target , Principaldebt ,4.5,0,4,4, SEGMENT 53: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb54, decile_b, V667 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 54: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb55, decile_b, V667 , target , Principaldebt ,0,0,4,4, SEGMENT 55: Decile ) ;
ods layout end ;
ods pdf close ;
ods layout end ;

ods pdf close ;

/*******************************************************************V635************************************************************/
ods pdf  body = "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\TU Performance Monitoring\reports\V6\Confident Band for V635 Model  &month..pdf";
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, month, V635 , target, Principaldebt ,0,0,4,4, V635 Overall Model ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, v6_Decile_b, V635, target, Principaldebt ,0,0,4,4, V635 Overall Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, v6_seg, V635, target, Principaldebt ,0,0,4,4, Segments ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, V635_RiskGroup, V635, target, Principaldebt ,0,0,4,4, Risk Group ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, INSTITUTIONCODE, V635, target , Principaldebt ,0,0,4,4, INSTITUTION CODE) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(BNKSTD, month, V635 , target, Principaldebt, 0,4.5,4,4, BNKSTD ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(BNKFNB, month, V635 , target, Principaldebt ,4.5,0,4,4, BNKFNB ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(BNKNED, month, V635 , target, Principaldebt ,4.5,4.5,4,4, BNKNED ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(BNKABS, month , V635 , target , Principaldebt ,0,0,4,4, BNKABS) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(BNKCAP, month, V635 , target , Principaldebt ,0,4.5,4,4, BNKCAP ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(BNKABL, month, V635 , target , Principaldebt ,0,0,4,4, BNKABL ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(BNKINV, month, V635 , target , Principaldebt ,0,0,4,4, BNKINV ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(BNKOTH, month, V635 , target , Principaldebt ,0,0,4,4, BNKOTH ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb11, month , V635 , target , Principaldebt ,0,0,4,4, SEGMENT 11 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb12, month, V635 , target , Principaldebt ,0,4.5,4,4, SEGMENT 12 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb13, month, V635 , target , Principaldebt ,4.5,0,4,4, SEGMENT 13) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb14, month, V635 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 14 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb15, month, V635 , target , Principaldebt ,0,0,4,4, SEGMENT 15 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb21, month, V635 , target , Principaldebt ,0,4.5,4,4, SEGMENT 21 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb22, month, V635 , target , Principaldebt ,4.5,0,4,4, SEGMENT 22) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb23, month, V635 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 23 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb24, month, V635 , target , Principaldebt ,0,0,4,4, SEGMENT 24 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb25, month, V635 , target , Principaldebt ,0,4.5,4,4, SEGMENT 25 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb31, month, V635 , target , Principaldebt ,4.5,0,4,4, SEGMENT 31) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb32, month, V635 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 32 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb33, month, V635 , target , Principaldebt ,0,0,4,4, SEGMENT 33 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb34, month, V635 , target , Principaldebt ,0,4.5,4,4, SEGMENT 34 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb35, month, V635 , target , Principaldebt ,4.5,0,4,4, SEGMENT 35) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb41, month, V635 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 41 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb42, month, V635 , target , Principaldebt ,0,0,4,4, SEGMENT 42 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb43, month, V635 , target , Principaldebt ,0,4.5,4,4, SEGMENT 43 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb44, month, V635 , target , Principaldebt ,4.5,0,4,4, SEGMENT 44) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb45, month, V635 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 45 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb51, month, V635 , target , Principaldebt ,0,0,4,4, SEGMENT 51 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb52, month, V635 , target , Principaldebt ,0,4.5,4,4, SEGMENT 52 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb53, month, V635 , target , Principaldebt ,4.5,0,4,4, SEGMENT 53) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb54, month, V635 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 54 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb55, month, V635 , target , Principaldebt ,0,0,4,4, SEGMENT 55 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb11, v6_decile_b , V635 , target , Principaldebt ,0,0,4,4, SEGMENT 11: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb12, v6_decile_b, V635 , target , Principaldebt ,0,4.5,4,4, SEGMENT 12: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb13, v6_decile_b, V635 , target , Principaldebt ,4.5,0,4,4, SEGMENT 13: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb14, v6_decile_b, V635 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 14: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb15, v6_decile_b, V635 , target , Principaldebt ,0,0,4,4, SEGMENT 15: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb21, v6_decile_b, V635 , target , Principaldebt ,0,4.5,4,4, SEGMENT 21: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb22, v6_decile_b, V635 , target , Principaldebt ,4.5,0,4,4, SEGMENT 22: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb23, v6_decile_b, V635 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 23: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb24, v6_decile_b, V635 , target , Principaldebt ,0,0,4,4, SEGMENT 24: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb25, v6_decile_b, V635 , target , Principaldebt ,0,4.5,4,4, SEGMENT 25: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb31, v6_decile_b, V635 , target , Principaldebt ,4.5,0,4,4, SEGMENT 31: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb32, v6_decile_b, V635 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 32: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb33, v6_decile_b, V635 , target , Principaldebt ,0,0,4,4, SEGMENT 33: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb34, v6_decile_b, V635 , target , Principaldebt ,0,4.5,4,4, SEGMENT 34: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb35, v6_decile_b, V635 , target , Principaldebt ,4.5,0,4,4, SEGMENT 35: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb41, v6_decile_b, V635 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 41: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb42, v6_decile_b, V635 , target , Principaldebt ,0,0,4,4, SEGMENT 42: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb43, v6_decile_b, V635 , target , Principaldebt ,0,4.5,4,4, SEGMENT 43: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb44, v6_decile_b, V635 , target , Principaldebt ,4.5,0,4,4, SEGMENT 44: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb45, v6_decile_b, V635 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 45: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb51, v6_decile_b, V635 , target , Principaldebt ,0,0,4,4, SEGMENT 51: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb52, v6_decile_b, V635 , target , Principaldebt ,0,4.5,4,4, SEGMENT 52: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb53, v6_decile_b, V635 , target , Principaldebt ,4.5,0,4,4, SEGMENT 53: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb54, v6_decile_b, V635 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 54: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb55, v6_decile_b, V635 , target , Principaldebt ,0,0,4,4, SEGMENT 55: Decile ) ;
ods layout end ;
ods pdf close ;
ods layout end ;
ods pdf close ;

/*******************************************************************V636************************************************************/
ods pdf  body = "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\TU Performance Monitoring\reports\V6\Confident Band for V636 Model  &month..pdf";
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, month, V636 , target, Principaldebt ,0,0,4,4, V636 Overall Model ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, v6_decile_b, V636, target, Principaldebt ,0,0,4,4, V636 Overall Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, v6_seg, V636, target, Principaldebt ,0,0,4,4, Segments ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, V636_RiskGroup, V636, target, Principaldebt ,0,0,4,4, Risk Group ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(DISBURSEDBASE4REBLT_&month, INSTITUTIONCODE, V636, target , Principaldebt ,0,0,4,4, INSTITUTION CODE) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(BNKSTD, month, V636 , target, Principaldebt, 0,4.5,4,4, BNKSTD ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(BNKFNB, month, V636 , target, Principaldebt ,4.5,0,4,4, BNKFNB ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(BNKNED, month, V636 , target, Principaldebt ,4.5,4.5,4,4, BNKNED ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(BNKABS, month , V636 , target , Principaldebt ,0,0,4,4, BNKABS) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(BNKCAP, month, V636 , target , Principaldebt ,0,4.5,4,4, BNKCAP ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(BNKABL, month, V636 , target , Principaldebt ,0,0,4,4, BNKABL ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(BNKINV, month, V636 , target , Principaldebt ,0,0,4,4, BNKINV ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(BNKOTH, month, V636 , target , Principaldebt ,0,0,4,4, BNKOTH ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb11, month , V636 , target , Principaldebt ,0,0,4,4, SEGMENT 11 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb12, month, V636 , target , Principaldebt ,0,4.5,4,4, SEGMENT 12 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb13, month, V636 , target , Principaldebt ,4.5,0,4,4, SEGMENT 13) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb14, month, V636 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 14 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb15, month, V636 , target , Principaldebt ,0,0,4,4, SEGMENT 15 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb21, month, V636 , target , Principaldebt ,0,4.5,4,4, SEGMENT 21 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb22, month, V636 , target , Principaldebt ,4.5,0,4,4, SEGMENT 22) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb23, month, V636 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 23 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb24, month, V636 , target , Principaldebt ,0,0,4,4, SEGMENT 24 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb25, month, V636 , target , Principaldebt ,0,4.5,4,4, SEGMENT 25 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb31, month, V636 , target , Principaldebt ,4.5,0,4,4, SEGMENT 31) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb32, month, V636 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 32 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb33, month, V636 , target , Principaldebt ,0,0,4,4, SEGMENT 33 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb34, month, V636 , target , Principaldebt ,0,4.5,4,4, SEGMENT 34 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb35, month, V636 , target , Principaldebt ,4.5,0,4,4, SEGMENT 35) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb41, month, V636 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 41 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb42, month, V636 , target , Principaldebt ,0,0,4,4, SEGMENT 42 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb43, month, V636 , target , Principaldebt ,0,4.5,4,4, SEGMENT 43 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb44, month, V636 , target , Principaldebt ,4.5,0,4,4, SEGMENT 44) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb45, month, V636 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 45 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb51, month, V636 , target , Principaldebt ,0,0,4,4, SEGMENT 51 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb52, month, V636 , target , Principaldebt ,0,4.5,4,4, SEGMENT 52 ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb53, month, V636 , target , Principaldebt ,4.5,0,4,4, SEGMENT 53) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb54, month, V636 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 54 ) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb55, month, V636 , target , Principaldebt ,0,0,4,4, SEGMENT 55 ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb11, v6_decile_b , V636 , target , Principaldebt ,0,0,4,4, SEGMENT 11: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb12, v6_decile_b, V636 , target , Principaldebt ,0,4.5,4,4, SEGMENT 12: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb13, v6_decile_b, V636 , target , Principaldebt ,4.5,0,4,4, SEGMENT 13: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb14, v6_decile_b, V636 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 14: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb15, v6_decile_b, V636 , target , Principaldebt ,0,0,4,4, SEGMENT 15: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb21, v6_decile_b, V636 , target , Principaldebt ,0,4.5,4,4, SEGMENT 21: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb22, v6_decile_b, V636 , target , Principaldebt ,4.5,0,4,4, SEGMENT 22: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb23, v6_decile_b, V636 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 23: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb24, v6_decile_b, V636 , target , Principaldebt ,0,0,4,4, SEGMENT 24: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb25, v6_decile_b, V636 , target , Principaldebt ,0,4.5,4,4, SEGMENT 25: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb31, v6_decile_b, V636 , target , Principaldebt ,4.5,0,4,4, SEGMENT 31: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb32, v6_decile_b, V636 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 32: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb33, v6_decile_b, V636 , target , Principaldebt ,0,0,4,4, SEGMENT 33: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb34, v6_decile_b, V636 , target , Principaldebt ,0,4.5,4,4, SEGMENT 34: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb35, v6_decile_b, V636 , target , Principaldebt ,4.5,0,4,4, SEGMENT 35: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb41, v6_decile_b, V636 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 41: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb42, v6_decile_b, V636 , target , Principaldebt ,0,0,4,4, SEGMENT 42: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb43, v6_decile_b, V636 , target , Principaldebt ,0,4.5,4,4, SEGMENT 43: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb44, v6_decile_b, V636 , target , Principaldebt ,4.5,0,4,4, SEGMENT 44: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb45, v6_decile_b, V636 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 45: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb51, v6_decile_b, V636 , target , Principaldebt ,0,0,4,4, SEGMENT 51: Decile ) ;
ods region y=4.5in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb52, v6_decile_b, V636 , target , Principaldebt ,0,4.5,4,4, SEGMENT 52: Decile ) ;
ods layout end ;
ods pdf startpage = now;
ods layout start;
ods region y=0in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb53, v6_decile_b, V636 , target , Principaldebt ,4.5,0,4,4, SEGMENT 53: Decile) ;
ods region y=0in x=4.5in width=4in height=4in;
%VarCheckConfidenceBand1(disb54, v6_decile_b, V636 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 54: Decile ) ;
ods region y=4.5in x=0in width=4in height=4in;
%VarCheckConfidenceBand1(disb55, v6_decile_b, V636 , target , Principaldebt ,0,0,4,4, SEGMENT 55: Decile ) ;
ods layout end ;
ods pdf close ;
ods layout end ;
ods pdf close ;

ODS _ALL_ CLOSE;

filename macros2 'H:\Process_Automation\macros';
options sasautos = (sasautos  macros2);
