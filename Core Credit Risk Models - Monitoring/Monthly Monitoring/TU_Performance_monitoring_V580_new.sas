%include "\\neptune\sasa$\SAS_Automation\SAS_Autoexec\autoexec6.sas";
/*\\mpwsas64\SAS_Automation\SAS_Autoexec\autoexec2.sas */

OPTIONS NOSYNTAXCHECK ;
options compress = yes;
%include "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\macros\CreateMonthlyGini.sas";
libname tu '\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets\V580';
libname tu2 '\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets';
libname decile "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\SAS Decile Tables\TU V580";
libname Lookup '\\mpwsas64\Core_Credit_Risk_Models\V6\MetaData';
libname V6 '\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\V6 dataset';
libname d1 "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU\Rebuild_TU_seg1";
libname d2 "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU\Rebuild_TU_seg2";
libname d3 "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU\Rebuild_TU_seg3";
libname d4 "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU\Rebuild_TU_seg4";
libname d5 "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU\Rebuild_TU_seg5";
libname seg1 "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU\scores";
libname seg2 "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU\scores";
libname seg3 "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU\scores";
libname seg4 "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU\scores";
libname seg5 "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU\scores";
libname results "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V632 Model\V623\Data";
libname rebuilt "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\scores";
libname refit "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\V580 TU_Refit\Refit_scores";
libname crebuilt "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\calibrated_scores";
libname crefit "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\V580 TU_Refit\Calibrated_refit_scores";
libname source "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\TU Performance Monitoring\data";
libname Rejects "\\mpwsas64\Core_Credit_Risk_Models\V5\New_Rejects";
%let odbc = MPWAPS;

*production;
filename macros4 '\\mpwsas65\process_automation\Codes\Git_Repositories\Core Credit Risk Models - Monitoring\Macros';
options sasautos = (macros4);
*Testing;
/*filename macros4 '\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros';*/
/*options sasautos = (macros4);*/
%let odbc = MPWAPS;
data _null_;
     call symput("enddate",cats("'",put(intnx('month',today(),-1,'end'),yymmddd10.),"'"));
     call symput("startdate",cats("'",put(intnx('month',today(),-13,'end'),yymmddd10.),"'"));
     call symput('tday',put(intnx('day',today(),-1),yymmddn8.));
     call symput("actual_date", put(intnx("month", today(),-9,'end'),date9.));
     call symput("month", put(intnx("month", today(),-1,'end'),yymmn6.));
     call symput("prevmonth", put(intnx("month", today(),-2,'end'),yymmn6.));
	 call symput("prev2month", put(intnx("month", today(),-3,'end'),yymmn6.));
	 call symput("build_start", put(intnx("month", today(),-12,'end'),yymmn6.));
	 call symput("build_end", put(intnx("month", today(),-7,'end'),yymmn6.));
run;

%put &startdate ;
%put &enddate ;
%put &tday;
%put &actual_date;
%put &month;
%put &prevmonth;
%put &prev2month;
%put &build_start;
%put &build_end;

%macro ODSOff(); 
%mend;

%macro ODSOn(); 
%mend;

proc format cntlin =decile._decile1_ fmtlib ;run;
proc format cntlin =decile._decile2_ fmtlib ;run;
proc format cntlin =decile._decile3_ fmtlib ;run;
proc format cntlin =decile._decile4_ fmtlib ;run;
proc format cntlin =decile._decile5_ fmtlib ;run;

%let codepath = \\mpwsas64\Core_Credit_Risk_Model_Team\Elvis\Monthly Monitoring\New Codes;

%include "&codepath\Create_TransUnionCSIReport.sas";

%include "&codepath\Create_TransUnionSlopeRateReport.sas";

%include "&codepath\Create_GiniReport_TU.sas";

/* %include "&codepath\Create_Calibrated_Report.sas"; */

%include "&codepath\FinalizingVariableStability_TU.sas";

%include "&codepath\Create_ChallengerModelsTU.sas";

%macro UploadFinalTablesForDashboard();
	
/*	%if %sysfunc(exist(currentmodel_bench_reblt_&month.)) = 0 or %sysfunc(exist(tu.giniperseg_summary_reblt_&month.)) = 0 */
/*		or %sysfunc(exist(tu.Summarytable_01_&month.)) = 0 or %sysfunc(exist(tu.Tu_Var_Distribution_reblt_&month.)) = 0*/
/*		or %sysfunc(exist(tu.Variables_stability_reblt_&month.)) = 0 or %sysfunc(exist(tu.V5_sloperate_reblt_&month.)) = 0*/
/*		or %sysfunc(exist(tu.CSI_Distribution_reblt_&month.)) = 0 or %sysfunc(exist(tu.CHALLENGER_MODELS_&month.)) = 0*/
/*	%then %do;*/

 
	/********************************************************************************************************************************************************/
	/* 												Refresh Power BI after these codes			 															*/
	/********************************************************************************************************************************************************/
	libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
	proc delete data =cred_scr.tu_v5_Gini_relative_reblt; run;
	proc sql;
		create table  cred_scr.tu_v5_Gini_relative_reblt(BULKLOAD=YES) as
			select  distinct * 
			from tu.currentmodel_bench_reblt_&month.;
	quit;


	libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
	proc delete data =cred_scr.tu_Gini_summary_reblt; run;
	proc sql;
		create table  cred_scr.tu_Gini_summary_reblt(BULKLOAD=YES) as
			select  distinct * 
			from tu.giniperseg_summary_reblt_&month;
	quit;

	libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
	proc delete data =cred_scr.TU_v5_Segment_distribution_reblt; run;
	proc sql;
		create table  cred_scr.tu_v5_Segment_distribution_reblt(BULKLOAD=YES) as
			select  distinct * 
			from tu.Summarytable_01_&month.;
	quit;


	libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
	proc delete data =cred_scr.TU_distribution_summary_reblt; run;
	proc sql;
		create table  cred_scr.TU_distribution_summary_reblt(BULKLOAD=YES) as
			select  distinct * 
			from tu.Tu_Var_Distribution_reblt_&month.;
	quit; 


	libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
	proc delete data =cred_scr.TU_V5_Variables_stability_reblt; run;
	proc sql;
		create table  cred_scr.TU_V5_Variables_stability_reblt(BULKLOAD=YES) as
			select  distinct * 
			from tu.Variables_stability_reblt_&month.;
	quit;


	libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
	proc delete data =cred_scr.TU_v5_sloperate_reblt; run;
	proc sql;
		create table  cred_scr.TU_v5_sloperate_reblt(BULKLOAD=YES) as
			select  distinct * 
			from tu.V5_sloperate_reblt_&month.;
	quit;


	libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
	proc delete data =cred_scr.TU_CSI_Distribution_reblt; run;
	proc sql;
		create table  cred_scr.TU_CSI_Distribution_reblt(BULKLOAD=YES) as
			select  distinct * 
			from tu.CSI_Distribution_reblt_&month.;
	quit;


	libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
	proc delete data =cred_scr.TU_Challanger_models_reblt; run;
	proc sql;
		create table  cred_scr.TU_Challanger_models_reblt(BULKLOAD=YES) as
			select  distinct * 
			from tu.CHALLENGER_MODELS_&month.;
	quit;

/*	%else*/
/*	%put "One or more of tha tables are missing. Please check which table was not created and create it";*/
/*	%end;*/
/*%mend;*/
%UploadFinalTablesForDashboard();

/*CREATE PLOTS AND REPORTS*/
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\Elvis\Monthly Monitoring\New Codes\position.sas";
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\Elvis\Monthly Monitoring\New Codes\plotconfidencebands.sas";
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\Elvis\Monthly Monitoring\New Codes\VarCheckConfidenceBand1.sas";
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\Elvis\Monthly Monitoring\New Codes\buildreporttu.sas";

%include "\\mpwsas64\Core_Credit_Risk_Model_Team\Elvis\Monthly Monitoring\New Codes\Create_TUFinalPlots_Reports.sas";