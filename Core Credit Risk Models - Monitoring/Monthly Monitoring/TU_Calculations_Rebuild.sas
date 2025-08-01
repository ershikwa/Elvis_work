*Gets the overall gini and puts then in one dataset;
libname scores '\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\scores';
libname scores1 '\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\calibrated_scores';
%let lstmnth = %sysfunc(intnx(month,%sysfunc(today()),-1,e), yymmn6.);
%let lst2mnths = %sysfunc(intnx(month,%sysfunc(today()),-2,e), yymmn6.);
%let lst3mnths = %sysfunc(intnx(month,%sysfunc(today()),-3,e), yymmn6.);

/****************	Calculating the overall gini  *****************/

data scores.build_scorecomb_Rebuild;
	set scores.build_score1 scores.build_score2 scores.build_score3 scores.build_score4 scores.build_score5;
run;
%let Target = target; 
%let Final_Score = Final_Score;
%Calc_Gini (Final_Score, scores.build_scorecomb_Rebuild, target, work.GINITABLE) ;

Data scores.GINITABLE_Overall_Rebuild (keep=Gini segment);
	set GINITABLE;
	segment = 0;
	Rebuild_TU = Gini;
run;
/******** Collecting the Ginis including the over for the month ***********/

Data scores.Ginis_&lstmnth;
	set scores.GINITABLE_Overall_Rebuild scores.GINITABLE1 scores.GINITABLE2 scores.GINITABLE3 scores.GINITABLE4 scores.GINITABLE5;
	Score_type = "Rebuild_TU";
	Month = &lstmnth;
run;


/*Gets and applied ginis for the 3 applies and put them in one dataset*/
libname lindolib "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\scores\month1";
libname sphelib2 '\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\scores\month2';
libname sphelib3 '\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\scores\month3';


data scores.applied_ginis_&lstmnth;
	set lindolib.Ginis_&lstmnth sphelib2.Ginis_&lstmnth sphelib3.Ginis_&lst2mnths;
	keep gini segment Applied_Model Score_type Month;
	
run;

/*Appends the ginis in the consolidatedGini dataset for the last 3 months */
data scores.consolidatedGinis ;
	set scores.ginis_&lst3mnths scores.ginis_&lst2mnths scores.ginis_&lstmnth;
run;

/*Appends the applied ginis in the consolidatedGini dataset*/
data scores.consolidatedAppliedGinis ;
	set  scores.applied_ginis_&lstmnth;
run;
/**************************************************************************************************************
											Calculation after calibrations
**************************************************************************************************************/


/*	Calculating the overall gini  */
data scores1.calibrated_scorecomb_Rebuild;
	set scores1.Calibration_bank_1 scores1.Calibration_bank_2 scores1.Calibration_bank_3 scores1.Calibration_bank_4 scores1.Calibration_bank_5;
run;
%let Target = target; 
%let Final_Score = Prob3;
%Calc_Gini (Final_Score, scores1.calibrated_scorecomb_Rebuild, target, work.GINITABLE) ;

Data scores1.calibrated_Overall_Rebuild (keep=Gini segment);
	set GINITABLE;
	segment = 0;
	Rebuild_TU = Gini;
run;

/* Collecting the Ginis including the over for the month */
Data scores1.calibrated_Ginis_&lstmnth;
	set scores1.calibrated_Overall_Rebuild scores1.GINI1_&lstmnth scores1.GINI2_&lstmnth scores1.GINI3_&lstmnth scores1.GINI4_&lstmnth scores1.GINI5_&lstmnth;
	Score_type = "Rebuild_TU";
	Month = &lstmnth;
run;


/*Gets the applied ginis for the 3 applies and put them in one dataset*/
libname lindolib "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\scores\month1";
libname sphelib2 '\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\scores\month2';
libname sphelib3 '\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\scores\month3';
data scores1.calibrated_applied_&lstmnth;
	set lindolib.calibrated_Ginis_&lstmnth sphelib2.calibrated_Ginis_&lstmnth sphelib3.calibrated_Ginis_&lst2mnths;
	keep gini segment Applied_Model Score_type Month;
	
run;

/* Appends the ginis in the consolidatedGini dataset for the last 3 months */
data scores1.calibrated_rebuild_ginis ;
	set scores1.calibrated_ginis_&lst3mnths scores1.calibrated_ginis_&lst2mnths scores1.calibrated_ginis_&lstmnth;
run;

/*Appends the applied ginis in the consolidatedGini dataset*/
data scores1.Applied_calibrated_ginis ;
	set scores1.calibrated_applied_&lstmnth;
run;

filename macros2 'H:\Process_Automation\macros';
options sasautos = (sasautos  macros2);