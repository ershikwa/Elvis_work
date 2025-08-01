*Gets the overall gini and puts then in one dataset;
libname scores '\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\Refit_scores';
libname scores1 '\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\Calibrated_refit_scores';
%let lstmnth = %sysfunc(intnx(month,%sysfunc(today()),-1,e), yymmn6.);
%let lst2mnths = %sysfunc(intnx(month,%sysfunc(today()),-2,e), yymmn6.);
%let lst3mnths = %sysfunc(intnx(month,%sysfunc(today()),-3,e), yymmn6.);
************************************************************************************************************;
											*V570*
************************************************************************************************************;
/*data scores.build_scorecomb_Rebuild;*/
/*	set scores.build_score1 scores.build_score2 scores.build_score3 scores.build_score4 scores.build_score5;*/
/*run;*/
/**/
/*%let Target = target; */
/*%let Final_Score = Prob3;*/
/*%Calc_Gini (Final_Score, scores.build_scorecomb_Rebuild, target, work.GINITABLE) ;*/
/**/
/*Data scores.GINITABLE_Overall_Rebuild (keep=Gini segment);*/
/*	set GINITABLE;*/
/*	segment = 0;*/
/*	Refit_TU = Gini;*/
/*run;*/
/**/
/*Data scores.refit_Ginis_&lstmnth;*/
/*	set scores.GINITABLE_Overall_Rebuild scores.GINITABLE1 scores.GINITABLE2 scores.GINITABLE3 scores.GINITABLE4 scores.GINITABLE5;*/
/*	Score_type = "Refit_TU";*/
/*	Month = &lstmnth;*/
/*run;*/
/**/
/**/
/**Gets and applied ginis for the 3 applies and put them in one dataset;*/
/*libname lindolib "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\Refit_scores\month1";*/
/*libname sphelib2 '\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\Refit_scores\month2';*/
/*libname sphelib3 '\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\Refit_scores\month3';*/
/**/
/**/
/*data scores.refit_applied_ginis_&lstmnth;*/
/*	set lindolib.Ginis_&lstmnth sphelib2.Ginis_&lstmnth sphelib3.Ginis_&lst2mnths;*/
/*	keep gini segment Applied_Model Score_type Month;*/
/*	*/
/*run;*/
/**/
/**Appends the ginis in the consolidatedGini dataset;*/
/*data scores.Consolidated_Refit_Ginis  ;*/
/*	set scores.refit_Ginis_&lst3mnths scores.refit_Ginis_&lst2mnths  scores.refit_Ginis_&lstmnth;*/
/*run;*/
/**/
/**Appends the applied ginis in the consolidatedGini dataset;*/
/*data scores.Applied_Consolidated_Refit_Ginis ;*/
/*	set  scores.refit_applied_ginis_&lstmnth;*/
/*run;*/
/**/
/**********************************************************************************************************************;*/
/**After Calibration ;*/
/**********************************************************************************************************************;*/
/**/
/*data scores1.build_scorecomb_Rebuild;*/
/*	set scores1.Calibration_bank_1 scores1.Calibration_bank_2 scores1.Calibration_bank_3 scores1.Calibration_bank_4 scores1.Calibration_bank_5;*/
/*run;*/
/**/
/*%let Target = target; */
/*%let Final_Score = Prob3;*/
/*%Calc_Gini (Final_Score, scores1.build_scorecomb_Rebuild, target, work.GINITABLE) ;*/
/**/
/*Data scores1.GINITABLE_Overall_Rebuild (keep=Gini segment);*/
/*	set GINITABLE;*/
/*	segment = 0;*/
/*	Refit_TU = Gini;*/
/*run;*/
/**/
/*Data scores1.refit_Ginis_&lstmnth;*/
/*	set scores1.GINITABLE_Overall_Rebuild scores1.GINI1_&lstmnth scores1.GINI2_&lstmnth scores1.GINI3_&lstmnth scores1.GINI4_&lstmnth scores1.GINI5_&lstmnth;*/
/*	Score_type = "Refit_TU";*/
/*	Month = &lstmnth;*/
/*run;*/
/**/
/**/
/**Gets and applied ginis for the 3 applies and put them in one dataset;*/
/*libname lindolib "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\Refit_scores\month1";*/
/*libname sphelib2 '\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\Refit_scores\month2';*/
/*libname sphelib3 '\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\Refit_scores\month3';*/
/**/
/**/
/*data scores1.calibrated_applied_ginis_&lstmnth;*/
/*	set lindolib.c_Ginis_&lstmnth sphelib2.c_Ginis_&lstmnth sphelib3.c_Ginis_&lst2mnths;*/
/*	keep gini segment Applied_Model Score_type Month;*/
/*	*/
/*run;*/
/**/
/**Appends the ginis in the consolidatedGini dataset;*/
/*data scores1.calibrated_refit_ginis  ;*/
/*	set scores1.refit_Ginis_&lst3mnths  scores1.refit_Ginis_&lst2mnths scores1.refit_Ginis_&lstmnth;;*/
/*run;*/
/**/
/**Appends the applied ginis in the consolidatedGini dataset;*/
/*data scores1.applied_calibrated_refit_ginis ;*/
/*	set scores1.calibrated_applied_ginis_&lstmnth;*/
/*run;*/
/**/;






*Gets the overall gini and puts then in one dataset;
libname scores '\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\V580 TU_Refit\Refit_scores';
libname scores1 '\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\V580 TU_Refit\Calibrated_refit_scores';

************************************************************************************************************;
											*V580*
************************************************************************************************************;
data scores.build_scorecomb_Rebuild;
	set scores.build_score1 scores.build_score2 scores.build_score3 scores.build_score4 scores.build_score5;
run;

%let Target = target; 
%let Final_Score = Prob3;
%Calc_Gini (Final_Score, scores.build_scorecomb_Rebuild, target, work.GINITABLE) ;

Data scores.GINITABLE_Overall_Rebuild (keep=Gini segment);
	set GINITABLE;
	segment = 0;
	Refit_TU = Gini;
run;

Data scores.refit_Ginis_&lstmnth;
	set scores.GINITABLE_Overall_Rebuild scores.GINITABLE1 scores.GINITABLE2 scores.GINITABLE3 scores.GINITABLE4 scores.GINITABLE5;
	Score_type = "Refit_TU";
	Month = &lstmnth;
run;


*Gets and applied ginis for the 3 applies and put them in one dataset;
libname lindolib "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\V580 TU_Refit\Refit_scores\month1";
libname sphelib2 '\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\V580 TU_Refit\Refit_scores\month2';
libname sphelib3 '\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\V580 TU_Refit\Refit_scores\month3';


data scores.refit_applied_ginis_&lstmnth;
	set lindolib.Ginis_&lstmnth sphelib2.Ginis_&lstmnth sphelib3.Ginis_&lst2mnths;
	keep gini segment Applied_Model Score_type Month;
	
run;

*Appends the ginis in the consolidatedGini dataset;
data scores.Consolidated_Refit_Ginis  ;
	set scores.refit_Ginis_&lst3mnths scores.refit_Ginis_&lst2mnths  scores.refit_Ginis_&lstmnth;
run;

*Appends the applied ginis in the consolidatedGini dataset;
data scores.Applied_Consolidated_Refit_Ginis ;
	set  scores.refit_applied_ginis_&lstmnth;
run;

*********************************************************************************************************************;
*After Calibration ;
*********************************************************************************************************************;

data scores1.build_scorecomb_Rebuild;
	set scores1.Calibration_bank_1 scores1.Calibration_bank_2 scores1.Calibration_bank_3 scores1.Calibration_bank_4 scores1.Calibration_bank_5;
run;

%let Target = target; 
%let Final_Score = Prob3;
%Calc_Gini (Final_Score, scores1.build_scorecomb_Rebuild, target, work.GINITABLE) ;

Data scores1.GINITABLE_Overall_Rebuild (keep=Gini segment);
	set GINITABLE;
	segment = 0;
	Refit_TU = Gini;
run;

Data scores1.refit_Ginis_&lstmnth;
	set scores1.GINITABLE_Overall_Rebuild scores1.GINI1_&lstmnth scores1.GINI2_&lstmnth scores1.GINI3_&lstmnth scores1.GINI4_&lstmnth scores1.GINI5_&lstmnth;
	Score_type = "Refit_TU";
	Month = &lstmnth;
run;


*Gets and applied ginis for the 3 applies and put them in one dataset;
libname lindolib "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\V580 TU_Refit\Refit_scores\month1";
libname sphelib2 '\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\V580 TU_Refit\Refit_scores\month2';
libname sphelib3 '\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\V580 TU_Refit\Refit_scores\month3';


data scores1.calibrated_applied_ginis_&lstmnth;
	set lindolib.c_Ginis_&lstmnth sphelib2.c_Ginis_&lstmnth sphelib3.c_Ginis_&lst2mnths;
	keep gini segment Applied_Model Score_type Month;
	
run;

*Appends the ginis in the consolidatedGini dataset;
data scores1.calibrated_refit_ginis  ;
	set scores1.refit_Ginis_&lst3mnths  scores1.refit_Ginis_&lst2mnths scores1.refit_Ginis_&lstmnth;;
run;

*Appends the applied ginis in the consolidatedGini dataset;
data scores1.applied_calibrated_refit_ginis ;
	set scores1.calibrated_applied_ginis_&lstmnth;
run;

filename macros2 'H:\Process_Automation\macros';
options sasautos = (sasautos  macros2);
