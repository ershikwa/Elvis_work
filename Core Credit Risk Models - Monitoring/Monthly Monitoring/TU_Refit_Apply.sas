/*%include "\\neptune\credit$\AA_GROUP CREDIT\Scoring\SAS Macros\Upload_APS.sas";*/
libname paths "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\Disbursals"; 
libname dataset  "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets"; 
%let lstmnth = %sysfunc(intnx(month,%sysfunc(today()),-1,e), yymmn6.);
%let lst2mnths = %sysfunc(intnx(month,%sysfunc(today()),-2,e), yymmn6.);
%let lst3mnths = %sysfunc(intnx(month,%sysfunc(today()),-3,e), yymmn6.);
%let mnth6ago = %sysfunc(intnx(month,%sysfunc(today()),-6,e), yymmn6.);
%let mnth11ago = %sysfunc(intnx(month,%sysfunc(today()),-12, b), yymmn6.);

%put &lstmnth.;
%put &lst2mnths.;
%put &lst3mnths.;

data DISBURSEDBASE4REBLT_&lstmnth;
	set dataset.DISBURSEDBASE4REBLT_&lstmnth;
	format month1 BEST12. ;
	month1 = input(month,BEST12.);
	seg = tu_seg;
run;

/*&lstmnth. = 202002*/
/*&lst2mnths. = 202001*/
/*&lst3mnths. = 201912*/
%macro MonitorRebuilt(segnum);

%do p=1 %to &segnum; 

/*%let p = 1;*/

%let path = \\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\V570 TU Refit;
libname data3M_&p. "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\V570 TU Refit\TU_Refit_&lst3mnths.\Segment&p.";
libname data2M_&p. "\\mpwsas64\Core_Credit_Risk_Models\V5\application scorecard\v6 monitoring\V570 TU Refit\TU_Refit_&lst2mnths.\Segment&p.";
libname data1M_&p. "\\mpwsas64\Core_Credit_Risk_Models\V5\application scorecard\v6 monitoring\V570 TU Refit\TU_Refit_&lstmnth.\Segment&p.";
libname data 	 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets";
libname lindolib "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\Refit_scores\month1";

/*Get list of vars from 3 months ago*/
/*Sphe Notes: Use this in data step*/
proc sql; 
	select reverse(substr(reverse(compress(Parameter)),3)) into : segment_&p._list separated by ' ' 
	from data3M_&p..parameter_estimate
	where upcase(Parameter) ne 'INTERCEPT'; 
quit;

/*Sphe Notes: Use as is in proc sql step*/
proc sql; 
	select reverse(substr(reverse(compress(Parameter)),3)) into : segment_&p._list2 separated by ',' 
	from data3M_&p..parameter_estimate
	where upcase(Parameter) ne 'INTERCEPT'; 
quit;

%put &&segment_&p._list.;
%put &&segment_&p._list2.;

/*Apply Scoring Model*/
%macro applyscore(t);
	%do j = 1 %to %sysfunc(countw(&&segment_&t._list));
	    %let var = %scan(&&segment_&t._list, &j);
	    %include "&path.\TU_Refit_&lst3mnths.\Segment&t.\&var._if_statement_.sas";
	    %include "&path.\TU_Refit_&lst3mnths.\Segment&t.\&var._WOE_if_statement_.sas"; 
	%end;
	*****************************************;
	** SAS Scoring Code for PROC Hplogistic;
	*****************************************;
	%include "&path.\TU_Refit_&lst3mnths.\Segment&t.\creditlogisticcode2.sas";
%mend;

*****************************************;
** 			Score 3 Months Ago		*****;
*****************************************;


libname iv "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\TU Performance Monitoring\V570 TU Refit\Seg&p.\iv";

proc sql;
	select tranwrd(_NAME_,"_W","") into : varlist separated by " " 
	from iv.wght_trans 
	where upcase(_NAME_) ne "INTERCEPT"
;
quit;

%put &varlist.;

%macro m;
	data build1; 
		set disbursedbase4reblt_&lstmnth;
		%do k = 1 %to %sysfunc(countw(&varlist.));
			%let var = %scan(&varlist., &k);
			&var._sb = SUBSTR(&var._b, 1, 2);
		%end;
	run;
%mend;

%m;

proc sql;
	create table segment&p._data as 
	select uniqueid, &&segment_&p._list2., target, month, seg 
	from build1
	where seg = &p.;
quit;

/*Apply scoring model to dataset generated above*/
data segment_&p._&&lstmnth;
	set segment&p._data (where=(seg=&p.));
	%applyscore(&p.); 
	Final_score = P_target1;
	Score = 1000-(Final_score*1000);    
	drop _TEMP;
run;



data final_model_data_&lstmnth;
	set segment_&p._&&lstmnth.; /* Sphe Notes: period (.) tells SAS that this is a macro variable coz sometimes SAS gets confused */
run;

data lindolib.scored_&p._&&lstmnth;
    set final_model_data_&lstmnth;  
	probability = Final_score;
run;




%let Target = target; 
%let Final_Score = Final_Score;
%Calc_Gini (Final_Score, lindolib.scored_&p._&&lstmnth, target, work.GINITABLE) ;

data lindolib.Gini_Seg&p._&&lstmnth;
set  GiniTable;
Applied_Model = "&lst3mnths";
Segment = &p.;
where Gini ne .;
run;

	%end;




data lindolib.build_scorecomb_Refit;
	set lindolib.scored_1_&lstmnth lindolib.scored_2_&lstmnth lindolib.scored_3_&lstmnth lindolib.scored_4_&lstmnth lindolib.scored_5_&lstmnth;
run;


%Calc_Gini (Final_Score, lindolib.build_scorecomb_Refit, target, work.GINITABLE) ;

Data lindolib.GINITABLE_Overall_Refit (keep=Gini segment);
	set GINITABLE;
	Applied_Model = "&lst3mnths";
	segment = 0;
	Refit_TU = Gini;
run;

		Data lindolib.Ginis_&lstmnth;
			set lindolib.GINITABLE_Overall_Refit lindolib.Gini_Seg1_&lstmnth lindolib.Gini_Seg2_&lstmnth lindolib.Gini_Seg3_&lstmnth lindolib.Gini_Seg4_&lstmnth lindolib.Gini_Seg5_&lstmnth;
			Score_type = "Refit_TU";
			Month = &lstmnth;
			Applied_Model = "&lst3mnths";
		run;
%mend;

%MonitorRebuilt(5);


%macro MonitorRebuilt2MthsModel(segnum);

%do p=1 %to &segnum; 
%let path = \\mpwsas64\Core_Credit_Risk_Models\V5\application scorecard\v6 monitoring\V570 TU Refit;

libname sphelib2 '\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\Refit_scores\month2';

/*Get list of vars from 2 months ago seg */
/*Sphe Notes: Use this in data step*/
proc sql; 
	select reverse(substr(reverse(compress(Parameter)),3)) into : segment_&p._list separated by ' ' 
	from data2M_&p..parameter_estimate
	where upcase(Parameter) ne 'INTERCEPT'; 
quit;

/*Sphe Notes: Use as is in proc sql step*/
proc sql; 
	select reverse(substr(reverse(compress(Parameter)),3)) into : segment_&p._list2 separated by ',' 
	from data2M_&p..parameter_estimate
	where upcase(Parameter) ne 'INTERCEPT'; 
quit;

%put &&segment_&p._list.;
%put &&segment_&p._list2.;

/*Apply Scoring Model*/
%macro applyscore(t);
	%do j = 1 %to %sysfunc(countw(&&segment_&t._list));
	    %let var = %scan(&&segment_&t._list, &j);
	    %include "&path.\TU_Refit_&lst2mnths\Segment&t.\&var._if_statement_.sas";
	    %include "&path.\TU_Refit_&lst2mnths\Segment&t.\&var._WOE_if_statement_.sas"; 
	%end;
	*****************************************;
	** SAS Scoring Code for PROC Hplogistic;
	*****************************************;
	%include "&path.\TU_Refit_&lst2mnths\Segment&t.\creditlogisticcode2.sas";
%mend;

*****************************************;
** 			Score 3 Months Ago		*****;
*****************************************;
libname iv "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\TU Performance Monitoring\V570 TU Refit\Seg&p.\iv";

proc sql;
	select tranwrd(_NAME_,"_W","") into : varlist separated by " " 
	from iv.wght_trans 
	where upcase(_NAME_) ne "INTERCEPT"
;
quit;

%put &varlist.;

%macro m;
	data build1; 
		set disbursedbase4reblt_&lstmnth;
		%do k = 1 %to %sysfunc(countw(&varlist.));
			%let var = %scan(&varlist., &k);
			&var._sb = SUBSTR(&var._b, 1, 2);
		%end;
	run;
%mend;

%m;
/*Selecting vars from dataset to score*/
proc sql;
	create table segment&p._data as 
	select uniqueid, &&segment_&p._list2., target, month, seg 
	from build1
	where seg = &p.;
quit;

/*Apply scoring model to dataset generated above*/
data segment_&p._&&lstmnth;
	set segment&p._data (where=(seg=&p.));
	%applyscore(&p.); 
	Final_score = P_target1;
	Score = 1000-(Final_score*1000);    
	drop _TEMP;
run;

data final_model_data_&lstmnth;
	set segment_&p._&&lstmnth.;
run;

data sphelib2.scored_&p._&&lstmnth;
    set final_model_data_&lstmnth;  
	probability = Final_score;
run;



%let Target = target; 
%let Final_Score = Final_Score;
%Calc_Gini (Final_Score, sphelib2.scored_&p._&&lstmnth, target, work.GINITABLE) ;


data sphelib2.Gini_Seg&p._&&lstmnth;
	set  GiniTable;
	Applied_Model = "&lst2mnths";
	Segment = &p.;
	where Gini ne .;
run;


	%end;



data sphelib2.build_scorecomb_Rebuild;
	set sphelib2.scored_1_&lstmnth sphelib2.scored_2_&lstmnth sphelib2.scored_3_&lstmnth sphelib2.scored_4_&lstmnth sphelib2.scored_5_&lstmnth;
run;


%Calc_Gini (Final_Score, sphelib2.build_scorecomb_Rebuild, target, work.GINITABLE) ;

Data sphelib2.GINITABLE_Overall_Rebuild (keep=Gini segment);
	set GINITABLE;
	Applied_Model = "&lst2mnths";
	segment = 0;
	Refit_TU = Gini;
run;

Data sphelib2.Ginis_&lstmnth;
	set sphelib2.GINITABLE_Overall_Rebuild sphelib2.Gini_Seg1_&lstmnth sphelib2.Gini_Seg2_&lstmnth sphelib2.Gini_Seg3_&lstmnth sphelib2.Gini_Seg4_&lstmnth sphelib2.Gini_Seg5_&lstmnth;
	Score_type = "Refit_TU";
	Month = &lstmnth;
	Applied_Model = "&lst2mnths";
run;


%mend;
%MonitorRebuilt2MthsModel(5);

%macro MonitorRebuilt1MthModel(segnum);

%do p=1 %to &segnum; 
%let path = \\mpwsas64\Core_Credit_Risk_Models\V5\application scorecard\v6 monitoring\V570 TU Refit;

libname sphelib3 '\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\Refit_scores\month3';

/*Get list of vars from 1 month ago*/
/*Sphe Notes: Use this in data step*/
proc sql; 
	select reverse(substr(reverse(compress(Parameter)),3)) into : segment_&p._list separated by ' ' 
	from data3M_&p..parameter_estimate
	where upcase(Parameter) ne 'INTERCEPT'; 
quit;

/*Sphe Notes: Use as is in proc sql step*/
proc sql; 
	select reverse(substr(reverse(compress(Parameter)),3)) into : segment_&p._list2 separated by ',' 
	from data3M_&p..parameter_estimate
	where upcase(Parameter) ne 'INTERCEPT'; 
quit;

%put &&segment_&p._list.;
%put &&segment_&p._list2.;

/*Apply Scoring Model*/
%macro applyscore(t);
	%do j = 1 %to %sysfunc(countw(&&segment_&t._list));
	    %let var = %scan(&&segment_&t._list, &j);
	    %include "&path.\TU_Refit_&lst3mnths.\Segment&t.\&var._if_statement_.sas";
	    %include "&path.\TU_Refit_&lst3mnths.\Segment&t.\&var._WOE_if_statement_.sas"; 
	%end;
	*****************************************;
	** SAS Scoring Code for PROC Hplogistic;
	*****************************************;
	%include "&path.\TU_Refit_&lst3mnths.\Segment&t.\creditlogisticcode2.sas";
%mend;

*****************************************;
** 			Score 3 Months Ago		*****;
*****************************************;
libname iv "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\TU Performance Monitoring\V570 TU Refit\Seg&p.\iv";

proc sql;
	select tranwrd(_NAME_,"_W","") into : varlist separated by " " 
	from iv.wght_trans 
	where upcase(_NAME_) ne "INTERCEPT"
;
quit;

%put &varlist.;

%macro m;
	data build1; 
		set data.disbursedbase4reblt_&lst2mnths;
		 seg = Tu_seg;
		%do k = 1 %to %sysfunc(countw(&varlist.));
			%let var = %scan(&varlist., &k);
			&var._sb = SUBSTR(&var._b, 1, 2);
		%end;
	run;
%mend;

%m;
/*Selecting vars from dataset to score*/
proc sql;
	create table segment&p._data as 
	select uniqueid, &&segment_&p._list2., target, month, seg 
	from build1
	where seg = &p.;
quit;

/*Apply scoring model to dataset generated above*/
data segment_&p._&&lst2mnths;
	set segment&p._data (where=(seg=&p.));
	%applyscore(&p.); 
	Final_score = P_target1;
	Score = 1000-(Final_score*1000);    
	drop _TEMP;
run;

data final_model_data_&lst2mnths;
	set segment_&p._&&lst2mnths.;
run;

data sphelib3.scored_&p._&&lst2mnths;
    set final_model_data_&lst2mnths;  
	probability = Final_score;
run;

options spool mprint mlogic;



%let Target = target; 
%let Final_Score = Final_Score;
%Calc_Gini (Final_Score, sphelib3.scored_&p._&&lst2mnths, target, work.GINITABLE) ;


data sphelib3.Gini_Seg&p._&&lst2mnths;
	set  GiniTable;
	Applied_Model = "&lst3mnths";
	Segment = &p.;
	where Gini ne .;
run;


	%end;





data sphelib3.build_scorecomb_Rebuild;
	set sphelib3.scored_1_&lst2mnths sphelib3.scored_2_&lst2mnths sphelib3.scored_3_&lst2mnths sphelib3.scored_4_&lst2mnths sphelib3.scored_5_&lst2mnths;
run;


%Calc_Gini (Final_Score, sphelib3.build_scorecomb_Rebuild, target, work.GINITABLE) ;

Data sphelib3.GINITABLE_Overall_Rebuild (keep=Gini segment);
	set GINITABLE;
	Applied_Model = "&lst3mnths";
	segment = 0;
	Refit_TU = Gini;
run;

Data sphelib3.Ginis_&lst2mnths;
	set sphelib3.GINITABLE_Overall_Rebuild sphelib3.Gini_Seg1_&lst2mnths sphelib3.Gini_Seg2_&lst2mnths sphelib3.Gini_Seg3_&lst2mnths sphelib3.Gini_Seg4_&lst2mnths sphelib3.Gini_Seg5_&lst2mnths;
	Score_type = "Refit_TU";
	Month = &lst2mnths;
	Applied_Model = "&lst3mnths";
run;


%mend;
%MonitorRebuilt1MthModel(5);


/*&lstmnth. = 202002*/
/*&lst2mnths. = 202001*/
/*&lst3mnths. = 201912*/
proc datasets library=work kill nolist;
quit;

data DISBURSEDBASE4REBLT_&lstmnth;
	set data.DISBURSEDBASE4REBLT_&lstmnth;
	format month1 BEST12. ;
	month1 = input(month,BEST12.);
	seg = tu_seg;
run;

%macro MonitorRebuilt1(segnum);

%do p=1 %to &segnum; 

/*%let p = 1;seg */

%let path = \\mpwsas64\Core_Credit_Risk_Models\V5\application scorecard\v6 monitoring\V570 TU Refit;

libname data3M_&p. "\\mpwsas64\Core_Credit_Risk_Models\V5\application scorecard\v6 monitoring\V570 TU Refit\TU_Refit_&lst3mnths.\Segment&p.";
libname data2M_&p. "\\mpwsas64\Core_Credit_Risk_Models\V5\application scorecard\v6 monitoring\V570 TU Refit\TU_Refit_&lst2mnths.\Segment&p.";
libname data1M_&p. "\\mpwsas64\Core_Credit_Risk_Models\V5\application scorecard\v6 monitoring\V570 TU Refit\TU_Refit_&lstmnth.\Segment&p.";
libname data 	 " \\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets";
libname lindolib "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\Refit_scores\month1";



data Calibration_Scored;
	set lindolib.scored_&p._&&lstmnth;
run;
data ins_code ;
			set  disbursedbase4reblt_&lstmnth;
			Where seg = &p and
				 month1 >= &mnth11ago and month1 < &mnth6ago;
			keep uniqueid institutioncode Principaldebt;
		run;

		proc sort data= ins_code;
			by uniqueid;
		run;

		proc sort data= Calibration_Scored;
			by uniqueid;
		run;

		data Calibration_Scored;
			merge Calibration_Scored ins_code;
			by uniqueid;
		run;
		proc sql;
			create table Calibration_Scored as
				select *, ((a.Principaldebt/sum(a.Principaldebt)) * count(a.uniqueid)) as weight
				from Calibration_Scored A ;
		quit;


		/***************Segment Calibration*********************/
		/*Start of calibration - Creation of a and c for segment split*/
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

		%Loopthroughcal(Dset=Calibration_Scored,seg=&p,name = Segment,prob=Final_Score,Segmentation=seg,weight=weight);

		proc sql;
		      create table Calibration_seg
		      as select a.weight ,1/(1+(Final_Score/(1-Final_Score))**(-1*(c.a))*exp(c.c)) as Prob2, a.*
		      from  Calibration_Scored a
		      left join parameters_Segment c
		      on a.seg=c.Segment;
		quit;

		
		proc sql;
		      create table bank as
		      select distinct institutioncode
		      from Calibration_seg;
		quit;

		data Calibration_seg;
		      set Calibration_seg;
		      if (compress(INSTITUTIONCODE)) in ('','BNKGRN','BNKINV') then INSTITUTIONCODE ='BNKOTH';
		run;

		proc freq data = Calibration_seg;
		      tables INSTITUTIONCODE;
		run;
		%Loopthroughcal(Dset=Calibration_seg,seg='BNKABL' ,prob=Prob2,name = INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
		%Loopthroughcal(Dset=Calibration_seg,seg='BNKABS' ,prob=Prob2,name = INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
		%Loopthroughcal(Dset=Calibration_seg,seg='BNKCAP' ,prob=Prob2,name = INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
		%Loopthroughcal(Dset=Calibration_seg,seg='BNKFNB' ,prob=Prob2,name = INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
		%Loopthroughcal(Dset=Calibration_seg,seg='BNKNED' ,prob=Prob2,name = INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
		%Loopthroughcal(Dset=Calibration_seg,seg='BNKSTD' ,prob=Prob2,name = INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
		%Loopthroughcal(Dset=Calibration_seg,seg='BNKOTH' ,prob=Prob2,name = INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);

		proc sql;
		      create table Calibration_bank
		      as select a.*,
		      1/(1+(Prob2/(1-Prob2))**(-1*(c.a))*exp(c.c)) as Prob3
		      from  Calibration_seg a
		      left join PARAMETERS_INSTITUTIONCODE c
		      on a.Institutioncode=c.Institutioncode;
		quit;

		data lindolib.c_scored_&p._&&lstmnth;
			set Calibration_bank;
		run;





%let Target = target; 
%let Final_Score = Prob3;
%Calc_Gini (Final_Score, lindolib.c_scored_&p._&&lstmnth, target, work.GINITABLE) ;

data lindolib.c_Gini_Seg&p._&&lstmnth;
set  GiniTable;
Applied_Model = "&lst3mnths";
Segment = &p.;
where Gini ne .;
run;
	proc datasets library=work nolist;
	   delete PARAMETERS_INSTITUTIONCODE;
	quit;
	%end;


proc datasets library=work nolist;
	   delete parameters_Segmentation;
	quit;  

data lindolib.c_build_scorecomb_Refit;
	set lindolib.c_scored_1_&lstmnth lindolib.c_scored_2_&lstmnth lindolib.c_scored_3_&lstmnth lindolib.c_scored_4_&lstmnth lindolib.c_scored_5_&lstmnth;
run;


%Calc_Gini (Final_Score, lindolib.c_build_scorecomb_Refit, target, work.GINITABLE) ;

Data lindolib.c_GINITABLE_Overall_Refit (keep=Gini segment);
	set GINITABLE;
	Applied_Model = "&lst3mnths";
	segment = 0;
	Refit_TU = Gini;
run;

		Data lindolib.c_Ginis_&lstmnth;
			set lindolib.c_GINITABLE_Overall_Refit lindolib.c_Gini_Seg1_&lstmnth lindolib.c_Gini_Seg2_&lstmnth lindolib.c_Gini_Seg3_&lstmnth lindolib.c_Gini_Seg4_&lstmnth lindolib.c_Gini_Seg5_&lstmnth;
			Score_type = "Refit_TU";
			Month = &lstmnth;
			Applied_Model = "&lst3mnths";
		run;
%mend;

%MonitorRebuilt1(5);


%macro MonitorRebuilt2MthsModel1(segnum);

%do p=1 %to &segnum; 
%let path = \\mpwsas64\Core_Credit_Risk_Models\V5\application scorecard\v6 monitoring\V570 TU Refit;

libname sphelib2 '\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\Refit_scores\month2';

/*Get list of vars from 2 months ago*/
/*Sphe Notes: Use this in data step*/
proc sql; 
	select reverse(substr(reverse(compress(Parameter)),3)) into : segment_&p._list separated by ' ' 
	from data2M_&p..parameter_estimate
	where upcase(Parameter) ne 'INTERCEPT'; 
quit;

/*Sphe Notes: Use as is in proc sql step*/
proc sql; 
	select reverse(substr(reverse(compress(Parameter)),3)) into : segment_&p._list2 separated by ',' 
	from data2M_&p..parameter_estimate
	where upcase(Parameter) ne 'INTERCEPT'; 
quit;

%put &&segment_&p._list.;
%put &&segment_&p._list2.;

/*Apply Scoring Model*/
%macro applyscore(t);
	%do j = 1 %to %sysfunc(countw(&&segment_&t._list));
	    %let var = %scan(&&segment_&t._list, &j);
	    %include "&path.\TU_refit_&lst2mnths.\Segment&t.\&var._if_statement_.sas";
	    %include "&path.\TU_refit_&lst2mnths.\Segment&t.\&var._WOE_if_statement_.sas"; 
	%end;
	*****************************************;
	** SAS Scoring Code for PROC Hplogistic;
	*****************************************;
	%include "&path.\TU_refit_&lst2mnths.\Segment&t.\creditlogisticcode2.sas";
%mend;

*****************************************;
** 			Score 3 Months Ago		*****;
*****************************************;
libname iv "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\TU Performance Monitoring\V570 TU Refit\Seg&p.\iv";

proc sql;
	select tranwrd(_NAME_,"_W","") into : varlist separated by " " 
	from iv.wght_trans 
	where upcase(_NAME_) ne "INTERCEPT"
;
quit;

%put &varlist.;

%macro m;
	data build1; 
		set disbursedbase4reblt_&lstmnth;
		%do k = 1 %to %sysfunc(countw(&varlist.));
			%let var = %scan(&varlist., &k);
			&var._sb = SUBSTR(&var._b, 1, 2);
		%end;
	run;
%mend;

%m;
/*Selecting vars from dataset to score*/
proc sql;
	create table segment&p._data as 
	select uniqueid, &&segment_&p._list2., target, month, seg 
	from build1
	where seg = &p.;
quit;

/*Apply scoring model to dataset generated above*/
data segment_&p._&&lstmnth;
	set segment&p._data (where=(seg=&p.));
	%applyscore(&p.); 
	Final_score = P_target1;
	Score = 1000-(Final_score*1000);    
	drop _TEMP;
run;

data final_model_data_&lstmnth;
	set segment_&p._&&lstmnth.;
run;

data sphelib2.scored_&p._&&lstmnth;
    set final_model_data_&lstmnth;  
	probability = Final_score;
run;

data Calibration_Scored;
	set sphelib2.scored_&p._&&lstmnth;
run;
data ins_code ;
			set  disbursedbase4reblt_&lstmnth;
			Where seg = &p and
				 month1 >= &mnth11ago and month1 < &mnth6ago;
			keep uniqueid institutioncode Principaldebt;
		run;

		proc sort data= ins_code;
			by uniqueid;
		run;

		proc sort data= Calibration_Scored;
			by uniqueid;
		run;

		data Calibration_Scored;
			merge Calibration_Scored ins_code;
			by uniqueid;
		run;
		proc sql;
			create table Calibration_Scored as
				select *, ((a.Principaldebt/sum(a.Principaldebt)) * count(a.uniqueid)) as weight
				from Calibration_Scored A ;
		quit;


		/***************Segment Calibration*********************/
		/*Start of calibration - Creation of a and c for segment split*/
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

		%Loopthroughcal(Dset=Calibration_Scored,seg=&p,name = Segment,prob=Final_Score,Segmentation=seg,weight=weight);

		proc sql;
		      create table Calibration_seg
		      as select a.weight ,1/(1+(Final_Score/(1-Final_Score))**(-1*(c.a))*exp(c.c)) as Prob2, a.*
		      from  Calibration_Scored a
		      left join parameters_Segment c
		      on a.seg=c.Segment;
		quit;

		
		proc sql;
		      create table bank as
		      select distinct institutioncode
		      from Calibration_seg;
		quit;

		data Calibration_seg;
		      set Calibration_seg;
		      if (compress(INSTITUTIONCODE)) in ('','BNKGRN','BNKINV') then INSTITUTIONCODE ='BNKOTH';
		run;

		proc freq data = Calibration_seg;
		      tables INSTITUTIONCODE;
		run;
		%Loopthroughcal(Dset=Calibration_seg,seg='BNKABL' ,prob=Prob2,name = INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
		%Loopthroughcal(Dset=Calibration_seg,seg='BNKABS' ,prob=Prob2,name = INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
		%Loopthroughcal(Dset=Calibration_seg,seg='BNKCAP' ,prob=Prob2,name = INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
		%Loopthroughcal(Dset=Calibration_seg,seg='BNKFNB' ,prob=Prob2,name = INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
		%Loopthroughcal(Dset=Calibration_seg,seg='BNKNED' ,prob=Prob2,name = INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
		%Loopthroughcal(Dset=Calibration_seg,seg='BNKSTD' ,prob=Prob2,name = INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
		%Loopthroughcal(Dset=Calibration_seg,seg='BNKOTH' ,prob=Prob2,name = INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);

		proc sql;
		      create table Calibration_bank
		      as select a.*,
		      1/(1+(Prob2/(1-Prob2))**(-1*(c.a))*exp(c.c)) as Prob3
		      from  Calibration_seg a
		      left join PARAMETERS_INSTITUTIONCODE c
		      on a.Institutioncode=c.Institutioncode;
		quit;

		data sphelib2.c_scored_&p._&&lstmnth;
			set Calibration_bank;
		run;


%let Target = target; 
%let Final_Score = Prob3;
%Calc_Gini (Final_Score, sphelib2.c_scored_&p._&&lstmnth, target, work.GINITABLE) ;


data sphelib2.c_Gini_Seg&p._&&lstmnth;
	set  GiniTable;
	Applied_Model = "&lst2mnths";
	Segment = &p.;
	where Gini ne .;
run;

	proc datasets library=work nolist;
	   delete PARAMETERS_INSTITUTIONCODE;
	quit;
	%end;


proc datasets library=work nolist;
	   delete parameters_Segmentation;
	quit;  


data sphelib2.c_build_scorecomb_Rebuild;
	set sphelib2.c_scored_1_&lstmnth sphelib2.c_scored_2_&lstmnth sphelib2.c_scored_3_&lstmnth sphelib2.c_scored_4_&lstmnth sphelib2.c_scored_5_&lstmnth;
run;


%Calc_Gini (Final_Score, sphelib2.c_build_scorecomb_Rebuild, target, work.GINITABLE) ;

Data sphelib2.c_GINITABLE_Overall_Rebuild (keep=Gini segment);
	set GINITABLE;
	Applied_Model = "&lst2mnths";
	segment = 0;
	Refit_TU = Gini;
run;

Data sphelib2.c_Ginis_&lstmnth;
	set sphelib2.c_GINITABLE_Overall_Rebuild sphelib2.c_Gini_Seg1_&lstmnth sphelib2.c_Gini_Seg2_&lstmnth sphelib2.c_Gini_Seg3_&lstmnth sphelib2.c_Gini_Seg4_&lstmnth sphelib2.c_Gini_Seg5_&lstmnth;
	Score_type = "Refit_TU";
	Month = &lstmnth;
	Applied_Model = "&lst2mnths";
run;


%mend;
%MonitorRebuilt2MthsModel1(5);

%macro MonitorRebuilt1MthModel1(segnum);

%do p=1 %to &segnum; 
%let path = \\mpwsas64\Core_Credit_Risk_Models\V5\application scorecard\v6 monitoring\V570 TU Refit;

libname sphelib3 '\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\Refit_scores\month3';

/*Get list of vars from 1 month ago*/
/*Sphe Notes: Use this in data step*/
proc sql; 
	select reverse(substr(reverse(compress(Parameter)),3)) into : segment_&p._list separated by ' ' 
	from data3M_&p..parameter_estimate
	where upcase(Parameter) ne 'INTERCEPT'; 
quit;

/*Sphe Notes: Use as is in proc sql step*/
proc sql; 
	select reverse(substr(reverse(compress(Parameter)),3)) into : segment_&p._list2 separated by ',' 
	from data3M_&p..parameter_estimate
	where upcase(Parameter) ne 'INTERCEPT'; 
quit;

%put &&segment_&p._list.;
%put &&segment_&p._list2.;

/*Apply Scoring Model*/
%macro applyscore(t);
	%do j = 1 %to %sysfunc(countw(&&segment_&t._list));
	    %let var = %scan(&&segment_&t._list, &j);
	    %include "&path.\TU_Refit_&lst3mnths.\Segment&t.\&var._if_statement_.sas";
	    %include "&path.\TU_Refit_&lst3mnths.\Segment&t.\&var._WOE_if_statement_.sas"; 
	%end;
	*****************************************;
	** SAS Scoring Code for PROC Hplogistic;
	*****************************************;
	%include "&path.\TU_Refit_&lst3mnths.\Segment&t.\creditlogisticcode2.sas";
%mend;

*****************************************;
** 			Score 3 Months Ago		*****;
*****************************************;
libname iv "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\TU Performance Monitoring\V570 TU Refit\Seg&p.\iv";

proc sql;
	select tranwrd(_NAME_,"_W","") into : varlist separated by " " 
	from iv.wght_trans 
	where upcase(_NAME_) ne "INTERCEPT"
;
quit;

%put &varlist.;

%macro m;
	data build1; 
		set disbursedbase4reblt_&lstmnth;
		%do k = 1 %to %sysfunc(countw(&varlist.));
			%let var = %scan(&varlist., &k);
			&var._sb = SUBSTR(&var._b, 1, 2);
		%end;
	run;
%mend;

%m;
/*Selecting vars from dataset to score*/
proc sql;
	create table segment&p._data as 
	select uniqueid, &&segment_&p._list2., target, month, seg 
	from build1
	where seg = &p.;
quit;

/*Apply scoring model to dataset generated above*/
data segment_&p._&&lst2mnths;
	set segment&p._data (where=(seg=&p.));
	%applyscore(&p.); 
	Final_score = P_target1;
	Score = 1000-(Final_score*1000);    
	drop _TEMP;
run;

data final_model_data_&lst2mnths;
	set segment_&p._&&lst2mnths.;
run;

data sphelib3.scored_&p._&&lst2mnths;
    set final_model_data_&lst2mnths;  
	probability = Final_score;
run;

options spool mprint mlogic;

data Calibration_Scored;
	set sphelib3.scored_&p._&&lst2mnths;
run;


data ins_code ;
			set  disbursedbase4reblt_&lstmnth;
			Where seg = &p and
				 month1 >= &mnth11ago and month1 < &mnth6ago;
			keep uniqueid institutioncode Principaldebt;
		run;

		proc sort data= ins_code;
			by uniqueid;
		run;

		proc sort data= Calibration_Scored;
			by uniqueid;
		run;

		data Calibration_Scored;
			merge Calibration_Scored ins_code;
			by uniqueid;
		run;
		proc sql;
			create table Calibration_Scored as
				select *, ((a.Principaldebt/sum(a.Principaldebt)) * count(a.uniqueid)) as weight
				from Calibration_Scored A ;
		quit;


		/***************Segment Calibration*********************/
		/*Start of calibration - Creation of a and c for segment split*/
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

		%Loopthroughcal(Dset=Calibration_Scored,seg=&p,name = Segment,prob=Final_Score,Segmentation=seg,weight=weight);

		proc sql;
		      create table Calibration_seg
		      as select a.weight ,1/(1+(Final_Score/(1-Final_Score))**(-1*(c.a))*exp(c.c)) as Prob2, a.*
		      from  Calibration_Scored a
		      left join parameters_Segment c
		      on a.seg=c.Segment;
		quit;

		
		proc sql;
		      create table bank as
		      select distinct institutioncode
		      from Calibration_seg;
		quit;

		data Calibration_seg;
		      set Calibration_seg;
		      if (compress(INSTITUTIONCODE)) in ('','BNKGRN','BNKINV') then INSTITUTIONCODE ='BNKOTH';
		run;

		proc freq data = Calibration_seg;
		      tables INSTITUTIONCODE;
		run;
		%Loopthroughcal(Dset=Calibration_seg,seg='BNKABL' ,prob=Prob2,name = INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
		%Loopthroughcal(Dset=Calibration_seg,seg='BNKABS' ,prob=Prob2,name = INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
		%Loopthroughcal(Dset=Calibration_seg,seg='BNKCAP' ,prob=Prob2,name = INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
		%Loopthroughcal(Dset=Calibration_seg,seg='BNKFNB' ,prob=Prob2,name = INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
		%Loopthroughcal(Dset=Calibration_seg,seg='BNKNED' ,prob=Prob2,name = INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
		%Loopthroughcal(Dset=Calibration_seg,seg='BNKSTD' ,prob=Prob2,name = INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
		%Loopthroughcal(Dset=Calibration_seg,seg='BNKOTH' ,prob=Prob2,name = INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);

		proc sql;
		      create table Calibration_bank
		      as select a.*,
		      1/(1+(Prob2/(1-Prob2))**(-1*(c.a))*exp(c.c)) as Prob3
		      from  Calibration_seg a
		      left join PARAMETERS_INSTITUTIONCODE c
		      on a.Institutioncode=c.Institutioncode;
		quit;

		data sphelib3.c_scored_&p._&&lst2mnths;;
			set Calibration_bank;
		run;



%let Target = target; 
%let Final_Score = Prob3;
%Calc_Gini (Final_Score, sphelib3.c_scored_&p._&&lst2mnths, target, work.GINITABLE) ;


data sphelib3.c_Gini_Seg&p._&&lst2mnths;
	set  GiniTable;
	Applied_Model = "&lst3mnths";
	Segment = &p.;
	where Gini ne .;
run;

	proc datasets library=work nolist;
	   delete PARAMETERS_INSTITUTIONCODE;
	quit;
	%end;





data sphelib3.c_build_scorecomb_Rebuild;
	set sphelib3.c_scored_1_&lst2mnths sphelib3.c_scored_2_&lst2mnths sphelib3.c_scored_3_&lst2mnths sphelib3.c_scored_4_&lst2mnths sphelib3.c_scored_5_&lst2mnths;
run;


%Calc_Gini (Final_Score, sphelib3.c_build_scorecomb_Rebuild, target, work.GINITABLE) ;

Data sphelib3.c_GINITABLE_Overall_Rebuild (keep=Gini segment);
	set GINITABLE;
	Applied_Model = "&lst3mnths";
	segment = 0;
	Refit_TU = Gini;
run;

Data sphelib3.c_Ginis_&lst2mnths;
	set sphelib3.c_GINITABLE_Overall_Rebuild sphelib3.c_Gini_Seg1_&lst2mnths sphelib3.c_Gini_Seg2_&lst2mnths sphelib3.c_Gini_Seg3_&lst2mnths sphelib3.c_Gini_Seg4_&lst2mnths sphelib3.c_Gini_Seg5_&lst2mnths;
	Score_type = "Refit_TU";
	Month = &lst2mnths;
	Applied_Model = "&lst3mnths";
run;


%mend;
%MonitorRebuilt1MthModel1(5);

filename macros2 'H:\Process_Automation\macros';
options sasautos = (sasautos  macros2);