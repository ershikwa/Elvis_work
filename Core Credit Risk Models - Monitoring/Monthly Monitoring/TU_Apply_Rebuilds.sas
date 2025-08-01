/*%include "\\neptune\sasa$\SAS_Automation\SAS_Autoexec\autoexec2.sas";*/
/*%include "\\neptune\credit$\AA_GROUP CREDIT\Scoring\SAS Macros\Upload_APS.sas";*/
libname paths "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\Disbursals";  
libname tu1 '\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets\V580';
libname data  "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets"; 
%let lstmnth = %sysfunc(intnx(month,%sysfunc(today()),-1,e), yymmn6.);
%let lst2mnths = %sysfunc(intnx(month,%sysfunc(today()),-2,e), yymmn6.);
%let lst3mnths = %sysfunc(intnx(month,%sysfunc(today()),-3,e), yymmn6.);
%let mnth6ago = %sysfunc(intnx(month,%sysfunc(today()),-6,e), yymmn6.);
%let mnth11ago = %sysfunc(intnx(month,%sysfunc(today()),-12, b), yymmn6.);
%let odbc = MPWAPS;

%put &lstmnth.;
%put &lst2mnths.;
%put &lst3mnths.;
/*proc sql stimer;*/
/*	connect to ODBC (dsn=&odbc);*/
/*	create table DISBURSEDBASE4REBLT_&lstmnth as */
/*	select * from connection to odbc ( */
/*		select **/
/*				*/
/*		from 	DEV_DataDistillery_General.dbo.DISBURSEDBASE4REBLT_&lstmnth*/
/*		*/
/*	) ;*/
/*	disconnect from odbc ;*/
/*quit;*/
data DISBURSEDBASE4REBLT_&lstmnth;
set tu1.disbursedbase4reblt_&lstmnth;
run;




data DISBURSEDBASE4REBLT_&lstmnth;
	set DISBURSEDBASE4REBLT_&lstmnth;
	format month1 BEST12. ;
	month1 = input(month,BEST12.);
	seg = tu_seg;
run;


%macro MonitorRebuilt(segnum);

	%do p=1 %to &segnum; 

	

	%let path = \\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models;

	libname data3M_&p. "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models\TU_Rebuild_&lst3mnths.\Segment&p.";
	libname data2M_&p. "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models\TU_Rebuild_&lst2mnths.\Segment&p.";
	libname data1M_&p. "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models\TU_Rebuild_&lstmnth.\Segment&p.";
	libname data 	 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets";
	libname lindolib "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\scores\month1";

	/*Get list of vars from 3 months ago*/
	
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
		    %include "&path.\TU_Rebuild_&lst3mnths.\Segment&t.\&var._if_statement_.sas";
		    %include "&path.\TU_Rebuild_&lst3mnths.\Segment&t.\&var._WOE_if_statement_.sas"; 
		%end;
		*****************************************;
		** SAS Scoring Code for PROC Hplogistic;
		*****************************************;
		%include "&path.\TU_Rebuild_&lst3mnths.\Segment&t.\creditlogisticcode2.sas";
	%mend;

	*****************************************;
	** 			Score 3 Months Ago		*****;
	*****************************************;

	/*Selecting vars from dataset to score*/
	proc sql;
		create table segment&p._data as 
		select Capri_uniqueid, &&segment_&p._list2., target, month, seg 
		from disbursedbase4reblt_&lstmnth
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







	data lindolib.scored_&p._&&lstmnth;
	    set final_model_data_&lstmnth;  
		probability = Final_score;
	run;
	/* segment scoring and gini */

		%let Target = target; 
		%let Final_Score = Final_Score;
		%Calc_Gini (Final_Score,  lindolib.scored_&p._&&lstmnth, target, work.GINITABLE) ;


		data lindolib.Gini_Seg&p._&&lstmnth;
		set  GiniTable;
		Applied_Model = "&lst3mnths";
		Segment = &p.;
		where Gini ne .;
		run;


	%end;



	data lindolib.build_scorecomb_Rebuild;
		set lindolib.scored_1_&lstmnth lindolib.scored_2_&lstmnth lindolib.scored_3_&lstmnth lindolib.scored_4_&lstmnth lindolib.scored_5_&lstmnth;
	run;

	/***************************************************************************************************
									Calculating the overall gini 
	***************************************************************************************************/

	%let Target = target; 
	%let Final_Score = Final_Score;
	%Calc_Gini (Final_Score, lindolib.build_scorecomb_Rebuild, target, work.GINITABLE) ;

	Data lindolib.GINITABLE_Overall_Rebuild (keep=Gini segment);
		set GINITABLE;
		Applied_Model = "&lst3mnths";
		segment = 0;
		Rebuild_TU = Gini;
	run;

	Data lindolib.Ginis_&lstmnth;
		set lindolib.GINITABLE_Overall_Rebuild lindolib.Gini_Seg1_&lstmnth lindolib.Gini_Seg2_&lstmnth lindolib.Gini_Seg3_&lstmnth lindolib.Gini_Seg4_&lstmnth lindolib.Gini_Seg5_&lstmnth;
		Score_type = "Rebuild_TU";
		Month = &lstmnth;
		Applied_Model = "&lst3mnths";
	run;




%mend;


%MonitorRebuilt(5);



%macro MonitorRebuilt2MthsModel(segnum);

	%do p=1 %to &segnum; 
	%let path = \\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models;
	libname data3M_&p. "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models\TU_Rebuild_&lst3mnths.\Segment&p.";
	libname data2M_&p. "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models\TU_Rebuild_&lst2mnths.\Segment&p.";
	libname data1M_&p. "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models\TU_Rebuild_&lstmnth.\Segment&p.";
	libname data 	 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets";
	libname sphelib2 '\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\scores\month2';

	/*Get list of vars from 2 months ago*/
	
	proc sql; 
		select reverse(substr(reverse(compress(Parameter)),3)) into : segment_&p._list separated by ' ' 
		from data2M_&p..parameter_estimate
		where upcase(Parameter) ne 'INTERCEPT'; 
	quit;

	
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
		    %include "&path.\TU_Rebuild_&lst2mnths.\Segment&t.\&var._if_statement_.sas";
		    %include "&path.\TU_Rebuild_&lst2mnths.\Segment&t.\&var._WOE_if_statement_.sas"; 
		%end;
		*****************************************;
		** SAS Scoring Code for PROC Hplogistic;
		*****************************************;
		%include "&path.\TU_Rebuild_&lst2mnths.\Segment&t.\creditlogisticcode2.sas";
	%mend;

	*****************************************;
	** 			Score 3 Months Ago		*****;
	*****************************************;

	/*Selecting vars from dataset to score*/
	proc sql;
		create table segment&p._data as 
		select Capri_uniqueid, &&segment_&p._list2., target, month, seg 
		from disbursedbase4reblt_&lstmnth
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

	/* segment scoring and gini */

	options spool mprint mlogic;
	%let Target = target; 
	%let Final_Score = Prob3;
	%Calc_Gini (Final_Score, sphelib2.scored_&p._&&lstmnth, target, work.GINITABLE) ;


	data sphelib2.Gini_Seg&p._&&lstmnth;
		set  GiniTable;
		Applied_Model = "&lst2mnths";
		Segment = &p.;
		where Gini ne .;
	run;


	%end;

	/***************************************************************************************************
									Calculating the overall gini 
	***************************************************************************************************/

	data sphelib2.build_scorecomb_Rebuild;
		set sphelib2.scored_1_&lstmnth sphelib2.scored_2_&lstmnth sphelib2.scored_3_&lstmnth sphelib2.scored_4_&lstmnth sphelib2.scored_5_&lstmnth;
	run;

	%let Target = target; 
	%let Final_Score = Final_Score;
	%Calc_Gini (Final_Score, sphelib2.build_scorecomb_Rebuild, target, work.GINITABLE) ;

	Data sphelib2.GINITABLE_Overall_Rebuild (keep=Gini segment);
		set GINITABLE;
		Applied_Model = "&lst2mnths";
		segment = 0;
		Rebuild_TU = Gini;
	run;

	Data sphelib2.Ginis_&lstmnth;
		set sphelib2.GINITABLE_Overall_Rebuild sphelib2.Gini_Seg1_&lstmnth sphelib2.Gini_Seg2_&lstmnth sphelib2.Gini_Seg3_&lstmnth sphelib2.Gini_Seg4_&lstmnth sphelib2.Gini_Seg5_&lstmnth;
		Score_type = "Rebuild_TU";
		Month = &lstmnth;
		Applied_Model = "&lst2mnths";
	run;


%mend;
%MonitorRebuilt2MthsModel(5);




%macro MonitorRebuilt1MthModel(segnum);

	%do p=1 %to &segnum; 
	%let path = \\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models;
	libname data3M_&p. "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models\TU_Rebuild_&lst3mnths.\Segment&p.";
	libname data2M_&p. "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models\TU_Rebuild_&lst2mnths.\Segment&p.";
	libname data1M_&p. "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models\TU_Rebuild_&lstmnth.\Segment&p.";
	libname data 	 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets";
	libname sphelib3 '\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\scores\month3';

	/*Get list of vars from 1 month ago*/
	
	proc sql; 
		select reverse(substr(reverse(compress(Parameter)),3)) into : segment_&p._list separated by ' ' 
		from data3M_&p..parameter_estimate
		where upcase(Parameter) ne 'INTERCEPT'; 
	quit;

	
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
		    %include "&path.\TU_Rebuild_&lst3mnths.\Segment&t.\&var._if_statement_.sas";
		    %include "&path.\TU_Rebuild_&lst3mnths.\Segment&t.\&var._WOE_if_statement_.sas"; 
		%end;
		*****************************************;
		** SAS Scoring Code for PROC Hplogistic;
		*****************************************;
		%include "&path.\TU_Rebuild_&lst3mnths.\Segment&t.\creditlogisticcode2.sas";
	%mend;

	*****************************************;
	** 			Score 3 Months Ago		*****;
	*****************************************;

	/*Selecting vars from dataset to score*/
	proc sql;
		create table segment&p._data as 
		select Capri_uniqueid, &&segment_&p._list2., target, month, Tu_seg 
		from DISBURSEDBASE4REBLT_&lstmnth
		where Tu_seg = &p.;
	quit;

	/*Apply scoring model to dataset generated above*/
	data segment_&p._&&lst2mnths;
		set segment&p._data (where=(tu_seg=&p.));
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
		seg =Tu_seg;
		probability = Final_score;
	run;



	/* segment scoring and gini */
	 

	%let Target = target; 
	%let Final_Score = Prob3;
	%Calc_Gini (Final_Score, sphelib3.scored_&p._&&lst2mnths, target, work.GINITABLE) ;


	data sphelib3.Gini_Seg&p._&&lst2mnths;
		set  GiniTable;
		Applied_Model = "&lst3mnths";
		Segment = &p.;
		where Gini ne .;
	run;



	%end;

	/***************************************************************************************************
									Calculating the overall gini 
	***************************************************************************************************/
	data sphelib3.build_scorecomb_Rebuild;
		set sphelib3.scored_1_&lst2mnths sphelib3.scored_2_&lst2mnths sphelib3.scored_3_&lst2mnths sphelib3.scored_4_&lst2mnths sphelib3.scored_5_&lst2mnths;
	run;

	%let Target = target; 
	%let Final_Score = Final_Score;
	%Calc_Gini (Final_Score, sphelib3.build_scorecomb_Rebuild, target, work.GINITABLE) ;

	Data sphelib3.GINITABLE_Overall_Rebuild (keep=Gini segment);
		set GINITABLE;
		Applied_Model = "&lst3mnths";
		segment = 0;
		Rebuild_TU = Gini;
	run;

	Data sphelib3.Ginis_&lst2mnths;
		set sphelib3.GINITABLE_Overall_Rebuild sphelib3.Gini_Seg1_&lst2mnths sphelib3.Gini_Seg2_&lst2mnths sphelib3.Gini_Seg3_&lst2mnths sphelib3.Gini_Seg4_&lst2mnths sphelib3.Gini_Seg5_&lst2mnths;
		Score_type = "Rebuild_TU";
		Month = &lst2mnths;
		Applied_Model = "&lst3mnths";
	run;


%mend;
%MonitorRebuilt1MthModel(5);
/*proc sql stimer;*/
/*	connect to ODBC (dsn=&odbc);*/
/*	create table DISBURSEDBASE4REBLT_&lstmnth as */
/*	select * from connection to odbc ( */
/*		select **/
/*				*/
/*		from 	DEV_DataDistillery_General.dbo.DISBURSEDBASE4REBLT_&lstmnth*/
/*		*/
/*	) ;*/
/*	disconnect from odbc ;*/
/*quit;*/
data disbursedbase4reblt_&lstmnth;
set tu1.disbursedbase4reblt_&lstmnth;
run;



data DISBURSEDBASE4REBLT_&lstmnth;
	set DISBURSEDBASE4REBLT_&lstmnth;
	format month1 BEST12. ;
	month1 = input(month,BEST12.);
	seg = tu_seg;
run;
%macro MonitorRebuilt(segnum);

	%do p=1 %to &segnum; 

	/*%let p = 1;*/

	%let path = \\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models;

	libname data3M_&p. "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models\TU_Rebuild_&lst3mnths.\Segment&p.";
	libname data2M_&p. "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models\TU_Rebuild_&lst2mnths.\Segment&p.";
	libname data1M_&p. "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models\TU_Rebuild_&lstmnth.\Segment&p.";
	libname data 	 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets";
	libname lindolib "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\scores\month1";

	/********************************Calibrating Model  from 3 months ago**********************************/
	/* scored dataset*/
	data Calibration_Scored;
		set lindolib.scored_&p._&&lstmnth;
	run;

	/* adding the  Capri_uniqueid, institutioncode and Principaldebt to scored dataset*/
	data ins_code ;
		set  disbursedbase4reblt_&lstmnth;
		Where seg = &p and
			 month1 >= &mnth11ago and month1 < &mnth6ago;
		keep Capri_uniqueid institutioncode Principaldebt;
	run;

		proc sort data= ins_code;
			by Capri_uniqueid;
		run;

		proc sort data= Calibration_Scored;
			by Capri_uniqueid;
		run;

		data Calibration_Scored;
			merge Calibration_Scored ins_code;
			by Capri_uniqueid;
		run;
		proc sql;
			create table Calibration_Scored as
				select *, ((a.Principaldebt/sum(a.Principaldebt)) * count(a.Capri_uniqueid)) as weight
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

		/*Calibration on segment*/
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

		/*Calibration on Bank*/
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

		data lindolib.calibrated_scored_&p._&&lstmnth;
			set Calibration_bank;
		run;
		/* segment scoring and gini after calibration*/

		%let Target = target; 
		%let Final_Score = Prob3;
		%Calc_Gini (Final_Score,  lindolib.calibrated_scored_&p._&&lstmnth, target, work.GINITABLE) ;


		data lindolib.Calibrated_Gini_Seg&p._&&lstmnth;
		set  GiniTable;
		Applied_Model = "&lst3mnths";
		Segment = &p.;
		where Gini ne .;
		run;

		/*Clearing PARAMETERS_INSTITUTIONCODE table for next run*/
		proc datasets library=work nolist;
		   delete PARAMETERS_INSTITUTIONCODE;
		quit;
	%end;

	/*Clearing parameters_Segmentation table for next run*/
	proc datasets library=work nolist;
		   delete parameters_Segmentation;
		quit;  

	/***************************************************************************************************
									Calculating the overall gini 
	***************************************************************************************************/

	/*Collectingg the results  */
	data lindolib.Calibrated_build_scorecomb;
		set lindolib.calibrated_scored_1_&lstmnth lindolib.calibrated_scored_2_&lstmnth lindolib.calibrated_scored_3_&lstmnth lindolib.calibrated_scored_4_&lstmnth lindolib.calibrated_scored_5_&lstmnth;
	run;

	%let Target = target; 
	%let Final_Score = Prob3;
	%Calc_Gini (Final_Score, lindolib.Calibrated_build_scorecomb, target, work.GINITABLE) ;

	Data lindolib.Calibrated_Overall_Rebuild (keep=Gini segment);
		set GINITABLE;
		Applied_Model = "&lst3mnths";
		segment = 0;
		Rebuild_TU = Gini;
	run;

	Data lindolib.Calibrated_Ginis_&lstmnth;
		set lindolib.Calibrated_Overall_Rebuild lindolib.Calibrated_Gini_Seg1_&lstmnth lindolib.Calibrated_Gini_Seg2_&lstmnth lindolib.Calibrated_Gini_Seg3_&lstmnth lindolib.Calibrated_Gini_Seg4_&lstmnth lindolib.Calibrated_Gini_Seg5_&lstmnth;
		Score_type = "Rebuild_TU";
		Month = &lstmnth;
		Applied_Model = "&lst3mnths";
	run;




%mend;


%MonitorRebuilt(5);



%macro MonitorRebuilt2MthsModel(segnum);

	%do p=1 %to &segnum; 
		%let path = \\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models;
		libname data3M_&p. "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models\TU_Rebuild_&lst3mnths.\Segment&p.";
		libname data2M_&p. "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models\TU_Rebuild_&lst2mnths.\Segment&p.";
		libname data1M_&p. "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models\TU_Rebuild_&lstmnth.\Segment&p.";
		libname data 	 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets";
		libname sphelib2 '\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\scores\month2';

		/********************************Calibrating Model  from 2 months ago**********************************/
		/* scored dataset*/
		data Calibration_Scored;
			set sphelib2.scored_&p._&&lstmnth;
		run;

		/* adding the  Capri_uniqueid, institutioncode and Principaldebt to scored dataset*/
		data ins_code ;
					set  disbursedbase4reblt_&lstmnth;
					Where seg = &p and
						 month1 >= &mnth11ago and month1 < &mnth6ago;
					keep Capri_uniqueid institutioncode Principaldebt;
				run;

		proc sort data= ins_code;
			by Capri_uniqueid;
		run;

		proc sort data= Calibration_Scored;
			by Capri_uniqueid;
		run;

		data Calibration_Scored;
			merge Calibration_Scored ins_code;
			by Capri_uniqueid;
		run;
		proc sql;
			create table Calibration_Scored as
				select *, ((a.Principaldebt/sum(a.Principaldebt)) * count(a.Capri_uniqueid)) as weight
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

		/*Calibration on segment*/
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
		
		/*Calibration on Bank*/
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

		data sphelib2.Calibrated_scored_&p._&&lstmnth;
			set Calibration_bank;
		run;

		options spool mprint mlogic;

		/* segment scoring and gini after calibration*/
		%let Target = target; 
		%let Final_Score = Prob3;
		%Calc_Gini (Final_Score, sphelib2.Calibrated_scored_&p._&&lstmnth, target, work.GINITABLE) ;


		data sphelib2.Calibrated_Gini_Seg&p._&&lstmnth;
			set  GiniTable;
			Applied_Model = "&lst2mnths";
			Segment = &p.;
			where Gini ne .;
		run;

		/*Clearing PARAMETERS_INSTITUTIONCODE table for next run*/
		proc datasets library=work nolist;
			   delete PARAMETERS_INSTITUTIONCODE;
			quit;

	%end;
	/*Clearing parameters_Segmentation table for next run*/
	proc datasets library=work nolist;
		delete parameters_Segmentation;
	quit;  
	/***************************************************************************************************
									Calculating the overall gini 
	***************************************************************************************************/

	/*Collectingg the results  */

	data sphelib2.Calibrated_build_scorecomb;
		set sphelib2.Calibrated_scored_1_&lstmnth sphelib2.scored_2_&lstmnth sphelib2.scored_3_&lstmnth sphelib2.scored_4_&lstmnth sphelib2.scored_5_&lstmnth;
	run;

	%let Target = target; 
	%let Final_Score = Prob3;
	%Calc_Gini (Final_Score, sphelib2.Calibrated_build_scorecomb, target, work.GINITABLE) ;

	Data sphelib2.Calibrated_Overall_Rebuild (keep=Gini segment);
		set GINITABLE;
		Applied_Model = "&lst2mnths";
		segment = 0;
		Rebuild_TU = Gini;
	run;

	Data sphelib2.Calibrated_Ginis_&lstmnth;
		set sphelib2.Calibrated_Overall_Rebuild sphelib2.Gini_Seg1_&lstmnth sphelib2.Gini_Seg2_&lstmnth sphelib2.Gini_Seg3_&lstmnth sphelib2.Gini_Seg4_&lstmnth sphelib2.Gini_Seg5_&lstmnth;
		Score_type = "Rebuild_TU";
		Month = &lstmnth;
		Applied_Model = "&lst2mnths";
	run;


%mend;
%MonitorRebuilt2MthsModel(5);




%macro MonitorRebuilt1MthModel(segnum);

	%do p=1 %to &segnum; 
	
		%let path = \\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models;
		libname data3M_&p. "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models\TU_Rebuild_&lst3mnths.\Segment&p.";
		libname data2M_&p. "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models\TU_Rebuild_&lst2mnths.\Segment&p.";
		libname data1M_&p. "\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\V570 Rebuild Models\TU_Rebuild_&lstmnth.\Segment&p.";
		libname data 	 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets";

		libname sphelib3 '\\mpwsas64\Core_Credit_Risk_Models\v5\application scorecard\v6 monitoring\scores\month3';



	/********************************Calibrating Model  from 2 months ago with last months data**********************************/

		/* scored dataset*/ 
		data Calibration_Scored;
			set sphelib3.scored_&p._&&lst2mnths;
				run;

		/* adding the  Capri_uniqueid, institutioncode and Principaldebt to scored dataset*/
		data ins_code ;
			set  disbursedbase4reblt_&lstmnth;
			Where seg = &p and
				 month1 >= &mnth11ago and month1 < &mnth6ago;
			keep Capri_uniqueid institutioncode Principaldebt;
		run;

		proc sort data= ins_code;
			by Capri_uniqueid;
		run;

		proc sort data= Calibration_Scored;
			by Capri_uniqueid;
		run;

		data Calibration_Scored;
			merge Calibration_Scored ins_code;
			by Capri_uniqueid;
		run;
		proc sql;
			create table Calibration_Scored as
				select *, ((a.Principaldebt/sum(a.Principaldebt)) * count(a.Capri_uniqueid)) as weight
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

		/*Calibration on segment*/
		%Loopthroughcal(Dset=Calibration_Scored,seg=&p,name = Segment,prob=Final_Score,Segmentation=seg,weight=weight);

		proc sql;
		      create table Calibration_seg
		      as select a.weight ,1/(1+(Final_Score/(1-Final_Score))**(-1*(c.a))*exp(c.c)) as Prob2, a.*
		      from  Calibration_Scored a
		      left join parameters_Segment c
		      on a.seg=c.Segment;
		quit;

		
	

		/*Calibration on Bank*/
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

		data sphelib3.scored_&p._&&lst2mnths;;
			set Calibration_bank;
		run;
		/* segment scoring and gini after calibration*/
		%let Target = target; 
		%let Final_Score = Prob3;
		%Calc_Gini (Final_Score, sphelib3.scored_&p._&&lst2mnths, target, work.GINITABLE) ;


		data sphelib3.Gini_Seg&p._&&lst2mnths;
			set  GiniTable;
			Applied_Model = "&lst3mnths";
			Segment = &p.;
			where Gini ne .;
		run;


		proc datasets library=work nolist;
			   delete PARAMETERS_INSTITUTIONCODE;
			quit; 


	%end;

	/***************************************************************************************************
									Calculating the overall gini 
	***************************************************************************************************/

	/*Collectingg the results  */
	data sphelib3.Calibrated_build_scorecomb;
		set sphelib3.scored_1_&lst2mnths sphelib3.scored_2_&lst2mnths sphelib3.scored_3_&lst2mnths sphelib3.scored_4_&lst2mnths sphelib3.scored_5_&lst2mnths;
	run;
	
	%let Target = target; 
	%let Final_Score = Prob3;
	%Calc_Gini (Final_Score, sphelib3.Calibrated_build_scorecomb, target, work.GINITABLE) ;

	Data sphelib3.Calibrated_Overall_Rebuild (keep=Gini segment);
		set GINITABLE;
		Applied_Model = "&lst3mnths";
		segment = 0;
		Rebuild_TU = Gini;
	run;

	Data sphelib3.Calibrated_Ginis_&lst2mnths;
		set sphelib3.Calibrated_Overall_Rebuild sphelib3.Gini_Seg1_&lst2mnths sphelib3.Gini_Seg2_&lst2mnths sphelib3.Gini_Seg3_&lst2mnths sphelib3.Gini_Seg4_&lst2mnths sphelib3.Gini_Seg5_&lst2mnths;
		Score_type = "Rebuild_TU";
		Month = &lst2mnths;
		Applied_Model = "&lst3mnths";
	run;


%mend;
%MonitorRebuilt1MthModel(5);

filename macros2 'H:\Process_Automation\macros';
options sasautos = (sasautos  macros2);