libname calib "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\Compuscan Models\calibration\calibration_new\Rebuild_calib";
libname data "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\V570";
libname data2 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\V570";
libname comp1 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\V570";
%let odbc = MPWAPS;
/*Renaming variables*/
%let  oldvarlist= COMPUSCANVAR1401 COMPUSCANVAR1424 COMPUSCANVAR175 COMPUSCANVAR187 COMPUSCANVAR188
                COMPUSCANVAR2123 COMPUSCANVAR2312 COMPUSCANVAR2528 COMPUSCANVAR2678 COMPUSCANVAR2696
                COMPUSCANVAR3275 COMPUSCANVAR3916 COMPUSCANVAR3935 COMPUSCANVAR5208 COMPUSCANVAR5486
                COMPUSCANVAR5489 COMPUSCANVAR5579 COMPUSCANVAR5826 COMPUSCANVAR6073 COMPUSCANVAR6130
                COMPUSCANVAR6132 COMPUSCANVAR6134 COMPUSCANVAR6285 COMPUSCANVAR6788 COMPUSCANVAR716
                COMPUSCANVAR733 COMPUSCANVAR7430 COMPUSCANVAR7431 COMPUSCANVAR7479 COMPUSCANVAR753
                COMPUSCANVAR7547 COMPUSCANVAR7549 COMPUSCANVAR7550 COMPUSCANVAR7683;

 
%let newvarlist = UNS_PercUtilisation UNS_ValCurBalMR60Days CSN_TimeOldestTrade ALL_TimeMREnq ALL_TimeOldestEnq
                  AUL_NumOpenTrades ALL_MaxDelq1YearLT24M OWN_Perc1pDelq2Years OTH_MaxDelqEver
                  OTH_MaxDelq1YearLT24M REV_MaxDelq180DaysGE24M UNS_TimeMR3pDelq UNS_MaxDelq180DaysLT12M
                  AIL_Num1pDelq90Days ALL_NumEverTrades ALL_NumTrades90Days OTH_AvgMonthsOnBook UNS_AvgMonthsOnBook
                  RCG_AvgMonthsOnBook UNN_AvgMonthsOnBook ALL_ValOrgBalLim90Days ALL_ValOrgBalLim1Year
                  OTH_ValOrgBalLim180Days UNS_ValCurBal1Year OWN_PercUtiliSatisfTrades
                  OWN_AvgPercUtilisationMR60Days ALL_NumPayments2Years ALL_PercPayments2Years
                  OTH_PercPayments2Years OTH_ValOrgBalLim REV_PercPayments180Days REV_PercPayments1Year
                  REV_NumPayments2Years OPL_PercPayments2Years;

 %macro rename1(oldvarlist, newvarlist);
      %let k=1;
      %let old = %scan(&oldvarlist, &k);
      %let new = %scan(&newvarlist, &k);
         %do %while(("&old" NE "") & ("&new" NE ""));
          rename &old = &new;
            %let k = %eval(&k + 1);
          %let old = %scan(&oldvarlist, &k);
          %let new = %scan(&newvarlist, &k);
      %end;
    %mend;


options mprint mlogic;

/*Create dates*/
data _null_;
     call symput("month", put(intnx("month", today(),-1,'end'),yymmn6.));
	 call symput("prevmonth", put(intnx("month", today(),-2,'end'),yymmn6.));
	 call symput("prevprevmonth", put(intnx("month", today(),-3,'end'),yymmn6.));
run;

%let folder = \\mpwsas64\Core_Credit_Risk_Models\V5; *First part of path to Gini tables;
%let folder2 = \\mpwsas64\Core_Credit_Risk_Models\V5; *First part of path to Gini tables;
%put &month;
%put &prevmonth;
%put &prevprevmonth;

/*proc sql stimer;*/
/*	connect to ODBC (dsn=&odbc);*/
/*	create table Disbursedbase_&month as */
/*	select * from connection to odbc ( */
/*		select **/
/*				*/
/*		from 	DEV_DataDistillery_General.dbo.Disbursedbase_&month*/
/*		*/
/*	) ;*/
/*	disconnect from odbc ;*/
/*quit;*/
data Disbursedbase_&month;
set comp1.Disbursedbase_&month;
seg=comp_seg;
run;

%macro scoreinputdata(inputdataset=,numberofsegment=,outputdataset=, creditlogisticcode=creditlogisticcode,path=);
	%macro createivlibrary(h);
		%do i = 1 %to 5;
			libname Segment&i "\\mpwsas64\Core_Credit_Risk_Models\V5\CS_Rebuilds\Rebuild_&modelmonth.\Segment&i.";
			%global  segment_&i._list ;
			proc sql; select reverse(substr(reverse(compress(Parameter)),3)) into : segment_&i._list separated by ' ' from Segment&i..parameter_estimate where upcase(Parameter) ne 'INTERCEPT'; quit;
		%end;
	%mend;
	%createivlibrary(1);
	/* Change for pilot, eutopia and type code */
	%macro applyscore(t);
	    %do m = 1 %to %sysfunc(countw(&&&segment_&t._list));
			%let var = %scan(&&&segment_&t._list, &m);
			%include "\\mpwsas64\Core_Credit_Risk_Models\V5\CS_Rebuilds\Rebuild_&modelmonth.\Segment&t.\&var._if_statement_.sas";
			%include "\\mpwsas64\Core_Credit_Risk_Models\V5\CS_Rebuilds\Rebuild_&modelmonth.\Segment&t.\&var._WOE_if_statement_.sas"; 
	    %end;
	    *****************************************;
	    ** SAS Scoring Code for PROC Hplogistic;
	    *****************************************;
	    %include "\\mpwsas64\Core_Credit_Risk_Models\V5\CS_Rebuilds\Rebuild_&modelmonth.\Segment&t.\&creditlogisticcode..sas";
	%mend;
	%do n = 1 %to 5;
	    data segment_&n.;
			set &inputdataset(where=(seg=&n));
			%applyscore(&n); 
			Final_score = P_target1;
			Score = 1000-(final_score*1000);    
			drop _TEMP;
	    run;
	%end;
	data final_model_data;
	    set %do n = 1 %to 5; segment_&n %end;;
	run;
	data &outputdataset;
		set final_model_data;  
		probability = final_score ;
		finalRiskScore=Score;
		if ThinFileIndicator = 0 then do ;
			if finalRiskScore >= 932.242611756651  Then RG6T = 50;
			else if finalRiskScore >= 912.452480990053 Then RG6T = 51;
			else if finalRiskScore >= 878.956333489911 Then RG6T = 52;
			else if finalRiskScore >= 841.833690856176 Then RG6T = 53;
			else if finalRiskScore >= 811.989999894282 Then RG6T = 54;
			else if finalRiskScore >= 790.349339106057 Then RG6T = 55;
			else if finalRiskScore >= 778.068960766859 Then RG6T = 56;
			else if finalRiskScore >= 758.6444629 Then RG6T = 57;
			else if finalRiskScore >= 746.152798684895 Then RG6T = 58;
			else if finalRiskScore >= 732.001226390991 Then RG6T = 59;
			else if finalRiskScore >= 708.169317621721 Then RG6T = 60;
			else if finalRiskScore >= 690.87118531475 Then RG6T = 61;
			else if finalRiskScore >= 675.057720140646 Then RG6T = 62;
			else if finalRiskScore >= 654.743779812149 Then RG6T = 63;
			else if finalRiskScore >= 640.469307325178 Then RG6T = 64;
			else if finalRiskScore >= 622.758776266177 Then RG6T = 65;
			else if finalRiskScore >= 596.376955694516 Then RG6T = 66;
			else if finalRiskScore > 0    Then RG6T = 67;
		end;
		else if ThinFileIndicator = 1 then do ;
			if finalRiskScore >=      828.199869458644 then RG6T = 68 ;
			else if finalRiskScore >= 762.179100967216 then RG6T = 69 ;
			else if finalRiskScore >= 721.281349457995 then RG6T = 70 ;
			else if finalRiskScore > 0 then RG6T = 71 ;
	  	end;
	  	Scoreband = RG6T ;
	run;

	proc sql;
	      create table &outputdataset
	      as select 1/(1+(probability/(1-probability))**(-1*(c.a))*exp(c.c)) as V5_Rebuild_2, a.*
	      from  &outputdataset a
	      left join calib.PARAMETERS_REBUILD_SEG_&modelmonth. c
	      on a.comp_seg=c.Rebuild_Seg;
	quit;

	proc sql;
		create table &outputdataset
			as select
			1/(1+(V5_Rebuild_2/(1-V5_Rebuild_2))**(-1*(c.a))*exp(c.c)) as V5_Rebuild_3,  a.*
			from  &outputdataset a
			left join calib.PARAMETERS_REB_INSTITCODE_&modelmonth. c
			on put(a.Institutioncode,15.)=c.Reb_INSTITUTIONCODE;
	quit;

%mend;

%macro rebuild_gini(modelmonth=);
	%sysexec mkdir "\\mpwsas64\Core_Credit_Risk_Models\V5\CS_Rebuilds\Rebuild_&modelmonth.\Monitoring\&month.";
	libname comp "\\mpwsas64\Core_Credit_Risk_Models\V5\CS_Rebuilds\Rebuild_&modelmonth.\Monitoring\&month.";

	%scoreinputdata(inputdataset=Disbursedbase_&month,numberofsegment=5,outputdataset=comp.final, creditlogisticcode=creditlogisticcode2, path=\\neptune\SASA$\MPWSAS15\Team Work\Elia\Challenger_model\Rebuild_&modelmonth.);

	%macro Elia(seg=0);
		Data final_&seg.;
			set comp.final;
			where seg = &seg.;
		run;
		%let Target = target; 
		%let Final_Score = Final_Score;
		%Calc_Gini (Final_Score, final_&seg., target, comp.GINITABLE&seg.) ;
		Data comp.GINITABLE&seg. (keep=Gini segment Score_type);
			set comp.GINITABLE&seg.;
			segment = &seg.;
			Score_type = "Rebuild_Compuscan"; 
		run;

		/* calibration part */
		%Calc_Gini(V5_Rebuild_3, final_&seg., target, comp.CALIB_GINITABLE&seg.);
		Data comp.CALIB_GINITABLE&seg. (keep=gini segment Score_type);
			set comp.CALIB_GINITABLE&seg.;
			segment = &seg.;
			Score_type = "Rebuild_Calib_Comp";
		run;

		data comp.GINITABLE&seg.;
			set comp.GINITABLE&seg. comp.CALIB_GINITABLE&seg.;
		run;
	%mend;
	%Elia(seg=1);
	%Elia(seg=2);
	%Elia(seg=3);
	%Elia(seg=4);
	%Elia(seg=5);
	/***************************************************************************************************
									Calculating the overall gini 
	***************************************************************************************************/
	%Calc_Gini (Final_Score, comp.final, target, comp.GINITABLE0) ;
	Data comp.GINITABLE0 (keep=Gini segment Score_type);
		set comp.GINITABLE0;
		segment = 0;
		Score_type = "Rebuild_Compuscan";
	run;

	%Calc_Gini(V5_Rebuild_3, comp.final, target, comp.CALIB_GINITABLE0);
	Data comp.CALIB_GINITABLE0 (keep=gini segment Score_type);
		set comp.CALIB_GINITABLE0;
		segment = 0;
		Score_type = "Rebuild_Calib_Comp";
	run;

	data comp.GINITABLE0;
		set comp.GINITABLE0 comp.CALIB_GINITABLE0;
	run;
	/***************************************************************************************************
									Collating the results  
	***************************************************************************************************/
	data comp.GiniTable;
		set comp.ginitable0 comp.ginitable1 comp.ginitable2 comp.ginitable3 comp.ginitable4 comp.ginitable5;
	run; 
%mend;
%rebuild_gini(modelmonth=&prevprevmonth);
%rebuild_gini(modelmonth=&prevmonth);









/***************************************************************************************************************************************
								Preparing the above output to be used on the dashboard, collating all the Ginis
**************************************************************************************************************************************/
/*Create macro variables*/
%let Score_type1 = Rebuild;
%let Score_type2 = Refit;*/


/*Import overall gini tables for last 3 months (prevprevmonth, prevmonth, month)*/;
%macro ImportGinis(period);

	data V560_overallgini_summary_&period;
		set data.overallgini_summary_&period;
		where Score_type = "Compuscan_Prob";
		month = "&period.";
		applied_model = "V560 Comp Model";
		Score_type = "V560 Comp Pr";
		Current_gini = gini;
	run;

	data V562_overallgini_summary_&period;
		set data.overallgini_summary_&period;
		where Score_type = "V562 Comp Prob";
		month = "&period.";
		applied_model = "V562 Comp Model";
		Score_type = "V562 Comp Pr";
		Current_gini2 = gini;
	run;

	data V560_gini&period(keep = gini  segment);
		set V560_overallgini_summary_&period;
	run;

	data V562_gini&period(keep = gini  segment);
		set V562_overallgini_summary_&period;
	run;

%mend;

%ImportGinis(&prevprevmonth.);
%ImportGinis(&prevmonth.);
%ImportGinis(&month.);


/*Combine all data sets needed for dashboard (for rebuild and refit seperately)*/
%macro createdata(Score_type, path);
	/*Create libraries*/
	libname ppm_1 "&path.\&Score_type._&prevprevmonth";
	libname ppm_2 "&path.\&Score_type._&prevprevmonth\Monitoring\&prevmonth";
	libname ppm_3 "&path.\&Score_type._&prevprevmonth\Monitoring\&month";

	libname pm_2 "&path.\&Score_type._&prevmonth";
	libname pm_3 "&path.\&Score_type._&prevmonth\Monitoring\&month";

	libname m_3 "&path.\&Score_type._&month";

	/***********************************/
	data ginitable_ppm_1;
		set ppm_1.&Score_type._gini_&prevprevmonth;
		month = "&prevprevmonth";
		if Score_type = "&Score_type._Compuscan" then
			applied_model = "&Score_type._Comp_&prevprevmonth ";
		else applied_model = "&Score_type._Calib_&prevprevmonth";
	run;

	data ginitable_ppm_2;
		set ppm_2.ginitable;
		month = "&prevmonth";
		if Score_type = "&Score_type._Compuscan" then
			applied_model = "&Score_type._Comp_&prevprevmonth ";
		else applied_model = "&Score_type._Calib_&prevprevmonth";
	run;

	data ginitable_ppm_3;
		set ppm_3.ginitable;
		month = "&month";
		if Score_type = "&Score_type._Compuscan" then
			applied_model = "&Score_type._Comp_&prevprevmonth ";
		else applied_model = "&Score_type._Calib_&prevprevmonth";
	run;

	/***********************************/
	data ginitable_pm_2;
		set pm_2.&Score_type._gini_&prevmonth;
		month = "&prevmonth";
		if Score_type = "&Score_type._Compuscan" then
			applied_model = "&Score_type._Comp_&prevmonth ";
		else applied_model = "&Score_type._Calib_&prevmonth";
	run;

	data ginitable_pm_3;
		set pm_3.ginitable;
		month = "&month";
		if Score_type = "&Score_type._Compuscan" then
			applied_model = "&Score_type._Comp_&prevmonth ";
		else applied_model = "&Score_type._Calib_&prevmonth";
	run;

	/***********************************/
	data ginitable_m_3;
		set m_3.&Score_type._gini_&month;
		month = "&month";
		if Score_type = "&Score_type._Compuscan" then
			applied_model = "&Score_type._Comp_&month ";
		else applied_model = "&Score_type._Calib_&month";
	run;


	/***********************************/
	data Gini_&Score_type.1; 
		set ginitable_ppm_1 ginitable_ppm_2 ginitable_ppm_3 
		ginitable_pm_2 ginitable_pm_3 ginitable_m_3;
	run;

	%macro BaseTab(period);	
		proc sql;
		create table currentgini_&period as
		select a.*, b.gini as Current_gini, c.gini as Current_gini2
		from Gini_&Score_type.1 a left join V560_gini&period b on a.segment = b.segment
		left join V562_gini&period c on b.segment = c.segment
			where a.month = "&period"; 
		quit;
	%mend;

	%BaseTab(&prevprevmonth);
	%BaseTab(&prevmonth);
	%BaseTab(&month);

	data Gini_&Score_type.;
		set currentgini_&prevprevmonth currentgini_&prevmonth currentgini_&month;
	run;
%mend;
%createdata(Score_type = &Score_type1, path = &folder\CS_Rebuilds);








libname calib "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\Compuscan Models\calibration\calibration_new\Refit_calib";
/*****************************************************************************************************************************************************
								Moving on to the refit_V570 monitoring
*********************************************************************************************************************************************************/
%macro scoreinputdata(inputdataset=,numberofsegment=,outputdataset=, creditlogisticcode=creditlogisticcode,path=);
	%macro createivlibrary(h);
	  %do i = 1 %to 5;
	      libname Segment&i "\\mpwsas64\Core_Credit_Risk_Models\V5\CS_Refit_V570\Refit_&modelmonth.\Segment&i.";
	      %global  segment_&i._list ;
	      proc sql; select reverse(substr(reverse(compress(Parameter)),3)) into : segment_&i._list separated by ' ' from Segment&i..parameter_estimate where upcase(Parameter) ne 'INTERCEPT'; quit;
	  %end;
	%mend;
	%createivlibrary(1);

	/* Change for pilot , eutopia and type codde  */
	%macro applyscore(t);
	    %do i = 1 %to %sysfunc(countw(&&segment_&t._list));
	          %let var = %scan(&&segment_&t._list, &i);
	          %include "\\mpwsas64\Core_Credit_Risk_Models\V5\CS_Refit_V570\Refit_&modelmonth.\Segment&t.\&var._if_statement_.sas";
	          %include "\\mpwsas64\Core_Credit_Risk_Models\V5\CS_Refit_V570\Refit_&modelmonth.\Segment&t.\&var._WOE_if_statement_.sas"; 
	    %end;
	    *****************************************;
	    ** SAS Scoring Code for PROC Hplogistic;
	    *****************************************;
	    %include "\\mpwsas64\Core_Credit_Risk_Models\V5\CS_Refit_V570\Refit_&modelmonth.\Segment&t.\&creditlogisticcode..sas";
	%mend;

	%do n = 1 %to 5;
	    data segment_&n.;
			set &inputdataset(where=(seg=&n));
			%applyscore(&n); 
			Final_score = P_target1;
			Score = 1000-(final_score*1000);    
			drop _TEMP;
	    run;
	%end;

	data final_model_data;
	    set %do n = 1 %to 5; segment_&n %end;;
	run;

	data &outputdataset;
		set final_model_data;  
		probability = final_score ;
		finalRiskScore=Score;

		if ThinFileIndicator = 0 then do ;
			if finalRiskScore >= 932.242611756651  Then RG6T = 50;
			else if finalRiskScore >= 912.452480990053 Then RG6T = 51;
			else if finalRiskScore >= 878.956333489911 Then RG6T = 52;
			else if finalRiskScore >= 841.833690856176 Then RG6T = 53;
			else if finalRiskScore >= 811.989999894282 Then RG6T = 54;
			else if finalRiskScore >= 790.349339106057 Then RG6T = 55;
			else if finalRiskScore >= 778.068960766859 Then RG6T = 56;
			else if finalRiskScore >= 758.6444629 Then RG6T = 57;
			else if finalRiskScore >= 746.152798684895 Then RG6T = 58;
			else if finalRiskScore >= 732.001226390991 Then RG6T = 59;
			else if finalRiskScore >= 708.169317621721 Then RG6T = 60;
			else if finalRiskScore >= 690.87118531475 Then RG6T = 61;
			else if finalRiskScore >= 675.057720140646 Then RG6T = 62;
			else if finalRiskScore >= 654.743779812149 Then RG6T = 63;
			else if finalRiskScore >= 640.469307325178 Then RG6T = 64;
			else if finalRiskScore >= 622.758776266177 Then RG6T = 65;
			else if finalRiskScore >= 596.376955694516 Then RG6T = 66;
			else if finalRiskScore > 0    Then RG6T = 67;
		end;
		else if ThinFileIndicator = 1 then do ;
			if finalRiskScore >=      828.199869458644 then RG6T = 68 ;
			else if finalRiskScore >= 762.179100967216 then RG6T = 69 ;
			else if finalRiskScore >= 721.281349457995 then RG6T = 70 ;
			else if finalRiskScore > 0 then RG6T = 71 ;
		end;
		Scoreband = RG6T ;
	run;

	proc sql;
	      create table &outputdataset
	      as select 1/(1+(probability/(1-probability))**(-1*(c.a))*exp(c.c)) as V5_Refit_2, a.*
	      from  &outputdataset a
	      left join calib.PARAMETERS_REFIT_SEG_&modelmonth. c
	      on a.comp_seg=c.Refit_Seg;
	quit;

	proc sql;
		create table &outputdataset
			as select
			1/(1+(V5_Refit_2/(1-V5_Refit_2))**(-1*(c.a))*exp(c.c)) as V5_Refit_3,  a.*
			from  &outputdataset a
			left join calib.PARAMETERS_REF_INSTITCODE_&modelmonth. c
			on put(a.Institutioncode,15.)=c.Ref_INSTITUTIONCODE;
	quit;
%mend;

%macro refit_gini(modelmonth=);
	
	%sysexec mkdir "\\mpwsas64\Core_Credit_Risk_Models\V5\CS_Refit_V570\Refit_&modelmonth.\Monitoring\&month.";
	libname comp "\\mpwsas64\Core_Credit_Risk_Models\V5\CS_Refit_V570\Refit_&modelmonth.\Monitoring\&month.";

	data disbursedbase_&month.;
        set disbursedbase_&month.;
        
        *%rename1(&oldvarlist, &newvarlist);
    run;

	%scoreinputdata(inputdataset=disbursedbase_&month.,numberofsegment=5,outputdataset=comp.final, creditlogisticcode=creditlogisticcode2, path=\\neptune\SASA$\MPWSAS15\Team Work\Elia\Challenger_model\Refit_&modelmonth.);
	%macro Elia(seg=0);
		Data final_&seg.;
			set comp.final;
			where seg = &seg.;
		run;
		%let Target = target; 
		%let Final_Score = Final_Score;
		%Calc_Gini (Final_Score, final_&seg., target, comp.GINITABLE&seg.) ;
		Data comp.GINITABLE&seg. (keep=Gini segment Score_type);
			set comp.GINITABLE&seg.;
			segment = &seg.;
			Score_type = "Refit_Compuscan";
		run;

		/* calibration part */
		%Calc_Gini(V5_Refit_3, final_&seg., target, comp.CALIB_GINITABLE&seg.);
		Data comp.CALIB_GINITABLE&seg. (keep=Gini segment Score_type);
			set comp.CALIB_GINITABLE&seg.;
			segment = &seg.;
			Score_type = "Refit_Calib_Comp";
		run;

		data comp.GINITABLE&seg.;
			set comp.GINITABLE&seg. comp.CALIB_GINITABLE&seg.;
		run;
	%mend;
	%Elia(seg=1);
	%Elia(seg=2);
	%Elia(seg=3);
	%Elia(seg=4);
	%Elia(seg=5);
	/***************************************************************************************************
									Calculating the overall gini 
	***************************************************************************************************/
	%Calc_Gini (Final_Score, comp.final, target, comp.GINITABLE0) ;
	Data comp.GINITABLE0 (keep=Gini segment Score_type);
		set comp.GINITABLE0;
		segment = 0;
		Score_type = "Refit_Compuscan";
	run;

	%Calc_Gini(V5_Refit_3, comp.final, target, comp.CALIB_GINITABLE0);
	Data comp.CALIB_GINITABLE0 (keep=gini segment Score_type);
		set comp.CALIB_GINITABLE0;
		segment = 0;
		Score_type = "Refit_Calib_Comp";
	run;

	data comp.GINITABLE0;
		set comp.GINITABLE0 comp.CALIB_GINITABLE0;
	run;
	/***************************************************************************************************
									Collating the results  
	***************************************************************************************************/
	data comp.GiniTable;
		set comp.ginitable0 comp.ginitable1 comp.ginitable2 comp.ginitable3 comp.ginitable4 comp.ginitable5;
	run;
%mend;
%refit_gini(modelmonth=&prevprevmonth);
%refit_gini(modelmonth=&prevmonth);





/***************************************************************************************************************************************
								Preparing the above output to be used on the CS V570 dashboard, collecting all the Ginis
**************************************************************************************************************************************/
/*Create macro variables*/

%let Score_type2 = Refit;


/*Import overall gini tables for last 3 months (prevprevmonth, prevmonth, month)*/
%macro ImportGinis(period);


	data V570_overallgini_summary_&period;
		set data2.overallgini_summary_&period;
		where Score_type = "Compuscan_Prob";
		month = "&period.";
		applied_model = "V570 Comp Model";
		Score_type = "V570 Comp Pr";
		Current_gini = gini;
	run;

	data V572_overallgini_summary_&period;
		set data2.overallgini_summary_&period;
		where Score_type = "V572 Comp Prob";
		month = "&period.";
		applied_model = "V572 Comp Model";
		Score_type = "V572 Comp Pr";
		Current_gini2 = gini;
	run;

	data V570_gini&period(keep = gini  segment);
		set V570_overallgini_summary_&period;
	run;

	data V572_gini&period(keep = gini  segment);
		set V572_overallgini_summary_&period;
	run;

%mend;

%ImportGinis(&prevprevmonth.);
%ImportGinis(&prevmonth.);
%ImportGinis(&month.);


/*Combine all data sets needed for dashboard (for rebuild and refit seperately)*/
%macro createdata(Score_type, path);
	/*Create libraries*/
	libname ppm_1 "&path.\&Score_type._&prevprevmonth";
	libname ppm_2 "&path.\&Score_type._&prevprevmonth\Monitoring\&prevmonth";
	libname ppm_3 "&path.\&Score_type._&prevprevmonth\Monitoring\&month";

	libname pm_2 "&path.\&Score_type._&prevmonth";
	libname pm_3 "&path.\&Score_type._&prevmonth\Monitoring\&month";

	libname m_3 "&path.\&Score_type._&month";

	/***********************************/
	data ginitable_ppm_1;
		set ppm_1.&Score_type._gini_&prevprevmonth;
		month = "&prevprevmonth";
		if Score_type = "&Score_type._Compuscan" then
			applied_model = "&Score_type._Comp_&prevprevmonth ";
		else applied_model = "&Score_type._Calib_&prevprevmonth";
	run;

	data ginitable_ppm_2;
		set ppm_2.ginitable;
		month = "&prevmonth";
		if Score_type = "&Score_type._Compuscan" then
			applied_model = "&Score_type._Comp_&prevprevmonth ";
		else applied_model = "&Score_type._Calib_&prevprevmonth";
	run;

	data ginitable_ppm_3;
		set ppm_3.ginitable;
		month = "&month";
		if Score_type = "&Score_type._Compuscan" then
			applied_model = "&Score_type._Comp_&prevprevmonth ";
		else applied_model = "&Score_type._Calib_&prevprevmonth";
	run;

	/***********************************/
	data ginitable_pm_2;
		set pm_2.&Score_type._gini_&prevmonth;
		month = "&prevmonth";
		if Score_type = "&Score_type._Compuscan" then
			applied_model = "&Score_type._Comp_&prevmonth ";
		else applied_model = "&Score_type._Calib_&prevmonth";
	run;

	data ginitable_pm_3;
		set pm_3.ginitable;
		month = "&month";
		if Score_type = "&Score_type._Compuscan" then
			applied_model = "&Score_type._Comp_&prevmonth ";
		else applied_model = "&Score_type._Calib_&prevmonth";
	run;

	/***********************************/
	data ginitable_m_3;
		set m_3.&Score_type._gini_&month;
		month = "&month";
		if Score_type = "&Score_type._Compuscan" then
			applied_model = "&Score_type._Comp_&month ";
		else applied_model = "&Score_type._Calib_&month";
	run;


	/***********************************/
	data Gini_&Score_type.1; 
		set ginitable_ppm_1 ginitable_ppm_2 ginitable_ppm_3 
		ginitable_pm_2 ginitable_pm_3 ginitable_m_3;
	run;

	%macro BaseTab(period);	
		proc sql;
		create table currentgini_&period as
		select a.*, b.gini as Current_gini, c.gini as Current_gini2
		from Gini_&Score_type.1 a left join V570_gini&period b on a.segment = b.segment
		left join V572_gini&period c on b.segment = c.segment
			where a.month = "&period"; 
		quit;
	%mend;

	%BaseTab(&prevprevmonth);
	%BaseTab(&prevmonth);
	%BaseTab(&month);

	data Gini_&Score_type.;
		set currentgini_&prevprevmonth currentgini_&prevmonth currentgini_&month;
	run;
%mend;

%createdata(Score_type = &Score_type2, path = &folder2.\CS_Refit_V570);
/*Output: Gini_Rebuild, Gini_Refit*/

/*Final table needed for dashboard - combine rebuild and refit*/
data Challenger_Models;
	set Gini_Rebuild Gini_Refit 
	V570_overallgini_summary_&prevprevmonth V572_overallgini_summary_&prevprevmonth 
	V570_overallgini_summary_&prevmonth V572_overallgini_summary_&prevmonth
	V570_overallgini_summary_&month V572_overallgini_summary_&month;
run;

data data2.Challenger_Models_&month;
	set Challenger_Models;
	rc = (Current_gini - Gini)/Gini;
	rc2 = (Current_gini2 - Gini)/Gini;
run;

data data2.Challenger_Models_&month (drop=rc2 Current_gini2);
	set data2.Challenger_Models_&month;
	if Score_type = 'Refit_Calib_Com' or Score_type = 'Rebuild_Calib_Com' or Score_type = 'V572 Comp Pr' then rc = rc2;
	if Score_type = 'Refit_Calib_Com' or Score_type = 'Rebuild_Calib_Com' or Score_type = 'V572 Comp Pr' then Current_gini = Current_gini2;
run;


/*** Save Ginis for Model Gini Comparison table on dashbaord ***/
data Rebuild_Refit_Ginis (keep=gini score_type segment);
set data2.Challenger_Models_&month;
if month = "&month." and (applied_model = "Rebuild_Comp_&prevprevmonth." or applied_model = "Refit_Comp_&prevprevmonth.");
run;

data data2.overallgini_summary_&month;
    set data2.overallgini_summary_&month Rebuild_Refit_Ginis;
run;

proc sql;
    create table data.currentmodel_benchmark_&month as
        select distinct  a.*,b.gini as Current_gini, (b.gini-a.gini)/a.gini as Relative_lift
        from data.overallgini_summary_&month a,
            (select segment, gini from data.overallgini_summary_&month
            where score_type='Compuscan_Prob') b
        where a.segment=b.segment;
quit;

data data2.currentmodel_benchmark_&month;
    set data2.currentmodel_benchmark_&month;
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

data data2.currentmodel_benchmark_&month.;
     set data2.currentmodel_benchmark_&month.;
     if score_type='Compuscan_Generic' then score_type ='Comp Generic Score';
     else if score_type='Tu_Generic' then score_type ='TU Generic Score';
     else if score_type='Compuscan_Prob' then score_type ='V560 Comp Prob';
     else if score_type='Tu_V570_prob' then score_type ='V570 TU Prob';
     else if score_type='V6_Prob3' then score_type ='V622 Prob';
     else if score_type='V635' then score_type ='V635 Prob';
     else if score_type='V636' then score_type ='V636 Prob';
     else if score_type='Rebuild_Compuscan' then score_type ='Rebuild Compuscan';
     else if score_type='Refit_Compuscan' then score_type ='Refit Compuscan';
	 else if score_type='V645' then score_type ='V645 Prob';
     else if score_type='tu_V580_prob' then score_type ='V580 TU Prob';
     else if score_type='Compuscan_Prob' then score_type ='V570 Comp Prob';
run;


/*** Upload data to cred_scoring ***/
libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

proc delete data =cred_scr.Challenger_Models_xml; run;
proc sql;
create table cred_scr.Challenger_Models_xml(BULKLOAD=YES) as
select distinct *
from data2.Challenger_Models_&month.;
quit;

libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

proc delete data =cred_scr.v5_Gini_relative_xml; run;
proc sql;
create table cred_scr.v5_Gini_relative_xml(BULKLOAD=YES) as
select distinct *
from data2.currentmodel_benchmark_&month.;
quit;



filename macros2 '\\mpwsas65\Process_Automation\macros';
options sasautos = (sasautos  macros2);
