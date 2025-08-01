/* Testing the GitLab automated pull schedule */

options compress = yes;
options compress = on;
options mprint mlogic symbolgen;
ods graphics off;
ods _all_ close;
ods listing;

%let todaysDate = %sysfunc(today(), yymmddn8.);
%put &todaysDate;
%let thismonth = %sysfunc(today(), yymmn6.);
%put &thismonth;

%include "\\mpwsas65\Process_Automation\Codes\Git_Repositories\Core Credit Risk Models - Monitoring\Macros\createdirectory.sas";
%include "\\mpwsas65\Process_Automation\Codes\Git_Repositories\Core Credit Risk Models - Monitoring\Macros\no_data_alert.sas";
%createdirectory(directory=\\MPWSAS64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\Data\&thismonth.\&todaysDate.);
%createdirectory(directory=\\MPWSAS64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\Data_IA\&thismonth.\&todaysDate.);
libname BatchTes "\\MPWSAS64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\Data\&thismonth.\&todaysDate.";
libname scoring odbc dsn=Dev_DDCr schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;


data _null_;
	call symput('lstmnth',cats("'",put(intnx('day',today(),-30),yymmddd10.),"'"));
run;

proc sql stimer;
	connect to ODBC (dsn=mpwaps);
	create table lastRun as 
	select * from connection to odbc 
	(	
		SELECT (datediff(day, max(applicationdate), getdate()) -  1) as days
		FROM DEV_DataDistillery_Credit.dbo.all_scores
	);
    disconnect from ODBC;
quit;

proc sql;
	Select days into: days 
	from lastRun;
quit;

data _null_;
	call symput('lstDate',cats("'",put(intnx('day',today(),-(&days)),yymmddd10.),"'"));
run;
%put &lstDate.;

%macro dailyMonitoring();

	%macro startJob();
		%let startAlert = 10;
		%global no_data;
		%let no_data = 0;
		%let hour=%sysfunc(hour(%sysfunc(time())));
		%let status = 0;

		%do %while(%eval(&Status) eq 0 and &hour ne 0);
			%let time=%sysfunc(time(), hhmm);
			%let hour=%sysfunc(hour(%sysfunc(time())));

			%if &hour eq &startAlert and &hour ne 0 and &no_data eq 0 %then %do;
				%let no_data = 1; /*No data alert*/
				%no_data_alert(alert=&no_data)
				%let startAlert = %eval(&startAlert + 2);
				%let no_data = 0; /*No data alert*/
			%end;

			%else %if &hour eq 0 %then %do;
				%let no_data = 2; /*No data end alert*/
				%no_data_alert(alert=&no_data)
			%end;

			proc sql stimer;
				connect to ODBC (dsn=MPWAPS);
				create table ImportStatus as 
				select * from connection to odbc 
				(	
					SELECT CAPRIIMPORTSTATUS
					FROM PRD_PRESS.CAPRI.CAPRI_DAILY_IMPORT_COMPLETED
				);
			    disconnect from ODBC;
			quit;

			proc sql;
				Select CAPRIIMPORTSTATUS into: Status 
				from importstatus;
			quit;

			%if (%eval(&Status) eq 0) %then %do;	
				data _null_;
					call sleep(60000);
				run;
			%end;
		%end;
		
	%mend;
	%startJob();

	%put &no_data;
	%if &no_data ne 2 %then %do;

		/*Get cleaned data*/
		%include "\\MPWSAS64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Daily Monitoring2\GetData_V655.sas";
		%GetData(table=DEV_DataDistillery_Credit.dbo.V6DailyData, outDataset=rawdata5, dsn=DEV_DDCr);

/*		data rawdata5a;*/
/*			set vic.rawdata5 ;*/
/*			if applicationdate >= '2023-06-03';*/
/*		run;*/
/**/
/*		data rawdata5b;*/
/*			set vic.rawdata5 ;*/
/*			if applicationdate < '2023-06-03';*/
/*		run;*/
	

		/*Apply TU and CS scoring*/
/*		%include "\\mpwsas65\Process_Automation\Codes\Git_Repositories\Core Credit Risk Models - Monitoring\Macros\Scoring.sas";*/
/*		%scoring(inDataset=rawdata5b, outDataset=CS_V570_score,Path=\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V570 Compuscan,scored_name =CS_V570, modelType =C);*/
/*		%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Apply_V575.sas";*/
/*		%applyV575(inDataset=CS_V570_score, outDataset=CS_V575_score);*/
/*		%scoring(inDataset=CS_V575_score, outDataset=TU_V580_score,Path=\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU,scored_name =TU_V580, modelType =T);*/
/*		%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Apply_V585.sas";*/
/*		%applyV585(inDataset=TU_V580_score, outDataset=TU_V585_score);*/
/**/

		%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Apply_V580_V570_new.sas";
		%applyCS_TU(inDataset=rawdata5, outDataset=TBL_Scored);
		%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Apply_V575.sas";
		%applyV575(inDataset=TBL_Scored, outDataset=CS_V575_score2);
		%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Apply_V585.sas";
		%applyV585(inDataset=CS_V575_score2, outDataset=TU_V585_score2);


		/*TU_SCORED*/

		data TBL_Scored2;
			set /*TU_V585_Score*/ TU_V585_Score2 scoring.TU_scored;
			if applicationdate >= &lstmnth; ;
		run;

		proc sort data = TBL_Scored2; by tranappnumber descending uniqueid; run;
		proc sort data = TBL_Scored2 nodupkey out= TBL_Scored2; by tranappnumber; run;


		proc sql;
		libname scoring odbc dsn=Dev_DDCr schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

   			drop table scoring.TU_scored;

		    create table scoring.TU_scored like TBL_Scored2;

		    insert into scoring.TU_scored (bulkload=Yes)
		    select *
		    from TBL_Scored2;
		quit;
		/**/

		/*CS_SCORED*/


		proc sql;
		libname scoring odbc dsn=Dev_DDCr schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

			drop table scoring.CS_scored;

		    create table scoring.CS_scored like TBL_Scored2;

		    insert into scoring.CS_scored (bulkload=Yes)
		    select *
		    from TBL_Scored2;
		quit;
		/**/

		/**/

		/*Apply V655 and IA Scoring*/
		%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\ApplyV655.sas";
		%applyV655(inDataset=TU_V585_score2, outDataset=rawdata_V655);

		%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\ApplyV667.sas";
		%applyV667(indataset = rawdata_V655, outDataset=rawdata_V667);


		data rawdata_V658a;
			set rawdata_V667 ;
			if scorecardversion in ("V655","V585","V5");
		run;

		data rawdata_V658b;
			set rawdata_V667 ;
			if scorecardversion in ("V667","V585","V5");
		run;

		%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\ApplyV658.sas";
		%applyV658(inDataset=rawdata_V658a, prob_name=V655, score_name=OVERALLSCORE, outDataset=rawdata_V658);


		%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\ApplyV669.sas";
		%applyV669(inDataset=rawdata_V658b, prob_name=V667, score_name=OVERALLSCORE, outDataset=rawdata_V669);


		data rawdata_IA;
			set rawdata_V658 rawdata_V669;
		run;

	
		/*Calculate final score*/
		data rawdata_IA;
			set rawdata_V658;
			if SCORECARDVERSION = "V585" then V6_finalriskscore =  1000-(V585*1000);
			if SCORECARDVERSION = "V575" then V6_finalriskscore =  1000-(V575*1000);
			if SCORECARDVERSION = "V655" then V6_finalriskscore =  1000-(V655*1000);
			if SCORECARDVERSION = "V667" then V6_finalriskscore =  1000-(V667*1000);
			TU_V585_finalriskscore = 1000-(V585*1000);
			V655_finalriskscore =  1000-(V655*1000);
			V667_finalriskscore =  1000-(V667*1000);
			V6_finalriskscore_IA =  1000-(V658*1000);
			if scorecardversion = "V667" then 
			V6_RiskGroup = V667_RiskGroup;
			else if scorecardversion = "V655" then
			V6_RiskGroup = V655_RiskGroup;
			else if scorecardversion = "V585" then
			V6_RiskGroup = V585_RiskGroup;
			else if scorecardversion = "V5" then
			V6_RiskGroup = V575_RiskGroup;
			if scorecardversion = "V667" then 
			V6_RiskGroup_IA = V669_RiskGroup;
			else if scorecardversion = "V655" then
			V6_RiskGroup = V658_RiskGroup;
		run;
		/**/

		
		/*Scorecard Distribution*/
		data scorecardDistribution;
			set scoring.scorecardDistribution (keep=applicationdate uniqueid tranappnumber scorecardversion appdate)
			all_scores1 (keep= weekday applicationdate uniqueid tranappnumber scorecardversion appdate);
			where applicationdate >= &lstmnth;
			format appdate yymmdds10.;
			appdate = input(applicationdate,yymmdd10.);
		run;

		proc sort data = scorecardDistribution; by tranappnumber descending uniqueid; run;
		proc sort data = scorecardDistribution nodupkey dupout=dups  out= scorecardDistribution; by tranappnumber; run;
 
		proc sql;
		libname scoring odbc dsn=Dev_DDCr schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

			drop table scoring.scorecardDistribution;
	
		    create table scoring.scorecardDistribution like scorecardDistribution;

		    insert into scoring.scorecardDistribution (bulkload=Yes)
		    select *
		    from scorecardDistribution;
		quit;
		/**/

		data all_scores2;
			set all_scores1;
			V6_finalriskscore_IA_prod = input(IAfinalriskscore,best16.);
			if scorecardversion in ("V655","V667") then do;
				if Min(ThinFileIndicator,TU_Thin) = 0 then do;
					if V6_finalriskscore_IA_prod >= 929.70414109 then V6_RiskGroup_IA_prod = 50;
					else if V6_finalriskscore_IA_prod >= 909.59116387 then V6_RiskGroup_IA_prod = 51;
					else if V6_finalriskscore_IA_prod >= 872.15152879 then V6_RiskGroup_IA_prod = 52;
					else if V6_finalriskscore_IA_prod >= 827.34047213 then V6_RiskGroup_IA_prod = 53;
					else if V6_finalriskscore_IA_prod >= 790.59177669 then V6_RiskGroup_IA_prod = 54;
					else if V6_finalriskscore_IA_prod >= 764.70857459 then V6_RiskGroup_IA_prod = 55;
					else if V6_finalriskscore_IA_prod >= 750.21402079 then V6_RiskGroup_IA_prod = 56;
					else if V6_finalriskscore_IA_prod >= 727.94382999 then V6_RiskGroup_IA_prod = 57;
					else if V6_finalriskscore_IA_prod >= 714.25140059 then V6_RiskGroup_IA_prod = 58;
					else if V6_finalriskscore_IA_prod >= 699.11965741 then V6_RiskGroup_IA_prod = 59;
					else if V6_finalriskscore_IA_prod >= 676.02770676 then V6_RiskGroup_IA_prod = 60;
					else if V6_finalriskscore_IA_prod >= 660.32199365 then V6_RiskGroup_IA_prod = 61;
					else if V6_finalriskscore_IA_prod >= 646.97357763 then V6_RiskGroup_IA_prod = 62;
					else if V6_finalriskscore_IA_prod >= 620 		  then V6_RiskGroup_IA_prod = 63;
					else if V6_finalriskscore_IA_prod >= 543.49446576 then V6_RiskGroup_IA_prod = 64;
					else if V6_finalriskscore_IA_prod >= 542.20967069 then V6_RiskGroup_IA_prod = 65;
					else if V6_finalriskscore_IA_prod >= 525.71483413 then V6_RiskGroup_IA_prod = 66;
					else if V6_finalriskscore_IA_prod >= 0 			  then V6_RiskGroup_IA_prod = 67;
				end;

				else if Min(ThinFileIndicator,TU_Thin) = 1 then do;
					if V6_finalriskscore_IA_prod >= 771.77225514  then V6_RiskGroup_IA_prod = 68;
					else if V6_finalriskscore_IA_prod >= 714.29321139 then V6_RiskGroup_IA_prod = 69;
					else if V6_finalriskscore_IA_prod >= 681.51627236 then V6_RiskGroup_IA_prod = 70;
					else if V6_finalriskscore_IA_prod >= 0 then V6_RiskGroup_IA_prod = 71;
				end;
			end;
		run;




		/*Match rates*/
		data all_scores3;
			set all_scores2 ;

			if  round(TU_V580_prod,0.0001) - round(input(TU_V580_prob,best16.),0.0001) = 0 then TU_V580_match = 1;
		    else TU_V580_match =0;

			if scorecardversion = 'V585' then do;

			if  round(V581_prod ,0.0001) - round(input(V581,best16.),0.0001) = 0 then V581_Match = 1;
		    else V581_Match =0;

			if  round(V582_prod ,0.0001) - round(input(V582,best16.),0.0001) = 0 then V582_Match = 1;
		    else V582_Match =0;

			if  round(V583_prod ,0.0001) - round(input(V583,best16.),0.0001) = 0 then V583_Match = 1;
		    else V583_Match =0;

			if  round(V584_prod ,0.0001) - round(input(V584,best16.),0.0001) = 0 then V584_Match = 1;
		    else V584_Match =0;


			if  round(TU_V585_prod,0.0001) - round(input(V585,best16.),0.0001) = 0 then TU_V585_match = 1;
		    else TU_V585_match =0;
			end;

			if scorecardversion = "V5" then do;

			if  round(V571_prod ,0.0001) - round(input(V571,best16.),0.0001) = 0 then V571_Match = 1;
		    else V571_Match =0;

			if  round(V572_prod ,0.0001) - round(input(V572,best16.),0.0001) = 0 then V572_Match = 1;
		    else V572_Match =0;

			if  round(V573_prod ,0.0001) - round(input(V573,best16.),0.0001) = 0 then V573_Match = 1;
		    else V573_Match =0;

			if  round(V574_prod ,0.0001) - round(input(V574,best16.),0.0001) = 0 then V574_Match = 1;
		    else V574_Match =0;


			if  round(CS_V575_prod,0.0001) - round(input(V575,best16.),0.0001) = 0 then CS_V575_match = 1;
		    else CS_V575_match =0;

			end;


			if  round(CS_V570_prod,0.0001) - round(input(CS_V570_prob,best16.),0.0001) = 0 then CS_V570_match = 1;
		    else CS_V570_match =0;

			
			if  round(V650_prod ,0.0001) - round(input(V650,best16.),0.0001) = 0 then V650_Match = 1;
		    else V650_Match =0;

			if  round(V651_prod ,0.0001) - round(input(V651,best16.),0.0001) = 0 then V651_Match = 1;
		    else V651_Match =0;

			if  round(V652_prod ,0.0001) - round(input(V652,best16.),0.0001) = 0 then V652_Match = 1;
		    else V652_Match =0;

			if  round(V653_prod ,0.0001) - round(input(V653,best16.),0.0001) = 0 then V653_Match = 1;
		    else V653_Match =0;

			if  round(V654_prod ,0.0001) - round(input(V654,best16.),0.0001) = 0 then V654_Match = 1;
		    else V654_Match =0;

			if  round(V655_prod ,0.0001) - round(input(V655,best16.),0.0001) = 0 then V655_Match = 1;
		    else V655_Match =0;

			if  round(V658_prod ,0.0001) - round(input(V658,best16.),0.0001) = 0 then V658_Match = 1;
		    else V658_Match =0;

			if  round(V660_prod ,0.0001) - round(input(V660,best16.),0.0001) = 0 then V660_Match = 1;
		    else V660_Match =0;

			if  round(V661_prod ,0.0001) - round(input(V661,best16.),0.0001) = 0 then V661_Match = 1;
		    else V661_Match =0;

			if  round(V662_prod ,0.0001) - round(input(V662,best16.),0.0001) = 0 then V662_Match = 1;
		    else V662_Match =0;

			if  round(V663_prod ,0.0001) - round(input(V663,best16.),0.0001) = 0 then V663_Match = 1;
		    else V663_Match =0;

			if  round(V664_prod ,0.0001) - round(input(V664,best16.),0.0001) = 0 then V664_Match = 1;
		    else V664_Match =0;

			if  round(V665_prod ,0.01) - round(input(V665,best16.),0.01) = 0 then V665_Match = 1;
		    else V665_Match =0;


			if  round(V667_prod ,0.01) - round(input(V667,best16.),0.01) = 0 then V667_Match = 1;
		    else V667_Match =0;

			if  round(V669_prod ,0.01) - round(input(V669,best16.),0.01) = 0 then V669_Match = 1;
		    else V669_Match =0;



			if scorecardversion ne "V4";
			if scorecardversion = "V655" then V6_match = V655_match;
			if scorecardversion = "V667" then V6_match = V667_match;
			if scorecardversion in ("V655") then V6_match_IA = V658_match;
			else if scorecardversion in ("V667") then V6_match_IA = V669_match;


			if  round(input(V6_RiskGroup_prod,best16.) ,0.01) - round(V6_Riskgroup,0.01) = 0 then RG_Match = 1;
		    else RG_Match =0;
			if scorecardversion in ("V655","V667") and round(V6_RiskGroup_IA_prod ,0.01) - round(V6_Riskgroup_IA,0.01) = 0 then RG_Match_IA = 1;
		    else RG_Match_IA =0;
		run;
		/**/


		/*Finalize*/
		data all_scores4;
			set  all_scores3;
			format appdate2 yymmdds10.;
			newvar = input(applicationdate,yymmdd10.);
			format newvar yymmddn8.;
			WeekDay = weekday(newvar);
			Week = week(newvar, 'v'); 
			appdate2 = appdate;
		run;



		data all_scores5;
			set all_scores4 scoring.all_scores  ;
			format appdate2 yymmdds10.;
			if applicationdate >= &lstmnth; 
			if v6_match ne .;
			appdate2 = appdate;
			if sessionid ne '' then IA = 'IA_flag';
		run;

		proc sort data = all_scores5; by tranappnumber descending uniqueid; run;
		proc sort data = all_scores5 nodupkey out= all_scores5; by tranappnumber; run;


		proc sql;
		libname scoring odbc dsn=Dev_DDCr schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

			drop table scoring.all_scores;
	
		    create table scoring.all_scores like all_scores5;

		    insert into scoring.all_scores (bulkload=Yes)
		    select *
		    from all_scores5;
		quit;

		proc sql;
		libname scoring odbc dsn=Dev_DDCr schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

			drop table scoring.all_scores2;
	
		    create table scoring.all_scores2 like all_scores5;

		    insert into scoring.all_scores2 (bulkload=Yes)
		    select *
		    from all_scores5;
		quit;

		data batchtes.all_scores;
			set scoring.all_scores;
			if sessionid ne '' then IA = 'IA_flag';
		run;
		
		proc sql;
		create table V622_intables as
			select 	applicationdate, weekday,scorecardversion,
				count(*) AS Volume, 
/*				sum(CAPRI_APPLICANT)/count(CAPRI_APPLICANT) as APPLICANT_TABLE format=percent9.2,*/
				sum(CAPRI_EMPLOYMENT)/count(CAPRI_EMPLOYMENT) as EMPLOYMENT_TABLE format=percent9.2,
				sum(CAPRI_AFFORDABILITY)/count(CAPRI_AFFORDABILITY) as AFFORDABILITY_TABLE format=percent9.2,
				sum(CAPRI_BANKING)/count(CAPRI_BANKING) as BANKING_TABLE format=percent9.2,
				sum(CAPRI_BUR_PROFILE_PINPOINT)/count(CAPRI_BUR_PROFILE_PINPOINT) as BUR_PROFILE_PINPOINT_TABLE format=percent9.2,			
				sum(CAPRI_BUR_PROFILE_TRANSUNION_AGG)/count(CAPRI_BUR_PROFILE_TRANSUNION_AGG) as BUR_PROFILE_TU_AGG_TABLE format=percent9.2,
				sum(CAPRI_BUR_PROFILE_TRANSUNION_BCC)/count(CAPRI_BUR_PROFILE_TRANSUNION_BCC) as BUR_PROFILE_TU_BCC_TABLE format=percent9.2,
				sum(capri_behavioural_score)/count(capri_behavioural_score) as Behavioural_score_TABLE format=percent9.2,
				sum(capri_scoring_results)/count(capri_scoring_results) as Scoring_results_TABLE format=percent9.2,
				sum(offer)/count(offer) as Loan_application_TABLE format=percent9.2
			from intables
			group by applicationdate;
		quit;


		proc sql;
		create table V622_intables_IA as
			select 	applicationdate, weekday,scorecardversion,
				count(*) AS Volume, 
/*				sum(CAPRI_APPLICANT)/count(CAPRI_APPLICANT) as APPLICANT_TABLE format=percent9.2,*/
				sum(CAPRI_EMPLOYMENT)/count(CAPRI_EMPLOYMENT) as EMPLOYMENT_TABLE format=percent9.2,
				sum(CAPRI_AFFORDABILITY)/count(CAPRI_AFFORDABILITY) as AFFORDABILITY_TABLE format=percent9.2,
				sum(CAPRI_BANKING)/count(CAPRI_BANKING) as BANKING_TABLE format=percent9.2,
				sum(CAPRI_BUR_PROFILE_PINPOINT)/count(CAPRI_BUR_PROFILE_PINPOINT) as BUR_PROFILE_PINPOINT_TABLE format=percent9.2,			
				sum(CAPRI_BUR_PROFILE_TRANSUNION_AGG)/count(CAPRI_BUR_PROFILE_TRANSUNION_AGG) as BUR_PROFILE_TU_AGG_TABLE format=percent9.2,
				sum(CAPRI_BUR_PROFILE_TRANSUNION_BCC)/count(CAPRI_BUR_PROFILE_TRANSUNION_BCC) as BUR_PROFILE_TU_BCC_TABLE format=percent9.2,
				sum(capri_behavioural_score)/count(capri_behavioural_score) as Behavioural_score_TABLE format=percent9.2,
				sum(capri_scoring_results)/count(capri_scoring_results) as Scoring_results_TABLE format=percent9.2,
				sum(offer)/count(offer) as Loan_application_TABLE format=percent9.2,
				'IA_flag' as IA
			from intables
			where sessionid ne ''
			group by applicationdate;
		quit;


		data batchtes.V622_intables;
			set scoring.V622_intables V622_intables V622_intables_IA;
			format appdate yymmdds10.;
			if applicationdate >= &lstmnth; 
			if Volume not in (12424);
			appdate = input(applicationdate,yymmdd10.);
		run;


		proc sort data=batchtes.V622_intables nodupkey out=batchtes.V622_intables;
		by applicationdate IA;
		run;

		libname scoring odbc dsn=Dev_DDCr schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

		proc sql;

			drop table scoring.V622_intables;

		    create table scoring.V622_intables like batchtes.V622_intables;

		    insert into scoring.V622_intables (bulkload=Yes)
		    select *
		    from batchtes.V622_intables;
		quit;

		
		%include "\\MPWSAS64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Daily Monitoring2\Report_inclWeekends.sas";
		%include "\\MPWSAS64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Daily Monitoring2\Report_exclWeekends.sas";
		%include "\\MPWSAS64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Daily Monitoring2\Report_Dashboard.sas";
		%include "\\MPWSAS64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Daily Monitoring2\TU_PSI.sas";
		%include "\\MPWSAS64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Daily Monitoring2\CS_PSI.sas";

	%end;
%mend;
%dailyMonitoring;

