/* Testing the GitLab automated pull schedule */

options compress = yes;
options compress = on;
options mprint mlogic symbolgen;
options nomprint nomlogic nosymbolgen; 
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
libname dadi odbc dsn=Prd_DaDi schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;


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
	

		/*Apply TU and CS scoring*/
		%include "\\mpwsas65\Process_Automation\Codes\Git_Repositories\Core Credit Risk Models - Monitoring\Macros\Scoring.sas";
		%scoring(inDataset=rawdata5, outDataset=CS_V570_score,Path=\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V570 Compuscan,scored_name =CS_V570, modelType =C);
		%scoring(inDataset=rawdata5, outDataset=TU_V580_score,Path=\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU,scored_name =TU_V580, modelType =T);
		
		/*TU_SCORED*/


		data TU_V580_Scored;
			set TU_V580_Score scoring.TU_V580_Scored;
			if applicationdate >= &lstmnth; ;
		run;

		data TU_scored;
			set TU_V580_Score scoring.TU_scored;
			if applicationdate >= &lstmnth; ;
		run;

		proc sql;
		libname scoring odbc dsn=Dev_DDCr schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

   			drop table scoring.TU_V580_Scored;
   			drop table scoring.TU_scored;

		    create table scoring.TU_V580_Scored like TU_V580_Scored;
		    create table scoring.TU_scored like TU_scored;

		    insert into scoring.TU_V580_Scored (bulkload=Yes)
		    select *
		    from TU_V580_Scored;
		    insert into scoring.TU_scored (bulkload=Yes)
		    select *
		    from TU_scored;
		quit;
		/**/

		/*CS_SCORED*/


		data CS_V570_scored;
			set scoring.CS_V570_scored CS_V570_score;
			if applicationdate >= &lstmnth; ;
		run;

		data CS_scored;
			set scoring.CS_scored CS_V570_score;
			if applicationdate >= &lstmnth; 
		run;

		proc sql;
		libname scoring odbc dsn=Dev_DDCr schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

			drop table scoring.CS_V570_scored;
			drop table scoring.CS_scored;

		    create table scoring.CS_V570_scored like CS_V570_scored;
		    create table scoring.CS_scored like CS_scored;

		    insert into scoring.CS_V570_scored (bulkload=Yes)
		    select *
		    from CS_V570_scored;
		    insert into scoring.CS_scored (bulkload=Yes)
		    select *
		    from CS_scored;
		quit;
		/**/
		
		proc sql;
			create table rawdata6 as
			select c.TU_V580_prob,  b.CS_V570_prob, a.*
			from rawdata5 a
			left join scoring.CS_scored b
			on a.uniqueid = b.uniqueid
			left join scoring.TU_scored c
			on a.uniqueid = c.uniqueid;
		quit;
		/**/

		proc sort data = rawdata6; by tranappnumber descending uniqueid; run;
		proc sort data = rawdata6 nodupkey dupout=dups  out= rawdata6; by tranappnumber; run;


		/*Apply V655 and IA Scoring*/
		%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\ApplyV655.sas";
		%applyV655(inDataset=rawdata6, outDataset=rawdata_V655);

		%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\ApplyV658.sas";
		%applyV658(indataset = rawdata_V655, prob_name = V655, score_name = overallscore, outDataset=rawdata_V658);

	
		/*Join V655 and IA scores*/
		proc sql;
			create table rawdata7 as
			select e.V655_Riskgroup, g.V658_Riskgroup,
			e.V650, e.V651, e.V652, e.V653, e.V654, e.V655, g.V658, e.EmployerVolumeIndexGroup_V2, e.SubSectorIndexGroup_v2 
			, a.*
			from rawdata6 a
			left join rawdata_V655 as e
			on a.uniqueid = e.uniqueid
			left join rawdata_V658 as g
			on a.uniqueid = g.uniqueid;
		quit;

		/*Calculate final score*/
		data all_scores1;
			set rawdata7;
			V655_finalriskscore =  1000-(V655*1000);
			V6_finalriskscore_IA =  1000-(V658*1000);
			V6_RiskGroup = V655_RiskGroup;
			V6_RiskGroup_IA = V658_RiskGroup;
		run;
		/**/

		
		/*Scorecard Distribution*/
		data scorecardDistribution;
			set scoring.scorecardDistribution (keep= weekday applicationdate uniqueid tranappnumber scorecardversion appdate)
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
			if maxscorecardversion = 'V655' then do;
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

			if  round(V658_prod ,0.01) - round(input(V658,best16.),0.01) = 0 then V658_Match = 1;
		    else V658_Match =0;

			if maxscorecardversion ne 'V4';
			if maxscorecardversion = 'V655' then V6_match = V655_match;
			if maxscorecardversion = 'V655' then V6_match_IA = V658_match;


			if  round(input(V6_RiskGroup_prod,best16.) ,0.01) - round(V6_Riskgroup,0.01) = 0 then RG_Match = 1;
		    else RG_Match =0;
			if  round(V6_RiskGroup_IA_prod ,0.01) - round(V6_Riskgroup_IA,0.01) = 0 then RG_Match_IA = 1;
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
			set all_scores4 scoring.all_scores;
			format appdate2 yymmdds10.;
			if applicationdate >= &lstmnth; 
			if v6_match ne .;
			appdate2 = appdate;
			if sessionid ne '' then IA = 'IA_flag';
			if STRATEGY11RANDOMNUMBER >= 0 and STRATEGY11RANDOMNUMBER < 0.2 then STRATEGY = 'CHP';
		    if STRATEGY11RANDOMNUMBER >= 0.2 and STRATEGY11RANDOMNUMBER < 1 then STRATEGY = 'CHL';
		    if STRATEGY12RANDOMNUMBER >= 0 and STRATEGY12RANDOMNUMBER < 0.2 then OFFER_STRATEGY = 'CHP';
		    if STRATEGY12RANDOMNUMBER >= 0.2 and STRATEGY12RANDOMNUMBER < 1 then OFFER_STRATEGY = 'CHL';

		run;

		proc sort data = all_scores5; by tranappnumber descending uniqueid; run;
		proc sort data = all_scores5 nodupkey out= all_scores6; by tranappnumber; run;


		proc sql;
		create table V6_vs_IA as
			select distinct	V6_Riskgroup, OFFER_STRATEGY, STRATEGY,
				count(*) AS Volume, 'V655' as Population 
			from all_scores6
			where sessionid ne '' and applicationdate >= &lstDate
			group by V6_Riskgroup;
		quit;

		proc sql;
		create table V6_vs_IA2 as
			select distinct	V658_Riskgroup as V6_Riskgroup, OFFER_STRATEGY, STRATEGY,
				count(*) AS Volume, 'V658' as Population 
			from all_scores6
			where sessionid ne '' and applicationdate >= &lstDate
			group by V658_Riskgroup;
		quit;

		data V6_vs_IA;
			set V6_vs_IA V6_vs_IA2;
		run;

		proc sort data = V6_vs_IA out= V6_vs_IA; by V6_Riskgroup; run;

		proc sort data = V6_vs_IA out= V6_vs_IA; by V6_Riskgroup; run;

		proc sql;
		libname scoring odbc dsn=Dev_DDCr schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

			drop table scoring.V6_vs_IA;
	
		    create table scoring.V6_vs_IA like V6_vs_IA;

		    insert into scoring.V6_vs_IA (bulkload=Yes)
		    select *
		    from V6_vs_IA;
		quit;


		proc sql;
		libname scoring odbc dsn=Dev_DDCr schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

			drop table scoring.all_scores;
	
		    create table scoring.all_scores like all_scores6;

		    insert into scoring.all_scores (bulkload=Yes)
		    select *
		    from all_scores6;
		quit;

		data batchtes.all_scores;
			set scoring.all_scores;
			if sessionid ne '' then IA = 'IA_flag';
		run;
		
		proc sql;
		create table V622_intables as
			select 	applicationdate, weekday,
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
			where applicationdate >= &lstdate
			group by applicationdate;
		quit;


		proc sql;
		create table V622_intables_IA as
			select 	applicationdate, weekday,
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

