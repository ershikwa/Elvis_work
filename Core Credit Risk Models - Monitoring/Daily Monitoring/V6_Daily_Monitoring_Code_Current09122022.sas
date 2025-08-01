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
libname scoring odbc dsn=Cre_Scor schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
libname dadi odbc dsn=Prd_DaDi schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;


data _null_;
	call symput('lstmnth',cats("'",put(intnx('day',today(),-30),yymmddd10.),"'"));
run;

proc sql stimer;
	connect to ODBC (dsn=etlscratch);
	create table lastRun as 
	select * from connection to odbc 
	(	
		SELECT (datediff(day, max(applicationdate), getdate()) -  1) as days
		FROM cred_scoring.dbo.all_scores
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
		%GetData(table=DEV_DataDistillery_General.dbo.V6DailyData, outDataset=rawdata5, dsn=DEV_DDGe);
	

		/*Apply TU and CS scoring*/
		%include "\\mpwsas65\Process_Automation\Codes\Git_Repositories\Core Credit Risk Models - Monitoring\Macros\Scoring.sas";
		%scoring(inDataset=rawdata5, outDataset=TU_V570_score,Path=\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\TU Models\v570,scored_name =TU_V570, modelType =T);
		%scoring(inDataset=rawdata5, outDataset=CS_V560_score,Path=\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\Compuscan Models\V560,scored_name =CS_V560, modelType =C);
		%scoring(inDataset=rawdata5, outDataset=CS_V570_score,Path=\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V570 Compuscan,scored_name =CS_V570, modelType =C);
		%scoring(inDataset=rawdata5, outDataset=TU_V580_score,Path=\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU,scored_name =TU_V580, modelType =T);
		
		/*TU_SCORED*/
		proc sql;
		    create table TU_scored as
		    select * from TU_V570_Score a
		    left join TU_V580_score b
		    on a.uniqueid = b.uniqueid;
		quit;


		data TU_V570_Scored;
			set TU_V570_Score scoring.TU_V570_Scored;
			if applicationdate >= &lstmnth; ;
		run;


		data TU_V580_Scored;
			set TU_V580_Score scoring.TU_V580_Scored;
			if applicationdate >= &lstmnth; ;
		run;

		data TU_scored;
			set TU_scored scoring.TU_scored;
			if applicationdate >= &lstmnth; ;
		run;

		proc sql;
   			drop table scoring.TU_V570_Scored;
   			drop table scoring.TU_V580_Scored;
   			drop table scoring.TU_scored;

		    create table scoring.TU_V570_Scored like TU_V570_Scored;
		    create table scoring.TU_V580_Scored like TU_V580_Scored;
		    create table scoring.TU_scored like TU_scored;

		    insert into scoring.TU_V570_Scored (bulkload=Yes)
		    select *
		    from TU_V570_Scored;
		    insert into scoring.TU_V580_Scored (bulkload=Yes)
		    select *
		    from TU_V580_Scored;
		    insert into scoring.TU_scored (bulkload=Yes)
		    select *
		    from TU_scored;
		quit;
		/**/

		/*CS_SCORED*/
		proc sql;
		    create table CS_scored as
		    select * from CS_V560_Score a
		    left join CS_V570_score b
		    on a.uniqueid = b.uniqueid;
		quit;

		data CS_V560_Scored;
			set scoring.CS_V560_Scored CS_V560_Score;
			if applicationdate >= &lstmnth; ;
		run;

		data CS_V570_scored;
			set scoring.CS_V570_scored CS_V570_score;
			if applicationdate >= &lstmnth; ;
		run;

		data CS_scored;
			set scoring.CS_scored CS_scored;
			if applicationdate >= &lstmnth; 
		run;

		proc sql;
			drop table scoring.CS_V560_Scored;
			drop table scoring.CS_V570_scored;
			drop table scoring.CS_scored;

		    create table scoring.CS_V560_Scored like CS_V560_Scored;
		    create table scoring.CS_V570_scored like CS_V570_scored;
		    create table scoring.CS_scored like CS_scored;

		    insert into scoring.CS_V560_Scored (bulkload=Yes)
		    select *
		    from CS_V560_Scored;
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
			select c.TU_V570_prob, c.TU_V580_prob, b.CS_V560_prob, b.CS_V570_prob, a.*
			from rawdata5 a
			left join CS_scored b
			on a.uniqueid = b.uniqueid
			left join TU_scored c
			on a.uniqueid = c.uniqueid;
		quit;
		/**/

		proc sort data = rawdata6; by tranappnumber descending uniqueid; run;
		proc sort data = rawdata6 nodupkey dupout=dups  out= rawdata6; by tranappnumber; run;


		/*Apply V622, V636 and V645 scoring*/
		%include "\\mpwsas65\Process_Automation\Codes\Git_Repositories\Core Credit Risk Models - Monitoring\Macros\ApplyV622.sas";
		%applyV622(inDataset=rawdata6, outDataset=rawdata_V622);
		%include "\\mpwsas65\Process_Automation\Codes\Git_Repositories\Core Credit Risk Models - Monitoring\Macros\ApplyV636.sas";
		%applyV636(inDataset=rawdata6, outDataset=rawdata_V636);
		%include "\\mpwsas65\Process_Automation\Codes\Git_Repositories\Core Credit Risk Models - Monitoring\Macros\ApplyV645.sas";
		%applyV645(inDataset=rawdata6, outDataset=rawdata_V645);
		%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\ApplyV655.sas";
		%applyV655(inDataset=rawdata6, outDataset=rawdata_V655);

		data rawdata_V645;
			set rawdata_V645;
			V645_B10 = V645*1.1;
		run;

		data rawdata_V655;
			set rawdata_V655;
			V655_B20 = V655*1.2;
		run;

		%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\ApplyV648.sas";
		%applyV648(indataset = rawdata_V645,prob_name = V645_B10, score_name = overallscore, outdataset = rawdata_V648);
		%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\ApplyV658.sas";
		%applyV658(indataset = rawdata_V655, prob_name = V655_B20, score_name = overallscore, outDataset=rawdata_V658);

	
		/*Join V622, V636, V645 and V655*/
		proc sql;
			create table rawdata7 as
			select b.V620, b.V621, b.V622,
			c.V630, c.V631, c.V632, c.V633, c.V634, c.V635, c.V636,
			d.V640, d.V641, d.V642, d.V643, d.V644, d.V645, f.V645_B10, f.V648, d.EmployerVolumeIndexGroup_V2, d.SubSectorIndexGroup_v2,
			e.V650, e.V651, e.V652, e.V653, e.V654, e.V655, g.V655_B20, g.V658, e.EmployerVolumeIndexGroup_V2, e.SubSectorIndexGroup_v2 
			, a.*
			from rawdata6 a
			left join rawdata_V622 as b
			on a.uniqueid = b.uniqueid
			left join rawdata_V636 as c
			on a.uniqueid = c.uniqueid
			left join rawdata_V645 as d
			on a.uniqueid = d.uniqueid
			left join rawdata_V655 as e
			on a.uniqueid = e.uniqueid
			left join rawdata_V648 as f
			on a.uniqueid = f.uniqueid
			left join rawdata_V658 as g
			on a.uniqueid = g.uniqueid;
		quit;

		/*Calculate final score*/
		data all_scores1;
			set rawdata7;
			if maxscorecardversion = "V622" then V6_finalriskscore =  1000-(V622*1000);
			if maxscorecardversion = "V636" then V6_finalriskscore =  1000-(V636*1000);
			if maxscorecardversion = "V645" then V6_finalriskscore =  1000-(V645*1000);
			if maxscorecardversion = "V655" then V6_finalriskscore =  1000-(V655*1000);
			if maxscorecardversion = "V645" then V6_finalriskscore_IA =  1000-(V648*1000);
			if maxscorecardversion = "V655" then V6_finalriskscore_IA =  1000-(V658*1000);
		run;
		/**/


		/*Assign risk groups*/
		data all_scores2;
			set all_scores1;
			if maxscorecardversion = 'V645' then do;
				if Min(ThinFileIndicator,TU_Thin) = 0 then do;
				if V6_finalriskscore >= 932.242611756651      then V6_RiskGroup = 50;
				else if V6_finalriskscore >= 912.452480990053 then V6_RiskGroup = 51;
				else if V6_finalriskscore >= 878.956333489911 then V6_RiskGroup = 52;
				else if V6_finalriskscore >= 841.833690856176 then V6_RiskGroup = 53;
				else if V6_finalriskscore >= 811.989999894282 then V6_RiskGroup = 54;
				else if V6_finalriskscore >= 790.349339106057 then V6_RiskGroup = 55;
				else if V6_finalriskscore >= 778.068960766859 then V6_RiskGroup = 56;
				else if V6_finalriskscore >= 758.6444629 	  then V6_RiskGroup = 57;
				else if V6_finalriskscore >= 746.152798684895 then V6_RiskGroup = 58;
				else if V6_finalriskscore >= 732.001226390991 then V6_RiskGroup = 59;
				else if V6_finalriskscore >= 708.169317621721 then V6_RiskGroup = 60;
				else if V6_finalriskscore >= 690.87118531475  then V6_RiskGroup = 61;
				else if V6_finalriskscore >= 675.057720140646 then V6_RiskGroup = 62;
				else if V6_finalriskscore >= 530 			  then V6_RiskGroup = 63;
				else if V6_finalriskscore >= 529			  then V6_RiskGroup = 64;
				else if V6_finalriskscore >= 527 			  then V6_RiskGroup = 65;
				else if V6_finalriskscore >= 500 			  then V6_RiskGroup = 66;
				else if V6_finalriskscore > 0    			  then V6_RiskGroup = 67;
				end;

				else if Min(ThinFileIndicator,TU_Thin) = 1 then do;
					if V6_finalriskscore >=      828.199869458644 then V6_RiskGroup = 68;
					else if V6_finalriskscore >= 762.179100967216 then V6_RiskGroup = 69;
					else if V6_finalriskscore >= 721.281349457995 then V6_RiskGroup = 70;
					else if V6_finalriskscore > 0 				  then V6_RiskGroup = 71;
				end;
			end;

			if maxscorecardversion = 'V645' then do;
				if Min(ThinFileIndicator,TU_Thin) = 0 then do;
					if V6_finalriskscore_IA >= 932.242611756651      then V6_RiskGroup_IA = 50;
					else if V6_finalriskscore_IA >= 912.452480990053 then V6_RiskGroup_IA = 51;
					else if V6_finalriskscore_IA >= 878.956333489911 then V6_RiskGroup_IA = 52;
					else if V6_finalriskscore_IA >= 841.833690856176 then V6_RiskGroup_IA = 53;
					else if V6_finalriskscore_IA >= 811.989999894282 then V6_RiskGroup_IA = 54;
					else if V6_finalriskscore_IA >= 790.349339106057 then V6_RiskGroup_IA = 55;
					else if V6_finalriskscore_IA >= 778.068960766859 then V6_RiskGroup_IA = 56;
					else if V6_finalriskscore_IA >= 758.6444629 	    then V6_RiskGroup_IA = 57;
					else if V6_finalriskscore_IA >= 746.152798684895 then V6_RiskGroup_IA = 58;
					else if V6_finalriskscore_IA >= 732.001226390991 then V6_RiskGroup_IA = 59;
					else if V6_finalriskscore_IA >= 708.169317621721 then V6_RiskGroup_IA = 60;
					else if V6_finalriskscore_IA >= 690.87118531475  then V6_RiskGroup_IA = 61;
					else if V6_finalriskscore_IA >= 675.057720140646 then V6_RiskGroup_IA = 62;
					else if V6_finalriskscore_IA >= 530 			    then V6_RiskGroup_IA = 63;
					else if V6_finalriskscore_IA >= 529			    then V6_RiskGroup_IA = 64;
					else if V6_finalriskscore_IA >= 527 			    then V6_RiskGroup_IA = 65;
					else if V6_finalriskscore_IA >= 500 			    then V6_RiskGroup_IA = 66;
					else if V6_finalriskscore_IA > 0    			    then V6_RiskGroup_IA = 67;
				end;

				else if Min(ThinFileIndicator,TU_Thin) = 1 then do;
					if V6_finalriskscore_IA >=      828.199869458644 then V6_RiskGroup_IA = 68;
					else if V6_finalriskscore_IA >= 762.179100967216 then V6_RiskGroup_IA = 69;
					else if V6_finalriskscore_IA >= 721.281349457995 then V6_RiskGroup_IA = 70;
					else if V6_finalriskscore_IA > 0 				then V6_RiskGroup_IA = 71;
				end;
			end;



			if maxscorecardversion = 'V655' then do;
				if Min(ThinFileIndicator,TU_Thin) = 0 then do;
				if V6_finalriskscore >= 929.70414109 then V6_RiskGroup = 50;
				else if V6_finalriskscore >= 909.59116387 then V6_RiskGroup = 51;
				else if V6_finalriskscore >= 872.15152879 then V6_RiskGroup = 52;
				else if V6_finalriskscore >= 827.34047213 then V6_RiskGroup = 53;
				else if V6_finalriskscore >= 790.59177669 then V6_RiskGroup = 54;
				else if V6_finalriskscore >= 764.70857459 then V6_RiskGroup = 55;
				else if V6_finalriskscore >= 750.21402079 then V6_RiskGroup = 56;
				else if V6_finalriskscore >= 727.94382999 then V6_RiskGroup = 57;
				else if V6_finalriskscore >= 714.25140059 then V6_RiskGroup = 58;
				else if V6_finalriskscore >= 699.11965741 then V6_RiskGroup = 59;
				else if V6_finalriskscore >= 676.02770676 then V6_RiskGroup = 60;
				else if V6_finalriskscore >= 660.32199365 then V6_RiskGroup = 61;
				else if V6_finalriskscore >= 646.97357763 then V6_RiskGroup = 62;
				else if V6_finalriskscore >= 620 then V6_RiskGroup = 63;
				else if V6_finalriskscore >= 543.49446576 then V6_RiskGroup = 64;
				else if V6_finalriskscore >= 542.20967069 then V6_RiskGroup = 65;
				else if V6_finalriskscore >= 525.71483413 then V6_RiskGroup = 66;
				else if V6_finalriskscore >= 0 then V6_RiskGroup = 67;
				end;

				else if Min(ThinFileIndicator,TU_Thin) = 1 then do;
					if V6_finalriskscore >= 771.77225514 then V6_RiskGroup = 68;
					else if V6_finalriskscore >= 714.29321139 then V6_RiskGroup = 69;
					else if V6_finalriskscore >= 681.51627236 then V6_RiskGroup = 70;
					else if V6_finalriskscore >= 0 then V6_RiskGroup = 71;
				end;
			end;

			if maxscorecardversion = 'V655' then do;
				if Min(ThinFileIndicator,TU_Thin) = 0 then do;
					if V6_finalriskscore_IA >= 929.70414109 then V6_RiskGroup_IA = 50;
					else if V6_finalriskscore_IA >= 909.59116387 then V6_RiskGroup_IA = 51;
					else if V6_finalriskscore_IA >= 872.15152879 then V6_RiskGroup_IA = 52;
					else if V6_finalriskscore_IA >= 827.34047213 then V6_RiskGroup_IA = 53;
					else if V6_finalriskscore_IA >= 790.59177669 then V6_RiskGroup_IA = 54;
					else if V6_finalriskscore_IA >= 764.70857459 then V6_RiskGroup_IA = 55;
					else if V6_finalriskscore_IA >= 750.21402079 then V6_RiskGroup_IA = 56;
					else if V6_finalriskscore_IA >= 727.94382999 then V6_RiskGroup_IA = 57;
					else if V6_finalriskscore_IA >= 714.25140059 then V6_RiskGroup_IA = 58;
					else if V6_finalriskscore_IA >= 699.11965741 then V6_RiskGroup_IA = 59;
					else if V6_finalriskscore_IA >= 676.02770676 then V6_RiskGroup_IA = 60;
					else if V6_finalriskscore_IA >= 660.32199365 then V6_RiskGroup_IA = 61;
					else if V6_finalriskscore_IA >= 646.97357763 then V6_RiskGroup_IA = 62;
					else if V6_finalriskscore_IA >= 620 then V6_RiskGroup_IA = 63;
					else if V6_finalriskscore_IA >= 543.49446576 then V6_RiskGroup_IA = 64;
					else if V6_finalriskscore_IA >= 542.20967069 then V6_RiskGroup_IA = 65;
					else if V6_finalriskscore_IA >= 525.71483413 then V6_RiskGroup_IA = 66;
					else if V6_finalriskscore_IA >= 0 then V6_RiskGroup_IA = 67;
				end;

				else if Min(ThinFileIndicator,TU_Thin) = 1 then do;
					if V6_finalriskscore_IA >= 771.77225514 then V6_RiskGroup_IA = 68;
					else if V6_finalriskscore_IA >= 714.29321139 then V6_RiskGroup_IA = 69;
					else if V6_finalriskscore_IA >= 681.51627236 then V6_RiskGroup_IA = 70;
					else if V6_finalriskscore_IA >= 0 then V6_RiskGroup_IA = 71;
				end;
			end;

		run;
		/**/

		
		/*Scorecard Distribution*/
		data scorecardDistribution;
			set scoring.scorecardDistribution (keep=applicationdate uniqueid tranappnumber scorecardversion) all_scores2 (keep= applicationdate uniqueid tranappnumber scorecardversion);
			where applicationdate >= &lstmnth;
		run;

		proc sort data = scorecardDistribution; by tranappnumber descending uniqueid; run;
		proc sort data = scorecardDistribution nodupkey dupout=dups  out= scorecardDistribution; by tranappnumber; run;
 
		proc sql;

			drop table scoring.scorecardDistribution;
	
		    create table scoring.scorecardDistribution like scorecardDistribution;

		    insert into scoring.scorecardDistribution (bulkload=Yes)
		    select *
		    from scorecardDistribution;
		quit;
		/**/

		data all_scores2;
			set all_scores2;
			V6_finalriskscore_IA_prod = input(IAfinalriskscore,best16.);
			if maxscorecardversion = 'V645' then do;
				if Min(ThinFileIndicator,TU_Thin) = 0 then do;
					if V6_finalriskscore_IA_prod >= 932.242611756651      then V6_RiskGroup_IA_prod = 50;
					else if V6_finalriskscore_IA_prod >= 912.452480990053 then V6_RiskGroup_IA_prod = 51;
					else if V6_finalriskscore_IA_prod >= 878.956333489911 then V6_RiskGroup_IA_prod = 52;
					else if V6_finalriskscore_IA_prod >= 841.833690856176 then V6_RiskGroup_IA_prod = 53;
					else if V6_finalriskscore_IA_prod >= 811.989999894282 then V6_RiskGroup_IA_prod = 54;
					else if V6_finalriskscore_IA_prod >= 790.349339106057 then V6_RiskGroup_IA_prod = 55;
					else if V6_finalriskscore_IA_prod >= 778.068960766859 then V6_RiskGroup_IA_prod = 56;
					else if V6_finalriskscore_IA_prod >= 758.6444629 	  then V6_RiskGroup_IA_prod = 57;
					else if V6_finalriskscore_IA_prod >= 746.152798684895 then V6_RiskGroup_IA_prod = 58;
					else if V6_finalriskscore_IA_prod >= 732.001226390991 then V6_RiskGroup_IA_prod = 59;
					else if V6_finalriskscore_IA_prod >= 708.169317621721 then V6_RiskGroup_IA_prod = 60;
					else if V6_finalriskscore_IA_prod >= 690.87118531475  then V6_RiskGroup_IA_prod = 61;
					else if V6_finalriskscore_IA_prod >= 675.057720140646 then V6_RiskGroup_IA_prod = 62;
					else if V6_finalriskscore_IA_prod >= 530 			  then V6_RiskGroup_IA_prod = 63;
					else if V6_finalriskscore_IA_prod >= 529			  then V6_RiskGroup_IA_prod = 64;
					else if V6_finalriskscore_IA_prod >= 527 			  then V6_RiskGroup_IA_prod = 65;
					else if V6_finalriskscore_IA_prod >= 500 			  then V6_RiskGroup_IA_prod = 66;
					else if V6_finalriskscore_IA_prod > 0    			  then V6_RiskGroup_IA_prod = 67;
				end;

				else if Min(ThinFileIndicator,TU_Thin) = 1 then do;
					if V6_finalriskscore_IA_prod >=      828.199869458644 then V6_RiskGroup_IA_prod = 68;
					else if V6_finalriskscore_IA_prod >= 762.179100967216 then V6_RiskGroup_IA_prod = 69;
					else if V6_finalriskscore_IA_prod >= 721.281349457995 then V6_RiskGroup_IA_prod = 70;
					else if V6_finalriskscore_IA_prod > 0 				  then V6_RiskGroup_IA_prod = 71;
				end;
			end;

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
			if  round(TU_V570_prod,0.0001) - round(input(TU_V570_prob,best16.),0.0001) = 0 then TU_V570_match = 1;
		    else TU_V570_match =0;

			if  round(TU_V580_prod,0.0001) - round(input(TU_V580_prob,best16.),0.0001) = 0 then TU_V580_match = 1;
		    else TU_V580_match =0;

			if  round(CS_V560_prod,0.0001) - round(input(CS_V560_prob,best16.),0.0001) = 0 then CS_V560_match = 1;
		    else CS_V560_match =0;

			if  round(CS_V570_prod,0.0001) - round(input(CS_V570_prob,best16.),0.0001) = 0 then CS_V570_match = 1;
		    else CS_V570_match =0;

			if  round(V620_prod,0.0001) - round(input(V620,best16.),0.0001) = 0 then V620_Match = 1;
		    else V620_Match =0;

			if  round(V621_prod ,0.0001) - round(input(V621,best16.),0.0001) = 0 then V621_Match = 1;
		    else V621_Match =0;

			if  round(V622_prod ,0.0001) - round(input(V622,best16.),0.0001) = 0 then V622_Match = 1;
		    else V622_Match =0;
			
			if  round(V631_prod ,0.0001) - round(input(V631,best16.),0.0001) = 0 then V631_Match = 1;
		    else V631_Match =0;

			if  round(V632_prod ,0.0001) - round(input(V632,best16.),0.0001) = 0 then V632_Match = 1;
		    else V632_Match =0;

			if  round(V633_prod ,0.0001) - round(input(V633,best16.),0.0001) = 0 then V633_Match = 1;
		    else V633_Match =0;

			if  round(V634_prod ,0.0001) - round(input(V634,best16.),0.0001) = 0 then V634_Match = 1;
		    else V634_Match =0;

			if  round(V635_prod ,0.0001) - round(input(V635,best16.),0.0001) = 0 then V635_Match = 1;
		    else V635_Match =0;

			if  round(V636_prod ,0.0001) - round(input(V636,best16.),0.0001) = 0 then V636_Match = 1;
		    else V636_Match =0;
			
			if  round(V640_prod ,0.0001) - round(input(V640,best16.),0.0001) = 0 then V640_Match = 1;
		    else V640_Match =0;

			if  round(V641_prod ,0.0001) - round(input(V641,best16.),0.0001) = 0 then V641_Match = 1;
		    else V641_Match =0;

			if  round(V642_prod ,0.0001) - round(input(V642,best16.),0.0001) = 0 then V642_Match = 1;
		    else V642_Match =0;

			if  round(V643_prod ,0.0001) - round(input(V643,best16.),0.0001) = 0 then V643_Match = 1;
		    else V643_Match =0;

			if  round(V644_prod ,0.0001) - round(input(V644,best16.),0.0001) = 0 then V644_Match = 1;
		    else V644_Match =0;

			if  round(V645_prod ,0.0001) - round(input(V645,best16.),0.0001) = 0 then V645_Match = 1;
		    else V645_Match =0;

			if  round(V648_prod ,0.01) - round(input(V648,best16.),0.01) = 0 then V648_Match = 1;
		    else V648_Match =0;


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
			if maxscorecardversion = 'V622' then V6_match = V622_match;
			if maxscorecardversion = 'V636' then V6_match = V636_match;
			if maxscorecardversion = 'V645' then V6_match = V645_match;
			if maxscorecardversion = 'V655' then V6_match = V655_match;
			if maxscorecardversion = 'V645' then V6_match_IA = V648_match;
			if maxscorecardversion = 'V655' then V6_match_IA = V658_match;


			if  round(input(V6_RiskGroup_prod,best16.) ,0.01) - round(V6_Riskgroup,0.01) = 0 then RG_Match = 1;
		    else RG_Match =0;
			if  round(input(V6_RiskGroup_IA_prod,best16.) ,0.01) - round(V6_Riskgroup_IA,0.01) = 0 then RG_Match_IA = 1;
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
			set all_scores4 scoring.all_scores ;
			format appdate2 yymmdds10.;
			if applicationdate >= &lstmnth; 
			if v6_match ne .;
			appdate2 = appdate;
		run;

		proc sort data = all_scores5; by tranappnumber descending uniqueid; run;
		proc sort data = all_scores5 nodupkey out= all_scores6; by tranappnumber; run;


		proc sql;

			drop table scoring.all_scores;
	
		    create table scoring.all_scores like all_scores6;

		    insert into scoring.all_scores (bulkload=Yes)
		    select *
		    from all_scores6;
		quit;

		data batchtes.all_scores;
			set all_scores6;
		run;
		
		proc sql;
		create table V622_intables as
			select 	applicationdate, 
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

		data batchtes.V622_intables;
			set scoring.V622_intables V622_intables;
			if applicationdate >= &lstmnth; ;
			if Volume not in (12424);
		run;

		proc sort data=batchtes.V622_intables nodupkey out=batchtes.V622_intables;
		by applicationdate;
		run;


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
		%include "\\mpwsas65\Process_Automation\Codes\Git_Repositories\Core Credit Risk Models - Monitoring\Macros\TU_PSI.sas";
		%include "\\mpwsas65\Process_Automation\Codes\Git_Repositories\Core Credit Risk Models - Monitoring\Macros\CS_PSI.sas";

	%end;
%mend;
%dailyMonitoring;

