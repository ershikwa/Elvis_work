libname scoring odbc dsn=Cred_Scoring schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
	data all_scores;
	set scoring.all_scores;
	V6_Seg = cats(comp_seg,Tu_seg);
	V655_finalriskscore =  1000-(V655*1000);
		if Min(ThinFileIndicator,TU_Thin) = 0 then do;
			if V655_finalriskscore >= 929.70414109 then V655_RiskGroup = 50;
			else if V655_finalriskscore >= 909.59116387 then V655_RiskGroup = 51;
			else if V655_finalriskscore >= 872.15152879 then V655_RiskGroup = 52;
			else if V655_finalriskscore >= 827.34047213 then V655_RiskGroup = 53;
			else if V655_finalriskscore >= 790.59177669 then V655_RiskGroup = 54;
			else if V655_finalriskscore >= 764.70857459 then V655_RiskGroup = 55;
			else if V655_finalriskscore >= 750.21402079 then V655_RiskGroup = 56;
			else if V655_finalriskscore >= 727.94382999 then V655_RiskGroup = 57;
			else if V655_finalriskscore >= 714.25140059 then V655_RiskGroup = 58;
			else if V655_finalriskscore >= 699.11965741 then V655_RiskGroup = 59;
			else if V655_finalriskscore >= 676.02770676 then V655_RiskGroup = 60;
			else if V655_finalriskscore >= 660.32199365 then V655_RiskGroup = 61;
			else if V655_finalriskscore >= 646.97357763 then V655_RiskGroup = 62;
			else if V655_finalriskscore >= 620 then V655_RiskGroup = 63;
			else if V655_finalriskscore >= 543.49446576 then V655_RiskGroup = 64;
			else if V655_finalriskscore >= 542.20967069 then V655_RiskGroup = 65;
			else if V655_finalriskscore >= 525.71483413 then V655_RiskGroup = 66;
			else if V655_finalriskscore >= 0 then V655_RiskGroup = 67;
		end;

		else if Min(ThinFileIndicator,TU_Thin) = 1 then do;
			if V655_finalriskscore >= 771.77225514 then V655_RiskGroup = 68;
			else if V655_finalriskscore >= 714.29321139 then V655_RiskGroup = 69;
			else if V655_finalriskscore >= 681.51627236 then V655_RiskGroup = 70;
			else if V655_finalriskscore >= 0 then V655_RiskGroup = 71;
	end;
	if maxscorecardversion = 'V655' then V6_RiskGroup = V655_RiskGroup;
	run;	

	proc sort data=all_scores;
		by applicationdate;
	run;

	data scorecardDistribution;
	set scoring.scorecardDistribution;
	run;
	
	proc sort data=scorecardDistribution;
		by applicationdate;
	run;

	proc freq data=scorecardDistribution;
		by applicationdate;
		tables SCORECARDVERSION / out = scorecarddist;
	run;

	%macro prepReportData();

		proc sort data= all_scores;
		by ApplicationDate;
		run;

		proc sql;
			create table matchrate0 as
			select ApplicationDate, count(uniqueid) as Volume, (sum(V6_Match)/count(uniqueid)) as Match_Rate format=percent9.2
			from  all_scores
			group by ApplicationDate;
		quit;

		proc sql;
			create table matchrate1 as
			select ApplicationDate, count(uniqueid) as Volume_V622, (sum(V6_Match)/count(uniqueid)) as Match_Rate_V622 format=percent9.2
			from  all_scores
			where maxSCORECARDVERSION = 'V622'
			group by ApplicationDate;
		quit;

		proc sql;
			create table matchrate2 as
			select ApplicationDate, count(uniqueid) as Volume_V636, (sum(V6_Match)/count(uniqueid)) as Match_Rate_V636 format=percent9.2
			from  all_scores
			where maxSCORECARDVERSION = 'V636'
			group by ApplicationDate;
		quit;

		proc sql;
			create table matchrate3 as
			select ApplicationDate, count(uniqueid) as Volume_V645, (sum(V6_Match)/count(uniqueid)) as Match_Rate_V645 format=percent9.2
			from  all_scores
			where maxSCORECARDVERSION = 'V645'
			group by ApplicationDate;
		quit;

		proc sql;
			create table matchrate4 as
			select ApplicationDate, count(uniqueid) as Volume_V655, (sum(V6_Match)/count(uniqueid)) as Match_Rate_V655 format=percent9.2
			from  all_scores
			where maxSCORECARDVERSION = 'V655'
			group by ApplicationDate;
		quit;


		proc sql;
		  create table batchtes.matchrate as
		  select *
	   	  from matchrate0 a left join matchrate1 b
		  on a.ApplicationDate = b.ApplicationDate
		  left join matchrate2 c
		  on a.ApplicationDate = c.ApplicationDate
		  left join matchrate3 d
		  on a.ApplicationDate = d.ApplicationDate
		  left join matchrate4 e
		  on a.ApplicationDate = e.ApplicationDate;
		quit;

		data matchrate;/**/
			set /*batchtes.*/matchrate;
		run;

		proc sql;
			select count(*) into: TotalCount from matchrate;
		quit;

		data _null_; 
			set matchrate;
			if _N_ = (&TotalCount - 1) then call symput('Volpastlast2', Volume);
		run;

		data _null_; 
			set matchrate;
			if _N_ = &TotalCount then call symput('Volpastlastday', Volume);
		run;

		%put &Volpastlastday;
		%put &Volpastlast2;
		%let VolChange = &Volpastlastday - &Volpastlast2;

		data matchratetwo;
			set matchrate; 
			VolChange = &Volpastlastday - &Volpastlast2;
			PercVolChange = (VolChange / &Volpastlast2);
		run;

		proc sql;
			drop table scoring.V622_Matchrate;

			create table scoring.V622_Matchrate like matchratetwo;

			insert into scoring.V622_Matchrate (Bulkload = Yes)
			select *
			from matchratetwo;
		quit;

		proc freq data = all_scores;
			tables v6_seg /norow nocol nocum nofreq out=segmentdist;
			by ApplicationDate;
		run;

		data segmentdist (rename=(ApplicationDate=Month));
			set segmentdist;
		run;

		proc freq data =  all_scores;
			tables V6_RiskGroup_prod /norow nocol nocum nofreq out=sysriskgroupdist;
			by ApplicationDate;
		run;

		data t1 t2 t3 t4;
			set all_scores;
			if maxSCORECARDVERSION = 'V622' then output t1;
			else if maxSCORECARDVERSION = 'V636' then output t2;
			else if maxSCORECARDVERSION = 'V645' then output t3;
			else if maxSCORECARDVERSION = 'V655' then output t4;
		run;

		proc freq data = t1;
			tables V6_RiskGroup_prod /norow nocol nocum nofreq out=sysriskgroupdist1;
			by ApplicationDate;
		run;

		proc freq data = t2;
			tables V6_RiskGroup_prod /norow nocol nocum nofreq out=sysriskgroupdist2;
			by ApplicationDate;
		run;

		proc freq data = t3;
			tables V6_RiskGroup_prod /norow nocol nocum nofreq out=sysriskgroupdist3;
			by ApplicationDate;
		run;

		proc freq data = t4;
			tables V6_RiskGroup_prod /norow nocol nocum nofreq out=sysriskgroupdist4;
			by ApplicationDate;
		run;

		proc freq data =  all_scores;
			tables channelcode /norow nocol nocum nofreq out=channelcodedist;
			by ApplicationDate;
		run;

		proc freq data =  all_scores;
			tables V6_RiskGroup /norow nocol nocum nofreq out=riskgroupdist;
			by ApplicationDate;
		run;

		proc freq data =  t1;
			tables V6_RiskGroup /norow nocol nocum nofreq out=riskgroupdist1;
			by ApplicationDate;
		run;

		proc freq data =  t2;
			tables V6_RiskGroup /norow nocol nocum nofreq out=riskgroupdist2;
			by ApplicationDate;
		run;

		proc freq data =  t3;
			tables V6_RiskGroup /norow nocol nocum nofreq out=riskgroupdist3;
			by ApplicationDate;
		run;

		proc freq data =  t4;
			tables V6_RiskGroup /norow nocol nocum nofreq out=riskgroupdist4;
			by ApplicationDate;
		run;

		proc freq data =  all_scores;
			tables INSTITUTIONCODE /norow nocol nocum nofreq out=bankdist;
			by ApplicationDate;
		run;
		
		proc freq data =  all_scores;
			tables tu_seg /norow nocol nocum nofreq out=tu_seg;
			by ApplicationDate;
		run;

		proc freq data =  all_scores;
			tables comp_seg /norow nocol nocum nofreq out=comp_seg;
			by ApplicationDate;
		run;
	%mend;

	%macro drawReportGraphs();
		Title "V6 Volumes and Match Rate";
		proc sgplot data=matchrate0;
			vbar ApplicationDate / response = Volume  NOSTATLABEL  BARWIDTH = 0.8 ;
			vline ApplicationDate / response = Match_Rate legendlabel="Match Rate"  y2axis markerattrs=(symbol=squarefilled) markers;
			yaxis label = 'Volume'; 
			xaxis label = "Date";
			y2axis label = 'Match Rate %' min = 0 max = 1 ;
		run;

		Title "V622 Volumes and Match Rate";
		proc sgplot data=matchrate1;
	    	vbar ApplicationDate / response = Volume_V622  NOSTATLABEL  BARWIDTH = 0.8 ;
			vline ApplicationDate / response = Match_Rate_V622 legendlabel="Match Rate"  y2axis markerattrs=(symbol=squarefilled) markers;
			yaxis label = 'Volume'; 
			xaxis label = "Date";
			y2axis label = 'Match Rate %' min = 0 max = 1 ;
		run;
		
		Title "V636 Volumes and Match Rate";
		proc sgplot data=matchrate2;
		    vbar ApplicationDate / response = Volume_V636  NOSTATLABEL  BARWIDTH = 0.8 ;
		    vline ApplicationDate / response = Match_Rate_V636 legendlabel="Match Rate"  y2axis markerattrs=(symbol=squarefilled) markers;
			yaxis label = 'Volume'; 
			xaxis label = "Date";
			y2axis label = 'Match Rate %' min = 0 max = 1 ;
		run;
		
		Title "V645 Volumes and Match Rate";
		proc sgplot data=matchrate3;
			vbar ApplicationDate / response = Volume_V645  NOSTATLABEL  BARWIDTH = 0.8 ;
			vline ApplicationDate / response = Match_Rate_V645 legendlabel="Match Rate"  y2axis markerattrs=(symbol=squarefilled) markers;
			yaxis label = 'Volume'; 
			xaxis label = "Date";
			y2axis label = 'Match Rate %' min = 0 max = 1 ;
		run;

		Title "V655 Volumes and Match Rate";
		proc sgplot data=matchrate4;
			vbar ApplicationDate / response = Volume_V655  NOSTATLABEL  BARWIDTH = 0.8 ;
			vline ApplicationDate / response = Match_Rate_V655 legendlabel="Match Rate"  y2axis markerattrs=(symbol=squarefilled) markers;
			yaxis label = 'Volume'; 
			xaxis label = "Date";
			y2axis label = 'Match Rate %' min = 0 max = 1 ;
		run;

		Proc print data=matchrate0 noobs;
			Title "Daily Match Rate";
		run;
		
		Proc print data=matchrate1 noobs;
			Title "Daily V622 Match Rate";
		run;
		
		Proc print data=matchrate2 noobs;
			Title "Daily V636 Match Rate";
		run;

		Proc print data=matchrate3 noobs;
			Title "Daily V645 Match Rate";
		run;

		Proc print data=matchrate4 noobs;
			Title "Daily V655 Match Rate";
		run;
		
		Title "V6 Data in Tables";
		proc sgplot data=scoring.V622_INTABLES ;
		    vbar ApplicationDate / response = Volume  NOSTATLABEL  BARWIDTH = 0.8 ;
		    vline ApplicationDate / response = AFFORDABILITY_TABLE legendlabel="Affordability"  y2axis markerattrs=(symbol=squarefilled) markers;
		    vline ApplicationDate / response = EMPLOYMENT_TABLE legendlabel="Employment"  y2axis markerattrs=(symbol=squarefilled) markers;
		    vline ApplicationDate / response = Scoring_results_TABLE legendlabel="Scoring"  y2axis markerattrs=(symbol=squarefilled) markers;
/*		    vline ApplicationDate / response = TU_CREDITVISION_TABLE legendlabel="TU Creditvision"  y2axis markerattrs=(symbol=squarefilled) markers;*/
		    vline ApplicationDate / response = BUR_PROFILE_TU_BCC_TABLE legendlabel="TU_BCC"  y2axis markerattrs=(symbol=squarefilled) markers;
		    vline ApplicationDate / response = BUR_PROFILE_TU_AGG_TABLE legendlabel="TU Agg"  y2axis markerattrs=(symbol=squarefilled) markers;
		    vline ApplicationDate / response = BUR_PROFILE_PINPOINT_TABLE legendlabel="Pinpoint"  y2axis markerattrs=(symbol=squarefilled) markers;
		    vline ApplicationDate / response = BANKING_TABLE legendlabel="Banking"  y2axis markerattrs=(symbol=squarefilled) markers;
/*		    vline ApplicationDate / response = APPLICANT_TABLE legendlabel="Applicant"  y2axis markerattrs=(symbol=squarefilled) markers;*/
			yaxis label = 'Volume'; 
			xaxis label = "Date";
			y2axis label = 'Match Rate %' min = 0 max = 1 ;
		run;
		
		Title "Scorecard Distribution";
		proc sgplot data= Scorecarddist ;
			vbar ApplicationDate / response = PERCENT group=SCORECARDVERSION NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending; 
			yaxis label = 'Population %'; 
			xaxis label = "Date";
		run;

		Title "ChannelCode Distribution";
		proc sgplot data=channelcodedist ;
			vbar Applicationdate / response = PERCENT group=channelcode NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending;
			yaxis label = 'Population %'; 
			xaxis label = "Date";
		run;

		Title "V6 Segment Distribution";
		proc sgplot data=segmentdist ;
			vbar Month / response = PERCENT group=v6_seg NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending;
			yaxis label = 'Population %'; 
			xaxis label = "Date";
		run;

		Title "Institution(Bank) Distribution";
		proc sgplot data=bankdist ;
			vbar ApplicationDate / response = PERCENT group=INSTITUTIONCODE NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending; 
			yaxis label = 'Population %'; 
			xaxis label = "Date";
		run;
		
		Title "System Risk Group Distribution";
		proc sgplot data=sysRiskGroupdist ;
			vbar ApplicationDate / response = PERCENT group=V6_RiskGroup_prod NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending; 
			yaxis label = 'Population %'; 
			xaxis label = "Date";
		run;

		
		Title "System V622 Risk Group Distribution";
		proc sgplot data=sysRiskGroupdist1;
			vbar ApplicationDate / response = PERCENT group=V6_RiskGroup_prod NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending; 
			yaxis label = 'Population %'; 
			xaxis label = "Date";
		run;
		
		Title "System V636 Risk Group Distribution";
		proc sgplot data=sysRiskGroupdist2;
			vbar ApplicationDate / response = PERCENT group=V6_RiskGroup_prod NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending; 
			yaxis label = 'Population %'; 
			xaxis label = "Date";
		run;
		
		Title "System V645 Risk Group Distribution";
		proc sgplot data=sysRiskGroupdist3;
			vbar ApplicationDate / response = PERCENT group=V6_RiskGroup_prod NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending; 
			yaxis label = 'Population %'; 
			xaxis label = "Date";
		run;

		Title "System V655 Risk Group Distribution";
		proc sgplot data=sysRiskGroupdist4;
			vbar ApplicationDate / response = PERCENT group=V6_RiskGroup_prod NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending; 
			yaxis label = 'Population %'; 
			xaxis label = "Date";
		run;

		Title "Risk Group Distribution";
		proc sgplot data=RiskGroupdist ;
			vbar ApplicationDate / response = PERCENT group=V6_RiskGroup NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending; 
			yaxis label = 'Population %'; 
			xaxis label = "Date";
		run;
		
		Title "V622 Risk Group Distribution";
		proc sgplot data=RiskGroupdist1;
			   vbar ApplicationDate / response = PERCENT group=V6_RiskGroup NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending; 
			yaxis label = 'Population %'; 
			xaxis label = "Date";
		run;
		
		Title "V636 Risk Group Distribution";
		proc sgplot data=RiskGroupdist2;
			   vbar ApplicationDate / response = PERCENT group=V6_RiskGroup NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending; 
			yaxis label = 'Population %'; 
			xaxis label = "Date";
		run;
		
		Title "V645 Risk Group Distribution";
		proc sgplot data=RiskGroupdist3;
			   vbar ApplicationDate / response = PERCENT group=V6_RiskGroup NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending; 
			yaxis label = 'Population %'; 
			xaxis label = "Date";
		run;

		Title "V655 Risk Group Distribution";
		proc sgplot data=RiskGroupdist4;
			   vbar ApplicationDate / response = PERCENT group=V6_RiskGroup NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending; 
			yaxis label = 'Population %'; 
			xaxis label = "Date";
		run;
		
		Title "Compuscan Segment Distribution";
		proc sgplot data=comp_seg ;
			   vbar ApplicationDate / response = PERCENT group=comp_seg NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending; 
			yaxis label = 'Population %'; 
			xaxis label = "Date";
		run;
		
		Title "TU Segment Distribution";
		proc sgplot data=tu_seg ;
			   vbar ApplicationDate / response = PERCENT group=tu_seg NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending; 
			yaxis label = 'Population %'; 
			xaxis label = "Date";
		run;
	/*	ods pdf close;*/
	/*	ods _all_ close;*/
	%mend;
%prepReportData;

%let todaysDate = %sysfunc(today(), yymmddn8.);
%let thismonth = %sysfunc(today(), yymmn6.);
%put &todaysDate;
%put &thismonth.;

%createdirectory(directory=\\MPWSAS64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\Reports\Daily\Overall\&thismonth.\&todaysDate.);
options nodate;
ods pdf file="\\MPWSAS64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\Reports\Daily\Overall\&thismonth.\&todaysDate.\V6 Daily Monitoring Report - Weekends Incl &todaysDate..pdf"   UNifORM ; 
%drawReportGraphs();
ods pdf close;
ods _all_ close;