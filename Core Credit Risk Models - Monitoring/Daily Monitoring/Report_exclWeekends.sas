*libname scoring odbc dsn=Cred_Scoring schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;


	data results_exc_wknds;
	set scoring.all_scores;
	if (weekday ne 1);
	if (weekday ne 7);
	V6_Seg = cats(comp_seg,Tu_seg);
	run;

	proc sort data=results_exc_wknds;
		by applicationdate;
	run;

%macro prepReportData();

	proc sort data= results_exc_wknds;
	by ApplicationDate;
	run;

	proc sql;
		create table matchrate as
			select ApplicationDate, count(uniqueid) as Volume, (sum(V6_Match)/count(uniqueid)) as Match_Rate format=percent9.2
			from  results_exc_wknds
			group by ApplicationDate;
	quit;

	proc sql;
		create table matchrate1 as
			select ApplicationDate, count(uniqueid) as Volume_V622, (sum(V6_Match)/count(uniqueid)) as Match_Rate_V622 format=percent9.2
			from  results_exc_wknds
			where maxSCORECARDVERSION = 'V622'
			group by ApplicationDate;
	quit;

	proc sql;
	   create table matchrate2 as
			select ApplicationDate, count(uniqueid) as Volume_V636, (sum(V6_Match)/count(uniqueid)) as Match_Rate_V636 format=percent9.2
			from  results_exc_wknds
			where maxSCORECARDVERSION = 'V636'
			group by ApplicationDate;
	quit;

	proc sql;
	   create table matchrate3 as
			select ApplicationDate, count(uniqueid) as Volume_V645, (sum(V6_Match)/count(uniqueid)) as Match_Rate_V645 format=percent9.2
			from  results_exc_wknds
			where maxSCORECARDVERSION = 'V645'
			group by ApplicationDate;
	quit;

	proc sql;
	   create table matchrate4 as
			select ApplicationDate, count(uniqueid) as Volume_V655, (sum(V6_Match)/count(uniqueid)) as Match_Rate_V655 format=percent9.2
			from  results_exc_wknds
			where maxSCORECARDVERSION = 'V655'
			group by ApplicationDate;
	quit;

	proc freq data = results_exc_wknds;
		tables v6_seg /norow nocol nocum nofreq out=segmentdist;
		by ApplicationDate;
	run;

	data segmentdist (rename=(ApplicationDate=Month));
		set segmentdist;
	run;
/**/
/*	proc sql;*/
/*		delete from scoring.V622_segmentdist; */
/*		*/
/*		insert into scoring.V622_segmentdist (Bulkload = Yes)*/
/*		select **/
/*		from segmentdist;*/
/*	quit;*/

	proc freq data =  results_exc_wknds;
		tables V6_RiskGroup_prod /norow nocol nocum nofreq out=sysriskgroupdist;
		by ApplicationDate;
	run;

	data rew1 rew2 rew3 rew4;
		set results_exc_wknds;
		if maxSCORECARDVERSION = 'V622' then output rew1;
		else if maxSCORECARDVERSION = 'V636' then output rew2;
		else if maxSCORECARDVERSION = 'V645' then output rew3;
		else if maxSCORECARDVERSION = 'V655' then output rew4;
	run;

	proc freq data = rew1;
		tables V6_RiskGroup_prod /norow nocol nocum nofreq out=sysriskgroupdist1;
		by ApplicationDate;
	run;

	proc freq data = rew2;
		tables V6_RiskGroup_prod /norow nocol nocum nofreq out=sysriskgroupdist2;
		by ApplicationDate;
	run;

	proc freq data =  rew3;
		tables V6_RiskGroup_prod /norow nocol nocum nofreq out=sysriskgroupdist3;
		by ApplicationDate;
	run;

	proc freq data =  rew4;
		tables V6_RiskGroup_prod /norow nocol nocum nofreq out=sysriskgroupdist4;
		by ApplicationDate;
	run;

	proc freq data= results_exc_wknds;
		tables EQ2012AL  /norow nocol nocum nofreq out=EQ2012ALdist;
			by ApplicationDate;
	run;

	proc freq data= results_exc_wknds;
		tables EQ2015PL   /norow nocol nocum nofreq out=EQ2015PLdist;
			by ApplicationDate;
	run;

	proc sql;
		delete from scoring.v622_sysriskgroupdist; 
		
		insert into scoring.v622_sysriskgroupdist (Bulkload = Yes)
		select *
		from sysriskgroupdist;
	quit;

	proc freq data =  results_exc_wknds;
		tables V6_RiskGroup /norow nocol nocum nofreq out=riskgroupdist;
		by ApplicationDate;
	run;

	proc freq data =  rew1;
		tables V6_RiskGroup /norow nocol nocum nofreq out=riskgroupdist1;
		by ApplicationDate;
	run;

	proc freq data =  rew2;
		tables V6_RiskGroup /norow nocol nocum nofreq out=riskgroupdist2;
		by ApplicationDate;
	run;

	proc freq data =  rew3;
		tables V6_RiskGroup /norow nocol nocum nofreq out=riskgroupdist3;
		by ApplicationDate;
	run;

	proc freq data =  rew4;
		tables V6_RiskGroup /norow nocol nocum nofreq out=riskgroupdist3;
		by ApplicationDate;
	run;

	proc freq data =  results_exc_wknds;
		tables INSTITUTIONCODE /norow nocol nocum nofreq out=bankdist;
		by ApplicationDate;
	run;

	proc sql;
		delete from scoring.v622_bankdist; 
		
		insert into scoring.v622_bankdist (Bulkload = Yes)
		select *
		from bankdist;
	quit;
	
	proc freq data =  results_exc_wknds;
		tables tu_seg /norow nocol nocum nofreq out=tu_seg;
		by ApplicationDate;
	run;

	proc freq data =  results_exc_wknds;
		tables channelcode /norow nocol nocum nofreq out=channelcode;
		by ApplicationDate;
	run;

	proc sql;
	 	delete from scoring.v622_tu_seg; 
		
		insert into scoring.v622_tu_seg (Bulkload = Yes)
		select *
		from tu_seg;
	quit;

	proc freq data =  results_exc_wknds;
		tables comp_seg /norow nocol nocum nofreq out=comp_seg;
		by ApplicationDate;
	run;

	proc sql;
		delete from scoring.v622_comp_seg; 
		
		insert into scoring.v622_comp_seg (Bulkload = Yes)
		select *
		from comp_seg;
	quit;

%mend;

%macro drawReportGraphs();

	Title "V6 Volumes and Match Rate";
	proc sgplot data=matchrate ;
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
	
	Proc print data=matchrate noobs;
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

/*	Title "V6 Data in Tables";*/
/*	proc sgplot data=scoring.V622_intables ;*/
/*		   vbar ApplicationDate / response = Volume  NOSTATLABEL  BARWIDTH = 0.8 ;*/
/*		   vline ApplicationDate / response = AFFORDABILITY_TABLE legendlabel="Affordability"  y2axis markerattrs=(symbol=squarefilled) markers;*/
/*		   vline ApplicationDate / response = EMPLOYMENT_TABLE legendlabel="Employment"  y2axis markerattrs=(symbol=squarefilled) markers;*/
/*		   vline ApplicationDate / response = Scoring_results_TABLE legendlabel="Scoring"  y2axis markerattrs=(symbol=squarefilled) markers;*/
/*		   vline ApplicationDate / response = TU_CREDITVISION_TABLE legendlabel="TU Creditvision"  y2axis markerattrs=(symbol=squarefilled) markers;*/
/*		   vline ApplicationDate / response = BUR_PROFILE_TU_BCC_TABLE legendlabel="TU_BCC"  y2axis markerattrs=(symbol=squarefilled) markers;*/
/*		   vline ApplicationDate / response = BUR_PROFILE_TU_AGG_TABLE legendlabel="TU Agg"  y2axis markerattrs=(symbol=squarefilled) markers;*/
/*		   vline ApplicationDate / response = BUR_PROFILE_PINPOINT_TABLE legendlabel="Pinpoint"  y2axis markerattrs=(symbol=squarefilled) markers;*/
/*		   vline ApplicationDate / response = BANKING_TABLE legendlabel="Banking"  y2axis markerattrs=(symbol=squarefilled) markers;*/
/*		   vline ApplicationDate / response = APPLICANT_TABLE legendlabel="Applicant"  y2axis markerattrs=(symbol=squarefilled) markers;*/
/*		yaxis label = 'Volume'; */
/*		xaxis label = "Date";*/
/*		y2axis label = 'Match Rate %' min = 0 max = 1 ;*/
/*	run;*/
	
/*	Proc print data= CS_Unstable_Vars_L30D noobs;*/
/*		Title "Compuscan Unstable Variables - Past Month";*/
/*	run;*/
/*	*/
/*	Proc print data= TU_Unstable_Vars_L30D noobs;*/
/*		Title "Transunion Unstable Variables - Past Month";*/
/*	run;*/
	
	Title "ChannelCode Distribution";
		proc sgplot data=channelcode ;
			   vbar applicationdate / response = PERCENT group=channelcode NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending;
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
	proc sgplot data=sysriskgroupdist ;
		   vbar ApplicationDate / response = PERCENT group=V6_RiskGroup_prod NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending; 
		yaxis label = 'Population %'; 
		xaxis label = "Date";
	run;
	
	Title "System V622 Risk Group Distribution";
	proc sgplot data=sysriskgroupdist1;
		   vbar ApplicationDate / response = PERCENT group=V6_RiskGroup_prod NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending; 
		yaxis label = 'Population %'; 
		xaxis label = "Date";
	run;
	
	Title "System V636 Risk Group Distribution";
	proc sgplot data=sysriskgroupdist2;
		   vbar ApplicationDate / response = PERCENT group=V6_RiskGroup_prod NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending; 
		yaxis label = 'Population %'; 
		xaxis label = "Date";
	run;
	
	Title "System V645 Risk Group Distribution";
	proc sgplot data=sysriskgroupdist3;
		   vbar ApplicationDate / response = PERCENT group=V6_RiskGroup_prod NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending; 
		yaxis label = 'Population %'; 
		xaxis label = "Date";
	run;

	Title "System V655 Risk Group Distribution";
	proc sgplot data=sysriskgroupdist4;
		   vbar ApplicationDate / response = PERCENT group=V6_RiskGroup_prod NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending; 
		yaxis label = 'Population %'; 
		xaxis label = "Date";
	run;
	
	Title "Risk Group Distribution";
	proc sgplot data=riskgroupdist ;
		   vbar ApplicationDate / response = PERCENT group=V6_RiskGroup NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending; 
		yaxis label = 'Population %'; 
		xaxis label = "Date";
	run;
	
	Title "V622 Risk Group Distribution";
	proc sgplot data=riskgroupdist1;
		   vbar ApplicationDate / response = PERCENT group=V6_RiskGroup NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending; 
		yaxis label = 'Population %'; 
		xaxis label = "Date";
	run;
	
	Title "V636 Risk Group Distribution";
	proc sgplot data=riskgroupdist2;
		   vbar ApplicationDate / response = PERCENT group=V6_RiskGroup NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending; 
		yaxis label = 'Population %'; 
		xaxis label = "Date";
	run;

	
	Title "V645 Risk Group Distribution";
	proc sgplot data=riskgroupdist3;
		   vbar ApplicationDate / response = PERCENT group=V6_RiskGroup NOSTATLABEL  BARWIDTH = 0.8 grouporder=ascending; 
		yaxis label = 'Population %'; 
		xaxis label = "Date";
	run;

	Title "V655 Risk Group Distribution";
	proc sgplot data=riskgroupdist4;
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
	
/*	Proc print data= CS_Unstable_Variables noobs;*/
/*		Title "Compuscan Unstable Variables in the Last 30 Days";*/
/*	run;*/
/*	*/
/*	Proc print data= TU_Unstable_Variables noobs;*/
/*		Title "Transunion Unstable Variables in the Last 30 Days";*/
/*	run;*/
%mend;

%prepReportData;

%let todaysDate = %sysfunc(today(), yymmddn8.);
%let thismonth = %sysfunc(today(), yymmn6.);
%put &todaysDate;
%put &thismonth.;

%createdirectory(directory=\\MPWSAS64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\Reports\Daily\Overall\&thismonth.\&todaysDate.);
options nodate;
ods pdf file="\\MPWSAS64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\Reports\Daily\Overall\&thismonth.\&todaysDate.\V6 Daily Monitoring Report - Weekends Excl &todaysDate..pdf"   UNifORM ; 
%drawReportGraphs();
ods pdf close;
ods _all_ close;