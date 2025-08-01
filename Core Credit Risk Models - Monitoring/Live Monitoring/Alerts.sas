libname scoring odbc dsn=Cre_Scor schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

%let todaysDate = %sysfunc(today(), yymmddn8.);
%put &todaysDate;
%let thismonth = %sysfunc(today(), yymmn6.);
%put &thismonth;

proc sql;
	select count(*) into :count
	from scoring.V622_Live_All_scores_v622;
quit;

/*Match rates*/;
%macro V645_alert();
	proc sql;
		select V640_match, V641_match, V642_match, V643_match, V644_match, V645_match into 
				:V640_match_old, :V641_match_old, :V642_match_old, :V643_match_old, :V644_match_old, :V645_match_old
		from scoring.V6_Live_V645;

		drop table scoring.V6_Live_V645;

		Create table scoring.V6_Live_V645 as
		select round(AVG(V640_match*100),0.01) as V640_match, round(AVG(V641_match*100),0.01)as V641_match , round(AVG(V642_match*100),0.01) as V642_match, round(AVG(V643_match*100),0.01) as V643_match, round(AVG(V644_match*100),0.01) as V644_match, round(AVG(V645_match*100),0.01) as V645_match		
		from scoring.V622_Live_All_scores_v622
		where scorecardversion = "V645";

		select V640_match, V641_match, V642_match, V643_match, V644_match, V645_match into 
				:V640_match_new, :V641_match_new, :V642_match_new, :V643_match_new, :V644_match_new, :V645_match_new
		from scoring.V6_Live_V645;
	quit;

	/*next two lines were added because there's a weird blank space when the long macro variable name is used*/
	%let x = &V645_match_new;
	%let y = &V645_match_old;

	%if &count > 300 and %sysevalf(((&x. < 98.0) and (&x. < &y.))) %then %do;
		options mprint mlogic;
		options emailport=25;
		options emailsys =SMTP;
		options emailhost = midrandcasarray.africanbank.net;
		options emailackwait=300; *Default wait time is 30 seconds - for 'Email server did not respond' error;
		FILENAME outbox EMAIL ("DataScienceAutomation@africanbank.co.za");
		DATA _NULL_;
		FILE outbox
		TO=("NSenokwane1@africanbank.co.za" "LDeBruyn@AfricanBank.co.za" "EShikwambana@africanbank.co.za" "DMarcus1@africanbank.co.za" "SGcabashe1@africanbank.co.za" 
			"TMphogo@africanbank.co.za" "VShabalala@africanbank.co.za" "Mthebeyagae1@africanbank.co.za" "TSehlapelo1@africanbank.co.za" "ryisa@africanbank.co.za" "ab@alternativedataapp.com")
			FROM=("DataScienceAutomation@AfricanBank.co.za")
	        SUBJECT=("V6.45 Live Monitoring Match Rate Alert");
			PUT " ";
			PUT "Good day all";
			PUT " ";
			PUT "The match rate for V6.45 dropped from &y.% to &x.%. Please investigate this drop urgently.";
			PUT " ";
			PUT "The full report can be found in the URL below.";
			PUT "https://aitooltest.africanbank.net/#/LiveReports";
			PUT " ";
			PUT "Kind regards";
	        PUT "Data Science Automation Team";
	    RUN;
	%end;
%mend;
%V645_alert


%macro V655_alert();
	proc sql;
		select V650_match, V651_match, V652_match, V653_match, V654_match, V655_match into 
				:V650_match_old, :V651_match_old, :V652_match_old, :V653_match_old, :V654_match_old, :V655_match_old
		from scoring.V6_Live_V655;

		drop table scoring.V6_Live_V655;

		Create table scoring.V6_Live_V655 as
		select round(AVG(V650_match*100),0.01) as V650_match, round(AVG(V651_match*100),0.01)as V651_match , round(AVG(V652_match*100),0.01) as V652_match, round(AVG(V653_match*100),0.01) as V653_match, round(AVG(V654_match*100),0.01) as V654_match, round(AVG(V655_match*100),0.01) as V655_match		
		from scoring.V622_Live_All_scores_v622
		where scorecardversion = "V655";

		select V650_match, V651_match, V652_match, V653_match, V654_match, V655_match into 
				:V650_match_new, :V651_match_new, :V652_match_new, :V653_match_new, :V654_match_new, :V655_match_new
		from scoring.V6_Live_V655;
	quit;

	/*next two lines were added because there's a weird blank space when the long macro variable name is used*/
	%let x = &V655_match_new;
	%let y = &V655_match_old;

	%if &count > 300 and %sysevalf(((&x. < 98.0) and (&x. < &y.))) %then %do;
		options mprint mlogic;
		options emailport=25;
		options emailsys =SMTP;
		options emailhost = midrandcasarray.africanbank.net;
		options emailackwait=300; *Default wait time is 30 seconds - for 'Email server did not respond' error;
		FILENAME outbox EMAIL ("DataScienceAutomation@africanbank.co.za");
		DATA _NULL_;
		FILE outbox
		TO=("NSenokwane1@africanbank.co.za" "LDeBruyn@AfricanBank.co.za" "EShikwambana@africanbank.co.za" "DMarcus1@africanbank.co.za" "SGcabashe1@africanbank.co.za" 
			"TMphogo@africanbank.co.za" "VShabalala@africanbank.co.za" "Mthebeyagae1@africanbank.co.za" "TSehlapelo1@africanbank.co.za" "ryisa@africanbank.co.za" "ab@alternativedataapp.com")
			FROM=("DataScienceAutomation@AfricanBank.co.za")
	        SUBJECT=("V6.55 Live Monitoring Match Rate Alert");
			PUT " ";
			PUT "Good day all";
			PUT " ";
			PUT "The match rate for V6.55 dropped from &y.% to &x.%. Please investigate this drop urgently.";
			PUT " ";
			PUT "The full report can be found in the URL below.";
			PUT "https://aitooltest.africanbank.net/#/LiveReports";
			PUT " ";
			PUT "Kind regards";
	        PUT "Data Science Automation Team";
	    RUN;
	%end;
%mend;
%V655_alert