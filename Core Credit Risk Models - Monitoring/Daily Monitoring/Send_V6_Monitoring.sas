options compress = yes;
options compress = on;

%let todaysDate = %sysfunc(today(), yymmddn8.);
%put &todaysDate;
%let thismonth = %sysfunc(today(), yymmn6.);
%put &thismonth;

libname BatchTes "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\Data\&thismonth.\&todaysDate.";


%let yday = %sysfunc(intnx(day,%sysfunc(today()),-1),yymmddd10.);
%put &yday;

proc sql;
	select Match_Rate into : Todays_rate separated by ' '
	from batchtes.matchrate
	having ApplicationDate = MAX(ApplicationDate);

	select MAX(ApplicationDate) into : latest_date separated by ' '
	from batchtes.matchrate;
quit;

proc sql;
	select Match_Rate_IA into : Todays_rate2 separated by ' '
	from batchtes.matchrate
	having ApplicationDate = MAX(ApplicationDate);

	select MAX(ApplicationDate) into : latest_date separated by ' '
	from batchtes.matchrate;
quit;

%put &Todays_rate;
%put &Todays_rate2;
%put &latest_date;

%let reportPath = \\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\Reports\Daily\Overall\&thismonth.\&todaysDate.; 

%macro sendMonitorReport();

%if (&latest_date ne &yday) %then %do;
	
	options mprint mlogic;
	options emailport=25;
	options emailsys =SMTP;
	options emailhost = midrandcasarray.africanbank.net;
	
	*Default wait time is 30 seconds - for 'Email server did not respond' error;
	options emailackwait=300;

	FILENAME outbox EMAIL ("DataScienceAutomation@africanbank.co.za");
	DATA _NULL_;
		FILE outbox
		TO=(/*"GAlexander@AfricanBank.co.za" "DVanZyl@AfricanBank.co.za"*/ "LDeBruyn@AfricanBank.co.za" /*"JSproule@AfricanBank.co.za"
			"TWheatley@AfricanBank.co.za"*/ "eshikwambana@africanbank.co.za" /*"TOsmanLatib@AfricanBank.co.za"*/ 
			"TMphogo@AfricanBank.co.za" "SGcabashe1@AfricanBank.co.za" /*"KMolawa@AfricanBank.co.za"*/ "DMarcus1@AfricanBank.co.za" "DNgwenya1@AfricanBank.co.za" "NSenokwane1@AfricanBank.co.za"
			/*"ERoussos@AfricanBank.co.za" "CBooysen1@AfricanBank.co.za" "ELeRoux@AfricanBank.co.za"*/ "tsehlapelo1@africanbank.co.za" "mthebeyagae1@africanbank.co.za" 
			"vshabalala@africanbank.co.za" "lmasina1@africanbank.co.za" /*"tqobisa1@africanbank.co.za" "VMillican@africanbank.co.za" "LSegoale1@africanbank.co.za"*/)
		FROM=("DataScienceAutomation@africanbank.co.za")
		SUBJECT=("V6 Application Scorecard Daily Monitoring" );
		PUT " ";
		PUT "Good day all";
		PUT " ";
		PUT "Capri import status returned true however, no new data was detected.";
		PUT " ";
		PUT " ";
		PUT "Kind regards,";
		PUT "Scoring Team";
	RUN;
%end;
%else %do;

	options mprint mlogic;
	options emailport=25;
	options emailsys =SMTP;
	options emailhost = midrandcasarray.africanbank.net;
	
	*Default wait time is 30 seconds - for 'Email server did not respond' error;
	options emailackwait=300;

	FILENAME outbox EMAIL ("DataScienceAutomation@africanbank.co.za");
	DATA _NULL_;
		FILE outbox
		TO=("GAlexander@AfricanBank.co.za" "DVanZyl@AfricanBank.co.za" "LDeBruyn@AfricanBank.co.za" "JSproule@AfricanBank.co.za"
			"TWheatley@AfricanBank.co.za" "eshikwambana@africanbank.co.za" "TOsmanLatib@AfricanBank.co.za" 
			"TMphogo@AfricanBank.co.za" "SGcabashe1@AfricanBank.co.za" "KMolawa@AfricanBank.co.za" "DMarcus1@AfricanBank.co.za" "DNgwenya1@AfricanBank.co.za" "NSenokwane1@AfricanBank.co.za"
			"CBooysen1@AfricanBank.co.za" "ELeRoux@AfricanBank.co.za" "tsehlapelo1@africanbank.co.za" "mthebeyagae1@africanbank.co.za" 
			"vshabalala@africanbank.co.za" "lmasina1@africanbank.co.za" "tqobisa1@africanbank.co.za" "LSegoale1@africanbank.co.za")
		/*TO=("NSenokwane1@africanbank.co.za")*/
		FROM=("DataScienceAutomation@africanbank.co.za")
		SUBJECT=("V6 Application Scorecard Daily Monitoring" )
		FROM=("DataScienceAutomation@africanbank.co.za")
		ATTACH=("&reportPath.\V6 Daily Monitoring Report - Weekends Excl &todaysDate..pdf" "&reportPath.\V6 Daily Monitoring Report - Weekends Incl &todaysDate..pdf" "&reportPath.\V6 Daily Monitoring.pdf");
		PUT " ";
		PUT "Good day";
		PUT " ";
		PUT "The V6 match rate for yesterday is &Todays_rate.. The IA match rate for yesterday is &Todays_rate2..  Please find the attached reports.";
		PUT " ";
/*			PUT "The full report can be found in the URL below.";*/
/*			PUT "&reportPath. ";*/
/*			PUT " ";*/
		PUT "Kind regards,";
		PUT "Scoring Team";
	RUN;

%end;
%mend;

options nomprint nosymbolgen nomlogic;
%sendMonitorReport;
