options compress = yes;
options compress = on;

%let todaysDate = %sysfunc(today(), yymmddn8.);
%put &todaysDate;
%let thismonth = %sysfunc(today(), yymmn6.);
%put &thismonth;



%let time=%sysfunc(time(), hhmm);
%let hour=%sysfunc(hour(%sysfunc(time())));
%let yday = %sysfunc(intnx(day,%sysfunc(today()),-1),yymmddd10.);
%put &yday;
%put &hour;
%put &time;


%let reportPath = \\mpwsas5\G\Automation\Behavescore\reports; 
%let reportPath2 = \\MPWSAS64\Core_Credit_Risk_Model_Team\Behavescore_V2 Monitoring\Reports; 
%let approvalPath = \\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros; 


libname data "\\mpwsas5\G\Automation\Behavescore\Datasets";
libname data2 "\\MPWSAS64\Core_Credit_Risk_Model_Team\Behavescore_V2 Monitoring\Data";


%macro sendMonitorReport();

%if %sysfunc(exist(data2.Behavev2_&todaysDate.)) and %sysfunc(exist(data.Behave_&todaysDate.)) %then %do;

	options mprint mlogic;
	options emailport=25;
	options emailsys =SMTP;
	options emailhost = midrandcasarray.africanbank.net;
	
	*Default wait time is 30 seconds - for 'Email server did not respond' error;
	options emailackwait=300;

	FILENAME outbox EMAIL ("DataScienceAutomation@africanbank.co.za");
	DATA _NULL_;
		FILE outbox
		TO=("eshikwambana@africanbank.co.za" "TMphogo@AfricanBank.co.za" "DNgwenya1@AfricanBank.co.za" "NSenokwane1@AfricanBank.co.za"
			"tsehlapelo1@africanbank.co.za" "vshabalala@africanbank.co.za")
/*		TO=("vshabalala@africanbank.co.za" "tsehlapelo1@africanbank.co.za")*/
		FROM=("DataScienceAutomation@africanbank.co.za")
		SUBJECT=("Behavescore Monitoring" )
		FROM=("DataScienceAutomation@africanbank.co.za")
		ATTACH=("&reportPath\Monthly Total Run Monitoring &todaysdate..pdf" 
		"&reportPath2\BehavescoreV2 Monitoring Report &todaysdate..pdf");
		PUT " ";
		PUT "Good day all,";
		PUT " ";
		PUT "Attached herewith are the Behavescore and the Behavescore V2 Monitoring reports . ";
		PUT " ";
			PUT "The full reports can be found in the URLs below:";
			PUT "&reportPath. ";
			PUT "&reportPath2. ";
			PUT " ";
			PUT "Elvis/Tshepo, please run the Behavescore approval code 'BehaveScore Approval Code' in the path below: ";
			PUT "&approvalPath. ";
		PUT "Kind regards,";
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
		TO=("eshikwambana@africanbank.co.za" "TMphogo@AfricanBank.co.za" "DNgwenya1@AfricanBank.co.za" "NSenokwane1@AfricanBank.co.za"
			"tsehlapelo1@africanbank.co.za" "vshabalala@africanbank.co.za")
/*		TO=("vshabalala@africanbank.co.za" "tsehlapelo1@africanbank.co.za")*/
		FROM=("DataScienceAutomation@africanbank.co.za")
		SUBJECT=("Behavescore V2 Monitoring" );
		PUT " ";
		PUT "Good day all";
		PUT " ";
		PUT "There is no new data available for the Behavescore and Behavescore V2 report at &time .";
		PUT " ";
		PUT " ";
		PUT "Kind regards,";
		PUT "Scoring Team";
	RUN;

%end;
%mend;

options nomprint nosymbolgen nomlogic;
%sendMonitorReport;


