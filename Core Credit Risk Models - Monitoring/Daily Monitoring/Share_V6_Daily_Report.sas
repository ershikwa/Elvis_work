%let todaysDate = %sysfunc(today(), yymmddn8.);
%put &todaysDate;
%let thismonth = %sysfunc(today(), yymmn6.);
%put &thismonth;

%let reportfile = "\\MPWSAS64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\Reports\Daily\Overall\&thismonth.\&todaysDate.\V6 Daily Monitoring.pdf";

options compress = yes;
options compress = on;

options noxwait noxsync;

%macro findit;                                 
  %if %sysfunc(fileexist(&reportfile)) %then %do;  
		%put The file &reportfile exists. ;
		

		%let start_sas = 'E:\SASHome\SASFoundation\9.4\sas.exe';

		data _null_;
			call symput('sas_program',cats("'","\\mpwsas65\Process_Automation\Codes\Git_Repositories\Core Credit Risk Models - Monitoring\Macros\Send_V6_Monitoring.sas'"));
			call symput('sas_log', cats("'","H:\Process_Automation\logs\send_v6_monitoring.log'"));
		run;

		options noxwait noxsync;
		x " &start_sas -sysin &sas_program -log &sas_log ";
	%end; 
  %else %put The file &reportfile does not exist. ;
%mend;     

%findit;
