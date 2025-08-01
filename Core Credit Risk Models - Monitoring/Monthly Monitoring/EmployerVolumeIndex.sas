%let path=\\mpwsas64\Core_Credit_Risk_Model_Team\Mpho_Thebeyagae\EmployerIndex;
options compress=yes;
options nodate;
libname Data "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V636\calibration_new";
libname Data2 '\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V645\EmployerVolumeIndex';
libname mpho '\\mpwsas64\Core_Credit_Risk_Model_Team\Mpho_Thebeyagae\EmployerIndex';

proc sql stimer;
    connect to ODBC (dsn=mpwaps);
    create table Combined_EVI as 
    select * from connection to odbc ( 
        select *
        from DEV_DataDistillery_General.dbo.Combined_EVI 
         ) ;
    disconnect from odbc ;
quit;
proc sql stimer;
    connect to ODBC (dsn=mpwaps);
    create table Combined_EVI_V2 as 
    select * from connection to odbc ( 
        select *
        from DEV_DataDistillery_General.dbo.Combined_EVI_V2 
         ) ;
    disconnect from odbc ;
quit;
%let Input_Data = Combined_EVI;
%let Input_Data_V2 = Combined_EVI_V2;
data _null_;
	call symput('today',put(today(),ddmmyyn8.));
	run;
%put &today;
/*upload new table back to APS*/
data Combined_EVI;
set &Input_Data DATA.EMPLOYERVOLUMEINDEX&today;
run;
data Combined_EVI_V2;
set &Input_Data_V2 DATA2.EMPLOYERVOLUMEINDEX&today;
run;
%Upload_APS(Set = Combined_EVI , Server = Work, APS_ODBC = Dev_DDGe, APS_DB = DEV_DataDistillery_General , distribute = HASH([runmonth]));
%Upload_APS(Set = Combined_EVI_V2 , Server = Work, APS_ODBC = Dev_DDGe, APS_DB = DEV_DataDistillery_General , distribute = HASH([runmonth]));

%macro plot(input_dataset=,X_Var=,Y_Var=);
proc sort data=&input_dataset;
	by &X_Var;
run;

proc freq data=&input_dataset noprint;
	by &X_Var; 
	tables &Y_Var / out=Summary; 
run;
title "&Y_Var Distribution by &X_Var";
proc sgplot data=Summary;
	vbar &X_Var / response=Percent  group=&Y_Var groupdisplay=stack;
	xaxis discreteorder=data;
	yaxis grid values=(0 to 100 by 10);
run;
%mend;
/*PDF STARTS HERE*/
ods pdf file = "&Path.\EmployerSubGroupDistributionGraph&today.pdf" ;
options orientation=landscape nocenter;
%plot(input_dataset=Combined_EVI,X_Var=runmonth,Y_Var=EmployerVolumeIndexGroup);
%plot(input_dataset=Combined_EVI,X_Var=runmonth,Y_Var=SubSectorIndexGroup);
%plot(input_dataset=Combined_EVI_V2,X_Var=runmonth,Y_Var=EmployerVolumeIndexGroup_V2);
%plot(input_dataset=Combined_EVI_V2,X_Var=runmonth,Y_Var=SubSectorIndexGroup_V2);
ods pdf close;
ods _all_ close;
/*PDF ends here*/

%macro pl(input_dataset=,X_Var=,Y_Var=);
proc sort data=&input_dataset;
	by &X_Var;
run;

proc freq data=&input_dataset noprint;
	by &X_Var; 
	tables &Y_Var / out=S_&Y_Var; 
run;

data t_&Y_Var (drop=&Y_Var);
	set S_&Y_Var;
	Variable="&Y_Var";
	Bin=&Y_Var;
run;
%mend;
%pl(input_dataset=Combined_EVI,X_Var=runmonth,Y_Var=EmployerVolumeIndexGroup);
%pl(input_dataset=Combined_EVI,X_Var=runmonth,Y_Var=SubSectorIndexGroup);
%pl(input_dataset=Combined_EVI_V2,X_Var=runmonth,Y_Var=EmployerVolumeIndexGroup_V2);
%pl(input_dataset=Combined_EVI_V2,X_Var=runmonth,Y_Var=SubSectorIndexGroup_V2);

data Combined_EVI_sum;
set T_EmployerVolumeIndexGroup_V2  T_SubSectorIndexGroup_V2
	T_EmployerVolumeIndexGroup T_SubSectorIndexGroup;
run;
%Upload_APS(Set = Combined_EVI_sum , Server = Work, APS_ODBC = Dev_DDGe, APS_DB = DEV_DataDistillery_General , distribute = HASH([runmonth]));

/*Email of pdf*/

%macro sendMonitorReport();
options mprint mlogic;
options emailport = 25;
options emailsys = SMTP;
options emailhost = midrandcasarray.africanbank.net;
options emailackwait=300;



FILENAME outbox EMAIL ("DataScienceAutomation@africanbank.co.za");
DATA _NULL_;
FILE outbox
TO=("mthebeyagae1@africanbank.co.za" "NSenokwane1@africanbank.co.za" "EShikwambana@africanbank.co.za"  "DMarcus1@africanbank.co.za" "SGcabashe1@africanbank.co.za"  "dngwenya1@africanbank.co.za"
 "TMphogo@africanbank.co.za"  "LMasina1@africanbank.co.za"  "VShabalala@africanbank.co.za"
"TSehlapelo1@africanbank.co.za" 
)
FROM=("DataScienceAutomation@africanbank.co.za")
SUBJECT=("EmployerVolumeIndex distribution graphs")
ATTACH=("&Path.\EmployerSubGroupDistributionGraph&today.pdf");
PUT " ";
PUT "Good day all";
PUT " ";
PUT "Please find attached the EmployerVolumeIndex V636 and V645 distribution over time graphs.";
PUT " ";
PUT " ";
PUT "Kind regards,";
PUT "Scoring Team";
RUN;
%mend;
options nomprint nosymbolgen nomlogic;
%sendMonitorReport;






 











