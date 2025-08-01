*%include "\\Neptune\SASA$\SAS_Automation\SAS_Autoexec\autoexec2.sas";
%let programpath = \\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\TU_Rebuild;
%let num_of_segs =5;
%let lstmnth = %sysfunc(intnx(month,%sysfunc(today()),-1,e), yymmn6.);
libname data '\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\calibrated_scores';
options noxwait compress=binary;

%macro ODSOff(); /* Call prior to BY-group processing */
	ods graphics off;
	ods exclude all;
	ods noresults;
%mend;
 
%macro ODSOn(); /* Call after BY-group processing */
	ods graphics on;
	ods exclude none;
	ods results;
%mend;

%macro loop();
	%ODSOff();
	%do i = 1 %to &num_of_segs;
		%let programtocopy=\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\TU_Rebuild\Tu_Rebuild_seg.sas;
		%let segmentno =  \\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\TU_Rebuild\Tu_Rebuild_seg&i..sas;
		
		%ODSOff();
		data _null_;
			call symput("copy",cats("'", 'copy "',"&programtocopy",'" "',"&segmentno",'"', "'"));
		run;
		%ODSOn();

		x &copy;

		options xwait noxsync;
		%let start_sas = 'E:\SASHome\SASFoundation\9.4\sas.exe';
		data _null_;
			call symput('sas_program',cats("'","&programpath.\Tu_Rebuild_seg&i.",".sas'"));
			call symput('sas_log', cats("'","&programpath.\Tu_Rebuild_seg&i.",".log'"));
			call symput('sas_print', cats("'","&programpath\Tu_Rebuild_seg&i.",".lst'"));
			call symput("sysparm_v", cats("'","&i","'"));
		run;
		x " &start_sas -sysin &sas_program -nosplash -log &sas_log -print &sas_print -nostatuswin  -noerrorabend -noterminal -noicon -sysparm &sysparm_v ";
	%end;
	%let tableexist=0;
	 
	%do %while (&tableexist = 0);
	    %let countflag = 0;

	    %do h = 1 %to 5;
	        %if %sysfunc(exist(data.gini&h._&lstmnth)) %then 
	        %let countflag = %eval(&countflag+1);
	    %end;
	    %if &countflag=5 %then  %let tableexist = 1;
	    %else %let tableexist = 0; 

	    data _null_;
	        call sleep(10000);
	    run;
	%end;
 %ODSOn;
%mend;
%loop;

filename macros2 'H:\Process_Automation\macros';
options sasautos = (sasautos  macros2);
