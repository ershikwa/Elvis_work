%include "\\MPWSAS65\Process_Automation\sas_autoexec\sas_autoexec.sas";

%let projectcode =\\MPWSAS65\Process_Automation\Codes;

%let runmonth = %scan(&sysparm,1);
%let process_number = %scan(&sysparm,2);
%let process =  %scan(&sysparm,3);

%let project = pj;

libname &project "&process";

%start_program;

options mprint mlogic symbolgen;

%include "&projectcode\Git_Repositories\Core Credit Risk Models - Monitoring\Daily Monitoring\V6_Daily_Monitoring_Code.sas";

%end_program(&process_number);

