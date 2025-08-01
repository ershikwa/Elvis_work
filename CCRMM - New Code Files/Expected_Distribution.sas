%include "\\mpwsas65\Process_Automation\sas_autoexec\sas_autoexec.sas";

options compress =yes;

%let runmonth = %scan(&sysparm,1);
%let process_number = %scan(&sysparm,2);
%let process =  %scan(&sysparm,3);

%let project =pj;
libname &project "&process";
%start_program;

%include "&projectcode\Codes\Git_Repositories\Core Credit Risk Models - Monitoring\Live Monitoring\Expected_Distribution.sas";

%end_program(&process_number);