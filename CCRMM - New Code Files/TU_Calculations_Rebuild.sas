%include "H:\Process_Automation\sas_autoexec\sas_autoexec.sas";

%let projectcode =H:\Process_Automation\Codes;

%let runmonth = %scan(&sysparm,1);
%let process_number = %scan(&sysparm,2);
%let process =  %scan(&sysparm,3);

%let project =pj;

libname &project "&process";

%start_program;

%include "\\MPWSAS65\Process_Automation\Codes\Git_Repositories\Core Credit Risk Models - Monitoring\Monthly Monitoring\TU_Calculations_Rebuild.sas";

%end_program(&process_number);