data _null_;
	call symput("month", put(intnx("month", today(),-1,'end'),yymmn6.));
	call symput("lastmonth", put(intnx("month", today(),-2,'end'),yymmn6.));
	call symput("sixmonthago", put(intnx("month", today(),-6,'end'),yymmn6.));
run;
%put &month;
%put &lastmonth;
%put &sixmonthago;
%let test = ;

libname Rejects "\\Neptune\SASA$\V5\New_Rejects";
libname rejects1 "\\MPWSAS5\projects3\Compuscan\Reject Inference data";


data r1;
	set rejects1.outcome_rejects_&month.;
	if month = &sixmonthago.;
run;

data rejects.rejects_snapshot_&test.&month.;
	set rejects.rejects_snapshot_&lastmonth. r1;
run;

/*Apps*/
data rejects.application_rejects_&test.&month. ;
	set rejects1.application_rejects_v2_&month. (drop=month month2);
	month = put(AppDate, yymmn6.);
run;
