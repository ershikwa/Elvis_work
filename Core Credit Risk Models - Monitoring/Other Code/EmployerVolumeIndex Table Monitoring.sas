libname Data "\\neptune\sasa$\V5\Application Scorecard\V636\calibration_new";

data In_data (keep=EmployerVolumeIndexGroup SubSectorIndexGroup runmonth);
set DATA.EMPLOYERVOLUMEINDEX09062021 DATA.EMPLOYERVOLUMEINDEX19072021 DATA.EMPLOYERVOLUMEINDEX09082021 
	DATA.EMPLOYERVOLUMEINDEX09092021 DATA.EMPLOYERVOLUMEINDEX09102021 DATA.EMPLOYERVOLUMEINDEX09112021
	DATA.EMPLOYERVOLUMEINDEX09122021;
run;

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
%plot(input_dataset=In_data,X_Var=runmonth,Y_Var=EmployerVolumeIndexGroup);
%plot(input_dataset=In_data,X_Var=runmonth,Y_Var=SubSectorIndexGroup);

proc freq data=In_data;
	tables 	EmployerVolumeIndexGroup*runmonth 
			SubSectorIndexGroup*runmonth 
			/ nocum norow nocol nopercent;
run;
