%include "H:\Process_Automation\sas_autoexec\sas_autoexec.sas";

%let projectcode =H:\Process_Automation\Codes;

%let runmonth = %scan(&sysparm,1);
%let process_number = %scan(&sysparm,2);
%let process =  %scan(&sysparm,3);

%let project =pj;

options compress = yes;

libname &project "&process";

%start_program;

libname Kat "H:\ProcessData\BNM Daily Stats";


%macro BNMChecks;

proc sql stimer;
	connect to ODBC (dsn=ETLScratch);
	create table Stats as 
		select * from connection to odbc 
		(	
			Select * from Cred_Scoring.dbo.BNM_Best_Numbers_CIC_status
	     );
      disconnect from ODBC;
quit;

proc sql stimer;
	connect to ODBC (dsn=MPWAPS);
	create table BNM_Best_Numbers_CIC as 
		select * from connection to odbc 
		(	
			Select * from Prd_ContactInfo.dbo.BNM_Best_Numbers_CIC_V2
	     );
      disconnect from ODBC;
quit;

proc sql stimer;
	connect to ODBC (dsn=MPWAPS);
	create table CIC_AB_col_tallyman_staging as 
		select * from connection to odbc 
		(	
			Select * from edwdw.dbo.CIC_AB_col_tallyman_staging 
	     );
      disconnect from ODBC;
quit;

proc sql;
	create table CIC_joined as 
		select * , case when input(a.clientnumber,25.) = b.ClientNumber then 1 else 0 end as Merge_CIC, b.Number_1 as BNumber_1
		from CIC_AB_col_tallyman_staging as a
		left join BNM_Best_Numbers_CIC as b 
		on input(a.clientnumber,25.) = b.ClientNumber;
quit;

proc sql;
	create table CIC_joined_Simplified as 
		select distinct CLIENTNUMBER, IDNUMBER,  Merge_CIC , BNumber_1 as Number_1, Number_2, Number_3, Number_4,
		Number_5, Number_6, Number_7, Number_8, Number_9, Number_10, Number_11, Number_12
		from CIC_joined ;
quit;

data CIC_Joined_4_Stats;
	set CIC_joined_Simplified;
	if IDNUMBER ne '' then IDNumberPresent = 1;
		else IDNumberPresent = 0;
run;

proc freq data = CIC_Joined_4_Stats ;
	tables 	IDNumberPresent / out=CIC_Stats;
run;

data BNM_Unique_Number_Count;
	set CIC_joined_Simplified;
	array number {12} Number_1 Number_2 Number_3 Number_4 Number_5 
	Number_6 Number_7 Number_8 Number_9 Number_10 Number_11 Number_12;
	Number_Of_Unique_Numbers = 0;
	do i=1 to 12;
		if missing(number[i]) then continue;
		nonmatches = 1;
		do j=1 to i;
			if j ne i then do;
				if number[i] ne number[j] then do;
					nonmatches = nonmatches + 1;
				end; 
			end;
		end;
		if nonmatches = i then do;
			Number_Of_Unique_Numbers = Number_Of_Unique_Numbers + 1;
		end;
	end;
	keep IDNumber ClientNumber Number_Of_Unique_Numbers;
run;

proc freq data=BNM_Unique_Number_Count noprint;
	table Number_Of_Unique_Numbers/ out=BNM_Unique_Number_Count_ nocum;
run;

data results (keep=idnumber clientnumber num1 - num12) ;
	set CIC_joined_Simplified ;
	%do i = 1 %to 12 ;
		if Number_&i ne '' then Num&i = 1 ;
		else Num&i = 0 ;
	%end;
run;

data temp (keep=idnumber clientnumber num1 - num12) ;
	set BNM_Best_Numbers_CIC ;
	%do i = 1 %to 12 ;
		if Number_&i ne '' then Num&i = 1 ;
		else Num&i = 0 ;
	%end;
run;

%mend;

%BNMChecks;


proc sql;
create table Check1 as 
select 'Number of Records in the Table' as Checks , count(*) as Volume
from temp;
quit;

proc sql;
	create table Check2 as 
		select 'Number of Duplicates on idnumber and Client Idnumber combination' as Checks , count(*) as Volume
		from 		(select idnumber , clientnumber , count(*) from temp
		group by idnumber , clientnumber 
		having count(*) > 1) as A  ;
quit;

proc sql;
	create table Check3 as 
	select 'Number of missing idnumbers' as Checks , count(*) as Volume
	from temp
	where idnumber is null;
quit;

proc sql;
	create table Check4 as 
	select 'Number of missing client numbers' as Checks , count(*) as Volume
	from temp
	where clientnumber is null;
quit;

proc summary data=temp nway missing ;
	var num1 - num12 ;
	output out=NumSum sum=;
run;

proc summary data=results nway missing ;
	var num1 - num12 ;
	output out=CICNumSum sum=;
run;

proc transpose data=Numsum out=T ;
	by _freq_ ;
run;

proc transpose data=CICNumSum out=X ;
	by _freq_ ;
run;

data X (Rename=(_Freq_ = Total Col1 = Volume _Name_ = Numbers)) ;
	set X ;
	where _Name_ ne '_TYPE_';
	Pct = Col1/_freq_;
	Number = _N_ ;
run;

data T (Rename=(_Freq_ = Total Col1 = Volume _Name_ = Numbers)) ;
	set T ;
	where _Name_ ne '_TYPE_';
	Pct = Col1/_freq_;
	Number = _N_ ;
run;

data FinalChecks ;
	format Checks $100. ;
	set Check: ;
run;

%let todaysDate = %sysfunc(today(), yymmddn8.);

options  NODATE nocenter orientation=landscape     ;
ods graphics  / border=off ;
ods pdf FILE="H:\ProcessData\BNM Daily Stats\PostBNMNewStratApplied_Report_&todaysDate..pdf" style=htmlblue ;
Title1 'BNM_Best_Numbers_CIC Checks';
Title2 'Post New Strategy';

proc print data=Stats noobs;
format Volume comma9. ;
run;

proc print data=FinalChecks noobs;
format Volume comma9. ;
run;
Title1;
Title2;

Title 'Staging - Populated ID Numbers';
proc print data=CIC_Stats noobs;
run;


Title 'BNM CIC - Volume distribution of the numbers';
proc sgplot data=T ;
format Volume comma6. ;
vbar Number / groupdisplay=cluster response=Volume stat=mean  ;
yaxis label="Volume";
run;

Title 'Staging - Volume distribution of the  numbers';
proc sgplot data=X ;
format Volume comma6. ;
vbar Number / groupdisplay=cluster response=Volume stat=mean  ;
yaxis label="Volume";
run;

Title 'BNM CIC - Percentages within each number position that are populated';
proc sgplot data=T ;
format Pct Percent10.1 ;
vbar Number / groupdisplay=cluster response=Pct stat=mean  ;
yaxis label="Percentage Populated";
run;

Title 'Staging - Percentages within each number position that are populated';
proc sgplot data=X ;
format Pct Percent10.1 ;
vbar Number / groupdisplay=cluster response=Pct stat=mean  ;
yaxis label="Percentage Populated";
run;

Title 'Staging - Summary of Unique Numbers per customer';
proc print data=BNM_Unique_Number_Count_ noobs;
run;




ods pdf close ;

%let start_sas = 'E:\SASHome\SASFoundation\9.4\sas.exe';

data _null_;
	call symput('sas_program',cats("'","H:\Process_Automation\Codes\Send BNM Stats\Send_BNM_Stats.sas'"));
	call symput('sas_log', cats("'","H:\Process_Automation\logs\Send_BNM_Stats.log'"));
run;

options noxwait noxsync;
x " &start_sas -sysin &sas_program -log &sas_log ";

filename macros2 'H:\Process_Automation\macros';
options sasautos = (sasautos  macros2);

%end_program(&process_number);

