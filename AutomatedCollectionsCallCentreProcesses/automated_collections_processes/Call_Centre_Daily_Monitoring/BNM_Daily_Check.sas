/*
 ***BNM Daily Check***
This code checks the status of the BNM Processes at the end of the processes 
and executes the Restore/Rollback Strategy if required.
*/
%include "H:\Process_Automation\sas_autoexec\sas_autoexec.sas";
%let projectcode =H:\Process_Automation\Codes;
%let runmonth = %scan(&sysparm,1);
%let process_number = %scan(&sysparm,2);
%let process =  %scan(&sysparm,3);
%let project =pj;
libname &project "&process";

%start_program;
options mprint symbolgen mlogic;
libname data "\\MPWSAS65\Process_Automation\Data";

Data _Null_;
	Call Symput('Start',intnx('Day',today(),0));
Run;

Data _Null_;
	Call symput('Date_T',cats("'",put(&Start,yymmddn8.),"'"));
	Call symput('Stage_T',cats("'",put(Intnx('Day',&Start,-1),yymmddd10.),"'"));
Run;

%Put &Date_T;
%Put &Stage_T;

/*Extract New BNM and Current BNM Tables*/
Proc Sql;
	Connect To ODBC(DSN=MPWAPS);
	Create Table Current_BNM as 
		Select * from connection to ODBC
			(
		Select * from Prd_ContactInfo.dbo.BNM_Best_Numbers_CIC_V2 /*Change to V2 Table*/
			);
	Disconnect from ODBC;
Quit;

Proc Sql;
	Connect To ODBC(DSN=MPWAPS);
	Create Table New_BNM as 
		Select * from connection to ODBC
			(
		Select * from Prd_ContactInfo.dbo.BNM_Best_Numbers_CIC  /*Change to CIC Table*/
			);
	Disconnect from ODBC;
Quit;

/*Calculating The Number of Records Populated Per Column for Current and New BNM*/
Proc Sql;
	Create Table Current_BNM_Stats as
		Select &Date_T as Check_Date, 
			Count(IDNumber) as Total_Clients,
			Count(Number_1) as Cycle_1_Count,
			Count(Number_2) as Cycle_2_Count,
			Count(Number_3) as Cycle_3_Count,
			Count(Number_4) as Cycle_4_Count,
			Count(Number_5) as Cycle_5_Count,
			Count(Number_6) as Cycle_6_Count,
			Count(Number_7) as Cycle_7_Count,
			Count(Number_8) as Cycle_8_Count,
			Count(Number_9) as Cycle_9_Count,
			Count(Number_10) as Cycle_10_Count,
			Count(Number_11) as Cycle_11_Count,
			Count(Number_12) as Cycle_12_Count
		from Current_BNM;
Quit;

Proc Sql;
	Create Table New_BNM_Stats as
		Select &Date_T as Check_Date, 
			Count(IDNumber) as Total_Clients,
			Count(Number_1) as Cycle_1_Count,
			Count(Number_2) as Cycle_2_Count,
			Count(Number_3) as Cycle_3_Count,
			Count(Number_4) as Cycle_4_Count,
			Count(Number_5) as Cycle_5_Count,
			Count(Number_6) as Cycle_6_Count,
			Count(Number_7) as Cycle_7_Count,
			Count(Number_8) as Cycle_8_Count,
			Count(Number_9) as Cycle_9_Count,
			Count(Number_10) as Cycle_10_Count,
			Count(Number_11) as Cycle_11_Count,
			Count(Number_12) as Cycle_12_Count
		from New_BNM;
Quit;

/*Comparing the Record Count as a Percentage of the Current BNM vs New BNM*/
Proc Sql;
	Create Table BNM_Comparison_Stats as 
		Select a.Check_Date,
			(a.Total_Clients / b.Total_Clients) as Total_Client_Percent,
			(a.Cycle_1_Count/b.Cycle_1_Count) as Cycle_1_Percent,
			(a.Cycle_2_Count/b.Cycle_2_Count) as Cycle_2_Percent,
			(a.Cycle_3_Count/b.Cycle_3_Count) as Cycle_3_Percent,
			(a.Cycle_4_Count/b.Cycle_4_Count) as Cycle_4_Percent,
			(a.Cycle_5_Count/b.Cycle_5_Count) as Cycle_5_Percent,
			(a.Cycle_6_Count/b.Cycle_6_Count) as Cycle_6_Percent,
			(a.Cycle_7_Count/b.Cycle_7_Count) as Cycle_7_Percent,
			(a.Cycle_8_Count/b.Cycle_8_Count) as Cycle_8_Percent,
			(a.Cycle_9_Count/b.Cycle_9_Count) as Cycle_9_Percent,
			(a.Cycle_10_Count/b.Cycle_10_Count) as Cycle_10_Percent,
			(a.Cycle_11_Count/b.Cycle_11_Count) as Cycle_11_Percent,
			(a.Cycle_12_Count/b.Cycle_12_Count) as Cycle_12_Percent
		from New_BNM_Stats as A
			Left Join Current_BNM_Stats as B
				On a.Check_Date = b.Check_Date;
Quit;

/*Extracting ThresholdTable*/
Proc Sql;
	Connect To ODBC(DSN=MPWAPS);
	Create Table BNM_Threshholds as 
		Select * from connection to ODBC
			(
		Select &Date_T as Check_date,Test_Ind,Test_Threshold,Prod_Threshold
			from Prd_ContactInfo.dbo.BNM_Threshholds
			);
	Disconnect from ODBC;
Quit;

/*Checking If New BNM Meets The Threshold Criteria*/
%Macro Thresh_Check();

	Data _null_;
		Set BNM_Threshholds;
		call symput('Test_Ind',Test_Ind);
	Run;

	%Put Test Ind is set to &Test_Ind;

	%If &Test_Ind = 0 %Then
		%Do;

			Proc Sql;
				Create Table BNM_Threshholds_Check as
					Select a.Check_Date,
						Case 
							When a.Total_Client_Percent > b.Prod_Threshold 
							and a.Cycle_1_Percent >= b.Prod_Threshold
							and a.Cycle_2_Percent >= b.Prod_Threshold
							and a.Cycle_3_Percent >= b.Prod_Threshold
							and a.Cycle_4_Percent >= b.Prod_Threshold
							and a.Cycle_5_Percent >= b.Prod_Threshold
							and a.Cycle_6_Percent >= b.Prod_Threshold
							and a.Cycle_7_Percent >= b.Prod_Threshold
							and a.Cycle_8_Percent >= b.Prod_Threshold
							and a.Cycle_9_Percent >= b.Prod_Threshold
							and a.Cycle_10_Percent >= b.Prod_Threshold
							and a.Cycle_11_Percent >= b.Prod_Threshold
							and a.Cycle_12_Percent >= b.Prod_Threshold
							Then 1 
						Else 0
						End 
					as Threshold_Check_Ind
						from BNM_Comparison_Stats as A
							Left Join BNM_Threshholds as B
								On a.Check_date = b.Check_date;
			Quit;

			Data _null_;
				Set BNM_Threshholds_Check;
				call symput('Threshold_Check_Ind',Threshold_Check_Ind);
			Run;

			Proc Sql;
				Connect To ODBC(DSN=MPWAPS);
				Create Table Staging_Table as
					Select * from Connection to ODBC
						(
					Select * from edwdw.dbo.CIC_AB_col_tallyman_staging 
						Where Date_imported > &Stage_T
						);
				Disconnect from ODBC;
			Quit;

			Proc Sql;
				Create Table CIC_Joined as 
					Select *,
						Case 
							When Input(a.ClientNumber,25.) = b.ClientNumber Then 1 
							Else 0 
						End 
					as Merge_CIC,
						b.Number_1 as BNumber_1
					from Staging_Table as A
						Left Join New_BNM as B
							On Input(a.ClientNumber,25.) = b.ClientNumber;
			Quit;

			Proc Sql;
				Create Table CIC_Joined_No_Dupe as 
					Select Distinct ClientNumber,IDNumber,Merge_CIC,
						BNumber_1 as Number_1,Number_2, Number_3, Number_4,Number_5, Number_6, Number_7, Number_8, Number_9, Number_10, Number_11, Number_12
					from CIC_Joined;
			Quit;

			Data Results(Keep=IDNumer ClientNumber Num1 - Num12);
				Set CIC_Joined_No_Dupe;

				%Do i = 1 %To 12;
					If Number_&i <> '' THen
						Num&i = 1;
					Else Num&i = 0;
				%End;
			Run;

			proc summary data=results nway missing;
				var num1 - num12;
				output out=CICNumSum sum=;
			run;

			proc transpose data=CICNumSum out=X;
				by _freq_;
			run;

			data X (Rename=(_Freq_ = Total Col1 = Volume _Name_ = Numbers));
				set X;
				where _Name_ ne '_TYPE_';
				Pct = Col1/_freq_;
				Number = _N_;
			run;

			Proc Transpose data=X Name=Numbers Prefix=Number Out=X_2;
			Quit;

			Proc Sql;
				Create Table BNM_Intensity_Check as 
					Select &Date_T as Check_Date,
						Case 
							When 
							Number1 < Number2 and 
							Number2 < Number3 and 
							Number3 < Number4 and 
							Number4 < Number5 and 
							Number5 < Number6 and 
							Number6 < Number7 and 
							Number7 < Number8 and 
							Number8 < Number9 and 
							Number10 < Number11 and 
							Number11 < Number12
							Then 1 
						Else 0 
						End 
					as Intensity_Check_Ind
						from X_2
							Where Numbers = 'Pct';
			Quit;

			Data _null_;
				Set BNM_Intensity_Check;
				call symput('Intensity_Check_Ind',Intensity_Check_Ind);
			Run;

		%End;
	%Else
		%Do;

			Proc Sql;
				Create Table BNM_Threshholds_Check as
					Select a.Check_Date,
						Case 
							When a.Total_Client_Percent > b.Test_Threshold 
							and a.Cycle_1_Percent >= b.Test_Threshold
							and a.Cycle_2_Percent >= b.Test_Threshold
							and a.Cycle_3_Percent >= b.Test_Threshold
							and a.Cycle_4_Percent >= b.Test_Threshold
							and a.Cycle_5_Percent >= b.Test_Threshold
							and a.Cycle_6_Percent >= b.Test_Threshold
							and a.Cycle_7_Percent >= b.Test_Threshold
							and a.Cycle_8_Percent >= b.Test_Threshold
							and a.Cycle_9_Percent >= b.Test_Threshold
							and a.Cycle_10_Percent >= b.Test_Threshold
							and a.Cycle_11_Percent >= b.Test_Threshold
							and a.Cycle_12_Percent >= b.Test_Threshold
							Then 1 
						Else 0
						End 
					as Threshold_Check_Ind
						from BNM_Comparison_Stats as A
							Left Join BNM_Threshholds as B
								On a.Check_date = b.Check_date;
			Quit;

			Data _null_;
				Set BNM_Threshholds_Check;
				call symput('Threshold_Check_Ind',Threshold_Check_Ind);
			Run;

			Data BNM_Intensity_Check;
				Check_Date = &Date_T;
				Intensity_Check_Ind = 1;
			Run;

			Data _null_;
				call symput('Intensity_Check_Ind',Intensity_Check_Ind);
			Run;

		%End;

	%If &Threshold_Check_Ind = 1 and &Intensity_Check_Ind = 1 %Then
		%Do;
			%SQLDEL(Cred_Scoring.dbo.BNM_Best_Numbers_CIC_Status);

			proc sql;
				connect to ODBC as Cred_Scr (dsn=Cre_Scor);
				execute(
					create table  Cred_Scoring.dbo.BNM_Best_Numbers_CIC_Status
						(Status varchar(20));
				) by Cred_Scr;
			quit;

			proc sql;
				connect to ODBC as Cred_Scr (dsn=Cre_Scor);
				execute(
					insert into Cred_Scoring.dbo.BNM_Best_Numbers_CIC_Status (Status)
						values ('In Progress');
				) by Cred_Scr;
			quit;

			Proc Sql;
				Connect To ODBC(DSN=MPWAPS);
				Execute	(
					Drop Table Prd_ContactInfo.dbo.BNM_Best_Numbers_CIC_V2;
				) By ODBC;
			Quit;

			Proc Sql;
				Connect To ODBC(DSN=MPWAPS);
				Execute(
					Create Table Prd_ContactInfo.dbo.BNM_Best_Numbers_CIC_V2
						WITH (
						distribution=hash(IDNumber),
						clustered columnstore index
						) 
					AS
						SELECT * 
							FROM Prd_ContactInfo.dbo.BNM_Best_Numbers_CIC
								) By ODBC;
			Quit;

			%SQLDEL(Cred_Scoring.dbo.BNM_Best_Numbers_CIC_Status);

			proc sql;
				connect to ODBC as Cred_Scr (dsn=Cre_Scor);
				execute(
					create table  Cred_Scoring.dbo.BNM_Best_Numbers_CIC_Status
						(Status varchar(20), loaddate datetime);
				) by Cred_Scr;
			quit;

			proc sql;
				connect to ODBC as Cred_Scr (dsn=Cre_Scor);
				execute(
					insert into Cred_Scoring.dbo.BNM_Best_Numbers_CIC_Status (Status,loaddate)
						values ('Completed',getdate());
				) by Cred_Scr;
			quit;

			%Put 'Successful';

			/*Siphe Add Mail Here To Say BNM Ran Sucessfully and BNM_Best_Numbers_CIC_V2*/
			/* Send BNM Successful Email */
			%let start_sas = 'E:\SASHome\SASFoundation\9.4\sas.exe';

			data _null_;
				call symput('sas_program',cats("'","H:\Process_Automation\codes\send_bnm_daily_check_success.sas'"));
				call symput('sas_log', cats("'","H:\Process_Automation\logs\send_bnm_daily_check_success.log'"));
			run;

			options noxwait noxsync;
			x " &start_sas -sysin &sas_program -log &sas_log ";
		%End;
	%Else
		%Do;
			%Put 'Unsuccessful';

			/*Siphe Add Mail Here To Say BNM Did Not Run Sucessfully and BNM_Best_Numbers_CIC_V2 was not updated.*/
			/* Send BNM Failed Email */
			%let start_sas = 'E:\SASHome\SASFoundation\9.4\sas.exe';

			data _null_;
				call symput('sas_program',cats("'","H:\Process_Automation\codes\send_bnm_daily_check_fail.sas'"));
				call symput('sas_log', cats("'","H:\Process_Automation\logs\send_bnm_daily_check_fail.log'"));
			run;

			options noxwait noxsync;
			x " &start_sas -sysin &sas_program -log &sas_log ";
		%End;
%Mend;

%Thresh_Check();

Proc Sql;
	Connect To ODBC(DSN=MPWAPS);
	Create Table BNM_Threshold_History as 
		Select * from connection to ODBC
			(
		Select * from Prd_ContactInfo.dbo.BNM_Threshold_History
			);
	Disconnect from ODBC;
Quit;;

Data BNM_Threshold_Current;
	Merge BNM_Threshholds BNM_Comparison_Stats BNM_Intensity_Check;
	By Check_date;
Run;

Proc Append Base=BNM_Threshold_History Data= BNM_Threshold_Current;
Run;

%SQLDEL_APS(Prd_ContactInfo.dbo.BNM_Threshold_History);
%Upload_APS(Set =BNM_Threshold_History, Server = work, APS_ODBC = Prd_Cont, APS_DB = Prd_ContactInfo, Distribute = Replicate);
%end_program(&process_number);