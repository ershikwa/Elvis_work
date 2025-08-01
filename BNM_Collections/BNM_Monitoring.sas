
/*Setting up macro and libname statements*/
options compress=yes;
Libname Pop '\\MPWSAS65\BNM\BNM_Data';
Libname Data '\\MPWSAS65\BNM\New BNM Monitoring\Data';
libname dev_cont odbc dsn=dev_cont schema="DBO" preserve_tab_names=yes CONNECTION=UNIQUE direct_sql=yes;
libname Prd_Cont odbc dsn=Prd_Cont schema="DBO" preserve_tab_names=yes CONNECTION=UNIQUE direct_sql=yes;

%Include '\\MPWSAS65\macros\psi_Calculation.sas';
%Include '\\MPWSAS65\macros\Calc_Gini.sas';
%Include '\\MPWSAS65\macros\VarExist.sas';


ods graphics off;
ods _all_ close;

%Macro BNM_Mon_New(Input=);

	/*%do i = &Input %to 0 %by -1;*/
	/*%If &Input = 0 %Then %Do;
		Data _NUll_;
		Call Symput('Date',today());
		Run; 
	%End;
	%Else %Do;*/
		Data _NUll_;
		Call Symput('Date',intnx('Day',today(),-&Input));
		Run;
	/*%End; */

	/*Declaring Date Variable*/
	Data _Null_;
		Call symput('Run',cats("'",put(intnx('Day',&Date,-2),Date9.),"'"));
		Call Symput('Exist',put(intnx('Day',&Date,-2),yymmddn.));
		Call Symput('Delete',put(intnx('Day',&Date,-5),yymmddn.));
		Call Symput('Exist2',cats("'",put(intnx('Day',&Date,-2),yymmddn.),"'"));
		Call Symput('DayBefore',put(intnx('Day',&Date,-3),yymmddn.));
		Call Symput('Month',put(intnx('Day',&Date,-2),yymmn.));
		Call Symput('Month1',cats("'",put(intnx('Day',&Date,-2),yymmn.),"'"));
		Call Symput('Month2',put(intnx('Day',&Date,-1),yymmn.));
		Call symput('Call',put(intnx('Day',&Date,-1),yymmddn.));
		Call symput('Call1',cats("'",put(intnx('Day',&Date,-0),yymmdd10.),"'"));
		Call symput('Call2',cats("'",put(intnx('Day',&Date,-1),yymmdd10.),"'"));
		Call symput('Seven',cats("'",put(intnx('Day',&Date,-8),yymmddn.),"'"));
		Call symput('Three',cats("'",put(intnx('Day',&Date,-4),yymmddn.),"'"));
		
		Call symput('testdate',cats("'",put(intnx('Day',&Date,-0),date9.),"'"));
	Run;

	Data _Null_;
		Call Symput('Table',cats("'","PRD_ContactInfo.dbo.BNM_New_Best_Numbers_","&Exist","'"));
	Run;

	Data _Null_;
		Call Symput('TableDayBefore',cats("'","PRD_ContactInfo.dbo.BNM_New_Best_Numbers_","&DayBefore","'"));
	Run;

	%Put &Run;
	%Put &Exist;
	%Put &Exist2;
	%Put &DayBefore;
	%Put &Month;
	%Put &Month1;
	%Put &Month2;
	%Put &Call;
	%Put &Call1;
	%Put &Call2;
	%Put &Table;
	%Put &TableDayBefore;
	%Put &Three;
	%Put &Seven;

	Proc Sql;
		Connect TO ODBC(DSN=MPWAPS);
		Select * Into: Call_Check
		from connection to ODBC
		(
			Select Count(Distinct CallID_Detail) as Call_Volumes
			from PRD_ContactInfo.dbo.[MD_RPC_Activities_&Month2]
			Where InitiatedDate> &Call2 and InitiatedDate < &Call1 and IDNumber <> ' ' and campaign <> ' '
		);
		Disconnect from ODBC;
	Quit;
	
%Put &Call_Check;

	%If &Call_Check > 0 %Then %Do;

		
		/*Table Check*/
		Proc SQl;
			Connect to ODBC (DSN=MPWAPS);
			Create Table BNM_&Exist._Ind as
				Select * from connection to ODBC
					(
					If Object_ID (&Table) is not null
					BEGIN
				Select &Exist2 as Date,1 as Table_INd
					END
					ELSE
					Begin
				Select &Exist2 as Date, 0 as Table_INd
					END
					);
			Disconnect from ODBC;
		Quit;

		Proc Append Data=BNM_&Exist._Ind base=data.BNM_Table_Check_&Month force;
		Run;

		/* Check if BNM_New_Best_Numbers_&Exist (table from 2 days ago) exists. If not, send email */
		Data _Null_;
			Call Symput('TableExist',cats("'","BNM_New_Best_Numbers_","&Exist","'"));
		Run;		
			
		proc sql stimer;
			connect to ODBC (dsn = MPWAPS);
			create table PRDCtInfo as
				select * from connection to odbc 
				(
				Use PRD_ContactInfo
					SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = &TableExist
				);
			disconnect from odbc;
		quit;

		proc sql;
			select count(*) into: TblCntE from PRDCtInfo;
		quit;		
	
		%if (&TblCntE. = 0) %then %do;
			*Send error email and end gracefully;
			%let start_sas = 'E:\SASHome\SASFoundation\9.4\sas.exe';

			data _null_;
				call symput('sas_program',cats("'","H:\Process_Automation\codes\send_bnmmon_nodaytable.sas'"));
				call symput('sas_log', cats("'","H:\Process_Automation\logs\send_bnmmon_nodaytable.log'"));
			run;

			%put NOTE: &Table not found during run. Ending session gracefully...;

			options noxwait noxsync;
			x " &start_sas -sysin &sas_program -log &sas_log ";

			%end_program(&process_number);
			endsas;
			
		%end;		

		
			
		/* Check if BNM_New_Best_Numbers_&Exist exists (table from 3 days ago). If not, send email */	
	/*	Data _Null_;
			Call Symput('TableDBExist',cats("'","BNM_New_Best_Numbers_","&DayBefore","'"));
		Run;

		proc sql stimer;
			connect to ODBC (dsn = MPWAPS);
			create table TDBf as
				select * from connection to odbc 
				(
				Use PRD_ContactInfo
					SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = &TableDBExist 
				);
			disconnect from odbc;

		quit;

		proc sql;
			select count(*) into: TblCntDBf from TDBf;
		quit;		
		
		%if (&TblCntDBf. = 0) %then %do;
			*Send error email and end gracefully;
			%let start_sas = 'E:\SASHome\SASFoundation\9.4\sas.exe';

			data _null_;
				call symput('sas_program',cats("'","H:\Process_Automation\codes\send_bnmmon_nodaybftable.sas'"));
				call symput('sas_log', cats("'","H:\Process_Automation\logs\send_bnmmon_nodaybftable.log'"));
			run;

			%put NOTE: &TableDayBefore was not found during run. Ending session gracefully...;	

			options noxwait noxsync;
			x " &start_sas -sysin &sas_program -log &sas_log ";			

			%end_program(&process_number);
			endsas;
			
		%end;	*/

		
		/* BNM Calls Monitoring*/
		Proc SQl stimer;
			connect to ODBC (dsn = MPWAPS);
			Create Table BNM_Calls_&Call as
				select distinct * from connection to odbc 
					(
				Select Distinct CallID_Detail,InitiatedDate,PhoneNumber,IDNumber,RPC_FinishCOde,cast(finishcode as varchar(50)) as finishcode,RPC
					from PRD_ContactInfo.dbo.[MD_RPC_Activities_&Month2]
						Where InitiatedDate > &Call2 and InitiatedDate < &Call1 and IDNumber <> ' ' and campaign <> ' '
					);
			disconnect from odbc;
		quit;
		

		/*Proc SQl;
			Create Table BNM_Calls_&Call._2 as 
				Select a.*,b.FCode
					from BNM_Calls_&Call as A
						Left Join data.FinishCode_lookup as B
							on a.finishcode = b.finishcode;
		Quit;*/
		

		Proc SQl stimer;
			Create Table BNM_Calls_&Call._3 as
	
				Select Distinct CallID_Detail,InitiatedDate,PhoneNumber,
				IDNumber,RPC_FinishCOde,finishcode,RPC,
					case 
						when finishcode = "TPC" or finishcode = "LMTPC" and rpc = 0 then 1 
						Else 0 
					End 
				as TPC_Target,
					case 
						when RPC = 1 Then 1 
						Else 0 
					End 
				as RPC_Target
					from BNM_Calls_&Call
							;
				quit;


		Proc SQl stimer;
			Create Table BNM_RPC_Variable_&Call as
				Select 
					&Exist2 as Date,
					a.*,
					b.p_target1 as bnm_prob,
					b.isrpc90,
					b.isrpc90_B,
					b.isrpc90_W,
					b.avg_lineduration,
					b.avg_lineduration_B,
					b.avg_lineduration_W,
					b.istbclilent,
					b.istbclilent_B,
					b.istbclilent_W,
					b.failed30,
					b.failed30_B,
					b.failed30_W,
					b.tot_callduration,
					b.tot_callduration_B,
					b.tot_callduration_W,
					b.isrpc30,
					b.isrpc30_B,
					b.isrpc30_W,	
					b.tot_calldropped90,
					b.tot_calldropped90_B,
					b.tot_calldropped90_W,
					b.tot_callback45,
					b.tot_callback45_B,
					b.tot_callback45_W,
					b.tot_failed45,
					b.tot_failed45_B,
					b.tot_failed45_W,
					b.isrpc7,
					b.isrpc7_B,
					b.isrpc7_W,
					b.faileddaydiff,
					b.faileddaydiff_B,
					b.faileddaydiff_W
				from  BNM_Calls_&Call._3 as A
					Inner Join dev_cont.data_scored_BNM_&Exist as B
						On a.IDNumber = b.IDNumber and a.PhoneNumber = b.Number;
		Quit;

		Data BNM_RPC_Variable_&Call._Rank;
		set BNM_RPC_Variable_&Call;
		Month = &Month1;
		If BNM_Prob < 0.0009030654 Then Decile = 0;
		Else If BNM_Prob < 0.0015553821 Then Decile = 1;
		Else If BNM_Prob < 0.0018740251 Then Decile = 2;
		Else If BNM_Prob < 0.0025678258 Then Decile = 3;
		Else If BNM_Prob < 0.0030932403 Then Decile = 4;
		Else If BNM_Prob < 0.0057160887 Then Decile = 5;
		Else If BNM_Prob < 0.0088395624 Then Decile = 6;
		Else If BNM_Prob < 0.0151854866 Then Decile = 7;
		Else If BNM_Prob < 0.041183932 Then Decile = 8;
		Else If BNM_Prob > 0.041183932 Then Decile = 9;
		Run;

		Proc Append Data= BNM_RPC_Variable_&Call._Rank base=data.BNM_RPC_All_Ranked_&Month Force;
		Run;



		%psi_calculation(build=data.scored_bnm_build_ranked,base=BNM_RPC_Variable_&Call,period=Date,var=isrpc90,psi_var=isrpc90_W,outputdataset=RPC_View_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=BNM_RPC_Variable_&Call,period=date,var=istbclilent,psi_var=istbclilent_W,outputdataset=RPC_View_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=BNM_RPC_Variable_&Call,period=date,var=avg_lineduration,psi_var=avg_lineduration_W,outputdataset=RPC_View_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=BNM_RPC_Variable_&Call,period=date,var=failed30,psi_var=failed30_W,outputdataset=RPC_View_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=BNM_RPC_Variable_&Call,period=date,var=tot_callduration,psi_var=tot_callduration_W,outputdataset=RPC_View_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=BNM_RPC_Variable_&Call,period=date,var=isrpc30,psi_var=isrpc30_W,outputdataset=RPC_View_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=BNM_RPC_Variable_&Call,period=date,var=tot_calldropped90,psi_var=tot_calldropped90_W,outputdataset=RPC_View_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=BNM_RPC_Variable_&Call,period=date,var=isrpc7,psi_var=isrpc7_W,outputdataset=RPC_View_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=BNM_RPC_Variable_&Call,period=date,var=faileddaydiff,psi_var=faileddaydiff_W,outputdataset=RPC_View_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=BNM_RPC_Variable_&Call,period=date,var=tot_callback45,psi_var=tot_callback45_W,outputdataset=RPC_View_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=BNM_RPC_Variable_&Call,period=date,var=tot_failed45,psi_var=tot_failed45_W,outputdataset=RPC_View_1_&Exist);
	
		%psi_calculation(build=data.scored_bnm_build_ranked,base=BNM_RPC_Variable_&Call,period=Date,var=isrpc90,psi_var=isrpc90_B,outputdataset=RPC_View_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=BNM_RPC_Variable_&Call,period=date,var=istbclilent,psi_var=istbclilent_B,outputdataset=RPC_View_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=BNM_RPC_Variable_&Call,period=date,var=avg_lineduration,psi_var=avg_lineduration_B,outputdataset=RPC_View_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=BNM_RPC_Variable_&Call,period=date,var=failed30,psi_var=failed30_B,outputdataset=RPC_View_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=BNM_RPC_Variable_&Call,period=date,var=tot_callduration,psi_var=tot_callduration_B,outputdataset=RPC_View_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=BNM_RPC_Variable_&Call,period=date,var=isrpc30,psi_var=isrpc30_B,outputdataset=RPC_View_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=BNM_RPC_Variable_&Call,period=date,var=tot_calldropped90,psi_var=tot_calldropped90_B,outputdataset=RPC_View_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=BNM_RPC_Variable_&Call,period=date,var=isrpc7,psi_var=isrpc7_B,outputdataset=RPC_View_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=BNM_RPC_Variable_&Call,period=date,var=faileddaydiff,psi_var=faileddaydiff_B,outputdataset=RPC_View_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=BNM_RPC_Variable_&Call,period=date,var=tot_callback45,psi_var=tot_callback45_B,outputdataset=RPC_View_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=BNM_RPC_Variable_&Call,period=date,var=tot_failed45,psi_var=tot_failed45_B,outputdataset=RPC_View_2_&Exist);

		Proc SQl;
			Create Table RPC_Graph_&Exist as
				Select 
					b.Date, a.scores,b.percent, b.variablename,b.psi, b.marginal_stable,
					b.unstable,
					b.Scores as Variable,
					cats(b.Scores,"(",a.scores,")") as Full_Name
				from RPC_View_2_&Exist as B
					Left Join  RPC_View_1_&Exist as A
						On a.Date = b.Date and a.Percent = b.Percent and a.VariableName = b.VariableName
					Where a.Date <> ' BUILD'
						Order By Date,VariableName;
		Quit;

		Proc Append data=RPC_Graph_&Exist Base=data.RPC_PSI_&Month force;
		Run;


		%psi_calculation(build=data.scored_bnm_build_ranked,base=data.BNM_RPC_All_Ranked_&Month,period=Month,var=isrpc90,psi_var=isrpc90_W,outputdataset=RPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=data.BNM_RPC_All_Ranked_&Month,period=Month,var=istbclilent,psi_var=istbclilent_W,outputdataset=RPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=data.BNM_RPC_All_Ranked_&Month,period=Month,var=avg_lineduration,psi_var=avg_lineduration_W,outputdataset=RPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=data.BNM_RPC_All_Ranked_&Month,period=Month,var=failed30,psi_var=failed30_W,outputdataset=RPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=data.BNM_RPC_All_Ranked_&Month,period=Month,var=tot_callduration,psi_var=tot_callduration_W,outputdataset=RPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=data.BNM_RPC_All_Ranked_&Month,period=Month,var=isrpc30,psi_var=isrpc30_W,outputdataset=RPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=data.BNM_RPC_All_Ranked_&Month,period=Month,var=tot_calldropped90,psi_var=tot_calldropped90_W,outputdataset=RPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=data.BNM_RPC_All_Ranked_&Month,period=Month,var=isrpc7,psi_var=isrpc7_W,outputdataset=RPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=data.BNM_RPC_All_Ranked_&Month,period=Month,var=faileddaydiff,psi_var=faileddaydiff_W,outputdataset=RPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=data.BNM_RPC_All_Ranked_&Month,period=Month,var=tot_callback45,psi_var=tot_callback45_W,outputdataset=RPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=data.BNM_RPC_All_Ranked_&Month,period=Month,var=tot_failed45,psi_var=tot_failed45_W,outputdataset=RPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=data.BNM_RPC_All_Ranked_&Month,period=Month,var=isrpc90,psi_var=isrpc90_B,outputdataset=RPC_Month_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=data.BNM_RPC_All_Ranked_&Month,period=Month,var=istbclilent,psi_var=istbclilent_B,outputdataset=RPC_Month_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=data.BNM_RPC_All_Ranked_&Month,period=Month,var=avg_lineduration,psi_var=avg_lineduration_B,outputdataset=RPC_Month_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=data.BNM_RPC_All_Ranked_&Month,period=Month,var=failed30,psi_var=failed30_B,outputdataset=RPC_Month_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=data.BNM_RPC_All_Ranked_&Month,period=Month,var=tot_callduration,psi_var=tot_callduration_B,outputdataset=RPC_Month_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=data.BNM_RPC_All_Ranked_&Month,period=Month,var=isrpc30,psi_var=isrpc30_B,outputdataset=RPC_Month_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=data.BNM_RPC_All_Ranked_&Month,period=Month,var=tot_calldropped90,psi_var=tot_calldropped90_B,outputdataset=RPC_Month_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=data.BNM_RPC_All_Ranked_&Month,period=Month,var=isrpc7,psi_var=isrpc7_B,outputdataset=RPC_Month_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=data.BNM_RPC_All_Ranked_&Month,period=Month,var=faileddaydiff,psi_var=faileddaydiff_B,outputdataset=RPC_Month_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=data.BNM_RPC_All_Ranked_&Month,period=Month,var=tot_callback45,psi_var=tot_callback45_B,outputdataset=RPC_Month_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=data.BNM_RPC_All_Ranked_&Month,period=Month,var=tot_failed45,psi_var=tot_failed45_B,outputdataset=RPC_Month_2_&Exist);


		Proc SQl;
			Create Table Data.RPC_PSI_Monthly_&Month as
				Select 
					b.Month, a.scores,b.percent, b.variablename,b.psi, b.marginal_stable,
					b.unstable,
					b.Scores as Variable,
					cats(b.Scores,"(",a.scores,")") as Full_Name
				from RPC_Month_2_&Exist as B
					Left Join  RPC_Month_1_&Exist as A
						On a.Month = b.Month and a.Percent = b.Percent and a.VariableName = b.VariableName
						Order By Month,VariableName;
		Quit;


		Proc SQl;
			Create Table RPC_Data_3Days as
			Select * from data.BNM_RPC_All_Ranked_&Month
			Where Date between &Three and &Exist2;
		Quit;



		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_3Days,period=Month,var=isrpc90,psi_var=isrpc90_W,outputdataset=RPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_3Days,period=Month,var=istbclilent,psi_var=istbclilent_W,outputdataset=RPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_3Days,period=Month,var=avg_lineduration,psi_var=avg_lineduration_W,outputdataset=RPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_3Days,period=Month,var=failed30,psi_var=failed30_W,outputdataset=RPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_3Days,period=Month,var=tot_callduration,psi_var=tot_callduration_W,outputdataset=RPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_3Days,period=Month,var=isrpc30,psi_var=isrpc30_W,outputdataset=RPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_3Days,period=Month,var=tot_calldropped90,psi_var=tot_calldropped90_W,outputdataset=RPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_3Days,period=Month,var=isrpc7,psi_var=isrpc7_W,outputdataset=RPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_3Days,period=Month,var=faileddaydiff,psi_var=faileddaydiff_W,outputdataset=RPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_3Days,period=Month,var=tot_callback45,psi_var=tot_callback45_W,outputdataset=RPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_3Days,period=Month,var=tot_failed45,psi_var=tot_failed45_W,outputdataset=RPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_3Days,period=Month,var=isrpc90,psi_var=isrpc90_B,outputdataset=RPC_3Days_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_3Days,period=Month,var=istbclilent,psi_var=istbclilent_B,outputdataset=RPC_3Days_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_3Days,period=Month,var=avg_lineduration,psi_var=avg_lineduration_B,outputdataset=RPC_3Days_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_3Days,period=Month,var=failed30,psi_var=failed30_B,outputdataset=RPC_3Days_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_3Days,period=Month,var=tot_callduration,psi_var=tot_callduration_B,outputdataset=RPC_3Days_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_3Days,period=Month,var=isrpc30,psi_var=isrpc30_B,outputdataset=RPC_3Days_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_3Days,period=Month,var=tot_calldropped90,psi_var=tot_calldropped90_B,outputdataset=RPC_3Days_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_3Days,period=Month,var=isrpc7,psi_var=isrpc7_B,outputdataset=RPC_3Days_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_3Days,period=Month,var=faileddaydiff,psi_var=faileddaydiff_B,outputdataset=RPC_3Days_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_3Days,period=Month,var=tot_callback45,psi_var=tot_callback45_B,outputdataset=RPC_3Days_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_3Days,period=Month,var=tot_failed45,psi_var=tot_failed45_B,outputdataset=RPC_3Days_2_&Exist);

		Proc SQl;
			Create Table Data.RPC_PSI_3Days_&Month as
				Select 
					b.Month, a.scores,b.percent, b.variablename,b.psi, b.marginal_stable,
					b.unstable,
					b.Scores as Variable,
					cats(b.Scores,"(",a.scores,")") as Full_Name
				from RPC_3Days_2_&Exist as B
					Left Join  RPC_3Days_1_&Exist as A
						On a.Month = b.Month and a.Percent = b.Percent and a.VariableName = b.VariableName
						Order By Month,VariableName;
		Quit;

		
		Proc SQl;
			Create Table RPC_Data_7Days as
			Select * from data.BNM_RPC_All_Ranked_&Month
			Where Date between &Seven and &Exist2;
		Quit;

		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_7Days,period=Month,var=isrpc90,psi_var=isrpc90_W,outputdataset=RPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_7Days,period=Month,var=istbclilent,psi_var=istbclilent_W,outputdataset=RPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_7Days,period=Month,var=avg_lineduration,psi_var=avg_lineduration_W,outputdataset=RPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_7Days,period=Month,var=failed30,psi_var=failed30_W,outputdataset=RPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_7Days,period=Month,var=tot_callduration,psi_var=tot_callduration_W,outputdataset=RPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_7Days,period=Month,var=isrpc30,psi_var=isrpc30_W,outputdataset=RPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_7Days,period=Month,var=tot_calldropped90,psi_var=tot_calldropped90_W,outputdataset=RPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_7Days,period=Month,var=isrpc7,psi_var=isrpc7_W,outputdataset=RPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_7Days,period=Month,var=faileddaydiff,psi_var=faileddaydiff_W,outputdataset=RPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_7Days,period=Month,var=tot_callback45,psi_var=tot_callback45_W,outputdataset=RPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_7Days,period=Month,var=tot_failed45,psi_var=tot_failed45_W,outputdataset=RPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_7Days,period=Month,var=isrpc90,psi_var=isrpc90_B,outputdataset=RPC_7Days_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_7Days,period=Month,var=istbclilent,psi_var=istbclilent_B,outputdataset=RPC_7Days_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_7Days,period=Month,var=avg_lineduration,psi_var=avg_lineduration_B,outputdataset=RPC_7Days_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_7Days,period=Month,var=failed30,psi_var=failed30_B,outputdataset=RPC_7Days_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_7Days,period=Month,var=tot_callduration,psi_var=tot_callduration_B,outputdataset=RPC_7Days_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_7Days,period=Month,var=isrpc30,psi_var=isrpc30_B,outputdataset=RPC_7Days_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_7Days,period=Month,var=tot_calldropped90,psi_var=tot_calldropped90_B,outputdataset=RPC_7Days_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_7Days,period=Month,var=isrpc7,psi_var=isrpc7_B,outputdataset=RPC_7Days_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_7Days,period=Month,var=faileddaydiff,psi_var=faileddaydiff_B,outputdataset=RPC_7Days_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_7Days,period=Month,var=tot_callback45,psi_var=tot_callback45_B,outputdataset=RPC_7Days_2_&Exist);
		%psi_calculation(build=data.scored_bnm_build_ranked,base=RPC_Data_7Days,period=Month,var=tot_failed45,psi_var=tot_failed45_B,outputdataset=RPC_7Days_2_&Exist);


		Proc SQl;
			Create Table Data.RPC_PSI_7Days_&Month as
				Select 
					b.Month, a.scores,b.percent, b.variablename,b.psi, b.marginal_stable,
					b.unstable,
					b.Scores as Variable,
					cats(b.Scores,"(",a.scores,")") as Full_Name
				from RPC_7Days_2_&Exist as B
					Left Join  RPC_7Days_1_&Exist as A
						On a.Month = b.Month and a.Percent = b.Percent and a.VariableName = b.VariableName
						Order By Month,VariableName;
		Quit;

		%Calc_Gini (Predicted_col=bnm_Prob, Results_table=BNM_RPC_Variable_&Call, Target_Variable=RPC_Target, Gini_output=Model_Gini_&Exist);

		Data Model_Gini_&Exist._2;
			Set Model_Gini_&Exist (Keep=Gini);
			Type = 'RPC';
			Date = &Exist2;
		Run;

		Proc Append Data=Model_Gini_&Exist._2 base=Data.Model_Gini_&Month force;
		Run;

		%Calc_Gini (Predicted_col=bnm_Prob, Results_table=data.BNM_RPC_All_Ranked_&Month, Target_Variable=RPC_Target, Gini_output=Monthly_RPC_Gini_&Month);

		Data data.Monthly_RPC_Gini_&Month;
		Set Monthly_RPC_Gini_&Month(Keep=Gini);
		Month = &Month1;
		Type = 'RPC';
		Run;


		%Calc_Gini (Predicted_col=isrpc90, Results_table=BNM_RPC_Variable_&Call, Target_Variable=RPC_Target, Gini_output=Variable_Gini_1_&Exist);
		%Calc_Gini (Predicted_col=istbclilent, Results_table=BNM_RPC_Variable_&Call, Target_Variable=RPC_Target, Gini_output=Variable_Gini_2_&Exist);
		%Calc_Gini (Predicted_col=avg_lineduration, Results_table=BNM_RPC_Variable_&Call, Target_Variable=RPC_Target, Gini_output=Variable_Gini_3_&Exist);
		%Calc_Gini (Predicted_col=failed30, Results_table=BNM_RPC_Variable_&Call, Target_Variable=RPC_Target, Gini_output=Variable_Gini_4_&Exist);
		%Calc_Gini (Predicted_col=tot_callduration, Results_table=BNM_RPC_Variable_&Call, Target_Variable=RPC_Target, Gini_output=Variable_Gini_5_&Exist);
		%Calc_Gini (Predicted_col=isrpc30, Results_table=BNM_RPC_Variable_&Call, Target_Variable=RPC_Target, Gini_output=Variable_Gini_6_&Exist);
		%Calc_Gini (Predicted_col=tot_calldropped90, Results_table=BNM_RPC_Variable_&Call, Target_Variable=RPC_Target, Gini_output=Variable_Gini_7_&Exist);
		%Calc_Gini (Predicted_col=isrpc7, Results_table=BNM_RPC_Variable_&Call, Target_Variable=RPC_Target, Gini_output=Variable_Gini_8_&Exist);
		%Calc_Gini (Predicted_col=faileddaydiff, Results_table=BNM_RPC_Variable_&Call, Target_Variable=RPC_Target, Gini_output=Variable_Gini_9_&Exist);
		%Calc_Gini (Predicted_col=tot_callback45, Results_table=BNM_RPC_Variable_&Call, Target_Variable=RPC_Target, Gini_output=Variable_Gini_10_&Exist);
		%Calc_Gini (Predicted_col=tot_failed45, Results_table=BNM_RPC_Variable_&Call, Target_Variable=RPC_Target, Gini_output=Variable_Gini_11_&Exist);

		Data Variable_Gini_1_&Exist._2;
			Set Variable_Gini_1_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'isrpc90';
		Run;

		Data Variable_Gini_2_&Exist._2;
			Set Variable_Gini_2_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'istbclilent';
		Run;

		Data Variable_Gini_3_&Exist._2;
			Set Variable_Gini_3_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'avg_lineduration';
		Run;

		Data Variable_Gini_4_&Exist._2;
			Set Variable_Gini_4_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'failed30';
		Run;

		Data Variable_Gini_5_&Exist._2;
			Set Variable_Gini_5_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'tot_callduration';
		Run;

		Data Variable_Gini_6_&Exist._2;
			Set Variable_Gini_6_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'isrpc30';
		Run;

		Data Variable_Gini_7_&Exist._2;
			Set Variable_Gini_7_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'tot_calldropped90';
		Run;

		Data Variable_Gini_8_&Exist._2;
			Set Variable_Gini_8_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'isrpc7';
		Run;

		Data Variable_Gini_9_&Exist._2;
			Set Variable_Gini_9_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'faileddaydiff';
		Run;

		Data Variable_Gini_10_&Exist._2;
			Set Variable_Gini_10_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'tot_callback45';
		Run;

		Data Variable_Gini_11_&Exist._2;
			Set Variable_Gini_11_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'tot_failed45';
		Run;

		Data Variable_Gini_All_&Exist;
			format Variable $15.;
			set Variable_Gini_1_&Exist._2 
				Variable_Gini_2_&Exist._2 
				Variable_Gini_3_&Exist._2
				Variable_Gini_4_&Exist._2
				Variable_Gini_5_&Exist._2
				Variable_Gini_6_&Exist._2
				Variable_Gini_7_&Exist._2
				Variable_Gini_8_&Exist._2
				Variable_Gini_9_&Exist._2
				Variable_Gini_10_&Exist._2
				Variable_Gini_11_&Exist._2;
		Run;

		Proc Append data=Variable_Gini_All_&Exist base=Data.RPC_Variable_Gini_All_&Month force;
		Run;
/****************************************TPC***********************************************************/
		Proc SQl;
			Create Table BNM_TPC_Variable_&Call as
				Select 
					&Exist2 as Date,
					a.*,
					b.p_target1 as tpc_Prob,
					b.tpcdaydiff,
					b.tpcdaydiff_B,	
					b.tpcdaydiff_W,	
					b.recentfcode,
					b.recentfcode_B,
					b.recentfcode_W,
					b.tot_failed90,
					b.tot_failed90_B,
					b.tot_failed90_W,
					b.avg_lineduration,
					b.avg_lineduration_B,
					b.avg_lineduration_W,
					b.avg_callduration,
					b.avg_callduration_B,
					b.avg_callduration_W,
					b.busy,
					b.busy_B,
					b.busy_W,
					b.TU_Home_Rank,
					b.TU_Home_Rank_B,
					b.TU_Home_Rank_W,
					b.tot_tpc30,
					b.tot_tpc30_B,
					b.tot_tpc30_W,
					b.tbcelldaydiff,
					b.tbcelldaydiff_B,
					b.tbcelldaydiff_W,
					b.tot_noanswer60,
					b.tot_noanswer60_B,
					b.tot_noanswer60_W,
					b.tpc,
					b.tpc_B,
					b.tpc_W,
					b.tot_calldropped90,
					b.tot_calldropped90_B,
					b.tot_calldropped90_W,
					b.tot_callback7,
					b.tot_callback7_B,
					b.tot_callback7_W
				from  BNM_Calls_&Call._3 as A
					Inner Join dev_cont.data_scored_tpc_&Exist as B
						On a.IDNumber = b.IDNumber and a.PhoneNumber = b.Number;
		Quit;

		

		Data BNM_TPC_Variable_&Call._Rank;
		set BNM_TPC_Variable_&Call;
		Month = &Month1;
		If TPC_Prob < 0.0058815607 Then Decile = 0;
		Else If TPC_Prob < 0.0059886279 Then Decile = 1;
		Else If TPC_Prob < 0.0061340658 Then Decile = 2;
		Else If TPC_Prob < 0.0098669748 Then Decile = 3;
		Else If TPC_Prob < 0.0121990913 Then Decile = 4;
		Else If TPC_Prob < 0.0155923001 Then Decile = 5;
		Else If TPC_Prob < 0.0242441114 Then Decile = 6;
		Else If TPC_Prob < 0.0403076763 Then Decile = 7;
		Else If TPC_Prob < 0.0911581116 Then Decile = 8;
		Else If TPC_Prob > 0.0911581116 Then Decile = 9;
		Run;

		Proc Append Data= BNM_TPC_Variable_&Call._Rank base=data.BNM_TPC_All_Ranked_&Month Force;
		Run;

		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=tpcdaydiff,psi_var=tpcdaydiff_B,outputdataset=TPC_View_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=recentfcode,psi_var=recentfcode_B,outputdataset=TPC_View_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=tot_failed90,psi_var=tot_failed90_B,outputdataset=TPC_View_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=avg_lineduration,psi_var=avg_lineduration_B,outputdataset=TPC_View_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=avg_callduration,psi_var=avg_callduration_B,outputdataset=TPC_View_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=busy,psi_var=busy_B,outputdataset=TPC_View_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=TU_Home_Rank,psi_var=TU_Home_Rank_B,outputdataset=TPC_View_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=tot_tpc30,psi_var=tot_tpc30_B,outputdataset=TPC_View_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=tbcelldaydiff,psi_var=tbcelldaydiff_B,outputdataset=TPC_View_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=tot_noanswer60,psi_var=tot_noanswer60_B,outputdataset=TPC_View_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=tpc,psi_var=tpc_B,outputdataset=TPC_View_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=tot_calldropped90,psi_var=tot_calldropped90_B,outputdataset=TPC_View_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=tot_callback7,psi_var=tot_callback7_B,outputdataset=TPC_View_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=tpcdaydiff,psi_var=tpcdaydiff_W,outputdataset=TPC_View2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=recentfcode,psi_var=recentfcode_W,outputdataset=TPC_View2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=tot_failed90,psi_var=tot_failed90_W,outputdataset=TPC_View2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=avg_lineduration,psi_var=avg_lineduration_W,outputdataset=TPC_View2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=avg_callduration,psi_var=avg_callduration_W,outputdataset=TPC_View2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=busy,psi_var=busy_W,outputdataset=TPC_View2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=TU_Home_Rank,psi_var=TU_Home_Rank_W,outputdataset=TPC_View2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=tot_tpc30,psi_var=tot_tpc30_W,outputdataset=TPC_View2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=tbcelldaydiff,psi_var=tbcelldaydiff_W,outputdataset=TPC_View2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=tot_noanswer60,psi_var=tot_noanswer60_W,outputdataset=TPC_View2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=tpc,psi_var=tpc_W,outputdataset=TPC_View2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=tot_calldropped90,psi_var=tot_calldropped90_W,outputdataset=TPC_View2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=BNM_TPC_Variable_&Call,period=Date,var=tot_callback7,psi_var=tot_callback7_W,outputdataset=TPC_View2_&Exist);

		Proc SQl;
			Create Table TPC_Graph_&Exist as
				Select 
					a.*,
					b.Scores as Variable,
					cats(b.Scores,"(",a.scores,")") as Full_Name
				from TPC_View_1_&Exist as A
					Left Join  TPC_View2_&Exist as B
						On a.Date = b.Date and a.Percent = b.Percent and a.VariableName = b.VariableName
					Where a.Date <> ' BUILD'
						Order By Date,VariableName;
		Quit;

		Proc Append data=TPC_Graph_&Exist base=data.TPC_PSI_&Month force;
		Run;

		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=tpcdaydiff,psi_var=tpcdaydiff_B,outputdataset=TPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=recentfcode,psi_var=recentfcode_B,outputdataset=TPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=tot_failed90,psi_var=tot_failed90_B,outputdataset=TPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=avg_lineduration,psi_var=avg_lineduration_B,outputdataset=TPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=avg_callduration,psi_var=avg_callduration_B,outputdataset=TPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=busy,psi_var=busy_B,outputdataset=TPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=TU_Home_Rank,psi_var=TU_Home_Rank_B,outputdataset=TPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=tot_tpc30,psi_var=tot_tpc30_B,outputdataset=TPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=tbcelldaydiff,psi_var=tbcelldaydiff_B,outputdataset=TPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=tot_noanswer60,psi_var=tot_noanswer60_B,outputdataset=TPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=tpc,psi_var=tpc_B,outputdataset=TPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=tot_calldropped90,psi_var=tot_calldropped90_B,outputdataset=TPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=tot_callback7,psi_var=tot_callback7_B,outputdataset=TPC_Month_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=tpcdaydiff,psi_var=tpcdaydiff_W,outputdataset=TPC_Month2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=recentfcode,psi_var=recentfcode_W,outputdataset=TPC_Month2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=tot_failed90,psi_var=tot_failed90_W,outputdataset=TPC_Month2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=avg_lineduration,psi_var=avg_lineduration_W,outputdataset=TPC_Month2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=avg_callduration,psi_var=avg_callduration_W,outputdataset=TPC_Month2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=busy,psi_var=busy_W,outputdataset=TPC_Month2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=TU_Home_Rank,psi_var=TU_Home_Rank_W,outputdataset=TPC_Month2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=tot_tpc30,psi_var=tot_tpc30_W,outputdataset=TPC_Month2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=tbcelldaydiff,psi_var=tbcelldaydiff_W,outputdataset=TPC_Month2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=tot_noanswer60,psi_var=tot_noanswer60_W,outputdataset=TPC_Month2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=tpc,psi_var=tpc_W,outputdataset=TPC_Month2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=tot_calldropped90,psi_var=tot_calldropped90_W,outputdataset=TPC_Month2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=data.BNM_TPC_All_Ranked_&Month,period=Month,var=tot_callback7,psi_var=tot_callback7_W,outputdataset=TPC_Month2_&Exist);


		
		Proc SQl;
			Create Table Data.TPC_PSI_Monthly_&Month as
				Select 
					a.*,
					b.Scores as Variable,
					cats(b.Scores,"(",a.scores,")") as Full_Name
				from TPC_Month_1_&Exist as A
					Left Join  TPC_Month2_&Exist as B
						On a.Month = b.Month and a.Percent = b.Percent and a.VariableName = b.VariableName
						Order By Month,VariableName;
		Quit;

		
		Proc SQl;
		Create Table TPC_Data_3Days as
		Select * 
		from data.BNM_TPC_All_Ranked_&Month
		Where Date between &Three and &Exist2;
		Quit;


		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=tpcdaydiff,psi_var=tpcdaydiff_B,outputdataset=TPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=recentfcode,psi_var=recentfcode_B,outputdataset=TPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=tot_failed90,psi_var=tot_failed90_B,outputdataset=TPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=avg_lineduration,psi_var=avg_lineduration_B,outputdataset=TPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=avg_callduration,psi_var=avg_callduration_B,outputdataset=TPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=busy,psi_var=busy_B,outputdataset=TPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=TU_Home_Rank,psi_var=TU_Home_Rank_B,outputdataset=TPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=tot_tpc30,psi_var=tot_tpc30_B,outputdataset=TPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=tbcelldaydiff,psi_var=tbcelldaydiff_B,outputdataset=TPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=tot_noanswer60,psi_var=tot_noanswer60_B,outputdataset=TPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=tpc,psi_var=tpc_B,outputdataset=TPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=tot_calldropped90,psi_var=tot_calldropped90_B,outputdataset=TPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=tot_callback7,psi_var=tot_callback7_B,outputdataset=TPC_3Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=tpcdaydiff,psi_var=tpcdaydiff_W,outputdataset=TPC_3Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=recentfcode,psi_var=recentfcode_W,outputdataset=TPC_3Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=tot_failed90,psi_var=tot_failed90_W,outputdataset=TPC_3Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=avg_lineduration,psi_var=avg_lineduration_W,outputdataset=TPC_3Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=avg_callduration,psi_var=avg_callduration_W,outputdataset=TPC_3Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=busy,psi_var=busy_W,outputdataset=TPC_3Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=TU_Home_Rank,psi_var=TU_Home_Rank_W,outputdataset=TPC_3Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=tot_tpc30,psi_var=tot_tpc30_W,outputdataset=TPC_3Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=tbcelldaydiff,psi_var=tbcelldaydiff_W,outputdataset=TPC_3Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=tot_noanswer60,psi_var=tot_noanswer60_W,outputdataset=TPC_3Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=tpc,psi_var=tpc_W,outputdataset=TPC_3Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=tot_calldropped90,psi_var=tot_calldropped90_W,outputdataset=TPC_3Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_3Days,period=Month,var=tot_callback7,psi_var=tot_callback7_W,outputdataset=TPC_3Days2_&Exist);

		Proc SQl;
			Create Table Data.TPC_PSI_3Days_&Month as
				Select 
					a.*,
					b.Scores as Variable,
					cats(b.Scores,"(",a.scores,")") as Full_Name
				from TPC_3Days_1_&Exist as A
					Left Join  TPC_3Days2_&Exist as B
						On a.Month = b.Month and a.Percent = b.Percent and a.VariableName = b.VariableName
						Order By Month,VariableName;
		Quit;

		Proc SQl;
		Create Table TPC_Data_7Days as
		Select * from data.BNM_TPC_All_Ranked_&Month
		Where Date between &Seven and &Exist2;
		Quit;

		
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=tpcdaydiff,psi_var=tpcdaydiff_B,outputdataset=TPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=recentfcode,psi_var=recentfcode_B,outputdataset=TPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=tot_failed90,psi_var=tot_failed90_B,outputdataset=TPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=avg_lineduration,psi_var=avg_lineduration_B,outputdataset=TPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=avg_callduration,psi_var=avg_callduration_B,outputdataset=TPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=busy,psi_var=busy_B,outputdataset=TPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=TU_Home_Rank,psi_var=TU_Home_Rank_B,outputdataset=TPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=tot_tpc30,psi_var=tot_tpc30_B,outputdataset=TPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=tbcelldaydiff,psi_var=tbcelldaydiff_B,outputdataset=TPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=tot_noanswer60,psi_var=tot_noanswer60_B,outputdataset=TPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=tpc,psi_var=tpc_B,outputdataset=TPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=tot_calldropped90,psi_var=tot_calldropped90_B,outputdataset=TPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=tot_callback7,psi_var=tot_callback7_B,outputdataset=TPC_7Days_1_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=tpcdaydiff,psi_var=tpcdaydiff_W,outputdataset=TPC_7Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=recentfcode,psi_var=recentfcode_W,outputdataset=TPC_7Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=tot_failed90,psi_var=tot_failed90_W,outputdataset=TPC_7Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=avg_lineduration,psi_var=avg_lineduration_W,outputdataset=TPC_7Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=avg_callduration,psi_var=avg_callduration_W,outputdataset=TPC_7Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=busy,psi_var=busy_W,outputdataset=TPC_7Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=TU_Home_Rank,psi_var=TU_Home_Rank_W,outputdataset=TPC_7Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=tot_tpc30,psi_var=tot_tpc30_W,outputdataset=TPC_7Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=tbcelldaydiff,psi_var=tbcelldaydiff_W,outputdataset=TPC_7Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=tot_noanswer60,psi_var=tot_noanswer60_W,outputdataset=TPC_7Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=tpc,psi_var=tpc_W,outputdataset=TPC_7Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=tot_calldropped90,psi_var=tot_calldropped90_W,outputdataset=TPC_7Days2_&Exist);
		%psi_calculation(build=data.scored_tpc_build_ranked,base=TPC_Data_7Days,period=Month,var=tot_callback7,psi_var=tot_callback7_W,outputdataset=TPC_7Days2_&Exist);


				Proc SQl;
			Create Table Data.TPC_PSI_7Days_&Month as
				Select 
					a.*,
					b.Scores as Variable,
					cats(b.Scores,"(",a.scores,")") as Full_Name
				from TPC_7Days_1_&Exist as A
					Left Join  TPC_7Days2_&Exist as B
						On a.Month = b.Month and a.Percent = b.Percent and a.VariableName = b.VariableName
						Order By Month,VariableName;
		Quit;


		%Calc_Gini (Predicted_col=tpc_Prob, Results_table=BNM_TPC_Variable_&Call, Target_Variable=TPC_Target, Gini_output=TPC_Model_Gini_&Exist);

		Data TPC_Model_Gini_&Exist._2;
			Set TPC_Model_Gini_&Exist (Keep=Gini);
			Type = 'TPC';
			Date = &Exist2;
		Run;

		Proc Append Data=TPC_Model_Gini_&Exist._2 base=Data.Model_Gini_&Month force;
		Run;

		
		%Calc_Gini (Predicted_col=tpc_Prob, Results_table=data.BNM_TPC_All_Ranked_&Month, Target_Variable=TPC_Target, Gini_output=Monthly_TPC_Gini_&Month);

		Data data.Monthly_TPC_Gini_&Month;
		Set Monthly_TPC_Gini_&Month(Keep=Gini);
		Month = &Month1;
		Type = 'TPC';
		Run;

		%Calc_Gini (Predicted_col=tpcdaydiff, Results_table=BNM_TPC_Variable_&Call, Target_Variable=TPC_Target, Gini_output=TPC_Variable1_Gini_&Exist);
		/*%Calc_Gini (Predicted_col=recentfcode, Results_table=BNM_TPC_Variable_&Call, Target_Variable=TPC_Target, Gini_output=TPC_Variable2_Gini_&Exist);*/
		%Calc_Gini (Predicted_col=tot_failed90, Results_table=BNM_TPC_Variable_&Call, Target_Variable=TPC_Target, Gini_output=TPC_Variable3_Gini_&Exist);
		%Calc_Gini (Predicted_col=avg_lineduration, Results_table=BNM_TPC_Variable_&Call, Target_Variable=TPC_Target, Gini_output=TPC_Variable4_Gini_&Exist);
		%Calc_Gini (Predicted_col=avg_callduration, Results_table=BNM_TPC_Variable_&Call, Target_Variable=TPC_Target, Gini_output=TPC_Variable5_Gini_&Exist);
		%Calc_Gini (Predicted_col=busy, Results_table=BNM_TPC_Variable_&Call, Target_Variable=TPC_Target, Gini_output=TPC_Variable6_Gini_&Exist);
		%Calc_Gini (Predicted_col=TU_Home_Rank, Results_table=BNM_TPC_Variable_&Call, Target_Variable=TPC_Target, Gini_output=TPC_Variable7_Gini_&Exist);
		%Calc_Gini (Predicted_col=tot_tpc30, Results_table=BNM_TPC_Variable_&Call, Target_Variable=TPC_Target, Gini_output=TPC_Variable8_Gini_&Exist);
		%Calc_Gini (Predicted_col=tbcelldaydiff, Results_table=BNM_TPC_Variable_&Call, Target_Variable=TPC_Target, Gini_output=TPC_Variable9_Gini_&Exist);
		%Calc_Gini (Predicted_col=tot_noanswer60, Results_table=BNM_TPC_Variable_&Call, Target_Variable=TPC_Target, Gini_output=TPC_Variable10_Gini_&Exist);
		%Calc_Gini (Predicted_col=tpc, Results_table=BNM_TPC_Variable_&Call, Target_Variable=TPC_Target, Gini_output=TPC_Variable11_Gini_&Exist);
		%Calc_Gini (Predicted_col=tot_calldropped90, Results_table=BNM_TPC_Variable_&Call, Target_Variable=TPC_Target, Gini_output=TPC_Variable12_Gini_&Exist);
		%Calc_Gini (Predicted_col=tot_callback7, Results_table=BNM_TPC_Variable_&Call, Target_Variable=TPC_Target, Gini_output=TPC_Variable13_Gini_&Exist);

		Data TPC_Variable1_Gini_&Exist._2;
			set TPC_Variable1_Gini_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'tpcdaydiff';
		Run;

		/*Data TPC_Variable2_Gini_&Exist._2;
			set TPC_Variable2_Gini_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'recentfcode';
		Run;*/

		Data TPC_Variable3_Gini_&Exist._2;
			set TPC_Variable3_Gini_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'tot_failed90';
		Run;

		Data TPC_Variable4_Gini_&Exist._2;
			set TPC_Variable4_Gini_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'avg_lineduration';
		Run;

		Data TPC_Variable5_Gini_&Exist._2;
			set TPC_Variable5_Gini_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'avg_callduration';
		Run;

		Data TPC_Variable6_Gini_&Exist._2;
			set TPC_Variable6_Gini_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'busy';
		Run;

		Data TPC_Variable7_Gini_&Exist._2;
			set TPC_Variable7_Gini_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'TU_Home_Rank';
		Run;

		Data TPC_Variable8_Gini_&Exist._2;
			set TPC_Variable8_Gini_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'tot_tpc30';
		Run;

		Data TPC_Variable9_Gini_&Exist._2;
			set TPC_Variable9_Gini_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'tbcelldaydiff';
		Run;

		Data TPC_Variable10_Gini_&Exist._2;
			set TPC_Variable10_Gini_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'tot_noanswer60';
		Run;

		Data TPC_Variable11_Gini_&Exist._2;
			set TPC_Variable11_Gini_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'tpc';
		Run;

		Data TPC_Variable12_Gini_&Exist._2;
			set TPC_Variable12_Gini_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'tot_calldropped90';
		Run;

		Data TPC_Variable13_Gini_&Exist._2;
			set TPC_Variable13_Gini_&Exist (Keep=Gini);
			Date = &Exist2;
			Variable = 'tot_callback7';
		Run;

		Data TPC_Variable_Gini_All_&Exist;
			Format Variable $20.;
			set TPC_Variable1_Gini_&Exist._2
				TPC_Variable3_Gini_&Exist._2
				TPC_Variable4_Gini_&Exist._2
				TPC_Variable5_Gini_&Exist._2
				TPC_Variable6_Gini_&Exist._2
				TPC_Variable7_Gini_&Exist._2
				TPC_Variable8_Gini_&Exist._2
				TPC_Variable9_Gini_&Exist._2
				TPC_Variable10_Gini_&Exist._2
				TPC_Variable11_Gini_&Exist._2
				TPC_Variable12_Gini_&Exist._2
				TPC_Variable13_Gini_&Exist._2;
		Run;

		Proc Append data=TPC_Variable_Gini_All_&Exist base=TPC_Variable_Gini_All_&Month force;
		Run;

	

		/*Combined Monthly Gini*/

		Data Monthly_Gini;
		Set data.Monthly_RPC_Gini_: data.Monthly_TPC_Gini_:;
		Run;

		/*Combine Monthly PSI*/

		Data Monthly_RPC_PSI;
		Set data.RPC_PSI_MONTHLY_:;
		Run;

		Data TPC_PSI_MONTHLY;
		Set data.TPC_PSI_MONTHLY_:;
		Run;

		/*PDF Report*/
		Proc format;
			Value NIND 
				low-<1.0='Red'
				1-high='Green';
		run;

		Proc format;
			Value DIND 
				low-<1.0='Red'
				1-high='Green';
		run;

		Proc format;
			Value TIND 
				low-<1.0='Red'
				1-high='Green';
		run;

		Proc format;
			Value Robot 
				low-<0.1='Green'
				0.1-<0.25='Yellow'
				0.25-high='Red';
		run;

		%Macro Graph(Var=,data=,axis=);
			Title "&Var";
			Symbol1 interpol=join height=10pt value=star  line=1 width=2 cv=bib;
			Symbol2 interpol=join height=10pt value=plus line=1 width=2 cv=gold;
			Symbol3 interpol=join height=10pt value=diamond line=1 width=2 cv=red;
			Legend1 frame;
			Legend2 frame;
			Axis1 style=1 width=1 minor=none label=("%");
			axis2 style=1 width=1;
			axis3 style=1 width=1 minor=none order=(0.00 to &axis by 0.10)  Label=("PSI %");

			Proc GbarLine data=&data;
				Where variablename = UPCASE("&Var");
				Bar Date / sumvar=percent subgroup=Full_Name frame type=mean coutline=black raxis=axis1 maxis=axis2 legend=legend2;
				plot / sumvar=psi type=mean axis=axis3 legend=legend1;
				plot /sumvar=marginal_stable type=mean axis=axis3;
				plot / sumvar=unstable type=mean axis=axis3;
				footnote '';
			Run;

		%Mend;

		ods graphics off;
		ods _all_ close;
		/*PDF Report*/
		options  NODATE center orientation=landscape;  
		ODS PDF File="\\mpwsas64\KMcCleanAdmin\BNM\New BNM Monitoring\Reports\BNM_Sample_&Month..pdf" notoc;
		ods pdf startpage=yes;
		Title;
		Footnote;

		proc gslide border
			cframe="dark blue"
			wframe=4;
			note height=14;
			note height=5
				justify=center 
				color="black" 
				"BNM  Monitoring"
				justify=center 
				"Month:&Month";
		run;

		/*Proc Report Data=data.BNM_Table_Check_&Month nowd out=Report;
			Title 'Table Run Check';
			column Date Table_INd;
			Define Table_Ind / Mean Style={background=TINd.};
		Run;

		Proc Report data=data.BNM_ID_Shift_&Month nowd out=Report;
			Title 'BNM Change In ID';
			Column Date Total Ind N_Ind D_Ind;
			Define Total / format=comma9.;
			Define Ind / format=comma9.;
			Define N_Ind /Mean style={background=NIND.} format=comma5.;
			Define D_Ind /Mean style={background=DIND.} format=comma5.;
		Run;

		Proc Report Data=data.BNM_New_Num_&Month nowd out=report;
			Title 'BNM Change In Numbers';
			Column Date Total Ind N_Ind D_Ind;
			Define Total / format=comma9.;
			Define Ind / format=comma9.;
			Define N_Ind /Mean style={background=NIND.} format=comma7.;
			Define D_Ind /Mean style={background=DIND.} format=comma7.;
		Run;

		Proc Report Data=data.BNM_Avg_Nums_ID_&Month nowd out=Report;
			Title 'Average Numeber of Numbers Per ID';
			Column Date Avg_Nums_Per_ID Total_ID Total_Numbers;
			define Avg_Nums_Per_ID / format=comma.1;
			Define Total_ID / format=comma9.;
			Define Total_Numbers / format=comma9.;
		Run;

		Proc SGplot data=Data.BNM_Tot_Number_Split_&Month;
			Title 'Percentage Distribution of Numbers Per Client';
			yaxis label='Percent';
			Vbar Day / response=Percent group=Total_Per_ID groupdisplay=stack;
		Run;

		Proc SGplot data=Data.BNM_Score_Stats_&Month;
			Title 'Percentage Distribution of Score Change';
			yaxis label='Percent';
			Vbar Day / response=Percentage group=Score_Ind groupdisplay=stack;
		Run;

		Proc SGplot data=Data.BNM_Record_Shift_&Month;
			Title 'Percentage Distribution of Client Record Change';
			yaxis label='Percent';
			Vbar Day/ response=Percentage group=Client_Ind groupdisplay=stack;
		Run;*/


		/*Monitoring*/
		Proc SGplot data=Monthly_Gini;
			Title 'RPC and TPC Gini Monthly';
			yaxis values=(0.00 to 1.00 by 0.10) label='Gini';
			Vline Month /response=Gini group=Type markers;
			refline 0.7999197888 / axis=y lineattrs=(color=blue) label=("RPC Gini");
			refline 0.7077851722 / axis=y lineattrs=(color=darkred) label=("TPC Gini") ;
		Run;

		Proc Sgplot data=Data.Model_Gini_&Month;
			Where Date <> 'Build';
			Title 'RPC and TPC Model Gini Per Day ';
			yaxis values=(0.00 to 1.00 by 0.10) label='Gini';
			Vline Date /response=Gini group=Type markers;
			refline 0.7999197888 / axis=y lineattrs=(color=blue) label=("RPC Gini");
			refline 0.7077851722 / axis=y lineattrs=(color=darkred) label=("TPC Gini") ;
		Run;

		Proc Report data=data.RPC_PSI_&Month nowd out=Report;
			Where Date <> ' BUILD';
			Title 'RPC Model CSI';
			Column variablename Date,PSI;
			Define Variablename / group;
			define Date / across;
			Define PSI /Mean style={background=ROBOT.};
		Run;

		Proc Report Data=Monthly_RPC_PSI;
			Where Month <> ' BUILD';
			Title 'RPC Model CSI Monthly';
			Column variablename Month,PSI;
			Define Variablename / group;
			define Month / across;
			Define PSI /Mean style={background=ROBOT.};
		Run;

		Proc Report Data=data.rpc_psi_3days_&Month;
			Where Month <> ' BUILD';
			Title 'RPC Model CSI 3 Days';
			Column variablename Month,PSI;
			Define Variablename / group;
			define Month / across;
			Define PSI /Mean style={background=ROBOT.};
		Run;

		Proc Report Data=data.rpc_psi_7days_&Month;
			Where Month <> ' BUILD';
			Title 'RPC Model CSI 7 Days';
			Column variablename Month,PSI;
			Define Variablename / group;
			define Month / across;
			Define PSI /Mean style={background=ROBOT.};
		Run;


		%Graph(Var=isrpc90,data=data.RPC_PSI_&Month,axis=2);
		%Graph(Var=istbclilent,data=data.RPC_PSI_&Month,axis=2);
		%Graph(Var=avg_lineduration,data=data.RPC_PSI_&Month,axis=2);
		%Graph(Var=failed30,data=data.RPC_PSI_&Month,axis=2);
		%Graph(Var=tot_callduration,data=data.RPC_PSI_&Month,axis=2);
		%Graph(Var=isrpc30,data=data.RPC_PSI_&Month,axis=2);
		%Graph(Var=tot_calldropped90,data=data.RPC_PSI_&Month,axis=2);
		%Graph(Var=isrpc7,data=data.RPC_PSI_&Month,axis=2);
		%Graph(Var=faileddaydiff,data=data.RPC_PSI_&Month,axis=2);
		%Graph(Var=tot_callback45,data=data.RPC_PSI_&Month,axis=2);
		%Graph(Var=tot_failed45,data=data.RPC_PSI_&Month,axis=2)


		Proc Report data=data.TPC_PSI_&Month nowd out=Report;
			Where Date <> ' BUILD';
			Title 'TPC Model CSI';
			Column variablename Date,PSI;
			Define Variablename / group;
			define Date / across;
			Define PSI /Mean style={background=ROBOT.};
		Run;


		Proc Report Data=TPC_PSI_MONTHLY;
			Where Month <> ' BUILD';
			Title 'TPC Model CSI Monthly';
			Column variablename Month,PSI;
			Define Variablename / group;
			define Month / across;
			Define PSI /Mean style={background=ROBOT.};
		Run;

		Proc Report Data=data.TPC_psi_3days_&Month;
			Where Month <> ' BUILD';
			Title 'TPC Model CSI 3 Days';
			Column variablename Month,PSI;
			Define Variablename / group;
			define Month / across;
			Define PSI /Mean style={background=ROBOT.};
		Run;

		Proc Report Data=data.TPC_psi_7days_&Month;
			Where Month <> ' BUILD';
			Title 'TPC Model CSI 7 Days';
			Column variablename Month,PSI;
			Define Variablename / group;
			define Month / across;
			Define PSI /Mean style={background=ROBOT.};
		Run;

		%Graph(Var=tpcdaydiff,data=data.TPC_PSI_&Month,axis=2);
		%Graph(Var=recentfcode,data=data.TPC_PSI_&Month,axis=2);
		%Graph(Var=tot_failed90,data=data.TPC_PSI_&Month,axis=2);
		%Graph(Var=avg_lineduration,data=data.TPC_PSI_&Month,axis=2);
		%Graph(Var=avg_callduration,data=data.TPC_PSI_&Month,axis=2);
		%Graph(Var=busy,data=data.TPC_PSI_&Month,axis=2);
		%Graph(Var=TU_Home_Rank,data=data.TPC_PSI_&Month,axis=2);
		%Graph(Var=tot_tpc30,data=data.TPC_PSI_&Month,axis=2);
		%Graph(Var=tbcelldaydiff,data=data.TPC_PSI_&Month,axis=2);
		%Graph(Var=tot_noanswer60,data=data.TPC_PSI_&Month,axis=2);
		%Graph(Var=tpc,data=data.TPC_PSI_&Month,axis=2);
		%Graph(Var=tot_calldropped90,data=data.TPC_PSI_&Month,axis=2);
		%Graph(Var=tot_callback7,data=data.TPC_PSI_&Month,axis=2);

		/*Proc Report Data=Data.BNM_Pop_Split_&Month;
			Title 'Champion and Challenger Data Split';
			Column Date C1_Percent	C2_Percent	CH_Percent WO_1_Percent	WO_2_Percent;
			Define C1_Percent / format=comma.1;
			Define C2_Percent / format=comma.1;
			Define CH_Percent / format=comma.1;
			Define WO_1_Percent / format=comma.1;
			Define WO_2_Percent / format=comma.1;
		Run;

		Proc Sgplot data=data.BNM_Tot_Number_Split_&Month;
			Title 'Distribution Of Numbers Per Client';
			yaxis label='Percentage Distribution';
			Vbar Day / Response=Percent Group=Total_Per_ID groupdisplay=stack;
		Run;

		Proc Sgplot data=data.BNM_Score_Stats_&Month;
			Title 'Percentage Change in Score';
			Yaxis label='Percentage';
			Vbar Day / Response=Percentage Group=Score_ind Groupdisplay=stack;
		Run;

		Proc Sgplot data=data.BNM_Record_Shift_&Month;
			Title 'Percentage Change in Clients';
			Yaxis label='Percentage';
			Vbar Day / Response=Percentage Group=Client_ind Groupdisplay=stack;
			Footnote '1 = Clients Who Remained the Same 2 = Clients Who Change';
		Run;

		footnote;
		Proc Report data=BNM_Work_Check_2;
			Title 'Percentage OF Work Numbers Per BNM Rank';
			Column _All_;
			Define Percent_OF_Rank / format=comma.1;
		Run;*/

		ODS PDF Close;

		/*Proc Delete data=data.data_scored_bnm_&Delete;
		Run;


		Proc Delete data=data.data_scored_tpc_&Delete;
		Run;*/	
	%end;
%Mend;

%BNM_Mon_New(Input=0);






