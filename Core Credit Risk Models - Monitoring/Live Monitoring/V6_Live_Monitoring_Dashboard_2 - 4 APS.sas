/* Test Change */
%include "\\mpwsas65\Process_Automation\sas_autoexec\sas_autoexec.sas";
options compress = yes;
options compress = on;
%let todaysDate = %sysfunc(today(), yymmddn8.);

libname scoring odbc dsn=DEV_DDRe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

libname scoring2 odbc dsn=cre_scor schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

%include "\\mpwsas65\Process_Automation\Codes\Git_Repositories\Core Credit Risk Models - Monitoring\Macros\createdirectory.sas";
libname LiveDash "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard Live Monitoring\Data";

/*Main data source, pulls data for the last 15minutes*/
proc sql stimer;
	connect to ODBC (dsn=CAPRILIVEETL);
	create table TempData as 
	select * from connection to odbc 
		(
		EXEC [CAPRIBLAZE].[dbo].[stp_Get_CapriLiveMonitoringV6Data] '15'
		);
	disconnect from ODBC;
quit;
/**/

data TempData;
	set TempData;
	V6_finalriskscore_prod = scoringfinalriskscore; /*ia*/
	V6_RiskGroup_prod = scoringscoreband; /*ia*/
	V620_prod = V601;
	V621_prod = V602;
	V622_prod = V603;
	drop behavescore;
run;

data tempdata;
	set tempdata;
	rename V601 = V630_prod;
	rename V602 = V631_prod;
	rename V603 = V632_prod;
	rename V633 = V633_prod;
	rename V634 = V634_prod;
	rename V635 = V635_prod;
	rename V636 = V636_prod;
	rename V640 = V640_prod;
	rename V641 = V641_prod;
	rename V642 = V642_prod;
	rename V643 = V643_prod;
	rename V644 = V644_prod;
	rename V645 = V645_prod; 
	rename V648 = V648_prod;
	rename V650 = V650_prod;
	rename V651 = V651_prod;
	rename V652 = V652_prod;
	rename V653 = V653_prod;
	rename V654 = V654_prod;
	rename V655 = V655_prod; 
	rename V658 = V658_prod; 
	if (scorecardversion = "V645" and V645 = " ") or (scorecardversion = "V636" and V636 = " ") or (scorecardversion = "V655" and V655 = " ") then scoreBlank = 1;
	else scoreBlank = 0;
	if scoreBlank = 0;
	if channelcode ne 'CCC006';
	if channelcode ne 'CCC023';
run;

/*Sourcing behavescore*/
proc sql;
	create table tempdata2 as 
	select distinct * 
	from tempdata;
quit;

proc sort data = tempdata2; by tranappnum descending uniqueid; run;
proc sort data = tempdata2 nodupkey; by tranappnum; run;


data V6_live_scorecardversion;
	set tempdata2 (keep=tranappnum uniqueid scorecardversion) scoring.V6_live_scorecardversion;
run;

proc sort data = V6_live_scorecardversion; by tranappnum descending uniqueid; run;
proc sort data = V6_live_scorecardversion nodupkey; by tranappnum; run;

/*proc sql;*/
/*	drop table scoring.V6_live_scorecardversion;*/
/*quit;*/

%Upload_APS(Set=V6_live_scorecardversion, Server = work, APS_ODBC = DEV_DDRe, APS_DB = Dev_DataDistillery_Reporting, Distribute=hash(tranappnum));


/*data scoring.V6_live_scorecardversion;*/
/*	set V6_live_scorecardversion;*/
/*run;*/

proc sql;
	create table Idnumbers as
	select distinct NationalId 
	from tempdata2;
quit;

proc sql stimer;
	connect to ODBC (dsn=MPWAPS);
	create table BehaveScore as 
	select * from connection to odbc 
		(
			Select * 
			from Prd_datadistillery.dbo.BehaveScorev2
		);
	disconnect from ODBC;
quit;

proc sort data = behavescore nodupkey ; by idno; run;

proc sql;
	create table inBehaveTable as  
	select * 
	from behavescore
	where idno in (select NationalId from Idnumbers);
quit;

proc sql;
	create table rawdata5_IN_BEHAVE as  
	select b.behavescore, b.behavescorev2, a.* 
	from tempdata2 a 
	left join inBehaveTable b
	on a.NationalId = b.idno;
quit;

data tempData3(drop= ProductOfferId ProductCategory MappedChannelCode Term);
	set rawdata5_IN_BEHAVE;
run;
/**/

/*Prep data for scoring*/
%let Varlist = AIL_NUM1PDELQ90DAYS ALL_NUM1PDELQ90DAYS ALL_NUMADVS1YEAR ALL_NUMEVERTRADES
	ALL_NUMPAYMENTS2YEARS ALL_NUMTRADES90DAYS ALL_PERCPAYMENTS2YEARS ALL_RATIOORGBALLIM1YEAR 
	ALL_TIMEMRAO ALL_TIMEMRENQ ALL_TIMEMRJUDG ALL_TIMEMRNOTICE ALL_TIMEOLDESTENQ ALL_VALORGBALLIM1YEAR 
	ALL_VALORGBALLIM90DAYS AUL_NUMOPENTRADES CALCULATEDNETTINCOME CSN_NUMTRADES90DAYS
	CSN_TIMEOLDESTTRADE	CST_CUSTOMERAGE MONTHLYGROSSINCOME MONTHLYNETTINCOME OPL_PERCPAYMENTS2YEARS
	OTH_AVGMONTHSONBOOK	OTH_PERCPAYMENTS2YEARS OTH_VALORGBALLIM OTH_VALORGBALLIM180DAYS	OWN_AVGPERCUTILISATIONMR60DAYS
	OWN_NUMOPENTRADES OWN_PERC1PDELQ2YEARS OWN_PERCUTILISATISFTRADES PRISMSCOREMI PRISMSCORETM RCG_AVGMONTHSONBOOK
	REV_NUMPAYMENTS2YEARS REV_PERCPAYMENTS180DAYS REV_PERCPAYMENTS1YEAR	UNN_AVGMONTHSONBOOK	UNS_AVGMONTHSONBOOK
	UNS_PERCUTILISATION	UNS_TIMEMR3PDELQ UNS_VALCURBAL1YEAR UNS_VALCURBALMR60DAYS VAP_GMIPVALUE;

data tempdata4;
	set tempdata3;
	AppTime = input(ApplicationTime,time5.);
	format AppTime time8.;
	AppHour = hour(AppTime);
run;

%macro renameVars;
	data tempdata5(rename =( %do_over(values =&Varlist,phrase=?_R=?, between=)));
		set tempdata4;

		%do i = 1 %to %sysfunc(countw(&Varlist));
			%let var = %scan(&Varlist, &i.);
			&var._R = input(&var., 20.);
			drop &var.;
		%end;
	run;
%mend;
%renameVars;

%let newvarlist = COMPUSCANVAR1401 COMPUSCANVAR1424 COMPUSCANVAR175 COMPUSCANVAR187 COMPUSCANVAR188
                  COMPUSCANVAR2123 COMPUSCANVAR2312 COMPUSCANVAR2528 COMPUSCANVAR2678 COMPUSCANVAR2696
                  COMPUSCANVAR3275 COMPUSCANVAR3916 COMPUSCANVAR3935 COMPUSCANVAR5208 COMPUSCANVAR5486
                  COMPUSCANVAR5489 COMPUSCANVAR5579 COMPUSCANVAR5826 COMPUSCANVAR6073 COMPUSCANVAR6130
                  COMPUSCANVAR6132 COMPUSCANVAR6134 COMPUSCANVAR6285 COMPUSCANVAR6788 COMPUSCANVAR716
                  COMPUSCANVAR733 COMPUSCANVAR7430 COMPUSCANVAR7431 COMPUSCANVAR7479 COMPUSCANVAR753
                  COMPUSCANVAR7547 COMPUSCANVAR7549 COMPUSCANVAR7550 COMPUSCANVAR7683;

%let oldvarlist = UNS_PercUtilisation UNS_ValCurBalMR60Days CSN_TimeOldestTrade ALL_TimeMREnq ALL_TimeOldestEnq
                  AUL_NumOpenTrades ALL_MaxDelq1YearLT24M OWN_Perc1pDelq2Years OTH_MaxDelqEver OTH_MaxDelq1YearLT24M 
				  REV_MaxDelq180DaysGE24M UNS_TimeMR3pDelq UNS_MaxDelq180DaysLT12M AIL_Num1pDelq90Days ALL_NumEverTrades 
				  ALL_NumTrades90Days OTH_AvgMonthsOnBook UNS_AvgMonthsOnBook RCG_AvgMonthsOnBook UNN_AvgMonthsOnBook 
				  ALL_ValOrgBalLim90Days ALL_ValOrgBalLim1Year OTH_ValOrgBalLim180Days UNS_ValCurBal1Year OWN_PercUtiliSatisfTrades
                  OWN_AvgPercUtilisationMR60Days ALL_NumPayments2Years ALL_PercPayments2Years OTH_PercPayments2Years OTH_ValOrgBalLim 
				  REV_PercPayments180Days REV_PercPayments1Year REV_NumPayments2Years OPL_PercPayments2Years;

%macro rename1(oldvarlist, newvarlist);
	%let k=1;
	%let old = %scan(&oldvarlist, &k);
	%let new = %scan(&newvarlist, &k);
	%do %while(("&old" NE "") & ("&new" NE ""));
		&new = &old;
		%let k = %eval(&k + 1);
		%let old = %scan(&oldvarlist, &k);
		%let new = %scan(&newvarlist, &k);
	%end;
%mend;

data RawData1;
	set TempData5;
	%rename1(&oldvarlist, &newvarlist);	
run;

%macro change(var=);
	%let newvar=&var._a;

	data RawData2(rename = (&newvar=&var));
		set RawData1;
		var = input(&var,12.);
		rename var=&newvar;
		drop &var;
	run;
%mend;

%change(var=PP003);
%change(var=NP003);
%change(var=EQ0012AL);
%change(var=EQ2012AL);
%change(var=EQ0015PL);
%change(var=EQ2015PL);
%change(var=DM0001AL);
%change(var=PP0406AL);
%change(var=PP0421CL);
%change(var=PP0935AL);
%change(var=PP0714AL);
%change(var=PP0327AL);
%change(var=NP013);
%change(var=PP0801AL);
%change(var=PP0325AL);
%change(var=NP015);
%change(var=PP0901AL);
%change(var=PP0051CL);
%change(var=PP0171CL);
%change(var=NG004);
%change(var=PP0601AL);
%change(var=PP0604AL);
%change(var=RE006);
%change(var=PP0407AL);
%change(var=PP149);
%change(var=PP0521LB);
%change(var=PP116);
%change(var=PP173);
%change(var=PP0603AL);
%change(var=PP0503AL);
%change(var=PP0505AL);
%change(var=PP0111LB);
%change(var=PP0313LN);
%change(var=PP0515AL);

proc sql;
	create table rawdata3 as 
	select *,
		case 
			when PP283_ADJ = '0' then '+000000000.0'
			when PP283_ADJ = '1' then '+000000001.0'
			when PP283_ADJ = '2' then '+000000002.0'
			when PP283_ADJ = '3' then '+000000003.0'
			when PP283_ADJ = '4' then '+000000004.0'
			when PP283_ADJ = '5' then '+000000005.0'
			when PP283_ADJ = '6' then '+000000006.0'
			when PP283_ADJ = '7' then '+000000007.0'
			when PP283_ADJ = '8' then '+000000008.0'
			when PP283_ADJ = '9' then '+000000009.0'
			else PP283_ADJ 
		end as PP283 format $12.         
	from rawdata2 (rename=(PP283 = PP283_ADJ));
quit;

proc sql;
	create table rawdata3 as 
	select *,
		case 
			when RE019_ADJ = '0' then '+000000000.0'
			when RE019_ADJ = '1' then '+000000001.0'
			when RE019_ADJ = '2' then '+000000002.0'
			when RE019_ADJ = '3' then '+000000003.0'
			when RE019_ADJ = '4' then '+000000004.0'
			when RE019_ADJ = '5' then '+000000005.0'
			when RE019_ADJ = '6' then '+000000006.0'
			when RE019_ADJ = '7' then '+000000007.0'
			when RE019_ADJ = '8' then '+000000008.0'
			when RE019_ADJ = '9' then '+000000009.0'
			else RE019_ADJ 
		end as RE019 format $12.         
	from rawdata3 (rename=(RE019 = RE019_ADJ));
quit;

data rawdata4;
	set rawdata3;
	format AppDate yymmddn8.;
	AppDate = input(ApplicationDate,yymmdd10.);
	Week = put(week(AppDate, 'w'),z2.);

	if BehaveScore in ( . , 0, 9999) then Repeat = 0;
	else if BehaveScore not in (., 0, 9999) then Repeat = 1;

	if (ACCREDITEDINDICATOR = 'P' OR MEMBER IN ('PERSAL','PSAL') OR  EMPLOYERGROUPCODE IN ('PERSAL','PSAL')) THEN PERSALINDICATOR = 1;
	else PERSALINDICATOR = 0;

	if SUBGROUPCREATEDATE  ne '' THEN SG_CREATIONYEAR = input(put(SUBSTR(SUBGROUPCREATEDATE,1,4),$4.),4.);
	ELSE SG_CREATIONYEAR = input(put(SUBSTR(APPLICATIONDATE,1,4),$4.),4.);

	if PERSALINDICATOR = 1 then do;
		SG_CREATIONYEAR1 = SG_CREATIONYEAR;
		SG_CREATIONYEAR2 = -10;
	end;
	else do;
		SG_CREATIONYEAR2 = SG_CREATIONYEAR;
		SG_CREATIONYEAR1 = -10;
	end;

	if CALCULATEDNETTINCOME = 0.00 then NETINCOME = MONTHLYNETTINCOME;
	else if CALCULATEDNETTINCOME < MONTHLYNETTINCOME then NETINCOME = MONTHLYNETTINCOME;
	else NETINCOME = CALCULATEDNETTINCOME;

	if WAGEFREQUENCYCODE = 'WAG001' then do;
		if MONTHLYGROSSINCOME = 0.00 then
			GROSSINCOME = NETINCOME;
		else GROSSINCOME = MONTHLYGROSSINCOME;
	end;

	if WAGEFREQUENCYCODE = 'WAG002' then do;
		if MONTHLYGROSSINCOME = 0.00 then GROSSINCOME = NETINCOME;
		else if MONTHLYGROSSINCOME < NETINCOME then GROSSINCOME =MONTHLYGROSSINCOME * 52/12;
		else GROSSINCOME = MONTHLYGROSSINCOME;
	end;

	if WAGEFREQUENCYCODE = 'WAG003' then do;
		if MONTHLYGROSSINCOME = 0.00 then GROSSINCOME = NETINCOME;
		else if MONTHLYGROSSINCOME < NETINCOME then GROSSINCOME =MONTHLYGROSSINCOME * 26/12;
		else GROSSINCOME = MONTHLYGROSSINCOME;
	end;

	MonthDiff = intck('Months',input(ApplicationDate,yymmdd10.), '01APR2017'd);
	GrossIncomeAdjusted = GrossIncome*(1.004752962)**(MonthDiff);
	MonthsAtCurrentEmployer = intck('Months', input(CURRENTEMPLOYMENTSTARTDATE,yymmdd10.),input(ApplicationDate,yymmdd10.));
run;

data rawdata4;
	set  rawdata4;

	if channelcode IN ('CCC002','CCC003','CCC004','CCC005') then InSecondsFlag = 1;
	else if (channelcode ='CCC013' AND BRANCHCODE='2247') then InSecondsFlag = 1;
	else InSecondsFlag = 0;

	if InSecondsHistFlag = . then InSecondsHistFlag = 0;
	else InSecondsHistFlag = InSecondsHistFlag;
	InSec = max(InSecondsHistFlag,InSecondsFlag);

	if BehaveScore in ( . , 0, 9999) then Repeat = 0;
	else if BehaveScore not in (., 0, 9999) then Repeat = 1;

	if (Repeat = 0 and COMPUSCANVAR5486 in (-9,-7,-6)) then ThinfileIndicator = 1;
	else ThinfileIndicator = 0;

	if Repeat = 0 and COMPUSCANVAR2123<= 0 then Comp_seg = 1;
	else if Repeat = 0 and COMPUSCANVAR2123= . then Comp_seg = 1;
	else if Repeat = 0 and COMPUSCANVAR2123> 0 then Comp_seg = 2;
	else if Repeat = 1 and COMPUSCANVAR2123<= 0 then Comp_seg = 3;
	else if Repeat = 1 and COMPUSCANVAR2123= . then Comp_seg = 3;
	else if Repeat = 1 and COMPUSCANVAR2123= 1 then Comp_seg = 4;
	else if Repeat = 1 and COMPUSCANVAR2123= 2 then Comp_seg = 4;
	else if Repeat = 1 and COMPUSCANVAR2123> 2 then Comp_seg = 5;

	MonthDiff = intck('Months',input(ApplicationDate,yymmdd10.), '01APR2017'd);
	GrossIncomeAdjusted = GrossIncome*(1.004752962)**(MonthDiff);
	MonthsAtCurrentEmployer = intck('Months', input(CURRENTEMPLOYMENTSTARTDATE,yymmdd10.),input(ApplicationDate,yymmdd10.));

	if COMPUSCANVAR7430 in (-7,-6,-9) then NumPaymentsRank = - 1;
	else if COMPUSCANVAR7430 <= 4 then NumPaymentsRank = 1;
	else if COMPUSCANVAR7430 <= 27 then NumPaymentsRank = 2;
	else if COMPUSCANVAR7430 <= 47 then NumPaymentsRank = 3;
	else if COMPUSCANVAR7430 > 47 then NumPaymentsRank = 4;

	if  COMPUSCANVAR7431 in (-6,-7,-9) then PercPaymentsRank = -1;
	else if COMPUSCANVAR7431  <= 44 then PercPaymentsRank = 1;
	else if COMPUSCANVAR7431  <= 66 then PercPaymentsRank = 2;
	else if COMPUSCANVAR7431  <= 90 then PercPaymentsRank = 3;
	else if COMPUSCANVAR7431  <= 98 then PercPaymentsRank = 4;
	else if COMPUSCANVAR7431  > 98 then PercPaymentsRank = 5;
	format PercAndNumPaymentRank $5.;

	if PercPaymentsRank = -1 then PercAndNumPaymentRank = "-1";
	else if NumPaymentsRank = -1 then PercAndNumPaymentRank = "-1";
	else PercAndNumPaymentRank = Compress(PercPaymentsRank||NumPaymentsRank);

	if Comp_seg = 4 then do;
		if COMPUSCANVAR716 = 0 then COMPUSCANVAR716 = -9;
	end;

	if  (COMPUSCANVAR1424  < 0 or COMPUSCANVAR6788 < 0 ) then UNS_RatioMR60DBal1YearAdj = -19;
	else if  (COMPUSCANVAR1424 = 0  and  COMPUSCANVAR6788 = 0) then UNS_RatioMR60DBal1YearAdj = -18;
	else if  COMPUSCANVAR6788 = 0 then UNS_RatioMR60DBal1YearAdj = -17;
	else if  COMPUSCANVAR1424 = 0 then UNS_RatioMR60DBal1YearAdj = -16;
	else UNS_RatioMR60DBal1YearAdj = COMPUSCANVAR1424/COMPUSCANVAR6788;

	if  (COMPUSCANVAR6132  < 0 or COMPUSCANVAR6134 < 0 ) then ALL_RatioOrgBalLim1Year = -19;
	else if  (COMPUSCANVAR6132 = 0  and  COMPUSCANVAR6134 = 0) then ALL_RatioOrgBalLim1Year = -18;
	else if  COMPUSCANVAR6134 = 0 then ALL_RatioOrgBalLim1Year = -17;
	else if  COMPUSCANVAR6132 = 0 then ALL_RatioOrgBalLim1Year = -16;
	else ALL_RatioOrgBalLim1Year = COMPUSCANVAR6132/COMPUSCANVAR6134;
	format adjCOMPUSCANVAR6289 comma6.2;

	if  (COMPUSCANVAR753  < 0 or COMPUSCANVAR6285 < 0 ) then adjCOMPUSCANVAR6289 = -19;
	else if  (COMPUSCANVAR753 = 0  and  COMPUSCANVAR6285 = 0) then adjCOMPUSCANVAR6289 = -18;
	else if  COMPUSCANVAR6285 = 0 then adjCOMPUSCANVAR6289 = -17;
	else adjCOMPUSCANVAR6289 = COMPUSCANVAR753/COMPUSCANVAR6285;
run;

data rawdata5;
	set rawdata4;

	if PP0001AL <= 0 then TU_THIN = 1;
	else TU_THIN = 0;
	NP003_T = NP003;
	PP003_T = PP003;

	IF NP003 < 0 then NP003_T = 0;

	IF PP003 < 0 then PP003_T = 0;
	PP003_NP003 = NP003_T + PP003_T;

	if PP116 <= 6 then PP173Adj = -13 ;
  	else PP173Adj = PP173 ;

	if PP003_NP003 = 0 then
		TU_Seg = 1;
	else if PP003_NP003 <= 2 then TU_Seg = 2;
	else if PP003_NP003 <= 8 then TU_Seg = 3;
	else if PP003_NP003 <= 12 then TU_Seg =4;
	else if PP003_NP003 > 12 then TU_Seg = 5;

	IF PP0714AL >= 0 AND GROSSINCOME > 0 THEN PP0714AL_GI_RATIO = PP0714AL /GROSSINCOME;
	ELSE IF PP0714AL < 0 THEN PP0714AL_GI_RATIO = PP0714AL;
	ELSE IF GROSSINCOME < 0 THEN PP0714AL_GI_RATIO =-10;
	ELSE if GROSSINCOME = 0 THEN PP0714AL_GI_RATIO = -20;

	IF PP0801AL >= 0 AND GROSSINCOME > 0 THEN PP0801AL_GI_RATIO = PP0801AL/GROSSINCOME;
	ELSE IF PP0801AL < 0 THEN PP0801AL_GI_RATIO = PP0801AL;
	ELSE IF GROSSINCOME < 0 THEN PP0801AL_GI_RATIO =-10;
	ELSE if GROSSINCOME = 0 THEN PP0801AL_GI_RATIO = -20;

	IF PP0801AL >= 0 AND GROSSINCOME > 0 THEN PP0801AL_GI_RATIO = PP0801AL/GROSSINCOME;
	ELSE IF PP0801AL < 0 THEN PP0801AL_GI_RATIO = PP0801AL;
	ELSE IF GROSSINCOME < 0 THEN PP0801AL_GI_RATIO =-10;
	ELSE if GROSSINCOME = 0 THEN PP0801AL_GI_RATIO = -20;

	IF PP0601AL >= 0 AND PP0604AL > 0 THEN PP0601AL_CU_RATIO_6 = PP0601AL/PP0604AL;
	ELSE IF PP0601AL < 0 THEN PP0601AL_CU_RATIO_6 = PP0601AL;
	ELSE IF PP0604AL < 0 THEN PP0601AL_CU_RATIO_6 = PP0604AL-10;
	ELSE IF PP0604AL = 0 THEN PP0601AL_CU_RATIO_6 = -20;

	IF PP0521LB >= 0 AND GROSSINCOME > 0 THEN PP0521LB_GI_RATIO = PP0521LB/GROSSINCOME;
	ELSE IF PP0521LB < 0 THEN PP0521LB_GI_RATIO = PP0521LB;
	ELSE IF GROSSINCOME < 0 THEN PP0521LB_GI_RATIO =-10;
	ELSE IF GROSSINCOME = 0 THEN PP0521LB_GI_RATIO = -20;

	if TU_Seg = 4 then do;
		format RE006_L $15. RE019_L $35. RE006_019 $55.;

		If RE006 in (.) then RE006_L = "High";
		Else If (RE006 > . and RE006 <= 1)  then RE006_L = "MedHigh";
		Else If (RE006 > . and RE006 <= 2)  then RE006_L = "Medium";
		Else If (RE006 > . and RE006 <= 4)  then RE006_L = "MedLow";
		Else If (RE006 > 4)  then RE006_L = "Low";
		else RE006_L ="(UNKNOWN)";

		if  compress(RE019) in ('L','','W','+000000009.0','I') then RE019_L ="High";
		else if  compress(RE019) in ('+000000007.0','+000000008.0','+000000005.0','+000000006.0') then RE019_L ="Med";
		else if  compress(RE019) in ('+000000000.0','+000000003.0','+000000004.0','+000000002.0','+000000001.0','E') then RE019_L ="Low";
		else RE019_L ="(UNKNOWN)";
		RE006_019 = compress(RE006_L||RE019_L);
	end;

	if TU_Seg = 5 then do;
		If ( RE006 > . and RE006 <= 1) or RE006 in (.) then RE006_l = "High";
		Else If (RE006 > . and RE006 <= 3)  then RE006_l = "Med";
		Else If (RE006 > 3)  then RE006_l = "Low";

		if  compress(RE019) in ('L','','W','J','I') then RE019_l ="High";
		else if  compress(RE019) in ('+000000009.0','+000000008.0','+000000007.0','E') then RE019_l ="Med";
		else if  compress(RE019) in ('+000000006.0','+000000005.0','+000000000.0','+000000004.0','+000000003.0','+000000002.0','+000000001.0') then RE019_l ="Low";
		RE006_019 = compress(RE006_L||RE019_L);
	end;

	IF PP0503AL >= 0 AND PP0505AL > 0 THEN PP0503AL_3_RATIO_12 = PP0503AL/PP0505AL;
	ELSE IF PP0503AL < 0 THEN PP0503AL_3_RATIO_12 = PP0503AL;
	ELSE IF PP0505AL < 0 THEN PP0503AL_3_RATIO_12 = PP0505AL-10;
	ELSE if PP0505AL = 0 THEN PP0503AL_3_RATIO_12 = -20;

	IF PP0601AL >= 0 AND PP0603AL > 0 THEN PP0601AL_CU_RATIO_3 = PP0601AL/PP0603AL;
	ELSE IF PP0601AL < 0 THEN PP0601AL_CU_RATIO_3 = PP0601AL;
	ELSE IF PP0603AL < 0 THEN PP0601AL_CU_RATIO_3 =PP0603AL-10;
	ELSE if PP0603AL = 0 THEN PP0601AL_CU_RATIO_3 = -20;

	IF PP0515AL >= 0 AND GROSSINCOME > 0 THEN PP0515AL_GI_RATIO = PP0515AL/GROSSINCOME;
	ELSE IF PP0515AL < 0 THEN P0515AL_GI_RATIO = PP0515AL;
	ELSE IF GROSSINCOME < 0 THEN PP0515AL_GI_RATIO =-10;
	ELSE if GROSSINCOME = 0 THEN PP0515AL_GI_RATIO = -20;
run;
/**/

/*Assign ia risk groups*/
data rawdata5;
set rawdata5;
iaFinalriskscore_prod = input(iaFinalriskscore,best16.);
	if Min(ThinFileIndicator,TU_Thin) = 0 and scorecardversion = 'V645' then do;
		if iaFinalriskscore_prod >= 932.242611756651      then iaRiskGroup_prod = 50;
		else if iaFinalriskscore_prod >= 912.452480990053 then iaRiskGroup_prod = 51;
		else if iaFinalriskscore_prod >= 878.956333489911 then iaRiskGroup_prod = 52;
		else if iaFinalriskscore_prod >= 841.833690856176 then iaRiskGroup_prod = 53;
		else if iaFinalriskscore_prod >= 811.989999894282 then iaRiskGroup_prod = 54;
		else if iaFinalriskscore_prod >= 790.349339106057 then iaRiskGroup_prod = 55;
		else if iaFinalriskscore_prod >= 778.068960766859 then iaRiskGroup_prod = 56;
		else if iaFinalriskscore_prod >= 758.6444629 	  then iaRiskGroup_prod = 57;
		else if iaFinalriskscore_prod >= 746.152798684895 then iaRiskGroup_prod = 58;
		else if iaFinalriskscore_prod >= 732.001226390991 then iaRiskGroup_prod = 59;
		else if iaFinalriskscore_prod >= 708.169317621721 then iaRiskGroup_prod = 60;
		else if iaFinalriskscore_prod >= 690.87118531475  then iaRiskGroup_prod = 61;
		else if iaFinalriskscore_prod >= 675.057720140646 then iaRiskGroup_prod = 62;
		else if iaFinalriskscore_prod >= 530 			  then iaRiskGroup_prod = 63;
		else if iaFinalriskscore_prod >= 529			  then iaRiskGroup_prod = 64;
		else if iaFinalriskscore_prod >= 527 			  then iaRiskGroup_prod = 65;
		else if iaFinalriskscore_prod >= 500 			  then iaRiskGroup_prod = 66;
		else if iaFinalriskscore_prod > 0    			  then iaRiskGroup_prod = 67;
	end;

	else if Min(ThinFileIndicator,TU_Thin) = 1 and scorecardversion = 'V645' then do;
		if iaFinalriskscore_prod >=      828.199869458644 then iaRiskGroup_prod = 68;
		else if iaFinalriskscore_prod >= 762.179100967216 then iaRiskGroup_prod = 69;
		else if iaFinalriskscore_prod >= 721.281349457995 then iaRiskGroup_prod = 70;
		else if iaFinalriskscore_prod > 0 				  then iaRiskGroup_prod = 71;
	end;
	/*ia*/
	if Min(ThinFileIndicator,TU_Thin) = 0 and scorecardversion = 'V655' then do;
            if iaFinalriskscore_prod >= 929.70414109 then iaRiskGroup_prod = 50;
            else if iaFinalriskscore_prod >= 909.59116387 then iaRiskGroup_prod = 51;
            else if iaFinalriskscore_prod >= 872.15152879 then iaRiskGroup_prod = 52;
            else if iaFinalriskscore_prod >= 827.34047213 then iaRiskGroup_prod = 53;
            else if iaFinalriskscore_prod >= 790.59177669 then iaRiskGroup_prod = 54;
            else if iaFinalriskscore_prod >= 764.70857459 then iaRiskGroup_prod = 55;
            else if iaFinalriskscore_prod >= 750.21402079 then iaRiskGroup_prod = 56;
            else if iaFinalriskscore_prod >= 727.94382999 then iaRiskGroup_prod = 57;
            else if iaFinalriskscore_prod >= 714.25140059 then iaRiskGroup_prod = 58;
            else if iaFinalriskscore_prod >= 699.11965741 then iaRiskGroup_prod = 59;
            else if iaFinalriskscore_prod >= 676.02770676 then iaRiskGroup_prod = 60;
            else if iaFinalriskscore_prod >= 660.32199365 then iaRiskGroup_prod = 61;
            else if iaFinalriskscore_prod >= 646.97357763 then iaRiskGroup_prod = 62;
            else if iaFinalriskscore_prod >= 620 then iaRiskGroup_prod = 63;
            else if iaFinalriskscore_prod >= 543.49446576 then iaRiskGroup_prod = 64;
            else if iaFinalriskscore_prod >= 542.20967069 then iaRiskGroup_prod = 65;
            else if iaFinalriskscore_prod >= 525.71483413 then iaRiskGroup_prod = 66;
            else if iaFinalriskscore_prod >= 0 then iaRiskGroup_prod = 67;
        end;

       else if Min(ThinFileIndicator,TU_Thin) = 1 and scorecardversion = 'V655' then do;
            if iaFinalriskscore_prod >= 771.77225514 then iaRiskGroup_prod = 68;
            else if iaFinalriskscore_prod >= 714.29321139 then iaRiskGroup_prod = 69;
            else if iaFinalriskscore_prod >= 681.51627236 then iaRiskGroup_prod = 70;
            else if iaFinalriskscore_prod >= 0 then iaRiskGroup_prod = 71;
    end;
run;

/*Apply TU and CS scoring*/
%include "\\mpwsas65\Process_Automation\Codes\Git_Repositories\Core Credit Risk Models - Monitoring\Macros\Scoring.sas";
%scoring(inDataset=rawdata5, outDataset=rawdata6,Path=\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\TU Models\v570,scored_name =TU_V570, modelType =T);
%scoring(inDataset=rawdata6, outDataset=rawdata6,Path=\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\Compuscan Models\V560,scored_name =CS_V560, modelType =C);
%scoring(inDataset=rawdata6, outDataset=rawdata6,Path=\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V570 Compuscan,scored_name =CS_V570, modelType =C);
%scoring(inDataset=rawdata6, outDataset=rawdata6,Path=\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU,scored_name =TU_V580, modelType =T);

/*Apply V622, V636 and V645 scoring*/
%include "\\mpwsas65\Process_Automation\Codes\Git_Repositories\Core Credit Risk Models - Monitoring\Macros\ApplyV622.sas";
%applyV622(inDataset=rawdata6, outDataset=rawdata_V622);
%include "\\mpwsas65\Process_Automation\Codes\Git_Repositories\Core Credit Risk Models - Monitoring\Macros\ApplyV636.sas";
%applyV636(inDataset=rawdata6, outDataset=rawdata_V636);
%include "\\mpwsas65\Process_Automation\Codes\Git_Repositories\Core Credit Risk Models - Monitoring\Macros\ApplyV645.sas";
%applyV645(inDataset=rawdata6, outDataset=rawdata_V645); 
data rawdata_V645;
	set rawdata_V645;
	V645_B10 = V645 * 1.1;
	overallscore1 = input(overallscore, best16.);
run;
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\ApplyV648.sas";
%applyV648(inDataset=rawdata_V645, prob_name=V645_B10, score_name=OVERALLSCORE1, outDataset=rawdata_V648);

%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\ApplyV655.sas";
%applyV655(inDataset=rawdata6, outDataset=rawdata_V655);
data rawdata_V655;
	set rawdata_V655;
	V655_B20 = V655 * 1;
	overallscore1 = input(overallscore, best16.);
run;
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\ApplyV658.sas";
%applyV658(inDataset=rawdata_V655, prob_name=V655_B20, score_name=OVERALLSCORE1, outDataset=rawdata_V658);

proc sql;
	create table all_scores as
	select  b.V620, b.V621, b.V622, 
			c.V630, c.V631, c.V632, c.V633, c.V634, c.V635, c.V636, 
			d.V640, d.V641, d.V642, d.V643, d.V644, d.V645, 
			e.V648, e.V648_finalriskscore, e.V648_RiskGroup,
			f.V650, f.V651, f.V652, f.V653, f.V654, f.V655, g.V658, 
			g.V658_finalriskscore, g.V658_RiskGroup, a.*
	from rawdata6 a
	left join rawdata_V622 as b
	on a.uniqueid = b.uniqueid
	left join rawdata_V636 as c
	on a.uniqueid = c.uniqueid
	left join rawdata_V645 as d
	on a.uniqueid = d.uniqueid
	left join rawdata_V648 as e
	on a.uniqueid = e.uniqueid
	left join rawdata_V655 as f
	on a.uniqueid = f.uniqueid
	left join rawdata_V658 as g
	on a.uniqueid = g.uniqueid;
quit;
/**/

/*Calculate final score*/
data all_scores;
	set all_scores;
	if SCORECARDVERSION = "V622" then V6_finalriskscore =  1000-(V622*1000);
	if SCORECARDVERSION = "V636" then V6_finalriskscore =  1000-(V636*1000);
	if SCORECARDVERSION = "V645" then V6_finalriskscore =  1000-(V645*1000);
	if SCORECARDVERSION = "V655" then V6_finalriskscore =  1000-(V655*1000);
run;
/**/

/*Assign risk groups*/
data all_scores;
	set all_scores;
	if Min(ThinFileIndicator,TU_Thin) = 0 and scorecardversion ne 'V655' then do;
		if V6_finalriskscore >= 932.242611756651      then V6_RiskGroup = 50;
		else if V6_finalriskscore >= 912.452480990053 then V6_RiskGroup = 51;
		else if V6_finalriskscore >= 878.956333489911 then V6_RiskGroup = 52;
		else if V6_finalriskscore >= 841.833690856176 then V6_RiskGroup = 53;
		else if V6_finalriskscore >= 811.989999894282 then V6_RiskGroup = 54;
		else if V6_finalriskscore >= 790.349339106057 then V6_RiskGroup = 55;
		else if V6_finalriskscore >= 778.068960766859 then V6_RiskGroup = 56;
		else if V6_finalriskscore >= 758.6444629 	  then V6_RiskGroup = 57;
		else if V6_finalriskscore >= 746.152798684895 then V6_RiskGroup = 58;
		else if V6_finalriskscore >= 732.001226390991 then V6_RiskGroup = 59;
		else if V6_finalriskscore >= 708.169317621721 then V6_RiskGroup = 60;
		else if V6_finalriskscore >= 690.87118531475  then V6_RiskGroup = 61;
		else if V6_finalriskscore >= 675.057720140646 then V6_RiskGroup = 62;
		else if V6_finalriskscore >= 530 			  then V6_RiskGroup = 63;
		else if V6_finalriskscore >= 529			  then V6_RiskGroup = 64;
		else if V6_finalriskscore >= 527 			  then V6_RiskGroup = 65;
		else if V6_finalriskscore >= 500 			  then V6_RiskGroup = 66;
		else if V6_finalriskscore > 0    			  then V6_RiskGroup = 67;
	end;

	else if Min(ThinFileIndicator,TU_Thin) = 1 and scorecardversion ne 'V655' then do;
		if V6_finalriskscore >=      828.199869458644 then V6_RiskGroup = 68;
		else if V6_finalriskscore >= 762.179100967216 then V6_RiskGroup = 69;
		else if V6_finalriskscore >= 721.281349457995 then V6_RiskGroup = 70;
		else if V6_finalriskscore > 0 				  then V6_RiskGroup = 71;
	end;
	
	if Min(ThinFileIndicator,TU_Thin) = 0 and scorecardversion = 'V655' then do;
            if V6_finalriskscore >= 932.2426117 then V6_RiskGroup = 50;
			else if V6_finalriskscore >= 912.4524809 	then V6_RiskGroup = 51;
			else if V6_finalriskscore >= 878.9563334 	then V6_RiskGroup = 52;
			else if V6_finalriskscore >= 841.8336908 	then V6_RiskGroup = 53;
			else if V6_finalriskscore >= 811.9899998 	then V6_RiskGroup = 54;
			else if V6_finalriskscore >= 790.3493391 	then V6_RiskGroup = 55;
			else if V6_finalriskscore >= 778.0689607 	then V6_RiskGroup = 56;
			else if V6_finalriskscore >= 758.6444629 	then V6_RiskGroup = 57;
			else if V6_finalriskscore >= 746.1527986 	then V6_RiskGroup = 58;
			else if V6_finalriskscore >= 732.0012263 	then V6_RiskGroup = 59;
			else if V6_finalriskscore >= 708.1693176 	then V6_RiskGroup = 60;
			else if V6_finalriskscore >= 700 			then V6_RiskGroup = 61;
			else if V6_finalriskscore >= 690.8711853 	then V6_RiskGroup = 62;
			else if V6_finalriskscore >= 675.0577201 	then V6_RiskGroup = 63;
			else if V6_finalriskscore >= 543.4944658 	then V6_RiskGroup = 64;
			else if V6_finalriskscore >= 522.2096707 	then V6_RiskGroup = 65;
			else if V6_finalriskscore >= 500 			then V6_RiskGroup = 66;
			else if V6_finalriskscore >= 0 			then V6_RiskGroup = 67;
        end;

       else if Min(ThinFileIndicator,TU_Thin) = 1 and scorecardversion = 'V655' then do;
            if V6_finalriskscore >= 828.1998694 		then V6_RiskGroup = 68;
			else if V6_finalriskscore >= 762.1791009 	then V6_RiskGroup = 69;
			else if V6_finalriskscore >= 721.2813494 	then V6_RiskGroup = 70;
			else if V6_finalriskscore >= 700 			then V6_RiskGroup = 71;
			else if V6_finalriskscore >= 0 			then V6_RiskGroup = 72;
    end;
run;
/**/

/*Match rates*/
data all_scores;
	set all_scores ;
	if  round(V620_prod,0.0001) - round(input(V620,best16.),0.0001) = 0 then V620_Match = 1;
    else V620_Match =0;

	if  round(V621_prod ,0.0001) - round(input(V621,best16.),0.0001) = 0 then V621_Match = 1;
    else V621_Match =0;

	if  round(V622_prod ,0.0001) - round(input(V622,best16.),0.0001) = 0 then V622_Match = 1;
    else V622_Match =0;

	if  round(V630_prod ,0.0001) - round(input(V630,best16.),0.0001) = 0 then V630_Match = 1;
    else V630_Match =0;

	if  round(V631_prod ,0.0001) - round(input(V631,best16.),0.0001) = 0 then V631_Match = 1;
    else V631_Match =0;

	if  round(V632_prod ,0.0001) - round(input(V632,best16.),0.0001) = 0 then V632_Match = 1;
    else V632_Match =0;

	if  round(V633_prod ,0.0001) - round(input(V633,best16.),0.0001) = 0 then V633_Match = 1;
    else V633_Match =0;

	if  round(V634_prod ,0.0001) - round(input(V634,best16.),0.0001) = 0 then V634_Match = 1;
    else V634_Match =0;

	if  round(V635_prod ,0.0001) - round(input(V635,best16.),0.0001) = 0 then V635_Match = 1;
    else V635_Match =0;

	if  round(V636_prod ,0.0001) - round(input(V636,best16.),0.0001) = 0 then V636_Match = 1;
    else V636_Match =0;

	if  round(V640_prod ,0.0001) - round(input(V640,best16.),0.0001) = 0 then V640_Match = 1;
    else V640_Match =0;

	if  round(V641_prod ,0.0001) - round(input(V641,best16.),0.0001) = 0 then V641_Match = 1;
    else V641_Match =0;

	if  round(V642_prod ,0.0001) - round(input(V642,best16.),0.0001) = 0 then V642_Match = 1;
    else V642_Match =0;

	if  round(V643_prod ,0.0001) - round(input(V643,best16.),0.0001) = 0 then V643_Match = 1;
    else V643_Match =0;

	if  round(V644_prod ,0.0001) - round(input(V644,best16.),0.0001) = 0 then V644_Match = 1;
    else V644_Match =0;

	if  round(V645_prod ,0.0001) - round(input(V645,best16.),0.0001) = 0 then V645_Match = 1;
	else V645_Match =0;

	if  round(V648_prod ,0.01) - round(input(V648,best16.),0.01) = 0 then V648_Match = 1;
	else V648_Match =0;

	V603_match = V622_match;

	if  round(V650_prod ,0.0001) - round(input(V650,best16.),0.0001) = 0 then V650_Match = 1;
    else V650_Match =0;

	if  round(V651_prod ,0.0001) - round(input(V651,best16.),0.0001) = 0 then V651_Match = 1;
    else V651_Match =0;

	if  round(V652_prod ,0.0001) - round(input(V652,best16.),0.0001) = 0 then V652_Match = 1;
    else V652_Match =0;

	if  round(V653_prod ,0.0001) - round(input(V653,best16.),0.0001) = 0 then V653_Match = 1;
    else V653 =0;

/*	if   - round(input(V653,best16.),0.0001) = 0 then V653_Match = 1;*/
/*    else V653_Match =0;*/

	if  round(V654_prod ,0.0001) - round(input(V654,best16.),0.0001) = 0 then V654_Match = 1;
    else V654_Match =0;

	if  round(V655_prod ,0.0001) - round(input(V655,best16.),0.0001) = 0 then V655_Match = 1;
	else V655_Match =0;

	if  round(V658_prod ,0.01) - round(input(V658,best16.),0.01) = 0 then V658_Match = 1;
	else V658_Match =0;

	if scorecardversion ne 'V4';
	if scorecardversion = 'V622' then V6_match = V622_match;
	if scorecardversion = 'V636' then V6_match = V636_match;
	if scorecardversion = 'V645' then V6_match = V645_match;
	if scorecardversion = 'V655' then V6_match = V655_match;

	if  round(input(V6_RiskGroup_prod,best16.) ,0.01) - round(V6_Riskgroup,0.01) = 0 then RG_Match = 1;
    else RG_Match =0;

	if  scorecardversion = 'V655' and (round(iaRiskGroup_prod ,0.01) - round(V658_RiskGroup,0.01)) = 0 then iaRG_Match = 1; 
	else if scorecardversion = 'V645' and (round(iaRiskGroup_prod ,0.01) - round(V648_RiskGroup,0.01)) = 0 then iaRG_Match = 1; 
    else iaRG_Match =0;
run;
/**/

/*Include decline reasons in final dataset*/
proc sql;
	create table All_scores_v622_temp1 as 
	select * 
	from All_scores a
	left join scoring2.V6_Live_Decline_Codes b 
	on a.declinecode = b.declinecode
	left join scoring2.V6_TypeCode_lookup c
	on a.typecode = c.typecode;
quit;
/**/

/*1. Delete live data older than 30 days*/
/*2. If statement to create all_scores dataset if it does not already exist*/
%macro newDay ();
	proc contents data=LiveDash._all_ out=data memtype=data noprint; run;

	proc sql outobs=1;
		select count(distinct memname) into :count
		from data
	quit;

	%if &count > 30 %then %do;		
		proc sql outobs=1; 	
			select distinct memname into :drop
			from data
			order by memname asc;

			drop table LiveDash.&drop.;
		quit;
	%end;

	proc sql outobs=1; 	
		select distinct memname into :newDay
		from data
		order by memname desc;
	quit;
	
	%let today = ALL_SCORES_V622_&todaysDate.;

	%if &newDay. ne &today. %then %do;
		data All_scores_v622_Temp2;
			set All_scores_v622_Temp1;
		run;
	%end;

	%else %do;
		data All_scores_v622_Temp2;
			set LiveDash.All_scores_v622_&todaysDate. All_scores_v622_temp1 ;
		run;
	%end;	
%mend;
%newDay();
/**/

/*Finalize*/
proc sort data = All_scores_v622_Temp2; by tranappnum descending uniqueid; run;
proc sort data = All_scores_v622_Temp2 nodupkey; by tranappnum; run;

data LiveDash.All_scores_v622_&todaysDate.;
	set LiveDash.All_scores_v622_&todaysDate. All_scores_v622_temp2;
run;

/* Create the table on APS */
%Upload_APS(Set=V6_live_scorecardversion, Server = work, APS_ODBC = DEV_DDRe, APS_DB = Dev_DataDistillery_Reporting, Distribute=hash(tranappnum));

proc sql;
	drop table scoring2.V622_Live_All_scores_v622;

	create table scoring2.V622_Live_All_scores_v622 like LiveDash.All_scores_v622_&todaysDate.;

	insert into scoring2.V622_Live_All_scores_v622 (Bulkload = Yes)
	select distinct  *
	from LiveDash.All_scores_v622_&todaysDate.;
quit;
/**/

 %include "\\mpwsas65\Process_Automation\Codes\Git_Repositories\Core Credit Risk Models - Monitoring\Live Monitoring\Alerts.sas"; 
