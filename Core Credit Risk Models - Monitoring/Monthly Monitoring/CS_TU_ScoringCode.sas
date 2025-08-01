OPTIONS NOSYNTAXCHECK ;
options compress = yes;
%let odbc = MPWAPS;

/* filename macros4 '\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros'; */
/* options sasautos = (macros4); */

libname tu '\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets';
libname comp "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\data";

libname V6 '\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\V6 dataset';
libname Calib "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\TU Models\v570\Calibration";
libname Calib2 '\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V636\calibration_new';

%let test = ;
libname livedata '\\mpwsas64\Core_Credit_Risk_Models\RetroData\V622';

data _null_;
	call symput("month", put(intnx("month", today(),-1,'end'),yymmn6.));
	call symput("runmonth", put(intnx("month", today(),-1,'end'),yymmn6.));
	call symput("month2", put(intnx("month", today(),-1,'end'),eurdfmy5.));
	call symput("lastmonth2", put(intnx("month", today(),-2,'end'),eurdfmy5.));
	call symput("lastmonth", put(intnx("month", today(),-2,'end'),yymmn6.));
	call symput("sixmonthago", put(intnx("month", today(),-6,'end'),yymmn6.));
run;
%put &month;
%put &lastmonth;
%put &sixmonthago;

/* Create a backup of the TU and CS applicationbase */
proc sql; connect to odbc (dsn=MPWAPS);
execute (
		IF OBJECT_ID('DEV_DataDistillery_General.dbo.CS_applicationbase_BackUp', 'U') IS NOT NULL 
		DROP TABLE DEV_DataDistillery_General.dbo.CS_applicationbase_BackUp;
		create table DEV_DataDistillery_General.dbo.CS_applicationbase_BackUp 
		with (distribution = hash(loanid), clustered columnstore index ) as
			select *
			from DEV_DataDistillery_General.dbo.CS_applicationbase;
		) by odbc;
quit;

proc sql; connect to odbc (dsn=MPWAPS);
execute (
		IF OBJECT_ID('DEV_DataDistillery_General.dbo.TU_applicationbase_BackUp', 'U') IS NOT NULL 
		DROP TABLE DEV_DataDistillery_General.dbo.TU_applicationbase_BackUp;
		create table DEV_DataDistillery_General.dbo.TU_applicationbase_BackUp 
		with (distribution = hash(loanid), clustered columnstore index ) as
			select *
			from DEV_DataDistillery_General.dbo.TU_applicationbase;
		) by odbc;
quit; 

/*combing data*/
data livedata;
	set LIVEDATA.RAWAPPDATA_&month;
run;

/*removing duplicates*/
proc sort data = livedata ;
	by tranappnumber descending applicationdate;
run;

proc sort data = livedata nodupkey dupout=dups_tu out=tu;
	by tranappnumber;
run;

/*Converting PP283 and RE019 and PP173 and PP116*/
proc sql ;
	create table tu (drop = PP283 RE019 PP173 PP116) as 
		select A.*,
			case when PP283 = '0' then '+000000000.0'
			     when PP283 = '1' then '+000000001.0'
			     when PP283 = '2' then '+000000002.0'
			     when PP283 = '3' then '+000000003.0'
			     when PP283 = '4' then '+000000004.0'
			     when PP283 = '5' then '+000000005.0'
			     when PP283 = '6' then '+000000006.0'
			     when PP283 = '7' then '+000000007.0'
			     when PP283 = '8' then '+000000008.0'
			     when PP283 = '9' then '+000000009.0'
				 else PP283 end as PP283_ADJ format $12. ,

			case when RE019 = '0' then '+000000000.0'
			     when RE019 = '1' then '+000000001.0'
			     when RE019 = '2' then '+000000002.0'
			     when RE019 = '3' then '+000000003.0'
			     when RE019 = '4' then '+000000004.0'
			     when RE019 = '5' then '+000000005.0'
			     when RE019 = '6' then '+000000006.0'
			     when RE019 = '7' then '+000000007.0'
			     when RE019 = '8' then '+000000008.0'
			     when RE019 = '9' then '+000000009.0'
				 else RE019 end as RE019_ADJ format $12. ,

			case when PP173 = '0' then '+000000000.0'
			     when PP173 = '1' then '+000000001.0'
			     when PP173 = '2' then '+000000002.0'
			     when PP173 = '3' then '+000000003.0'
			     when PP173 = '4' then '+000000004.0'
			     when PP173 = '5' then '+000000005.0'
			     when PP173 = '6' then '+000000006.0'
			     when PP173 = '7' then '+000000007.0'
			     when PP173 = '8' then '+000000008.0'
			     when PP173 = '9' then '+000000009.0'
				 else PP173 end as PP173_ADJ format $12. ,

			case when PP116 = '0' then '+000000000.0'
			     when PP116 = '1' then '+000000001.0'
			     when PP116 = '2' then '+000000002.0'
			     when PP116 = '3' then '+000000003.0'
			     when PP116 = '4' then '+000000004.0'
			     when PP116 = '5' then '+000000005.0'
			     when PP116 = '6' then '+000000006.0'
			     when PP116 = '7' then '+000000007.0'
			     when PP116 = '8' then '+000000008.0'
			     when PP116 = '9' then '+000000009.0'
				 else PP116 end as PP116_ADJ format $12.  
		from tu a ;
quit;

/*droping the old var and renameing the new*/
data tu;
	set tu;
	rename RE019_ADJ = RE019;
	rename PP283_ADJ = PP283;
	rename PP173_ADJ = PP173;
	rename PP116_ADJ = PP116;
	appmonth = substr(compress(ApplicationDate,'-'),1,6);
run;

/*Renaming variables*/
%let newvarlist = COMPUSCANVAR1401 COMPUSCANVAR1424 COMPUSCANVAR175 COMPUSCANVAR187 COMPUSCANVAR188
				COMPUSCANVAR2123 COMPUSCANVAR2312 COMPUSCANVAR2528 COMPUSCANVAR2678 COMPUSCANVAR2696
				COMPUSCANVAR3275 COMPUSCANVAR3916 COMPUSCANVAR3935 COMPUSCANVAR5208 COMPUSCANVAR5486
				COMPUSCANVAR5489 COMPUSCANVAR5579 COMPUSCANVAR5826 COMPUSCANVAR6073 COMPUSCANVAR6130
				COMPUSCANVAR6132 COMPUSCANVAR6134 COMPUSCANVAR6285 COMPUSCANVAR6788 COMPUSCANVAR716
				COMPUSCANVAR733 COMPUSCANVAR7430 COMPUSCANVAR7431 COMPUSCANVAR7479 COMPUSCANVAR753
				COMPUSCANVAR7547 COMPUSCANVAR7549 COMPUSCANVAR7550 COMPUSCANVAR7683;

%let oldvarlist = UNS_PercUtilisation UNS_ValCurBalMR60Days CSN_TimeOldestTrade ALL_TimeMREnq ALL_TimeOldestEnq
                  AUL_NumOpenTrades ALL_MaxDelq1YearLT24M OWN_Perc1pDelq2Years OTH_MaxDelqEver
                  OTH_MaxDelq1YearLT24M REV_MaxDelq180DaysGE24M UNS_TimeMR3pDelq UNS_MaxDelq180DaysLT12M
                  AIL_Num1pDelq90Days ALL_NumEverTrades ALL_NumTrades90Days OTH_AvgMonthsOnBook UNS_AvgMonthsOnBook
                  RCG_AvgMonthsOnBook UNN_AvgMonthsOnBook ALL_ValOrgBalLim90Days ALL_ValOrgBalLim1Year
                  OTH_ValOrgBalLim180Days UNS_ValCurBal1Year OWN_PercUtiliSatisfTrades
                  OWN_AvgPercUtilisationMR60Days ALL_NumPayments2Years ALL_PercPayments2Years
                  OTH_PercPayments2Years OTH_ValOrgBalLim REV_PercPayments180Days REV_PercPayments1Year
                  REV_NumPayments2Years OPL_PercPayments2Years;

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

/*Compuscan variables transformation*/

data Comp ;
	set Tu ;
	%rename1(&oldvarlist, &newvarlist);
run;

/*changing from num to char and vica versa*/
%macro change(var=);
	%let newvar=&var._a;
	data Comp(rename = (&newvar=&var));
		set Comp;
		var = input(&var,12.);
		rename var=&newvar;
		drop &var;
	run;
%mend;

%let tu_varlist = PP003 NP003 NG004 NP013 NP015 EQ0012AL EQ2012AL EQ0015PL EQ2015PL DM0001AL PP0406AL PP0421CL
PP0935AL PP0714AL PP0327AL PP0801AL PP0325AL PP0901AL PP0051CL PP0171CL PP0601AL PP0604AL
RE006 PP0407AL PP149 PP0521LB PP116 PP173 PP0603AL PP0503AL PP0505AL PP0111LB PP0313LN
PP0515AL PP0001AL ALL_MaxDelq1YearLT24M OTH_MaxDelqEver REV_MaxDelq180DaysGE24M;

%macro ApplyChange();
	%do i = 1 %to %sysfunc(countw(&tu_varlist));
		%let variable = %scan(&tu_varlist, &i);
		%change(var=&variable);
    %end;
%mend;
%ApplyChange;

/* Derive ratio variables */
data Comp2;
	set Comp;
	format AppDate yymmddn8.;
	AppDate = input(ApplicationDate,yymmdd10.);
	Week = put(week(AppDate, 'w'),z2.);
	if channelcode IN ('CCC002','CCC003','CCC004','CCC005') then InSecondsFlag = 1 ;
	else if (channelcode ='CCC013' AND BRANCHCODE='2247') then  InSecondsFlag = 1 ;
	else InSecondsFlag = 0 ;

	if InSecondsHistFlag = . then InSecondsHistFlag = 0 ;
	else InSecondsHistFlag = InSecondsHistFlag ;
	InSec = max(InSecondsHistFlag,InSecondsFlag);
	if BehaveScore in ( . , 0, 9999) then Repeat = 0 ;
	else if BehaveScore not in (., 0, 9999) then Repeat = 1 ;

	if (Repeat = 0 and COMPUSCANVAR5486 in (-9,-7,-6)) then ThinfileIndicator = 1 ;
	else ThinfileIndicator = 0 ;

	if Repeat = 0 and COMPUSCANVAR2123<= 0 then seg= 1;
	else if Repeat = 0 and COMPUSCANVAR2123= . then seg= 1;
	else if Repeat = 0 and COMPUSCANVAR2123> 0 then seg= 2;
	else if Repeat = 1 and COMPUSCANVAR2123<= 0 then seg= 3;
	else if Repeat = 1 and COMPUSCANVAR2123= . then seg= 3;
	else if Repeat = 1 and COMPUSCANVAR2123= 1 then seg= 4;
	else if Repeat = 1 and COMPUSCANVAR2123= 2 then seg= 4;
	else if Repeat = 1 and COMPUSCANVAR2123> 2 then seg= 5;

	if (ACCREDITEDINDICATOR = 'P' OR MEMBER IN ('PERSAL','PSAL') OR  EMPLOYERGROUPCODE IN ('PERSAL','PSAL')) THEN  PERSALINDICATOR = 1 ;
	else PERSALINDICATOR = 0 ;

	if SUBGROUPCREATEDATE  ne '' THEN  SG_CREATIONYEAR = input(put(SUBSTR(SUBGROUPCREATEDATE,1,4),$4.),4.); 
	ELSE SG_CREATIONYEAR = input(put(SUBSTR(APPLICATIONDATE,1,4),$4.),4.);

	if PERSALINDICATOR = 1 then do ;
	  SG_CREATIONYEAR1 = SG_CREATIONYEAR ;
	  SG_CREATIONYEAR2 = -10 ;
	end;
	else do ;
	  SG_CREATIONYEAR2 = SG_CREATIONYEAR ;
	  SG_CREATIONYEAR1 = -10 ;
	end;

	if CALCULATEDNETTINCOME = 0.00  then NETINCOME = MONTHLYNETTINCOME ;
	else if CALCULATEDNETTINCOME < MONTHLYNETTINCOME then NETINCOME = MONTHLYNETTINCOME ;
	else NETINCOME = CALCULATEDNETTINCOME ;

	if WAGEFREQUENCYCODE = 'WAG001' then do ;
	  if MONTHLYGROSSINCOME = 0.00 then  GROSSINCOME = NETINCOME ;
	  else GROSSINCOME = MONTHLYGROSSINCOME ;
	end;

	if WAGEFREQUENCYCODE = 'WAG002' then do ;
	  if MONTHLYGROSSINCOME = 0.00 then  GROSSINCOME = NETINCOME ;
	  else if MONTHLYGROSSINCOME < NETINCOME then GROSSINCOME =MONTHLYGROSSINCOME * 52/12  ;
	  else GROSSINCOME = MONTHLYGROSSINCOME ; 
	end;

	if WAGEFREQUENCYCODE = 'WAG003' then do ;
	  if MONTHLYGROSSINCOME = 0.00 then  GROSSINCOME = NETINCOME ;
	  else if MONTHLYGROSSINCOME < NETINCOME then GROSSINCOME =MONTHLYGROSSINCOME * 26/12  ;
	  else GROSSINCOME = MONTHLYGROSSINCOME ; 
	end;

	MonthDiff = intck('Months',input(ApplicationDate,yymmdd10.), '01APR2017'd) ;
	GrossIncomeAdjusted = GrossIncome*(1.004752962)**(MonthDiff);

	MonthsAtCurrentEmployer = intck('Months', input(CURRENTEMPLOYMENTSTARTDATE,yymmdd10.),input(ApplicationDate,yymmdd10.));     

	if COMPUSCANVAR7430 in (-7,-6,-9) then NumPaymentsRank = -1 ;
	else if COMPUSCANVAR7430 <= 4 then NumPaymentsRank = 1 ;
	else if COMPUSCANVAR7430 <= 27 then NumPaymentsRank = 2 ;
	else if COMPUSCANVAR7430 <= 47 then NumPaymentsRank = 3 ;
	else if COMPUSCANVAR7430 > 47 then NumPaymentsRank = 4 ;

	if  COMPUSCANVAR7431 in (-6,-7,-9) then PercPaymentsRank = -1 ;
	else if COMPUSCANVAR7431  <= 44 then PercPaymentsRank = 1 ;
	else if COMPUSCANVAR7431  <= 66 then PercPaymentsRank = 2 ;
	else if COMPUSCANVAR7431  <= 90 then PercPaymentsRank = 3 ;
	else if COMPUSCANVAR7431  <= 98 then PercPaymentsRank = 4 ;
	else if COMPUSCANVAR7431  > 98 then PercPaymentsRank = 5 ;

	format PercAndNumPaymentRank $5. ;

	if PercPaymentsRank = -1 then PercAndNumPaymentRank = "-1";
	else if NumPaymentsRank = -1 then PercAndNumPaymentRank = "-1";
	else PercAndNumPaymentRank = Compress(PercPaymentsRank||NumPaymentsRank) ;

	if seg = 4 then do; 
	  if COMPUSCANVAR716 = 0 then COMPUSCANVAR716 = -9 ;
	end;

	if  (COMPUSCANVAR1424  < 0 or COMPUSCANVAR6788 < 0 ) then  UNS_RatioMR60DBal1YearAdj = -19 ;
	else if  (COMPUSCANVAR1424 = 0  and  COMPUSCANVAR6788 = 0) then UNS_RatioMR60DBal1YearAdj = -18 ;
	else if  COMPUSCANVAR6788 = 0 then UNS_RatioMR60DBal1YearAdj = -17 ;
	else if  COMPUSCANVAR1424 = 0 then UNS_RatioMR60DBal1YearAdj = -16 ;
	else UNS_RatioMR60DBal1YearAdj = COMPUSCANVAR1424/COMPUSCANVAR6788 ;

	if  (COMPUSCANVAR6132  < 0 or COMPUSCANVAR6134 < 0 ) then  ALL_RatioOrgBalLim1Year = -19 ;
	else if  (COMPUSCANVAR6132 = 0  and  COMPUSCANVAR6134 = 0) then ALL_RatioOrgBalLim1Year = -18 ;
	else if  COMPUSCANVAR6134 = 0 then ALL_RatioOrgBalLim1Year = -17 ;
	else if  COMPUSCANVAR6132 = 0 then ALL_RatioOrgBalLim1Year = -16 ;
	else ALL_RatioOrgBalLim1Year = COMPUSCANVAR6132/COMPUSCANVAR6134 ;

	format adjCOMPUSCANVAR6289 comma6.2;
	if  (COMPUSCANVAR753  < 0 or COMPUSCANVAR6285 < 0 ) then  adjCOMPUSCANVAR6289 = -19 ;
	else if  (COMPUSCANVAR753 = 0  and  COMPUSCANVAR6285 = 0) then adjCOMPUSCANVAR6289 = -18 ;
	else if  COMPUSCANVAR6285 = 0 then adjCOMPUSCANVAR6289 = -17 ;
	else adjCOMPUSCANVAR6289 = COMPUSCANVAR753/COMPUSCANVAR6285 ;

	comp_seg =seg;
run;

%macro scoreinputdata(inputdataset=,numberofsegment=,outputdataset=, path=);
	%macro createivlibrary(h);
	    %do i = 1 %to &numberofsegment;
	        libname iv&i "&path\App Seg&i. Model\IV";
	        %global  segment_&i._list ;
	        proc sql; select reverse(substr(reverse(compress(Parameter)),3)) into : segment_&i._list separated by ' ' from iv&i.._estimates_ where upcase(Parameter) ne 'INTERCEPT'; quit;
	    %end;
	%mend;
	%createivlibrary(1);

	/* Change for pilot , eutopia and type codde  */
	%macro applyscore(t);
	      %do i = 1 %to %sysfunc(countw(&&segment_&t._list));
	            %let var = %scan(&&segment_&t._list, &i);
	            %include "&path\App Seg&t. Model\Bucketing Code\&var._if_statement_.sas";
	            %include "&path\App Seg&T. Model\Bucketing Code\&var._WOE_if_statement_.sas"; 
	      %end;
	      *****************************************;
	      ** SAS Scoring Code for PROC Hplogistic;
	      *****************************************;
	      %include "&path\App Seg&t. Model\Scoring Code\creditlogisticcode.sas";
	%mend;

	%do n = 1 %to &numberofsegment;
		data segment_&n.;
			set &inputdataset(where=(seg=&n));
			%applyscore(&n); 
			Final_score = P_CONTRACTUAL_3_LE91;
			Score = 1000-(final_score*1000);    
			drop _TEMP;
		run;
	%end;

	data final_model_data;
		set %do n = 1 %to &numberofsegment; segment_&n %end;;
	run;

	data &outputdataset;
	    set final_model_data;  
		probability = final_score ;
	    %include "&path\calibration code\calibration_code.sas";

	    if ThinFileIndicator = 0 then do ;
			if finalRiskScore >= 932.242611756651  Then RG6T = 50;
			else if finalRiskScore >= 912.452480990053 Then RG6T = 51;
			else if finalRiskScore >= 878.956333489911 Then RG6T = 52;
			else if finalRiskScore >= 841.833690856176 Then RG6T = 53;
			else if finalRiskScore >= 811.989999894282 Then RG6T = 54;
			else if finalRiskScore >= 790.349339106057 Then RG6T = 55;
			else if finalRiskScore >= 778.068960766859 Then RG6T = 56;
			else if finalRiskScore >= 758.6444629 Then RG6T = 57;
			else if finalRiskScore >= 746.152798684895 Then RG6T = 58;
			else if finalRiskScore >= 732.001226390991 Then RG6T = 59;
			else if finalRiskScore >= 708.169317621721 Then RG6T = 60;
			else if finalRiskScore >= 690.87118531475 Then RG6T = 61;
			else if finalRiskScore >= 675.057720140646 Then RG6T = 62;
			else if finalRiskScore >= 654.743779812149 Then RG6T = 63;
			else if finalRiskScore >= 640.469307325178 Then RG6T = 64;
			else if finalRiskScore >= 622.758776266177 Then RG6T = 65;
			else if finalRiskScore >= 596.376955694516 Then RG6T = 66;
			else if finalRiskScore > 0    Then RG6T = 67;
	    end;
	    else if ThinFileIndicator = 1 then do ;
	        if finalRiskScore >=      828.199869458644 then RG6T = 68 ;
	        else if finalRiskScore >= 762.179100967216 then RG6T = 69 ;
	        else if finalRiskScore >= 721.281349457995 then RG6T = 70 ;
	        else if finalRiskScore > 0 then RG6T = 71 ;
	    end;
	    Scoreband = RG6T ;
	run;
%mend;

/*Scoring V505 and V535*/
%scoreinputdata(inputdataset=Comp2,numberofsegment=5,outputdataset=final_model_v535, path=\\mpwsas64\Core_Credit_Risk_Models\V5\Segmentation Models For Compuscan\Versions\Date20181008);
%scoreinputdata(inputdataset=Comp2,numberofsegment=5,outputdataset=final_model_v505, path=\\mpwsas64\Core_Credit_Risk_Models\V5\Segmentation Models For Compuscan\Versions\Date20180326);

/*Scoring CS_V560 an CS_V570*/
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\Scoring.sas" ;
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\Scoring_new.sas";
%scoring(inDataset=Comp2, outDataset=scored_cs,Path=\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\Compuscan Models\V560,scored_name =CS_V560, modelType =C);
%Scoring_new(inDataset=Comp2, outDataset=scored_cs2,Path=\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V570 Compuscan,scored_name =CS_V570, modelType =C);

/*%macro ex_CS();*/
/*	if applicationdate < '2023-06-15' then %do;*/
/*	%scoring(inDataset=Comp2, outDataset=scored_cs2old,Path=\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V570 Compuscan,scored_name =CS_V570, modelType =C);*/
/*	else */
/*	%Scoring_new(inDataset=Comp2, outDataset=scored_cs2new,Path=\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V570 Compuscan,scored_name =CS_V570, modelType =C);*/
/*	%end;*/
/**/
/*	data scored_cs2;*/
/*		set scored_csnew scored_csold;*/
/*	run;*/
/*%mend;*/
/*%ex_CS();*/

data scored_cs;
	set scored_cs;
		if ThinFileIndicator = 0 then do ;
			if CS_V560_Score >= 932.242611756651  Then CS_V560_RG = 50;
			else if CS_V560_Score >= 912.452480990053 Then CS_V560_RG = 51;
			else if CS_V560_Score >= 878.956333489911 Then CS_V560_RG = 52;
			else if CS_V560_Score >= 841.833690856176 Then CS_V560_RG = 53;
			else if CS_V560_Score >= 811.989999894282 Then CS_V560_RG = 54;
			else if CS_V560_Score >= 790.349339106057 Then CS_V560_RG = 55;
			else if CS_V560_Score >= 778.068960766859 Then CS_V560_RG = 56;
			else if CS_V560_Score >= 758.6444629 Then CS_V560_RG = 57;
			else if CS_V560_Score >= 746.152798684895 Then CS_V560_RG = 58;
			else if CS_V560_Score >= 732.001226390991 Then CS_V560_RG = 59;
			else if CS_V560_Score >= 708.169317621721 Then CS_V560_RG = 60;
			else if CS_V560_Score >= 690.87118531475 Then CS_V560_RG = 61;
			else if CS_V560_Score >= 675.057720140646 Then CS_V560_RG = 62;
			else if CS_V560_Score >= 654.743779812149 Then CS_V560_RG = 63;
			else if CS_V560_Score >= 640.469307325178 Then CS_V560_RG = 64;
			else if CS_V560_Score >= 622.758776266177 Then CS_V560_RG = 65;
			else if CS_V560_Score >= 596.376955694516 Then CS_V560_RG = 66;
		      else if CS_V560_Score > 0    Then CS_V560_RG = 67;
		end;
		else if ThinFileIndicator = 1 then do ;
			if CS_V560_Score >=      828.199869458644 then CS_V560_RG = 68 ;
			else if CS_V560_Score >= 762.179100967216 then CS_V560_RG = 69 ;
			else if CS_V560_Score >= 721.281349457995 then CS_V560_RG = 70 ;
			else if CS_V560_Score > 0 then CS_V560_RG = 71 ;
		end;
		
		if ThinFileIndicator = 0 then do ;
			if CS_V570_Score >= 932.242611756651  Then CS_V570_RG = 50;
			else if CS_V570_Score >= 912.452480990053 Then CS_V570_RG = 51;
			else if CS_V570_Score >= 878.956333489911 Then CS_V570_RG = 52;
			else if CS_V570_Score >= 841.833690856176 Then CS_V570_RG = 53;
			else if CS_V570_Score >= 811.989999894282 Then CS_V570_RG = 54;
			else if CS_V570_Score >= 790.349339106057 Then CS_V570_RG = 55;
			else if CS_V570_Score >= 778.068960766859 Then CS_V570_RG = 56;
			else if CS_V570_Score >= 758.6444629 Then CS_V570_RG = 57;
			else if CS_V570_Score >= 746.152798684895 Then CS_V570_RG = 58;
			else if CS_V570_Score >= 732.001226390991 Then CS_V570_RG = 59;
			else if CS_V570_Score >= 708.169317621721 Then CS_V570_RG = 60;
			else if CS_V570_Score >= 690.87118531475 Then CS_V570_RG = 61;
			else if CS_V570_Score >= 675.057720140646 Then CS_V570_RG = 62;
			else if CS_V570_Score >= 654.743779812149 Then CS_V570_RG = 63;
			else if CS_V570_Score >= 640.469307325178 Then CS_V570_RG = 64;
			else if CS_V570_Score >= 622.758776266177 Then CS_V570_RG = 65;
			else if CS_V570_Score >= 596.376955694516 Then CS_V570_RG = 66;
		      else if CS_V570_Score > 0    Then CS_V570_RG = 67;
		end;
		else if ThinFileIndicator = 1 then do ;
			if CS_V570_Score >=      828.199869458644 then CS_V570_RG = 68 ;
			else if CS_V570_Score >= 762.179100967216 then CS_V570_RG = 69 ;
			else if CS_V570_Score >= 721.281349457995 then CS_V570_RG = 70 ;
			else if CS_V570_Score > 0 then CS_V570_RG = 71 ;
		end;
run;

proc sql;
	create table Base(drop=seg) as	
		select 
		a.tranappnumber as baseloanid, a.seg as comp_seg, a.thinfileindicator as comp_thin, 
		b.probability as V530, b.ProbabilityAdjusted5 as V535, b.finalRiskScore as V535_finalscore, b.RG6T as V535_RG,
		c.Probability as V500, c.ProbabilityAdjusted5 as V505, d.CS_V560_prob, a.*
		from scored_cs2 a, scored_cs d, final_model_v535 b, final_model_v505 c
		where 	a.tranappnumber = b.tranappnumber and 
				a.tranappnumber = c.tranappnumber and 
				a.tranappnumber = d.tranappnumber;
quit;

/*Creating ops variables to help us score*/
data base_ops;
	set Base;
	format AppDate yymmddn8.;
	AppDate = input(ApplicationDate,yymmdd10.);
	Week = put(week(AppDate, 'w'),z2.);

	if BehaveScore in ( . , 0, 9999) then Repeat = 0 ;
	else if BehaveScore not in (., 0, 9999) then Repeat = 1 ;

	if (ACCREDITEDINDICATOR = 'P' OR MEMBER IN ('PERSAL','PSAL') OR  EMPLOYERGROUPCODE IN ('PERSAL','PSAL')) THEN  PERSALINDICATOR = 1 ;
	else PERSALINDICATOR = 0 ;

	if SUBGROUPCREATEDATE  ne '' THEN  SG_CREATIONYEAR = input(put(SUBSTR(SUBGROUPCREATEDATE,1,4),$4.),4.); 
	ELSE SG_CREATIONYEAR = input(put(SUBSTR(APPLICATIONDATE,1,4),$4.),4.);

	if PERSALINDICATOR = 1 then do ;
		SG_CREATIONYEAR1 = SG_CREATIONYEAR ;
		SG_CREATIONYEAR2 = -10 ;
	end;
	else do ;
		SG_CREATIONYEAR2 = SG_CREATIONYEAR ;
		SG_CREATIONYEAR1 = -10 ;
	end;

	if CALCULATEDNETTINCOME = 0.00  then NETINCOME = MONTHLYNETTINCOME ;
	else if CALCULATEDNETTINCOME < MONTHLYNETTINCOME then NETINCOME = MONTHLYNETTINCOME ;
	else NETINCOME = CALCULATEDNETTINCOME ;

	if WAGEFREQUENCYCODE = 'WAG001' then do ;
		if MONTHLYGROSSINCOME = 0.00 then  GROSSINCOME = NETINCOME ;
		else GROSSINCOME = MONTHLYGROSSINCOME ;
	end;

	if WAGEFREQUENCYCODE = 'WAG002' then do ;
		if MONTHLYGROSSINCOME = 0.00 then  GROSSINCOME = NETINCOME ;
		else if MONTHLYGROSSINCOME < NETINCOME then GROSSINCOME =MONTHLYGROSSINCOME * 52/12  ;
		else GROSSINCOME = MONTHLYGROSSINCOME ; 
	end;

	if WAGEFREQUENCYCODE = 'WAG003' then do ;
		if MONTHLYGROSSINCOME = 0.00 then  GROSSINCOME = NETINCOME ;
		else if MONTHLYGROSSINCOME < NETINCOME then GROSSINCOME =MONTHLYGROSSINCOME * 26/12  ;
		else GROSSINCOME = MONTHLYGROSSINCOME ; 
	end;

	MonthDiff = intck('Months',input(ApplicationDate,yymmdd10.), '01APR2017'd) ;
	GrossIncomeAdjusted = GrossIncome*(1.004752962)**(MonthDiff);

	MonthsAtCurrentEmployer = intck('Months', input(CURRENTEMPLOYMENTSTARTDATE,yymmdd10.),input(ApplicationDate,yymmdd10.));     
run;

/*Creating the Tu thinfiles and Tu segments*/
data base_segmentation;
	set base_ops; 

	if PP0001AL <= 0 then TU_THIN = 1 ;
	else TU_THIN = 0 ;

	NP003_T = NP003;
	PP003_T = PP003;
	IF NP003 < 0 then NP003_T = 0;
	IF PP003 < 0 then PP003_T = 0;
	PP003_NP003 = NP003_T + PP003_T;

	if PP003_NP003 = 0 then  Tu_seg = 1;
	else if PP003_NP003 <= 2 then Tu_seg = 2;
	else if PP003_NP003 <= 8 then Tu_seg = 3;
	else if PP003_NP003 <= 12 then Tu_seg =4;
	else if PP003_NP003 > 12 then  Tu_seg = 5;
run;

/*Creating ratio variables to help us score*/
data base_ratio;
	set base_segmentation;

	IF PP0714AL >= 0 AND GROSSINCOME > 0 THEN PP0714AL_GI_RATIO = PP0714AL /GROSSINCOME;
	ELSE IF PP0714AL < 0 THEN PP0714AL_GI_RATIO = PP0714AL;
	ELSE IF GROSSINCOME < 0 THEN PP0714AL_GI_RATIO =-10 ;
	ELSE if GROSSINCOME = 0 THEN PP0714AL_GI_RATIO = -20;

	IF PP0801AL >= 0 AND GROSSINCOME > 0 THEN PP0801AL_GI_RATIO = PP0801AL/GROSSINCOME;
	ELSE IF PP0801AL < 0 THEN PP0801AL_GI_RATIO = PP0801AL;
	ELSE IF GROSSINCOME < 0 THEN PP0801AL_GI_RATIO =-10 ;
	ELSE if GROSSINCOME = 0 THEN PP0801AL_GI_RATIO = -20;


	IF PP0601AL >= 0 AND PP0604AL > 0 THEN PP0601AL_CU_RATIO_6 = PP0601AL/PP0604AL;
	ELSE IF PP0601AL < 0 THEN PP0601AL_CU_RATIO_6 = PP0601AL;
	ELSE IF PP0604AL < 0 THEN PP0601AL_CU_RATIO_6 =PP0604AL-10 ;
	ELSE IF PP0604AL = 0 THEN PP0601AL_CU_RATIO_6 = -20;

	IF PP0521LB >= 0 AND GROSSINCOME > 0 THEN PP0521LB_GI_RATIO = PP0521LB/GROSSINCOME;
	ELSE IF PP0521LB < 0 THEN PP0521LB_GI_RATIO = PP0521LB;
	ELSE IF GROSSINCOME < 0 THEN PP0521LB_GI_RATIO =-10 ;
	ELSE IF GROSSINCOME = 0 THEN PP0521LB_GI_RATIO = -20;

	if Tu_seg = 4 then do ;

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

	if Tu_seg = 5 then do ;
	   If ( RE006 > . and RE006 <= 1) or RE006 in (.)  then RE006_l = "High";
	      Else If (RE006 > . and RE006 <= 3)  then RE006_l = "Med";
	      Else If (RE006 > 3)  then RE006_l = "Low";

	      if  compress(RE019) in ('L','','W','J','I') then RE019_l ="High";
	      else if  compress(RE019) in ('+000000009.0','+000000008.0','+000000007.0','E') then RE019_l ="Med";
	      else if  compress(RE019) in ('+000000006.0','+000000005.0','+000000000.0','+000000004.0','+000000003.0','+000000002.0','+000000001.0') then RE019_l ="Low";

	      RE006_019 = compress(RE006_L||RE019_L);
	end;

	IF PP0503AL >= 0 AND PP0505AL > 0 THEN PP0503AL_3_RATIO_12 = PP0503AL/PP0505AL;
	ELSE IF PP0503AL < 0 THEN PP0503AL_3_RATIO_12 = PP0503AL;
	ELSE IF PP0505AL < 0 THEN PP0503AL_3_RATIO_12 =PP0505AL-10 ;
	ELSE if PP0505AL = 0 THEN PP0503AL_3_RATIO_12 = -20;

	IF PP0601AL >= 0 AND PP0603AL > 0 THEN PP0601AL_CU_RATIO_3 = PP0601AL/PP0603AL;
	ELSE IF PP0601AL < 0 THEN PP0601AL_CU_RATIO_3 = PP0601AL;
	ELSE IF PP0603AL < 0 THEN PP0601AL_CU_RATIO_3 =PP0603AL-10 ;
	ELSE if PP0603AL = 0 THEN PP0601AL_CU_RATIO_3 = -20;

	IF PP0515AL >= 0 AND GROSSINCOME > 0 THEN PP0515AL_GI_RATIO = PP0515AL/GROSSINCOME;
	ELSE IF PP0515AL < 0 THEN PP0515AL_GI_RATIO = PP0515AL;
	ELSE IF GROSSINCOME < 0 THEN PP0515AL_GI_RATIO =-10 ;
	ELSE if GROSSINCOME = 0 THEN PP0515AL_GI_RATIO = -20; 

	if PP116 <= 6 then PP173Adj = -13 ;
	else PP173Adj = PP173 ;
run;

data _null_;
	call symput("month2", put(intnx("month", today(),-1,'end'),eurdfmy5.));
run;
%put &month2;

/*Scoring TU_V570 an TU_V580*/
%scoring(inDataset=base_ratio, outDataset=scored_tu,Path=\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\TU Models\v570,scored_name =TU_V570, modelType =T);
%Scoring_new(inDataset=base_ratio, outDataset=scored_tu2,Path=\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU,scored_name =TU_V580, modelType =T);
/*%scoring(inDataset=base_ratio, outDataset=scored_tu2,Path=\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU,scored_name =TU_V580, modelType =T); */
/*%macro ex_TU();*/
/*	if applicationdate < '2023-06-15' then %do;*/
/*	%scoring(inDataset=base_ratio, outDataset=scored_tu2old,Path=\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU,scored_name =TU_V580, modelType =T);*/
/*	else */
/*	%Scoring_new(inDataset=base_ratio, outDataset=scored_tu2new,Path=\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU,scored_name =TU_V580, modelType =T);*/
/*	%end;*/
/**/
/*	data scored_tu2;*/
/*		set scored_tu2new scored_tu2old;*/
/*	run;*/
/*%mend;*/
/*%ex_TU();*/


proc sql;
    create table tu_scored as
	    select a.*, b.TU_V570_prob 
		from scored_tu2 a
	    left join scored_tu b
	    on a.tranappnumber = b.tranappnumber;
quit;

data BASE5_LIVE_&month2. (drop=seg);
	set tu_scored;
	if Min(comp_thin,TU_Thin) = 0 then Combine_Thin =0;
	else Combine_Thin=1;
run;

/******************V6 and Calibration*******************/
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\ApplyV622.sas";
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\ApplyV636.sas";
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\ApplyV645.sas";
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\ApplyV655.sas";
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\ApplyV667.sas";

%applyV622(inDataset=BASE5_LIVE_&month2., outDataset=v622_scored);
%applyV636(inDataset=BASE5_LIVE_&month2., outDataset=v636_scored);
%applyV645(inDataset=BASE5_LIVE_&month2., outDataset=v645_scored);
%applyV655(inDataset=BASE5_LIVE_&month2., outDataset=v655_scored);
%applyV667(inDataset=BASE5_LIVE_&month2., outDataset=v667_scored)

proc sql;
	create table scored as
		select 
		b.V620, b.V621, b.V622,
		c.V630, c.V631, c.V632, c.V633, c.V634, c.V635, c.V636,
		d.V640, d.V641, d.V642, d.V643, d.V644, d.V645,
		e.V650 as V650_2, e.V651 as V651_2, e.V652 as V652_2, e.V653 as V653_2, e.V654 as V654_2, e.V655 as V655_2,
		f.V660, f.V661, f.V662, f.V663, f.V664, f.V665, f.V667,
		 . as V655_2_B05,
		b.V622_finalriskscore, b.V622_RiskGroup,
		c.V636_finalriskscore, c.V636_RiskGroup,
		d.V645_finalriskscore, d.V645_RiskGroup,
		e.V655_finalriskscore as V655_2_finalriskscore, e.V655_RiskGroup as V655_2_RiskGroup,
		 . as V655_2_B05_finalriskscore, . as V655_2_B05_RiskGroup,
		f.V667_finalriskscore, f.V667_RiskGroup,
		a.*
		from BASE5_LIVE_&month2. a
		left join v622_scored b
		on a.tranappnumber = b.tranappnumber
		left join v636_scored c
		on a.tranappnumber = c.tranappnumber
		left join v645_scored d
		on a.tranappnumber = d.tranappnumber
		left join v655_scored e
		on a.tranappnumber = e.tranappnumber
		left join v667_scored f
		on a.tranappnumber = f.tranappnumber
		;
quit;

/* score the whole apploication base using the v655 macro */
data v645_scored;
	set scored;
	V635_finalriskscore =  1000-(V635*1000);
	if Min(comp_thin,TU_Thin) = 0 then do;
		if V635_finalriskscore >= 932.242611756651      then V635_RiskGroup = 50;
		else if V635_finalriskscore >= 912.452480990053 then V635_RiskGroup = 51;
		else if V635_finalriskscore >= 878.956333489911 then V635_RiskGroup = 52;
		else if V635_finalriskscore >= 841.833690856176 then V635_RiskGroup = 53;
		else if V635_finalriskscore >= 811.989999894282 then V635_RiskGroup = 54;
		else if V635_finalriskscore >= 790.349339106057 then V635_RiskGroup = 55;
		else if V635_finalriskscore >= 778.068960766859 then V635_RiskGroup = 56;
		else if V635_finalriskscore >= 758.6444629 	    then V635_RiskGroup = 57;
		else if V635_finalriskscore >= 746.152798684895 then V635_RiskGroup = 58;
		else if V635_finalriskscore >= 732.001226390991 then V635_RiskGroup = 59;
		else if V635_finalriskscore >= 708.169317621721 then V635_RiskGroup = 60;
		else if V635_finalriskscore >= 690.87118531475  then V635_RiskGroup = 61;
		else if V635_finalriskscore >= 675.057720140646 then V635_RiskGroup = 62;
		else if V635_finalriskscore >= 530 			    then V635_RiskGroup = 63;
		else if V635_finalriskscore >= 529			    then V635_RiskGroup = 64;
		else if V635_finalriskscore >= 527 			    then V635_RiskGroup = 65;
		else if V635_finalriskscore >= 500 			    then V635_RiskGroup = 66;
		else if V635_finalriskscore > 0    			    then V635_RiskGroup = 67;
	end;

	else if Min( comp_thin,TU_Thin) = 1 then do;
		if V635_finalriskscore >=      828.199869458644 then V635_RiskGroup = 68;
		else if V635_finalriskscore >= 762.179100967216 then V635_RiskGroup = 69;
		else if V635_finalriskscore >= 721.281349457995 then V635_RiskGroup = 70;
		else if V635_finalriskscore > 0 				then V635_RiskGroup = 71;
	end;

	V645_adj=v645*1.1;  * with 10% buffer;
	V645_Adjfinalriskscore  =  1000-(V645_adj*1000);
	if Min( comp_thin,TU_Thin) = 0 then do;
		if V645_Adjfinalriskscore >= 932.242611756651      then V645_AdjRG = 50;
		else if V645_Adjfinalriskscore >= 912.452480990053 then V645_AdjRG = 51;
		else if V645_Adjfinalriskscore >= 878.956333489911 then V645_AdjRG = 52;
		else if V645_Adjfinalriskscore >= 841.833690856176 then V645_AdjRG = 53;
		else if V645_Adjfinalriskscore >= 811.989999894282 then V645_AdjRG = 54;
		else if V645_Adjfinalriskscore >= 790.349339106057 then V645_AdjRG = 55;
		else if V645_Adjfinalriskscore >= 778.068960766859 then V645_AdjRG = 56;
		else if V645_Adjfinalriskscore >= 758.6444629 	    then V645_AdjRG = 57;
		else if V645_Adjfinalriskscore >= 746.152798684895 then V645_AdjRG = 58;
		else if V645_Adjfinalriskscore >= 732.001226390991 then V645_AdjRG = 59;
		else if V645_Adjfinalriskscore >= 708.169317621721 then V645_AdjRG = 60;
		else if V645_Adjfinalriskscore >= 690.87118531475  then V645_AdjRG = 61;
		else if V645_Adjfinalriskscore >= 675.057720140646 then V645_AdjRG = 62;
		else if V645_Adjfinalriskscore >= 530 			    then V645_AdjRG = 63;
		else if V645_Adjfinalriskscore >= 529			    then V645_AdjRG = 64;
		else if V645_Adjfinalriskscore >= 527 			    then V645_AdjRG = 65;
		else if V645_Adjfinalriskscore >= 500 			    then V645_AdjRG = 66;
		else if V645_Adjfinalriskscore > 0    			    then V645_AdjRG = 67;
	end;

	else if Min( comp_thin,TU_Thin) = 1 then do;
		if V645_Adjfinalriskscore >=      828.199869458644 then V645_AdjRG = 68;
		else if V645_Adjfinalriskscore >= 762.179100967216 then V645_AdjRG = 69;
		else if V645_Adjfinalriskscore >= 721.281349457995 then V645_AdjRG = 70;
		else if V645_Adjfinalriskscore > 0 				then V645_AdjRG = 71;
	end;
run;

/*data comp.all_scores_v645_&runmonth;*/
/*	set v645_scored;*/
/*run;*/

proc sql;
	create table CS_applicationbase_&month as
		select 
		b.TU_V570_Prob, b.TU_V580_Prob,
		b.V620, b.V621, b.V622, 
		b.V630, b.V631, b.V632, b.V633, b.V634, b.V635, b.V636,
		b.V640, b.V641, b.V642, b.V643, b.V644, b.V645, b.V645_adj,
		b.V650_2, b.V651_2, b.V652_2, b.V653_2, b.V654_2, b.V655_2, b.V655_2_B05,
		b.V660, b.V661, b.V662, b.V663, b.V664, b.V665, b.V667,
		b.V622_RiskGroup, b.V635_RiskGroup as V635_RG, b.V636_RiskGroup as V636_RG, b.V636_RiskGroup,
		b.V645_RiskGroup as V645_RG , b.V645_Adjfinalriskscore, b.V645_AdjRG, 
		b.V655_2_RiskGroup as V655_2_RG , b.V655_2_finalriskscore, 
		b.V655_2_B05_RiskGroup as V655_2_B05_RG, b.V655_2_B05_finalriskscore, 
		b.V667_RiskGroup, b.V667_finalriskscore,
		b.Combine_Thin, b.appmonth, a.*
		from Base a
		left join v645_scored b
		on a.tranappnumber = b.tranappnumber;
quit;

data TU_applicationbase_&month;
	set V645_SCORED(drop=COMPUSCANVAR1401 COMPUSCANVAR1424 COMPUSCANVAR175 COMPUSCANVAR187 COMPUSCANVAR188
				COMPUSCANVAR2123 COMPUSCANVAR2312 COMPUSCANVAR2528 COMPUSCANVAR2678 COMPUSCANVAR2696
				COMPUSCANVAR3275 COMPUSCANVAR3916 COMPUSCANVAR3935 COMPUSCANVAR5208 COMPUSCANVAR5486
				COMPUSCANVAR5489 COMPUSCANVAR5579 COMPUSCANVAR5826 COMPUSCANVAR6073 COMPUSCANVAR6130
				COMPUSCANVAR6132 COMPUSCANVAR6134 COMPUSCANVAR6285 COMPUSCANVAR6788 COMPUSCANVAR716
				COMPUSCANVAR733 COMPUSCANVAR7430 COMPUSCANVAR7431 COMPUSCANVAR7479 COMPUSCANVAR753
				COMPUSCANVAR7547 COMPUSCANVAR7549 COMPUSCANVAR7550 COMPUSCANVAR7683 UNS_PercUtilisation UNS_ValCurBalMR60Days CSN_TimeOldestTrade ALL_TimeMREnq ALL_TimeOldestEnq
                AUL_NumOpenTrades ALL_MaxDelq1YearLT24M OWN_Perc1pDelq2Years OTH_MaxDelqEver
                OTH_MaxDelq1YearLT24M REV_MaxDelq180DaysGE24M UNS_TimeMR3pDelq UNS_MaxDelq180DaysLT12M
                AIL_Num1pDelq90Days ALL_NumEverTrades ALL_NumTrades90Days OTH_AvgMonthsOnBook UNS_AvgMonthsOnBook
                RCG_AvgMonthsOnBook UNN_AvgMonthsOnBook ALL_ValOrgBalLim90Days ALL_ValOrgBalLim1Year
                OTH_ValOrgBalLim180Days UNS_ValCurBal1Year OWN_PercUtiliSatisfTrades
                OWN_AvgPercUtilisationMR60Days ALL_NumPayments2Years ALL_PercPayments2Years
                OTH_PercPayments2Years OTH_ValOrgBalLim REV_PercPayments180Days REV_PercPayments1Year
                REV_NumPayments2Years OPL_PercPayments2Years week);
	UNIQUEID0 = UNIQUEID;
	loanid = BaseLoanid;
run;


%macro add_miss_vars(appbase= ,base_cols=);

	libname cols "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard Monthly Monitoring";
	data &base_cols ;
		set cols.&base_cols ;
	run;

	proc contents data= &appbase._&month out=month_Cols(keep=NAME TYPE LENGTH VARNUM ) noprint;run;


	proc sql ; select a.NAME into :misscolmth_num separated by ' ' from &base_cols a full join month_Cols b on upcase(a.NAME)=upcase(b.NAME) where upcase(a.NAME)<>upcase(b.NAME) and a.TYPE = 1;quit;
	proc sql ; select a.NAME into :miscolmth_char separated by ' ' from &base_cols a full join month_Cols b on upcase(a.NAME)=upcase(b.NAME) where upcase(a.NAME)<>upcase(b.NAME) and a.TYPE = 2;quit;
	proc sql ; select b.NAME into :misscol_ separated by ' ' from &base_cols a full join month_Cols b on upcase(a.NAME)=upcase(b.NAME) where upcase(a.NAME)<>upcase(b.NAME);quit;

	%macro nullify(columns,null_val);
		%do i = 1 %to %sysfunc(countw(&columns)); 
			%let var = %scan(&columns,&i);
			&var = &null_val;
		%end;
	%mend;

	%if %length(&misscolmth_num) > 0 or %length(&miscolmth_char) > 0 %then %do;	
		data &appbase._&month (drop=BRANCHCODE rename=(branchcodex=branchcode));
			set &appbase._&month;
			%nullify(&misscolmth_num,.);
			%nullify(&miscolmth_char,"");
			format branchcodex 23. ;
			branchcodex = input(BRANCHCODE, 8.);
			drop &misscol_ miscolmth_char misscolmth_num;
		run;
	%end;


	proc sql; create table fincols as select * from &base_cols order by VARNUM; quit;
	proc sql; select NAME into :finalcols separated by ',' from fincols;quit;

	/*MAKE ORDER THE SAME*/
	proc sql; 
		create table &appbase._&month as
			select &finalcols 
			from &appbase._&month;
	quit;
		
%mend;
%add_miss_vars(appbase=CS_applicationbase,base_cols= CS_base_Columns)

data CS_applicationbase_&month;
	set CS_applicationbase_&month;
	rundate = input(put(today(),yymmddn8.),8.);
	if loanid = '' then loanid=tranappnumber;
run;

%Upload_APS(Set =CS_applicationbase_&month , Server =Work, APS_ODBC = Dev_DDGe, APS_DB = DEV_DataDistillery_General , distribute = HASH([baseloanid]));



/*dropping compuscan variables from TU apps before uploading*/
%let newvarlist =	COMPUSCANVAR187 COMPUSCANVAR188 COMPUSCANVAR2123 COMPUSCANVAR2312 COMPUSCANVAR2528
					COMPUSCANVAR2678 COMPUSCANVAR2696 COMPUSCANVAR3275 COMPUSCANVAR3916 COMPUSCANVAR3935
					COMPUSCANVAR5208 COMPUSCANVAR5486 COMPUSCANVAR5489 COMPUSCANVAR5579 COMPUSCANVAR5826
					COMPUSCANVAR6073 COMPUSCANVAR6130 COMPUSCANVAR6132 COMPUSCANVAR6134 COMPUSCANVAR6285
					COMPUSCANVAR6788 COMPUSCANVAR716 COMPUSCANVAR733 COMPUSCANVAR7430 COMPUSCANVAR7431
					COMPUSCANVAR7479 COMPUSCANVAR753 COMPUSCANVAR7547 COMPUSCANVAR7549 COMPUSCANVAR7550 COMPUSCANVAR7683;
%let oldvarlist =	UNS_PercUtilisation UNS_ValCurBalMR60Days CSN_TimeOldestTrade ALL_TimeMREnq ALL_TimeOldestEnq
					AUL_NumOpenTrades ALL_MaxDelq1YearLT24M OWN_Perc1pDelq2Years OTH_MaxDelqEver
					OTH_MaxDelq1YearLT24M REV_MaxDelq180DaysGE24M UNS_TimeMR3pDelq UNS_MaxDelq180DaysLT12M
					AIL_Num1pDelq90Days ALL_NumEverTrades ALL_NumTrades90Days OTH_AvgMonthsOnBook UNS_AvgMonthsOnBook
					RCG_AvgMonthsOnBook UNN_AvgMonthsOnBook ALL_ValOrgBalLim90Days ALL_ValOrgBalLim1Year
					OTH_ValOrgBalLim180Days UNS_ValCurBal1Year OWN_PercUtiliSatisfTrades
					OWN_AvgPercUtilisationMR60Days ALL_NumPayments2Years ALL_PercPayments2Years
					OTH_PercPayments2Years OTH_ValOrgBalLim REV_PercPayments180Days REV_PercPayments1Year
					REV_NumPayments2Years OPL_PercPayments2Years;

%macro ApplyLoop();
	%do i = 1 %to %sysfunc(countw(&newvarlist));
		%let variable = %scan(&newvarlist, &i);
		&variable._b &variable._W &variable._S
    %end;
	%do i = 1 %to %sysfunc(countw(&oldvarlist));
		%let variable = %scan(&oldvarlist, &i);
		&variable._b &variable._W &variable._S
    %end;
%mend;

data TU_applicationbase_&month;
	set TU_applicationbase_&month;
	drop &newvarlist &oldvarlist;
	drop %ApplyLoop();
run;

%add_miss_vars(appbase=TU_applicationbase,base_cols= TU_base_Columns);

data TU_applicationbase_&month;
	set TU_applicationbase_&month;
	rundate = input(put(today(),yymmddn8.),8.);
run;
%Upload_APS(Set =TU_applicationbase_&month , Server =Work, APS_ODBC = Dev_DDGe, APS_DB = DEV_DataDistillery_General , distribute = HASH([baseloanid]));


/* APPLICATION BASE TABLES CHECKS */
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\cs_checks.sas";
%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\tu_checks.sas";

/* SEND EMAIL WITH REPORTS FOR APPROVAL */
%macro sendEmail();
%let Attachment1 = \\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard Monthly Monitoring\base_tables_reports\cs_basechecks_&month..pdf;
%let Attachment2 = \\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard Monthly Monitoring\base_tables_reports\tu_basechecks_&month..pdf;

%let approvalPath = \\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros;

%IF %SYSFUNC(FILEEXIST("&Attachment1")) AND %SYSFUNC(FILEEXIST(&Attachment2)) %THEN %DO;
	options emailport=25 emailsys =SMTP emailhost = midrandcasarray.africanbank.net;
	options emailackwait=300;
	filename output email 
	to=("AMpyatona@africanbank.co.za" "kmolawa@africanbank.co.za" "TMphogo@AfricanBank.co.za" "Eshikwambana@AfricanBank.co.za" "ryisa@africanbank.co.za")
	from="DataScienceAutomation@AfricanBank.co.za"
/*	to=("AMpyatona@africanbank.co.za") */
/*	from="AMpyatona@africanbank.co.za" */

	content_type="text/html"
	subject="Appbase Tables Approval"
	attach=("&Attachment1" "&Attachment2");

	ods html file=output rs=none;

	proc odstext;
	   p "Hi everyone,";
	   p " ";
	   p " ";
	   p "Please find attached the detailed report on the TU and CS applicationbase tables. ";
	   p " ";
	   p "Elvis/Tshepo, please run the approval code 'CS_TU_Approval' in the path below:";
	   p "&approvalPath. ";
	   p " ";
	   p "Kind Regards";
	   p "Data Science Automation.";
	run;

	ods html close;
%END;
%ELSE %PUT ERROR: FILE DOES NOT EXIST AND NO EMAIL WILL BE SENT.;
%mend;
%sendEmail();

