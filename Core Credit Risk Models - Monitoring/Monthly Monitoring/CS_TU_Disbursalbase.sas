*%include "\\neptune\sasa$\SAS_Automation\SAS_Autoexec\autoexec2.sas";

OPTIONS NOSYNTAXCHECK;
options compress = yes;

libname tu '\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets\V580';
libname comp "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\V570";
libname decile "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\SAS Decile Tables\TU V570";
libname decile2 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\SAS Decile Tables\TU V580";
libname Rejects "\\Neptune\SASA$\V5\New_Rejects";

data _null_;
     call symput("startdate",cats("'",put(intnx('month',today(),-13,'end'),yymmn6.),"'"));
     call symput("actual_date", put(intnx("month", today(),-9,'end'),date9.));
     call symput("month", put(intnx("month", today(),-1,'end'),yymmn6.));
run;
%let odbc = MPWAPS;
%put &startdate;
%put &actual_date;
%put &month;

/*------------------------------------------------------------------*/
/* Change Control: */
/*------------------------------------------------------------------*/
/* Developer: Lindokuhle Masina */
/* Date: 30 June 2021*/
/*
Changes Made:
1.Removing the rejets
*/
/* Reason: Our reject inference model has broken due to us not receiving information on the OM. 
	In the mean time can we change our monitoring to only be on Disbursals and not with rejects included*/
/*------------------------------------------------------------------*/
%macro getOutcomeData(Inputdataset=, OutcomeBase=);
	proc sql stimer;
		connect to ODBC (dsn=&odbc);
		create table DisbursedBase as 
		select * from connection to odbc 
		( 
			select 	B.Principaldebt,
					B.Capital_OR_Limit,
					B.product,
					B.Contractual_3_LE9,
					B.FirstDueMonth,
					E.FirstDueDate,
                    E.product as product1,
					coalesce(C.Final_score_1, F.Final_score_1) as Final_score_1,
					coalesce(C.FirstDueMonth, F.FirstDueMonth) as PredictorMonth,
					cast(d.LNG_SCR as int) as TU_Score, A.*
			from (select * from &Inputdataset where appmonth >=&startdate.) A 
			inner join PRD_DataDistillery_data.dbo.Disbursement_Info E
			on a.tranappnumber = e.loanid
			left join PRD_DataDistillery_data.dbo.JS_Outcome_base_final B 
			on a.tranappnumber = B.loanid 
			left join CREDITPROFITABILITY.dbo.ELR_LOANESTIMATES_3_9_CALIB C 
			on b.loanid = c.loanid
		 	left join CREDITPROFITABILITY.dbo.ELR_CARDESTIMATES_3_9_CALIB F
    		on b.loanid = F.loanid 
			left join PRD_PRESS.capri.CAPRI_BUR_PROFILE_TRANSUNION_PLSCORECARD d
			on a.uniqueid = d.uniqueid
		) ;
		disconnect from odbc ;
	quit;

 	proc sort data = DisbursedBase nodupkey;
		by tranappnumber;
	run;

	data DisbursedBase1;
		set DisbursedBase;
		/* Step 1: Fill in missings */
		if product = "" then product = product1;
		if Principaldebt = . and product = 'Card' then Principaldebt = Capital_OR_Limit;

		/* Step 2: Fromat Dates */
		month2 = datepart(FirstDueMonth);
		month = put(month2, yymmn6.);
		if month=. or month=0 then delete;
		format month2 MONYY5.;

		/* Step 3: Create Target */
		if month2 >= intnx('month',"&actual_date"d,-2,'begin') and month2 <= intnx('month',"&actual_date"d,3,'end');
		count=1;
		target = Contractual_3_LE9 ;
		randomnum = uniform(12) ; *12 is the seed, and the random does not change;
		if month2 <= "&actual_date"d then HaveActuals = 1 ;
		else HaveActuals = 0 ; 
		if HaveActuals = 1 then Target = CONTRACTUAL_3_LE9 ;
		else if  (HaveActuals = 0 and  randomnum <= Final_score_1)  then Target = 1 ;
		else if  (HaveActuals = 0 and  randomnum > Final_score_1) then Target = 0;
	run;
	
	data &OutcomeBase (drop=month5 Capital_OR_Limit product1);
		set DisbursedBase1 (rename=(month = month5));
		format month $6.;
		month = month5;
    run;
%mend;

%getOutcomeData(Inputdataset=DEV_DataDistillery_General.dbo.TU_ApplicationBase, OutcomeBase=Disbursedbase4reblt_&month);
%getOutcomeData(Inputdataset=DEV_DataDistillery_General.dbo.CS_Applicationbase, OutcomeBase=disbursedbase_&month);

data disbursedbase_&month;
	set disbursedbase_&month;
	drop PP003_NP003 PP0714AL_GI_RATIO PP0801AL_GI_RATIO PP0601AL_CU_RATIO_6 PP0521LB_GI_RATIO RE006_L 
	RE019_L RE006_019 PP0503AL_3_RATIO_12 PP0601AL_CU_RATIO_3 PP0515AL_GI_RATIO PP173Adj 
	TU_V570_Score PP173_B PP173_W PP173_S PP0601AL_CU_RATIO_6_B PP0601AL_CU_RATIO_6_W 
	PP0714AL_GI_RATIO_B PP0714AL_GI_RATIO_W PP149_B PP149_W RE006_019_B RE006_019_W PP0407AL_S 
	PP0601AL_CU_RATIO_6_S PP0714AL_GI_RATIO_S PP149_S RE006_019_S TU_V580_Score  
	CAPRI_AFFORDABILITY CAPRI_BANKING CAPRI_BUR_PROFILE_PINPOINT 
	CAPRI_BUR_PROFILE_TRANSUNION_AGG CAPRI_BUR_PROFILE_TRANSUNION_BCC 
	EQ0015PL_W DM0001AL_B DM0001AL_W EQ2012AL_B EQ2012AL_W 
	DM0001AL_S EQ2012AL_S PP173Adj_B PP173Adj_W PP0801AL_GI_RATIO_B PP0801AL_GI_RATIO_W PP0406AL_B 
	PP0406AL_W PP0327AL_B PP0327AL_W PP0601AL_CU_RATIO_3_B PP0601AL_CU_RATIO_3_W PP0325AL_B 
	PP0325AL_W PP173ADJ_S PP0801AL_GI_RATIO_S PP0406AL_S PP0327AL_S PP0601AL_CU_RATIO_3_S 
	PP0325AL_S PP0407AL_B PP0407AL_W PP0935AL_B PP0935AL_W PP0051CL_B PP0051CL_W PP0313LN_B 
	PP0313LN_W PP0935AL_S PP0051CL_S PP0313LN_S PP0503AL_3_RATIO_12_B PP0503AL_3_RATIO_12_W 
	EQ0015PL_B PP0503AL_3_RATIO_12_S EQ0015PL_S PP0901AL_B PP0901AL_W PP0111LB_B PP0111LB_W 
	PP0171CL_B PP0171CL_W PP0901AL_S PP0111LB_S PP0171CL_S;
run;

data comp.disbursedbase_&month;
set disbursedbase_&month;
run;

data tu.Disbursedbase4reblt_&month;
set Disbursedbase4reblt_&month;
run;

%Upload_APS(Set=disbursedbase_&month, Server =Work, APS_ODBC = Dev_DDGe, APS_DB = DEV_DataDistillery_General , distribute = HASH([tranappnumber]));
%Upload_APS(Set =Disbursedbase4reblt_&month , Server =Work, APS_ODBC = Dev_DDGe, APS_DB = DEV_DataDistillery_General , distribute = HASH([tranappnumber]));

*filename macros2 'H:\Process_Automation\macros';
*options sasautos = (sasautos  macros2);
