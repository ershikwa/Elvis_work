/*%GetData(table=DEV_DataDistillery.dbo.V6DailyData, outDataset=rawdata5, dsn=DEV_DDGe);*/
/*%let table=DEV_DataDistillery_general.dbo.V6DailyData;*/
/*%let dsn=DEV_DDGe;*/
/*%let outdataset=rawdata5;*/
%macro GetData(table=, outDataset=, dsn=);
	/*Source data used for scoring on APS*/
	libname tablelib odbc dsn=&dsn schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

	proc sql stimer;	
		connect to ODBC (dsn=MPWAPS);
			create table V6DailyData as 
			select * from connection to odbc 
			(	
				Select distinct uniqueid, caprikey, tranappnumber, transysteminstance, branchcode, channelcode, applicationdate, applicationtime, nationalid
				from PRD_Press.capri.capri_loan_application 
				where  applicationdate >= &lstDate and TRANSEQUENCE <> '005' and channelcode <> 'CCC006' and channelcode <> 'CCC023' and channelcode <> 'CCC030' 
				
		 	);
		disconnect from ODBC;
	quit;

	proc sort data = V6DailyData; by tranappnumber descending uniqueid; run;
	proc sort data = V6DailyData nodupkey dupout=dups  out= V6DailyData; by tranappnumber; run;
	
	proc sql;
   		drop table tablelib.V6DailyData;

	    create table tablelib.V6DailyData like V6DailyData;

	    insert into tablelib.V6DailyData (bulkload=Yes)
	    select *
	    from V6DailyData;
	quit;

	proc sql stimer;	
		connect to ODBC (dsn=MPWAPS);
			create table applicationtable1 as 
			select * from connection to odbc 
			(	
				Select distinct a.*,
					c.TypeCode, c.CURRENTEMPLOYMENTSTARTDATE,c.EMPLOYERGROUPCODE,c.EMPLOYERSUBGROUPCODE, c.MEMBER, 
					c.ACCREDITEDINDICATOR, c.SUBGROUPCREATEDATE,c.WAGEFREQUENCYCODE,
					d.MONTHLYGROSSINCOME,d.MONTHLYNETTINCOME, d.CALCULATEDNETTINCOME,
					e.INSTITUTIONCODE,
/*					case when a.uniqueid = b.uniqueid then 1 else 0 end as CAPRI_APPLICANT,*/
					case when a.uniqueid = c.uniqueid then 1 else 0 end as CAPRI_EMPLOYMENT,
					case when a.uniqueid = d.uniqueid then 1 else 0 end as CAPRI_AFFORDABILITY,
					case when a.uniqueid = e.uniqueid then 1 else 0 end as CAPRI_BANKING
				from &table a
/*				left join PRD_Press.capri.CAPRI_APPLICANT b*/
/*				on a.uniqueid = b.uniqueid*/
				left join PRD_Press.capri.CAPRI_EMPLOYMENT c 
				on a.uniqueid = c.uniqueid
				left join PRD_Press.capri.CAPRI_AFFORDABILITY d 
				on a.uniqueid = d.uniqueid
				left join PRD_Press.capri.CAPRI_BANKING e
				on a.uniqueid = e.uniqueid;	
		 	 );
		disconnect from ODBC;
	quit;
	
	proc sql stimer;	
		connect to ODBC (dsn=MPWAPS);
			create table applicationtable2 as 
			select * from connection to odbc 
			(	
				Select distinct a.*,
					f.AIL_NUM1PDELQ90DAYS, f.ALL_MAXDELQ1YEARLT24M, f.ALL_NUMPAYMENTS2YEARS,
					f.ALL_NUMEVERTRADES, f.ALL_NUMTRADES90DAYS, f.ALL_PERCPAYMENTS2YEARS, f.ALL_RATIOORGBALLIM1YEAR,
					f.ALL_TIMEMRENQ, f.ALL_TIMEOLDESTENQ, f.ALL_VALORGBALLIM1YEAR, f.ALL_VALORGBALLIM90DAYS,
					f.AUL_NUMOPENTRADES, f.CSN_TIMEOLDESTTRADE, f.CST_CUSTOMERAGE, f.OPL_PERCPAYMENTS2YEARS,
					f.OTH_AVGMONTHSONBOOK, f.OTH_MAXDELQ1YEARLT24M, f.OTH_MAXDELQEVER, f.OTH_PERCPAYMENTS2YEARS,
					f.OTH_VALORGBALLIM, f.OTH_VALORGBALLIM180DAYS, f.OWN_AVGPERCUTILISATIONMR60DAYS,
					f.OWN_PERC1PDELQ2YEARS, f.OWN_PERCUTILISATISFTRADES, f.RCG_AVGMONTHSONBOOK,
					f.REV_MAXDELQ180DAYSGE24M, f.REV_NUMPAYMENTS2YEARS, f.REV_PERCPAYMENTS180DAYS,
					f.REV_PERCPAYMENTS1YEAR, f.UNN_AVGMONTHSONBOOK, f.UNS_AVGMONTHSONBOOK, f.UNS_MAXDELQ180DAYSLT12M,
					f.UNS_PERCUTILISATION, f.UNS_TIMEMR3PDELQ, f.UNS_VALCURBAL1YEAR, f.UNS_VALCURBALMR60DAYS,
					f.VAP_GMIPVALUE, f.PRISMSCORETM, f.PRISMSCOREMI, f.ALL_MAXDELQ90DAYS, f.ALL_NUM1PDELQ90DAYS,
					f.ALL_NUMADVS1YEAR, f.ALL_TIMEMRAO, f.ALL_TIMEMRJUDG, f.ALL_TIMEMRNOTICE, f.CSN_NUMTRADES90DAYS,
					f.CST_DEBTREVIEWGRANTED, f.CST_DEBTREVIEWREQUESTED, f.CST_DISPUTE, f.OWN_NUMOPENTRADES,					   
					g.behavescore, g.behavescorev2,
					h.HD001, h.HD002, h.HD003, h.JN001, h.JN002, h.JN003, h.JN004, h.JN010, h.NG004, h.NP003, h.NP013,
					h.NP015, h.NT001, h.NT003, h.NT004, h.NT005, h.NT006, h.NT007, h.NT008, h.PP003, h.PP013, h.PP014, 
					h.PP015, h.PP116, h.PP149, h.PP173, h.PP283, h.RE006, h.RE019, h.JD002, h.JD003, h.NP005, h.PP005, 
					h.DF001, h.NG2401AL, h.NG3402AL, 
					i.DM0001AL, i.EQ0012AL, i.EQ2012AL, i.EQ0015PL, i.EQ2015PL, i.PP0001AL, i.PP0051CL, i.PP0111LB, 
					i.PP0171CL, i.PP0313LN, i.PP0325AL, i.PP0327AL, i.PP0406AL, i.PP0407AL, i.PP0421CL, i.PP0503AL, 
					i.PP0505AL, i.PP0515AL, i.PP0521LB, i.PP0601AL, i.PP0603AL, i.PP0604AL, i.PP0714AL, i.PP0801AL, 
					i.PP0901AL, i.PP0935AL,
					case when a.uniqueid = f.uniqueid then 1 else 0 end as  CAPRI_BUR_PROFILE_PINPOINT,
					case when a.uniqueid = g.uniqueid then 1 else 0 end as  capri_behavioural_score,
					case when a.uniqueid = h.uniqueid then 1 else 0 end as  CAPRI_BUR_PROFILE_TRANSUNION_AGGREGATE ,
					case when a.uniqueid = i.uniqueid then 1 else 0 end as  CAPRI_BUR_PROFILE_TRANSUNION_BCC					   
				from &table a
				left join PRD_Press.[capri].CAPRI_BUR_PROFILE_PINPOINT as f
				on a.uniqueid = f.uniqueid
				left join (select uniqueid, score as behavescore , scorev2 as behavescorev2 from PRD_Press.capri.capri_behavioural_score where classification = 'AB')  g
				on a.uniqueid = g.uniqueid
				left join PRD_Press.CAPRI.CAPRI_BUR_PROFILE_TRANSUNION_AGGREGATE h
				on a.uniqueid = h.uniqueid
				left join PRD_Press.CAPRI.CAPRI_BUR_PROFILE_TRANSUNION_BCC i
				on a.uniqueid = i.uniqueid;
		 	 );
		disconnect from ODBC;
	quit;

	proc sql stimer;	
		connect to ODBC (dsn=MPWAPS);
			create table applicationtable3 as 
			select * from connection to odbc 
			(	
				Select distinct a.*,
					j.scoreband, j.applicationrouting, j.scorecard, j.overridescoreband, j.canoverridescoreband,
					l.declinecode, l.declinedescription, ScoringDecline,
					m.SCORECARDVERSION,		   				   
					case when a.uniqueid = j.uniqueid then 1 else 0 end as capri_scoring_results,
					/*case when a.uniqueid = k.uniqueid then 1 else 0 end as capri_loan_application,*/
					case when a.uniqueid = k.uniqueid then 1 else 0 end as offer
				from &table a
				left join PRD_Press.Capri.capri_scoring_results j
				on a.uniqueid = j.uniqueid
				left join PRD_Press.capri.capri_offer_results k
				on a.uniqueid = k.uniqueid
				left join (select uniqueid, declinecode, declinedescription, max(a.ScoreDecline) as ScoringDecline
							from (select uniqueid, declinecode, declinedescription, case when declinecode in ('DS276','DS293','DS608','DS783','DS784','DS785') then 1 else 0 end as ScoreDecline 
									from PRD_Press.capri.capri_application_decline) a
							group by uniqueid, declinecode, declinedescription) l
			on a.uniqueid = l.uniqueid
			left join PRD_Press.capri.CAPRI_TESTING_STRATEGY_RESULTS m
			on a.uniqueid = m.uniqueid;
		 	);
		disconnect from ODBC;
	quit;

	proc sql stimer;	
		connect to ODBC (dsn=MPWAPS);
			create table applicationtable4 as 
			select * from connection to odbc 
			(
				Select distinct a.*	, 
					n.applicantsegment, 
					o.V601 as V630_prod, o.V602 as V631_prod, o.V603 as V632_prod, n.V633 as V633_prod, n.V634 as V634_prod, n.V635 as V635_prod, n.V636 as V636_prod, /*V636 40%*/
					o.V640 as V640_prod, o.V641 as V641_prod, o.V642 as V642_prod, o.V643 as V643_prod, o.V644 as V644_prod, o.V645 as V645_prod, /*V645 20%*/
					o.V650 as V650_Prod, o.V651 as V651_Prod, o.V652 as V652_Prod, o.V653 as V653_Prod, o.V654 as V654_Prod, o.V655 as V655_Prod,
					o.V601 as V620_prod, o.V602 as V621_prod, o.V603 as V622_prod, /*V622 20%*/
					o.scoringfinalRiskScore as V6_FinalRiskScore_prod , o.scoringScoreBand as V6_Riskgroup_prod,
					p.probability as TU_V570_prod,  p.probabilityCS as CS_V560_prod,
					coalesce(q.transunionProb,e.transunionProb) as TU_V580_prod,  coalesce(q.compuscanProb,e.compuscanProb) as CS_V570_prod
				from &table a
				left join PRD_Press.Capri.CreditRisk_SegmentProbabilityAdjTUFunction n
				on a.uniqueid = n.uniqueid
				left join PRD_Press.Capri.CreditRisk_RiskGroup o
				on a.uniqueid = o.uniqueid
				left join PRD_Press.[Capri].[CreditRisk_RiskGrouptu] p
				on a.uniqueid = p.uniqueid
				left join PRD_Press.[Capri].[CreditRisk_ProbabilityV645] q
				on a.uniqueid = q.uniqueid
	            left join PRD_press.[Capri].[CreditRisk_ProbabilityV655] e
	            on a.uniqueid = e.uniqueid;
			);
		disconnect from ODBC;
	quit;

	proc sql;
		create table rawdata as  	
		Select *
		from applicationtable1 a
		left join applicationtable2 b
		on a.uniqueid = b.uniqueid
		left join applicationtable3 c
		on a.uniqueid = c.uniqueid
		left join applicationtable4 d
		on a.uniqueid = d.uniqueid;
	quit;
	/**/

	proc sort data = rawdata; by tranappnumber descending uniqueid; run;
	proc sort data = rawdata nodupkey dupout=dups  out= rawdata; by tranappnumber; run;

	data rawdatax_1;
		set rawdata;
		newvar = input(applicationdate,yymmdd10.);
		format newvar yymmddn8.;
		WeekDay = weekday(newvar);
		Week = week(newvar, 'v');
	run;

	proc sql; 
		create table rawdatax_2 as 
		select * , max(applicationdate) as MaxDate, Min(Applicationdate) as MinDate
		from rawdatax_1
		group by week;
	quit;

	data rawdata;
		set rawdatax_2;
		maxScorecardversion = scorecardversion;
		period = cat('W ', cats(substr(MinDate,6,2),substr(MinDate,9,2)), ' - ', cats(substr(MaxDate,6,2),substr(MaxDate,9,2)));
	run;

	data rawdata missing_data;
		set rawdata;
		if Min(/*CAPRI_APPLICANT,*/CAPRI_EMPLOYMENT,CAPRI_AFFORDABILITY,CAPRI_BANKING,CAPRI_BUR_PROFILE_PINPOINT,CAPRI_BUR_PROFILE_TRANSUNION_AGG,
				CAPRI_BUR_PROFILE_TRANSUNION_BCC, capri_scoring_results/*, capri_loan_application*/) = 1
		then 
			output rawdata;
		else
			output missing_data; 
	run; 

	data intables;
		set rawdata missing_data;
	run;

	/*Initially this step was to rename Compuscan variables because they too long for SAS*/
	/*However now we creating duplicate columns because V622 and V636 use newvarlist and V645 oldvarlist*/
	/*Note the rename step below has been commented out*/
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
		set RawData;
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

	%change(var=PP003  );
	%change(var=NP003  );
	%change(var=EQ0012AL   );
	%change(var=EQ2012AL  );
	%change(var=EQ0015PL  );
	%change(var=EQ2015PL  );
	%change(var=DM0001AL  );
	%change(var=PP0406AL  );
	%change(var=PP0421CL  );
	%change(var=PP0935AL   );
	%change(var=PP0714AL  );
	%change(var=PP0327AL  );
	%change(var=NP013  );
	%change(var=PP0801AL  );
	%change(var=PP0325AL  );
	%change(var=NP015  );
	%change(var=PP0901AL  );
	%change(var=PP0051CL  );
	%change(var=PP0171CL   );
	%change(var=NG004  );
	%change(var=PP0601AL  );
	%change(var=PP0604AL  );
	%change(var=RE006  );
	%change(var=PP0407AL  );
	%change(var=PP149  );
	%change(var=PP0521LB  );
	%change(var=PP116  );
	%change(var=PP173  );
	%change(var=PP0603AL  );
	%change(var=PP0503AL  );
	%change(var=PP0505AL  );
	%change(var=PP0111LB  );
	%change(var=PP0313LN  );
	%change(var=PP0515AL  );

	
	proc sql ;
		create table rawdata3 as 
		select *,
			case when PP283_ADJ = '0' then '+000000000.0'
			     when PP283_ADJ = '1' then '+000000001.0'
			     when PP283_ADJ = '2' then '+000000002.0'
			     when PP283_ADJ = '3' then '+000000003.0'
			     when PP283_ADJ = '4' then '+000000004.0'
			     when PP283_ADJ = '5' then '+000000005.0'
			     when PP283_ADJ = '6' then '+000000006.0'
			     when PP283_ADJ = '7' then '+000000007.0'
			     when PP283_ADJ = '8' then '+000000008.0'
			     when PP283_ADJ = '9' then '+000000009.0'
			     else PP283_ADJ end as PP283 format $12.         
		from rawdata2 (rename=(PP283 = PP283_ADJ));
	quit;


	proc sql ;
		create table rawdata3 as 
		select *,
			case when RE019_ADJ = '0' then '+000000000.0'
			     when RE019_ADJ = '1' then '+000000001.0'
			     when RE019_ADJ = '2' then '+000000002.0'
			     when RE019_ADJ = '3' then '+000000003.0'
			     when RE019_ADJ = '4' then '+000000004.0'
			     when RE019_ADJ = '5' then '+000000005.0'
			     when RE019_ADJ = '6' then '+000000006.0'
			     when RE019_ADJ = '7' then '+000000007.0'
			     when RE019_ADJ = '8' then '+000000008.0'
			     when RE019_ADJ = '9' then '+000000009.0'
			     else RE019_ADJ end as RE019 format $12.         
		from rawdata3 (rename=(RE019 = RE019_ADJ));
	quit;

	data rawdata4;
	      set rawdata3;
	      format AppDate yymmddn8.;
	      AppDate = input(ApplicationDate,yymmdd10.);
	      Week = put(week(AppDate, 'w'),z2.);
	      
	      if BehaveScore in ( . , 0, 9999) then Repeat = 0 ;
	      else if BehaveScore not in (., 0, 9999) then Repeat = 1;

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

	data rawdata4;
		set  rawdata4;
		
/*		if INSTITUTIONCODE not in ('BNKABS','BNKFNB','BNKSTD','BNKNED','BNKABL','BNKINV', 'BNKCAP') then INSTITUTIONCODE = 'BNKOTH';*/

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

		if Repeat = 0 and COMPUSCANVAR2123<= 0 then Comp_seg = 1;
		else if Repeat = 0 and COMPUSCANVAR2123= . then Comp_seg = 1;
		else if Repeat = 0 and COMPUSCANVAR2123> 0 then Comp_seg = 2;
		else if Repeat = 1 and COMPUSCANVAR2123<= 0 then Comp_seg = 3;
		else if Repeat = 1 and COMPUSCANVAR2123= . then Comp_seg = 3;
		else if Repeat = 1 and COMPUSCANVAR2123= 1 then Comp_seg = 4;
		else if Repeat = 1 and COMPUSCANVAR2123= 2 then Comp_seg = 4;
		else if Repeat = 1 and COMPUSCANVAR2123> 2 then Comp_seg = 5;

		MonthDiff = intck('Months',input(ApplicationDate,yymmdd10.), '01APR2017'd) ;
		GrossIncomeAdjusted = GrossIncome*(1.004752962)**(MonthDiff);

		MonthsAtCurrentEmployer = intck('Months', input(CURRENTEMPLOYMENTSTARTDATE,yymmdd10.),input(ApplicationDate,yymmdd10.));     

		if COMPUSCANVAR7430 in (-7,-6,-9) then NumPaymentsRank = - 1 ;
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

	run;

	data &outDataset.;
		set rawdata4;
	
		if PP116 <= 6 then PP173Adj = -13 ;
  		else PP173Adj = PP173 ;

		if PP0001AL <= 0 then TU_THIN = 1 ;
		else TU_THIN = 0 ;

		NP003_T = NP003;
		PP003_T = PP003;
		IF NP003 < 0 then NP003_T = 0;
		IF PP003 < 0 then PP003_T = 0;
		PP003_NP003 = NP003_T + PP003_T;

		if PP003_NP003 = 0 then  TU_Seg = 1;
		else if PP003_NP003 <= 2 then TU_Seg = 2;
		else if PP003_NP003 <= 8 then TU_Seg = 3;
		else if PP003_NP003 <= 12 then TU_Seg =4;
		else if PP003_NP003 > 12 then  TU_Seg = 5;


		IF PP0714AL >= 0 AND GROSSINCOME > 0 THEN PP0714AL_GI_RATIO = PP0714AL /GROSSINCOME;
		ELSE IF PP0714AL < 0 THEN PP0714AL_GI_RATIO = PP0714AL;
		ELSE IF GROSSINCOME < 0 THEN PP0714AL_GI_RATIO =-10 ;
		ELSE if GROSSINCOME = 0 THEN PP0714AL_GI_RATIO = -20;

		IF PP0801AL >= 0 AND GROSSINCOME > 0 THEN PP0801AL_GI_RATIO = PP0801AL/GROSSINCOME;
		ELSE IF PP0801AL < 0 THEN PP0801AL_GI_RATIO = PP0801AL;
		ELSE IF GROSSINCOME < 0 THEN PP0801AL_GI_RATIO =-10 ;
		ELSE if GROSSINCOME = 0 THEN PP0801AL_GI_RATIO = -20;


		IF PP0801AL >= 0 AND GROSSINCOME > 0 THEN PP0801AL_GI_RATIO = PP0801AL/GROSSINCOME;
		ELSE IF PP0801AL < 0 THEN PP0801AL_GI_RATIO = PP0801AL;
		ELSE IF GROSSINCOME < 0 THEN PP0801AL_GI_RATIO =-10 ;
		ELSE if GROSSINCOME = 0 THEN PP0801AL_GI_RATIO = -20;

		IF PP0601AL >= 0 AND PP0604AL > 0 THEN PP0601AL_CU_RATIO_6 = PP0601AL/PP0604AL;
		ELSE IF PP0601AL < 0 THEN PP0601AL_CU_RATIO_6 = PP0601AL;
		ELSE IF PP0604AL < 0 THEN PP0601AL_CU_RATIO_6 = PP0604AL-10 ;
		ELSE IF PP0604AL = 0 THEN PP0601AL_CU_RATIO_6 = -20;

		IF PP0521LB >= 0 AND GROSSINCOME > 0 THEN PP0521LB_GI_RATIO = PP0521LB/GROSSINCOME;
		ELSE IF PP0521LB < 0 THEN PP0521LB_GI_RATIO = PP0521LB;
		ELSE IF GROSSINCOME < 0 THEN PP0521LB_GI_RATIO =-10 ;
		ELSE IF GROSSINCOME = 0 THEN PP0521LB_GI_RATIO = -20;

		if TU_Seg = 4 then do ;

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

		if TU_Seg = 5 then do ;

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
		ELSE IF PP0505AL < 0 THEN PP0503AL_3_RATIO_12 = PP0505AL-10 ;
		ELSE if PP0505AL = 0 THEN PP0503AL_3_RATIO_12 = -20;

		IF PP0601AL >= 0 AND PP0603AL > 0 THEN PP0601AL_CU_RATIO_3 = PP0601AL/PP0603AL;
		ELSE IF PP0601AL < 0 THEN PP0601AL_CU_RATIO_3 = PP0601AL;
		ELSE IF PP0603AL < 0 THEN PP0601AL_CU_RATIO_3 =PP0603AL-10 ;
		ELSE if PP0603AL = 0 THEN PP0601AL_CU_RATIO_3 = -20;

		IF PP0515AL >= 0 AND GROSSINCOME > 0 THEN PP0515AL_GI_RATIO = PP0515AL/GROSSINCOME;
		ELSE IF PP0515AL < 0 THEN PP0515AL_GI_RATIO = PP0515AL;
		ELSE IF GROSSINCOME < 0 THEN PP0515AL_GI_RATIO =-10 ;
		ELSE if GROSSINCOME = 0 THEN PP0515AL_GI_RATIO = -20; 
	run;
%mend;
/*%GetData(table=DEV_DataDistillery_General.dbo.V6DailyData, outDataset=vic.rawdata5, dsn=Dev_DDGe);*/

