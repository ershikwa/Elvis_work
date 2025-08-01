data _null_;
     call symput('firstday',cats("'",put(intnx('month',today(),-1,'B'),yymmddd10.),"'"));
     call symput('lastday',cats("'",put(intnx('month',today(),-1,'E'),yymmddd10.),"'"));
     call symput('lastmonth',put(intnx('month',today(),-1),yymmn6.));
run;
%let odbc = MPWAPS;
%put &firstday;
%put &lastday;
%put &lastmonth;

options compress=yes;
proc sql stimer;	
		connect to ODBC (dsn=MPWAPS);
			create table MonthlyData as 
			select * from connection to odbc 
			(	
				Select distinct uniqueid, caprikey, tranappnumber, transysteminstance, branchcode, channelcode, applicationdate, applicationtime, nationalid
				from PRD_Press.capri.capri_loan_application 
				where  applicationdate >= &firstday and applicationdate <= &lastday and  TRANSEQUENCE <> '005' and channelcode <> 'CCC006' and channelcode <> 'CCC023' and channelcode <> 'CCC030' 
				
		 	);
		disconnect from ODBC;
	quit;

	proc sort data = MonthlyData; by tranappnumber descending uniqueid; run;
	proc sort data = MonthlyData nodupkey dupout=dups  out= MonthlyData; by tranappnumber; run;
data MonthlyApps(keep=uniqueid caprikey tranappnumber applicationdate nationalid);
set MonthlyData;
run;
%Upload_APS(Set =MonthlyApps , Server =Work, APS_ODBC = Dev_DDGe, APS_DB = DEV_DataDistillery_General , distribute = HASH([uniqueid]));

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
				from DEV_DataDistillery_General.dbo.MonthlyApps a
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
				from DEV_DataDistillery_General.dbo.MonthlyApps a
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
				from DEV_DataDistillery_General.dbo.MonthlyApps a
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
	proc sql;
		create table RawAppData_&lastmonth as  	
		Select a.*,b.*,c.*
		from applicationtable1 a
		inner join applicationtable2 b
		on a.tranappnumber = b.tranappnumber
		inner join applicationtable3 c
		on a.tranappnumber = c.tranappnumber
		;
	quit;
	




proc sort data = RawAppData_&lastmonth ;
      by tranappnumber descending uniqueid  ;
run;

proc sort data = RawAppData_&lastmonth nodupkey dupout=dups  out= rawdata_no_dup;
      by tranappnumber  ;
run;

libname livedata '\\mpwsas64\Core_Credit_Risk_Models\RetroData\V622';

data livedata.RawAppData_&lastmonth;
	set rawdata_no_dup;
	if compress(scorecard) = 'V6' and compress(SCORECARDVERSION) = '' then SCORECARDVERSION = 'V622';
	else if compress(scorecard) = 'V5' and compress(SCORECARDVERSION) = '' then SCORECARDVERSION = 'V5';
	else if compress(scorecard) = 'V4' and compress(SCORECARDVERSION) = '' then SCORECARDVERSION = 'V4';
	maxSCORECARDVERSION = SCORECARDVERSION;
run;

filename macros2 'H:\Process_Automation\macros';
options sasautos = (sasautos  macros2);