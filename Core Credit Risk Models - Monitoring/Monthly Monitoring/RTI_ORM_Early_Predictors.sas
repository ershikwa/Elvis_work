/*filename macros4 '\\mpwsas65\process_automation\Codes\Git_Repositories\Core Credit Risk Models - Monitoring\Macros';*/
filename macros4 '\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros';
options sasautos = (macros4);

data _null_;
	call symput("actual_date", put(intnx("month", today(),-9,'end'),yymmn6.));
     call symput("month", put(intnx("month", today(),0,'end'),yymmn6.));
     call symput("startdate",cats("'",put(intnx('month',today(),-5),yymmddd10.),"'"));
     call symput("enddate",cats("'",put(intnx('month',today(),-3,'end'),yymmddd10.),"'"));
run;
%put &actual_date;
%put &month ;
%put &startdate ;
%put &enddate ;

libname tu '\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets\V580';

proc sql stimer;
connect to ODBC (dsn=MPWAPS);
create table GetTarget as
select * from connection to odbc 
(
	select 
	B.loanid as Tranappnumber,
	T.Uniqueid, T.PRISMSCORETM, T.AppDate,
	T.V655_2 as V655, T.V655_2_RiskGroup as V655_RiskGroup, 
	T.V667, T.V667_RiskGroup,
	B.Contractual_3_LE9,
	B.FirstdueMonth, D.Principaldebt, D.product,
	coalesce(C.Final_score_1, F.Final_score_1) as Final_score_1
	from PRD_DataDistillery.dbo.JS_OUTCOME_BASE_FINAL B
	left join CREDITPROFITABILITY.dbo.ELR_LOANESTIMATES_3_9_CALIB C
	on b.loanid = c.loanid
	left join CREDITPROFITABILITY.dbo.ELR_CARDESTIMATES_3_9_CALIB F
	on b.loanid = F.loanid
	left join (
			select loanid, 'Loan' as product, 
			OPD as Principaldebt
			from CreditProfitability.dbo.Loan_Pricing_Daily
			union all
			select loanid,'Card' as product, 
			OrigPrinciple as Principaldebt
			from CreditProfitability.dbo.Card_Pricing_Daily
			) D
	on B.loanid = D.loanid
	left join DEV_DataDistillery_General.dbo.TU_applicationbase T
	on B.loanid = T.baseloanID
	where B.FirstdueMonth between &startdate and &enddate
) ;
disconnect from odbc;
quit;

proc sort data = GetTarget nodupkey;
by Tranappnumber descending Uniqueid;
run;

data GetTarget_1;
	set GetTarget;
	Month = put(datepart(FirstDueMonth),yymmn6.);

	target = Contractual_3_LE9 ;
	randomnum = uniform(12) ; *12 is the seed, and the random does not change;
	if month <= "&actual_date" then HaveActuals = 1 ;
	else HaveActuals = 0 ;
	if HaveActuals = 1 then Target = CONTRACTUAL_3_LE9 ;
	else if (HaveActuals = 0 and randomnum <= Final_score_1) then Target = 1;
	else if (HaveActuals = 0 and randomnum > Final_score_1) then Target = 0;
	if Target ne .;
	rename V655 = V655_2;
run;

%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\V655_RTI_ORM_scoring.sas";
%V655_RTI_ORM_scoring(indata=GetTarget_1);

proc sql;
	create table GetTarget_2 as
	select &month as Table_RunMonth,
	B.RTI, B.ORM, B.ORM_2, B.ORM3_2, A.V655_2, A.V655_RiskGroup as V655_2_RiskGroup,
	A.V667, A.V667_RiskGroup, A.* 
	from GetTarget_1 A
	left join RTI_ORM B
	on A.tranappnumber = B.tranappnumber;
quit;

proc sort data=GetTarget_2 nodupkey;
	by tranappnumber descending uniqueid;
run;

data tu.RTI_ORM_Early_Predictors;
set GetTarget_2;
run;
