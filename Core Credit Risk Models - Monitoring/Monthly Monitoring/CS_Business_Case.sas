/* Test line for Developer branch fixing */
libname V5 '\\neptune\sasa$\V5\V5 Monitoring\Performance Monitoring\data'; /* Path of Compuscan applicationbase */
libname calib '\\neptune\sasa$\MPWSAS15\Team Work\Elvis\calibration\calibration_new'; /* Get the calibration tables - 'a' and 'c' values */
libname comp "\\neptune\sasa$\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring"; /* Save the final summary table here */
libname tu "\\neptune\sasa$\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets"; /* Path of TU applicationbase */
%include "\\neptune\sasa$\V5\V5 Monitoring\Performance Monitoring\macros\Calc_Gini.sas";

data _null_;
     call symput("month", put(intnx("month", today(),-1,'end'),yymmn6.));
	 call symput("Assump_month", put(intnx("month", today(),-3,'end'),yymmn6.));
run;
%put &month;
%put &Assump_month; 


data model_data;
	set V5.Applicationbase_&month;
	month = substr(compress(ApplicationDate, '-'), 1, 6);
	if month >= &month;
	loanid = input(tranappnumber,9.);
	seg = comp_seg;
run;
		
/******************** Get disburseddata ********************/
proc sql stimer; 
	connect to odbc (dsn=mpwaps);
	select * from connection to odbc (
	    drop table scoring.dbo.final_model_Apps
	);
	disconnect from odbc;
quit;

data final_model_Apps;
	set model_data (keep = tranappnumber);
run;
%Upload_APS(Set = final_model_apps , Server = Work, APS_ODBC = Scoring, APS_DB = Scoring , distribute = HASH([tranappnumber]));

proc sql stimer;        
		connect to ODBC (dsn=mpwaps);
        create table DisbursedBase as
        select * from connection to odbc (
            select A.tranappnumber,
                    B.Principaldebt ,
                    B.product ,
                    B.Contractual_3_LE9 ,
                    B.FirstDueMonth,
                    E.FirstDueDate,
                    C.Final_score_1, C.FirstDueMonth as PredictorMonth
            from scoring.dbo.final_model_Apps A
            inner join scoring.dbo.Disbursement_Info E
            on a.tranappnumber = e.loanid
            left join scoring.dbo.JS_Outcome_base_final B
            on a.tranappnumber = B.loanid
            left join SCORING.DBO.NM_LOANESTIMATES_3_9_CALIB_CYB C
            on b.loanid = c.loanid
        ) ;
        disconnect from odbc ;
quit;

data Disbursedbase;
	set Disbursedbase;
	Sample_Ind = 'DISBURSED';
run;

proc sql;
	create table FinalModel_Data as
		select *
		from model_data a left join DisbursedBase b
		on a.tranappnumber = b.tranappnumber;
quit;

proc sort data=FinalModel_Data nodupkey; by tranappnumber; run;


*--------------------------------------------------------------------------------;
*------------------------ Add in Rebuild probabilities --------------------------;

%macro scoreinputdata(inputdataset=,numberofsegment=,outputdataset=, path=);
	%macro createivlibrary(h);
	  %do i = 1 %to &numberofsegment;
	      libname iv&i "&path\Segment&i";
	      %global  segment_&i._list ;
	      proc sql; select reverse(substr(reverse(compress(Parameter)),3)) into : segment_&i._list separated by ' ' from iv&i..parameter_estimate where upcase(Parameter) ne 'INTERCEPT'; quit;
	  %end;
	%mend;
	%createivlibrary(1);

	/* Change for pilot , eutopia and type codde  */
	%macro applyscore(t);
	    %do i = 1 %to %sysfunc(countw(&&segment_&t._list));
	          %let var = %scan(&&segment_&t._list, &i);
	          %include "&path\Segment&t\&var._if_statement_.sas";
	          %include "&path\Segment&t\&var._WOE_if_statement_.sas"; 
	    %end;
	    *****************************************;
	    ** SAS Scoring Code for PROC Hplogistic;
	    *****************************************;
	    %include "&path\Segment&t\creditlogisticcode2.sas";
	%mend;

	%do n = 1 %to &numberofsegment;
	    data segment_&n.;
			set &inputdataset(where=(seg=&n));
			%applyscore(&n); 
			Final_score = P_target1;
			Score = 1000-(final_score*1000);    
			drop _TEMP;
	    run;
	%end;

	data final_model_data;
	    set %do n = 1 %to &numberofsegment; segment_&n %end;;
	run;

	data &outputdataset;
	  set final_model_data;  
	run;
%mend;
options mprint mlogic;
%scoreinputdata(inputdataset=model_data,numberofsegment=5,outputdataset=Rebuild_model, path=\\neptune\sasa$\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\Rebuild_Comp\Rebuild_&month);

data Rebuild_scorecard (keep=tranappnumber V5_Rebuild);
	set Rebuild_model;
	V5_Rebuild = Final_score;
run;

proc sort data=Rebuild_scorecard nodupkey; by tranappnumber; run;

proc sql;
    create table Added_Rebuilt
        as select b.V5_Rebuild, a.*
        from FinalModel_Data a left join Rebuild_scorecard b
        on a.tranappnumber = b.tranappnumber;
quit;



*--------------------------------------------------------------------------------;
*------------------------- Add in Refit probabilities ---------------------------;

%macro scoreinputdata(inputdataset=,numberofsegment=,outputdataset=, path=);
	%macro createivlibrary(h);
	  %do i = 1 %to &numberofsegment;
	      libname iv&i "&path\Segment&i";
	      %global  segment_&i._list ;
	      proc sql; select reverse(substr(reverse(compress(Parameter)),3)) into : segment_&i._list separated by ' ' from iv&i..parameter_estimate where upcase(Parameter) ne 'INTERCEPT'; quit;
	  %end;
	%mend;
	%createivlibrary(1);

	/* Change for pilot , eutopia and type codde  */
	%macro applyscore(t);
	    %do i = 1 %to %sysfunc(countw(&&segment_&t._list));
	          %let var = %scan(&&segment_&t._list, &i);
	          %include "&path\Segment&t\&var._if_statement_.sas";
	          %include "&path\Segment&t\&var._WOE_if_statement_.sas"; 
	    %end;
	    *****************************************;
	    ** SAS Scoring Code for PROC Hplogistic;
	    *****************************************;
	    %include "&path\Segment&t\creditlogisticcode2.sas";
	%mend;

	%do n = 1 %to &numberofsegment;
	    data segment_&n.;
			set &inputdataset(where=(seg=&n));
			%applyscore(&n); 
			Final_score = P_target1;
			Score = 1000-(final_score*1000);    
			drop _TEMP;
	    run;
	%end;

	data final_model_data;
	    set %do n = 1 %to &numberofsegment; segment_&n %end;;
	run;

	data &outputdataset;
	  set final_model_data;  
	run;
%mend;
options mprint mlogic;
%scoreinputdata(inputdataset=model_data,numberofsegment=5,outputdataset=Refit_model, path=\\neptune\sasa$\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\Refit_Comp\Refit_&month);

data Refit_scorecard (keep=tranappnumber V5_Refit);
	set Refit_model;
	V5_Refit = Final_score;
run;

proc sort data=Refit_scorecard nodupkey; by tranappnumber; run;

proc sql;
    create table Added_Refit
        as select a.*, b.*
        from Added_Rebuilt a left join Refit_scorecard b
        on a.tranappnumber = b.tranappnumber;
quit;





*----------------------------------------------------------------;
*-------------------- Create assumption table -------------------;
*----------------------------------------------------------------;
data Applicationbase_&Assump_month;
	set V5.Applicationbase_&Assump_month;
	month = substr(compress(ApplicationDate, '-'), 1, 6);
	if month >= &Assump_month;
	loanid = input(tranappnumber,9.);
run;

/* Pull disbursed data for the applications */
proc sql stimer; 
	connect to odbc (dsn=mpwaps);
	select * from connection to odbc (
	    drop table scoring.dbo.final_model_Apps
	);
	disconnect from odbc;
quit;

data final_model_Apps;
	set Applicationbase_&Assump_month (keep = tranappnumber);
run;
%Upload_APS(Set = final_model_apps , Server = Work, APS_ODBC = Scoring, APS_DB = Scoring , distribute = HASH([tranappnumber]));

proc sql stimer;        
		connect to ODBC (dsn=mpwaps);
        create table DisbursedBase as
        select * from connection to odbc (
            select A.tranappnumber,
                    B.Principaldebt ,
                    B.product ,
                    B.Contractual_3_LE9 ,
                    B.FirstDueMonth,
                    E.FirstDueDate,
                    C.Final_score_1, C.FirstDueMonth as PredictorMonth
            from scoring.dbo.final_model_Apps A
            inner join scoring.dbo.Disbursement_Info E
            on a.tranappnumber = e.loanid
            left join scoring.dbo.JS_Outcome_base_final B
            on a.tranappnumber = B.loanid
            left join SCORING.DBO.NM_LOANESTIMATES_3_9_CALIB_CYB C
            on b.loanid = c.loanid
        ) ;
        disconnect from odbc ;
    quit;

proc sort data=DisbursedBase nodupkey; by tranappnumber; run;

/* Add Disbursed data to the applications - Need Principaldept for assumptions */
proc sql;
	create table App_Disb_&Assump_month as
		select *
		from Applicationbase_&Assump_month a left join DisbursedBase b
		on a.tranappnumber = b.tranappnumber;
quit;

/* Create Risk groups - Need it for assumptions */
Data Create_RG;
	set App_Disb_&Assump_month;
    finalRiskScore = 1000-(comp_prob*1000);

		thinfileindicator = comp_thin;

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
        if scoreband = . then delete ;
run;

data Disbursals;
	set Create_RG;
	if Principaldebt = '.' then Disbursed = 0 ;
	else Disbursed = 1;
run;
 
/* Get the loanAmount */
data Disbursals;
	set Disbursals;
	if Disbursed = 1 then LoanAmount = Principaldebt;
	else LoanAmount = 0;
run;

/* Create assumptions table */
proc sql;
	create table Assumption as
	select sum(Disbursed)/count(*) as disbursalrate, ((sum(LoanAmount))/(sum(Disbursed))) as AvgLoansize, Scoreband
	from Disbursals
	group by Scoreband;
quit;

data _null_;
      set Assumption;
      call symput(compress('Disb'||_N_), disbursalrate);
      call symput(compress('LN'||_N_), AvgLoansize);
      call symput(compress('RiskG'||_N_), Scoreband);
      call symput(compress('Num'), _N_);
run;
%put &LN1 &LN2;

%macro runme(Input,Prob,expected,MName,Thin);

	data Applied ;
	    set &input ;
	    keep month seg ThinFileIndicator &prob &expected &thin finalRiskScore Scoreband;
	    thinfileindicator = &thin ;
	    finalRiskScore = 1000-(&Prob*1000);

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

		if scoreband = . then delete ;
	run;

	/*run some checks*/
	/*-- there should be no missing risk groups/scorebands, range between 50 and 71 */
	/*-- probabilities between 0 and 1 and no missings*/
	/*-- final risk score between 0 and 1000 and no missings ; */

	proc freq data = Applied ;
	    tables scoreband / missing ;
	run;

	proc freq data = Applied ;
	    tables seg / missing ;
	run;

	proc means data= Applied missing StackODSOutput  P5 Mean  P95 Std Min Max nmiss ;
	    var &Prob finalriskscore;
	    ods output summary=UnivariateSummary;
	run;

	data Applied;
	    set Applied;

	    %do i = 1 %to &num;
			if scoreband = &&RiskG&i then do;
			DisbRate = &&Disb&i;
			AvgCap = &&LN&i;
			end;
	    %end;

	    randomnum  = uniform(7);
	    if randomnum  <= DisbRate then Disbursed = 1;
	    else Disbursed = 0;
	    if disbursed = 1 then LoanSize = AvgCap;
	    else LoanSize = 0;

	    RRisk = &expected * LoanSize ;
	run;

	proc sort data = Applied;
		by Scoreband;
	run;
      
	proc sql ;
		create table DisbNum as 
		  select Scoreband , disbrate ,  count(*) as volume 
  		  from Applied 
		  group by Scoreband , disbrate;
	quit; 

	data DisbNum;
		set DisbNum;
		DisbNum = round(Volume * disbrate);
	run;

	proc sort data = DisbNum nodupkey;
		by Scoreband  disbrate;
	run;

	proc sql ;
		create table Applied as 
		  select A.* , B.disbnum
		  from Applied  A
		  left join DisbNum B 
		  on A.Scoreband = B.Scoreband;
	quit;

	data Applied;
		set Applied;
		randnum = uniform(12);
	run;

	proc sort data = Applied;
		by Scoreband randnum;
	run;

	data Applied;
	   set Applied;
	   retain N;
	   by Scoreband;
	   if first.Scoreband then N = 1;
	   else N+1;
	run;

   	data Applied;
	   set Applied;
	   if  N <= DisbNum then Disbursed = 1;
	   else Disbursed = 0;
	   if disbursed = 1 then LoanSize = AvgCap;
	   else LoanSize = 0;
	   RRisk = &expected * LoanSize;
   	run;

	proc summary data = Applied nway missing;
	    class  Month;
	    var disbursed LoanSize RRisk;
	    output out = Summary1 (drop = _type_) sum() =;
	run;

	data Summary1 (drop = _freq_ RRisk);
		retain Model Volume disbursed Loansize AvgLoansize disbursalrate ExpectedRisk;
		set Summary1;
		Model = "&MName";
		Volume = _Freq_ ;
		format model $30. AvgLoansize Loansize comma15. disbursalrate ExpectedRisk percent8.6  volume disbursed comma9.     ;
		AvgLoansize = Loansize / Disbursed;
		disbursalrate = Disbursed/Volume; 
		ExpectedRisk = RRisk/Loansize;
		SalesBenefit = Loansize*0.22;
	run;

	proc print data = Summary1 noobs; run;

	proc summary data = Applied nway missing;
	    class  month Scoreband;
	    var disbursed LoanSize RRisk;
	    output out = Summary2 (drop = _type_) sum() =;
	run;

	data Summary2 (drop = _freq_ RRisk) ;
	    retain Model Month Scoreband Volume disbursed Loansize AvgLoansize disbursalrate ExpectedRisk ;
	    set Summary2 ;
	    Model = "&MName";
	    Volume = _Freq_ ;
	    format model $30. AvgLoansize Loansize comma15. disbursalrate ExpectedRisk percent8.6  volume comma9.     ;
	    AvgLoansize = Loansize / Disbursed;
	    disbursalrate = Disbursed/Volume; 
	    ExpectedRisk = RRisk/Loansize;
	run;

	proc print data = Summary2 noobs; run;

%mend;

/* Remove missing Rebuild probabilities */
data rebuild;
	set Added_Refit;
	where V5_Rebuild ne .;
run;

/* Remove missing Refit probabilities */
data refit;
	set Added_Refit;
	where V5_Refit ne .;
run;

/* Remove missing V622 probabilities */
data Comp;
	set Added_Refit;
	where comp_prob ne .;
run;

/* Remove missing V535 probabilities */
data V535;
	set Added_Refit;
	where v535_probability ne .;
run;

/* Remove missing TU probabilities */
data TU;
	set Added_Refit;
	where tu_prob ne .;
run;

/* Delete tables if they are already created because it appends */
/*
proc delete data=all_summary1; run;
proc delete data=all_summary2; run;
*/

%runme(Input=Rebuild, prob=V5_Rebuild, expected=comp_prob, MName=Rebuild, Thin=comp_thin);
proc append base = all_summary1 data = summary1 force; run;
proc append base = all_summary2 data = summary2 force; run;

%runme(Input=Refit, Prob=V5_Refit, expected=comp_prob, MName=Refit, Thin=comp_thin);
proc append base = all_summary1 data = summary1 force; run;
proc append base = all_summary2 data = summary2 force; run;

%runme(Input=Comp, Prob=comp_prob, expected=comp_prob, MName=Comp, Thin=comp_thin);
proc append base = all_summary1 data = summary1 force; run;
proc append base = all_summary2 data = summary2 force; run;

%runme(Input=V535, Prob=v535_probability, expected=comp_prob, MName=V535, Thin=comp_thin);
proc append base = all_summary1 data = summary1; run;
proc append base = all_summary2 data = summary2; run;

%runme(Input=TU, Prob=tu_prob, expected=comp_prob, MName=TU, Thin=comp_thin);
proc append base = all_summary1 data = summary1; run;
proc append base = all_summary2 data = summary2; run;

data Disbursedbase_&month;
set Comp.Disbursedbase_&month;
run;

%scoreinputdata(inputdataset=Disbursedbase_&month,numberofsegment=5,outputdataset=Rebuild_model, path=\\neptune\sasa$\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\Rebuild_Comp\Rebuild_&month);
data Rebuild_model (keep=tranappnumber V5_Rebuild);
	set Rebuild_model;
	V5_Rebuild = Final_score;
run;

proc sql;
    create table Disbursedbase_&month
        as select b.V5_Rebuild, a.*
        from Disbursedbase_&month a left join Rebuild_model b
        on a.tranappnumber = b.tranappnumber;
quit;

%scoreinputdata(inputdataset=Disbursedbase_&month,numberofsegment=5,outputdataset=Refit_model, path=\\neptune\sasa$\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\Refit_Comp\Refit_&month);
data Refit_model (keep=tranappnumber V5_Refit);
	set Refit_model;
	V5_Refit = Final_score;
run;

proc sql;
    create table Disbursedbase_&month
        as select b.V5_Refit, a.*
        from Disbursedbase_&month a left join Refit_model b
        on a.tranappnumber = b.tranappnumber;
quit;

%macro Ginis(Prob=, Input=, MName=, target=);

	%Calc_Gini(&prob, &input, &target, ginitable);
	data ginitable1 (keep=gini Model);
		set ginitable;
		Model = "&MName";
	run;

	data Disb_only;
		set &Input;
		where Sample_Ind = "DISBURSED";
	run;

	%Calc_Gini(&prob, Disb_only, target, ginitable);
	data ginitable2 (keep=gini Model);
		set ginitable;
		Model = "&MName";
	run;

	proc sql;
	  create table comb_gini as
	  select a.Model, a.gini as Gini_Rej_Inc, b.gini as Gini_Disb
	  from ginitable1 a left join ginitable2 b
	  on a.Model = b.Model;
	quit;

	proc append base = GiniSummary data = comb_gini force; run;

%mend;

%Ginis(Prob=V5_Rebuild, Input=Disbursedbase_&month, MName=Rebuild, target=target);
%Ginis(Prob=V5_Refit, Input=Disbursedbase_&month, MName=Refit, target=target);
%Ginis(Prob=Comp_prob, Input=Disbursedbase_&month, MName=Comp, target=target);
%Ginis(Prob=V535_probability, Input=Disbursedbase_&month, MName=V535, target=target);
%Ginis(Prob=TU_prob, Input=Disbursedbase_&month, MName=TU, target=target);

/*
proc delete data=GiniSummary; run;
*/

proc sql;
	create table all_summary1 as
	  select a.*, b.Gini_Disb, b.Gini_Rej_Inc
	  from all_summary1 a left join GiniSummary b 
	  on a.Model = b.Model;
quit;

proc transpose data=all_summary1 out=transposed; 
	id Model; 
	var Loansize ExpectedRisk Gini_Disb Gini_Rej_Inc SalesBenefit; 
run;

data relative_change;
	set transposed;
	rc_Rebuild = (Comp-Rebuild)/Rebuild;
	rc_Refit = (Comp-Refit)/Refit;
	rc_V535 = (Comp-V535)/V535;
	rc_TU = (Comp-TU)/TU;
run;

proc transpose data=relative_change out=relative_change2; 
	id _NAME_ ; 
	var Rebuild Refit V535 TU Comp rc_Rebuild rc_Refit rc_V535 rc_TU; 
run;

data relative_change2 (drop=_NAME_);
	set relative_change2;
	Model = _NAME_;
run;

%macro format(in=, Mod=);

	data test1 (keep=Model Attribute Value);
		set &in;
		if Model = "&Mod" then do;
		   Attribute = "Gini_Rej_Inc";
		   Value = Gini_Rej_Inc;
		   output;
	  	end;
	run;

	data test2 (keep=Model Attribute Value);
		set &in;
		if Model = "&Mod" then do;
		   Attribute = "Gini_Disb";
		   Value = Gini_Disb;
		   output;
	  	end;
	run;

	data test3 (keep=Model Attribute Value);
		set &in;
		if Model = "&Mod" then do;
		   Attribute = "Loansize";
		   Value = Loansize;
		   output;
	  	end;
	run;

	data test4 (keep=Model Attribute Value);
		set &in;
		if Model = "&Mod" then do;
		   Attribute = "ExpectedRisk";
		   Value = ExpectedRisk;
		   output;
	  	end;
	run;

	data test5 (keep=Model Attribute Value);
		set &in;
		if Model = "&Mod" then do;
		   Attribute = "SalesBenefit";
		   Value = SalesBenefit;
		   output;
	  	end;
	run;

	data test;
		set test1 test2 test3 test4 test5;
	run;

	proc append base = bc_change data = test FORCE; run;
%mend;

%format(in=relative_change2, mod=Rebuild);
%format(in=relative_change2, mod=Refit);
%format(in=relative_change2, mod=V535);
%format(in=relative_change2, mod=TU);
%format(in=relative_change2, mod=Comp);

%macro format(in=, Mod=);

	data test1 (keep=Model Attribute Value);
		set &in;
		if Model = "&Mod" then do;
		   Attribute = "Gini_Rej_Inc";
		   Value = Gini_Rej_Inc;
		   output;
	  	end;
	run;

	data test2 (keep=Model Attribute Value);
		set &in;
		if Model = "&Mod" then do;
		   Attribute = "Gini_Disb";
		   Value = Gini_Disb;
		   output;
	  	end;
	run;

	data test3 (keep=Model Attribute Value);
		set &in;
		if Model = "&Mod" then do;
		   Attribute = "Loansize";
		   Value = Loansize;
		   output;
	  	end;
	run;

	data test4 (keep=Model Attribute Value);
		set &in;
		if Model = "&Mod" then do;
		   Attribute = "ExpectedRisk";
		   Value = ExpectedRisk;
		   output;
	  	end;
	run;

	data test5 (keep=Model Attribute Value);
		set &in;
		if Model = "&Mod" then do;
		   Attribute = "SalesBenefit";
		   Value = SalesBenefit;
		   output;
	  	end;
	run;

	data test;
		set test1 test2 test3 test4 test5;
	run;

	proc append base = bc_change2 data = test FORCE; run;
%mend;

%format(in=relative_change2, mod=rc_Rebuild);
%format(in=relative_change2, mod=rc_Refit);
%format(in=relative_change2, mod=rc_V535);
%format(in=relative_change2, mod=rc_TU);

data bc_Tables;
	set bc_change bc_change2;
run;

/* Save summary1 in the comp library */
data comp.business_case_&month.;
	set all_summary1;
run;

data comp.bc_Tables_&month.;
	set bc_Tables;
run;

/* Upload summary1 to cred_scoring for power BI */
libname cred_scr odbc dsn=cred_scoring schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.v5_Business_Case_xml; run;
proc sql;
	create table  cred_scr.v5_Business_Case_xml(BULKLOAD=YES) as
	    select  distinct * 
		from Comp.business_case_&month.;
quit;

libname cred_scr odbc dsn=cred_scoring schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.bc_Tables; run;
proc sql;
	create table  cred_scr.bc_Tables (BULKLOAD=YES) as
	    select  distinct * 
		from Comp.bc_Tables_&month.;
quit;

filename macros2 'H:\Process_Automation\macros';
options sasautos = (sasautos  macros2);

