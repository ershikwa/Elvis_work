options compress = yes;
options compress = on;
options mprint mlogic symbolgen;
options nomprint nomlogic nosymbolgen; 

%let Rundate = %sysfunc(today(), yymmddn8.);
%put &Rundate;
%let thismonth = %sysfunc(today(), date9.);
%put &thismonth;

libname data "\\MPWSAS64\Core_Credit_Risk_Model_Team\Behavescore_V2 Monitoring\Data";
libname ES "\\mpwsas5\G\Automation\Behavescore\Datasets";
libname TF "\\mpwsas64\Core_Credit_Risk_Models\BehaveScoreV2 Data";
%let path = \\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\Behavioral Model\Bucketing Code;

/*SOURCE THE INPUT DATA */

libname Prd_DDDa odbc dsn=PRD_DDDa schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
libname pre "\\neptune\sasa$\V5\Behavioral Model\Pre Development data";

data _null_;
	call symput('last12month', put(intnx('month' ,today(),-11,'begin'),yymmn6.));
	call symput('Rundate', put(intnx('day',today(),0),yymmddn8.));
	call symput('runmonth',put(intnx('month',today(),0),yymmn6.));
run;

%put &last12month;
%put &Rundate;
%put &runmonth;


/* Getting Behavescore V2 data */ 
data data.behavev2_&Rundate ;
	set TF.behavev2_&Rundate;
run;


/* Create the date partition file using the previous run dates */
%macro createdatelist();
	%if %sysfunc(exist(data.datepartition)) %then %do;
		data datepart;
			Format AppDate date9. ;
			AppDate = input(put(&Rundate.,8.),yymmdd8.);
			output;
		run;
		proc append base = data.datepartition data = datepart force; run;
	%end;
%mend;
%createdatelist;


************RUN BEHAVE MODEL FOR THE LATEST MONTH ***************** ;

%macro MOM(Prev,Curr) ;
	data PrevMonth ;
		set &Prev ;
		keep idno BehaveDecileV2  ;
	run;
	data CurrMonth ;
		set &Curr ;
		keep idno BehaveDecileV2 ;
	run;
	proc sort data = PrevMonth nodupkey ;
		by idno ;
	run;
	proc sort data = CurrMonth nodupkey ;
		by idno ;
	run;
	data Venn ;
		merge PrevMonth (in = a rename = ( BehaveDecileV2 = PrevBehaveDecileV2) )  CurrMonth (in = b) ;
		by idno ;
		if a or b ;
		if a = 1 then PrevMonth = 'Y';
		else PrevMonth = 'N';
		if b = 1 then CurrMonth = 'Y';
		else CurrMonth = 'N';
	run;

	data Venn2 ;
		set Venn ;
		Shift = BehaveDecileV2 - PrevBehaveDecileV2 ;
		if PrevMonth = 'Y' and CurrMonth = 'Y' ;
	run;

	proc summary data = Venn2 nway missing ;
		class shift ;
		output out = summary5;
	run;
%mend;

%macro psi_calculation(build=, base=,period=month,var=,psi_var=, outputdataset=);
/*   %if %VarExist(&build, &psi_var)=1 and %VarExist(&base, &psi_var)=1 and %VarExist(&base, &period)=1 %then %do;*/
     proc freq data = &base /*(where=(Month = "202106"))*/;
           tables &period*&psi_var / missing outpct out=basetable(keep =&period &psi_var pct_row rename =(pct_row = percent));
     run;
     proc freq data = &build;
           tables &psi_var /missing out=buildtable(keep=&psi_var percent);
     run;
     data buildtable;
           set buildtable;
           Bin=_n_;
     run;


     proc sql;
           create table basetable2 as
                select distinct  *
                from basetable a full join  buildtable(keep = &psi_var Bin)  b
                on a.&psi_var = b.&psi_var
                ;
     quit;
     proc sort data = basetable2;
           by &period &psi_var;
     run;
     proc transpose data = basetable2 out = psitrans prefix = bin ;  
           by &period;
           id Bin;
           var percent;
     run;
     proc transpose data = buildtable out = buildtrans prefix = build;
           var percent;
     run;
     proc sql; select count(distinct &psi_var) into : numBuckets separated by "" from buildtable; quit;
     proc sql;
           create table all_psi as
              select *
              from psitrans , buildtrans;
     quit;
     data all_psi_results(keep = Variablename &period psi marginal_stable unstable Bin);
           set all_Psi;
           length variablename $32.;
           array pred [&numBuckets] bin1 - bin&numBuckets;
           array build [&numBuckets] build1 - build&numBuckets;
           item = 0;
           do p = 1 to &numBuckets;
             item = sum(item,(pred[p]-build[p])*(log(pred[p]/build[p])));
           end;
           psi = item/100;
           marginal_stable = 0.1;
           unstable=0.25;
           variablename = tranwrd(upcase("&var."),"_B","");
     run;
     data buildset ;
           set buildtable(rename=(&psi_var =scores));
           length variablename $32.;
           &period =" BUILD";
           psi=.;
           marginal_stable=.;
           unstable=.;
           variablename=tranwrd(upcase("&var."),"_B","");
     run;
     proc sql;
           create table &outputdataset(rename=(&psi_var=scores)) as
                select *
                from basetable(keep = &period &psi_var percent) a inner join all_psi_results b
                on a.&period = b.&period;
     quit;
     proc append base = APPSTAGEPSI data = &outputdataset force; run;
	 proc append base = &outputdataset data = buildset force; run;
     proc datasets lib = work;
        delete all_psi_results summarytable basetable2 all_psi psitrans buildtrans ;
     run;quit;
%mend;


%macro scoring(inDataset=, outDataset=,Path=);
	%macro ApplyScoring(Varlist1,Path1) ;
		%do i = 1 %to %sysfunc(countw(&Varlist1));
			%let var = %scan(&Varlist1, &i);
			%include "&Path1.\&var._if_statement_.sas";
			%include "&Path1.\&var._WOE_if_statement_.sas"; 
	    %end;
		%include "&Path1.\creditlogisticcode2.sas";
	%mend;

	%let path1 = &path.;
	libname d "&path.";
	proc sql; 
		select reverse(substr(reverse(compress(Parameter)),3)) into : Varlist1 separated by ' ' 
		from d._estimates_
		where Parameter ne "Intercept"; 
	quit;

	data &outDataset ;
		set &inDataset./*(keep = idno no_zero_arrear_24_2 paid2owed12_V3 MaxArrearsCode DAYSSINCELASTABDISB DAYSSINCEFIRSTABDISB Maxcd BehaveScore BehaveDecile)*/ ;
		%ApplyScoring(varlist1=&Varlist1,Path1=&path1);
		final_Score = P_Target1;
		BehaveScoreV2 = (1000-ceil(final_score*1000));

		if BehaveScoreV2 <= 	728	then BehaveDecileV2 = 	1	;
		else if BehaveScoreV2 <= 	770	then BehaveDecileV2 = 	2	;
		else if BehaveScoreV2 <= 	799	then BehaveDecileV2 = 	3	;
		else if BehaveScoreV2 <= 	818	then BehaveDecileV2 = 	4	;
		else if BehaveScoreV2 <= 	837	then BehaveDecileV2 = 	5	;
		else if BehaveScoreV2 <= 	858	then BehaveDecileV2 = 	6	;
		else if BehaveScoreV2 <= 	878	then BehaveDecileV2 = 	7	;
		else if BehaveScoreV2 <= 	890	then BehaveDecileV2 = 	8	;
		else if BehaveScoreV2 <= 	917	then BehaveDecileV2 = 	9	;
		else if BehaveScoreV2 <= 	1000	then BehaveDecileV2 = 10 ;

		/*RunDate2 = input(&Rundate,best12.);	*/
	run;
%mend;

%scoring(inDataset=data.Behavev2_&Rundate, outDataset=Behavev2_&Rundate, Path=&path);

%macro VolumeComparison(Prev,Curr) ;

	data PrevMonth ;
		set &Prev ;
		keep idno ;
	run;

	data CurrMonth ;
		set &Curr ;
		keep idno ;
	run;
	proc sort data = PrevMonth nodupkey ;
		by idno ;
	run;

	proc sort data = CurrMonth nodupkey ;
		by idno ;
	run;

	data Venn ;
		merge PrevMonth (in = a )  CurrMonth (in = b) ;
		by idno ;
		if a or b ;
		if a = 1 then PrevMonth = 'Y';
		else PrevMonth = 'N';
		if b = 1 then CurrMonth = 'Y';
		else CurrMonth = 'N';
	run;
%mend;



proc sort data = data.datepartition;
	by descending appdate;
run;

data _null_;
set data.datepartition;
if _n_ = 1 then 
	call symput("todaydate", put(appdate,yymmddn8.));
run;
%put &todaydate;

******Generate report to check happy with monitoring ******************* ;

%macro report(date) ;

data BehaveV2_&date ;
		set BehaveV2_&date ;
		Format seg3Bucket $100. ;
		If (BehavescoreV2 >= 0 and BehavescoreV2 <= 739) or BehavescoreV2 in (.)  then seg3Bucket = "B1: BehavescoreV2  <= 739 or BehavescoreV2 in (.)";
		Else If (BehavescoreV2 > 739 and BehavescoreV2 <= 833)  then seg3Bucket = "B2: BehavescoreV2  <= 833";
		Else If (BehavescoreV2 > 833)  then seg3Bucket = "B3: BehavescoreV2  > 833";
		Else seg3Bucket = "B4: BehavescoreV2  in (UNKNOWN)";


		Format seg4Bucket $100. ;
		If (BehaveScoreV2 >= 0 and BehaveScoreV2 <= 738)  then seg4Bucket = "B1: BehaveScoreV2  <= 738";
		Else If ( BehaveScoreV2 > 738 and BehaveScoreV2 <= 796) or BehaveScoreV2 in (.)  then seg4Bucket = "B2: BehaveScoreV2  <= 796 or BehaveScoreV2 in (.)";
		Else If (BehaveScoreV2 > 796 and BehaveScoreV2 <= 832)  then seg4Bucket = "B3: BehaveScoreV2  <= 832";
		Else If (BehaveScoreV2 > 832 and BehaveScoreV2 <= 882)  then seg4Bucket = "B4: BehaveScoreV2  <= 882";
		Else If (BehaveScoreV2 > 882 and BehaveScoreV2 <= 915)  then seg4Bucket = "B5: BehaveScoreV2  <= 915";
		Else If (BehaveScoreV2 > 915)  then seg4Bucket = "B6: BehaveScoreV2  > 915";
		Else seg4Bucket = "B7: BehaveScoreV2  in (UNKNOWN)";


		Format seg5Bucket $100. ;
		If ( BehavescoreV2 >= 0 and BehavescoreV2 <= 665) or BehavescoreV2 in (.)  then seg5Bucket = "B1: BehavescoreV2  <= 665 or BehavescoreV2 in (.)";
		Else If (BehavescoreV2 > 665 and BehavescoreV2 <= 771)  then seg5Bucket = "B2: BehavescoreV2  <= 771";
		Else If (BehavescoreV2 > 771 and BehavescoreV2 <= 833)  then seg5Bucket = "B3: BehavescoreV2  <= 833";
		Else If (BehavescoreV2 > 833 and BehavescoreV2 <= 887)  then seg5Bucket = "B4: BehavescoreV2  <= 887";
		Else If (BehavescoreV2 > 887 and BehavescoreV2 <= 915)  then seg5Bucket = "B5: BehavescoreV2  <= 915";
		Else If (BehavescoreV2 > 915)  then seg5Bucket = "B6: BehavescoreV2  > 915";
		Else seg5Bucket = "B7: BehavescoreV2  in (UNKNOWN)";


	run;

	proc summary data = BehaveV2_&Date nway missing ;
		class rundate DAYSSINCEFIRSTABDISB_B DAYSSINCELASTABDISB_B   
		no_zero_arrear_24_2_B Maxcd_B
		paid2owed12_V3_B seg3Bucket seg4Bucket seg5Bucket 
		BehaveDecileV2 
		;
		output out = _run_&date ;
	run;
%mend;

/*%report(date=20210913);*/
%report(date=&todaydate);


data data.report ;
	set data.report work._run_:;
	Month = substr(put(RunDate,8.),1,6) ;
run;

proc sort data = data.report nodupkey ;
	by _all_ ;
run;
proc sort data = data.report ;
	by rundate ;
run;

proc sort data = data.Datepartition; by descending AppDate; run;


data _null_;
	set data.Datepartition;
	if _n_ = 2 then call symput("prevrundate", put(Appdate,yymmddn8.));
run;
%put &prevrundate;


%VolumeComparison(Prev=data.behavev2_&prevrundate,Curr=data.behavev2_&todaydate) ;
%MOM(Prev=data.behavev2_&prevrundate,Curr=data.behavev2_&todaydate) ;

data Report ;
	set data.Report ;
	Month = substr(RunDate,1,6) ;
	Decile = put(BehaveDecileV2,z2.);
	if input(month,6.) >= &last12month;;
run;


%psi_calculation(build=data.Appbuildmonitoring, base=Report,period=Month,var=DECILE,psi_var=DECILE, outputdataset=test);
%psi_calculation(build=data.Appbuildmonitoring, base=Report,period=Month,var=SEG3BUCKET,psi_var=SEG3BUCKET, outputdataset=test);
%psi_calculation(build=data.Appbuildmonitoring, base=Report,period=Month,var=SEG4BUCKET,psi_var=SEG4BUCKET, outputdataset=test);
%psi_calculation(build=data.Appbuildmonitoring, base=Report,period=Month,var=SEG5BUCKET,psi_var=SEG5BUCKET, outputdataset=test);
%psi_calculation(build=data.Appbuildmonitoring, base=Report,period=Month,var=DAYSSINCEFIRSTABDISB_B,psi_var=DAYSSINCEFIRSTABDISB_B, outputdataset=test);
%psi_calculation(build=data.Appbuildmonitoring, base=Report,period=Month,var=DAYSSINCELASTABDISB_B,psi_var=DAYSSINCELASTABDISB_B, outputdataset=test);
%psi_calculation(build=data.Appbuildmonitoring, base=Report,period=Month,var=NO_ZERO_ARREAR_24_2_B,psi_var=NO_ZERO_ARREAR_24_2_B, outputdataset=test);
%psi_calculation(build=data.Appbuildmonitoring, base=Report,period=Month,var=PAID2OWED12_V3_B,psi_var=PAID2OWED12_V3_B, outputdataset=test);
%psi_calculation(build=data.Appbuildmonitoring, base=Report,period=Month,var=MAXCD_B,psi_var=MAXCD_B, outputdataset=test);

data AppStagePSI ;
	set AppStagePSI ;
	rename scores = Bin;
run;

options center;
options nodate nonumber;
options orientation=landscape;
ods pdf body = "\\MPWSAS64\Core_Credit_Risk_Model_Team\Behavescore_V2 Monitoring\Reports\BehavescoreV2 Monitoring Report &todaydate..pdf" ;

	ods startpage = no;
	Title "Number of records processed"; 
	proc sgplot data = report  ;
		format  _freq_  comma9. ;
		vbar rundate / response = _freq_  ;
		yaxis  label = "Volume";
	run; 
	Title;

	ods startpage = no;
	proc freq data = Venn ;
		table CurrMonth *PrevMonth  / missing  nocum nocol norow nopercent FORMAT=COMMA9. ;
	run;

	Title "Change in Decile from Current Month to Previous";
	proc sgplot data = summary5 ;
		vbar shift / response = _FREQ_  stat = percent ;
		xaxis type = discrete ;
		yaxis label = "Percent";
	run;
	footnote1 "Current decile minus previous month's decile";
	Title;
	footnote1;

	
ods startpage = now;
/*ods layout absolute;*/
/*ods region y=0in x=0in width=5in height=4in;*/
Title "Decile Distribution";
proc sgplot data=AppStagePSI ;
where VariableName in ('DECILE');
       vbar month / response = percent stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	   vline month / response = psi y2axis stat = mean group = Bin ;
	   vline month / response = Marginal_Stable y2axis stat = mean group = Bin ;
	   vline month / response = Unstable y2axis stat = mean group = Bin ;
yaxis label = 'Percentage'; 
xaxis label = 'ApplicationMonth';
y2axis label = 'PSI' min = 0 max = 1 ;
run;
Title;

ods startpage = now;
Title "Bucketed as per Segment3";
proc sgplot data=AppStagePSI ;
where VariableName in ('SEG3BUCKET')  ;
       vbar month / response = percent stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	   vline month / response = psi y2axis stat = mean group = Bin ;
	   vline month / response = Marginal_Stable y2axis stat = mean group = Bin ;
	   vline month / response = Unstable y2axis stat = mean group = Bin ;
yaxis label = 'Percentage'; 
xaxis label = 'ApplicationMonth';
y2axis label = 'PSI' min = 0 max = 1 ;
run;
Title;

ods startpage = now;
Title "Bucketed as per Segment4";
proc sgplot data=AppStagePSI ;
where VariableName in ('SEG4BUCKET') ;
       vbar month / response = percent stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	   vline month / response = psi y2axis stat = mean group = Bin ;
	   vline month / response = Marginal_Stable y2axis stat = mean group = Bin ;
	   vline month / response = Unstable y2axis stat = mean group = Bin ;
yaxis label = 'Percentage'; 
xaxis label = 'ApplicationMonth';
y2axis label = 'PSI' min = 0 max = 1 ;
run;
Title;
/*ods region y=4in x=5in width=5in height=4in;*/
ods startpage = now;
Title "Bucketed as per Segment5";
proc sgplot data=AppStagePSI ;
where VariableName in ('SEG5BUCKET') ;
       vbar month / response = percent stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	   vline month / response = psi y2axis stat = mean group = Bin ;
	   vline month / response = Marginal_Stable y2axis stat = mean group = Bin ;
	   vline month / response = Unstable y2axis stat = mean group = Bin ;
yaxis label = 'Percentage'; 
xaxis label = 'ApplicationMonth';
y2axis label = 'PSI' min = 0 max = 1 ;
run;
Title;
/*ods layout end  ;*/

ods startpage = now;
/*ods layout absolute;*/
/*ods region y=0in x=0in width=5in height=4in;*/
Title "DAYSSINCEFIRSTABDISB";
proc sgplot data=AppStagePSI ;
where VariableName in ('DAYSSINCEFIRSTABDISB')  ;
       vbar month / response = Percent stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	   vline month / response = psi y2axis stat = mean group = Bin ;
	   vline month / response = Marginal_Stable y2axis stat = mean group = Bin ;
	   vline month / response = Unstable y2axis stat = mean group = Bin ;
yaxis label = 'Percentage'; 
xaxis label = 'ApplicationMonth';
y2axis label = 'PSI' min = 0 max = 1 ;
run;
Title;

ods startpage = now;
/*ods region y=0in x=5in width=5in height=4in;*/
Title "DAYSSINCELASTABDISB";
proc sgplot data=AppStagePSI ;
where VariableName in ('DAYSSINCELASTABDISB')  ;
       vbar month / response = Percent stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	   	   vline month / response = psi y2axis stat = mean group = Bin ;
	   vline month / response = Marginal_Stable y2axis stat = mean group = Bin ;
	   vline month / response = Unstable y2axis stat = mean group = Bin ;
yaxis label = 'Percentage'; 
xaxis label = 'ApplicationMonth';
y2axis label = 'PSI' min = 0 max = 1 ;
run;
Title;

ods startpage = now;
/*ods region y=4in x=5in width=5in height=4in;*/
Title "MAXCD";
proc sgplot data=AppStagePSI ;
where VariableName in ('MAXCD')  ;
       vbar month / response = Percent stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	   	   vline month / response = psi y2axis stat = mean group = Bin ;
	   vline month / response = Marginal_Stable y2axis stat = mean group = Bin ;
	   vline month / response = Unstable y2axis stat = mean group = Bin ;
yaxis label = 'Percentage'; 
xaxis label = 'ApplicationMonth';
y2axis label = 'PSI' min = 0 max = 1 ;
run;
Title;

ods startpage = now;
/*ods layout absolute;*/
/*ods region y=0in x=0in width=5in height=4in;*/
Title "NO_ZERO_ARREAR_24_2";
proc sgplot data=AppStagePSI ;
where VariableName in ('NO_ZERO_ARREAR_24_2') ;
       vbar month / response = Percent stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ; 
 vbar month / response = Percent stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	   vline month / response = psi y2axis stat = mean group = Bin ;
	   vline month / response = Marginal_Stable y2axis stat = mean group = Bin ;
	   vline month / response = Unstable y2axis stat = mean group = Bin ;
yaxis label = 'Percentage'; 
xaxis label = 'ApplicationMonth';
y2axis label = 'PSI' min = 0 max = 1 ; 
run;
Title;

ods startpage = now;
/*ods region y=0in x=5in width=5in height=4in;*/
Title "PAID2OWED12_V3";
proc sgplot data=AppStagePSI ;
where VariableName in ('PAID2OWED12_V3')  ;
       vbar month / response = Percent stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	   vline month / response = psi y2axis stat = mean group = Bin ;
	   vline month / response = Marginal_Stable y2axis stat = mean group = Bin ;
	   vline month / response = Unstable y2axis stat = mean group = Bin ;
yaxis label = 'Percentage'; 
xaxis label = 'ApplicationMonth';
y2axis label = 'PSI' min = 0 max = 1 ;

run;
Title;
/*ods layout end;*/
ods pdf close;
