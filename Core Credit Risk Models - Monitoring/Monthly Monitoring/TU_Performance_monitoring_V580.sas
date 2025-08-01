
%macro ODSOff(); /* Call prior to BY-group processing */
*	ods graphics off;
*	ods exclude all;
*	ods noresults;
%mend;

%macro ODSOn(); /* Call after BY-group processing */
*	ods graphics on;
*	ods exclude none;
*	ods results;
%mend;


%include "\\neptune\sasa$\SAS_Automation\SAS_Autoexec\autoexec6.sas";

OPTIONS NOSYNTAXCHECK ;
options compress = yes;
%include "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\macros\CreateMonthlyGini.sas";
libname tu '\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets\V580';
libname tu2 '\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets';
libname decile "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\SAS Decile Tables\TU V580";
libname Lookup '\\mpwsas64\Core_Credit_Risk_Models\V6\MetaData';
libname V6 '\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\V6 dataset';
libname d1 "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU\Rebuild_TU_seg1";
libname d2 "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU\Rebuild_TU_seg2";
libname d3 "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU\Rebuild_TU_seg3";
libname d4 "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU\Rebuild_TU_seg4";
libname d5 "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU\Rebuild_TU_seg5";
libname seg1 "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU\scores";
libname seg2 "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU\scores";
libname seg3 "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU\scores";
libname seg4 "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU\scores";
libname seg5 "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V580 TU\V580 TU\scores";
libname results "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V632 Model\V623\Data";
libname rebuilt "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\scores";
libname refit "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\V580 TU_Refit\Refit_scores";
libname crebuilt "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\calibrated_scores";
libname crefit "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\V580 TU_Refit\Calibrated_refit_scores";
libname source "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\TU Performance Monitoring\data";
libname Rejects "\\mpwsas64\Core_Credit_Risk_Models\V5\New_Rejects";
%let odbc = MPWAPS;

*production;
filename macros4 '\\mpwsas65\process_automation\Codes\Git_Repositories\Core Credit Risk Models - Monitoring\Macros';
options sasautos = (macros4);
*Testing;
/*filename macros4 '\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros';*/
/*options sasautos = (macros4);*/
%let odbc = MPWAPS;
data _null_;
     call symput("enddate",cats("'",put(intnx('month',today(),-1,'end'),yymmddd10.),"'"));
     call symput("startdate",cats("'",put(intnx('month',today(),-13,'end'),yymmddd10.),"'"));
     call symput('tday',put(intnx('day',today(),-1),yymmddn8.));
     call symput("actual_date", put(intnx("month", today(),-9,'end'),date9.));
     call symput("month", put(intnx("month", today(),-1,'end'),yymmn6.));
     call symput("prevmonth", put(intnx("month", today(),-2,'end'),yymmn6.));
	 call symput("prev2month", put(intnx("month", today(),-3,'end'),yymmn6.));
	 call symput("build_start", put(intnx("month", today(),-12,'end'),yymmn6.));
	 call symput("build_end", put(intnx("month", today(),-7,'end'),yymmn6.));
run;
%put &startdate ;
%put &enddate ;
%put &tday;
%put &actual_date;
%put &month;
%put &prevmonth;
%put &prev2month;
%put &build_start;
%put &build_end;
	proc format cntlin =decile._decile1_ fmtlib ;run;
	proc format cntlin =decile._decile2_ fmtlib ;run;
	proc format cntlin =decile._decile3_ fmtlib ;run;
	proc format cntlin =decile._decile4_ fmtlib ;run;
	proc format cntlin =decile._decile5_ fmtlib ;run;
proc sql stimer;
	connect to ODBC (dsn=&odbc);
	create table applicationbase_&month as 
	select * from connection to odbc ( 
		select *
		from DEV_DataDistillery_General.dbo.TU_applicationbase
		where appmonth>=&startdate.
	) ;
	disconnect from odbc ;
quit;

/* 
data applicationbase_&month;
set tu2.applicationbase_&month;
run;
*/
proc sql stimer;
	connect to ODBC (dsn=&odbc);
	create table DISBURSEDBASE4REBLT_&month as 
	select * from connection to odbc ( 
		select *
		from DEV_DataDistillery_General.dbo.DISBURSEDBASE4REBLT_&month
	) ;
	disconnect from odbc ;
quit;

data applicationbase_&month;
	SET applicationbase_&month;
		finalriskscore_TU = (1- (TU_V580_prob))*1000;
		if tu_seg = 1 then decile = put(finalriskscore_TU,s1core.);
		else if tu_seg = 2 then decile = put(finalriskscore_TU,s2core.);
		else if tu_seg = 3 then decile = put(finalriskscore_TU,s3core.);
		else if tu_seg = 4 then decile = put(finalriskscore_TU,s4core.);
		else if tu_seg = 5 then decile = put(finalriskscore_TU,s5core.);
		decile_b = put(input(decile,8.)+1,z2.);
		decile_w = input(decile,8.)+1;
		decile_s = decile_w;
RUN;

%macro getOutcomeData(inputdataset=,outcomeBase=);
	proc format cntlin =decile._decile1_ fmtlib ;run;
	proc format cntlin =decile._decile2_ fmtlib ;run;
	proc format cntlin =decile._decile3_ fmtlib ;run;
	proc format cntlin =decile._decile4_ fmtlib ;run;
	proc format cntlin =decile._decile5_ fmtlib ;run;

/*TO remove special characters in baseloanids 	*/
	data final_model(keep =uniqueid tranappnumber);
		set applicationbase_&month;
		if input(tranappnumber, best.) =. then delete;
	run;

/*	proc sql stimer;*/
/*		connect to odbc (dsn=mpwaps);*/
/*		select * from connection to odbc (*/
/*			drop table DEV_DataDistillery_General.dbo.final_model*/
/*		);*/
/*		disconnect from odbc;*/
/*	quit;*/
	%Upload_APS(Set =final_model , Server =Work, APS_ODBC = Dev_DDGe, APS_DB = DEV_DataDistillery_General , distribute = HASH([tranappnumber]));

	proc sql stimer;
		connect to ODBC (dsn=&odbc);
		create table DisbursedBase as 
		select * from connection to odbc ( 
			select A.tranappnumber,
					B.Principaldebt ,
					B.product , 
					B.Contractual_3_LE9 ,
					B.FirstDueMonth,
					E.FirstDueDate,
					cast(d.LNG_SCR as int) as TU_Score,
					F.Final_score_1 ,F.FirstDueMonth as PredictorMonth,
					C.Final_score_1, C.FirstDueMonth as PredictorMonth
			from (select tranappnumber, uniqueid from DEV_DataDistillery_General.dbo.final_model where(isnumeric(tranappnumber) =1)) A 
			inner join PRD_DataDistillery_data.dbo.Disbursement_Info E
			on a.tranappnumber = e.loanid
			left join PRD_DataDistillery_data.dbo.JS_Outcome_base_final B 
			on a.tranappnumber = B.loanid 
			left join CREDITPROFITABILITY.dbo.ELR_LOANESTIMATES_3_9_CALIB C 
			on b.loanid = c.loanid 
			left join CREDITPROFITABILITY.dbo.ELR_CARDESTIMATES_3_9_CALIB F
    		on b.loanid = F.loanid 
			left join PRD_PRESS.[capri].[CAPRI_BUR_PROFILE_TRANSUNION_PLSCORECARD] d
			on a.tranappnumber = right(d.uniqueid,9)
		) ;
		disconnect from odbc ;
	quit;

	proc sort data = DisbursedBase nodupkey;
		by tranappnumber;
	run;

	data &outcomeBase;
		set DISBURSEDBASE4REBLT_&month.;
	run;
%mend;
%getOutcomeData(inputdataset=applicationbase_&month,outcomeBase=Disbursedbase4);


/********************************************************************************************************************************************************/
/* 														Calibration			 																			*/
/********************************************************************************************************************************************************/
%macro Loopthroughcal(Dset,seg=,name=,prob=, Segmentation=, weight=);
      proc nlmixed data=&Dset (where = (&Segmentation = &seg))  ; 
            parms a=1 c=0;
            x=1/(1+((&prob/(1-&prob))**(-a))*exp(c) );
            MODEL Target ~ BINARY(x);
            _ll = _ll*&weight; 
            ODS OUTPUT ParameterEstimates= parameters (keep = Parameter Estimate);
      run;
      proc transpose data =  parameters out = parameters ;
            var Estimate ;
            id Parameter ;
      run;
      data parameters (drop = _NAME_ ) ;
            set parameters ; 
            Model = "&Name";
            &Name = &seg ;
      run;
      proc append base  = Parameters_&name data = parameters force ;
      quit;
%mend;


data Disbursedbase4;
	SET DISBURSEDBASE4REBLT_&month.;
		finalriskscore_TU = (1- (TU_V580_prob))*1000;
		if tu_seg = 1 then decile = put(finalriskscore_TU,s1core.);
		else if tu_seg = 2 then decile = put(finalriskscore_TU,s2core.);
		else if tu_seg = 3 then decile = put(finalriskscore_TU,s3core.);
		else if tu_seg = 4 then decile = put(finalriskscore_TU,s4core.);
		else if tu_seg = 5 then decile = put(finalriskscore_TU,s5core.);
		decile_b = put(input(decile,8.)+1,z2.);
		decile_w = input(decile,8.)+1;
		decile_s = decile_w;
RUN;

%macro calibration(Input=,Output=);
	proc sql;
		create table subset as
			select *, ((Principaldebt/sum(Principaldebt))*count(baseloanid)) as weight
			from &Input;
	quit;

	%Loopthroughcal(Dset=subset,seg=1,name = V571_seg,prob=cs_V570_prob,Segmentation=Comp_seg,weight=weight);
	%Loopthroughcal(Dset=subset,seg=2,name = V571_Seg,prob=cs_V570_prob,Segmentation=Comp_seg,weight=weight);
	%Loopthroughcal(Dset=subset,seg=3,name = V571_Seg,prob=cs_V570_prob,Segmentation=Comp_seg,weight=weight);
	%Loopthroughcal(Dset=subset,seg=4,name = V571_Seg,prob=cs_V570_prob,Segmentation=Comp_seg,weight=weight);
	%Loopthroughcal(Dset=subset,seg=5,name = V571_Seg,prob=cs_V570_prob,Segmentation=Comp_seg,weight=weight);

	%Loopthroughcal(Dset=subset,seg=1,name = V581_seg,prob=TU_V580_prob,Segmentation=Tu_seg,weight=weight);
	%Loopthroughcal(Dset=subset,seg=2,name = V581_Seg,prob=TU_V580_prob,Segmentation=Tu_seg,weight=weight);
	%Loopthroughcal(Dset=subset,seg=3,name = V581_Seg,prob=TU_V580_prob,Segmentation=Tu_seg,weight=weight);
	%Loopthroughcal(Dset=subset,seg=4,name = V581_Seg,prob=TU_V580_prob,Segmentation=Tu_seg,weight=weight);
	%Loopthroughcal(Dset=subset,seg=5,name = V581_Seg,prob=TU_V580_prob,Segmentation=Tu_seg,weight=weight);

	proc sql;
	      create table Segment_calib
	      as select a.*,
		  1/(1+(cs_V570_prob/(1-cs_V570_prob))**(-1*(b.a))*exp(b.c)) as V571,
	      1/(1+(TU_V580_prob/(1-TU_V580_prob))**(-1*(c.a))*exp(c.c)) as V581
	      from  subset a
		  left join parameters_V571_seg b
		  on a.Tu_seg = b.V571_Seg
	      left join parameters_V581_seg c
	      on a.Tu_seg = c.V581_Seg;
	quit;

	proc sort data=Segment_calib nodupkey out=predata;
		by baseloanid;
	run;

	data predata;
	      set predata;
	      if compress(INSTITUTIONCODE) in ('','BNKINV') then INSTITUTIONCODE ='BNKOTH';
	run;

	%Loopthroughcal(Dset=predata,seg='BNKABL' ,prob=V571,name = V572_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
	%Loopthroughcal(Dset=predata,seg='BNKABS' ,prob=V571,name = V572_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
	%Loopthroughcal(Dset=predata,seg='BNKCAP' ,prob=V571,name = V572_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
	%Loopthroughcal(Dset=predata,seg='BNKFNB' ,prob=V571,name = V572_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
	%Loopthroughcal(Dset=predata,seg='BNKNED' ,prob=V571,name = V572_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
	%Loopthroughcal(Dset=predata,seg='BNKSTD' ,prob=V571,name = V572_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
	%Loopthroughcal(Dset=predata,seg='BNKOTH' ,prob=V571,name = V572_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);

	%Loopthroughcal(Dset=predata,seg='BNKABL' ,prob=V581,name = V582_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
	%Loopthroughcal(Dset=predata,seg='BNKABS' ,prob=V581,name = V582_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
	%Loopthroughcal(Dset=predata,seg='BNKCAP' ,prob=V581,name = V582_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
	%Loopthroughcal(Dset=predata,seg='BNKFNB' ,prob=V581,name = V582_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
	%Loopthroughcal(Dset=predata,seg='BNKNED' ,prob=V581,name = V582_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
	%Loopthroughcal(Dset=predata,seg='BNKSTD' ,prob=V581,name = V582_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
	%Loopthroughcal(Dset=predata,seg='BNKOTH' ,prob=V581,name = V582_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);

	proc sql;
	      create table Bank_Calibration
	      as select a.*,
	      1/(1+(V571/(1-V571))**(-1*(c.a))*exp(c.c)) as V572,
		  1/(1+(V581/(1-V581))**(-1*(c.a))*exp(c.c)) as V582
	      from predata a
		  left join PARAMETERS_V572_INSTITUTIONCODE b
		  on a.Institutioncode = b.V572_Institutioncode
	      left join PARAMETERS_V582_INSTITUTIONCODE c
	      on a.Institutioncode = c.V582_Institutioncode;
	quit;

	proc sort data=Bank_Calibration nodupkey out=&Output;
		by baseloanid;
	run;
%mend;
%calibration(Input=Disbursedbase4,Output=Disbursedbase5);

data Disbursedbase5;
 set Disbursedbase5;
 where maxSCORECARDVERSION <> 'V4';
run;

%macro disb(in=);
	%do i=1 %to 5;
		data disb&i.;
			set &in.;
			if tu_seg = &i. then do;
				seg = tu_seg;
				output disb&i.;
			end;
		run;
	%end;
%mend;
%disb(in=Disbursedbase5);

/********************************************************************************************************************************************************/
/* 														Columns preparation 																			*/
/********************************************************************************************************************************************************/
%macro create_scorecard_variables_list(numberofsegments=,modeltype=);
	%if &modelType = T %then %do;
		proc sql;
	        create table _estimates_&numberofsegments. as
	              select a.variable as parameter,  upcase(a.new_variable) as scorecardvariable
	              from Lookup.Tu_Lookup a, d&numberofsegments..parameter_estimate b
	              where cats(upcase(a.variable),"_W") = upcase(b.Effect);
	    quit;
	%end;
	%else %if &modelType = C %then %do;
		proc sql;
		    create table _estimates_&numberofsegments. as
		      select tranwrd(a.parameter,"_W"," ") as parameter,  b.NAME as scorecardvariable
		      from data&numberofsegments.._estimates_ a left outer join Lookup.Compscanvar_lookup b
		      on  upcase(a.Parameter) = cats(upcase(b.newcolumn),"_W");
		quit;

		Data _estimates_&numberofsegments.;
			set _estimates_&numberofsegments.;
			if parameter = "Intercept" then delete;
			if scorecardvariable = " " then scorecardvariable = parameter;
		run;
	%end;

    data _estimates_&numberofsegments;
		set _estimates_&numberofsegments end = eof;
		output;
		if eof then do;
			parameter = "DECILE" ;
			scorecardvariable="DECILE"; 
			output;
		end;
    run;
    %global  segment_&numberofsegments._list segment_&numberofsegments._list_woe segment_&numberofsegments._list_buckets segment_&numberofsegments._list_s  ;
    proc sql;
		select parameter into : segment_&numberofsegments._list separated by " " from _estimates_&numberofsegments;
		select cats(parameter,"_B") into : segment_&numberofsegments._list_buckets separated by " " from _estimates_&numberofsegments;
		select cats(parameter,"_W") into : segment_&numberofsegments._list_woe separated by " " from _estimates_&numberofsegments;
		select cats(parameter,"_S") into : segment_&numberofsegments._list_S separated by " " from _estimates_&numberofsegments;
    quit;
%mend;
%create_scorecard_variables_list(numberofsegments=1,modeltype=T);
%create_scorecard_variables_list(numberofsegments=2,modeltype=T);
%create_scorecard_variables_list(numberofsegments=3,modeltype=T);
%create_scorecard_variables_list(numberofsegments=4,modeltype=T);
%create_scorecard_variables_list(numberofsegments=5,modeltype=T);

data applicationsbase;
	set applicationbase_&month;
	if input(ApplicationDate , yymmdd10.) >= intnx('month',today(),-12,'begin');
run;

proc sql;
	create table NewAppbase as
		select *
		from applicationsbase
		where baseloanid in (select baseloanid from Disbursedbase5 where Principaldebt ne .);
quit;

data NewAppbase;
	set NewAppbase;
	month = appmonth;
run;

data build_new;
	set applicationsbase;
	if appmonth >= &build_start and appmonth <= &build_end; 
run;

%macro datapreparation(applicationbase=,numberofsegment=);
	%do seg = 1 %to &numberofsegment;
		proc sql;
			create table _estimates_ as
				select a.*, b.Estimate as target 
				from _estimates_&seg. a left join d&seg.._estimates_ b
				on UPCASE(a.parameter) = UPCASE(tranwrd(b.Effect,"_W",""));
		quit;

		filename _temp_ temp;

		data _null_;
			set  _estimates_;
			where upcase(parameter) not in ("DECILE","INTERCEPT");
			file _temp_;
			formula1 = cats(parameter,'_S = ',parameter,'_W*',target,';');
			formula2 = cat('if ', compress(parameter),'_S = . then delete;');
			put formula1;
			put formula2;
		run;

		data segment_&seg.;
			set &applicationbase(where= (tu_seg=&seg));
			month = put(input(ApplicationDate,yymmdd10.),yymmn6.);
			decile_b = put(input(decile,8.)+1,z2.);
			decile_w =input(decile,8.)+1;
			decile_s = decile_w;
			count=1;
			seg = tu_seg;
			%include _temp_;
		run;

		data build_&seg.;
			set build_new;
			count=1;
			%include _temp_;
			if tu_seg = &seg. then output build_&seg.;
		run;

		data disb&seg.;
			set disb&seg.;
			%include _temp_;
		run;
	%end;
%mend;
%datapreparation(applicationbase=applicationsbase,numberofsegment=5);

%macro rename();
	%do seg=1 %to 5;
        proc sql;
            create table _estimates_&seg. as
                  select a.variable as parameter, a.new_variable as scorecardvariable
                  from Lookup.Tu_Lookup a, d&seg.._estimates_ b
                  where cats(upcase(a.variable),"_W") = upcase(b.Effect);
        quit;
	%end;
%mend;
%rename();

%macro psi_calculation_monitoring(build=, base=,period=month,var=,psi_var=, outputdataset=) ;
/*	%if %VarExist(&build, &psi_var)=1 and %VarExist(&base, &psi_var)=1 and %VarExist(&base, &period)=1 %then %do;*/
	proc freq data = &base;
		tables &period*&psi_var / missing outpct out=basetable(keep =&period &psi_var pct_row rename =(pct_row = percent));
	run;
	proc freq data = &build;
		tables &psi_var /missing out=buildtable(keep=&psi_var percent);
	run;
	data buildtable;
		set buildtable;
		binnumber=_n_;
	run;
	proc sql;
		create table basetable2 as
			select distinct  *
			from basetable a inner join  buildtable(keep = &psi_var binnumber)  b
			on a.&psi_var = b.&psi_var
			;
	quit;
	proc sort data = basetable2;
		by &period &psi_var;
	run;
	proc transpose data = basetable2 out = psitrans prefix = bin ;	
		by &period;
		id binnumber;
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
	data all_psi_results(keep = Variablename &period psi marginal_stable unstable);
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
		variablename = tranwrd(upcase("&var."),"_W","");
	run;
	data buildset ;
		set buildtable(rename=(&psi_var =scores));
		length variablename $32.;
		&period =" BUILD";
		psi=.;
		marginal_stable=.;
		unstable=.;
		variablename=tranwrd(upcase("&var."),"_W","");
	run;
	proc sql;
		create table summarytable(rename=(&psi_var=scores)) as
			select *
			from basetable(keep = &period &psi_var percent) a inner join all_psi_results b
			on a.&period = b.&period;
	quit;
	proc append base = &outputdataset data = summarytable force; run;
	proc append base = &outputdataset data = buildset force; run;
	proc datasets lib = work;
       delete buildset all_psi_results summarytable basetable basetable2 all_psi psitrans buildtrans buildtable ;
  	run;quit;
%mend;

%macro looppervariables_monitoring(segment=0,base=,build1=,variable_list=,outdataset=) ;
	data segment;
		set &base.;
		where Seg = &segment;
	run;
	proc delete data = &outdataset._&segment; run;

	%do i = 1 %to %sysfunc(countw(&variable_list));
		%let vari = %scan(&variable_list, &i.);
		%psi_calculation_monitoring(build=&build1, base=segment,period=month,var=&vari,psi_var=&vari._S, outputdataset=&outdataset._&segment);
		data &outdataset._&segment;
			set &outdataset._&segment;
			seg = &segment;
		run;
	%end;
%mend;

%looppervariables_monitoring(segment=1,base=segment_1, build1=build_1,variable_list=&segment_1_list decile,outdataset=summarytable);
%looppervariables_monitoring(segment=2,base=segment_2, build1=build_2,variable_list=&segment_2_list decile,outdataset=summarytable);
%looppervariables_monitoring(segment=3,base=segment_3, build1=build_3,variable_list=&segment_3_list decile,outdataset=summarytable);
%looppervariables_monitoring(segment=4,base=segment_4, build1=build_4,variable_list=&segment_4_list decile,outdataset=summarytable);
%looppervariables_monitoring(segment=5,base=segment_5, build1=build_5,variable_list=&segment_5_list decile,outdataset=summarytable);

/********************************************************************************************************************************************************/
/* 												Segment Distribution Plot 																	        	*/
/********************************************************************************************************************************************************/
data build;
	set build_new;
	seg = tu_seg;
	month = substr(compress(ApplicationDate,'-'),1,6);
run;

data applicationsbase1;
	set NewAppbase;
	month = Appmonth;
	seg = tu_seg;
run; 


/* Overall segment distribution */
%psi_calculation_monitoring(build=build, base=applicationsbase1,period=month,var=seg,psi_var=seg, outputdataset=summarytable_01);

data tu.summarytable_01_&month.;
   set summarytable_01;
run;

/********************************************************************************************************************************************************/
/* 												Variable Distribution Plot 																	        	*/
/********************************************************************************************************************************************************/
%Macro PSIRenaming (seg);
	proc delete data = all_summarytable ; run;
	proc delete data = segments; run;
	%do i= 1 %to &seg;
		data summarytable_&i;
			set summarytable_&i;
			label scores = 'Scores';
		run;
		Proc sql;
			create table PSI_&i. as
				select a.* ,b.scorecardvariable
				from summarytable_&i. a
				left join _estimates_&i. b
				on upcase(a.variablename) = upcase(b.Parameter);
		quit;
		proc append base = all_summarytable data = psi_&i force; run;
		proc append base = segments data = segment_&i force ; run;
	%end;
%mend;
%PSIRenaming(5);

/* Calculate Overall Decile */
data build;	set build_1 build_2 build_3 build_4 build_5;run;
data segments;	set segment_1 segment_2 segment_3 segment_4 segment_5;run;
%let segment_0_list = DECILE;
%let segment_0_list_woe = DECILE_W;
%let segment_0_list_buckets = DECILE_B;
%let segment_0_list_S = DECILE_S;

%macro looppervariables(segment=0,base=,build1=,variable_list=,outdataset=);
	data segment;
		set &base.;
	run;
	proc delete data = &outdataset._&segment; run;
	%do i = 1 %to %sysfunc(countw(&variable_list));
		%let vari = %scan(&variable_list, &i.);
		%psi_calculation_monitoring(build=&build1, base=segment,period=month,var=&vari,psi_var=&vari._W, outputdataset=&outdataset._&segment);
		data &outdataset._&segment;
			set &outdataset._&segment;
			seg = &segment;
		run;
	%end;
%mend;
%looppervariables(segment=0,base=segments, build1=build,variable_list=decile,outdataset=summarytable);

%macro appendPSI(outdataset=);
	proc delete data = &outdataset; run;
	%do i = 0 %to 5;
		%if &i= 0 %then %do;
			data _tempPSI_;
				set summarytable_&i;
				length variable_name $32.;
				variable_name ="DECILE";
				segment = &i.;
			run;
		%end;
		%else %do;
			proc sql;
				create table _tempPSI_ as
					select &i as segment, b.scorecardvariable as variable_name, a.*
					from (select * from summarytable_&i) a left join _estimates_&i b
					on  upcase(a.variablename) = upcase(b.Parameter);
			quit;
		%end;
		proc append base = &outdataset  data=_tempPSI_ force; run;
	%end;
%mend;
%appendPSI(outdataset =Tu.Tu_Var_Distribution_Reblt_&month.); 

%macro Create3MonthPsiReport(inputDataset=,outputDataset=);
	proc sql;
		create table max_psi as
			select  seg,variablename, max(psi) as psi
			from &inputDataset 
			where variablename ne 'DECILE'
			group by seg, variablename;
	quit;
	data max_psi;
		set max_psi;
		month = "max csi";
	run;
	proc sql;
		create table last3month as	
			select distinct month
			from &inputDataset
			where month ne . /*" BUILD"*/
			order by month desc;
	quit;
	data last3month;
		set last3month(obs=3);
	run;	
	proc sql;
		create table last3monthdata as
			select seg, put(a.month, 8.) as month,variablename, psi
			from (select * from &inputDataset where variablename ne 'DECILE' )
			a inner join  last3month b
			on a.month = b.month;
	quit;
	data CSI_Distribution_reblt_&month.;
		length month $8.;
		set last3monthdata max_psi;
	run;
	proc sql;
	    create table &outputDataset as
			select b.New_variable as variable_name, a.*
			from CSI_Distribution_reblt_&month. a 
			left join Lookup.Tu_Lookup b
			on upcase(a.variablename) = upcase(b.variable)
			order by a.seg, a.month desc;
	quit;
%mend;
%Create3MonthPsiReport(inputDataset=Tu.Tu_Var_Distribution_Reblt_&month., outputDataset=tu.CSI_Distribution_reblt_&month. );

/********************************************************************************************************************************************************/
/* 												Trend Over Time calculation 																			*/
/********************************************************************************************************************************************************/
%PercentageSloping_Monitoring(inputdataset=disb1,listofvars=&segment_1_list_S,target=target, period=month, trendreporttable=Variables_trendreport_reblt_seg1,slopingreporttable=alltrendtable_reblt_seg1,segment=1);
%PercentageSloping_Monitoring(inputdataset=disb2,listofvars=&segment_2_list_S,target=target, period=month, trendreporttable=Variables_trendreport_reblt_seg2,slopingreporttable=alltrendtable_reblt_seg2,segment=2);
%PercentageSloping_Monitoring(inputdataset=disb3,listofvars=&segment_3_list_S,target=target, period=month, trendreporttable=Variables_trendreport_reblt_seg3,slopingreporttable=alltrendtable_reblt_seg3,segment=3);
%PercentageSloping_Monitoring(inputdataset=disb4,listofvars=&segment_4_list_S,target=target, period=month, trendreporttable=Variables_trendreport_reblt_seg4,slopingreporttable=alltrendtable_reblt_seg4,segment=4);
%PercentageSloping_Monitoring(inputdataset=disb5,listofvars=&segment_5_list_S,target=target, period=month, trendreporttable=Variables_trendreport_reblt_seg5,slopingreporttable=alltrendtable_reblt_seg5,segment=5);

%macro AppendingSlopingTable (segment=,outdataset=);
	proc sql;
		create table _temptrendtable_ as
			select b.scorecardvariable as variable_name, a.*
			from Alltrendtable_reblt_seg&segment a left join _estimates_&segment b
			on  upcase(tranwrd(a.variable_name,"_S"," ")) = upcase(b.Parameter);
	quit;

	data trendtables_&segment (keep =variable_name segment slope_rate Recommended_Action);
	    set _temptrendtable_;
		if upcase(variable_name) = "" then delete;
	    format Recommended_Action $500.;
	    segment = &segment.;
		overall_threshold=overall_threshold/100;	    
	    label variable_name ="Variable Name" overall_threshold = "Slope Rate" Recommended_Action ="Recommended Action" ;
	    if overall_threshold >=0.8 then do;
	          Recommended_Action = cats("No Action ");
	    end;
	    else if overall_threshold <0.8 then do;
	          Recommended_Action =cats("Investigate if rebucketing or collapsing of bucket is required and Investigate if variable should be removed");
	    end;
	    rename overall_threshold = slope_rate;
	run;

	proc sql;
		create table Variables_trendreport_reblt_seg&segment as
			select b.scorecardvariable as variable_name, a.*
			from Variables_trendreport_reblt_seg&segment a left join _estimates_&segment b
			on  upcase(tranwrd(a.VarName,"_S"," ")) = (b.Parameter);
	quit;

	data Variables_trendreport_reblt_seg&segment;
		set Variables_trendreport_reblt_seg&segment;
		if upcase(VarName) = "DECILE_S" then delete;
	run;
	
	proc append base = tu.Var_trendreport_reblt_&month data = Variables_trendreport_reblt_seg&segment force; run;
	proc append base = &outdataset data = trendtables_&segment force; run;
%mend;
%AppendingSlopingTable (segment=1,outdataset=v5_sloperate_reblt_&month);
%AppendingSlopingTable (segment=2,outdataset=v5_sloperate_reblt_&month);
%AppendingSlopingTable (segment=3,outdataset=v5_sloperate_reblt_&month);
%AppendingSlopingTable (segment=4,outdataset=v5_sloperate_reblt_&month);
%AppendingSlopingTable (segment=5,outdataset=v5_sloperate_reblt_&month);

/********************************************************************************************************************************************************/
/* 												Variable Contribution 		    															        	*/
/********************************************************************************************************************************************************/
%macro combineContributionPerSegment();
	proc delete data=contrib; run;
	%do seg = 1 %to 5;
		proc delete data = variablecontributions var; run;
		%ContribCalc_Montoring(d&seg.._ESTIMATES_, disb&seg,seg&seg._contrib);
		data seg&seg._contrib;
			set work.seg&seg._contrib;
			segment = &seg.; 
		run;
		proc append base=contrib data=seg&seg._contrib force ; run;
	%end;
	proc sql;
	    create table contrib2 as
	          select b.New_variable, a.*
	          from contrib a 
			  left join Lookup.Tu_Lookup b
	          on upcase(a.variable) = cats(upcase(b.variable),"_W");
	quit;
%mend;
%combineContributionPerSegment();

/********************************************************************************************************************************************************/
/* 												Variable Correlation     																	        	*/
/********************************************************************************************************************************************************/
%macro combineCorrelationPerSegment();
	proc delete data = corr_percent ; run;
	%do seg = 1 %to 5;
		proc sql; select Parameter into : modvars separated by ' ' from d&seg.._ESTIMATES_ where  estimate ne . and upcase(parameter) ne 'INTERCEPT'; quit;
		%CORR_Monitoring (disb&seg, &modvars ,segcorr&seg, Summary&seg., 101, -101) ;

		%let vcount = %sysfunc(countw(&modvars));
		data corr_percent&seg;
			set work.segcorr&seg;
			array vars [&vcount] &modvars;
			flag = 0;
			do i = 1 to &vcount;
				if vars[i] ne 1 and vars[i] > 0.6 then do;
					flag = flag+1;
				end;
			end;
			corr_percent=1-flag/(&vcount-1);
		run;

		data corr_percent&seg;
			set work.corr_percent&seg;
			segment = &seg;
		run;
		proc append base = corr_percent data = corr_percent&seg force ; run;
	%end;
	proc sql;
	    create table corr_percent2 as
	          select b.New_variable, a.*
	          from corr_percent(keep= _name_ segment corr_percent) a 
			  left join Lookup.Tu_Lookup b
	          on upcase(a._name_) = cats(upcase(b.variable),"_W");
	quit;
%mend;
%combineCorrelationPerSegment();

/********************************************************************************************************************************************************/
/* 												Variable VIF  			 																	        	*/
/********************************************************************************************************************************************************/
%macro combineVifPerSegment();
	%do seg = 1 %to 5;
		proc sql; select Parameter into : modvars separated by ' ' from d&seg.._ESTIMATES_ where  estimate ne . and upcase(parameter) ne 'INTERCEPT'; quit;
		%VIF_Monitoring(Scoreset=disb&seg,Vars=&modvars,NameOutput=vif,outdataset=vif_seg&seg); 
		data vif_seg&seg;
			set vif;
			segment = &seg;
		run;
*		proc append base=temp_vif data = d&seg..vif_seg&seg. force;* run;
	%end;
	data temp_vif;
		set vif_seg1 vif_seg2 vif_seg3 vif_seg4 vif_seg5;
	run;
	proc sql;
	    create table vif2 as
			select b.New_variable, a.*
			from temp_vif a 
			left join Lookup.Tu_Lookup b
			on upcase(a.variable) = cats(upcase(b.variable),"_W");
	quit;
%mend;
%combineVifPerSegment();

/********************************************************************************************************************************************************/
/* 												Variable Confidence Bands 																	        	*/
/********************************************************************************************************************************************************/
%macro plotconfidencebands_report(inputdata=, segment=) ;
	proc sql;
		select parameter into : confidvar separated by ' ' 
		from _estimates_&segment;
	quit;
	proc sql noprint;
		select parameter into : descriptionvar separated by ' ' 
		from _estimates_&segment;
	quit;

	%do u = 1 %to %sysfunc(countw(&confidvar));
		%let var = %scan(&confidvar, &u);
		%let dvar = %scan(&descriptionvar,&u);
		
		proc sql noprint;
			create table allout as
				select &var,count(*)
				from &inputdata
				group by &var;
		quit;
		%do i = 1 %to %obscnt(allout);

			data onescore;
				set allout;
				if _n_ = &i;
			run;

			proc sql noprint; select cats("&dvar woe : ",&var) into : score  from onescore ; quit;

			proc sql noprint;
				create table Alloutx as 
					select * from &inputdata
					where &var in (select &var from onescore );
			quit;

		    %let position = %sysfunc( mod(&i, 4) );
		    %if &position = 1 %then %position_report(0,0);
		    %else %if &position = 2 %then %position_report(0,4);
		    %else %if &position = 3 %then %position_report(4.5,0);
		    %else %if &position = 0 %then %position_report(4.5,4.5);

		    %let width = 4;
		    %let height = 4;
		    %if &position = 1 %then %do;
		          ods pdf startpage = now;
		          ods layout start;
		    %end;

		    ods region y=&y.in x=&x.in width=&width.in height=&height.in;
		    %VarCheckConfidenceBand1_Report(Alloutx, month, V655_2 , target , Principaldebt ,0,0,4,4, &score ) ;

			data summary1;
				set summary1;
				length variablename $40 bin 8.;
				if actual>= lowerbound and actual<= upperbound then flag = 0;
				else flag = 1;
				bin=&i;
				variablename ="&var" ;
			run;

			proc append base= seg_summary&segment data=summary1 force ; run;

			%let obs = %obscnt(allout);
			%let count = %sysfunc( mod(&obs, 4) );

		    %if &position = 0  %then %do;
		          ods layout end ;
		    %end;

			%if (%obscnt(allout)= &i.) %then %do;
		          ods layout end ;
		    %end;
		%end;
	%end;
%mend;

%macro combineConfidentPerSegment();
	proc delete data = confidence; run;
	%do seg = 1 %to 5 ;
		/*Segment 1*/
		data _estimates_&seg;
			set D&seg.._ESTIMATES_;
			where upcase(parameter) ne "INTERCEPT";
		run;
		%plotconfidencebands_report(inputdata=disb&seg., segment=&seg.);
		data seg_summary&seg;
			set seg_summary&seg;
			segment = &seg;
		run;
		proc sql;
			create table seg&seg._confidence  as
				select distinct (variablename), segment, sum(flag)/count(actual) as no_of_points_outside_cb
				from seg_summary&seg
				group by variablename;
		quit;
		proc append base=confidence data = seg&seg._confidence force ; run;
	%end;
	proc sql;
		create table confidence2 as
			select b.New_variable, a.*
			from confidence a 
			left join Lookup.Tu_Lookup b
			on upcase(a.variablename) = cats(upcase(b.variable),"_W");
	quit;
%mend;
%combineConfidentPerSegment();

/********************************************************************************************************************************************************/
/* 												Variable Slope Rate		 																	        	*/
/********************************************************************************************************************************************************/
proc sql;
	create table tu.v5_sloperate_reblt_&month. as
		select a.*, corr_percent as correlation, c.contribution, d.vif, e.No_of_points_outside_CB
		from V5_SLOPERATE_REBLT_&month. a
		left join CORR_PERCENT2 b
		on upcase(a.variable_name) = upcase(b.New_variable)
			and a.segment = b.segment
		left join CONTRIB2 c
		on  upcase(a.variable_name) = upcase(c.New_variable)
			and a.segment = c.segment
		left join VIF2 d
		on  upcase(a.variable_name) = upcase(d.New_variable)
			and a.segment = d.segment
		left join CONFIDENCE2 e
		on  upcase(d.VARIABLE) = upcase(e.variablename)
			and a.segment = e.segment;
quit;

/********************************************************************************************************************************************************/
/* 												Calculate the month Ginis for the variables 															*/
/********************************************************************************************************************************************************/
%macro CreateMonthlyGini(Dset = , TargetVar =  , Score =  ,   Measurement = ,outdataset=Ginitable) ; 
	proc contents data =&Dset out = names; run;
	data _null_;
		set names;
		if upcase(name)=upcase("&Measurement");
		call symput('colformat',TYPE);
	run;

	%if &colFormat = 2 %then %do;
		proc sql noprint ; 
			select count(distinct &Measurement) into : NumberofIntervals separated by '' 
			from &Dset   ;
		quit;
		proc sql noprint ;
			select distinct &Measurement into : list separated by ' '
			from
			&Dset ;
		quit;
		proc delete data = GiniTable ;	run;

		* Count the number of words in &LIST;
		%local count;
		%let count=0; 
		%do %while(%qscan(&list,&count+1,%str( )) ne %str());
			%let count = %eval(&count+1);
		%end;
		%let cntlist = &count ; 
		%put &cntlist;

		%do k =1 %to &cntlist ;
			%let InMeasurement = %scan(&list, &k);
			data _&k._ ;
				set &Dset;
				if  &Measurement  = "&InMeasurement" then output ;
			run;
			proc delete data = Gini ;
			run;
			%Calc_Gini (Predicted_col=&score, Results_table =  _&k._ , Target_Variable = target , Gini_output = Gini );
			data Gini ;
				retain &Measurement ; 
				set Gini ;
				format  &Measurement $200. ;
				&Measurement = "&InMeasurement";
			run;
			proc append base = &outdataset force data = Gini (keep = &Measurement Gini) ;
			run;
		%end;
	%end;
	%else %do;
		data allout;
			set &dset(rename =(&measurement = month5));
			&measurement = put(month5, yymmn6.);
			drop month5;
		run;
		proc sql noprint ; 
			select count(distinct &Measurement) into : NumberofIntervals separated by '' 
			from allout   ;
		quit;
		proc sql noprint ;
			select distinct &Measurement into : list separated by ' '
			from allout ;
		quit;
		proc delete data = GiniTable ;	run;

		* Count the number of words in &LIST;
		%local count;
		%let count=0; 
		%do %while(%qscan(&list,&count+1,%str( )) ne %str());
			%let count = %eval(&count+1);
		%end;
		%let cntlist = &count ; 
		%put &cntlist;

		%do k =1 %to &cntlist ;
			%let InMeasurement = %scan(&list, &k);
			data _&k._ ;
				set allout;
				if  &Measurement  = "&InMeasurement" then output ;
			run;
			proc delete data = Gini ;run;
			%Calc_Gini (Predicted_col=&score, Results_table =  _&k._ , Target_Variable = target , Gini_output = Gini );
			data Gini ;
				retain &Measurement ; 
				set Gini ;
				format  &Measurement $200. ;
				&Measurement = "&InMeasurement";
			run;
			proc append base = &outdataset force data = Gini (keep = &Measurement Gini) ;
			run;
		%end;
	%end;
%mend;

%macro giniovertime(inset=,var=,target=target, period=month,output=,varname=,ReportTable=);
	data combined1 ( keep =  &period Prob &target ) ;
		set &inset ;
		Prob = logistic(&var);
	run;
	data _tmp;
		set combined1;
	run; 
	%let y = target;
	%CreateMonthlyGini(Dset = combined1 , TargetVar = &target  , Score = Prob ,   Measurement = &Period ) ;
	data &output ;
		set Ginitable ;
		format VarName $32. ;
		VarName = tranwrd(upcase("&VarName"),"_W","") ;
	run;
	proc append base = &ReportTable. data = &output. force nowarn ;	quit;
%mend;
%GiniPerVariable_Monitoring(segment=1,inputdata=disb1,FinalScoreField=P_Target1,period=month,target=target,outputdata=Ginis1 );
%GiniPerVariable_Monitoring(segment=2,inputdata=disb2,FinalScoreField=P_Target1,period=month,target=target,outputdata=Ginis2 );
%GiniPerVariable_Monitoring(segment=3,inputdata=disb3,FinalScoreField=P_Target1,period=month,target=target,outputdata=Ginis3 );
%GiniPerVariable_Monitoring(segment=4,inputdata=disb4,FinalScoreField=P_Target1,period=month,target=target,outputdata=Ginis4 );
%GiniPerVariable_Monitoring(segment=5,inputdata=disb5,FinalScoreField=P_Target1,period=month,target=target,outputdata=Ginis5 );

%macro appendginis(outdataset=);
	%macro rename();
		%do seg=1 %to 5;
	        proc sql;
	            create table _estimates_&seg. as
	                  select a.variable as parameter, a.new_variable as scorecardvariable
	                  from Lookup.Tu_Lookup a, d&seg.._ESTIMATES_ b
	                  where cats(upcase(a.variable),"_W") = upcase(b.Effect);
	        quit;
		%end;
	%mend;
	%rename();
	%do i = 1 %to 5;
		proc sql;
			create table _tempgini_ as
				select b.scorecardvariable as variable_name,&i as segment, a.*
				from (select * from Ginis&i ) a left join _estimates_&i b
				on  upcase(a.VarName) = (b.Parameter);
		quit;
		proc append base = tu.var_gini_summary_reblt_&month. data=_tempgini_ force; run;
	%end;
%mend;
%appendginis(outdataset=tu.var_gini_summary_reblt_&month.);

/********************************************************************************************************************************************************/
/* 												Calculate overall gini for each segments 																*/
/********************************************************************************************************************************************************/
data Disbursedbase6;
	set DISBURSEDBASE4REBLT_&month;
	Compuscan_Generic = prismscoremi;
	Tu_Generic = Tu_Score   ;
run;

%V655_RTI_ORM_scoring(indata=Disbursedbase6);

data tu.RTI_ORM;
set RTI_ORM;
run;

proc sql;
	create table Disbursedbase6 as
	select B.RTI, B.ORM, B.ORM_2, A.* 
	from Disbursedbase6 A
	left join tu.RTI_ORM B
	on A.tranappnumber = B.tranappnumber;
quit;

proc sort data=Disbursedbase6 nodupkey;
	by tranappnumber;
run;

data segment_1_gini segment_2_gini segment_3_gini segment_4_gini segment_5_gini;
	set Disbursedbase6;
	if tu_seg=1 then output segment_1_gini ;
	else if tu_seg=2 then output segment_2_gini ;
	else if tu_seg=3 then output segment_3_gini ;
	else if tu_seg=4 then output segment_4_gini ;
	else if tu_seg=5 then output segment_5_gini ;
run;

options mprint mlogic symbolgen;
%macro calcginimetrics(segment =0,indataset=, period=,target=, listofvargini=);
	%checkifcolumnsexist(indataset=&indataset,outdataset=missingvariables,columnlist=&period &target &listofvargini);
	%if %obscnt(missingvariables) = 0 %then %do;
		data score_table;
			set &indataset(keep = &target &period &listofvargini);
		run;
		proc delete data = _temp_; run;
		proc delete data = ginipersegment_seg&segment; run;

		%do v = 1 %to %sysfunc(countw(&listofvargini));
			%let scorevar = %scan(&listofvargini,&v.);
			proc delete data = &scorevar.; run;
			proc delete data = _temp1_; run;
			%CreateMonthlyGini(Dset =score_table , TargetVar = &target  , Score = &scorevar ,   Measurement = &period, outdataset =&scorevar.  ) ;
			data _temp1_(keep = segment &period Score_type gini);
				set &scorevar.;
				length Score_type $32.;
				Score_type ="&scorevar.";
				segment = &segment;
			run;
			proc append base = ginipersegment_seg&segment data = _temp1_ force;
			run;			
		%end;

		proc delete data = overallgini_seg&segment;
		run;
		%do v = 1 %to %sysfunc(countw(&listofvargini));
			%let scorevar = %scan(&listofvargini,&v.);	
			proc delete data = _temp_; run;
			proc delete data = &scorevar; run;
			%Calc_Gini (Predicted_col=&scorevar, Results_table = score_table , Target_Variable = &target , Gini_output = &scorevar );
			data _temp_(keep = segment score_type gini);
				set &scorevar;
				length Score_type $32.;
				Score_type ="&scorevar.";
				segment = &segment;
			run;
			proc append base = overallgini_seg&segment data =_temp_ force; 
			run;
		%end;
		Proc sort data=ginipersegment_seg&segment;
			by Month;
		run;
		proc transpose data=ginipersegment_seg&segment out=Gini_trans(drop=_name_);
			by Month;
			id score_type;
			var gini;
		run;;
		data ginipersegment_seg&segment;
			set Gini_trans;
			segment = &segment;
		run;
		proc append base =tu.giniperseg_summary_reblt_&month data = ginipersegment_seg&segment force; run;
		proc append base =tu.overallgini_summary_reblt_&month data = overallgini_seg&segment force; run;
	%end;
	%else %do;
		%put One column supplied does not exist ;
	%end;		
%mend;

%CalcGinimetrics(segment =1,indataset=segment_1_gini, period=month,target=target, listofvargini=cs_V570_prob CS_V560_Prob Compuscan_Generic Tu_Generic TU_V580_prob TU_V570_prob  V622  V635 V636 V645 V645_adj V655_2 RTI ORM ORM_2);
%CalcGinimetrics(segment =2,indataset=segment_2_gini, period=month,target=target, listofvargini=cs_V570_prob CS_V560_Prob Compuscan_Generic Tu_Generic TU_V580_prob TU_V570_prob  V622  V635 V636 V645 V645_adj V655_2 RTI ORM ORM_2);
%CalcGinimetrics(segment =3,indataset=segment_3_gini, period=month,target=target, listofvargini=cs_V570_prob CS_V560_Prob Compuscan_Generic Tu_Generic TU_V580_prob TU_V570_prob  V622  V635 V636 V645 V645_adj V655_2 RTI ORM ORM_2);
%CalcGinimetrics(segment =4,indataset=segment_4_gini, period=month,target=target, listofvargini=cs_V570_prob CS_V560_Prob Compuscan_Generic Tu_Generic TU_V580_prob TU_V570_prob  V622  V635 V636 V645 V645_adj V655_2 RTI ORM ORM_2);
%CalcGinimetrics(segment =5,indataset=segment_5_gini, period=month,target=target, listofvargini=cs_V570_prob CS_V560_Prob Compuscan_Generic Tu_Generic TU_V580_prob TU_V570_prob  V622  V635 V636 V645 V645_adj V655_2 RTI ORM ORM_2);
%CalcGinimetrics(segment =0,indataset=Disbursedbase6, period=month,target=target, listofvargini=cs_V570_prob CS_V560_Prob Compuscan_Generic Tu_Generic TU_V580_prob TU_V570_prob  V622  V635 V636 V645 V645_adj V655_2 RTI ORM ORM_2);

data tu.giniperseg_summary_reblt_&month;
	set  tu.giniperseg_summary_reblt_&month;
	rename cs_V570_prob=V570_Comp_Prob  Compuscan_Generic=Comp_Generic_Score Tu_Generic=TU_Generic_Score TU_V580_prob=V580_TU_Prob V622=V622_Prob 
	V572=V572_Comp_Prob V582=V582_TU_Prob V635=V635_Prob V636=V636_Prob V645=V645_Prob V645_adj=V645_AdjProb TU_V570_prob=V570_TU_Prob V655_2=V655_2_Prob cs_V560_prob=V560_Comp_Prob;
run;

/********************************************************************************************************************************************************/
/* 												Calculate build gini  																					*/
/********************************************************************************************************************************************************/
/*%macro buildgini();*/
/*	%do seg = 1 %to 5;*/
/*		data build_&seg.;*/
/*			set d&seg..build_scored(where= (seg=&seg.));*/
/*		run;*/
/*		%Calc_Gini (Predicted_col=P_Target1, Results_table = build_&seg. , Target_Variable = target , Gini_output = gini_&seg );*/
/**/
/*		data gini_&seg;*/
/*			set gini_&seg;*/
/*			segment = &seg.;*/
/*			score_type = 'Build TU Prob';*/
/*		run;*/
/**/
/*		proc append data=gini_&seg base=allgini force;*/
/*		run;*/
/*	%end;*/
/*%mend;*/
/*%buildgini();*/
/**/
/*data build;*/
/*	set build_1 build_2 build_3 build_4 build_5;*/
/*run;*/
/*%Calc_Gini (Predicted_col=P_Target1, Results_table = build , Target_Variable = target , Gini_output = buildgini );*/
/**/
/*data buildgini;*/
/*	set buildgini;*/
/*	segment = 0;*/
/*	score_type = 'Build TU Prob';*/
/*run;*/
/**/
/*data tu.TU_BUILD_GINI;*/
/*	set buildgini(keep=segment score_type gini) allgini(keep=segment score_type gini);*/
/*run;*/

data tu.overallgini_summary_reblt_&month;
	set tu.overallgini_summary_reblt_&month tu.TU_BUILD_GINI;
run;

proc sort data = tu.Tu_var_distribution_reblt_&month.(keep = month) nodupkey out = last_2_month; by descending month; run;
data last_2_month;	set last_2_month(obs = 2); run;

proc sql;
	create table variable_distribution as
		select a.month, a.segment, cats('Segment ',a.segment) as Population, 
				case when a.psi >=0.25 then 'Unstable %'
					 when a.psi >=0.1 then 'Marginally Unstable %'
					 else ' Stable %'
				end as Reason 
			   ,count(distinct variablename) as numberofvariables
			   , avg(psi) as avg_psi
		from (select  * from tu.Tu_var_distribution_reblt_&month where upcase(variablename) ne 'DECILE')  a, last_2_month b
		where a.month = b.month
		group by a.month, a.segment,3,4;
quit;

proc sql;
	create table reason_month as
		select distinct a.month, 
				case when a.psi >=0.25 then 'Unstable %'
					 when a.psi >=0.1 then 'Marginally Unstable %'
					 else ' Stable %'
				end as Reason 
		from (select  * from tu.Tu_var_distribution_reblt_&month where upcase(variablename) ne 'DECILE')  a, last_2_month b
		where a.month = b.month
		and segment ne 0
		group by a.month, 2;
quit;

data reason_month2;
	set last_2_month;
	Reason = 'Unstable %';
run;

data reason_month3;
	set last_2_month;
	Reason = 'Marginally Unstable %';
run;

proc append base=reason_month data= reason_month2; run;
proc append base=reason_month data= reason_month3; run;
proc sort data=reason_month noduprecs;	by month; run;

proc sql;
	create table segments as
		select segment, count(distinct variable_name) as total_variables 
		from tu.Tu_var_distribution_reblt_&month
		where segment ne 0 and upcase(variablename) ne 'DECILE'
		group by segment;
quit;

proc sql;
	create table reason2 as
		select *
		from reason_month a, segments b;
quit;
proc sort data = reason2; by month segment reason; run;
proc sort data = variable_distribution;	by month segment reason; run;

data tu.variables_stability_reblt_&month.;
	merge variable_distribution(in = a)
		  reason2(in = b );
	by month segment reason;
	if b;
	if b and not a then numberofvariables=0;
	Population=cats('Segment ',segment);
	Stable_percentage= numberofvariables/ total_variables;
run;

/********************************************************************************************************************************************************/
/* 												Challenger Models																						*/
/********************************************************************************************************************************************************/
/**************************************************Rebuilt***********************************************************************************************/
data rebuilt(drop=model);
	set rebuilt.CONSOLIDATEDAPPLIEDGINIS(rename=(applied_model=model));
	applied_Model = catx('_', Score_type, model);
run;

data rebuilt2;
	set rebuilt.CONSOLIDATEDGINIS;
	applied_Model = catx('_', Score_type, month);
run;

data tu.rebuilt_&month.;
	set rebuilt rebuilt2;
run;

/**************************************************Refit*************************************************************************************************/
data refit(drop=model);
	set REFIT.APPLIED_CONSOLIDATED_REFIT_GINIS(rename=(applied_model=model));
	applied_Model = catx('_', Score_type, model);
run;

data refit2;
	set refit.CONSOLIDATED_REFIT_GINIS;
	applied_Model = catx('_', Score_type, month);
run;

data tu.refit_&month.;
	set refit refit2;
run;

/**************************************************Challenger Models*************************************************************************************/
data basegini1(drop=type);
	set tu.OVERALLGINI_SUMMARY_REBLT_&month.(rename=(score_type = type));
	where type = 'TU_V580_prob';
	month = &month.;
	score_type = 'V580 TU Prob';
	applied_Model = 'V580 TU Model';
run;

data basegini2(drop=type);
	set tu.OVERALLGINI_SUMMARY_REBLT_&prevmonth.(rename=(score_type = type));
	where type = 'TU_V580_prob';
	month = &prevmonth.;
	score_type = 'V580 TU Prob';
	applied_Model = 'V580 TU Model';
run;

data basegini3(drop=type);
	set tu.OVERALLGINI_SUMMARY_REBLT_&prev2month.(rename=(score_type = type));
	where type = 'TU_V580_prob';
	month = &prev2month.;
	score_type = 'V580 TU Prob';
	applied_Model = 'V580 TU Model';
run;

data baseginifinal;
	set basegini1 basegini2 basegini3;
run;

data challenger_&month.;
	set tu.rebuilt_&month. tu.refit_&month. baseginifinal;
run;

proc sql;
	create table CHALLENGER_MODELS_FINAL1 as
	select b.gini as basegini, a.*
	from challenger_&month. a
	left join baseginifinal b
	on a.month = b.month and a.segment = b.segment;
quit;

/**************************************************Calibrated Rebuilt**************************************************************************************/
data crebuilt(drop=model);
	set crebuilt.APPLIED_CALIBRATED_GINIS(rename=(applied_model=model));
	applied_Model = catx('_', 'Calib', Score_type, model);
run;

data crebuilt2;
	set crebuilt.CALIBRATED_REBUILD_GINIS;
	applied_Model = catx('_', 'Calib', Score_type, month);
run;

data tu.rebuilt_calib_&month.;
	set crebuilt crebuilt2;
run;

/**************************************************Calibrated Refit******************************************************************************************/
data crefit(drop=model);
	set crefit.APPLIED_CALIBRATED_REFIT_GINIS(rename=(applied_model=model));
	applied_Model = catx('_', 'Calib', Score_type, model);
run;

data crefit2;
	set crefit.CALIBRATED_REFIT_GINIS;
	applied_Model = catx('_', 'Calib', Score_type, month);
run;

data tu.refit_calib_&month.;
	set crefit crefit2;
run;

/**************************************************Challenger Models*************************************************************************************/
data cbasegini1(drop=type);
	set tu.OVERALLGINI_SUMMARY_REBLT_&month.(rename=(score_type = type));
	where type = 'V582';
	month = &month.;
	score_type = 'V582 TU Prob';
	applied_Model = 'V582 TU Model';
run;

data cbasegini2(drop=type);
	set tu.OVERALLGINI_SUMMARY_REBLT_&prevmonth.(rename=(score_type = type));
	where type = 'V582';
	month = &prevmonth.;
	score_type = 'V582 TU Prob';
	applied_Model = 'V582 TU Model';
run;

data cbasegini3(drop=type);
	set tu.OVERALLGINI_SUMMARY_REBLT_&prev2month.(rename=(score_type = type));
	where type = 'V582';
	month = &prev2month.;
	score_type = 'V582 TU Prob';
	applied_Model = 'V582 TU Model';
run;

data cbaseginifinal;
	set cbasegini1 cbasegini2 cbasegini3;
run;

data calib_challenger_&month.;
	set tu.rebuilt_calib_&month. tu.refit_calib_&month. cbaseginifinal;
run;

proc sql;
	create table CHALLENGER_MODELS_FINAL2 as
	select b.gini as basegini, a.*
	from calib_challenger_&month. a
	left join cbaseginifinal b
	on a.month = b.month and a.segment = b.segment;
quit;

data tu.CHALLENGER_MODELS_&month.;
	set CHALLENGER_MODELS_FINAL1 CHALLENGER_MODELS_FINAL2;
run;

/**************************************************Challenger Models per segment*************************************************************************/
data seg_data;
	set source.TU_SEGMENTATION_RESULT_&month;
run;

proc sql;
create table seg_table_1 as
	select SegmentGini as Gini,	
		"Resegment TU Prob" as Score_type,
		segment_no as segment
	from seg_data
	having OverallGini = max(OverallGini)
	union all
	select max(OverallGini) as Gini, 
		"Resegment TU Prob" as Score_type, 
		0 as segment
	from seg_data;
quit;

proc append data=seg_table_1 base=tu.overallgini_summary_reblt_&month force;
run;

data r1(keep=gini score_type segment);
	set tu.REBUILT_&month.;
	where month = &month and applied_Model=cats('Rebuild_TU_',&prev2month.);
run;

proc append data=r1 base=tu.overallgini_summary_reblt_&month force;
run;

data r2(keep=gini score_type segment);
	set tu.REFIT_&month.;
	where month = &month and applied_Model=cats('Refit_TU_',&prev2month.);
run;

proc append data=r2 base=tu.overallgini_summary_reblt_&month force;
run;

proc sql;
	create table tu.currentmodel_bench_reblt_&month as
		select distinct  a.*, b.gini as Current_gini, (b.gini-a.gini)/a.gini as Relative_change
		from tu.overallgini_summary_reblt_&month a,
		(select segment, gini from tu.overallgini_summary_reblt_&month
		where score_type='TU_V580_prob') b
		where a.segment=b.segment;
quit;

data tu.currentmodel_bench_reblt_&month;
	set tu.currentmodel_bench_reblt_&month;
    format Recommended_Action $500.;
    if relative_change >=-0.10 then do;
   		Recommended_Action = cats("No Action ");
    end;
    else if relative_change <-0.10 and relative_change >-0.15 then do;
    	Recommended_Action =cats("Check Additional Metrics and Establish if a score to Risk Calibration is required");
    end;
	else if relative_change <-0.15 then do;
		Recommended_Action =cats("Check Additional Metrics and Establish if a score to Risk Calibration is required");
	end;
run;

data tu.currentmodel_bench_reblt_&month.;
	set tu.currentmodel_bench_reblt_&month.;
	if score_type='Compuscan_Generic' then score_type ='Comp Generic Score';
	else if score_type='Tu_Generic' then score_type ='TU Generic Score';
	else if score_type='CS_V560_Prob' then score_type ='V560 Comp Prob';
	else if score_type='CS_V570_Prob' then score_type ='V570 Comp Prob';
	else if score_type='V572' then score_type ='V572 Comp Prob';
	else if score_type='V622' then score_type ='V622 Prob';
	else if score_type='TU_V570_prob' then score_type ='V570 TU Prob';
	else if score_type='TU_V580_prob' then score_type ='V580 TU Prob';
	else if score_type='V582' then score_type ='V582 TU Prob';
	else if score_type='V635' then score_type ='V635 Prob';
	else if score_type='V636' then score_type ='V636 Prob';
	else if score_type='V645' then score_type ='V645 Prob';
	else if score_type='V655_2' then score_type ='V655_2 Prob';
	else if score_type='ORM_2' then score_type ='ORM.2';
	else if score_type='V645_adj' then score_type ='V645_adj Prob';
	else if score_type='Rebuild_TU' then score_type ='Rebuild TU Prob';
	else if score_type='Refit_TU' then score_type ='Refit TU Prob';
run;

/********************************************************************************************************************************************************/
/* 												Refresh Power BI after these codes			 															*/
/********************************************************************************************************************************************************/
libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.tu_v5_Gini_relative_reblt; run;
proc sql;
	create table  cred_scr.tu_v5_Gini_relative_reblt(BULKLOAD=YES) as
		select  distinct * 
		from tu.currentmodel_bench_reblt_&month.;
quit;


libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.tu_Gini_summary_reblt; run;
proc sql;
	create table  cred_scr.tu_Gini_summary_reblt(BULKLOAD=YES) as
		select  distinct * 
		from tu.giniperseg_summary_reblt_&month;
quit;


data tu.Summarytable_01_&month. (rename=(month1=month));
set tu.Summarytable_01_&month.;
month1 = put(month, 8.);
if month = . then month1 =' BUILD';
drop month;
run;


libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.TU_v5_Segment_distribution_reblt; run;
proc sql;
	create table  cred_scr.tu_v5_Segment_distribution_reblt(BULKLOAD=YES) as
		select  distinct * 
		from tu.Summarytable_01_&month.;
quit;


libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.TU_distribution_summary_reblt; run;
proc sql;
	create table  cred_scr.TU_distribution_summary_reblt(BULKLOAD=YES) as
		select  distinct * 
		from tu.Tu_Var_Distribution_reblt_&month.;
quit;


libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.TU_V5_Variables_stability_reblt; run;
proc sql;
	create table  cred_scr.TU_V5_Variables_stability_reblt(BULKLOAD=YES) as
		select  distinct * 
		from tu.Variables_stability_reblt_&month.;
quit;


libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.TU_v5_sloperate_reblt; run;
proc sql;
	create table  cred_scr.TU_v5_sloperate_reblt(BULKLOAD=YES) as
		select  distinct * 
		from tu.V5_sloperate_reblt_&month.;
quit;


libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.TU_CSI_Distribution_reblt; run;
proc sql;
	create table  cred_scr.TU_CSI_Distribution_reblt(BULKLOAD=YES) as
		select  distinct * 
		from tu.CSI_Distribution_reblt_&month.;
quit;


libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc delete data =cred_scr.TU_Challanger_models_reblt; run;
proc sql;
	create table  cred_scr.TU_Challanger_models_reblt(BULKLOAD=YES) as
		select  distinct * 
		from tu.CHALLENGER_MODELS_&month.;
quit;

/********************************************************************************************************************************************************/
/* 												Plotting Confidence bands for all the segments and Institution Codes 																*/
/********************************************************************************************************************************************************/
%create_scorecard_variables_list(numberofsegments=1,modeltype=T);
%create_scorecard_variables_list(numberofsegments=2,modeltype=T);
%create_scorecard_variables_list(numberofsegments=3,modeltype=T);
%create_scorecard_variables_list(numberofsegments=4,modeltype=T);
%create_scorecard_variables_list(numberofsegments=5,modeltype=T);

%macro Estimates(numberofsegment=);
	%do seg = 1 %to &numberofsegment;
		proc sql;
			create table _estimates_ as
				select a.*, b.ESTIMATE
				from _estimates_&seg. a left join d&seg.._ESTIMATES_ b
				on UPCASE(a.parameter) = UPCASE(tranwrd(b.Effect,"_W",""));
		quit;
		filename _temp_ temp;
		data _null_;
			set  _estimates_;
			where upcase(parameter) not in ("DECILE","INTERCEPT");
			file _temp_;
			formula = cats(parameter,'_S = ',parameter,'_W*',target,';');
			put formula;
		run;
	%end;
%mend;
%Estimates(numberofsegment=5);

%macro rename();
	%do seg=1 %to 5;
        proc sql;
            create table _estimates_&seg. as
                  select a.variable as parameter, a.new_variable as scorecardvariable
                  from Lookup.Tu_Lookup a, d&seg.._ESTIMATES_ b
                  where cats(upcase(a.variable),"_W") = upcase(b.Effect);
        quit;
	%end;
%mend;
%rename();

%macro VarCheckConfidenceBand1(Dset, Var, PredProb , Outcome , LSize ,y,x,width,height, Heading ) ;
	data final ;
		set &Dset ;
		RRisk = &Outcome * &LSize ;
		LoanSize = &LSize;
		LSizeDSquared =  &LSize *  &LSize ;
		PredProb = input(&PredProb,20.);
		RPredProb = PredProb * Loansize ;
		format bucket $500. ;
		bucket = compress(&var) ;
	run;

	proc summary data = final nway missing ;
		class bucket ;
		var RRisk RPredProb LoanSize LSizeDSquared ;
		output out = Summary (drop = _type_) sum() =  ;
	run;

	data Summary1  ( keep = bucket Predicted LowerBound  UpperBound Actual ) ;
		retain bucket Predicted Actual;
		set Summary ;
		Predicted = RPredProb / Loansize ;
		Actual =   RRisk/ Loansize ;
		SE = SQRT(((Predicted)*(1-Predicted)*LSizeDSquared)/(Loansize*Loansize));
		factor = 3 ;
		UpperBound = Predicted + factor*SE ;
		LowerBound = Predicted - factor*SE ;
		if LowerBound <= 0 then LowerBound = 0 ;
		if UpperBound >= 1 then UpperBound = 1 ;
	run;

	data Summary1 ;
	set Summary1 ;
	Max = max(Predicted ,LowerBound,  UpperBound, Actual) ;
	if Max > 1 then Max = 1 ;
	run;

	proc sql noprint ;
	select sum(round(max(max),0.1),0.1) into : UpperRange separated by ''
	from summary1 ;
	quit;


	goptions reset = all ;
	pattern1 v=s c=white;
	pattern2 v=s c=grayee;
	pattern3 v=s c=graybb;
	pattern4 v=s c=graybb;
	pattern5 v=s c=grayee;

	symbol1 value=none interpol=join color=pink;
	symbol2 value=none interpol=join color=red width=2;

	axis1 label=("&var")
	offset=(0,0);                                                                                                                  
	axis2 label=(angle=90 'Rand Weighted Risk') order = 0 to &UpperRange by 0.1 
	offset=(0,0);

	symbol3 height=1.5 value=dot;
	
	%ODSOff();
	/* Define legend characteristics */
	legend1 order=('Predicted') label=none frame;
	legend2 order=('Actual') label=none frame;
	Title "&Heading";
	proc gplot data=Summary1;   
		plot
		LowerBound*bucket=1
		LowerBound*bucket=1
		Predicted*bucket=1
		UpperBound*bucket=1
		UpperBound*bucket=1
		Predicted*bucket=2
		/ overlay areas=5 vaxis = axis2 haxis = axis1 legend = legend1 ;
		plot2 Actual*bucket / vaxis = axis2 haxis = axis1  legend = legend2;  
	run;
	quit;
	Title ;
	%ODSOn();
%mend;

%macro position(y1,x1);
	%global x y;
	%let x = &x1;
	%let y = &y1;
%mend ;

%macro plotconfidencebands(inputdata=, segment=);
	proc sql noprint;
		select cats(parameter,'_W') into : confidvar separated by ' ' 
		from _estimates_&segment
		;
	quit;
	proc sql noprint;
		select scorecardvariable into : descriptionvar separated by ' ' 
		from _estimates_&segment
		;
	quit;

	%do u = 1 %to %sysfunc(countw(&confidvar));
		%let var = %scan(&confidvar, &u);
		%let dvar = %scan(&descriptionvar,&u);
		
		proc sql noprint;
			create table allout as
				select &var,count(*)
				from &inputdata
				group by &var;
		quit;
		%do i = 1 %to %obscnt(allout);

			data onescore;
				set allout;
				if _n_ = &i;
			run;

			proc sql noprint; select cats("&dvar woe : ",&var) into : score  from onescore ; quit;

			proc sql noprint;
				create table Alloutx as 
					select * from &inputdata
					where &var in (select &var from onescore );
			quit;

		    %let position = %sysfunc( mod(&i, 4) );
		    %if &position = 1 %then %position(0,0);
		    %else %if &position = 2 %then %position(0,4);
		    %else %if &position = 3 %then %position(4.5,0);
		    %else %if &position = 0 %then %position(4.5,4.5);

		    %let width = 4;
		    %let height = 4;
		    %if &position = 1 %then %do;
		          ods pdf startpage = now;
		          ods layout start;
		    %end;

		    ods region y=&y.in x=&x.in width=&width.in height=&height.in;
		    %VarCheckConfidenceBand1(Alloutx, month, V645_adj , target , Principaldebt ,0,0,4,4, &score ) ;

			%let obs = %obscnt(allout);
			%let count = %sysfunc( mod(&obs, 4) );

		    %if &position = 0  %then %do;
		          ods layout end ;
		    %end;

			%if (%obscnt(allout)= &i.) %then %do;
		          ods layout end ;
		    %end;
		%end;
	%end;
%mend;

/*We will only start monitoring ABL and INV 6 months from August  (deployment) as we wait for predictors*/
data BNKABS BNKCAP BNKFNB BNKNED BNKOTH BNKSTD BNKABL BNKINV;
	SET Disbursedbase4;
	if INSTITUTIONCODE = 'BNKABS' then output BNKABS;
	else if INSTITUTIONCODE = 'BNKCAP' then output BNKCAP;
	else if INSTITUTIONCODE = 'BNKFNB' then output BNKFNB;
	else if INSTITUTIONCODE = 'BNKNED' then output BNKNED;
	else if INSTITUTIONCODE = 'BNKOTH' then output BNKOTH;
	else if INSTITUTIONCODE = 'BNKSTD' then output BNKSTD;
	else if INSTITUTIONCODE = 'BNKABL' then output BNKABL;
	else if INSTITUTIONCODE = 'BNKINV' then output BNKINV;
run;

data Disbursedbase4;
set Disbursedbase4;
   old = input(month, 8.);
   drop month;
   rename old=month;
run;

%macro buildreport(seg);
	%macro plotpsi(var=,title1=,dset=,xlbl=);
		%ODSOff();
		Title "&title1";
		proc sgplot data=&dset ;
			where upcase(VariableName) in ("&var")  ;
		       vbar month / response = percent stat = mean group = scores  NOSTATLABEL  BARWIDTH = 0.8 ;  
			   vline month / response = psi y2axis stat = mean group = scores ;
			   vline month / response = marginal_stable y2axis stat = mean group = scores ;
			   vline month / response = Unstable y2axis stat = mean group = scores ;
			yaxis label = 'Percentage'; 
			xaxis label = "&xlbl";
			y2axis label = 'PSI' min = 0 max = 1 ;
		
		run;
		Title;
		%ODSOn();
	%mend;
	proc sql;
	    create table _estimates_ as
	          select a.variable as parameter, a.new_variable as scorecardvariable
	          from Lookup.Tu_Lookup a, d&seg.._ESTIMATES_ b
	          where cats(upcase(a.variable),"_W") = upcase(b.Effect);
	    ;
	quit;

	data _null_ ;
	    set _estimates_  ;
	    rownum= _n_;
	    call symput (compress("X"||rownum),upcase(Parameter));
	    call symput (compress("Y"||rownum),upcase(scorecardvariable));
	    call symput ('NumVars',compress(rownum));
	run;

	%ODSOff();
	%do i = 1 %to &NumVars;
		%let var = &&X&i..;
		ods pdf startpage = now;
		ods layout start;
		ods region x = 1in y = 0in;
		ods text = "Performance Monitoring Segment&seg.: &&Y&i.." ;

		ods region y=0.5in x=0in width=4.5in height=4in;

		ods graphics / reset attrpriority=color;
		Title "Gini over Time";
		footnote "&&Y&i..";
		proc sort data=Ginis&seg;
		      by Month;
		run;
		data ginitabletemp;;
		      set Ginis&seg;
		      where upcase(VarName) in ("OVERALLSEGMENT","&var"); 
		      varname = tranwrd(varname,upcase("&var"),upcase("&&Y&i.."));
		run;
		proc sgplot data= ginitabletemp;
		      series x=Month y=Gini / group= Varname  lineattrs= (pattern=solid Thickness = 2  ) ;
		      yaxis min = 0  grid offsetmin=.05 offsetmax=.05;
		      xaxis  grid  ;
		      keylegend / Title = '' ;
		run;
		Title;
		footnote;

		ods region y=0.5in x=5in width=4.5in height=4in;

		Title "Bad Rate Slope";
		footnote "Latest FirstDueMonth with full outcome"; 
		proc sgplot data= Variables_trendreport_reblt_seg&seg;
		      where  upcase(VarName) in ("&var._S") and  segment = &seg ;
		      vbar Bin / response = volume stat = percent  NOSTATLABEL    FILLATTRS=(color = VLIGB ) ;
		      vline Bin / response=badrate y2axis stat = mean NOSTATLABEL lineattrs= (pattern=solid Thickness = 2 color = gray)   ;
		      y2axis min = 0  grid;
		      yaxis min = 0  grid ;
			  xaxis label ='Scores';
			  keylegend / Title = '' ;
		run;
		Title;
		footnote;

		ods region y=4.5in x=0in width=4.5in height=4in;

		%plotpsi(var=&var,title1=Application Distribution,dset=summarytable_&seg) ;

		ods region y=4.5in x=5in width=4.5in height=4in;

		ods graphics / reset attrpriority=color;
		Title "Trends Over Time";
		proc sort data=Variables_trendreport_reblt_seg&seg;
		      by Month;
		run;
		proc sgplot data= Variables_trendreport_reblt_seg&seg;
		      where upcase(VarName) in ("&var._S") and segment = &seg; 
		      series x=Month y=BadRate / group= BIN  lineattrs= (pattern=solid Thickness = 2  );
		      xaxis grid ;
		      yaxis min = 0  grid offsetmin=.05 offsetmax=.05;
			  keylegend / Title = '' ;
		run;
		Title;
		ods LAYOUT END ;
	%ODSOn();
	%end;
%mend;

data WORK._ESTIMATES_4;
	set WORK._ESTIMATES_4;
	if parameter = 'EQ0015PL' then delete;
run;

data WORK._ESTIMATES_5;
	set WORK._ESTIMATES_5;
	if parameter = 'EQ0015PL' then delete;
run;

options nodate nonumber;
Title ; 
TITLE1; TITLE2;
Footnote;
Footnote1;
Footnote2;
options orientation=landscape;
ods pdf body = "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\TU Performance Monitoring\reports\V5.80\V580 monitoring pack &month..pdf"  ;

	ods pdf startpage = now;
	ods layout start;
	ods region y=0in x=0in width=4in height=4in;
	%VarCheckConfidenceBand1(Disbursedbase4,month, V655_2 , target , Principaldebt ,0,0,4,4, V580 TU Overall Model ) ;
	ods region y=0in x=4.5in width=4in height=4in;
	%VarCheckConfidenceBand1(Disbursedbase4, tu_seg, V655_2 , target , Principaldebt ,0,0,4,4, Segments ) ;
	ods region y=4.5in x=0in width=4in height=4in;
	%VarCheckConfidenceBand1(Disbursedbase4, Decile_b, V655_2 , target , Principaldebt ,0,0,4,4, Overall Decile ) ;
	ods region y=4.5in x=4.5in width=4in height=4in;
	%VarCheckConfidenceBand1(Disbursedbase4, V655_2_RiskGroup, V655_2 , target , Principaldebt ,0,0,4,4, Risk Group ) ;
	ods layout end ;
	ods pdf startpage = now;
	ods layout start;
	ods region y=0in x=0in width=4in height=4in;
	%VarCheckConfidenceBand1(Disbursedbase4, INSTITUTIONCODE , V655_2 , target , Principaldebt ,0,0,4,4, INSTITUTION CODE) ;
	ods region y=0in x=4.5in width=4in height=4in;
	%VarCheckConfidenceBand1(BNKSTD, month, V655_2 , target , Principaldebt ,0,4.5,4,4, BNKSTD ) ;
	ods region y=4.5in x=0in width=4in height=4in;
	%VarCheckConfidenceBand1(BNKFNB, month, V655_2 , target , Principaldebt ,4.5,0,4,4, BNKFNB ) ;
	ods region y=4.5in x=4.5in width=4in height=4in;
	%VarCheckConfidenceBand1(BNKNED, month, V655_2 , target , Principaldebt ,4.5,4.5,4,4, BNKNED ) ;
	ods layout end ;
	ods pdf startpage = now;
	ods layout start;
	ods region y=0in x=0in width=4in height=4in;
	%VarCheckConfidenceBand1(BNKABS, month , V655_2 , target , Principaldebt ,0,0,4,4, BNKABS) ;
	ods region y=0in x=4.5in width=4in height=4in;
	%VarCheckConfidenceBand1(BNKCAP, month, V655_2 , target , Principaldebt ,0,4.5,4,4, BNKCAP ) ;
	ods region y=4.5in x=0in width=4in height=4in;
	%VarCheckConfidenceBand1(BNKABL, month, V655_2 , target , Principaldebt ,0,0,4,4, BNKABL ) ;
	ods region y=4.5in x=4.5in width=4in height=4in;
	%VarCheckConfidenceBand1(BNKINV, month, V655_2 , target , Principaldebt ,0,0,4,4, BNKINV ) ;
	ods layout end ;
	ods pdf startpage = now;
	ods layout start;
	ods region y=0in x=0in width=4in height=4in;
	%VarCheckConfidenceBand1(BNKOTH, month, V655_2 , target , Principaldebt ,0,0,4,4, BNKOTH ) ;
	ods region y=0in x=4.5in width=4in height=4in;
	%VarCheckConfidenceBand1(disb1, month , V655_2 , target , Principaldebt ,0,0,4,4, SEGMENT 1 ) ;
	ods region y=4.5in x=0in width=4in height=4in;
	%VarCheckConfidenceBand1(disb2, month, V655_2 , target , Principaldebt ,0,4.5,4,4, SEGMENT 2 ) ;
	ods region y=4.5in x=4.5in width=4in height=4in;
	%VarCheckConfidenceBand1(disb3, month, V655_2 , target , Principaldebt ,4.5,0,4,4, SEGMENT 3) ;
	ods layout end ;
	ods pdf startpage = now;
	ods layout start;
	ods region y=0in x=0in width=4in height=4in;
	%VarCheckConfidenceBand1(disb4, month, V655_2 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 4 ) ;
	ods region y=0in x=4.5in width=4in height=4in;
	%VarCheckConfidenceBand1(disb5, month, V655_2 , target , Principaldebt ,0,0,4,4, SEGMENT 5 ) ;
	ods region y=4.5in x=0in width=4in height=4in;
	%VarCheckConfidenceBand1(disb1, decile_b , V655_2 , target , Principaldebt ,0,0,4,4, SEGMENT 1: Decile ) ;
	ods region y=4.5in x=4.5in width=4in height=4in;
	%VarCheckConfidenceBand1(disb2, decile_b, V655_2 , target , Principaldebt ,0,4.5,4,4, SEGMENT 2: Decile ) ;
	ods layout end ;
	ods pdf startpage = now;
	ods layout start;
	ods region y=0in x=0in width=4in height=4in;
	%VarCheckConfidenceBand1(disb3, decile_b, V655_2 , target , Principaldebt ,4.5,0,4,4, SEGMENT 3: Decile) ;
	ods region y=0in x=4.5in width=4in height=4in;
	%VarCheckConfidenceBand1(disb4, decile_b, V655_2 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 4: Decile ) ;
	ods region y=4.5in x=0in width=4in height=4in;
	%VarCheckConfidenceBand1(disb5, decile_b, V655_2 , target , Principaldebt ,0,0,4,4, SEGMENT 5: Decile ) ;
	ods layout end ;
	%buildreport(1);
	%plotconfidencebands(inputdata=disb1, segment=1);
	%buildreport(2);
	%plotconfidencebands(inputdata=disb2, segment=2);
	%buildreport(3);
	%plotconfidencebands(inputdata=disb3, segment=3);
	%buildreport(4);
	%plotconfidencebands(inputdata=disb4, segment=4);
	%buildreport(5);
	%plotconfidencebands(inputdata=disb5, segment=5);
ods pdf close;
	
*filename macros2 'H:\Process_Automation\macros';
*options sasautos = (sasautos  macros2);
