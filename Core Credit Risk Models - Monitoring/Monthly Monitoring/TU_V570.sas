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


/*%include "\\neptune\sasa$\SAS_Automation\SAS_Autoexec\autoexec2.sas";*/

OPTIONS NOSYNTAXCHECK ;
options compress = yes;

libname tu '\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets';
libname decile "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\SAS Decile Tables\TU V570";
libname Lookup '\\mpwsas64\Core_Credit_Risk_Models\V6\MetaData';
libname V6 '\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\V6 dataset';
libname d1 "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\TU Models\v570\Segment1\Development Data";
libname d2 "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\TU Models\v570\Segment2\Development Data";
libname d3 "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\TU Models\v570\Segment3\Development Data";
libname d4 "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\TU Models\v570\Segment4\Development Data";
libname d5 "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\TU Models\v570\Segment5\Development Data";
libname seg1 "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\TU Models\v570\Segment 1\Scored Output";
libname seg2 "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\TU Models\v570\Segment 2\Scored Output";
libname seg3 "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\TU Models\v570\Segment 3\Scored Output";
libname seg4 "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\TU Models\v570\Segment 4\Scored Output";
libname seg5 "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\TU Models\v570\Segment 5\Scored Output";
libname results "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V632 Model\V623\Data";
libname rebuilt "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\scores";
libname refit "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\refit_scores";
libname crebuilt "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\calibrated_scores";
libname crefit "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\calibrated_refit_scores";
libname source "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\TU Performance Monitoring\data";
libname Rejects "\\Neptune\SASA$\V5\New_Rejects";

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
				
		from 	DEV_DataDistillery_General.dbo.TU_applicationbase
		where appmonth>=&startdate.
	) ;
	disconnect from odbc ;
quit;

proc sql stimer;
	connect to ODBC (dsn=&odbc);
	create table DISBURSEDBASE4REBLT_&month as 
	select * from connection to odbc ( 
		select *
				
		from 	DEV_DataDistillery_General.dbo.DISBURSEDBASE4REBLT_&month
		
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

	proc sql stimer;
		connect to odbc (dsn=mpwaps);
		select * from connection to odbc (
			drop table DEV_DataDistillery_General.dbo.final_model
		);
		disconnect from odbc;
	quit;
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
					C.Final_score_1, C.FirstDueMonth as PredictorMonth
			from (select tranappnumber, uniqueid from DEV_DataDistillery_General.dbo.final_model where(isnumeric(tranappnumber) =1)) A 
			inner join PRD_DataDistillery_data.dbo.Disbursement_Info E
			on a.tranappnumber = e.loanid
			left join PRD_DataDistillery_data.dbo.JS_Outcome_base_final B 
			on a.tranappnumber = B.loanid 
			left join CREDITPROFITABILITY.dbo.ELR_LOANESTIMATES_3_9_CALIB C 
			on b.loanid = c.loanid 
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
	SET Disbursedbase4;




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
%macro create_scorecard_variables_list(numberofsegments=,modeltype=);
	%if &modelType = T %then %do;
		proc sql;
	        create table _estimates_&numberofsegments. as
	              select a.variable as parameter,  upcase(a.new_variable) as scorecardvariable
	              from Lookup.Tu_Lookup a, d&numberofsegments..WGHT_TRANS b
	              where cats(upcase(a.variable),"_W") = upcase(b._NAME_);
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
	set Applicationbase_&month;
	if input(ApplicationDate , yymmdd10.) >= intnx('month',today(),-12,'begin');
run;
proc sql;
	create table NewAppbase as
		select *
		from applicationsbase
		where baseloanid in (select baseloanid from DisbursedBase where Principaldebt ne .);
quit;


data NewAppbase;
	set NewAppbase(drop = month);
	month= appmonth;
run;

data build_new;
	set NewAppbase;
	if appmonth >= &build_start and appmonth <= &build_end; 
run;

%macro datapreparation(applicationbase=,numberofsegment=);
	%do seg = 1 %to &numberofsegment;
		proc sql;
			create table _estimates_ as
				select a.*, b.target 
				from _estimates_&seg. a left join d&seg..Wght_trans b
				on UPCASE(a.parameter) = UPCASE(tranwrd(b._NAME_,"_W",""));
		quit;

		filename _temp_ temp;

		data _null_;
			set  _estimates_;
			where upcase(parameter) not in ("DECILE","INTERCEPT");
			file _temp_;
			formula = cats(parameter,'_S = ',parameter,'_W*',target,';');
			put formula;
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
%datapreparation(applicationbase=NewAppbase,numberofsegment=5);

%macro rename();
	%do seg=1 %to 5;
        proc sql;
            create table _estimates_&seg. as
                  select a.variable as parameter, a.new_variable as scorecardvariable
                  from Lookup.Tu_Lookup a, d&seg..Wght_trans b
                  where cats(upcase(a.variable),"_W") = upcase(b._name_);
        quit;
	%end;
%mend;
%rename();

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
	set NewAppbase(drop = month);
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
%macro PSIRenaming (seg);
	proc delete data=all_summarytable; run;
	%do i= 1 %to &seg;
		Proc sql;
			create table PSI_&i. as 
				select a.* ,b.scorecardvariable
				from  summarytable_&i. a 
				left join _estimates_&i. b
				on a.variablename = b.parameter;
		quit;
		proc append base=all_summarytable data=psi_&i. force; run;
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
					on  upcase(a.variablename) = (b.Parameter);
			quit;
		%end;
		proc append base = &outdataset  data=_tempPSI_ force; run;
	%end;
%mend;
%appendPSI(outdataset =Tu.Tu_Var_Distribution_Reblt_&month.); 
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

data tu.v5_sloperate_reblt_&month;
set v5_sloperate_reblt_&month;
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
			where month ne " BUILD"
			order by month desc;
	quit;
	data last3month;
		set last3month(obs=3);
	run;	
	proc sql;
		create table last3monthdata as
			select  seg,a.month,variablename, psi
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