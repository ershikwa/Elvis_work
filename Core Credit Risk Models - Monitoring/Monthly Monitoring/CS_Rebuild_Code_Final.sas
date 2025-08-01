/*%include "\\neptune\credit$\AA_GROUP CREDIT\Scoring\SAS Macros\Upload_APS.sas";*/
%include "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\macros\Calc_Gini.sas";
%let odbc = MPWAPS;
libname lookup "\\mpwsas64\Core_Credit_Risk_Models\V5\Segmentation Models For Compuscan\lookup";
libname calib "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\Compuscan Models\calibration\calibration_new\Rebuild_calib";
libname comp "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring";
libname comp1 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\V570";
%macro ODSOff(); /* Call prior to BY-group processing */
ods graphics off;
ods exclude all;
ods noresults;
%mend;
 
%macro ODSOn(); /* Call after BY-group processing */
ods graphics on;
ods exclude none;
ods results;
%mend;

/* To change -2 to -1 after testing */
data _null_;
	lastmonthday = put(intnx('month', today(), -1, 'same'), yymmddn8.);
	lastmonth = substr(lastmonthday, 1, length(lastmonthday)-2);
	call symput('previous_month', compress(lastmonth));
run;

data disbursedbase_&previous_month;
set comp1.disbursedbase_&previous_month;
run;


options noxwait compress=binary;

%let directory = "\\mpwsas64\Core_Credit_Risk_Models\V5\CS_Rebuilds\Rebuild_&previous_month.";
%let main_dataset = "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\V570\disbursedbase_&previous_month..sas7bdat";
%let number_of_segments = 5;

%macro directory_exists(directory_);
	%let rc = %sysfunc(filename(fileref, &directory_.));
	%if %sysfunc(fexist(&fileref)) %then %do;
		%let result = 1;
	%end;
	%else %do;
		%let result = 0;
	%end;
	&result
%mend;

%macro import_data(data_directory, dataset_path);
	%let dir_cond = %directory_exists(&data_directory);
	%if (&dir_cond = 0) %then %do;
		%let file_cond = %directory_exists(&dataset_path);
		%if (&file_cond = 1) %then %do;
			%createdirectory(directory=%qsysfunc(dequote(&data_directory)));
			%sysexec copy &dataset_path &data_directory ;
		%end;
		%else %do;
			%raiseerror("ERR: Could not find dataset!");
		%end;
	%end;
	libname data &data_directory;
%mend;

/* Start rebuilding segments */
/*options noxwait;*/
%import_data(&directory, &main_dataset);
%createdirectory(directory=\\mpwsas64\Core_Credit_Risk_Models\V5\CS_Rebuilds\Rebuild_&previous_month.);
options mprint mlogic symbolgen;
%macro loop();
	%ODSOff;
	%do i = 1 %to 5;
		*proc delete data = &lib..flag_&i; 

		%let programtocopy=\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\CS_Rebuild\CS_Rebuild_seg.sas;
		%let segmentno =  \\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\CS_Rebuild\CS_Rebuild_seg&i..sas;
		%let programpath = \\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\CS_Rebuild;

		data _null_;
			call symput("copy",cats("'", 'copy "',"&programtocopy",'" "',"&segmentno",'"', "'"));
		run;
		%put &copy;
			x &copy;

			options xwait noxsync;
		%let start_sas = 'E:\SASHome\SASFoundation\9.4\sas.exe';
		data _null_;
			call symput('sas_program',cats("'","&programpath.\CS_Rebuild_seg&i",".sas'"));
			call symput('sas_log', cats("'","&programpath.\CS_Rebuild_seg&i",".log'"));
			call symput('sas_print', cats("'","&programpath.\CS_Rebuild_seg&i",".lst'"));
			call symput("sysparm_v", cats("'","&i","'"));
		run;
		x " &start_sas -sysin &sas_program -nosplash -log &sas_log -print &sas_print -nostatuswin  -noerrorabend -noterminal -noicon  -nosyntaxcheck -sysparm &sysparm_v ";	
		
		data _null_;
			call sleep(10000);
		run;
	%end;

	%let tableexist=0;

	libname data "\\mpwsas64\Core_Credit_Risk_Models\V5\CS_Rebuilds\Rebuild_&previous_month";
	 
	%do %while (&tableexist = 0);
		%let countflag = 0;
		%do h = 1 %to 5;
			%if %sysfunc(exist(data.ginitable&h.)) %then 
			%let countflag = %eval(&countflag+1);
		%end;
		%if &countflag=5 %then 
			%let tableexist = 1;
		%else 
			%let tableexist = 0; 

		data _null_;
			call sleep(10000);
		run;
	%end;
	%ODSOn;
%mend;

%loop;

/* Combine segment scores */
data data.build_scorecomb_Rebuild;
	set data.build_score1 data.build_score2 data.build_score3 data.build_score4 data.build_score5;
run;

/* Calculate overall gini */
%Calc_Gini (Final_Score, data.build_scorecomb_Rebuild, target, work.GINITABLE);

data data.GINITABLE_Overall_Rebuild (keep=Gini segment);
	set GINITABLE;
	segment = 0;
	Rebuild_Compuscan = Gini;
run;

data data.Rebuild_Gini_&previous_month.;
	set data.ginitable_overall_rebuild data.ginitable1 data.ginitable2 data.ginitable3 data.ginitable4 data.ginitable5;
	Score_type = "Rebuild_Compuscan";
run;

/************************** Calibrations ****************************/

data scores (keep=uniqueid  V5_Rebuild);
	set data.build_scorecomb_rebuild;
	rename Final_Score = V5_Rebuild;
run;

proc sql;
    create table use as
        select a.V5_Rebuild, b.*
        from scores a inner join disbursedbase_&previous_month. b
        on a.uniqueid = b.uniqueid ;
quit;

proc sql;
	create table Calibration_Scored as
		select *, ((a.Principaldebt/sum(a.Principaldebt)) * count(a.tranappnumber)) as weight
		from use A ;
quit;

proc sort data = Calibration_Scored;
	by tranappnumber uniqueid;
run;

proc sort data = Calibration_Scored nodupkey;
	by tranappnumber ;
run;


/******************** Segment Calibration *********************/
/*Start of calibration - Creation of a and c for segment split*/

%macro Loopthroughcal(Dset,comp_seg=,name=,prob=, Segmentation=, weight=);
      proc nlmixed data=&Dset (where = (&Segmentation = &comp_seg))  ; 
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
            &Name = &comp_seg ;
      run;
      proc append base  = Parameters_&name data = parameters force ;
      quit;

%mend;

%Loopthroughcal(Dset=Calibration_Scored,comp_seg=1,name = Rebuild_Seg,prob=V5_Rebuild,Segmentation=comp_seg,weight=weight);
%Loopthroughcal(Dset=Calibration_Scored,comp_seg=2,name = Rebuild_Seg,prob=V5_Rebuild,Segmentation=comp_seg,weight=weight);
%Loopthroughcal(Dset=Calibration_Scored,comp_seg=3,name = Rebuild_Seg,prob=V5_Rebuild,Segmentation=comp_seg,weight=weight);
%Loopthroughcal(Dset=Calibration_Scored,comp_seg=4,name = Rebuild_Seg,prob=V5_Rebuild,Segmentation=comp_seg,weight=weight);
%Loopthroughcal(Dset=Calibration_Scored,comp_seg=5,name = Rebuild_Seg,prob=V5_Rebuild,Segmentation=comp_seg,weight=weight);

data calib.Parameters_Rebuild_Seg_&previous_month.;
	set Parameters_Rebuild_Seg;
run;

proc sql;
      create table Rebuild_Calibration_seg
      as select 1/(1+(V5_Rebuild/(1-V5_Rebuild))**(-1*(c.a))*exp(c.c)) as V5_Rebuild_2, a.*
      from  Calibration_Scored a
      left join calib.PARAMETERS_REBUILD_SEG_&previous_month. c
      on a.comp_seg=c.Rebuild_Seg;
quit;

proc freq data = Rebuild_Calibration_seg;
	tables comp_seg;
run;

proc sql;
      create table bank as
      select distinct institutioncode
      from Rebuild_Calibration_seg;
quit;

/*********************** Bank Calibration**********************/
/*Start of calibration - Creation of a and c for segment split*/

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
            set parameters ; length &name $100.; 
            Model = "&Name";
            &Name = &seg ;
      run;
      proc append base  = Parameters_&name data = parameters force ;
      quit;
%mend;

data Rebuild_Calibration_seg;
	set Rebuild_Calibration_seg;
	if compress(INSTITUTIONCODE) not in ('BNKABL', 'BNKABS', 'BNKCAP', 'BNKFNB', 'BNKNED', 'BNKSTD', 'BNKOTH') then INSTITUTIONCODE ='BNKOTH';
run;   

%Loopthroughcal(Dset=Rebuild_Calibration_seg,seg='BNKABS' ,prob=V5_Rebuild_2,name = Reb_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
%Loopthroughcal(Dset=Rebuild_Calibration_seg,seg='BNKCAP' ,prob=V5_Rebuild_2,name = Reb_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
%Loopthroughcal(Dset=Rebuild_Calibration_seg,seg='BNKFNB' ,prob=V5_Rebuild_2,name = Reb_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
%Loopthroughcal(Dset=Rebuild_Calibration_seg,seg='BNKNED' ,prob=V5_Rebuild_2,name = Reb_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
%Loopthroughcal(Dset=Rebuild_Calibration_seg,seg='BNKSTD' ,prob=V5_Rebuild_2,name = Reb_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
%Loopthroughcal(Dset=Rebuild_Calibration_seg,seg='BNKOTH' ,prob=V5_Rebuild_2,name = Reb_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
%Loopthroughcal(Dset=Rebuild_Calibration_seg,seg='BNKABL' ,prob=V5_Rebuild_2,name = Reb_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
*%Loopthroughcal(Dset=Rebuild_Calibration_seg,seg='BNKINV' ,prob=V5_Rebuild_2,name = Reb_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);

data calib.Parameters_Reb_INSTITCODE_&previous_month.;
	set Parameters_Reb_INSTITUTIONCODE;
run;

proc sql;
	create table Rebuild_Calibration_bank
		as select
		1/(1+(V5_Rebuild_2/(1-V5_Rebuild_2))**(-1*(c.a))*exp(c.c)) as V5_Rebuild_3,  a.*
		from  Rebuild_Calibration_seg a
		left join calib.PARAMETERS_REB_INSTITCODE_&previous_month. c
		on a.Institutioncode=c.Reb_INSTITUTIONCODE;
quit;

%Calc_Gini (V5_Rebuild_3, Rebuild_Calibration_bank, target, GiniTable);

data Gini_Rebuild_Calib_Bank (keep=segment Score_type Gini);
	set GiniTable;
	Score_type = "Rebuild_Calib_Bank";
	segment = 0;
run;

%macro Create_gini_tables(Predicted_col, Dset, numberofsegment, Target_Variable, Gini_output);
    %do seg = 1 %to &numberofsegment;
        data Reb_Calib_bank_seg&seg;
            set &Dset;
            if comp_seg = &seg;
        run;

        %Calc_Gini(&Predicted_col, Reb_Calib_bank_seg&seg, &Target_Variable, gini_seg&seg);

        data gini_seg&seg (keep=segment Score_type Gini);
            set gini_seg&seg;
			Score_type = "Rebuild_Calib_Bank";
            segment = &seg;
        run;

        proc append base=&Gini_output data=gini_seg&seg force; run;
    %end;
%mend;

%Create_gini_tables(V5_Rebuild_3, Rebuild_Calibration_bank, 5, target, Gini_Rebuild_Calib_Bank)

data comp.GiniComparison_Rebuild_Calib;
	set Gini_Rebuild_Calib_Bank;
run;

data Gini_Calib;
	set Gini_Rebuild_Calib_Bank;
	Score_type = "Rebuild_Calib_Comp";
run;

data data.Rebuild_Gini_&previous_month.;
	set data.Rebuild_Gini_&previous_month. Gini_Calib;
run;

/*filename macros2 'H:\Process_Automation\macros';*/
/*options sasautos = (sasautos  macros2);*/
