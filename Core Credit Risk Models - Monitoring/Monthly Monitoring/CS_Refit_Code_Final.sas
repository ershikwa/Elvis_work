libname lookup "\\mpwsas64\Core_Credit_Risk_Models\V5\Segmentation Models For Compuscan\lookup";
libname calib "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\Compuscan Models\calibration\calibration_new\Refit_calib";
libname comp "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring";
%let odbc = MPWAPS;
/* To change -2 to -1 after testing */
data _null_;
	lastmonthday = put(intnx('month', today(), -1, 'same'), yymmddn8.);
	lastmonth = substr(lastmonthday, 1, length(lastmonthday)-2);
	call symput('previous_month', compress(lastmonth));
run;

%let directory = "\\mpwsas64\Core_Credit_Risk_Models\V5\CS_Refit_V560\Refit_&previous_month.";
%let main_dataset = "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\disbursedbase_&previous_month..sas7bdat";
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

/* create the current working directory */
%macro first_import_data(data_directory, dataset_path);
	%let dir_cond = %directory_exists(&data_directory);
	%if (&dir_cond = 0) %then %do;
		%let file_cond = %directory_exists(&dataset_path);
		%if (&file_cond = 1) %then %do;
			x mkdir &data_directory;
			x copy &dataset_path &data_directory;
		%end;
		%else %do;
			%put ERROR: Could not find dataset;
		%end;
	%end;
	libname data &data_directory;
%mend;
%first_import_data(&directory, &main_dataset);

options noxwait;
%macro loop();
	%do i = 1 %to 5;
		%let programtocopy=\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\CS_Refit\CS_Refit_seg.sas;
		%let segmentno =  \\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\CS_Refit\CS_Refit_seg&i..sas;
		%let programpath = \\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\CS_Refit;

		data _null_;
			call symput("copy",cats("'", 'copy "',"&programtocopy",'" "',"&segmentno",'"', "'"));
		run;
		x &copy;

		options noxwait xsync;
		%let start_sas = 'E:\SASHome\SASFoundation\9.4\sas.exe';
		data _null_;
			call symput('sas_program',cats("'","&programpath.\CS_Refit_seg&i",".sas'"));
			call symput('sas_log', cats("'","&programpath.\CS_Refit_seg&i",".log'"));
			call symput('sas_print', cats("'","&programpath.\CS_Refit_seg&i",".lst'"));
			call symput("sysparm_v", cats("'","&i","'"));
		run;
		x  " &start_sas -sysin &sas_program -nosplash -log &sas_log -print &sas_print -nostatuswin  -noerrorabend -noterminal -noicon  -nosyntaxcheck -sysparm &sysparm_v ";	
		
		data _null_;
			call sleep(10000);
		run;
	%end;

	%let tableexist=0;
	libname data "\\mpwsas64\Core_Credit_Risk_Models\V5\CS_Refit_V560\Refit_&previous_month";
	 
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
%mend;
%loop;

/* Combine segment scores */
data data.build_scorecomb_Refit;
	set data.build_score1 data.build_score2 data.build_score3 data.build_score4 data.build_score5;
run;

/* Calculate overall gini */
%Calc_Gini (Final_Score, data.build_scorecomb_Refit, target, work.GINITABLE);

data data.GINITABLE_Overall_Refit (keep=Gini segment);
	set GINITABLE;
	segment = 0;
	Refit_Compuscan = Gini;
run;

data data.Refit_Gini_&previous_month.;
	set data.GINITABLE_Overall_Refit data.GINITABLE1 data.GINITABLE2 data.GINITABLE3 data.GINITABLE4 data.GINITABLE5;
	Score_type = "Refit_Compuscan";
run;



/************************** Calibrations ****************************/
data scores (keep=uniqueid  V5_Refit);
	set data.build_scorecomb_refit;
	rename Final_Score = V5_Refit;
run;

proc sql;
    create table use as
        select a.V5_Refit, b.*
        from scores a inner join data.disbursedbase_&previous_month. b
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

%Loopthroughcal(Dset=Calibration_Scored,comp_seg=1,name = Refit_Seg,prob=V5_Refit,Segmentation=comp_seg,weight=weight);
%Loopthroughcal(Dset=Calibration_Scored,comp_seg=2,name = Refit_Seg,prob=V5_Refit,Segmentation=comp_seg,weight=weight);
%Loopthroughcal(Dset=Calibration_Scored,comp_seg=3,name = Refit_Seg,prob=V5_Refit,Segmentation=comp_seg,weight=weight);
%Loopthroughcal(Dset=Calibration_Scored,comp_seg=4,name = Refit_Seg,prob=V5_Refit,Segmentation=comp_seg,weight=weight);
%Loopthroughcal(Dset=Calibration_Scored,comp_seg=5,name = Refit_Seg,prob=V5_Refit,Segmentation=comp_seg,weight=weight);

data calib.Parameters_Refit_Seg_&previous_month.;
	set Parameters_Refit_Seg;
run;

proc sql;
      create table Refit_Calibration_seg
      as select 1/(1+(V5_Refit/(1-V5_Refit))**(-1*(c.a))*exp(c.c)) as V5_Refit_2, a.*
      from  Calibration_Scored a
      left join calib.PARAMETERS_REFIT_SEG_&previous_month. c
      on a.comp_seg = c.Refit_Seg;
quit;

proc freq data = Refit_Calibration_seg;
	tables comp_seg;
run;

proc sql;
      create table bank as
      select distinct institutioncode
      from Refit_Calibration_seg;
quit;


/********************* Bank Calibration ********************/
/*Start of calibration - Creation of a and c for bank split*/

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

data Refit_Calibration_seg;
	set Refit_Calibration_seg;
	if compress(INSTITUTIONCODE) not in ('BNKABL', 'BNKABS', 'BNKCAP', 'BNKFNB', 'BNKNED', 'BNKSTD', 'BNKOTH') then INSTITUTIONCODE ='BNKOTH';
run;   
 
%Loopthroughcal(Dset=Refit_Calibration_seg,seg='BNKABS' ,prob=V5_Refit_2,name = Ref_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
%Loopthroughcal(Dset=Refit_Calibration_seg,seg='BNKCAP' ,prob=V5_Refit_2,name = Ref_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
%Loopthroughcal(Dset=Refit_Calibration_seg,seg='BNKFNB' ,prob=V5_Refit_2,name = Ref_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
%Loopthroughcal(Dset=Refit_Calibration_seg,seg='BNKNED' ,prob=V5_Refit_2,name = Ref_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
%Loopthroughcal(Dset=Refit_Calibration_seg,seg='BNKSTD' ,prob=V5_Refit_2,name = Ref_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
%Loopthroughcal(Dset=Refit_Calibration_seg,seg='BNKOTH' ,prob=V5_Refit_2,name = Ref_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
%Loopthroughcal(Dset=Refit_Calibration_seg,seg='BNKABL' ,prob=V5_Refit_2,name = Ref_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);
*%Loopthroughcal(Dset=Refit_Calibration_seg,seg='BNKINV' ,prob=V5_Refit_2,name = Ref_INSTITUTIONCODE,Segmentation=INSTITUTIONCODE,weight=weight);

data calib.Parameters_Ref_INSTITCODE_&previous_month.;
	set Parameters_Ref_INSTITUTIONCODE;
run;

proc sql;
	create table Refit_Calibration_bank
		as select
		1/(1+(V5_Refit_2/(1-V5_Refit_2))**(-1*(c.a))*exp(c.c)) as V5_Refit_3,  a.*
		from  Refit_Calibration_seg a
		left join calib.PARAMETERS_REF_INSTITCODE_&previous_month. c
		on a.Institutioncode=c.Ref_INSTITUTIONCODE;
quit;

%Calc_Gini (V5_Refit_3, Refit_Calibration_bank, target, GiniTable);

data Gini_Refit_Calib_Bank (keep=segment Score_type Gini);
	set GiniTable;
	Score_type = "Refit_Calib_Bank";
	segment = 0;
run;

%macro Create_gini_tables(Predicted_col, Dset, numberofsegment, Target_Variable, Gini_output);
    %do seg = 1 %to &numberofsegment;
        data Ref_Calib_bank_seg&seg;
            set &Dset;
            if comp_seg = &seg;
        run;

        %Calc_Gini(&Predicted_col, Ref_Calib_bank_seg&seg, &Target_Variable, gini_seg&seg);

        data gini_seg&seg (keep=segment Score_type Gini);
            set gini_seg&seg;
			Score_type = "Refit_Calib_Bank";
            segment = &seg;
        run;

        proc append base=&Gini_output data=gini_seg&seg force; run;
    %end;
%mend;

%Create_gini_tables(V5_Refit_3, Refit_Calibration_bank, 5, target, Gini_Refit_Calib_Bank)

data comp.GiniComparison_Refit_Calib;
	set Gini_Refit_Calib_Bank;
run;

data Gini_Calib;
	set Gini_Refit_Calib_Bank;
	Score_type = "Refit_Calib_Comp";
run;

data data.Refit_Gini_&previous_month.;
	set data.Refit_Gini_&previous_month. Gini_Calib;
run;

filename macros2 'H:\Process_Automation\macros';
options sasautos = (sasautos  macros2);