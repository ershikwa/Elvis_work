/*%include "\\Neptune\SASA$\SAS_Automation\SAS_Autoexec\autoexec7.sas";*/
options noxwait compress=binary;
%let odbc = MPWAPS;
data _null_;
	 call symput("runmonth",put(intnx('month',today(),-1),yymmn6.));
run;
%put &runmonth;

%let sourcedata = \\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\V570;
%let currentworkingdirectory = \\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\Compuscan Models\V6_Scorecard_Monitoring\Compuscan\Segmentation;
%let remotepath = &currentworkingdirectory;
%let programpath = \\mpwsas64\Core_Credit_Risk_Models\SAS_Automation\Partition_Code;

%sysexec mkdir "&remotepath\&runmonth\buckets";
%sysexec mkdir "&remotepath\&runmonth\data";
%sysexec mkdir "&remotepath\&runmonth\logs";

/*******************************************************************************************************************/
/*                                			assigning libraries	    			                                   */
libname data "&remotepath\&runmonth\data";
libname tree "&remotepath\decisiontree";
libname source "&sourcedata";
libname comp1 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\V570";
/*******************************************************************************************************************/

/*******************************************************************************************************************/
/*                                				assigning the macro variables                                      */
%let data = &remotepath\&runmonth\data;
%let path = &remotepath\&runmonth\buckets;
%let lib = data;
%let model_no = compuscan;
%let _name_ = compuscan;
%let Target = target;
%let num_of_partitions = 8;
%let inputdata = build;
%let inputlib = data ;
%let test = 0;
%put &path;
/*																												   */
/*******************************************************************************************************************/

/*******************************************************************************************************************/
/*									prepare the input data into the segmentation 								   */
%let segmentationvariable = 
COMPUSCANVAR1401 COMPUSCANVAR1424 COMPUSCANVAR175 COMPUSCANVAR187 COMPUSCANVAR188 COMPUSCANVAR2123 COMPUSCANVAR2312 COMPUSCANVAR2528
COMPUSCANVAR2678 COMPUSCANVAR2696 COMPUSCANVAR3275 COMPUSCANVAR3916 COMPUSCANVAR3935 COMPUSCANVAR5208 
COMPUSCANVAR5486 COMPUSCANVAR5489 COMPUSCANVAR5579 COMPUSCANVAR5826 COMPUSCANVAR6073 COMPUSCANVAR6130 COMPUSCANVAR6132 
COMPUSCANVAR6134 COMPUSCANVAR6285 COMPUSCANVAR6788 COMPUSCANVAR716 COMPUSCANVAR733 COMPUSCANVAR7430 COMPUSCANVAR7431 
COMPUSCANVAR7479 COMPUSCANVAR753 COMPUSCANVAR7547 COMPUSCANVAR7549 COMPUSCANVAR7550 COMPUSCANVAR7683 
adjCOMPUSCANVAR6289;

proc surveyselect data= comp1.disbursedbase_&runmonth
	method=srs
    out= disbursedbase_&runmonth outall
    n=50000;
run;

/*Renaming variables*/
%let newvarlist = COMPUSCANVAR1401 COMPUSCANVAR1424 COMPUSCANVAR175 COMPUSCANVAR187 COMPUSCANVAR188
				COMPUSCANVAR2123 COMPUSCANVAR2312 COMPUSCANVAR2528 COMPUSCANVAR2678 COMPUSCANVAR2696
				COMPUSCANVAR3275 COMPUSCANVAR3916 COMPUSCANVAR3935 COMPUSCANVAR5208 COMPUSCANVAR5486
				COMPUSCANVAR5489 COMPUSCANVAR5579 COMPUSCANVAR5826 COMPUSCANVAR6073 COMPUSCANVAR6130
				COMPUSCANVAR6132 COMPUSCANVAR6134 COMPUSCANVAR6285 COMPUSCANVAR6788 COMPUSCANVAR716
				COMPUSCANVAR733 COMPUSCANVAR7430 COMPUSCANVAR7431 COMPUSCANVAR7479 COMPUSCANVAR753
				COMPUSCANVAR7547 COMPUSCANVAR7549 COMPUSCANVAR7550 COMPUSCANVAR7683;

%let oldvarlist = UNS_PercUtilisation UNS_ValCurBalMR60Days CSN_TimeOldestTrade ALL_TimeMREnq ALL_TimeOldestEnq
                  AUL_NumOpenTrades ALL_MaxDelq1YearLT24M OWN_Perc1pDelq2Years OTH_MaxDelqEver
                  OTH_MaxDelq1YearLT24M REV_MaxDelq180DaysGE24M UNS_TimeMR3pDelq UNS_MaxDelq180DaysLT12M
                  AIL_Num1pDelq90Days ALL_NumEverTrades ALL_NumTrades90Days OTH_AvgMonthsOnBook UNS_AvgMonthsOnBook
                  RCG_AvgMonthsOnBook UNN_AvgMonthsOnBook ALL_ValOrgBalLim90Days ALL_ValOrgBalLim1Year
                  OTH_ValOrgBalLim180Days UNS_ValCurBal1Year OWN_PercUtiliSatisfTrades
                  OWN_AvgPercUtilisationMR60Days ALL_NumPayments2Years ALL_PercPayments2Years
                  OTH_PercPayments2Years OTH_ValOrgBalLim REV_PercPayments180Days REV_PercPayments1Year
                  REV_NumPayments2Years OPL_PercPayments2Years;

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

/*Compuscan variables transformation*/
data disbursedbase_&runmonth ;
	set disbursedbase_&runmonth ;
	%rename1(&oldvarlist, &newvarlist);
run;

data &lib..&inputdata &inputdata;
	set disbursedbase_&runmonth;
	keep &segmentationvariable finalweight Month &target ;
	finalweight=1;
run;
data data.decision_tree;
	set tree.decision_tree;
run;
/*										end of the prepare data 												   */
/*******************************************************************************************************************/

/*******************************************************************************************************************/
/*                                Create tree structure if does not exist                                          */
%macro checktree();	
	%if %sysfunc(exist(&inputlib..decision_tree)) %then %do;
	%end;
	%else %do;
		%create_segmentation_trees(out_dataset = &inputlib..decision_tree, max_number_nodes = 8, max_number_of_endnode=5);
	%end;
%mend;
%checktree();
/*									end of the street structure macro 											   */
/*******************************************************************************************************************/

/*******************************************************************************************************************/
/*									Split the data into number of partion										   */
proc contents data = &inputlib..&inputdata (keep = &segmentationvariable) out=&inputlib..Fieldnames;
run;
%macro split_set(table_name=,num_of_partitions=);
	%do i=1 %to &num_of_partitions;
		proc sql;
			create table &inputlib..partition_&i 
				as select *
				from &table_name
				having (varnum > Round((max(varnum))*((&i-1)/&num_of_partitions))
				and varnum <= Round((max(varnum))*(&i/&num_of_partitions)));
		quit;
	%end;
%mend;
%split_set(table_name=&inputlib..Fieldnames,num_of_partitions=&num_of_partitions);
/*										end dof the splitting macro 											   */
/*******************************************************************************************************************/

/*******************************************************************************************************************/
/*									running level one segmentation 												   */
options mprint mlogic;
%macro determine_level_1_winner(lib, dataset, target, path, test);
	%let NUM_OF_LEVEL = 3;
	%if  &dataset ne %str() and &target ne %str() %then %do;
		%if   %sysfunc(exist(&dataset)) %then %do;
			%if (%bquote(&path) = ) %then %goto quit;
			%if ^%sysfunc(fileexist(&path)) %then %do;
				%put Note : Path does not exist;
				%goto quit;
			%end;

			%do i = 1 %to &num_of_partitions;
				proc delete data = &lib..flag_&i; run;
				systask command "copy &programpath\codes\Seg_Parallel_Level_1.sas
								      &programpath\codes\CS_Seg_Parallel_Level_1_&i..sas" 
								      wait mname=gddincopy taskname=gddincopy status=task; waitfor gddincopy;
				options xwait noxsync;
				%let start_sas = 'E:\SASHome\SASFoundation\9.4\sas.exe';
				data _null_;
					call symput('sas_program',cats("'","&programpath.\codes\CS_Seg_Parallel_Level_1_&i.",".sas'"));
					call symput('sas_log', cats("'","&remotepath\&runmonth\logs\CS_Seg_Parallel_Level_1_&i.",".log'"));
					call symput('sas_print', cats("'","&remotepath\&runmonth\logs\CS_Seg_Parallel_Level_1_&i.",".lst'"));
					call symput("sysparm_v", cats("'","build Ç &data Ç &i Ç &target Ç &path Ç &test ","'"));
				run;
				x " &start_sas -sysin &sas_program -nosplash -NOSTATUSWIN -log &sas_log -noerrorabend -nosyntaxcheck -sysparm &sysparm_v ";
			%end;
		%end;
		%else %put Note: one of the input dataset is missing;
	%end;
	%else %put Note: one of the input variable is missing;
	
	%let tableexist= 0;
	%do %while (&tableexist =0);
		%let countflag = 0;
		%do h = 1 %to &num_of_partitions;
			%if %sysfunc(exist(&inputlib..flag_&h.)) %then %let countflag = %eval(&countflag+1);
		%end;
		%if &countflag=&num_of_partitions %then %let tableexist = 1;
		%else %let tableexist = 0; 
		data _null_;
			call sleep(2000);
		run;
	%end;
	%quit:
%mend;
%determine_level_1_winner(&inputlib, &inputdata, &target, &path, &test);
/*												end of level 1 segmentation macro 								   */
/*******************************************************************************************************************/

/*******************************************************************************************************************/
/*							combine segmenttion results from all the partitions      							   */
%macro combine_partitions();
	proc delete data = data.comp_segment_report ; run;
	%do i = 1 %to &num_of_partitions;
		proc sql;
			create table all_gini_&i as 
				select a.level_1 as VariableName ,level_1_if_statements as Segment , a.badrate as BadRate, a.popsum/b.number_obs as PopPercentage,
					   a.good as Goods, a.bad as Bads, a.SEGMENT_GINI as SegmentGini, b.gini as OverallGini 
				from data.Overall_report_&i a , 
					 (select  distinct gini , level_winner,number_obs from data.Segment_results_&i) b
				where a.level_1 = b.level_winner;
		quit;
		proc append base = data.comp_segment_report data = all_gini_&i force; run;
	%end;
	data comp_segment_report;
		set data.comp_segment_report;
		count = mod(_n_,5);
		if count = 0 then segment_no = 5;
		else segment_no = count;
	run;
	proc delete data = current_segmentation ; run;
	%do i = 1 %to 5;
		data segment_data_&i. ;
			set  disbursedbase_&runmonth(keep = cs_V570_prob comp_seg  target );
			where comp_seg = &i;
			comp_prob = cs_V570_prob;
			keep  comp_seg comp_prob target ;
		run;
		%Calc_Gini(comp_prob,segment_data_&i. , target ,  model_results_output);
		data model_results_output ;
			set model_results_output;
			segment_no = &i. ;
		run;
		proc append base = current_segmentation data = model_results_output force ; run;
	%end;
	data current_gini ;
		set  disbursedbase_&runmonth(keep = cs_V570_prob  target );
		comp_prob = cs_V570_prob;
		keep  comp_prob target ;
	run;
	%Calc_Gini(comp_prob,current_gini , target ,  overallgini);
	proc sql;
		create table data.comp_segmentation_result_&runmonth as
			select a.*, b.gini as CurrentGini , &Gini as CurrentOverallGini
			from comp_segment_report a, current_segmentation b
			where a.segment_no = b.segment_no
			order by a.overallgini desc , a.variablename,a.segment_no;
	quit;
	data source.comp_segmentation_result_&runmonth;
		set data.comp_segmentation_result_&runmonth;
	run;
%mend;
%combine_partitions();

libname colookup "\\mpwsas64\Core_Credit_Risk_Models\V5\Compuscan MetaData";
libname credscr odbc dsn=cred_scoring schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
proc sort data = data.comp_segmentation_result_&runmonth out = comp_segmentation_result_&runmonth;
	by variablename segment_no;
run;

proc sql;
	create table comp_segmentation_result_&runmonth as
		select distinct a.*,b.name as variablename_d
		from comp_segmentation_result_&runmonth a
		inner join colookup.Compscanvar_lookup b 
		on compress(upcase(a.variablename)) = compress(upcase(b.newcolumn));
quit;

data comp_segmentation_result_&runmonth;
	set comp_segmentation_result_&runmonth;
	if find(upcase(variablename_d),'BAL') or find(upcase(variablename_d),'ENQ') then delete ;
run;

proc sql;
	create table comp_segmentation_result_&runmonth as
		select a.*, b.DescriptiveName, b.Description
		from comp_segmentation_result_&runmonth a left join source.variable_descriptions b
		on a.VariableName = b.CompuscanVarName;
quit;

data comp_segmentation_result_&runmonth;
	set comp_segmentation_result_&runmonth(obs=15);
run;

proc sql;
	create table comp_seg as
		select variablename,variablename_d, 0 as segment_no, 1 as PopPercentage,sum(bads)/(sum(goods)+sum(bads)) as badrate, sum(bads) as bads,
							sum(goods) as goods,avg(OverallGini) as SegmentGini , avg(CurrentOverallGini) as parent_model
		from comp_segmentation_result_&runmonth
		group by variablename, variablename_d;
quit;

data comp_segmentation_result;
	set comp_segmentation_result_&runmonth(drop = CurrentOverallGini count OverallGini);
	rename CurrentGini = parent_model;	
run;

proc append base = comp_segmentation_result data= comp_seg force ; run;

data comp_segmentation_result;
	set comp_segmentation_result;
	newcolumn = variablename;
	segment = upcase(tranwrd( segment, compress(variablename), compress(variablename_d)));
	variablename =upcase(variablename_d);
	AbsoluteLift = parent_model - SegmentGini;
	RelativeLift = (parent_model-SegmentGini)/SegmentGini;
	name = variablename_d;
	Month = &runmonth;
	drop variablename_d;
run;


/****************************************/
data inputdat;
	set disbursedbase_&runmonth;
run;
%Calc_Gini(cs_V570_prob, inputdat, target, gini_all);

data gini_all (keep=gini comp_seg);
	set gini_all;
	comp_seg = 0;
run;

%macro gini(in=);
	%do i=1 %to 5;
		data seg&i.;
			set &in;
			if comp_seg = &i;
		run;
		%Calc_Gini(cs_V570_prob, seg&i., target, gini_&i.);
		data gini_&i. (keep=gini comp_seg);
			set gini_&i.;
			comp_seg = &i.;
		run;
		proc append base=gini_all data=gini_&i. force; run;
	%end;
%mend;
%gini(in=inputdat);

proc sql;
	create table Comp_segmentation as
		select sum(target)/count(*) as badrate, count(*) as Popsum, sum(target) as bads, count(*) - sum(target) as goods, comp_seg
		from inputdat
		group by comp_seg;
quit;

proc sql;
	create table Comp_segmentation as
		select a.*, b.gini as segmentgini, sum(Popsum) as number_obs
		from Comp_segmentation a left join gini_all b
		on a.comp_seg = b.comp_seg;
quit;

proc sql;
	create table Comp_segmentation1 as
		select sum(target)/count(*) as badrate, count(*) as Popsum, count(*) as number_obs, sum(target) as bads, count(*) - sum(target) as goods, 0 as comp_seg
		from inputdat;
quit;

proc sql;
	create table Comp_segmentation2 as
		select a.*, b.gini as Segmentgini
		from Comp_segmentation1 a left join gini_all b
		on a.comp_seg = b.comp_seg;
quit;

data Comp_segmentation;
	set Comp_segmentation Comp_segmentation2;
	 var = "COMPUSCANVAR2123";
run;

proc sql;
	create table Comp_segmentation as
		select a.*, b.Description
		from Comp_segmentation a left join source.variable_descriptions b
		on a.var = b.CompuscanVarName;
quit;

data Comp_segmentation3 (drop=Popsum number_obs comp_seg var);
	set Comp_segmentation;
	VariableName = "Current Segmentation";
	Descriptivename = variablename;
	newcolumn = variablename;
	segment_no = comp_seg;	
	parent_model = segmentgini;
	if segment_no = 1 then Segment = "Repeat = 0 and (COMPUSCANVAR2123 <= 0 or COMPUSCANVAR2123 = .)";
	if segment_no = 2 then Segment = "Repeat = 0 and COMPUSCANVAR2123 > 0";
	if segment_no = 3 then Segment = "Repeat = 1 and (COMPUSCANVAR2123 <= 0 or COMPUSCANVAR2123 = .)";
	if segment_no = 4 then Segment = "Repeat = 1 and (COMPUSCANVAR2123 = 1 or COMPUSCANVAR2123 = 2";
	if segment_no = 5 then Segment = "Repeat = 1 and COMPUSCANVAR2123 > 2";
	PopPercentage = popsum/number_obs; 
	AbsoluteLift = SegmentGini-parent_model;
	RelativeLift = (SegmentGini-parent_model)/parent_model;
	name = variablename;
	Month = &runmonth;
run;
/********************************/

data source.comp_segmentation_result;
	set comp_segmentation_result Comp_segmentation3;
run;

proc sort data = source.comp_segmentation_result ;
	by variablename segment_no;
run;

proc delete data= credscr.comp_segmentation_result; run;
data credscr.comp_segmentation_result;
	set source.comp_segmentation_result;
run;

data dashboardvar (keep=Gini Score_type Segment);
	set source.comp_segmentation_result (drop=Segment);
	Score_type = VariableName;
	Gini = SegmentGini;
	Segment = segment_no;
run;

proc sort data=dashboardvar; by Segment descending Gini; run;

data dashboardvar1;
	set dashboardvar (obs=1);
run;

proc sql;
  create table dashboardvar2 as
  	select a.Score_type, b.segment, b.Gini
	from dashboardvar1 a inner join dashboardvar b
	on a.Score_type = b.score_type;
quit;

data dashboardvar2;
	set dashboardvar2 (drop=Score_type);
	Score_type = "Resegment Comp Prob";
run;

%macro checkifoverall_tableexist();
	%if %sysfunc(exist(source.overallgini_summary_&runmonth. )) %then %do;
		data source.overallgini_summary_&runmonth.;
			set source.overallgini_summary_&runmonth. dashboardvar2;
		run;
	%end;
	%else %do;
		data source.overallgini_summary_&runmonth.;
			length Score_type $50.;
			set  dashboardvar2;
		run;
	%end;
%mend;
%checkifoverall_tableexist();

/*%macro getfirstlevelwinner();*/
/*	%do i = 1 %to &num_of_partitions;*/
/*		%if %obscnt(&inputlib..Segment_results_&i.) > 0 %then %do;*/
/*			proc append base = &inputlib..all_segment_results data =  &inputlib..Segment_results_&i force ; run;*/
/*		%end;*/
/*	%end;*/
/*	data &inputlib..all_segment_results;*/
/*		set &inputlib..all_segment_results(where=( gini ne .));*/
/*	run;*/
/*	proc sql;*/
/*		create table &inputlib..level1winner as*/
/*			select distinct level_winner, gini */
/*			from &inputlib..all_segment_results;*/
/*	quit;*/
/*%mend;*/
/*%getfirstlevelwinner();*/
/*													end combine macro 											   */
/*******************************************************************************************************************/


/*******************************************************************************************************************/
/*											 Select the top 5 from level one winner 							   */
/*proc sort data = &inputlib..level1winner; by descending gini; run;*/
/*data  &inputlib..level1winner; 	set &inputlib..level1winner(obs = 5); run;*/
/*proc sql;select level_winner into : listofsegmentvar separated by " " from &inputlib..level1winner; quit;*/
/*%put &listofsegmentvar;*/
/*%let NoOfLevel1Winner = %obscnt(&inputlib..level1winner);*/
/*%put &NoOfLevel1Winner;*/
/*													end 			 											   */
/*******************************************************************************************************************/

/*******************************************************************************************************************/
/*									running level two segmentation 												   */
%macro determine_level_2_winner(lib, dataset, target, path, test);
	%let NUM_OF_LEVEL = 3;
	%if  &dataset ne %str() and &target ne %str() %then %do;
		%if   %sysfunc(exist(&dataset)) %then %do;
			%if (%bquote(&path) = ) %then %goto quit;
			%if ^%sysfunc(fileexist(&path)) %then %do;
				%put Note : Path does not exist;
				%goto quit;
			%end;

			%do i = 1 %to &NoOfLevel1Winner;
				%let nextpartition=%eval(&num_of_partitions+&i);
				proc delete data = &lib..flag_&nextpartition; run;

			/* Each partition contains one variables from the first level winner */
				proc sql;
					select level_winner into : winnerslist separated by " " 
					from &lib..level1winner;
				quit;
				%let winner = %scan(&winnerslist,&i);
			
				systask command "copy &programpath\Codes\Seg_Parallel_Level_2.sas
								      &programpath\Codes\Seg_Parallel_Level_2_&i..sas" 
								      wait mname=gddincopy taskname=gddincopy status=task; waitfor gddincopy;
				options xwait noxsync;
				%let start_sas = 'D:\SASHome\SASFoundation\9.4\sas.exe';
				data _null_;
					call symput('sas_program',cats("'","&programpath\codes\Seg_Parallel_Level_2_&i.",".sas'"));
					call symput('sas_log', cats("'","&remotepath\logs\Seg_Parallel_Level_2_&i.",".log'"));
					call symput('sas_print', cats("'","&remotepath\logs\Seg_Parallel_Level_2_&i.",".lst'"));
					call symput("sysparm_v", cats("'","build Ç &data Ç &nextpartition Ç &target Ç &path Ç &test Ç &winner ","'"));
				run;
				x " &start_sas -sysin &sas_program -nosplash -NOSTATUSWIN -log &sas_log -noerrorabend -nosyntaxcheck -sysparm &sysparm_v ";
			%end;
		%end;
		%else %put Note: one of the input dataset is missing;
	%end;
	%else %put Note: one of the input variable is missing;

	%let tableexist= 0;
	%do %while (&tableexist =0);
		%let countflag = 0;
		%do h = 1 %to &NoOfLevel1Winner;
			%let nextpartition=%eval(&num_of_partitions+&h);
			%if %sysfunc(exist(&lib..flag_&nextpartition.)) %then %let countflag = %eval(&countflag+1);
		%end;
		%if &countflag=&num_of_partitions %then %let tableexist = 1;
		%else %let tableexist = 0; 
		data _null_;
			call sleep(2000);
		run;
	%end;
	%quit:
%mend;
/*%determine_level_2_winner(&inputlib, &inputdata, &target, &path, &test);*/
/*												end of level 2 segmentation macro 								   */
/*******************************************************************************************************************/

%macro getlevel2Winner();
	%do i = 1 %to &NoOfLevel1Winner;
		%let nextpartition=%eval(&num_of_partitions+&i);
		%if %obscnt(&inputlib..Segment_results_&nextpartition.) > 0 %then %do;
			proc append base = &inputlib..all_segment_results data =  &inputlib..Segment_results_&i force ; run;
		%end;
	%end;
%mend;
/*%getlevel2Winner();*/


/*filename macros2 'H:\Process_Automation\macros';*/
/*options sasautos = (sasautos  macros2);*/
