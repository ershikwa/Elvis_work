/*%include "\\Neptune\SASA$\SAS_Automation\SAS_Autoexec\autoexec7.sas";*/
options noxwait compress=binary;

data _null_;
	 call symput("runmonth", put(intnx('month',today(),-1),yymmn6.));
run;
%put &runmonth;

%let sourcedata = \\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\TU Performance Monitoring\data;
%let currentworkingdirectory = \\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\TU Models\V6_Scorecard_Monitoring\Transunion;
%let remotepath = &currentworkingdirectory;
%let programpath = \\mpwsas64\Core_Credit_Risk_Models\SAS_Automation\Partition_Code;

%sysexec mkdir "&remotepath\&runmonth\buckets";
%sysexec mkdir "&remotepath\&runmonth\data";
%sysexec mkdir "&remotepath\&runmonth\logs";

/*******************************************************************************************************************/
/*                                			assigning libraries	    			                                   */
libname tshepo "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets";
libname data "&remotepath\&runmonth\data";
libname tree "&remotepath\decisiontree";
libname source "&sourcedata";
libname tulookup "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\TU Models\Lookup";
libname tumeta "\\mpwsas64\Core_Credit_Risk_Models\V6\MetaData";
libname tu1 '\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets\V580';
libname credscr odbc dsn=cred_scoring schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
%let odbc = MPWAPS;
/*******************************************************************************************************************/

/*******************************************************************************************************************/
/*                                				assigning the macro variables                                      */
%let data = &remotepath\&runmonth\data;
%let path = &remotepath\&runmonth\buckets;
%let lib = data;
%let model_no = transunion;
%let _name_ = transunion;
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
DM0001AL EQ0012AL EQ0015PL  NG004 NP003 PP0901AL PP0935AL RE006_019 PP0801AL PP0801AL_GI_RATIO
NP013 NP015 PERSALINDICATOR PP003 PP116 PP149 PP173 PP283 PP0001AL PP003_NP003  PP0051CL PP0111LB
PP0171CL PP0313LN PP0325AL PP0327AL PP0406AL PP0407AL PP0421CL PP0503AL PP0503AL_3_RATIO_12
PP0505AL PP0515AL PP0515AL_GI_RATIO PP0521LB PP0521LB_GI_RATIO PP0601AL PP0601AL_CU_RATIO_3
PP0601AL_CU_RATIO_6 PP0603AL PP0604AL PP0714AL PP0714AL_GI_RATIO ;

proc surveyselect data= tu1.disbursedbase4reblt_&runmonth
	method=srs
    out= disbursedbase4reblt_&runmonth outall
    n=50000;
run;

data &lib..&inputdata &inputdata;
	set disbursedbase4reblt_&runmonth;
	keep &segmentationvariable  Month &target ;
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
data MTableName;
	set  &inputlib..&inputdata(keep = &segmentationvariable);
	drop PP0801AL PP0801AL_GI_RATIO PP0503AL PP0503AL_3_RATIO_12 PP0505AL
	PP0515AL PP0515AL_GI_RATIO PP0521LB PP0521LB_GI_RATIO
	PP0601AL PP0601AL_CU_RATIO_3 PP0601AL_CU_RATIO_6
	PP0604AL PP0603AL PP0714AL PP0714AL_GI_RATIO;
run;

proc contents data = MTableName  out=&inputlib..Fieldnames;
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
								      &programpath\codes\TU_Seg_Parallel_Level_1_&i..sas" 
								      wait mname=gddincopy taskname=gddincopy status=task; waitfor gddincopy;
				options xwait noxsync;
				%let start_sas = 'E:\SASHome\SASFoundation\9.4\sas.exe';
				data _null_;
					call symput('sas_program',cats("'","&programpath.\codes\TU_Seg_Parallel_Level_1_&i.",".sas'"));
					call symput('sas_log', cats("'","&remotepath\&runmonth\logs\TU_Seg_Parallel_Level_1_&i.",".log'"));
					call symput('sas_print', cats("'","&remotepath\&runmonth\logs\TU_Seg_Parallel_Level_1_&i.",".lst'"));
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
/*							combine output from all the partitions												   */
%macro combine_partitions();
	proc delete data = data.tu_segment_report ; run;
	%do i = 1 %to &num_of_partitions;
		proc sql;
			create table all_gini_&i as 
				select a.level_1 as VariableName format $32. length=32 ,level_1_if_statements as Segment , a.badrate as BadRate, a.popsum/b.number_obs as PopPercentage,
					   a.good as Goods, a.bad as Bads, a.SEGMENT_GINI as SegmentGini, b.gini as OverallGini 
				from data.Overall_report_&i a , 
					 (select  distinct gini , level_winner,number_obs from data.Segment_results_&i) b
				where a.level_1 = b.level_winner;
		quit;
		proc append base = data.tu_segment_report data = all_gini_&i force; run;
	%end;
	data tu_segment_report;
		set data.tu_segment_report;
		count = mod(_n_,5);
		if count = 0 then segment_no = 5;
		else segment_no = count;
	run;
	proc delete data = current_segmentation ; run;
	%do i = 1 %to 5;
		data segment_data_&i. ;
			set  Disbursedbase4reblt_&runmonth(keep = tu_seg TU_V580_prob target );
			where tu_seg = &i;
		run;
		%Calc_Gini(TU_V580_prob,segment_data_&i. , target ,  model_results_output);
		data model_results_output ;
			set model_results_output;
			segment_no = &i. ;
		run;
		proc append base = current_segmentation data = model_results_output force ; run;
	%end;
	data curennt_gini ;
		set  Disbursedbase4reblt_&runmonth(keep = tu_seg TU_V580_prob target );
	run;
	%Calc_Gini(TU_V580_prob,curennt_gini , target ,  overallgini);
	proc sql;
		create table data.tu_segmentation_result_&runmonth as
			select a.*, b.gini as CurrentGini , &Gini as CurrentOverallGini
			from tu_segment_report a, current_segmentation b
			where a.segment_no = b.segment_no
			order by a.overallgini desc, a.variablename,a.segment_no;
	quit;

	data source.tu_segmentation_result_&runmonth;
		set data.tu_segmentation_result_&runmonth(obs = 15);
	run;
%mend;
%combine_partitions();


proc sort data = source.tu_segmentation_result_&runmonth out = tu_segmentation_result_&runmonth;
	by variablename segment_no;
run;

proc sql;
	create table tu_segmentation_result1_&runmonth as
		select a.*,b.New_variable as descpitivename, b.Description
		from tu_segmentation_result_&runmonth a left join tumeta.Tu_lookup b
		on compress(a.variablename) = compress(b.variable);
quit;

data tu_segmentation_result1_&runmonth;
	set tu_segmentation_result1_&runmonth;
	if VariableName = "PP003_NP003" then descpitivename = "othNumPPCPA and othNumPPNLR";
	if VariableName = "PP003_NP003" then description = "All number of payment profiles on accounts with Other institutions";
run;

proc sql;
	create table TU_seg as
		select variablename,descpitivename,Description, 0 as segment_no, 1 as PopPercentage,sum(bads)/(sum(goods)+sum(bads)) as badrate, sum(bads) as bads,
							sum(goods) as goods,avg(OverallGini) as SegmentGini , avg(CurrentOverallGini) as parent_model
		from tu_segmentation_result1_&runmonth
		group by variablename,descpitivename,Description;
quit;
proc sort data = TU_seg nodupkey ; 
	by variablename;
run;
data tu_segmentation_result;
	set tu_segmentation_result1_&runmonth(drop = CurrentOverallGini count OverallGini);
	rename CurrentGini = parent_model;
run;
proc append base = tu_segmentation_result data= tu_seg force ; run;
data tu_segmentation_result;
	set tu_segmentation_result;
	AbsoluteLift = parent_model-SegmentGini;
	RelativeLift = (parent_model-SegmentGini)/SegmentGini;
	Month = &runmonth;
run;
proc sort data = tu_segmentation_result;
	by variablename segment_no;
run;

/****************************************/
data inputdat;
	set disbursedbase4reblt_&runmonth;
run;
%Calc_Gini(TU_V580_prob, inputdat, target, gini_all);

data gini_all (keep=gini tu_seg);
	set gini_all;
	tu_seg = 0;
run;

%macro gini(in=);

	%do i=1 %to 5;
		data seg&i.;
			set &in;
			if tu_seg = &i;
		run;

		%Calc_Gini(TU_V580_prob, seg&i., target, gini_&i.);

		data gini_&i. (keep=gini tu_seg);
			set gini_&i.;
			tu_seg = &i.;
		run;

		proc append base=gini_all data=gini_&i. force; run;
	%end;
%mend;
%gini(in=inputdat);

proc sql;
	create table TU_segmentation as
		select sum(target)/count(*) as badrate, count(*) as Popsum, sum(target) as bads, count(*) - sum(target) as goods, tu_seg
		from inputdat
		group by tu_seg;
quit;

proc sql;
	create table TU_segmentation as
		select a.*, b.gini as segmentgini, sum(Popsum) as number_obs
		from TU_segmentation a left join gini_all b
		on a.tu_seg = b.tu_seg;
quit;

proc sql;
	create table TU_segmentation1 as
		select sum(target)/count(*) as badrate, count(*) as Popsum, count(*) as number_obs, sum(target) as bads, count(*) - sum(target) as goods, 0 as tu_seg
		from inputdat;
quit;

proc sql;
	create table TU_segmentation2 as
		select a.*, b.gini as Segmentgini
		from TU_segmentation1 a left join gini_all b
		on a.tu_seg = b.tu_seg;
quit;

data TU_segmentation;
	set TU_segmentation TU_segmentation2;
	 var = "PP003_NP003";
run;

proc sql;
	create table TU_segmentation as
		select a.*, b.Description
		from TU_segmentation a left join tumeta.Tu_lookup b
		on a.var = b.variable;
quit;

data TU_segmentation3 (drop=Popsum number_obs tu_seg var);
	set TU_segmentation;
	VariableName = "Current Segmentation";
	Descpitivename = variablename;
	segment_no = tu_seg;	
	parent_model = segmentgini;
	if segment_no = 1 then Segment = "PP003_NP003 <= 3)";
	if segment_no = 2 then Segment = "PP003_NP003 <= 6";
	if segment_no = 3 then Segment = "PP003_NP003 <= 8)";
	if segment_no = 4 then Segment = "PP003_NP003 <= 12";
	if segment_no = 5 then Segment = "PP003_NP003 > 12";
	PopPercentage = popsum/number_obs; 
	AbsoluteLift = parent_model-SegmentGini;
	RelativeLift = (parent_model-SegmentGini)/SegmentGini;
	Month = &runmonth;
run;
/********************************/

data source.tu_segmentation_result;
	set tu_segmentation_result TU_segmentation3;
run;

proc sort data = source.tu_segmentation_result ;
	by variablename segment_no;
run;

proc delete data= credscr.tu_segmentation_result; run;
data credscr.tu_segmentation_result;
	set source.tu_segmentation_result;
run;



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
				%let start_sas = 'E:\SASHome\SASFoundation\9.4\sas.exe';
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
