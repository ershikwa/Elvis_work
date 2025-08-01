options compress = yes;
options compress = on;

libname scoring odbc dsn=Cred_Scoring schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

libname board "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V6 Monitoring\Board\Dashboard";

data x;
	set board.all_scores_v645;
run;

/********************************************************************************************************************************************************/
/*                                                                      PSI calculation for each variable                                                                                                             */
/********************************************************************************************************************************************************/
%macro psi_calculation(build=, base=,period=month,var=,psi_var=, outputdataset=);
/*    %if %VarExist(&build, &psi_var)=1 and %VarExist(&base, &psi_var)=1 and %VarExist(&base, &period)=1 %then %do;*/
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
                  from basetable a full join  buildtable(keep = &psi_var binnumber)  b
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
            &period ="1900-01-01";
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

%macro looppervariables(segment=0,base=,build1=,variable_list=,outdataset=);
      proc delete data = &outdataset._&segment; run;
      %do i = 1 %to %sysfunc(countw(&variable_list));
            %let vari = %scan(&variable_list, &i.);
            %psi_calculation(build=&build1, base=&base,period=applicationdate,var=&vari,psi_var=&vari, outputdataset=&outdataset._&segment);
            data &outdataset._&segment;
                  set &outdataset._&segment;
                  seg = &segment;
            run;
      %end;
%mend;

data _null_;
	call symput('lstmnth',cats("'",put(intnx('day',today(),-30),yymmddd10.),"'"));
run;
%put &lstmnth;

data all_scores;
	set scoring.all_scores;
	where ApplicationDate >= &lstmnth;
	scoreband = V6_riskgroup;
	V6_Seg = input(cats(comp_seg,Tu_seg),8.);
run;  

data CS_SCORED;
	set scoring.CS_SCORED;
run;

data TU_SCORED;
	set scoring.TU_SCORED;
run;


%looppervariables(segment=0,base=TU_SCORED,build1=board.tu_build,variable_list=TU_Seg,outdataset=TU_SegDist);
%looppervariables(segment=0,base=CS_SCORED,build1=board.comp_build,variable_list=Comp_Seg,outdataset=CS_SegDist);
/*****************************************************************/
*%looppervariables(segment=0,base=TU_SCORED,build1=board.tu_build,variable_list=TU_Seg,outdataset=TU_SegDist);
*%looppervariables(segment=0,base=CS_SCORED,build1=board.comp_build,variable_list=Comp_Seg,outdataset=CS_SegDist);
/*****************************************************************/
%looppervariables(segment=0,base=all_scores,build1=board.ALL_SCORES,variable_list=institutioncode,outdataset=BankDist);
%looppervariables(segment=0,base=all_scores,build1=board.ALL_SCORES,variable_list=channelcode,outdataset=ChannelCodeDist);
%looppervariables(segment=0,base=all_scores,build1=board.ALL_SCORES,variable_list=v6_riskgroup,outdataset=ScorebandDist);
%looppervariables(segment=0,base=all_scores,build1=board.ALL_SCORES,variable_list=V6_Seg,outdataset=V6SegDist);

data all_scores_V622 all_scores_V636 all_scores_V645 all_scores_V655;
	set all_scores;
	if maxSCORECARDVERSION = 'V622' then output all_scores_V622;
	else if maxSCORECARDVERSION = 'V636' then output all_scores_V636;
	else if maxSCORECARDVERSION = 'V645' then output all_scores_V645;
	else if maxSCORECARDVERSION = 'V655' then output all_scores_V655;
run;

%looppervariables(segment=0,base=all_scores_V622,build1=board.ALL_SCORES_V622,variable_list=v6_riskgroup,outdataset=ScorebandDist1);
%looppervariables(segment=0,base=all_scores_V636,build1=board.ALL_SCORES_V636,variable_list=v6_riskgroup,outdataset=ScorebandDist2);
%looppervariables(segment=0,base=all_scores_V645,build1=board.ALL_SCORES_V645,variable_list=v6_riskgroup,outdataset=ScorebandDist3);
%looppervariables(segment=0,base=all_scores_V655,build1=board.ALL_SCORES_V655,variable_list=v6_riskgroup,outdataset=ScorebandDist4);

proc sql; /*V645*/
    create table ScorebandDist_0 as /*add scorecard*/
	select a.*, b.percent as percent_V622, b.psi as psi_V622, c.percent as percent_V636, c.psi as psi_V636, d.percent as percent_V645, d.psi as psi_V645,
	e.percent as percent_V655,e.psi as psi_V655
	from ScorebandDist_0 a left join ScorebandDist1_0 b
	on a.ApplicationDate = b.ApplicationDate and a.scores = b.scores
	left join ScorebandDist2_0 c
	on a.ApplicationDate = c.ApplicationDate and a.scores = c.scores
	left join ScorebandDist3_0 d
	on a.ApplicationDate = d.ApplicationDate and a.scores = d.scores
	left join ScorebandDist4_0 e
	on a.ApplicationDate = e.ApplicationDate and a.scores = e.scores;
quit;


proc sql;
	drop table scoring.V622_Covid_TU_SegDist; 

	create table  scoring.V622_Covid_TU_SegDist 
	like TU_SegDist_0; 
	
	insert into scoring.V622_Covid_TU_SegDist (Bulkload = Yes)
	select *
	from TU_SegDist_0;
quit;

proc sql;
	drop table scoring.V622_Covid_CS_SegDist; 

	create table  scoring.V622_Covid_CS_SegDist 
	like CS_SegDist_0; 
	
	insert into scoring.V622_Covid_CS_SegDist (Bulkload = Yes)
	select *
	from CS_SegDist_0;
quit;

proc sql;
	create table ChannelCodeDist_1 as 
		select * 
		from ChannelCodeDist_0 a 
		left join board.CHANNELCODEDESCRIP b
		on a.scores = b.channelcode;
quit;

proc sql;
	drop table scoring.V622_Covid_ChannelCodeDist; 

	create table  scoring.V622_Covid_ChannelCodeDist 
	like ChannelCodeDist_1; 
	
	insert into scoring.V622_Covid_ChannelCodeDist (Bulkload = Yes)
	select *
	from ChannelCodeDist_1;
quit;

proc sql;
	drop table scoring.V622_Covid_BankDist; 

	create table  scoring.V622_Covid_BankDist 
	like BankDist_0; 
	
	insert into scoring.V622_Covid_BankDist (Bulkload = Yes)
	select *
	from BankDist_0;
quit;


proc sql;
	drop table scoring.V622_Covid_RiskgroupDist; 

	create table  scoring.V622_Covid_RiskgroupDist 
	like ScorebandDist_0; 
	
	insert into scoring.V622_Covid_RiskgroupDist (Bulkload = Yes)
	select *
	from ScorebandDist_0;
quit;

proc sql;
	drop table scoring.V622_Covid_V6SegDist; 

	create table  scoring.V622_Covid_V6SegDist 
	like V6SegDist_0; 
	
	insert into scoring.V622_Covid_V6SegDist (Bulkload = Yes)
	select *
	from V6SegDist_0;
quit;
