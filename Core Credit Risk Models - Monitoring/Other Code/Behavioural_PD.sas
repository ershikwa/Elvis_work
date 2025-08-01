Libname IN '\\mpwsas64\DM Model\AB Monthly Scoring Process\AB Predicted V5 RG\History';
Libname out '\\mpwsas64\Core_Credit_Risk_Model_Team\Lindo\Scored CS\Predicted';
libname data '\\mpwsas64\Core_Credit_Risk_Models\BehaveScoreV2 Data';
*Testing;

data _null_;
	call symput('runmonth',put(intnx('month',today(),-1,'end'),yymmddn8.));
	call symput('behavemonth',put(intnx('month',today(),-1,'end'),yymmn6.) );
run;
%put &runmonth &behavemonth;

%macro CS_Scored(indata =, month=);
	/*Renaming variables*/
	%let  oldvarlist= COMPUSCANVAR1401 COMPUSCANVAR1424 COMPUSCANVAR175 COMPUSCANVAR187 COMPUSCANVAR188
	            COMPUSCANVAR2123 COMPUSCANVAR2312 COMPUSCANVAR2528 COMPUSCANVAR2678 COMPUSCANVAR2696
	            COMPUSCANVAR3275 COMPUSCANVAR3916 COMPUSCANVAR3935 COMPUSCANVAR5208 COMPUSCANVAR5486
	            COMPUSCANVAR5489 COMPUSCANVAR5579 COMPUSCANVAR5826 COMPUSCANVAR6073 COMPUSCANVAR6130
	            COMPUSCANVAR6132 COMPUSCANVAR6134 COMPUSCANVAR6285 COMPUSCANVAR6788 COMPUSCANVAR716
	            COMPUSCANVAR733 COMPUSCANVAR7430 COMPUSCANVAR7431 COMPUSCANVAR7479 COMPUSCANVAR753
	            COMPUSCANVAR7547 COMPUSCANVAR7549 COMPUSCANVAR7550 COMPUSCANVAR7683;       

%let newvarlist = UNS_PercUtilisation UNS_ValCurBalMR60Days CSN_TimeOldestTrade ALL_TimeMREnq ALL_TimeOldestEnq
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
	      rename &old = &new;
	        %let k = %eval(&k + 1);
	      %let old = %scan(&oldvarlist, &k);
	      %let new = %scan(&newvarlist, &k);
	  %end;
	%mend;       

	proc contents data= IN.&indata out=names (keep=name);
	run;

	data names;
		set names;
		where find(name,'_W') or find(name,'_S') or find(name,'_B');
	run;

	proc sql;
		select name into : names separated by ' '
		from names;
	quit;
	data Indata;
	    set  IN.&indata; *(where =( load_type = 'MONTHLY_AB'));
		drop &names;
		comp_seg =seg;
		if id_no ='' then delete;
		%rename1(&oldvarlist, &newvarlist);
	run;

	proc sql;
		select distinct memname into : dataset from dictionary.tables
		where libname ='DATA' and find(memname,'BEHAVEV2_' ) and find(memname,"&month." );
	quit;

	proc sql;
		create table Indata as 
			select A.*,B.BehaveScore as BehaveScoreV2
			from  Indata A 
			left join data.&dataset  B
			on A.id_no = B.idno ;
			%rename1(&oldvarlist, &newvarlist);
	run;

	%macro scoring(inDataset=, outDataset=,Path=,scored_name =, modelType =);
		%macro ApplyScoring(Varlist1,Path1) ;
			%do i = 1 %to %sysfunc(countw(&Varlist1));
				%let var = %scan(&Varlist1, &i);
				%include "&Path1.\&var._if_statement_.sas";
				%include "&Path1.\&var._WOE_if_statement_.sas"; 
		    %end;

			%include "&Path1.\creditlogisticcode2.sas";
		%mend;

		/*Apply */
		%macro applymodel(segment) ;
			%let path1 = &path.\Segment&segment.\Bucketing code;
			libname d&segment "&path.\Segment&segment.\IV";
			proc sql; 
				select reverse(substr(reverse(compress(Parameter)),3)) into : Varlist1 separated by ' ' 
				from d&segment..parameter_estimate
				where Parameter ne "Intercept"; 
			quit;

			filename _temp_ temp;
			data _null_;
				set d&segment..parameter_estimate(where=(upcase(Parameter) ne "INTERCEPT"));
				file _temp_;
				formula = cats(tranwrd(upcase(Parameter),"_W","_S"),"=", Parameter,"*",estimate,";");
				put formula;
			run;
			%if &modelType = T %then %do;
				data scored&segment ;
					set &inDataset.( where = ( TU_Seg = &segment )) ;
					%ApplyScoring(varlist1=&Varlist1,Path1=&path1);
					&scored_name._prob =  P_target1;
					&scored_name._Score = 1000-(&scored_name._prob*1000);
					%inc _temp_;
				run;
			%end;
			%if &modelType = C %then %do;
				data scored&segment (keep=id_no CS_V570_Score CS_V570_Prob ApplicationDate ThinFileIndicator seg);
					set &inDataset.( where = ( Comp_Seg = &segment )) ;
					%ApplyScoring(varlist1=&Varlist1,Path1=&path1);
					&scored_name._prob =  P_target1;
					&scored_name._Score = 1000-(&scored_name._prob*1000);
					%inc _temp_;
				run;
			%end;
		%mend;
		%applymodel(segment=1);
		%applymodel(segment=2);
		%applymodel(segment=3);
		%applymodel(segment=4);
		%applymodel(segment=5);

		data &outDataset. ;
			set scored1
				scored2
				scored3
				scored4
				scored5;
		run;
	%mend;
	%scoring(inDataset=Indata, outDataset=scored_tu_cs,Path=\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V570 Compuscan,scored_name =CS_V570, modelType =C);

	data out.&indata(keep= id_no CS_V570_Score CS_V570_Prob ApplicationDate V570_RiskGroup seg);
		set scored_tu_cs;
		CS_V570_Score =  1000-(CS_V570_prob*1000);
		if ThinFileIndicator = 0 then do;
			if CS_V570_Score >= 932.242611756651      	then V570_RiskGroup = 50;
			else if CS_V570_Score >= 912.452480990053 	then V570_RiskGroup = 51;
			else if CS_V570_Score >= 878.956333489911 	then V570_RiskGroup = 52;
			else if CS_V570_Score >= 841.833690856176 	then V570_RiskGroup = 53;
			else if CS_V570_Score >= 811.989999894282 	then V570_RiskGroup = 54;
			else if CS_V570_Score >= 790.349339106057 	then V570_RiskGroup = 55;
			else if CS_V570_Score >= 778.068960766859 	then V570_RiskGroup = 56;
			else if CS_V570_Score >= 758.6444629 	    then V570_RiskGroup = 57;
			else if CS_V570_Score >= 746.152798684895 	then V570_RiskGroup = 58;
			else if CS_V570_Score >= 732.001226390991 then V570_RiskGroup = 59;
			else if CS_V570_Score >= 708.169317621721 then V570_RiskGroup = 60;
			else if CS_V570_Score >= 690.87118531475  then V570_RiskGroup = 61;
			else if CS_V570_Score >= 675.057720140646 then V570_RiskGroup = 62;
			else if CS_V570_Score >= 530 			    then V570_RiskGroup = 63;
			else if CS_V570_Score >= 529			    then V570_RiskGroup = 64;
			else if CS_V570_Score >= 527 			    then V570_RiskGroup = 65;
			else if CS_V570_Score >= 500 			    then V570_RiskGroup = 66;
			else if CS_V570_Score > 0    			    then V570_RiskGroup = 67;
		end;

		else if ThinFileIndicator = 1 then do;
			if CS_V570_Score >=      828.199869458644 then V570_RiskGroup = 68;
			else if CS_V570_Score >= 762.179100967216 then V570_RiskGroup = 69;
			else if CS_V570_Score >= 721.281349457995 then V570_RiskGroup = 70;
			else if CS_V570_Score > 0 				then V570_RiskGroup = 71;
		end;
	run;
%mend;

options mprint mlogic;
%CS_Scored(indata = Ab_predicted_v5_rg_&runmonth,month=&behavemonth);

