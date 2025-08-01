libname ES "\\mpwsas5\G\Automation\Behavescore\Datasets";
libname TF "\\mpwsas64\Core_Credit_Risk_Models\BehaveScoreV2 Data";
%let path = \\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\Behavioral Model\Bucketing Code;

data _null_;
	call symput('RunDate', put(intnx('day',today(),0),yymmddn8.));
run;
%put &RunDate;

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
		set &inDataset.(keep = idno no_zero_arrear_24_2 paid2owed12_V3 MaxArrearsCode DAYSSINCELASTABDISB DAYSSINCEFIRSTABDISB Maxcd BehaveScore BehaveDecile) ;
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

		RunDate = "&RunDate";	
	run;
%mend;

%scoring(inDataset=ES.Behave_&RunDate, outDataset=TF.BehaveV2_&RunDate, Path=&path);
