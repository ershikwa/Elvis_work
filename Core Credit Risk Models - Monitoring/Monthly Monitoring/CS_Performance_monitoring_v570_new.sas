OPTIONS NOSYNTAXCHECK ;
options compress = yes;
options mstored sasmstore=sasmacs; 

%include "\\neptune\sasa$\SAS_Automation\SAS_Autoexec\autoexec2.sas";
/*\\mpwsas64\SAS_Automation\SAS_Autoexec\autoexec2.sas */

%include "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\macros\Calc_Gini.sas";
%include "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\macros\CreateMonthlyGini.sas";
%include "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\macros\giniovertime.sas";
%include "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\macros\checkifcolumnsexist.sas";

libname decile "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\SAS Decile Tables\CS V570";
libname comp "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\V570";
libname comp1 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\data";
libname comp3 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring";
libname V5 '\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\data';
libname lookup '\\mpwsas64\Core_Credit_Risk_Models\V5\Segmentation Models For Compuscan\lookup';
Libname Data1 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\V570\App Seg1 Model";
Libname Data2 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\V570\App Seg2 Model";
Libname Data3 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\V570\App Seg3 Model";
Libname Data4 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\V570\App Seg4 Model";
Libname Data5 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\V570\App Seg5 Model";
Libname seg1 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\App Seg1 Model\Data";
Libname seg2 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\App Seg2 Model\Data";
Libname seg3 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\App Seg3 Model\Data";
Libname seg4 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\App Seg4 Model\Data";
Libname seg5 "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\App Seg5 Model\Data";
Libname Kat "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\TU Models\v570\Calibration";
Libname tu '\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets';

%let odbc = MPWAPS;

data _null_;
     call symput("enddate",cats("'",put(intnx('month',today(),-1,'end'),yymmddd10.),"'"));
     call symput("startdate",cats("'",put(intnx('month',today(),-13,'end'),yymmddd10.),"'"));
     call symput("actual_date", put(intnx("month", today(),-9,'end'),date9.));
     call symput("month", put(intnx("month", today(),-1,'end'),yymmn6.));
     call symput("prevmonth", put(intnx("month", today(),-2,'end'),yymmn6.));
	 call symput("prevprevmonth", put(intnx("month", today(),-3,'end'),yymmn6.));
	 call symput("reject_month", put(intnx("month", today(),-7,'end'),yymmn6.));
	 call symput("build_start", put(intnx("month", today(),-12,'end'),yymmn6.));
	 call symput("build_end", put(intnx("month", today(),-7,'end'),yymmn6.));
run;

/*** Check dates ***/
%put &startdate; 		*Monitoring month minus a year;
%put &enddate; 			*Monitoring month (last day of month);
%put &actual_date; 		*Monitoring month minus 9 months;
%put &month; 			*Monitoring month (month);
%put &prevmonth; 		*Monitoring month minus a month;
%put &prevprevmonth; 	*Monitoring month minus 2 months;
%put &reject_month; 	*Monitoring month minus 7 months;
%put &build_start; 		*Monitoring month minus 12 months;
%put &build_end; 		*Monitoring month minus 7 months;

%let codepath = \\mpwsas64\Core_Credit_Risk_Model_Team\Elvis\Monthly Monitoring\New Codes;

%include "&codepath\Create_CompuscanCSIReport.sas";

%include "&codepath\Create_CompuscanSlopeRateReport.sas";

%include "&codepath\Create_GiniReport.sas";

%include "&codepath\Create_Calibrated_Report.sas";

%macro Finalise_GiniResults();
	%if %sysfunc(exist(giniperseg_sum_V582)) and %sysfunc(exist(Comp.overallgini_summary_&month)) and %sysfunc(exist(comp.ginipersegment_summary_&month)) %then %do;
		/**********************************************************************/

		/*** Create ginipersegment_summary table ***/
		proc sql;
			create table comp.ginipersegment_summary_&month as
				select a.First_Due_Month, a.V570_Comp_Prob, a.V560_Comp_Prob, a.Comp_Generic_Score, a.TU_Generic_Score, 
						a.V570_TU_Prob,a.V580_TU_Prob, a.V622 as V622_Prob, a.V635_Prob, a.V636_Prob, b.V572 as V572_Comp_Prob, c.V582 as V582_TU_Prob, a.segment,V645 as V645_prob, V645_adj
				from Comp.Ginipersegment_summary_&month a left join giniperseg_sum_V572 b
				on a.First_Due_Month = b.month and a.segment = b.segment
				left join giniperseg_sum_V582 c
				on b.month = c.month and b.segment = c.segment;
		quit;

		/*** Create currentmodel_benchmark table ***/
		data Comp.overallgini_summary_&month;
			set Comp.overallgini_summary_&month GiniTable_V572 GiniTable_V582 comp.GiniTable_Build;
		run;

		proc sql;
			create table Comp.currentmodel_benchmark_&month as
				select distinct  a.*,b.gini as Current_gini, (b.gini-a.gini)/a.gini as Relative_lift
				from Comp.overallgini_summary_&month a,
					(select segment, gini from Comp.overallgini_summary_&month
					where score_type='Compuscan_Prob') b
				where a.segment=b.segment;
		quit;

		data Comp.currentmodel_benchmark_&month;
			set Comp.currentmodel_benchmark_&month;
		    format Recommended_Action $500.;
		   
			if Relative_lift >=-0.10 then do;
			   	Recommended_Action = cats("No Action ");
			end;
			else if Relative_lift <-0.10 and Relative_lift >-0.15 then do;
				Recommended_Action =cats("Check Additional Metrics and Establish if a score to Risk Calibration is required");
			end;
			else if Relative_lift <-0.15 then do;
			   Recommended_Action =cats("Check Additional Metrics and Establish if a score to Risk Calibration is required");
			end;
		run;

		data Comp.currentmodel_benchmark_&month.;
		     set Comp.currentmodel_benchmark_&month.;
		     if score_type='Compuscan_Generic' then score_type ='Comp Generic Score';
		     else if score_type='Tu_Generic' then score_type ='TU Generic Score';
		     else if score_type='Compuscan_Prob' then score_type ='V570 Comp Prob';
			 else if score_type='TU_V580_Prob' then score_type ='V580 TU Prob';
			 else if score_type='V622' then score_type ='V622 Prob';
			 else if score_type='V635' then score_type ='V635 Prob';
			 else if score_type='V636' then score_type ='V636 Prob';
			 else if score_type='V645' then score_type ='V645 Prob';
			 else if score_type='V645_adj' then score_type ='V645_adj Prob';
		run;

		/*** Save Ginis for Model Gini Comparison table on dashbaord ***/
		data Rebuild_Refit_Ginis (keep=gini score_type segment);
			set Comp.Challenger_Models_&month;
			if month = "&month." and (applied_model = "Rebuild_Comp_&prevprevmonth." or applied_model = "Refit_Comp_&prevprevmonth.");
		run;

		data Comp.overallgini_summary_&month;
		    set Comp.overallgini_summary_&month Rebuild_Refit_Ginis;
		run;

		proc sql;
		    create table comp.currentmodel_benchmark_&month as
		        select distinct  a.*,b.gini as Current_gini, (b.gini-a.gini)/a.gini as Relative_lift
		        from comp.overallgini_summary_&month a,
		            (select segment, gini from comp.overallgini_summary_&month
		            where score_type='Compuscan_Prob') b
		        where a.segment=b.segment;
		quit;

		data Comp.currentmodel_benchmark_&month;
		    set Comp.currentmodel_benchmark_&month;
		    format Recommended_Action $500.;
		   
		    if Relative_lift >=-0.10 then do;
		           Recommended_Action = cats("No Action ");
		    end;
		    else if Relative_lift <-0.10 and Relative_lift >-0.15 then do;
		        Recommended_Action =cats("Check Additional Metrics and Establish if a score to Risk Calibration is required");
		    end;
		    else if Relative_lift <-0.15 then do;
		       Recommended_Action =cats("Check Additional Metrics and Establish if a score to Risk Calibration is required");
		    end;
		run;

		data Comp.currentmodel_benchmark_&month.;
		     set Comp.currentmodel_benchmark_&month.;
		     if score_type='Compuscan_Generic' then score_type ='Comp Generic Score';
		     else if score_type='Tu_Generic' then score_type ='TU Generic Score';
		     else if score_type='Compuscan_Prob' then score_type ='V570 Comp Prob';
		     else if score_type='Tu_V570_prob' then score_type ='V570 TU Prob';
		     else if score_type='V6_Prob3' then score_type ='V622 Prob';
		     else if score_type='V635' then score_type ='V635 Prob';
		     else if score_type='V636' then score_type ='V636 Prob';
		     else if score_type='Rebuild_Compuscan' then score_type ='Rebuild Compuscan';
		     else if score_type='Refit_Compuscan' then score_type ='Refit Compuscan';
			 else if score_type='V645' then score_type ='V645 Prob';
		     else if score_type='tu_V580_prob' then score_type ='V580 TU Prob';
		     else if score_type='Compuscan_Prob' then score_type ='V570 Comp Prob';
		run;

		/*** Upload data to cred_scoring ***/
		libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

		proc delete data =cred_scr.Challenger_Models_xml; run;
		proc sql;
			create table cred_scr.Challenger_Models_xml(BULKLOAD=YES) as
				select distinct *
				from comp.challenger_models_&month.;
		quit;

		libname cred_scr odbc dsn=Dev_DDGe schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;

		proc delete data =cred_scr.v5_Gini_relative_xml; run;
		proc sql;
			create table cred_scr.v5_Gini_relative_xml(BULKLOAD=YES) as
				select distinct *
				from Comp.currentmodel_benchmark_&month.;
		quit;

	%end;
%mend;
%Finalise_GiniResults();

%include "&codepath\VarCheckConfidenceBand1.sas";

%include "&codepath\position.sas";

%include "&codepath\plotconfidencebands.sas";

%macro create_CS_Monitoring_report();
	%if %sysfunc(exist(comp.Disbursedbase_&month)) and %sysfunc(exist(comp.Variables_trendreport_&month)) and %sysfunc(exist(comp.Variables_Distribution_&month)) 
	and %sysfunc(exist(comp.Variables_Distribution_&month)) %then %do;
 
	options dlcreatedir;
	libname reports "\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\Reports\v570\&month.";
	%let reports = \\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Compuscan Monitoring\Reports\v570\&month.;

	data Disbursedbase5;
		set comp.Disbursedbase_&month;
	run;

	data disb1 disb2 disb3 disb4 disb5;
	      set Disbursedbase5;
		  seg = comp_seg;
	      if seg = 1 then output disb1;
	      else if seg = 2 then output disb2;
	      else if seg = 3 then output disb3;
	      else if seg = 4 then output disb4;
	      else if seg = 5 then output disb5;
	run;
	/*** INSTITUTION CODE BASES ***/
	data BNKABS BNKCAP BNKFNB BNKNED BNKOTH BNKSTD;
	     SET Disbursedbase5;
	     if INSTITUTIONCODE = 'BNKABS' then output BNKABS;
	     else if INSTITUTIONCODE = 'BNKCAP' then output BNKCAP;
	     else if INSTITUTIONCODE = 'BNKFNB' then output BNKFNB;
	     else if INSTITUTIONCODE = 'BNKNED' then output BNKNED;
	     else if INSTITUTIONCODE = 'BNKOTH' then output BNKOTH;
	     else if INSTITUTIONCODE = 'BNKSTD' then output BNKSTD;
	run;
	%macro splitDataForReport();
		%do seg = 1 to 5 ;				
			data Variables_trendreport_seg&seg
				set comp.Variables_trendreport_&month;
				where segment = &seg;
			run;
			data summarytable_&seg;
				set comp.Variables_Distribution_&month.;
				where segment = &seg;
			run;
			data ginis&seg;
				set comp.variables_gini_summary_&month.;
				where segment=&seg;
			run;
		%end;
	%mend;
	%splitDataForReport;
	
	/*** Create V5 Comp monitoring for segments reports ***/
	%macro buildreport(seg);
	     options nodate nonumber;
	     Title ; 
	     TITLE1; TITLE2;
	     Footnote;
	     Footnote1;
	     Footnote2;
	     options orientation=landscape;
	     ods pdf body = "&reports\V5 Comp monitoring for segment &seg. &month..pdf" ;
		%macro plotpsi(var=,title1=,dset=,xlbl=);
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
		%mend;

		proc sql;
			create table _estimates_ as
				select tranwrd(a.parameter,"_W"," ") as parameter,  b.NAME as scorecardvariable
				from data&seg.._estimates_ a left outer join Lookup.Compscanvar_lookup b
				on  upcase(a.Parameter) = cats(upcase(b.newcolumn),"_W");
	    quit;

		Data _estimates_;
			set _estimates_;
			if parameter = "Intercept" then delete;
			if scorecardvariable = " " then scorecardvariable = parameter;
		run;

	     data _null_ ;
	         set _estimates_  ;
	         rownum= _n_;
	         call symput (compress("X"||rownum),upcase(Parameter));
	         call symput (compress("Y"||rownum),upcase(scorecardvariable));
	         call symput ('NumVars',compress(rownum));
	     run;
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
	           footnote ;

	           ods region y=0.5in x=5in width=4.5in height=4in;

	           Title "Bad Rate Slope";
	           footnote "Latest FirstDueMonth with full outcome"; 
	           proc sgplot data= Variables_trendreport_seg&seg;
	                 where  upcase(VarName) in ("&var._S") and  segment = &seg ;
	                 vbar Bin / response = volume stat = percent  NOSTATLABEL    FILLATTRS=(color = VLIGB ) ;
	                 vline Bin / response=badrate y2axis stat = mean NOSTATLABEL lineattrs= (pattern=solid Thickness = 2 color = gray)   ;
	                 y2axis min = 0  grid;
	                 yaxis min = 0  grid ;
					 xaxis label ='Scores';
				  	keylegend / Title = '' ;
	           run;
	           Title;
	           footnote ;

	           ods region y=4.5in x=0in width=4.5in height=4in;

	           %plotpsi(var=&var,title1=Application Distribution,dset=summarytable_&seg) ;

	           Data Variables_trendreport_seg&seg;
	              set Variables_trendreport_seg&seg;
	              if compress(month) = '.' then delete;
	           run;

	           ods region y=4.5in x=5in width=4.5in height=4in;

	           ods graphics / reset attrpriority=color;
	           Title "Trends Over Time";
	           proc sort data=Variables_trendreport_seg&seg;
	                 by Month;
	           run;
	           proc sgplot data= Variables_trendreport_seg&seg;
	                 where upcase(VarName) in ("&var._S") and segment = &seg ; 
	                 series x=Month y=BadRate / group= BIN  lineattrs= (pattern=solid Thickness = 2  );
	                 xaxis grid ;
	                 yaxis min = 0  grid offsetmin=.05 offsetmax=.05;
					 keylegend / Title = '' ;
	           run;
	           Title;
	           ods LAYOUT END ;
	     %end;
	     ods pdf close;
	%mend;

	%buildreport(1);
	%buildreport(2);
	%buildreport(3);
	%buildreport(4);
	%buildreport(5);

	/*** Create V5 monitoring Full Pack ***/
	options nodate nonumber;
	Title ; 
	TITLE1; TITLE2;
	Footnote;
	Footnote1;
	Footnote2;
	options orientation=landscape;
	ods pdf body = "&reports\V5 monitoring Full Pack &month..pdf"  ;
	     ods pdf startpage = now;
	     ods layout start;
	     ods region y=0in x=0in width=4in height=4in;
	     %VarCheckConfidenceBand1(Disbursedbase5, month, V655 , target , Principaldebt ,0,0,4,4, Overall Model ) ;
	     ods region y=0in x=4.5in width=4in height=4in;
	     %VarCheckConfidenceBand1(Disbursedbase5, seg, V655 , target , Principaldebt ,0,0,4,4, Segments ) ;
	     ods region y=4.5in x=0in width=4in height=4in;
	     %VarCheckConfidenceBand1(Disbursedbase5,Decile , V655 , target , Principaldebt ,0,0,4,4, Overall Decile ) ;
	     ods region y=4.5in x=4.5in width=4in height=4in;
	     %VarCheckConfidenceBand1(Disbursedbase5, V655_RiskGroup, V655 , target , Principaldebt ,0,0,4,4, Risk Group ) ;
	     ods layout end ;
	     ods pdf startpage = now;
	     ods layout start;
	     ods region y=0in x=0in width=4in height=4in;
	     %VarCheckConfidenceBand1(Disbursedbase5, INSTITUTIONCODE , V655 , target , Principaldebt ,0,0,4,4, INSTITUTION CODE) ;
	     ods region y=0in x=4.5in width=4in height=4in;
	     %VarCheckConfidenceBand1(BNKSTD, month, V655 , target , Principaldebt ,0,4.5,4,4, BNKSTD ) ;
	     ods region y=4.5in x=0in width=4in height=4in;
	     %VarCheckConfidenceBand1(BNKFNB, month, V655 , target , Principaldebt ,4.5,0,4,4, BNKFNB ) ;
	     ods region y=4.5in x=4.5in width=4in height=4in;
	     %VarCheckConfidenceBand1(BNKNED, month, V655 , target , Principaldebt ,4.5,4.5,4,4, BNKNED ) ;
	     ods layout end ;
	     ods pdf startpage = now;
	     ods layout start;
	     ods region y=0in x=0in width=4in height=4in;
	     %VarCheckConfidenceBand1(BNKABS, month , V655 , target , Principaldebt ,0,0,4,4, BNKABS) ;
	     ods region y=0in x=4.5in width=4in height=4in;
	     %VarCheckConfidenceBand1(BNKCAP, month, V655 , target , Principaldebt ,0,4.5,4,4, BNKCAP ) ;
	     ods region y=4.5in x=0in width=4in height=4in;
	     %VarCheckConfidenceBand1(BNKOTH, month, V655 , target , Principaldebt ,0,0,4,4, BNKOTH ) ;
	     ods layout end ;

	     ods pdf startpage = now;
	     ods layout start;
	     ods region y=0in x=0in width=4in height=4in;
	     %VarCheckConfidenceBand1(disb1, month , V655 , target , Principaldebt ,0,0,4,4, SEGMENT 1 ) ;
	     ods region y=0in x=4.5in width=4in height=4in;
	     %VarCheckConfidenceBand1(disb2, month, V655 , target , Principaldebt ,0,4.5,4,4, SEGMENT 2 ) ;
	     ods region y=4.5in x=0in width=4in height=4in;
	     %VarCheckConfidenceBand1(disb3, month, V655 , target , Principaldebt ,4.5,0,4,4, SEGMENT 3) ;
	     ods region y=4.5in x=4.5in width=4in height=4in;
	     %VarCheckConfidenceBand1(disb4, month, V655 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 4 ) ;
	     ods layout end ;
	     ods pdf startpage = now;
	     ods layout start;
	     ods region y=0in x=0in width=4in height=4in;
	     %VarCheckConfidenceBand1(disb5, month, V655 , target , Principaldebt ,0,0,4,4, SEGMENT 5 ) ;
	     ods region y=0in x=4.5in width=4in height=4in;
	     %VarCheckConfidenceBand1(disb1, decile_b , V655 , target , Principaldebt ,0,0,4,4, SEGMENT 1: Decile ) ;
	     ods region y=4.5in x=0in width=4in height=4in;
	     %VarCheckConfidenceBand1(disb2, decile_b, V655 , target , Principaldebt ,0,4.5,4,4, SEGMENT 2: Decile ) ;
	     ods region y=4.5in x=4.5in width=4in height=4in;
	     %VarCheckConfidenceBand1(disb3, decile_b, V655 , target , Principaldebt ,4.5,0,4,4, SEGMENT 3: Decile) ;
	     ods layout end ;
	     ods pdf startpage = now;
	     ods layout start;
	     ods region y=0in x=0in width=4in height=4in;
	     %VarCheckConfidenceBand1(disb4, decile_b, V655 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 4: Decile ) ;
	     ods region y=0in x=4.5in width=4in height=4in;
	     %VarCheckConfidenceBand1(disb5, decile_b, V655 , target , Principaldebt ,0,0,4,4, SEGMENT 5: Decile ) ;
	     ods layout end ;

/*V667 report */
 	     ods pdf startpage = now;
	     ods layout start;
	     ods region y=0in x=0in width=4in height=4in;
	     %VarCheckConfidenceBand1(Disbursedbase5, month, V667 , target , Principaldebt ,0,0,4,4, Overall Model ) ;
	     ods region y=0in x=4.5in width=4in height=4in;
	     %VarCheckConfidenceBand1(Disbursedbase5, seg, V667 , target , Principaldebt ,0,0,4,4, Segments ) ;
	     ods region y=4.5in x=0in width=4in height=4in;
	     %VarCheckConfidenceBand1(Disbursedbase5,Decile , V667 , target , Principaldebt ,0,0,4,4, Overall Decile ) ;
	     ods region y=4.5in x=4.5in width=4in height=4in;
	     %VarCheckConfidenceBand1(Disbursedbase5, V667_RiskGroup, V667 , target , Principaldebt ,0,0,4,4, Risk Group ) ;
	     ods layout end ;
	     ods pdf startpage = now;
	     ods layout start;
	     ods region y=0in x=0in width=4in height=4in;
	     %VarCheckConfidenceBand1(Disbursedbase5, INSTITUTIONCODE , V667 , target , Principaldebt ,0,0,4,4, INSTITUTION CODE) ;
	     ods region y=0in x=4.5in width=4in height=4in;
	     %VarCheckConfidenceBand1(BNKSTD, month, V667 , target , Principaldebt ,0,4.5,4,4, BNKSTD ) ;
	     ods region y=4.5in x=0in width=4in height=4in;
	     %VarCheckConfidenceBand1(BNKFNB, month, V667 , target , Principaldebt ,4.5,0,4,4, BNKFNB ) ;
	     ods region y=4.5in x=4.5in width=4in height=4in;
	     %VarCheckConfidenceBand1(BNKNED, month, V667 , target , Principaldebt ,4.5,4.5,4,4, BNKNED ) ;
	     ods layout end ;
	     ods pdf startpage = now;
	     ods layout start;
	     ods region y=0in x=0in width=4in height=4in;
	     %VarCheckConfidenceBand1(BNKABS, month , V667 , target , Principaldebt ,0,0,4,4, BNKABS) ;
	     ods region y=0in x=4.5in width=4in height=4in;
	     %VarCheckConfidenceBand1(BNKCAP, month, V667 , target , Principaldebt ,0,4.5,4,4, BNKCAP ) ;
	     ods region y=4.5in x=0in width=4in height=4in;
	     %VarCheckConfidenceBand1(BNKOTH, month, V667 , target , Principaldebt ,0,0,4,4, BNKOTH ) ;
	     ods layout end ;

	     ods pdf startpage = now;
	     ods layout start;
	     ods region y=0in x=0in width=4in height=4in;
	     %VarCheckConfidenceBand1(disb1, month , V667 , target , Principaldebt ,0,0,4,4, SEGMENT 1 ) ;
	     ods region y=0in x=4.5in width=4in height=4in;
	     %VarCheckConfidenceBand1(disb2, month, V667 , target , Principaldebt ,0,4.5,4,4, SEGMENT 2 ) ;
	     ods region y=4.5in x=0in width=4in height=4in;
	     %VarCheckConfidenceBand1(disb3, month, V667 , target , Principaldebt ,4.5,0,4,4, SEGMENT 3) ;
	     ods region y=4.5in x=4.5in width=4in height=4in;
	     %VarCheckConfidenceBand1(disb4, month, V667 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 4 ) ;
	     ods layout end ;
	     ods pdf startpage = now;
	     ods layout start;
	     ods region y=0in x=0in width=4in height=4in;
	     %VarCheckConfidenceBand1(disb5, month, V667 , target , Principaldebt ,0,0,4,4, SEGMENT 5 ) ;
	     ods region y=0in x=4.5in width=4in height=4in;
	     %VarCheckConfidenceBand1(disb1, decile_b , V667 , target , Principaldebt ,0,0,4,4, SEGMENT 1: Decile ) ;
	     ods region y=4.5in x=0in width=4in height=4in;
	     %VarCheckConfidenceBand1(disb2, decile_b, V667 , target , Principaldebt ,0,4.5,4,4, SEGMENT 2: Decile ) ;
	     ods region y=4.5in x=4.5in width=4in height=4in;
	     %VarCheckConfidenceBand1(disb3, decile_b, V667 , target , Principaldebt ,4.5,0,4,4, SEGMENT 3: Decile) ;
	     ods layout end ;
	     ods pdf startpage = now;
	     ods layout start;
	     ods region y=0in x=0in width=4in height=4in;
	     %VarCheckConfidenceBand1(disb4, decile_b, V667 , target , Principaldebt ,4.5,4.5,4,4, SEGMENT 4: Decile ) ;
	     ods region y=0in x=4.5in width=4in height=4in;
	     %VarCheckConfidenceBand1(disb5, decile_b, V667 , target , Principaldebt ,0,0,4,4, SEGMENT 5: Decile ) ;
	     ods layout end ;

	     %buildscorecardreport(1);
	     %plotconfidencebands(inputdata=disb1, segment=1);
	     %buildscorecardreport(2);
	     %plotconfidencebands(inputdata=disb2, segment=2);
	     %buildscorecardreport(3);
	     %plotconfidencebands(inputdata=disb3, segment=3);
	     %buildscorecardreport(4);
	     %plotconfidencebands(inputdata=disb4, segment=4);
	     %buildscorecardreport(5);
	     %plotconfidencebands(inputdata=disb5, segment=5);
	ods pdf close;
%mend;
