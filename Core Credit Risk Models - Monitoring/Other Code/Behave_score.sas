options compress = binary ;

/*libname Tue '\\mpwsas4\Final New Application behavescore\data1';*/
libname data "\\mpwsas5\G\Automation\Behavescore\Datasets";

/*SOURCE THE INPUT DATA */
libname Prd_DDDa odbc dsn=PRD_DDDa schema='dbo' preserve_tab_names=yes connection=unique direct_sql=yes;
libname pre "\\neptune\sasa$\V5\Behavioral Model\Pre Development data";
libname fix "\\mpwsas5\G\Automation\Behavescore\Datasets";

data _null_;
	call symput('last12month', put(intnx('month' ,today(),-12,'begin'),yymmn6.));
	call symput('last12monthdate9', put(intnx('month' ,today(),-12,'begin'),date9.));
	call symput('tday', put(intnx('day',today(),0),yymmddn8.));
	call symput('runmonth',put(intnx('month',today(),0),date9.));
run;
%put &last12month;
%put &last12monthdate9;
%put &tday;
%put &runmonth;


/* Create the date partition file using the previous run date */
%macro createdatelist();
	%if %sysfunc(exist(data.datepartition)) %then %do;
		data datepart;
			AppDate=today();
			output;
		run;
		proc append base = data.datepartition data = datepart force; run;
	%end;
	%else %do;
		data data.datepartition ;
			Format AppDate date9.  ;
			AppDate = '05JAN2017'd  ;  output;
			AppDate = '15FEB2017'd  ;  output;
			AppDate = '17MAR2017'd  ;  output;
			AppDate = '12APR2017'd  ;  output;
			AppDate = '12MAY2017'd  ;  output;
			AppDate = '12JUN2017'd  ;  output;
			AppDate = '12JUL2017'd  ;  output;
			AppDate = '10AUG2017'd  ;  output;
			AppDate = '14SEP2017'd  ;  output;
			AppDate = '18OCT2017'd  ;  output;
			AppDate = '16NOV2017'd  ;  output;
			AppDate = '19DEC2017'd  ;  output;
			AppDate = '18JAN2018'd  ;  output;
			AppDate = '28FEB2018'd  ;  output;
			AppDate = '16MAR2018'd  ;  output;
			AppDate = '04APR2018'd  ;  output;
			AppDate = '28MAY2018'd  ;  output;
			AppDate = '29JUN2018'd  ;  output;
			AppDate = '30JUL2018'd  ;  output;
			AppDate = '07AUG2018'd  ;  output;
			AppDate = '14AUG2018'd  ;  output;
			AppDate = '17SEP2018'd  ;  output;
			AppDate = '10OCT2018'd  ;  output;
			AppDate = '12NOV2018'd  ;  output;
			AppDate = '06DEC2018'd  ;  output;
			AppDate = '11JAN2019'd  ;  output;
		run;
		data datepart;
			AppDate=today();
			output;
		run;
		proc append base = data.datepartition data = datepart force; run;
	%end;
%mend;
%createdatelist;

proc freq data = data.datepartition;
	tables appdate;
run;

data datepartition1;
	set data.datepartition;
	info_date = put(lag(AppDate),yymmddn8.);
	if compress(info_date) = '.' then info_date ='20161215';
run;

************RUN BEHAVE MODEL FOR THE LATEST MONTH ***************** ;

%macro MOM(Prev,Curr) ;
	data PrevMonth ;
		set &Prev ;
		keep idno BehaveDecile  ;
	run;
	data CurrMonth ;
		set &Curr ;
		keep idno BehaveDecile ;
	run;
	proc sort data = PrevMonth nodupkey ;
		by idno ;
	run;
	proc sort data = CurrMonth nodupkey ;
		by idno ;
	run;
	data Venn ;
		merge PrevMonth (in = a rename = ( BehaveDecile = PrevBehaveDecile) )  CurrMonth (in = b) ;
		by idno ;
		if a or b ;
		if a = 1 then PrevMonth = 'Y';
		else PrevMonth = 'N';
		if b = 1 then CurrMonth = 'Y';
		else CurrMonth = 'N';
	run;

	data Venn2 ;
		set Venn ;
		Shift = BehaveDecile - PrevBehaveDecile ;
		if PrevMonth = 'Y' and CurrMonth = 'Y' ;
	run;

	proc summary data = Venn2 nway missing ;
		class shift ;
		output out = summary5;
	run;
%mend;

%macro calcPSI(In1,In2,var,Out,Period) ;
	proc sql noprint ;
		create table base as 
			select a.&var , (sum(_freq_) / (select sum(_freq_) from  &In1)) as basePercent format Percent8.2 
			from &In1 a 
			group by a.&Var ;
	quit;

	proc sql noprint ;
		create table actuals as 
			select b.&Period as Month , b.&var , (sum(_freq_)/ (select sum(_freq_) from  &In2 a  
			                                     where a.&Period = b.&Period )) as ActualPercent format Percent8.2 
			from &In2  b 
			group by b.&Period , b.&Var ;
	quit;
	 
	proc sql noprint ;
		create table temp as 
			select "&var" as VariableName , b.month , sum((ActualPercent - basePercent)*log(ActualPercent/basePercent)) as PSI
			from base a 
			left join actuals b 
			on a.&var = b.&var
			group by  VariableName , b.month;
	quit;

	proc sql noprint ;
		create table &out as 
			select B.VariableName format $32. , A.month , A.&var as Bin , ActualPercent as Percentage , b.PSI , 0.1 as MarginallyStable ,  0.25 as Unstable 
			from actuals A 
			left join temp B 
			on A.month = B.Month;
	quit;

	data &out ;
		format VariableName $32. Bin $500. ;
		set &out ;
	run;

	data &out ;
		set &out base (in = b rename = (&Var = Bin basePercent = Percentage ) ) ;
		if b then do ;
			Month = " BUILD";
			VariableName = "&Var";
		end;
	run;
	proc append base = AppStagePSI data = &out;
	quit;
	proc delete data = &out ;
	run;
%mend;

%macro runme3(RunDate);
	data _null_ ;
		call symput('Today',put(today(),yymmddn8.));
	run;

	data _null_;
		Abildata = "Abildata";
		RunDate2 =  put(intnx('Months',input(compress(&Rundate),yymmdd8.),-1),yymmddn8.);
		X1=substr(Compress(Rundate2),1,4);
		X2=substr(Compress(Rundate2),5,2);
		call symput('Date',compress("'"||&RunDate||"'"));
		if &Today = &Rundate then do ;
		call symput('Source',Abildata);
		end;
		else do ;
		call symput('Source',compress('Abildata_'||X1||'_'||X2));
		end;
	run;
	%put &Date &Source;

	proc sql stimer;
	    connect to ODBC (dsn=PRD_DDDa);
	EXECUTE ( 


		IF OBJECT_ID('Prd_datadistillery_data.dbo.JS_INPUTBASE', 'U') IS NOT NULL DROP TABLE prd_datadistillery_data.dbo.JS_INPUTBASE; 
		create table Prd_datadistillery_data.dbo.JS_INPUTBASE  with (DISTRIBUTION = hash(idno),clustered columnstore index ) as  
		select idno 
		from  edwdw.dbo.&Source  WHERE StartDate > 0  UNION  
		select idno  from Prd_datadistillery_data.dbo.JS_Abildata_20160507    ;

		IF OBJECT_ID('Prd_datadistillery_data.dbo.JS_Abil', 'U') IS NOT NULL DROP TABLE Prd_datadistillery_data.dbo.JS_Abil; 
		create table Prd_datadistillery_data.dbo.JS_Abil  with (DISTRIBUTION = hash(idno),clustered columnstore index ) as  
		select idno , min(DisbStartDate) as min_DisbDate , max(DisbStartDate)  as max_DisbDate
		from  Prd_datadistillery_data.dbo.JS_INPUTBASE  A  
		left join Prd_datadistillery_data.dbo.Disbursement_Info B 
		on A.idno = B.idnumber and B.Create_Date <= cast(&Date as date) 
		group by idno

		IF OBJECT_ID('Prd_datadistillery_data.dbo.JS_Arrears3', 'U') IS NOT NULL DROP TABLE Prd_datadistillery_data.dbo.JS_Arrears3; 
		    create table Prd_datadistillery_data.dbo.JS_Arrears3  with (DISTRIBUTION = REPLICATE,clustered columnstore index ) as
				select A.idno ,         
		          sum(case when Contractualcd = 0 then 1 else 0 end) as no_zero_arrear_24_2
		          from (
		                                    select  A.idnumber as idno , 
		                                    b.runmonth , max(Contractualcd) as Contractualcd
		                                    from  Prd_datadistillery_data.dbo.Disbursement_Info  A  
											left join  Provisions..ContractualCD_Final  b
		                                    on a.loanreference = b.loanref
		                                    where   b.runmonth >= cast(DATEADD(month,-25, cast(&Date as date)) as date) 
		                                    and b.runmonth <= cast(DATEADD(month,-1, cast(&Date as date)) as date) 
		                                    and b.balance > 0 and DisbStartDate > 0 
		                                    group by  A.idnumber , 
		                b.runmonth
		              ) as a
		         group by A.idno ;
		                                           
		       IF OBJECT_ID('Prd_datadistillery_data.dbo.JS_MaxCD6 ', 'U') IS NOT NULL DROP TABLE Prd_datadistillery_data.dbo.JS_MaxCD6; 
			   create table Prd_datadistillery_data.dbo.JS_MaxCD6  with (DISTRIBUTION = REPLICATE,clustered columnstore index ) as  
		       select A.idnumber as IDNO , max (b.CONTRACTUALCD)  as Maxcd6
		       from  Prd_datadistillery_data.dbo.Disbursement_Info A  
		       left join (select loanref,Runmonth,Contractualcd,STMT_NR from  Provisions..ContractualCD_Final ) B
		       on A.Loanreference = b.Loanref
		       where   b.runmonth >= cast(DATEADD(month,-7,cast(&Date as date)) as date) 
		       and b.runmonth <= cast(DATEADD(month,-1,cast(&Date as date)) as date) and a.DisbStartDate > 0
		       group by A.idnumber ;
												
		       IF OBJECT_ID('Prd_datadistillery_data.dbo.JS_MaxCDever ', 'U') IS NOT NULL DROP TABLE Prd_datadistillery_data.dbo.JS_MaxCDever; 
		       create table Prd_datadistillery_data.dbo.JS_MaxCDever  with (DISTRIBUTION = REPLICATE,clustered columnstore index ) as  
		       select A.idnumber as IDNo  ,max (b.CONTRACTUALCD)  as MaxcdEVER
		       from Prd_datadistillery_data.dbo.Disbursement_Info A 
		       left join (select loanref,Runmonth,Contractualcd,STMT_NR from  Provisions..ContractualCD_Final ) B
		       on a.loanreference = b.loanref
		       where  b.runmonth <= cast(DATEADD(month,-1, cast(&Date as date)) as date)  and a.DisbStartDate > 0
		       group by  A.idnumber  ;

		       IF OBJECT_ID('Prd_datadistillery_data.dbo.JS_Paid ', 'U') IS NOT NULL DROP TABLE Prd_datadistillery_data.dbo.JS_Paid; 
			   create table Prd_datadistillery_data.dbo.JS_Paid   with (DISTRIBUTION = REPLICATE,clustered columnstore index ) as 
		         select  A.idnumber as IDNo,
		         sum (b.EFF_RECEIPT*-1) as EFF_RECEIPT ,
		         sum (b.EVENINSTALMENT) as EVENINSTALMENT ,
		               sum(case when eff_receipt*-1>b.eveninstalment then b.eveninstalment else eff_receipt*-1 end) as capped_eff_receipt
		         from Prd_datadistillery_data.dbo.Disbursement_Info a left join provisions.dbo.payments_table b
		         on a.loanreference = b.loanref
		         where  b.runmonth >= cast(DATEADD(month,-13,cast(&Date as date)) as date) 
		         and b.runmonth <= cast(DATEADD(month,-1,cast(&Date as date)) as date) and a.DisbStartDate > 0  
		         group by A.idnumber;
		                             
				 IF OBJECT_ID('Prd_datadistillery_data.dbo.JS_paid2owed12 ', 'U') IS NOT NULL DROP TABLE Prd_datadistillery_data.dbo.JS_paid2owed12;   
		        create table Prd_datadistillery_data.dbo.JS_paid2owed12  with (DISTRIBUTION = REPLICATE,clustered columnstore index ) as 
		         select  A.IDNo,
		         sum(case when EVENINSTALMENT > 0 then (EFF_RECEIPT/EVENINSTALMENT) else -7 end) as paid2owed12_V2,
		         sum(case when EVENINSTALMENT > 0 then (capped_eff_receipt/EVENINSTALMENT) else -7 end) as paid2owed12_V3 
		         from Prd_datadistillery_data.dbo.JS_Paid  A
		         group by A.IDNo;

										
		       IF OBJECT_ID('Prd_datadistillery_data.dbo.JS_ArrearsCodeBucketed ', 'U') IS NOT NULL DROP TABLE Prd_datadistillery_data.dbo.JS_ArrearsCodeBucketed; 
			create table  Prd_datadistillery_data.dbo.JS_ArrearsCodeBucketed with (DISTRIBUTION = REPLICATE,clustered columnstore index ) as 
		select
		IDNo , 
		max( case when ReasonCode in ('140','528','131','360','172','150') then 5
		          when ReasonCode in ('149','135','549','A01','532','102','436','169','361','164','148','847','145','423','523' ) then 4
				  when ReasonCode in ('480','163','845','144','058','143','173','171','FM1','174','136','267','170') then 3 
		          when ReasonCode in ('844''059','133','001','499','166','548','380') then 2
				  when ReasonCode in ('190''142','137','178','139','918','X01','B20','160','527','116','A02','500','533','362','286','153','054') then 1
				  when ReasonCode in ('146','320','B13','204') then 1 
		      else 3 end)  as MaxArrearsCode 
		from  edwdw.dbo.&Source a 
		where a.startdate > 0
		group by IDNo;

		)
		by odbc;
	quit;

	proc sql stimer;
	    connect to ODBC (dsn=MPWAPS);
		create table Abil_base  as 
	    select * from connection to odbc ( select idno from  Prd_datadistillery_data.dbo.JS_INPUTBASE ) ;
	disconnect from odbc ;
	quit;

	proc sort data = Abil_base nodupkey;
		by IDNo;
	run;

	/*First and last days since disbursement*/

	%macro sourcedata(TableName);
		proc sql stimer;
		    connect to ODBC (dsn=MPWAPS);
			create table  &TableName as 
		    select * from connection to odbc (
				select distinct * from Prd_datadistillery_data.dbo.&TableName order by IDNo ) ;
			disconnect from odbc ;
		quit;

		proc sort data  = &TableName ;
			by idno ;
		run;
	%mend;

	%sourcedata(TableName = JS_Abil);
	%sourcedata(TableName = JS_Arrears3);
	%sourcedata(TableName = JS_MaxCD6);
	%sourcedata(TableName = JS_MaxCDever);
	%sourcedata(TableName = JS_paid2owed12);
	%sourcedata(TableName = JS_ArrearsCodeBucketed);

	data fix.Abil_base&Rundate ;
	merge Abil_base (in = a) JS_Abil  JS_Arrears3  JS_MaxCD6  JS_MaxCDever  JS_paid2owed12  JS_ArrearsCodeBucketed ;
	by idno ;
	if a ;
	if IDNo in ('','0') then delete;
	run;

	%let Target = CONTRACTUAL_3_LE9 ;

	data fix.Behave_&Rundate ;
	set fix.Abil_base&Rundate ;

	Maxcd = MaxcdEVER ; 
	if Maxcd = . then NoPaymentInfo = 1 ;
	else NoPaymentInfo = 0  ;

	if max_DisbDate = . then  DAYSSINCELASTABDISB = -9 ;
	else DAYSSINCELASTABDISB = intck('Days',input(compress(put(max_DisbDate,8.)),yymmdd8.),input(compress(&Rundate),yymmdd8.)) ;

	if DAYSSINCELASTABDISB < - 9 then DAYSSINCELASTABDISB = -8 ;
	else if DAYSSINCELASTABDISB < 0 and DAYSSINCELASTABDISB ne -9  then DAYSSINCELASTABDISB = -8 ;


	if min_DisbDate = . then  DAYSSINCEFIRSTABDISB = -9 ;
	else DAYSSINCEFIRSTABDISB = intck('Days',input(compress(put(min_DisbDate,8.)),yymmdd8.),input(compress(&Rundate),yymmdd8.)) ;

	if DAYSSINCEFIRSTABDISB < - 9 then DAYSSINCEFIRSTABDISB = -8 ;
	else if DAYSSINCEFIRSTABDISB < 0 and DAYSSINCEFIRSTABDISB ne -9  then DAYSSINCEFIRSTABDISB = -8 ;

	if  no_zero_arrear_24_2 = . and NoPaymentInfo = 1 then no_zero_arrear_24_2 =  -9 ;
	else if no_zero_arrear_24_2 = .  then no_zero_arrear_24_2 = -8 ;
	else no_zero_arrear_24_2 = no_zero_arrear_24_2 ;


	if Maxcd = . then Maxcd = -9 ;
	if Maxcd6 = . then Maxcd6 = -9 ;

	if paid2owed12_V3 = -7 then paid2owed12_V3 = -7 ;
	else if (paid2owed12_V3 = . and  NoPaymentInfo = 1) then paid2owed12_V3 = - 9 ;
	else if (paid2owed12_V3 = .) then paid2owed12_V3 = - 8 ;
	else if paid2owed12_V3 < 0 then paid2owed12_V3 = -6 ;
	else paid2owed12_V3 = paid2owed12_V3 ;

	if max_DisbDate = . then paid2owed12_V3 = - 9 ; 


	if MaxArrearsCode = . then MaxArrearsCode = -1 ;


	format Maxcd6B $2. MaxcdB $2.  MAXCD_6_EVER $2. ;

	if (Maxcd6 >= 0 and Maxcd6 <= 0) then Maxcd6B = 'L';
	else if ((Maxcd6 >= 0 and Maxcd6 <= 1) or (Maxcd6 in (-9))) then Maxcd6B = 'M';
	Else If (Maxcd6 > 1)  then  Maxcd6B = 'H';

	If (Maxcd >= 0 and Maxcd <= 0)  then MaxcdB = 'L' ;
	Else If ((Maxcd >= 0 and Maxcd <= 2) or (Maxcd in (-9)) ) then MaxcdB = 'M';
	Else If (Maxcd > 2)  then MaxcdB = 'H';

	MAXCD_6_EVER = compress(Maxcd6B||MaxcdB);

	Format DAYSSINCEFIRSTABDISB_B $500. ;
	If (DAYSSINCEFIRSTABDISB >= 0 and DAYSSINCEFIRSTABDISB <= 283)  then DAYSSINCEFIRSTABDISB_B = "B1: DAYSSINCEFIRSTABDISB  <= 283";
	Else If ( DAYSSINCEFIRSTABDISB >= 0 and DAYSSINCEFIRSTABDISB <= 682) or DAYSSINCEFIRSTABDISB in (-9,-8)  then DAYSSINCEFIRSTABDISB_B = "B2: DAYSSINCEFIRSTABDISB  <= 682 or DAYSSINCEFIRSTABDISB in (-9,-8)";
	Else If (DAYSSINCEFIRSTABDISB > 682)  then DAYSSINCEFIRSTABDISB_B = "B3: DAYSSINCEFIRSTABDISB  > 682";

	if  DAYSSINCEFIRSTABDISB_b ="B1: DAYSSINCEFIRSTABDISB  <= 283" then  DAYSSINCEFIRSTABDISB_W =0.4483981785;
	if  DAYSSINCEFIRSTABDISB_b ="B2: DAYSSINCEFIRSTABDISB  <= 682 or DAYSSINCEFIRSTABDISB in (-9,-8)" then  DAYSSINCEFIRSTABDISB_W =0.1122815977;
	if  DAYSSINCEFIRSTABDISB_b ="B3: DAYSSINCEFIRSTABDISB  > 682" then  DAYSSINCEFIRSTABDISB_W =-0.16021760566667;

	Format DAYSSINCELASTABDISB_B $500. ;
	If (DAYSSINCELASTABDISB >= 0 and DAYSSINCELASTABDISB <= 78)  then DAYSSINCELASTABDISB_B = "B1: DAYSSINCELASTABDISB  <= 78";
	Else If (DAYSSINCELASTABDISB >= 0 and DAYSSINCELASTABDISB <= 269)  then DAYSSINCELASTABDISB_B = "B2: DAYSSINCELASTABDISB  <= 269";
	Else If ( DAYSSINCELASTABDISB >= 0 and DAYSSINCELASTABDISB <= 572) or DAYSSINCELASTABDISB in (-9)  then DAYSSINCELASTABDISB_B = "B3: DAYSSINCELASTABDISB  <= 572 or DAYSSINCELASTABDISB in (-9)";
	Else If (DAYSSINCELASTABDISB > 572) or DAYSSINCELASTABDISB in (-8)  then DAYSSINCELASTABDISB_B = "B4: DAYSSINCELASTABDISB  > 572 or DAYSSINCELASTABDISB in (-8)";

	if  DAYSSINCELASTABDISB_b ="B1: DAYSSINCELASTABDISB  <= 78" then  DAYSSINCELASTABDISB_W =0.4480144773;
	if  DAYSSINCELASTABDISB_b ="B2: DAYSSINCELASTABDISB  <= 269" then  DAYSSINCELASTABDISB_W =0.17477599;
	if  DAYSSINCELASTABDISB_b ="B3: DAYSSINCELASTABDISB  <= 572 or DAYSSINCELASTABDISB in (-9)" then  DAYSSINCELASTABDISB_W =-0.03983485346554;
	if  DAYSSINCELASTABDISB_b ="B4: DAYSSINCELASTABDISB  > 572 or DAYSSINCELASTABDISB in (-8)" then  DAYSSINCELASTABDISB_W =-0.22260395806496;

	Format MaxArrearsCode_B $500. ;
	If ( MaxArrearsCode >= 0 and MaxArrearsCode <= 3) or MaxArrearsCode in (-1)  then MaxArrearsCode_B = "B1: MaxArrearsCode  <= 3 or MaxArrearsCode in (-1)";
	Else If (MaxArrearsCode >= 0 and MaxArrearsCode <= 4)  then MaxArrearsCode_B = "B2: MaxArrearsCode  <= 4";
	Else If (MaxArrearsCode > 4)  then MaxArrearsCode_B = "B3: MaxArrearsCode  > 4";


	if  MaxArrearsCode_b ="B1: MaxArrearsCode  <= 3 or MaxArrearsCode in (-1)" then  MaxArrearsCode_W =-0.06080661097787;
	if  MaxArrearsCode_b ="B2: MaxArrearsCode  <= 4" then  MaxArrearsCode_W =0.3687410096;
	if  MaxArrearsCode_b ="B3: MaxArrearsCode  > 4" then  MaxArrearsCode_W =0.7823304729;

	Format MAXCD_6_EVER_B $100. ;
	if  compress(MAXCD_6_EVER) in ('HH','MH','LH','HM') then MAXCD_6_EVER_B ="B1: MAXCD_6_EVER in ('HH','MH','LH','HM')";
	else if  compress(MAXCD_6_EVER) in ('MM') then MAXCD_6_EVER_B ="B2: MAXCD_6_EVER in ('MM')";
	else if  compress(MAXCD_6_EVER) in ('LL','LM','ML') then MAXCD_6_EVER_B ="B3: MAXCD_6_EVER in ('LL','LM','ML')";
	else MAXCD_6_EVER_B ="B4: (UNKNOWN)";

	if  MAXCD_6_EVER_b ="B1: MAXCD_6_EVER in ('HH','MH','LH','HM')" then  MAXCD_6_EVER_W =0.5276953109;
	if  MAXCD_6_EVER_b ="B2: MAXCD_6_EVER in ('MM')" then  MAXCD_6_EVER_W =0.0673267327;
	if  MAXCD_6_EVER_b ="B3: MAXCD_6_EVER in ('LL','LM','ML')" then  MAXCD_6_EVER_W =-0.22942201090208;

	Format no_zero_arrear_24_2_B $500. ;
	If ( no_zero_arrear_24_2 >= 0 and no_zero_arrear_24_2 <= 10) or no_zero_arrear_24_2 in (-8)  then no_zero_arrear_24_2_B = "B1: no_zero_arrear_24_2  <= 10 or no_zero_arrear_24_2 in (-8)";
	Else If ( no_zero_arrear_24_2 >= 0 and no_zero_arrear_24_2 <= 20) or no_zero_arrear_24_2 in (-9)  then no_zero_arrear_24_2_B = "B2: no_zero_arrear_24_2  <= 20 or no_zero_arrear_24_2 in (-9)";
	Else If (no_zero_arrear_24_2 >= 0 and no_zero_arrear_24_2 <= 23)  then no_zero_arrear_24_2_B = "B3: no_zero_arrear_24_2  <= 23";
	Else If (no_zero_arrear_24_2 > 23)  then no_zero_arrear_24_2_B = "B4: no_zero_arrear_24_2  > 23";

	if  no_zero_arrear_24_2_b ="B1: no_zero_arrear_24_2  <= 10 or no_zero_arrear_24_2 in (-8)" then  no_zero_arrear_24_2_W =0.3190078846;
	if  no_zero_arrear_24_2_b ="B2: no_zero_arrear_24_2  <= 20 or no_zero_arrear_24_2 in (-9)" then  no_zero_arrear_24_2_W =-0.02744387027889;
	if  no_zero_arrear_24_2_b ="B3: no_zero_arrear_24_2  <= 23" then  no_zero_arrear_24_2_W =-0.35295966583927;
	if  no_zero_arrear_24_2_b ="B4: no_zero_arrear_24_2  > 23" then  no_zero_arrear_24_2_W =-0.74154867065036;

	Format paid2owed12_V3_B $500. ;
	If (paid2owed12_V3 >= 0 and paid2owed12_V3 <= 0.71614)  then paid2owed12_V3_B = "B1: paid2owed12_V3  <= 0.71614";
	Else If ( paid2owed12_V3 >= 0 and paid2owed12_V3 <= 0.833333) or paid2owed12_V3 in (-6)  then paid2owed12_V3_B = "B2: paid2owed12_V3  <= 0.833333 or paid2owed12_V3 in (-6)";
	Else If ( paid2owed12_V3 >= 0 and paid2owed12_V3 <= 0.948402) or paid2owed12_V3 in (-8,-7,-9)  then paid2owed12_V3_B = "B3: paid2owed12_V3  <= 0.948402 or paid2owed12_V3 in (-8,-7,-9)";
	Else If (paid2owed12_V3 > 0.948402)  then paid2owed12_V3_B = "B4: paid2owed12_V3  > 0.948402";

	if  paid2owed12_V3_b ="B1: paid2owed12_V3  <= 0.71614" then  paid2owed12_V3_W =0.4596062185;
	if  paid2owed12_V3_b ="B2: paid2owed12_V3  <= 0.833333 or paid2owed12_V3 in (-6)" then  paid2owed12_V3_W =0.2186548149;
	if  paid2owed12_V3_b ="B3: paid2owed12_V3  <= 0.948402 or paid2owed12_V3 in (-8,-7,-9)" then  paid2owed12_V3_W =-0.02905939318573;
	if  paid2owed12_V3_b ="B4: paid2owed12_V3  > 0.948402" then  paid2owed12_V3_W =-0.38822452612736;

	*****************************************;
	** SAS Prd_datadistillery_data Code for PROC Hplogistic;
	*****************************************;

	length I_Target $ 12;
	label I_Target = 'Into: Target' ;
	label U_Target = 'Unnormalized Into: Target' ;

	label P_Target1 = 'Predicted: Target=1' ;
	label P_Target0 = 'Predicted: Target=0' ;

	drop _LMR_BAD;
	_LMR_BAD=0;

	*** Check no_zero_arrear_24_2_W for missing values;
	if missing(no_zero_arrear_24_2_W) then do;
	   _LMR_BAD=1;
	   goto _SKIP_000;
	end;

	*** Check MAXCD_6_EVER_W for missing values;
	if missing(MAXCD_6_EVER_W) then do;
	   _LMR_BAD=1;
	   goto _SKIP_000;
	end;

	*** Check paid2owed12_V3_W for missing values;
	if missing(paid2owed12_V3_W) then do;
	   _LMR_BAD=1;
	   goto _SKIP_000;
	end;

	*** Check DAYSSINCEFIRSTABDISB_W for missing values;
	if missing(DAYSSINCEFIRSTABDISB_W) then do;
	   _LMR_BAD=1;
	   goto _SKIP_000;
	end;

	*** Check DAYSSINCELASTABDISB_W for missing values;
	if missing(DAYSSINCELASTABDISB_W) then do;
	   _LMR_BAD=1;
	   goto _SKIP_000;
	end;

	*** Check MaxArrearsCode_W for missing values;
	if missing(MaxArrearsCode_W) then do;
	   _LMR_BAD=1;
	   goto _SKIP_000;
	end;

	*** Compute Linear Predictors;
	drop _LP0;
	_LP0 = 0;

	_LP0 = _LP0 + (0.29833386937663) * no_zero_arrear_24_2_W;
	_LP0 = _LP0 + (0.97483077987822) * MAXCD_6_EVER_W;
	_LP0 = _LP0 + (0.49034913622463) * paid2owed12_V3_W;
	_LP0 = _LP0 + (0.91284162381658) * DAYSSINCEFIRSTABDISB_W;
	_LP0 = _LP0 + (0.805438826058) * DAYSSINCELASTABDISB_W;
	_LP0 = _LP0 + (0.61344790679371) * MaxArrearsCode_W;

	*** Predicted values;
	drop _MAXP _IY _P0 _P1;
	_TEMP = -1.5766054420779  + _LP0;
	if (_TEMP < 0) then do;
	   _TEMP = exp(_TEMP);
	   _P0 = _TEMP / (1 + _TEMP);
	end;
	else _P0 = 1 / (1 + exp(-_TEMP));
	_P1 = 1.0 - _P0;
	P_Target1 = _P0;
	_MAXP = _P0;
	_IY = 1;
	P_Target0 = _P1;
	if (_P1 >  _MAXP + 1E-8) then do;
	   _MAXP = _P1;
	   _IY = 2;
	end;
	select( _IY );
	   when (1) do;
	      I_Target = '1' ;
	      U_Target = 1;
	   end;
	   when (2) do;
	      I_Target = '0' ;
	      U_Target = 0;
	   end;
	   otherwise do;
	      I_Target = '';
	      U_Target = .;
	   end;
	end;
	_SKIP_000:
	if _LMR_BAD = 1 then do;
	I_Target = '';
	U_Target = .;
	P_Target1 = .;
	P_Target0 = .;
	end;
	drop _TEMP;

	final_Score = P_Target1 ;


	BehaveScore = (1000-ceil(final_score*1000));

	if BehaveScore <= 	728	then BehaveDecile = 	1	;
	else if BehaveScore <= 	770	then BehaveDecile = 	2	;
	else if BehaveScore <= 	799	then BehaveDecile = 	3	;
	else if BehaveScore <= 	818	then BehaveDecile = 	4	;
	else if BehaveScore <= 	837	then BehaveDecile = 	5	;
	else if BehaveScore <= 	858	then BehaveDecile = 	6	;
	else if BehaveScore <= 	878	then BehaveDecile = 	7	;
	else if BehaveScore <= 	890	then BehaveDecile = 	8	;
	else if BehaveScore <= 	917	then BehaveDecile = 	9	;
	else if BehaveScore <= 	1000	then BehaveDecile = 	10	;

	RunDate = "&RunDate";
	run;
%mend;

%macro VolumeComparison(Prev,Curr) ;

	data PrevMonth ;
		set &Prev ;
		keep idno ;
	run;

	data CurrMonth ;
		set &Curr ;
		keep idno ;
	run;
	proc sort data = PrevMonth nodupkey ;
		by idno ;
	run;

	proc sort data = CurrMonth nodupkey ;
		by idno ;
	run;

	data Venn ;
		merge PrevMonth (in = a )  CurrMonth (in = b) ;
		by idno ;
		if a or b ;
		if a = 1 then PrevMonth = 'Y';
		else PrevMonth = 'N';
		if b = 1 then CurrMonth = 'Y';
		else CurrMonth = 'N';
	run;
%mend;

/*%runme3(RunDate=20180316);*/
/*%runme3(RunDate=20150915); */
/*%runme3(RunDate=20151115); */
/*%runme3(RunDate=20151015); */
/*%runme3(RunDate=20151215); */
/*%runme3(RunDate=20160115); */
/*%runme3(RunDate=20160215); */
/*%runme3(RunDate=20160315);*/
/*%runme3(RunDate=20160415);*/
/*%runme3(RunDate=20160515);*/
/*%runme3(RunDate=20160615);*/
/*%runme3(RunDate=20160715);*/
/*%runme3(RunDate=20160815);*/
/*%runme3(RunDate=20160915);*/
/*%runme3(RunDate=20161015);*/
/*%runme3(RunDate=20161115);*/
/*%runme3(RunDate=20161215);*/
/*%runme3(RunDate=20170115);*/
/*%runme3(RunDate=20170215);*/
/*%runme3(RunDate=20170317);*/
/*%runme3(RunDate=20170412);*/
/*%runme3(RunDate=20170512);*/
/*%runme3(RunDate=20170612);*/
/*%runme3(RunDate=20170712);*/
/*%runme3(RunDate=20170810);*/
/*%runme3(RunDate=20170914);*/
/*%runme3(RunDate=20171018);*/
/*%runme3(RunDate=20171116);*/
/*%runme3(RunDate=20171219);*/
/*%runme3(RunDate=20180118);*/
/*%runme3(RunDate=20180301);*/
/*%runme3(RunDate=20180314);*/
/*%runme3(RunDate=20180316);*/
/*%runme3(RunDate=20180424);*/
/*%runme3(RunDate=20180528);*/
/*%runme3(RunDate=20180629);*/
/*%runme3(RunDate=20180730);*/
/*%runme3(RunDate=20180814);*/
/*%runme3(RunDate=20180917);*/
/*%runme3(RunDate=20181010);*/
/*%runme3(RunDate=20181112);*/
/*%runme3(RunDate=201812106);*/
/*%runme3(RunDate=20190111);*/


proc sort data = data.datepartition;
	by descending appdate;
run;
data _null_;
set data.datepartition;
if _n_ = 1 then 
	call symput("todaydate", put(appdate,yymmddn8.));
run;
%put &todaydate;
%runme3(RunDate=&todaydate);

******Generate report to check happy with monitoring ******************* ;

%macro report(date) ;
	data fix.Behave_&date ;
		set fix.Behave_&date ;
		Format seg3Bucket $100. ;
		if BehaveScore >= 0 and BehaveScore <= 732 then seg3Bucket ="B1: BehaveScore  <= 732";
		else if BehaveScore >= 0 and BehaveScore <= 817 then seg3Bucket = "B2: BehaveScore  <= 817";
		else if BehaveScore >= 0 and BehaveScore <= 883 then seg3Bucket = "B3: BehaveScore  <= 883";
		else if BehaveScore > 883 then seg3Bucket = "B4: BehaveScore  > 883";
		else seg3Bucket = "B5: Unknown";

		Format seg4Bucket $100. ;
		if BehaveScore >= 0 and BehaveScore <= 733 then seg4Bucket ="B1: BehaveScore  <= 733";
		else if BehaveScore >= 0 and BehaveScore <= 842 then seg4Bucket = "B2: BehaveScore  <= 842";
		else if BehaveScore >= 0 and BehaveScore <= 869 then seg4Bucket = "B3: BehaveScore  <= 869";
		else if BehaveScore >= 0 and BehaveScore <= 922 then seg4Bucket = "B4: BehaveScore  <= 922";
		else if BehaveScore > 922 and BehaveScore <= 1000 then seg4Bucket = "B5: BehaveScore  > 922";
		else seg4Bucket = "B6: UNKNOWN";

		Format seg5Bucket $100. ;
		if BehaveScore >= 0 and BehaveScore <= 728 then seg5Bucket ="B1: BehaveScore  <= 728";
		else if BehaveScore >= 0 and BehaveScore <= 813 then seg5Bucket = "B2: BehaveScore  <= 813";
		else if BehaveScore >= 0 and BehaveScore <= 866 then seg5Bucket = "B3: BehaveScore  <= 866";
		else if BehaveScore >= 0 and BehaveScore <= 917 then seg5Bucket = "B4: BehaveScore  <= 917";
		else if BehaveScore > 917 then seg5Bucket = "B5: BehaveScore  > 917";
		else seg5Bucket = "B6: UNKNOWN";

	run;

	proc summary data = fix.Behave_&date nway missing ;
		class rundate DAYSSINCEFIRSTABDISB_B DAYSSINCELASTABDISB_B  MaxArrearsCode_B 
		MAXCD_6_EVER_B
		no_zero_arrear_24_2_B
		paid2owed12_V3_B seg3Bucket seg4Bucket seg5Bucket 
		Behavedecile 
		;
		output out = _run_&date ;
	run;
%mend;

/*%report(Date=20160115);*/
/*%report(Date=20160215);*/
/*%report(Date=20170712);*/
/*%report(Date=20170612);*/
/*%report(Date=20170512);*/
/*%report(Date=20170412);*/
/*%report(Date=20170317);*/
/*%report(Date=20170215);*/
/*%report(Date=20170115);*/
/*%report(Date=20161215);*/
/*%report(Date=20161015);*/
/*%report(Date=20160915);*/
/*%report(Date=20160815);*/
/*%report(Date=20160715);*/
/*%report(Date=20160615);*/
/*%report(Date=20160515);*/
/*%report(Date=20160415);*/
/*%report(Date=20160315);*/
/*%report(date=20170810) ;*/
/*%report(date=20170914) ;*/
/*%report(date=20171018) ;*/
/*%report(date=20171116) ;*/
/*%report(date=20171219) ;*/
/*%report(date=20180118) ;*/
/*%report(date=20180301) ;*/
/*%report(date=20180316) ;*/
/*%report(date=20180424) ;*/
/*%report(date=20180528) ;*/
/*%report(date=20180629) ;*/
/*%report(date=20180730) ;*/
/*%report(date=20180814);*/
/*%report(date=20180917);*/
/*%report(date=20181010);*/
/*%report(date=20181112);*/
/*%report(date=20181206);*/
%report(date=&todaydate);

data fix.backupreport ;
	set fix.report ;
run;

data fix.report ;
	set fix.report work._run_: ;
run;

proc sort data = fix.report nodupkey ;
	by _all_ ;
run;
proc sort data = fix.report ;
	by rundate ;
run;
data report ;
	set fix.report ;
run;

proc sort data = fix.Datepartition; by descending AppDate; run;
	

data _null_;
	set fix.Datepartition;
	if _n_ = 2 then call symput("prevrundate", put(Appdate,yymmddn8.));
run;
%put &prevrundate;

%VolumeComparison(Prev=data.Behave_&prevrundate,Curr=data.Behave_&tday) ;
%MOM(Prev=data.Behave_&prevrundate,Curr=data.Behave_&tday) ;

data Report ;
	set fix.Report ;
	Month = substr(RunDate,1,6) ;
	Decile = put(BehaveDecile,z2.);
	if Month > put(&last12month.,6.);
run;

data fix.Appbuildmonitoring ;
	set fix.Appbuildmonitoring ;
	Decile = put(BehaveDecile,z2.);
run;

%calcPSI(In1=fix.Appbuildmonitoring,In2=Report,var=DAYSSINCEFIRSTABDISB_B,Out=test,Period=Month); 
%calcPSI(In1=fix.Appbuildmonitoring,In2=Report,var=DAYSSINCELASTABDISB_B,Out=test,Period=Month); 
%calcPSI(In1=fix.Appbuildmonitoring,In2=Report,var=MaxArrearsCode_B,Out=test,Period=Month);  
%calcPSI(In1=fix.Appbuildmonitoring,In2=Report,var=MAXCD_6_EVER_B,Out=test,Period=Month); 
%calcPSI(In1=fix.Appbuildmonitoring,In2=Report,var=no_zero_arrear_24_2_B,Out=test,Period=Month); 
%calcPSI(In1=fix.Appbuildmonitoring,In2=Report,var=paid2owed12_V3_B,Out=test,Period=Month); 
%calcPSI(In1=fix.Appbuildmonitoring,In2=Report,var=seg3Bucket,Out=test,Period=Month); 
%calcPSI(In1=fix.Appbuildmonitoring,In2=Report,var=seg4Bucket,Out=test,Period=Month); 
%calcPSI(In1=fix.Appbuildmonitoring,In2=Report,var=seg5Bucket,Out=test,Period=Month); 
%calcPSI(In1=fix.Appbuildmonitoring,In2=Report,var=Decile,Out=test,Period=Month); 

data AppStagePSI ;
	set AppStagePSI ;
	if Month ='Build' then Month = ' Build';
run;

options center;
options nodate nonumber;
options orientation=landscape;
ods pdf body = "\\mpwsas5\G\Automation\Behavescore\reports\Monthly Total Run Monitoring &tday..pdf" ;

	ods startpage = no;
	Title "Number of records processed"; 
	proc sgplot data = report  ;
		format  _freq_  comma9. ;
		vbar rundate / response = _freq_  ;
		yaxis  label = "Volume";
	run; 
	Title;

	ods startpage = no;
	proc freq data = Venn ;
		table CurrMonth *PrevMonth  / missing  nocum nocol norow nopercent FORMAT=COMMA9. ;
	run;

	Title "Change in Decile from Current Month to Previous";
	proc sgplot data = summary5 ;
		vbar shift / response = _FREQ_  stat = percent ;
		xaxis type = discrete ;
		yaxis label = "Percent";
	run;
	footnote1 "Current decile minus previous month's decile";
	Title;
	footnote1;

	ods startpage = now;
	ods layout absolute;
	ods region y=0in x=0in width=5in height=4in;

	Title "Decile Distribution";
	proc sgplot data=AppStagePSI ;
		where VariableName in ('Decile') and Month ne ' Build' and input(Month,6.) >= &last12month. ;
		vbar month / response = Percentage stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	run;
	Title;

	Title "Bucketed as per Segment3";
	ods region y=0in x=5in width=5in height=4in;
	proc sgplot data=AppStagePSI ;
	where VariableName in ('seg3Bucket') and Month ne ' Build' and input(Month,6.) >= &last12month. ;
	       vbar month / response = Percentage stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	run;
	Title;

	Title "Bucketed as per Segment4";
	ods region y=4in x=0in width=5in height=4in;
	proc sgplot data=AppStagePSI ;
		where VariableName in ('seg4Bucket') and Month ne ' Build' and input(Month,6.) >= &last12month. ;
		vbar month / response = Percentage stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	run;
	Title;
	ods region y=4in x=5in width=5in height=4in;

	Title "Bucketed as per Segment5";
	proc sgplot data=AppStagePSI ;
		where VariableName in ('seg5Bucket') and Month ne ' Build' and input(Month,6.) >= &last12month. ;
		vbar month / response = Percentage stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	run;
	Title;
	ods layout end  ;

	ods startpage = now;
	ods layout absolute;
	ods region y=0in x=0in width=5in height=4in;
	Title "DAYSSINCEFIRSTABDISB";
	proc sgplot data=AppStagePSI ;
		where VariableName in ('DAYSSINCEFIRSTABDISB_B') and Month ne ' Build' and input(Month,6.) >= &last12month. ;
		vbar month / response = Percentage stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	run;
	Title;
	ods region y=0in x=5in width=5in height=4in;
	Title "DAYSSINCELASTABDISB";
	proc sgplot data=AppStagePSI ;
		where VariableName in ('DAYSSINCELASTABDISB_B') and Month ne ' Build' and input(Month,6.) >= &last12month. ;
		vbar month / response = Percentage stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	run;
	Title;
	ods region y=4in x=0in width=5in height=4in;
	Title "MaxArrearsCode";
	proc sgplot data=AppStagePSI ;
		where VariableName in ('MaxArrearsCode_B') and Month ne ' Build' and input(Month,6.) >= &last12month. ;
	    vbar month / response = Percentage stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	run;
	Title;
	ods region y=4in x=5in width=5in height=4in;
	Title "MAXCD_6_EVER";
	proc sgplot data=AppStagePSI ;
		where VariableName in ('MAXCD_6_EVER_B') and Month ne ' Build' and input(Month,6.) >= &last12month. ;
	    vbar month / response = Percentage stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	run;
	Title;
	ods layout end  ;


	ods startpage = now;
	ods layout absolute;
	ods region y=0in x=0in width=5in height=4in;
	Title "no_zero_arrear_24_2";
	proc sgplot data=AppStagePSI ;
		where VariableName in ('no_zero_arrear_24_2_B') and Month ne ' Build' and input(Month,6.) >= &last12month. ;
	    vbar month / response = Percentage stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	run;
	Title;
	ods region y=0in x=5in width=5in height=4in;
	Title "paid2owed12_V3";
	proc sgplot data=AppStagePSI ;
		where VariableName in ('paid2owed12_V3_B') and Month ne ' Build' and input(Month,6.) >= &last12month. ;
	    vbar month / response = Percentage stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	run;
	Title;
	ods layout end;
ods pdf close;

data appstagepsi1;
	set AppStagePSI;
run;

*****MONTHLY MONITORING******************************* ;
*****APPLICATION MONITORING*********************************** ;

proc sql stimer;
    connect to ODBC (dsn=MPWAPS);
	create table TotalApps as 
    select * from connection to odbc ( select Tranappnumber as loanid , Uniqueid , ApplicationDate  ,
			       NationalID 
			 from prd_press.capri.capri_loan_application
            where applicationdate > '2017-01-01' and isnull(channelcode,'') <> 'ccc006' 
			and TRANSEQUENCE <> '005'
          ) ;
	disconnect from odbc ;
quit;

proc sort data = TotalApps ;
	by loanid descending Uniqueid ;
run;

proc sort data = TotalApps nodupkey ;
	by loanid  ;
run;

data TotalApps ;
	set TotalApps ;
	Format AppMonth monyy7. ;
	AppMonth = input(ApplicationDate,yymmdd10.);
run;

proc freq data = TotalApps ;
	tables AppMonth / missing ;
run;

data TotalApps1 ;
	set TotalApps ;
	if AppMonth >= '01JAN2017'd ;
	count = 1 ;
run;

proc freq data = TotalApps1 ;
tables AppMonth / missing ;
run;

PROC SORT DATA = DATA.Datepartition;
	BY APPDATE;
RUN;
FILENAME _TEMP_ TEMP;

DATA _NULL_;
	SET DATA.Datepartition;
	FILE _TEMP_;
	IF _N_ = 1 THEN 
	FORMULA = CATS("IF APPDATE < '",PUT(APPDATE,DATE9.), "'D THEN InfoDt ='20161215';");
	ELSE 
	FORMULA = CATS("ELSE IF APPDATE < '",PUT(APPDATE,DATE9.), "'D THEN InfoDt ='",PUT(LAG(APPDATE),YYMMDDN8.),"';");
	PUT FORMULA;
RUN;

data TotalApps1 ;
	set TotalApps1 ;
	Format AppDate date9.  ;
	AppDate = input(ApplicationDate,yymmdd10.);
	%INC _TEMP_;
run;

proc freq data = TotalApps1 ;
	tables AppMonth * InfoDt / missing ;
run;
 
/************Change App Date************/
data TotalApps1 ;
	set TotalApps1 ;
	if AppDate >= INTNX('MONTH', TODAY(),0,'BEGIN') THEN DELETE; *'01JAN2019'd then delete ;
run;
 
%macro sourceAppData(dt,num);
	data temp ;
		set TotalApps1 ;
		if InfoDt = "&dt" ;
	run;
	proc sql ;
		create table _&num._ as 
			select A.LOANID, A.Uniqueid , A.ApplicationDate, A.AppMonth , A.InfoDt , A.NationalID, 
					DAYSSINCEFIRSTABDISB_B, DAYSSINCEFIRSTABDISB_W,
					DAYSSINCELASTABDISB_B, DAYSSINCELASTABDISB_W,
					MaxArrearsCode_B, MaxArrearsCode_W, MAXCD_6_EVER_B,
					MAXCD_6_EVER_W, no_zero_arrear_24_2_B,
					no_zero_arrear_24_2_W, paid2owed12_V3_B, paid2owed12_V3_W,
					I_Target, U_Target, P_Target1, P_Target0,
					final_Score, BehaveScore, BehaveDecile
			from  temp A 
			inner join  fix.Behave_&dt B 
			on A.NationalID = B.idno ;
	quit;

	data _&num._ ;
		set _&num._ ;
		Format seg3Bucket $100. ;
		if BehaveScore >= 0 and BehaveScore <= 732 then seg3Bucket ="B1: BehaveScore  <= 732";
		else if BehaveScore >= 0 and BehaveScore <= 817 then seg3Bucket = "B2: BehaveScore  <= 817";
		else if BehaveScore >= 0 and BehaveScore <= 883 then seg3Bucket = "B3: BehaveScore  <= 883";
		else if BehaveScore > 883 then seg3Bucket = "B4: BehaveScore  > 883";
		else seg3Bucket = "B5: Unknown";

		Format seg4Bucket $100. ;
		if BehaveScore >= 0 and BehaveScore <= 733 then seg4Bucket ="B1: BehaveScore  <= 733";
		else if BehaveScore >= 0 and BehaveScore <= 842 then seg4Bucket = "B2: BehaveScore  <= 842";
		else if BehaveScore >= 0 and BehaveScore <= 869 then seg4Bucket = "B3: BehaveScore  <= 869";
		else if BehaveScore >= 0 and BehaveScore <= 922 then seg4Bucket = "B4: BehaveScore  <= 922";
		else if BehaveScore > 922 and BehaveScore <= 1000 then seg4Bucket = "B5: BehaveScore  > 922";
		else seg4Bucket = "B6: UNKNOWN";

		Format seg5Bucket $100. ;
		if BehaveScore >= 0 and BehaveScore <= 728 then seg5Bucket ="B1: BehaveScore  <= 728";
		else if BehaveScore >= 0 and BehaveScore <= 813 then seg5Bucket = "B2: BehaveScore  <= 813";
		else if BehaveScore >= 0 and BehaveScore <= 866 then seg5Bucket = "B3: BehaveScore  <= 866";
		else if BehaveScore >= 0 and BehaveScore <= 917 then seg5Bucket = "B4: BehaveScore  <= 917";
		else if BehaveScore > 917 then seg5Bucket = "B5: BehaveScore  > 917";
		else seg5Bucket = "B6: UNKNOWN";

	run;
	proc delete data = temp ;
	run;
	proc append base = AppMonitoringBase data =_&num._ force;
	quit;
	ods listing;
	proc freq data = AppMonitoringBase ;
		tables AppMonth * InfoDt / missing ;
	run;
	ods listing close ;
%mend;
/************************Add the current Month**********/
/*%sourceAppData(dt=20161215,num=1);*/
/*%sourceAppData(dt=20170115,num=1);*/
/*%sourceAppData(dt=20170215,num=1);*/
/*%sourceAppData(dt=20170317,num=1);*/
/*%sourceAppData(dt=20170412,num=1);*/
/*%sourceAppData(dt=20170512,num=1);*/
/*%sourceAppData(dt=20170612,num=1);*/
/*%sourceAppData(dt=20170712,num=1);*/
/*%sourceAppData(dt=20170810,num=1);*/
/*%sourceAppData(dt=20170914,num=1);*/
/*%sourceAppData(dt=20171018,num=1);*/
/*%sourceAppData(dt=20171116,num=1);*/
/*%sourceAppData(dt=20171219,num=1);*/
/*%sourceAppData(dt=20180118,num=1);*/
/*%sourceAppData(dt=20180228,num=1);*/
/*%sourceAppData(dt=20180316,num=1);*/
/*%sourceAppData(dt=20180424,num=1);*/
/*%sourceAppData(dt=20180528,num=1);*/
/*%sourceAppData(dt=20180730,num=1);*/
/*%sourceAppData(dt=20180814,num=1);*/
/*%sourceAppData(dt=20180917,num=1);*/
/*%sourceAppData(dt=20181010,num=1);*/
/*%sourceAppData(dt=20181112,num=1);*/
/*%sourceAppData(dt=20181206,num=1);*/
/*%sourceAppData(dt=20190111,num=1);*/

DATA Datepartition;
	SET data.Datepartition;
	appdate2= PUT(LAG(APPDATE),YYMMDDN8.);
	if compress(appdate2) = '.' then appdate2='20161215';
RUN;

FILENAME _temp_ TEMP;
DATA _NULL_;
	SET Datepartition;
	FILE _temp_ ;
	FORMULA = CATS('%sourceAppData(dt='||appdate2||','||'num=1);');
	PUT FORMULA;
RUN;
%inc _temp_;

ods listing;
proc freq data = AppMonitoringBase ;
	tables AppMonth / missing ;
run;
ods listing close ;

proc sort data = AppMonitoringBase ;
	by loanid InfoDt ;
run;

proc sort data = AppMonitoringBase nodupkey ;
	by loanid ;
run;


data fix.AppMonitoringBase ;
	set AppMonitoringBase ;
	ApplicationMonth = put(AppMonth,yymmn6.);
	Decile = put(BehaveDecile,z2.);
run;

ods listing;
proc freq data = fix.AppMonitoringBase ;
	tables ApplicationMonth / missing ;
run;
ods listing close ;

data Appbuildmonitoring ;
	set fix.Appbuildmonitoring ;
	ApplicationMonth = ' BUILD';
	Decile = put(BehaveDecile,z2.);
run;

proc freq data = Appbuildmonitoring ;
	tables ApplicationMonth / missing ;
run;


proc summary data = fix.AppMonitoringBase  nway missing ;
	class ApplicationMonth DAYSSINCEFIRSTABDISB_B DAYSSINCELASTABDISB_B  MaxArrearsCode_B 
	MAXCD_6_EVER_B
	no_zero_arrear_24_2_B
	paid2owed12_V3_B seg3Bucket seg4Bucket seg5Bucket 
	Behavedecile  ;
	output out = fix.Report2;
run;

data fix.Report2; 
	set fix.Report2; 
	Decile = put(BehaveDecile,z2.);
run;


proc delete data = AppStagePSI ;
run;

%calcPSI(In1=Appbuildmonitoring,In2=fix.Report2,var=DAYSSINCEFIRSTABDISB_B,Out=test,period = ApplicationMonth); 
%calcPSI(In1=Appbuildmonitoring,In2=fix.Report2,var=DAYSSINCELASTABDISB_B,Out=test,period = ApplicationMonth); 
%calcPSI(In1=Appbuildmonitoring,In2=fix.Report2,var=MaxArrearsCode_B,Out=test,period = ApplicationMonth); 
%calcPSI(In1=Appbuildmonitoring,In2=fix.Report2,var=MAXCD_6_EVER_B,Out=test,period = ApplicationMonth);
%calcPSI(In1=Appbuildmonitoring,In2=fix.Report2,var=no_zero_arrear_24_2_B,Out=test,period = ApplicationMonth);
%calcPSI(In1=Appbuildmonitoring,In2=fix.Report2,var=paid2owed12_V3_B,Out=test,period = ApplicationMonth);
%calcPSI(In1=Appbuildmonitoring,In2=fix.Report2,var=seg3Bucket,Out=test,period = ApplicationMonth);
%calcPSI(In1=Appbuildmonitoring,In2=fix.Report2,var=seg4Bucket,Out=test,period = ApplicationMonth);
%calcPSI(In1=Appbuildmonitoring,In2=fix.Report2,var=seg5Bucket,Out=test,period = ApplicationMonth);
%calcPSI(In1=Appbuildmonitoring,In2=fix.Report2,var=Decile,Out=test,period = ApplicationMonth);

data Appstagepsi ;
set Appstagepsi ;
if month >= put(intnx('month',today(),0,'begin'),yymmn6.) then delete; * '201901' then delete ;
run;
proc freq data = Appstagepsi;
	tables month;
run;

Title "Decile Distribution";
proc sgplot data=AppStagePSI ;
where VariableName in ('Decile') ; ;
       vbar month / response = Percentage stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
run;
Title;

proc sql ;
create table AppReport1 as 
select a.* , case when b.loanid is not null then 1 else 0 end as Repeat , 1 as volume 
from Totalapps1 A 
left join AppMonitoringBase B 
on A.loanid = B.loanid ;
quit;

options center;
options nodate nonumber;
options orientation=landscape;
ods pdf body = "\\mpwsas5\G\Automation\Behavescore\reports\ApplicationMonitoring&tday..pdf" ;
ods startpage = no;

Title "Application Volumes";
proc sgplot data = AppReport1 ;
label volume = "Total Apps" repeat = "Repeat Apps" ;
format volume comma9. ;
yaxis label = "Applications";
vbar AppMonth / response = volume  ;
vline AppMonth / response = repeat ;
run; 
Title ;

ods startpage = now;
ods layout absolute;
ods region y=0in x=0in width=5in height=4in;

Title "Decile Distribution";
proc sgplot data=AppStagePSI ;
where VariableName in ('Decile');
       vbar month / response = Percentage stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	   vline month / response = psi y2axis stat = mean group = Bin ;
	   vline month / response = MarginallyStable y2axis stat = mean group = Bin ;
	   vline month / response = Unstable y2axis stat = mean group = Bin ;
yaxis label = 'Percentage'; 
xaxis label = 'ApplicationMonth';
y2axis label = 'PSI' min = 0 max = 1 ;
run;
Title;



Title "Bucketed as per Segment3";
ods region y=0in x=5in width=5in height=4in;
proc sgplot data=AppStagePSI ;
where VariableName in ('seg3Bucket')  ;
       vbar month / response = Percentage stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	   vline month / response = psi y2axis stat = mean group = Bin ;
	   vline month / response = MarginallyStable y2axis stat = mean group = Bin ;
	   vline month / response = Unstable y2axis stat = mean group = Bin ;
yaxis label = 'Percentage'; 
xaxis label = 'ApplicationMonth';
y2axis label = 'PSI' min = 0 max = 1 ;
run;
run;
Title;

Title "Bucketed as per Segment4";
ods region y=4in x=0in width=5in height=4in;
proc sgplot data=AppStagePSI ;
where VariableName in ('seg4Bucket')  ;
       vbar month / response = Percentage stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	   	   vline month / response = psi y2axis stat = mean group = Bin ;
	   vline month / response = MarginallyStable y2axis stat = mean group = Bin ;
	   vline month / response = Unstable y2axis stat = mean group = Bin ;
yaxis label = 'Percentage'; 
xaxis label = 'ApplicationMonth';
y2axis label = 'PSI' min = 0 max = 1 ;
run;
Title;
ods region y=4in x=5in width=5in height=4in;

Title "Bucketed as per Segment5";
proc sgplot data=AppStagePSI ;
where VariableName in ('seg5Bucket') ;
       vbar month / response = Percentage stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	   	   vline month / response = psi y2axis stat = mean group = Bin ;
	   vline month / response = MarginallyStable y2axis stat = mean group = Bin ;
	   vline month / response = Unstable y2axis stat = mean group = Bin ;
yaxis label = 'Percentage'; 
xaxis label = 'ApplicationMonth';
y2axis label = 'PSI' min = 0 max = 1 ;
run;
Title;
ods layout end  ;

ods startpage = now;
ods layout absolute;
ods region y=0in x=0in width=5in height=4in;
Title "DAYSSINCEFIRSTABDISB";
proc sgplot data=AppStagePSI ;
where VariableName in ('DAYSSINCEFIRSTABDISB_B')  ;
       vbar month / response = Percentage stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	   vline month / response = psi y2axis stat = mean group = Bin ;
	   vline month / response = MarginallyStable y2axis stat = mean group = Bin ;
	   vline month / response = Unstable y2axis stat = mean group = Bin ;
yaxis label = 'Percentage'; 
xaxis label = 'ApplicationMonth';
y2axis label = 'PSI' min = 0 max = 1 ;
run;
Title;
ods region y=0in x=5in width=5in height=4in;
Title "DAYSSINCELASTABDISB";
proc sgplot data=AppStagePSI ;
where VariableName in ('DAYSSINCELASTABDISB_B')  ;
       vbar month / response = Percentage stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	   	   vline month / response = psi y2axis stat = mean group = Bin ;
	   vline month / response = MarginallyStable y2axis stat = mean group = Bin ;
	   vline month / response = Unstable y2axis stat = mean group = Bin ;
yaxis label = 'Percentage'; 
xaxis label = 'ApplicationMonth';
y2axis label = 'PSI' min = 0 max = 1 ;
run;
Title;
ods region y=4in x=0in width=5in height=4in;
Title "MaxArrearsCode";
proc sgplot data=AppStagePSI ;
where VariableName in ('MaxArrearsCode_B')  ;
       vbar month / response = Percentage stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	   vline month / response = psi y2axis stat = mean group = Bin ;
	   vline month / response = MarginallyStable y2axis stat = mean group = Bin ;
	   vline month / response = Unstable y2axis stat = mean group = Bin ;
yaxis label = 'Percentage'; 
xaxis label = 'ApplicationMonth';
y2axis label = 'PSI' min = 0 max = 1 ;
run;
Title;

ods region y=4in x=5in width=5in height=4in;
Title "MAXCD_6_EVER";
proc sgplot data=AppStagePSI ;
where VariableName in ('MAXCD_6_EVER_B')  ;
       vbar month / response = Percentage stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	   	   vline month / response = psi y2axis stat = mean group = Bin ;
	   vline month / response = MarginallyStable y2axis stat = mean group = Bin ;
	   vline month / response = Unstable y2axis stat = mean group = Bin ;
yaxis label = 'Percentage'; 
xaxis label = 'ApplicationMonth';
y2axis label = 'PSI' min = 0 max = 1 ;
run;
Title;
ods layout end  ;

ods startpage = now;
ods layout absolute;
ods region y=0in x=0in width=5in height=4in;
Title "no_zero_arrear_24_2";
proc sgplot data=AppStagePSI ;
where VariableName in ('no_zero_arrear_24_2_B') ;
       vbar month / response = Percentage stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ; 
 vbar month / response = Percentage stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	   vline month / response = psi y2axis stat = mean group = Bin ;
	   vline month / response = MarginallyStable y2axis stat = mean group = Bin ;
	   vline month / response = Unstable y2axis stat = mean group = Bin ;
yaxis label = 'Percentage'; 
xaxis label = 'ApplicationMonth';
y2axis label = 'PSI' min = 0 max = 1 ; 
run;
Title;
ods region y=0in x=5in width=5in height=4in;
Title "paid2owed12_V3";
proc sgplot data=AppStagePSI ;
where VariableName in ('paid2owed12_V3_B')  ;
       vbar month / response = Percentage stat = mean group = Bin  NOSTATLABEL  BARWIDTH = 0.8 ;  
	   vline month / response = psi y2axis stat = mean group = Bin ;
	   vline month / response = MarginallyStable y2axis stat = mean group = Bin ;
	   vline month / response = Unstable y2axis stat = mean group = Bin ;
yaxis label = 'Percentage'; 
xaxis label = 'ApplicationMonth';
y2axis label = 'PSI' min = 0 max = 1 ;

run;
Title;
ods layout end;
ods pdf close;
