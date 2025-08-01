
%let projectcode =H:\Process_Automation\Codes;
%let runmonth = %scan(&sysparm,1);
%let process_number = %scan(&sysparm,2);
%let process =  %scan(&sysparm,3);

%let project =pj;
libname &project "&process";

%start_program;

%include "\\neptune\credit$\AA_GROUP CREDIT\Scoring\SAS Macros\Upload_APS.sas";
Libname data "\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\EmployerIndexTables";
libname indata "\\neptune\sasa$\MPWSAS15\Team Work\Elvis\inputdata2";
libname calib "\\neptune\sasa$\MPWSAS15\Team Work\Elvis\calibration\calibration_new";
Libname Calib2 '\\mpwsas64\Core_Credit_Risk_Models\V5\Application Scorecard\V636\calibration_new';
Libname calib3 '\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V645\EmployerVolumeIndex';
libname table '\\neptune\sasa$\Lindo\V6\Employerindex';

%let odbc = MPWAPS;
data _null_;
     call symput('runmonth',put(intnx('month',today(),0),yymmn6.));
	 call symput('lastmonth',put(intnx('month',today(),-1),date9.));
	 call symput('month12',put(intnx('month',today(),-12),date9.));
     call symput('lastmonth12',cats ("'",put(intnx('month',today(),-12),yymmddd10.),"'"));
     call symput('today',put(today(),ddmmyyn8.));
 run;
%put &runmonth;
%put &today;

proc sql stimer;
    connect to ODBC (dsn=mpwaps);
    create table disbursement_info as 
    select * from connection to odbc ( 
        select  b.uniqueid, a.idnumber, b.tranappnumber,a.LoanReference,c.employersubgroupcode, a.subgroupcode,b.applicationdate, a.DisbStartDate,a.firstduedate
        from PRD_DataDistillery.dbo.disbursement_info a
        left join prd_press.capri.capri_loan_application b
        on cast(a.loanid as varchar) = b.tranappnumber
        left join prd_press.capri.capri_employment c
        on b.uniqueid = c.uniqueid 
    ) ;
    disconnect from odbc ;
quit;

proc sort data = disbursement_info ;
	by tranappnumber descending uniqueid  ;
run;

proc sort data = disbursement_info nodupkey dupout=dups  out= rawdata_no_dup ;
	by tranappnumber  ;
run;

data disbursement_info;
    set rawdata_no_dup;
    where input(applicationdate,yymmdd10.) between "&month12"d and "&lastmonth"d;
    month = put(input(applicationdate,yymmdd10.),yymmn6.);
    disbursed_date =  intnx('month',input(applicationdate,yymmdd10.),0,'end');
    format disbursed_date date9.;
run;

proc sort data = disbursement_info ;
	by tranappnumber descending employersubgroupcode;
run;

proc sort data = disbursement_info nodupkey out = disbursement_info_2;
	by tranappnumber descending employersubgroupcode;
run;

%macro monthly_volume(period =);
    proc sql;
        select distinct month into : months separated by " "
        from disbursement_info_2
        order by month desc;
    quit;
    proc delete data = month_&period._volume; run;

    %do i = 1 %to   %sysfunc(countw( &months));
        %let month = %scan(&months, &i.);
        %let j =%eval( %sysfunc(countw( &months))-&i);

        %put &i &j;
        data _null_;
            call symput('month1',put(intnx('month',today(),-0-&i ,'end'),date9.));
            call symput('month6',put(intnx('month',today(), -11-&i,'begin'),date9.));
        run;

        %put &month ---> &month1 and &month6;
        proc sql;
            create table disbursement_&period._&month. as
                select employersubgroupcode, count(*) as volume 
                from disbursement_info_2
                where disbursed_date between "&month1"d and "&month6"d
                group by employersubgroupcode
                order by volume desc;
        quit; 

        data disbursement_&period._&month.;
            set disbursement_&period._&month.;
            month = &month;
        run;

        proc append base = month_&period._volume data =disbursement_&period._&month. ; run; 
    %end;
%mend;

options nomprint nomlogic nosymbolgen;
%monthly_volume(period =12);




/***************************************************V645 Calibration employmenttable*********************************************************/

data month_12_volume_v2;
    set month_12_volume;
    volume_last_12_month_V2 = volume;
    if employersubgroupcode ='UNKNOWN' then  volume_last_12_month_V2 = -1;
run;

data month_12_volume_v2;
	set month_12_volume_v2;
	employersubgroupcode_v2 = compress(employersubgroupcode,'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789','k');
run;
data month_12_volume_v2;
    set month_12_volume_v2;
    Format volume_last_12_month_V2_B $500. ;
    If (volume_last_12_month_V2 <= 10 or volume_last_12_month_V2 in (-1)) then volume_last_12_month_V2_B = "B1: volume_last_12_month_V2  <= 10 or volume_last_12_month_V2 in (-1)";
    Else if ( volume_last_12_month_V2 > . and volume_last_12_month_V2 <=99 )  then volume_last_12_month_V2_B = "B2: volume_last_12_month_V2  <= 99";
    Else if (volume_last_12_month_V2 > . and volume_last_12_month_V2 <= 903)  then volume_last_12_month_V2_B = "B3: volume_last_12_month_V2  <= 903";
    Else if (volume_last_12_month_V2 > 903)  then volume_last_12_month_V2_B = "B4: volume_last_12_month_V2  > 903";
run;

proc sort data=month_12_volume_v2;
	by employersubgroupcode_V2;
run;

proc sort data=month_12_volume_v2 nodupkey dupout=dupout out=final;
	by employersubgroupcode_V2;
run;

proc sql stimer;
      connect to ODBC (dsn=mpwaps);
      create table ICBDATA as 
      select * from connection to odbc ( 
            select distinct b.EmployerSUbGroupCode, d.Industry, D.SuperSector, d.Sector,d.subSector
            from (select uniqueid ,tranappnumber from prd_press.capri.capri_loan_application where applicationdate >= &lastmonth12) a
			left join  prd_press.capri.capri_employment b
			on a.uniqueid = b.uniqueid
			left  join  prd_crup.ic.crup_subgroup_industry c
			on b.employersubgroupcode = c.reference_concatenated
			left join prd_crup.ic.crup_subgroup_abicb_codes d
			on c.employer_tradename = d.tradename 
	) ;
      disconnect from odbc ;
quit;
data ICBDATA2;
	set ICBDATA;
	rename employersubgroupcode = employersubgroupcode_V2 subsector = subsector_V2 sector = sector_V2 industry = industry_V2 supersector = supersector_V2;
run;
proc sort data=ICBDATA2 nodupkey;
	by employersubgroupcode_V2;
run;

proc sql;
    create table month_12_volume1
        as select a.*, b.*
        from final a  
        left join ICBDATA2 B 
        on a.employersubgroupcode_V2 = B.EmployerSUbGroupCode_V2 ;
quit;

data month_12_volume2(keep =  employersubgroupcode_V2 volume_last_12_month_V2 volume_last_12_month_V2_B  industry_name_V2 supersector_name_V2 sector_name_V2 subsector_name_V2 subsector_V2 sector_V2 industry_V2 supersector_V2 
								   industry_ind_V2 supersector_ind_V2 sector_ind_V2 subsector_ind_V2);
	set month_12_volume1;
	industry_name_V2 = substr(industry_V2,4);
	supersector_name_V2 = substr(supersector_V2,6);
	sector_name_V2 = substr(sector_V2,8);
	subsector_name_V2 = substr(subsector_V2,10);

	industry_ind_V2 = substr(industry_V2,1,3);
	supersector_ind_V2 = substr(supersector_V2,1,4);
	sector_ind_V2 = substr(sector_V2,1,6);
	subsector_ind_V2 = substr(subsector_V2,1,8);
run;

proc sort data = month_12_volume2 nodupkey;
	by _all_;
run;

proc sql;
    create table month_12_volume3 as
        select distinct a.*,c.tvalue as tvalue_V2
        from  month_12_volume2 a 
        left join calib3.subsector_names b
        on upcase(a.subsector_V2) = upcase(b.label)
        left join calib3._ESTIMATE1_  c
        on upcase(b.name) = upcase(c.parameter);
quit;

data /*calib3.*/V6_calibration_data1_&runmonth ;
    set month_12_volume3;
	keep employersubgroupcode_V2 volume_last_12_month_V2 volume_last_12_month_V2_B subsector_name_V2 tValue_V2 SubSector_tValue_V2;
    if tvalue_V2 = . then tvalue_V2 = 0;
    Format SubSector_tValue_V2 $500. ;
	If (tValue_V2 > . and tValue_V2 <= -2.44620107) or tValue_V2 in (.)  then SubSector_tValue_V2 = "B1: tValue_V2  <= -2.44620107";
	Else If (tValue_V2 > . and tValue_V2 <= -2.0490827)  then SubSector_tValue_V2 = "B2: tValue_V2  <= -2.0490827";
	Else If (tValue_V2 > . and tValue_V2 <= 0)  then SubSector_tValue_V2 = "B3: tValue_V2  <= 0";
	Else If (tValue_V2 > 0)  then SubSector_tValue_V2 = "B4: tValue_V2  > 0";
	Subsector_name_V2=subsector_V2;
run;
/*rename*/
data EmployerVolumeIndex(keep=employersubgroupcode_V2 EmployerVolumeIndex_V2 EmployerVolumeIndexGroup_V2 ICBCode_V2 Subsector_name_V2 SubSectorIndex_V2 SubSectorIndexGroup_V2 runmonth);
	set V6_calibration_data1_&runmonth;
	format EmployerVolumeIndex_V2 ICBCode_V2 19. EmployerVolumeIndexGroup_V2 $18. subsector_name_V2 $70. SubSectorIndex_V2 20.8 SubSectorIndexGroup_V2 $500. runmonth $6.;
	ICBCode_V2 = .;
	EmployerVolumeIndex_V2 = Volume_last_12_month_V2;
	EmployerVolumeIndexGroup_V2 = substr(volume_last_12_month_V2_B, 2,1);
	SubSectorIndex_V2 = tvalue_V2;
	SubSectorIndexGroup_V2 = substr(SubSector_tValue_V2,2,1);
	runmonth = "&runmonth";
run;

proc sql;
	create table allout as
	select a.employersubgroupcode as employersubgroupcode_V2, b.*
	from calib3.specialchar a
	inner join employervolumeindex b
	on a.employersubgroupcode2 = b.employersubgroupcode_V2;
quit;





data calib3.EmployerVolumeIndex&today.;
	set EmployerVolumeIndex ;
run;

/****************************************************************V635 Calibration Employer tables***********************************************************************/
data month_12_volume;
    set month_12_volume;
    volume_last_12_month = volume;
    if employersubgroupcode ='UNKNOWN' then  volume_last_12_month = -1;
run;

data month_12_volume;
	set month_12_volume;
	employersubgroupcode = compress(employersubgroupcode,'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789','k');
run;
 
data month_12_volume;
    set month_12_volume;
    Format volume_last_12_month_B_V2 $500. ;
    If (volume_last_12_month <= 10 or volume_last_12_month in (-1)) then volume_last_12_month_B_V2 = "B1: volume_last_12_month  <= 10 or volume_last_12_month in (-1)";
    Else if ( volume_last_12_month > . and volume_last_12_month <=99 )  then volume_last_12_month_B_V2 = "B2: volume_last_12_month  <= 99";
    Else if (volume_last_12_month > . and volume_last_12_month <= 903)  then volume_last_12_month_B_V2 = "B3: volume_last_12_month  <= 903";
    Else if (volume_last_12_month > 903)  then volume_last_12_month_B_V2 = "B4: volume_last_12_month  > 903";
run;

proc sort data=month_12_volume;
	by employersubgroupcode;
run;

proc sort data=month_12_volume nodupkey dupout=dupout out=final;
	by employersubgroupcode;
run;

/*proc sql stimer;*/
/*      connect to ODBC (dsn=mpwaps);*/
/*      create table ICBDATA as */
/*      select * from connection to odbc ( */
/*            select distinct b.EmployerSUbGroupCode, d.Industry, D.SuperSector, d.Sector,d.subSector*/
/*            from (select uniqueid ,tranappnumber from prd_press.capri.capri_loan_application where applicationdate >= &lastmonth12) a*/
/*			left join  prd_press.capri.capri_employment b*/
/*			on a.uniqueid = b.uniqueid*/
/*			left  join  prd_crup.ic.crup_subgroup_industry c*/
/*			on b.employersubgroupcode = c.reference_concatenated*/
/*			left join prd_crup.ic.crup_subgroup_abicb_codes d*/
/*			on c.employer_tradename = d.tradename */
/*	) ;*/
/*      disconnect from odbc ;*/
/*quit;*/
data ICBDATA;
	set ICBDATA;
	
run;
proc sort data=ICBDATA nodupkey;
	by employersubgroupcode;
run;

proc sql;
    create table month_12_volume1
        as select a.*, b.*
        from final a  
        left join ICBDATA B 
        on a.employersubgroupcode = B.EmployerSUbGroupCode ;
quit;

data month_12_volume2(keep =  employersubgroupcode volume_last_12_month volume_last_12_month_B_V2  industry_name supersector_name sector_name subsector_name subsector sector industry supersector 
								   industry_ind supersector_ind sector_ind subsector_ind);
	set month_12_volume1;
	industry_name = substr(industry,4);
	supersector_name = substr(supersector,6);
	sector_name = substr(sector,8);
	subsector_name = substr(subsector,10);

	industry_ind = substr(industry,1,3);
	supersector_ind = substr(supersector,1,4);
	sector_ind = substr(sector,1,6);
	subsector_ind = substr(subsector,1,8);
run;

proc sort data = month_12_volume2 nodupkey;
	by _all_;
run;

proc sql;
    create table month_12_volume3 as
        select distinct a.*,c.tvalue
        from  month_12_volume2 a 
        left join calib2.subsector_names b
        on upcase(subsector) = upcase(b.label)
        left join calib2._ESTIMATE1_  c
        on upcase(b.name) = upcase(c.parameter);
quit;

data data.V6_calibration_data1_&runmonth ;
    set month_12_volume3;
	keep employersubgroupcode volume_last_12_month volume_last_12_month_B_V2 subsector_name tValue SubSector_tValue_V2;
    if tvalue = . then tvalue = 0;
    Format SubSector_tValue_V2 $500. ;
    If tValue <= -4.55506647 then SubSector_tValue_V2 = "B1: tValue  <= -4.55506647";
    Else If tValue <= -2 then SubSector_tValue_V2 = "B2: tValue  <= -2";
    Else If tValue <= 2  then SubSector_tValue_V2 = "B3: tValue  <= 2";
    Else If tValue > 2  then SubSector_tValue_V2 = "B4: tValue  > 2";
	Subsector_name=subsector;
run;

data EmployerVolumeIndex(keep=employersubgroupcode EmployerVolumeIndex EmployerVolumeIndexGroup ICBCode Subsector_name SubSectorIndex SubSectorIndexGroup runmonth);
	set data.V6_calibration_data1_&runmonth;
	format EmployerVolumeIndex ICBCode 19. EmployerVolumeIndexGroup $18. subsector_name $70. SubSectorIndex 20.8 SubSectorIndexGroup $500. runmonth $6.;
	ICBCode = .;
	EmployerVolumeIndex = Volume_last_12_month;
	EmployerVolumeIndexGroup = substr(volume_last_12_month_B_V2, 2,1);
	SubSectorIndex = tvalue;
	SubSectorIndexGroup = substr(SubSector_tValue_V2,2,1);
	runmonth = "&runmonth";
run;

/*proc sql;*/
/*	create table allout as*/
/*	select a.employersubgroupcode, b.**/
/*	from calib2.specialchar a*/
/*	inner join employervolumeindex b*/
/*	on a.employersubgroupcode2 = b.employersubgroupcode;*/
/*quit;*/
/**/
/*proc append base=employervolumeindex data=allout;*/
/*run;*/

data calib2.EmployerVolumeIndex&today.;
	set EmployerVolumeIndex;
run;

/*%Upload_APS(Set =EmployerVolumeIndex , Server =Work, APS_ODBC = PRD_Dist, APS_DB = PRD_DataDistillery , distribute = HASH([employersubgroupcode]));*/


%end_program(&process_number);
/*%sendemailend(&process_number);*/
