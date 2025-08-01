%include "\\neptune\credit$\AA_GROUP CREDIT\Scoring\SAS Macros\Upload_APS.sas";
Libname Calib '\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V665\Data';
libname ICB "\\mpwsas64\Core_Credit_Risk_Model_Team\Scorecard\V665\Data\";

%let odbc = MPWAPS;
data _null_;
     call symput('runmonth',put(intnx('month',today(),0),yymmn6.));
	 call symput('lastmonth',put(intnx('month',today(),-1,'E'),date9.));
	 call symput('month12',put(intnx('month',today(),-12),date9.));
     call symput('lastmonth12',cats ("'",put(intnx('month',today(),-12),yymmddd10.),"'"));
     call symput('today',put(today(),ddmmyyn8.));
 run;
%put &runmonth; *current month in format 202305;
%put &today; * toays date in format 18052023;
%put &lastmonth; * end of previous month in format 30APR23 ;
%put &month12; * beginning of the month ,12 months ago in format 1May2022 ;
%put &lastmonth12; * 12 months ago in format '2022-05-01' ;


proc sql stimer;
    connect to ODBC (dsn=mpwaps);
    create table disbursement_info as 
    select * from connection to odbc ( 
        select  b.uniqueid, a.idnumber, b.tranappnumber,a.LoanReference,c.employersubgroupcode, a.subgroupcode,b.applicationdate, a.DisbStartDate,a.firstduedate
        from PRD_DataDistillery.dbo.disbursement_info a
        left join prd_press.capri.capri_loan_application_2021 b
        on cast(a.loanid as varchar) = b.tranappnumber
        left join prd_press.capri.capri_employment c
        on b.uniqueid = c.uniqueid 
    ) ;
    disconnect from odbc ;
quit;

/*combine UBANK and all AB subgroup codes into AB*/
proc sql;
	create table disbursement_info as
	select case when employersubgroupcode like 'UBANK' or employersubgroupcode like '%AFRICANBANK%'
		then 'AFRICANBANKST' else employersubgroupcode end as employersubgroupcode, *
	from disbursement_info;
quit;



proc sort data = disbursement_info ;
	by tranappnumber descending uniqueid  ;
run;

proc sort data = disbursement_info nodupkey dupout=dups  out= rawdata_no_dup ;
	by tranappnumber  ;
run;
	

data disbursement_info;
    set rawdata_no_dup;
    where input(applicationdate,yymmdd10.) between "&month12"d and "&lastmonth"d; *1May2022 and 30APR23;
    month = put(input(applicationdate,yymmdd10.),yymmn6.);
    disbursed_date =  intnx('month',input(applicationdate,yymmdd10.),0,'end'); *end of the month of application;
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

		data _null_;
			format date1 date9.;

			date1 =input(cats(&month,'01'),yymmdd8.);

			put date1 ;
	run;

        %put &i &j;
        data _null_;
            call symput('month1',put(intnx('month',input(cats(&month,'01'),yymmdd8.),-0 ,'end'),date9.)); *end of that month;
            call symput('month6',put(intnx('month',input(cats(&month,'01'),yymmdd8.), -11,'begin'),date9.)); *12 months;
        run;

        %put &month ---> &month1 and &month6; *from end of month to beginning of the 12th month back;
/*		e.g 202205 ---> 31MAY2022 and 01JUN2021*/
        proc sql;
            create table disbursement_&period._&month. as
                select  employersubgroupcode, count(*) as volume 
                from disbursement_info
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

%monthly_volume(period =12);


/***************************************************Calibration employmenttable*********************************************************/

data month_12_volume_v2;
    set month_12_volume;
    volume_last_12_month = volume;
    if employersubgroupcode ='UNKNOWN' then  volume_last_12_month = -1;
run;

data month_12_volume_v2;
	set month_12_volume_v2;
	employersubgroupcode = compress(employersubgroupcode,'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789','k');
run;

data month_12_volume_v2;
    set month_12_volume_v2;
	Format volume_last_12_month_B $500. ;
	If ( volume_last_12_month <= 3712) or volume_last_12_month in (-1)  then volume_last_12_month_B = "B1: volume_last_12_month <= 3712 or volume_last_12_month in (-1)";
	Else If (volume_last_12_month > 3712)  then volume_last_12_month_B = "B2: volume_last_12_month  > 3712";
run;

proc sort data=month_12_volume_v2;
	by employersubgroupcode;
run;

proc sort data=month_12_volume_v2 nodupkey dupout=dupout out=final;
	by employersubgroupcode;
run;

/*proc sql stimer;*/
/*      connect to ODBC (dsn=mpwaps);*/
/*      create table ICBDATA as */
/*      select * from connection to odbc ( */
/*            select distinct b.EmployerSUbGroupCode, d.Industry, D.SuperSector, d.Sector,d.subSector*/
/*            from (select uniqueid ,tranappnumber from prd_press.capri.capri_loan_application_2021 where applicationdate >= &lastmonth12) a*/
/*			left join  prd_press.capri.capri_employment b*/
/*			on a.uniqueid = b.uniqueid*/
/*			left  join  prd_crup.ic.crup_subgroup_industry c*/
/*			on b.employersubgroupcode = c.reference_concatenated*/
/*			left join prd_crup.ic.crup_subgroup_abicb_codes d*/
/*			on c.employer_tradename = d.tradename */
/*	) ;*/
/*      disconnect from odbc ;*/
/*quit;*/
/**/
/*data ICBDATA2;*/
/*	set ICBDATA;*/
/*	rename employersubgroupcode = employersubgroupcode_V2 subsector = subsector_V2 sector = sector_V2 industry = industry_V2 supersector = supersector_V2;*/
/*run;*/

data ICBDATA2;
	set ICB.ICB;
run;

proc sort data=ICBDATA2 nodupkey;
	by employersubgroupcode;
run;

proc sql;
    create table month_12_volume1
        as select a.*, b.*
        from final a  
        left join ICBDATA2 B 
        on a.employersubgroupcode = B.EmployerSUbGroupCode ;
quit;
/*employersubgroupcode like "AFRICANBANK%"*/

data month_12_volume2(keep =  
		employersubgroupcode volume_last_12_month volume_last_12_month_B  
		industry_name supersector_name sector_name subsector_name subsector sector industry supersector
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
        left join calib.subsector_names b
        on upcase(a.subsector) = upcase(b.label)
        left join (select * from calib._ESTIMATE1_ where parameter not in ('V6_PROB5_TRANS' ,'')) c
        on upcase(b.name) = upcase(c.parameter)
		order by employersubgroupcode;
quit;
/*64 720*/

data V6_calibration_volume_subsector;
    set month_12_volume3;
	keep employersubgroupcode volume_last_12_month volume_last_12_month_B subsector_name tValue tValue_B;

	if tvalue = . then tvalue = 0;

	Format tValue_B $500. ;
	If tValue <= -4.420089908 then tValue_B = "B1: tValue <= -4.420089908";
	Else If tValue <= -3.248592369 then tValue_B = "B2: tValue <= -3.248592369";
	Else If tValue <= 3.9556976529  then tValue_B = "B3: <= 3.9556976529";
	Else If (tValue > 3.9556976529)  then tValue_B = "B4: tValue  > 3.9556976529";

	Subsector_name=subsector;
run;


data EmployerVolumeIndex(keep=employersubgroupcode EmployerVolumeIndex EmployerVolumeIndexGroup ICBCode Subsector_name SubSectorIndex SubSectorIndexGroup runmonth);
	set V6_calibration_volume_subsector;
	format EmployerVolumeIndex ICBCode 19. EmployerVolumeIndexGroup $18. subsector_name $70. SubSectorIndex 20.8 SubSectorIndexGroup $500. runmonth $6.;
	ICBCode = .;
	EmployerVolumeIndex = Volume_last_12_month;
	EmployerVolumeIndexGroup = substr(volume_last_12_month_B, 2,1);
	SubSectorIndex = tvalue;
	SubSectorIndexGroup = substr(tValue_B,2,1);
	runmonth = "&runmonth";
run;

data EmployerVolumeIndex;
	set EmployerVolumeIndex;
	if upcase(employersubgroupcode) ='PREAPPROVEDOFFER' then EmployerVolumeIndexGroup = '1';
	if upcase(employersubgroupcode) ='PREAPPROVEDOFFER' then SubSectorIndexGroup = '3';
run;

proc sql;
	create table EmployerVolumeIndex_V667 as
	select employersubgroupcode, subsector_name, EmployerVolumeIndex, ICBCode, EmployerVolumeIndexGroup,
		SubSectorIndex, SubSectorIndexGroup, runmonth
	from EmployerVolumeIndex;
quit;

%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\Upload_APS.sas";
%Upload_APS(Set = EmployerVolumeIndex_V667 , Server =Work, APS_ODBC = PRD_DaDi, APS_DB = PRD_DataDistillery , distribute = HASH([employersubgroupcode]));