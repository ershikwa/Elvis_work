/*The purpose of this code is to ensure we only delete the numbers from tbclinet that are either both business and model or both business and cell phone from tbclient
. Finally to also remove these numbers from the output of the OLD BNM*/

%include "H:\Process_Automation\sas_autoexec\sas_autoexec.sas";

%let projectcode =H:\Process_Automation\Codes;

%let runmonth = %scan(&sysparm,1);
%let process_number = %scan(&sysparm,2);
%let process =  %scan(&sysparm,3);

%let project =pj;

libname &project "&process";

%start_program;

/*The purpose of this code is to ensure we only delete the numbers from tbclient that are either both business and model or both business and cell phone from tbclient
. Finally to also remove these numbers from the output of the OLD BNM*/

options compress =  yes ;
libname bnmdata '\\MPWSAS65\BNM\BNM_Data';

/*pulling our tbclient data*/

proc sql stimer;
    connect to ODBC (dsn=MPWAPS);
	create table tb_client_latest as 
		select * from connection to odbc 
		( 	select identificationnumber, clientnumber, 
				   edwdw.dbo.udf_DeCodeTelNumber (residentialtelephone) as residentialtelephone, 
				   edwdw.dbo.udf_DeCodeTelNumber (Cellphone) as Cellphone, 
				   edwdw.dbo.udf_DeCodeTelNumber (Businesstelephone) as Businesstelephone 
			from edwdw.dbo.vwtbclientlatest
			where Businesstelephone <> ''
		);
	disconnect from odbc ;
quit;

data tb_client_latest;
	set tb_client_latest;
	format residentialtelephone $10. Cellphone $10. Businesstelephone $10.;
run;


/*creating a flag to see if the work number matches either the home or cell number*/

proc sql;
     create table work_delete as
     select a.*, 
     case when a.Businesstelephone = b.ResidentialTelephone or a.Businesstelephone = b.Cellphone then 0 else 0 end as delete /*change the else back to 1 to switch on work number dlete*/
     from tb_client_latest a
     left join tb_client_latest b
     on a.identificationnumber=b.identificationnumber and a.clientnumber = b.clientnumber;
quit;

proc freq data=work_delete;
     table delete / missing;
run;

data bnmdata.work_numbers_delete;
     set work_delete;
     where delete = 1;
run;

/*Deleting the wortk numbers from the CIC table*/

DATA bnm_data_flip_cic (KEEP=idnumber clientnumber Number RankScore Best_Mobile);
SET bnmdata.cred_scoring_cic_best;
     ARRAY Numbers{12} Number_1-Number_12;
     ARRAY RankScores{12} RankScore_1-RankScore_12; 

     do i = 1 to 12 ;
     Number = Numbers{i}    ;
     RankScore = RankScores{i};
     OUTPUT;
     end;
RUN;

proc sql;
     create table tb_flag_cic as
     select a.*, case when a.clientnumber = b.clientnumber and a.Number = b.Businesstelephone then 1 else 0 end as flag
     from bnm_data_flip_cic a
     left join bnmdata.work_numbers_delete b
     on a.clientnumber = b.clientnumber
     where a.number <> '';
quit;

data bnm_nums_no_work_cic(drop= flag);
     set tb_flag_cic;
     if flag = 1 then do;
     number = '';
     rankscore = .;
     end;
     else do;
     number = number;
     rankscore= rankscore;
     end;
run;

proc sort data=bnm_nums_no_work_cic;
     by idnumber clientnumber descending rankscore descending number best_mobile;
run;

data ranked_nums_cic;
     set bnm_nums_no_work_cic;
     by idnumber clientnumber descending rankscore descending number best_mobile;
     if first.clientnumber then rn =1;
     else rn +1;
     if rn <= 12;
run;

/*Flipping our data again to get our best numbers table*/

proc transpose data=ranked_nums_cic  prefix= Number_ out=test(drop= _name_ _label_);
     by idnumber clientnumber best_mobile;
     id rn;
     idlabel rn;
     var number ;
run;

proc transpose data=ranked_nums_cic  prefix= RankScore_ out=test2_(drop= _name_ _label_);
     by idnumber ClientNumber best_mobile;
     id rn;
     idlabel rn;
     var rankscore ;
run;

data fin_bnm_table_cic;
     merge test test2_;
     by idnumber clientnumber best_mobile;
run;

proc sort data=fin_bnm_table_cic nodupkey out=bnmdata.fin_bnm_table_cic;
     by _all_;
run;

/*Deleting the numbers from the BNM table*/

DATA bnm_data_flip (KEEP=idnumber clientnumber Number RankScore Best_Mobile);
SET bnmdata.cred_scoring_best;
     ARRAY Numbers{12} Number_1-Number_12;
     ARRAY RankScores{12} RankScore_1-RankScore_12; 

     do i = 1 to 12 ;
     Number = Numbers{i}    ;
     RankScore = RankScores{i};
     OUTPUT;
     end;
RUN;

/*proc sort data=bnm_data_flip;*/
/*   by _all_;*/
/*run;*/
/**/
/*data flip_no_blanks;*/
/*   set bnm_data_flip;*/
/*   where number <> '';*/
/*run; */

proc sql;
     create table tb_flag as
     select a.*, case when a.clientnumber = b.clientnumber and a.Number = b.Businesstelephone then 1 else 0 end as flag
     from bnm_data_flip a
     left join bnmdata.work_numbers_delete b
     on a.clientnumber = b.clientnumber
     where a.number <> '';
quit;

data bnm_nums_no_work(drop= flag);
     set tb_flag;
     if flag = 1 then do;
     number = '';
     rankscore = .;
     end;
     else do;
     number = number;
     rankscore= rankscore;
     end;
run;

proc sort data=bnm_nums_no_work;
     by idnumber clientnumber descending rankscore descending number best_mobile;
run;

data ranked_nums;
     set bnm_nums_no_work;
     by idnumber clientnumber descending rankscore descending number best_mobile;
     if first.clientnumber then rn =1;
     else rn +1;
     if rn <= 12;
run;

/*Flipping our data again to get our best numbers table*/

proc transpose data=ranked_nums  prefix= Number_ out=test(drop= _name_ _label_);
     by idnumber clientnumber;
     id rn;
     idlabel rn;
     var number ;
run;

proc transpose data=ranked_nums  prefix= RankScore_ out=test2_(drop= _name_ _label_);
     by idnumber ClientNumber ;
     id rn;
     idlabel rn;
     var rankscore ;
run;

data fin_bnm_table;
     merge test test2_;
     by idnumber clientnumber;
run;

proc sql;
     create table fin_bnm_table as
     select a.*, b.best_mobile from fin_bnm_table a
     left join ranked_nums b
     on a.idnumber =b.idnumber and a.clientnumber = b.clientnumber;
quit;

proc sort data=fin_bnm_table nodupkey out=bnmdata.fin_bnm_table;
     by _all_;
run;


filename macros2 'H:\Process_Automation\macros';
options sasautos = (sasautos  macros2);

%end_program(&process_number);
