%let odbc = MPWAPS;
data _null_;
     call symput("month", put(intnx("month", today(),-1,'end'),yymmn6.));
run;
libname tu '\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets\V580';
libname tu2 '\\mpwsas64\Core_Credit_Risk_Models\V5\V5 Monitoring\Performance Monitoring\Transunion Monitoring\datasets';

proc sql stimer;
	connect to ODBC (dsn=&odbc);
	create table DISBURSEDBASE4REBLT_&month as 
	select * from connection to odbc ( 
		select *
		from DEV_DataDistillery_General.dbo.DISBURSEDBASE4REBLT_&month
	) ;
	disconnect from odbc ;
quit;

data Disbursedbase6;
	set DISBURSEDBASE4REBLT_&month;
	Compuscan_Generic = prismscoremi;
	Tu_Generic = Tu_Score   ;
run;

%include "\\mpwsas64\Core_Credit_Risk_Model_Team\GitLab_Local_Repo\Macros\V655_RTI_ORM_scoring.sas";
%V655_RTI_ORM_scoring(indata=Disbursedbase6);

data tu.RTI_ORM;
set RTI_ORM;
run;