/*Append TU_applicationbase */

proc sql; connect to odbc (dsn=MPWAPS);
execute 
(
	/*---- Drop Table ----*/
	IF OBJECT_ID('DEV_DataDistillery_General.dbo.TU_applicationbase', 'U') IS NOT NULL 
	DROP TABLE DEV_DataDistillery_General.dbo.TU_applicationbase;

	/*---- Append Tables ----*/
	Create table DEV_DataDistillery_General.dbo.TU_applicationbase
	with (distribution = hash(baseloanid), clustered columnstore index ) as
	select * 
	from DEV_DataDistillery_General.dbo.TU_applicationbase_BackUp
	union all
	select * 
	from DEV_DataDistillery_General.dbo.TU_applicationbase_&month;
) by odbc;
quit;

/*Append CS_applicationbase */
proc sql; connect to odbc (dsn=MPWAPS);
execute 
(
	/*---- Drop Table ----*/
	IF OBJECT_ID('DEV_DataDistillery_General.dbo.CS_applicationbase', 'U') IS NOT NULL 
	DROP TABLE DEV_DataDistillery_General.dbo.CS_applicationbase;

	/*---- Append Tables ----*/
	Create table DEV_DataDistillery_General.dbo.CS_applicationbase
	with (distribution = hash(baseloanid), clustered columnstore index ) as
	select * 
	from DEV_DataDistillery_General.dbo.CS_applicationbase_BackUp
	union all
	select * 
	from DEV_DataDistillery_General.dbo.CS_applicationbase_&month;
) by odbc;
quit;