%let todaysDate = %sysfunc(today(), yymmddn8.);
%put &todaysDate;

proc sql; connect to odbc (dsn=MPWAPS);
execute (
		use PRD_DataDistillery_Data
		create table [dbo].[VW_CompBurTrades_&todaysDate] 
		with (distribution = hash(clientnumber ), clustered columnstore index ) as
			select clientnumber , 
       Retro_date,
          Load_type,
          info_date,
          Load_date,
          min(Open_date) as DateFirstTrade,
          max(Open_date) as DateLastTrade,
       count(*) as NumTradesEver , 
       sum(case when RETROCURINS1 > 0 then 1 else 0 end) as NumCurrentlyOpenTrades,
          sum(cast(isnull(RETROCURBAL1,0) as int)) as TotalOutstandingBalance,
          sum(cast(isnull(RETROCURINS1,0) as int)) as TotalInstalment,
          max(case when (HL  = 1 or (c.subscriber_name is null and b.account_type = 'B')) then 1 else 0 end) as HLEver,
          max(case when (HL  = 1 and RETROCURINS1 > 0 or (c.subscriber_name is null and b.account_type = 'B'  and RETROCURINS1 > 0))  then 1 else 0 end) as HL, 
          sum(case when (HL = 1 or (c.subscriber_name is null and b.account_type = 'B'))  then 1 else 0 end) as NumHLEver,
          sum(case when (HL = 1 and RETROCURINS1 > 0  or (c.subscriber_name is null and b.account_type = 'B'  and RETROCURINS1 > 0)) then 1 else 0 end) as NumCurrHl,
          sum(case when (HL = 1 and RETROCURINS1 > 0  or (c.subscriber_name is null and b.account_type = 'B'  and RETROCURINS1 > 0))then RETROCURBAL1 else 0 end) as HlCurrbalance,
          sum(case when (HL = 1 and RETROCURINS1 > 0  or (c.subscriber_name is null and b.account_type = 'B'  and RETROCURINS1 > 0))then RETROCURINS1 else 0 end) as HlCurrInstalment,
          sum(case when (HL = 1 and RETROCURINS1 > 0  or (c.subscriber_name is null and b.account_type = 'B'  and RETROCURINS1 > 0))then RETROOPBAL1 else 0 end) as HlOpenBal,
          sum(case when (HL = 1 and RETROCURINS1 > 0  or (c.subscriber_name is null and b.account_type = 'B'  and RETROCURINS1 > 0))then Terms else 0 end) as HlSumTerm,

          max(case when CC  = 1 then 1 else 0 end) as CCEver,
          max(case when CC  = 1 and RETROCURINS1 > 0  then 1 else 0 end) as CC, 
          sum(case when CC = 1 then 1 else 0 end) as NumCCEver,
          sum(case when CC = 1 and RETROCURINS1 > 0 then 1 else 0 end) as NumCurrCC,
          sum(case when CC = 1 and RETROCURINS1 > 0 then RETROCURBAL1 else 0 end) as CCCurrbalance,
          sum(case when CC = 1 and RETROCURINS1 > 0 then RETROCURINS1 else 0 end) as CCCurrInstalment,
          sum(case when CC = 1 and RETROCURINS1 > 0 then RETROOPBAL1 else 0 end) as CCOpenBal,
          sum(case when CC = 1 and RETROCURINS1 > 0 then Terms else 0 end) as CCSumTerm,


          max(case when VAF  = 1 then 1 else 0 end) as VAFEver,
          max(case when VAF  = 1 and RETROCURINS1 > 0  then 1 else 0 end) as VAF, 
          sum(case when VAF = 1 then 1 else 0 end) as NumVAFEver,
          sum(case when VAF = 1 and RETROCURINS1 > 0 then 1 else 0 end) as NumCurrVAF,
          sum(case when VAF = 1 and RETROCURINS1 > 0 then RETROCURBAL1 else 0 end) as VAFCurrbalance,
          sum(case when VAF = 1 and RETROCURINS1 > 0 then RETROCURINS1 else 0 end) as VAFCurrInstalment,
          sum(case when VAF = 1 and RETROCURINS1 > 0 then RETROOPBAL1 else 0 end) as VAFOpenBal,
          sum(case when VAF = 1 and RETROCURINS1 > 0 then Terms else 0 end) as VAFSumTerm,

          max(case when RCP  = 1 then 1 else 0 end) as RCPEver,
          max(case when RCP  = 1 and RETROCURINS1 > 0  then 1 else 0 end) as RCP, 
          sum(case when RCP = 1 then 1 else 0 end) as NumRCPEver,
          sum(case when RCP = 1 and RETROCURINS1 > 0 then 1 else 0 end) as NumCurrRCP,
          sum(case when RCP = 1 and RETROCURINS1 > 0 then RETROCURBAL1 else 0 end) as RCPCurrbalance,
          sum(case when RCP = 1 and RETROCURINS1 > 0 then RETROCURINS1 else 0 end) as RCPCurrInstalment,
          sum(case when RCP = 1 and RETROCURINS1 > 0 then RETROOPBAL1 else 0 end) as RCPOpenBal,
          sum(case when RCP = 1 and RETROCURINS1 > 0 then Terms else 0 end) as RCPSumTerm,

            max(case when RC  = 1 then 1 else 0 end) as RCEver,
          max(case when RC  = 1 and RETROCURINS1 > 0  then 1 else 0 end) as RC, 
          sum(case when RC = 1 then 1 else 0 end) as NumRCEver,
          sum(case when RC = 1 and RETROCURINS1 > 0 then 1 else 0 end) as NumCurrRC,
          sum(case when RC = 1 and RETROCURINS1 > 0 then RETROCURBAL1 else 0 end) as RCCurrbalance,
          sum(case when RC = 1 and RETROCURINS1 > 0 then RETROCURINS1 else 0 end) as RCCurrInstalment,
          sum(case when RC = 1 and RETROCURINS1 > 0 then RETROOPBAL1 else 0 end) as RCOpenBal,
          sum(case when RC = 1 and RETROCURINS1 > 0 then Terms else 0 end) as RCSumTerm,

          max(case when ((PL  = 1 and terms >= 6) or ( c.subscriber_name is null and B.Account_Type = 'P' and terms >=6 ))  then 1 else 0 end) as PLEver,
          max(case when ((PL  = 1 and  terms >= 6 and RETROCURINS1 > 0) or 
                        (c.subscriber_name is null and B.Account_Type = 'P' and terms >=6 and RETROCURINS1 > 0 ))  then 1 else 0 end) as PL,
          sum(case when ( (PL = 1 and  terms >= 6)  or (b.subscriber_name is null and B.Account_Type = 'P' and terms >=6) ) then 1 else 0 end) as NumPLEver,
          sum(case when ((PL = 1 and  terms >= 6 and RETROCURINS1 > 0) or (c.subscriber_name is null and B.Account_Type = 'P' and terms >=6 and RETROCURINS1 > 0))  then 1 else 0 end) as NumCurrPL,
          sum(case when ((PL = 1 and  terms >= 6 and RETROCURINS1 > 0) or (c.subscriber_name is null and B.Account_Type = 'P' and terms >=6 and RETROCURINS1 > 0))   then RETROCURBAL1 else 0 end) as PLCurrbalance,
          sum(case when ((PL = 1 and  terms >= 6 and RETROCURINS1 > 0) or (c.subscriber_name is null and B.Account_Type = 'P' and terms >=6 and RETROCURINS1 > 0))  then RETROCURINS1 else 0 end) as PLCurrInstalment,
          sum(case when ((PL = 1 and  terms >= 6 and RETROCURINS1 > 0) or (c.subscriber_name is null and B.Account_Type = 'P' and terms >=6 and RETROCURINS1 > 0))  then RETROOPBAL1 else 0 end) as PLOpenBal,
          sum(case when ((PL = 1 and  terms >= 6 and RETROCURINS1 > 0) or (c.subscriber_name is null and B.Account_Type = 'P' and terms >=6 and RETROCURINS1 > 0))  then Terms else 0 end) as PLSumTerm,


          
          max(case when ((PL  = 1 and terms <6) or ( c.subscriber_name is null and B.Account_Type = 'P' and terms <6 ))  then 1 else 0 end) as ShortPLEver,
          max(case when ((PL  = 1 and  terms <6 and RETROCURINS1 > 0) or 
                        (c.subscriber_name is null and B.Account_Type = 'P' and terms <6 and RETROCURINS1 > 0 ))  then 1 else 0 end) as ShortPL,
          sum(case when ( (PL = 1 and  terms <6)  or (b.subscriber_name is null and B.Account_Type = 'P' and terms <6) ) then 1 else 0 end) as NumShortPLEver,
          sum(case when ((PL = 1 and  terms <6 and RETROCURINS1 > 0) or (c.subscriber_name is null and B.Account_Type = 'P' and terms <6 and RETROCURINS1 > 0))  then 1 else 0 end) as NumCurrShortPL,
          sum(case when ((PL = 1 and  terms <6 and RETROCURINS1 > 0) or (c.subscriber_name is null and B.Account_Type = 'P' and terms <6 and RETROCURINS1 > 0))   then RETROCURBAL1 else 0 end) as ShortPLCurrbalance,
          sum(case when ((PL = 1 and  terms <6 and RETROCURINS1 > 0) or (c.subscriber_name is null and B.Account_Type = 'P' and terms <6 and RETROCURINS1 > 0))  then RETROCURINS1 else 0 end) as ShortPLCurrInstalment,
          sum(case when ((PL = 1 and  terms <6 and RETROCURINS1 > 0) or (c.subscriber_name is null and B.Account_Type = 'P' and terms <6 and RETROCURINS1 > 0))  then RETROOPBAL1 else 0 end) as ShortPLOpenBal,
          sum(case when ((PL = 1 and  terms <6 and RETROCURINS1 > 0) or (c.subscriber_name is null and B.Account_Type = 'P' and terms <6 and RETROCURINS1 > 0))  then Terms else 0 end) as ShortPLSumTerm,


          max(case when Telco  = 1 then 1 else 0 end) as TelcoEver,
          max(case when Telco  = 1 and RETROCURINS1 > 0  then 1 else 0 end) as Telco, 
          sum(case when Telco = 1 then 1 else 0 end) as NumTelcoEver,
          sum(case when Telco = 1 and RETROCURINS1 > 0 then 1 else 0 end) as NumCurrTelco,
          sum(case when Telco = 1 and RETROCURINS1 > 0 then RETROCURBAL1 else 0 end) as TelcoCurrbalance,
          sum(case when Telco = 1 and RETROCURINS1 > 0 then RETROCURINS1 else 0 end) as TelcoCurrInstalment,
          sum(case when Telco = 1 and RETROCURINS1 > 0 then RETROOPBAL1 else 0 end) as TelcoOpenBal,
          sum(case when Telco = 1 and RETROCURINS1 > 0 then Terms else 0 end) as TelcoSumTerm,

          max(case when ((OML  = 1) or (C.Subscriber_Name is null and b.account_type = 'M' )) then 1 else 0 end) as OMLEver,
          max(case when ((OML  = 1 and RETROCURINS1 > 0) or (C.Subscriber_Name is null and b.account_type = 'M' and RETROCURINS1 > 0))  then 1 else 0 end) as OML, 
          sum(case when ((OML = 1) or (C.Subscriber_Name is null and b.account_type = 'M' )) then 1 else 0 end) as NumOMLEver,
          sum(case when ((OML = 1 and RETROCURINS1 > 0) or (C.Subscriber_Name is null and b.account_type = 'M' and RETROCURINS1 > 0)) then 1 else 0 end) as NumCurrOML,
          sum(case when ((OML = 1 and RETROCURINS1 > 0) or (C.Subscriber_Name is null and b.account_type = 'M' and RETROCURINS1 > 0)) then RETROCURBAL1 else 0 end) as OMLCurrbalance,
          sum(case when ((OML = 1 and RETROCURINS1 > 0) or (C.Subscriber_Name is null and b.account_type = 'M' and RETROCURINS1 > 0)) then RETROCURINS1 else 0 end) as OMLCurrInstalment,
          sum(case when ((OML = 1 and RETROCURINS1 > 0) or (C.Subscriber_Name is null and b.account_type = 'M' and RETROCURINS1 > 0)) then RETROOPBAL1 else 0 end) as OMLOpenBal,
          sum(case when ((OML = 1 and RETROCURINS1 > 0) or (C.Subscriber_Name is null and b.account_type = 'M' and RETROCURINS1 > 0)) then Terms else 0 end) as OMLSumTerm,

                max(case when I  = 1 then 1 else 0 end) as IEver,
          max(case when I  = 1 and RETROCURINS1 > 0  then 1 else 0 end) as I, 
          sum(case when I = 1 then 1 else 0 end) as NumIEver,
          sum(case when I = 1 and RETROCURINS1 > 0 then 1 else 0 end) as NumCurrI,
          sum(case when I = 1 and RETROCURINS1 > 0 then RETROCURBAL1 else 0 end) as ICurrbalance,
          sum(case when I = 1 and RETROCURINS1 > 0 then RETROCURINS1 else 0 end) as ICurrInstalment,
          sum(case when I = 1 and RETROCURINS1 > 0 then RETROOPBAL1 else 0 end) as IOpenBal,
          sum(case when I = 1 and RETROCURINS1 > 0 then Terms else 0 end) as ISumTerm
from  PRD_BUR.dbo.Comp_CPANLREVO_20230531 B 
left join PRD_DataDistillery_Data.dbo.LSubscriberName_Compuscan C
on B.Subscriber_Name  = C.Subscriber_Name and B.Account_type = C.Account_type
inner join  PRD_DataDistillery_Data.dbo.VW_LatestClientNums D 
on ltrim(rtrim(B.id_no)) = D.idnumber
where isnumeric(RETROCURINS1) = 1 and isnumeric(RETROCURBAL1) = 1 and isnumeric(RETROOPBAL1) = 1 
group by clientnumber , 
       Retro_date,
          Load_type,
          info_date,
          Load_date;
 ;
		) by odbc;
quit;	
		


