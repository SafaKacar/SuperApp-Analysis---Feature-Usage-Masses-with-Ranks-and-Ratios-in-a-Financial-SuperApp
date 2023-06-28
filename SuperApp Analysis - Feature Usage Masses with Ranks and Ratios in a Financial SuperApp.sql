USE [DWH_DB]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
/*
PROJECT: User Behaviour through Main Features
*/
--set statistics io on
--set statistics time on

-- =============================================

ALTER PROCEDURE [dbo].[spMainFeatureCombinations]
as

DECLARE 
		 @DateStartTransactions as Date = dateadd(day, 1, eomonth(getdate(), -1))	--dateadd(day, 1, eomonth(getdate(), -80))		--Sadece rakamı değiştirerek kaç ay geriye gidileceği seçilir--1 > içinde bulunduğumuz ayı ifade eder*/
		,@DateEndTransactions   as Date = CAST(GETDATE() AS date)					--dateadd(day, 1, eomonth(getdate(), -79))		--DATEADD(MONTH,-1,eomonth(getdate())) /*Bir önceki ayın sonuna git*/

		IF DAY(GETDATE()) = 1
		   BEGIN
			  SET @DateStartTransactions = dateadd(day,1,eomonth(getdate(),-2))
		   END
DELETE FROM DWH_DB..FACT_MainFeatureCombinations WHERE MonthKey=FORMAT(@DateStartTransactions,'yyyyMM')
		;

WITH CTE_CombinationUserKey AS
	(
	select
				-- FORMAT(@DateStartTransactions,'yyyyMM') MonthKey-- CAST(CreatedAt as date) [Date]
				 UserKey
				,FeatureType
				, COUNT(TransactionsMANIPULATED.Id)  																												 TxCount
		--		,(SUM(COUNT(Id))   OVER (Partition By FeatureType,UserKey))*1.0 / SUM(COUNT(Id)) OVER ()	  Proportion
				, SUM(TransactionsMANIPULATED.Amount)																												 TxVolume
				, SUM(ABS(TransactionsMANIPULATED.Amount))																										 AbsTxVolume
				, SUM(ResultingBalance)																													     ResultingBalanceSum
				, COUNT(CASE WHEN ResultingBalance IS NOT NULL						 THEN ResultingBalance									  ELSE NULL END) ParametricResultingBalanceCountNotNull
				, MAX(	CASE WHEN UserType != 0 AND DateOfBirth < @DateStartTransactions   THEN DATEDIFF(DAY,DateOfBirth,@DateStartTransactions)/(365.25) ELSE NULL END) UserAge
				, MAX(IIF(UserType != 0 AND DateOfBirth < @DateStartTransactions,1,0))																			 ParameterAgeFlagManipulated	
			 
	from 
		
		(
		SELECT
																				Id
			,																	CreatedAt
			,																	UserKey
			,																	Amount
			,																	ResultingBalance
			,CASE WHEN FeatureType = 7 AND RemittanceType = 0 THEN 7.2--'7(0)'
				  WHEN FeatureType = 7 AND RemittanceType = 1 THEN 7.1--'7(1)'
				  WHEN FeatureType = 1 AND BankTransferType  = 0 THEN 1.0--'1(0)'
				  WHEN FeatureType = 1 AND BankTransferType  = 1 THEN 1.1--'1(1)'
				  WHEN FeatureType = 2 AND DBCardTxType  = 0 THEN 2.0--'2(0)'
				  WHEN FeatureType = 2 AND DBCardTxType  = 2 THEN 2.2--'2(2)'
				  WHEN FeatureType = 2 AND DBCardTxType  = 3 THEN 2.3--'2(3)'
				  WHEN FeatureType = 2 AND DBCardTxType  = 4 THEN 2.4--'2(4)'
				  WHEN FeatureType = 2 AND DBCardTxType  = 5 THEN 2.5--'2(5)'
				  WHEN FeatureType = 2 AND DBCardTxType  = 6 THEN 2.6--'2(6)'
				  WHEN FeatureType = 2 AND DBCardTxType  = 7 THEN 2.7--'2(7)'
				  WHEN FeatureType = 2 AND DBCardTxType  = 8 THEN 2.8--'2(8)'
				  WHEN FeatureType = 2 AND DBCardTxType  = 9 THEN 2.9--'2(9)'
				  WHEN FeatureType = 32 AND RemittanceType = 0 THEN 32.2--'32(0)'
				  WHEN FeatureType = 32 AND RemittanceType = 1 THEN 32.1--'32(0)'
				  WHEN FeatureType = 2 AND DBCardTxType  = 1 AND Is_Offline = 0 THEN 2.12
				  WHEN FeatureType = 2 AND DBCardTxType  = 1 AND Is_Offline = 1 THEN 2.11
			 ELSE CAST(FeatureType AS decimal(15,1))								   END FeatureType
		                                                       FROM
																(
																	SELECT L.Id,L.CreatedAt,UserKey,FeatureType,L.Amount,	  RemittanceType, BankTransferType, DBCardTxType, Is_Offline,ResultingBalance			  FROM FACT_Transactions		 L (nolock)
																																					LEFT JOIN FACT_BankTransferRequests btr on btr.Id = L.StartingRequestId
																																																						  WHERE L.CreatedAt >= @DateStartTransactions AND L.CreatedAt < @DateEndTransactions	--CAST(CreatedAt as date) = @Date																						
																	UNION all
																	SELECT Id,CreatedAt,UserKey,FeatureType,Amount,NULL RemittanceType,NULL BankTransferType,NULL DBCardTxType,NULL Is_Offline,NULL ResultingBalance FROM FACT_ExternalTransactions with (nolock) WHERE CreatedAt >= @DateStartTransactions AND CreatedAt < @DateEndTransactions	--CAST(CreatedAt as date) = @Date
																) 
																TransactionsS
		) 
		TransactionsMANIPULATED
		JOIN DIM_Users (nolock) u on u.User_Key = TransactionsMANIPULATED.UserKey
	group by FeatureType,UserKey
	)
	, CTE_FeatureTypeStringAggregations AS
	(
	SELECT
		 --  FORMAT(@DateStartTransactions,'yyyyMM') MonthKey--[Date]
		   UserKey
		  ,MAX(UserAge)		UserAge --YER DEĞİŞİLEBİLİR
		  ,COUNT(FeatureType) UserFeatureTypeCount
		  ,STRING_AGG(FeatureType,'/') WITHIN GROUP (ORDER BY FeatureType) FeatureTypeComb

	FROM CTE_CombinationUserKey
	group by --MonthKey,
			 UserKey
	), CTE_RowNumberGrouping AS
	(
		SELECT
		--	 FORMAT(@DateStartTransactions,'yyyyMM') MonthKey
			 cuk.UserKey
			,UserFeatureTypeCount
			,FeatureTypeComb
			,cuk.UserAge
			,cuk.ParametricResultingBalanceCountNotNull
			,cuk.ResultingBalanceSum
			,FeatureType
			,ParameterAgeFlagManipulated
			,TxCount
			,TxVolume
			,AbsTxVolume
		FROM CTE_CombinationUserKey cuk
		JOIN CTE_FeatureTypeStringAggregations etsa on cuk.UserKey = etsa.UserKey
	), CTE_Overalls AS
	(
	SELECT
		FORMAT(@DateStartTransactions,'yyyyMM') MonthKey
	   ,FeatureTypeComb
	   ,FeatureType
	   ,SUM(ParameterAgeFlagManipulated)										   ParameterAgeFlagManipulatedSum
	   ,SUM(UserAge)															   ParametricUserAgeSum
	   ,SUM(ParametricResultingBalanceCountNotNull)								   SumParametricResultingBalanceCountNotNull
	   ,SUM(TxCount)															   SumParameterTxCount
	   ,SUM(TxVolume)															   SumParameterTxVolume
	   ,SUM(AbsTxVolume)														   SumParameterAbsTxVolume
	   ,SUM(ResultingBalanceSum)												   SumResultingBalanceSum
	   ,SUM(AbsTxVolume)*1.0 / COUNT(DISTINCT UserKey)							   AvgTxVolumePerCapitaForAgg
	   ,SUM(TxCount)*1.0	 / COUNT(DISTINCT UserKey)							   AvgTxCountPerCapitaForAgg
	   ,SUM(AbsTxVolume)*1.0 / SUM(TxCount)										   AvgTicketSizeForAgg
	   ,SUM(ResultingBalanceSum)*1.0 / SUM(ParametricResultingBalanceCountNotNull) AvgResultingBalancebyFeatureType
	   ,COUNT(DISTINCT UserKey) UserCount
	FROM CTE_RowNumberGrouping
	GROUP BY FeatureTypeComb,FeatureType
	), Final_CTE AS
	(
	SELECT
		 FORMAT(@DateStartTransactions,'yyyyMM')																		  MonthKey
		,																										  FeatureTypeComb
		,CAST(SUM(ParametricUserAgeSum)   / (SUM(ParameterAgeFlagManipulatedSum))		    AS DECIMAL(15,2))	  AvgAge
		,SUM(UserCount) / COUNT(FeatureType)																		  UserCount
		,CAST(SUM(SumResultingBalanceSum) /  SUM(SumParametricResultingBalanceCountNotNull) AS DECIMAL(15,2))	  AvgResultingBalance
		,SUM(SumParameterTxCount)																				  SumTxCount
		,CAST(SUM(SumParameterTxVolume)	   AS DECIMAL (15,2))													  SumTxVolume
		,CAST(SUM(SumParameterAbsTxVolume) AS DECIMAL (15,2))													  SumAbsTxVolume
		,COUNT(FeatureType)																						  FeatureTypeCount
		,CAST(SUM(SumParameterAbsTxVolume)*1.0 / (SUM(UserCount) / COUNT(FeatureType)) AS DECIMAL(15,2))			  AvgTxVolumePerCapita
		,CAST(SUM(SumParameterTxCount)	  *1.0 / (SUM(UserCount) / COUNT(FeatureType)) AS DECIMAL(15,2))			  AvgTxCountPerCapita
		,CAST(SUM(SumParameterAbsTxVolume)*1.0 /  SUM(SumParameterTxCount)		     AS DECIMAL(15,2))			  AvgTicketSize
		,STRING_AGG(	 SumParameterTxCount					  , '/') WITHIN GROUP (ORDER BY FeatureType)							  FeatureTypeCombUptoTxCount
		,STRING_AGG(CAST(SumParameterTxVolume AS DECIMAL(15,2)) , '/') WITHIN GROUP (ORDER BY FeatureType)							  FeatureTypeCombUptoTxVolume
		,STRING_AGG(CAST(SumParameterAbsTxVolume AS DECIMAL(15,2)) , '/') WITHIN GROUP (ORDER BY FeatureType)							  FeatureTypeCombUptoAbsTxVolume
		,STRING_AGG(CAST(AvgResultingBalancebyFeatureType AS DECIMAL(15,2)), '/') WITHIN GROUP (ORDER BY FeatureType) AvgResultingBalancebyFeatureType
		,STRING_AGG(CAST(AvgTxVolumePerCapitaForAgg	    AS DECIMAL(15,2)), '/') WITHIN GROUP (ORDER BY FeatureType) AvgTxVolumePerCapitaForAgg
		,STRING_AGG(CAST(AvgTxCountPerCapitaForAgg	    AS DECIMAL(15,2)), '/') WITHIN GROUP (ORDER BY FeatureType) AvgTxCountPerCapitaForAgg
		,STRING_AGG(CAST(AvgTicketSizeForAgg			AS DECIMAL(15,2)), '/') WITHIN GROUP (ORDER BY FeatureType) AvgTicketSizeForAgg
	FROM CTE_Overalls
	group by FeatureTypeComb
	), CTE_DeviationAddIns AS
	(
	SELECT
		--TOP 100 
		FORMAT(@DateStartTransactions,'yyyyMM') MonthKey
	   ,FCTE.FeatureTypeComb
	   ,FeatureType
	   /*Tablonun Kuruluş Amacı*/
	   ,COALESCE((CTEO.SumParameterTxCount)*1.0 /	  (NULLIF(SumTxCount		, 0)), 0)	TxCountRates
	   ,COALESCE((CTEO.SumParameterAbsTxVolume)*1.0 / (NULLIF(SumAbsTxVolume	, 0)), 0)	AbsTxVolumeRates
	FROM Final_CTE	  FCTE
	JOIN CTE_Overalls CTEO ON FCTE.MonthKey		 = CTEO.MonthKey
						  AND FCTE.FeatureTypeComb = CTEO.FeatureTypeComb
	), CTE_DeviationAddInsSTRAggregating AS
	(
	SELECT
		FORMAT(@DateStartTransactions,'yyyyMM') MonthKey
	   ,FeatureTypeComb
	   ,STRING_AGG(CAST(TxCountRates									AS DECIMAL(15,2)) , '/') WITHIN GROUP (ORDER BY FeatureType)		TxCountRates																
	   ,STRING_AGG(CAST(AbsTxVolumeRates								AS DECIMAL(15,2)) , '/') WITHIN GROUP (ORDER BY FeatureType)		AbsTxVolumeRates								
	FROM CTE_DeviationAddIns
	GROUP BY FeatureTypeComb--,MonthKey
	)
INSERT INTO DWH_DB..FACT_MainFeatureCombinations
	SELECT
			 FCT.MonthKey
			,FCT.FeatureTypeComb
--			,IIF((FCT.UserCount*1.0 / SUM(FCT.UserCount) OVER ())<=0.01,1,0) IsAboveThreshold << boxplot 1stkartil al
			,FCT.FeatureTypeCount
			,FCT.AvgAge
			,FCT.UserCount
			,FCT.SumTxCount
			,FCT.SumAbsTxVolume
			,FCT.SumTxVolume
			,RANK() OVER (ORDER BY FCT.UserCount	  DESC)	RankByUserCount
			,RANK() OVER (ORDER BY FCT.SumTxCount	  DESC)	RankBySumTxCount
			,RANK() OVER (ORDER BY FCT.SumAbsTxVolume DESC) RankBySumAbsTxVolume
			,CAST((RANK() OVER (ORDER BY FCT.UserCount	  DESC) + RANK() OVER (ORDER BY FCT.SumTxCount	  DESC) + RANK() OVER (ORDER BY FCT.SumAbsTxVolume DESC))*1.0/3 AS decimal(10,3)) RankingMetricWeightedAverages
			,FCT.UserCount	    *1.0   / SUM(FCT.UserCount)	     OVER () UserCountOverallPercentage
			,FCT.SumTxCount	    *1.0   / SUM(FCT.SumTxCount)	 OVER () SumTxCountOverallPercentage
			,FCT.SumAbsTxVolume *1.0   / SUM(FCT.SumAbsTxVolume) OVER () SumAbsTxVolumeOverallPercentage
			,CAST(((FCT.UserCount *1.0 / SUM(FCT.UserCount)		 OVER ()) + (FCT.SumTxCount *1.0 / SUM(FCT.SumTxCount) OVER ()) + (FCT.SumAbsTxVolume *1.0 / SUM(FCT.SumAbsTxVolume) OVER ()))*1.0/3 AS decimal(10,9)) PercentageMetricWeightedAverages
			,FCT.AvgTicketSize
			,FCT.AvgResultingBalance
			,FCT.AvgTxCountPerCapita
			,FCT.AvgTxVolumePerCapita
			,FCT.FeatureTypeCombUptoTxCount
			,FCT.FeatureTypeCombUptoTxVolume
			,FCT.FeatureTypeCombUptoAbsTxVolume
			,FCT.AvgResultingBalancebyFeatureType
			,FCT.AvgTxVolumePerCapitaForAgg
			,FCT.AvgTxCountPerCapitaForAgg
			,FCT.AvgTicketSizeForAgg
			,TxCountRates	
			,AbsTxVolumeRates
					,REPLACE(REPLACE(REPLACE(REPLACE(
					 REPLACE(REPLACE(REPLACE(REPLACE(
					 REPLACE(REPLACE(REPLACE(REPLACE(
					 REPLACE(REPLACE(REPLACE(REPLACE(
					 REPLACE(REPLACE(REPLACE(REPLACE(
					 REPLACE(REPLACE(REPLACE(REPLACE(
					 REPLACE(REPLACE(REPLACE(REPLACE(
					 REPLACE(REPLACE(REPLACE(REPLACE(
					 REPLACE(REPLACE(REPLACE(REPLACE(
					 REPLACE(REPLACE(REPLACE(REPLACE(
					 REPLACE(REPLACE(REPLACE(REPLACE(
					 REPLACE(REPLACE(
					 FCT.FeatureTypeComb,
					 '35.00','Precious Metal Transaction'),
					 '32.10','Streamer Payment (Sending)'),
					 '32.20','Streamer Payment (Receiving)'),
					 '33.00','International Money Transfer'),
					 '31.00','Membership Payment'),
					 '29.00','Card Purchase Fee'),
					 '28.00','Investment'),
					 '27.00','Lottery Payment'),
					 '25.00','Gift Card Topup'),
					 '24.00','Insurance'),
					 '23.00','Gift Card Payment'),
					 '22.00','Donation'),
					 '21.00','IBAN Money Transfer'),
					 '20.00','Card Money Transfer'),
					 '19.00','Saving Balance Transfer'),
					 '18.00','CityRingTravelCard Topup'),
					 '17.00','Game Payment'),
					 '16.00','Crpyto Transfer'),
					 '15.00','Cashback Reward'),
					 '1114.00','Bill Payment'),
					 '13.00','FX Transaction'),
					 '12.00','Pocket Money Transfer'),
					 '11.00','Closed Loop Payment (Canteen)'),
					 '10.00','Invitation Bonus'),
					 '222.11','Card (POS|Offline Tx.)'),
					 '222.12','Card (POS|Online Tx.)'),
					 '222.20','Card (ATM Balance Inquiry)'),
					 '222.30','Card (Card Fee)'),
					 '222.40','Card to Card Money Transfer'),
					 '222.50','Card (ATM Deposit)'),
					 '222.60','Card (Virtual Card Fee)'),
					 '222.70','Card (Montly Card Fee)'),
					 '222.80','Card (Card Fee Refund)'),
					 '222.90','Corporate Card Balance Deposit'),
					 '117.10','Closed Loop Money Transfer (Sending)'),
					 '117.20','Closed Loop Money Transfer (Receiving)'),
					 '1.10','Bank Transfer (Withdrawal)'),
					 '1.00','Bank Transfer (Deposit)'),
					 '0.00','Manual Transaction'),
					 '222.00','Card (ATM Withdraw)'),
					 '333.00','Bank/Credit Card Deposit'),
					 '4.00','Mobile Deposit'),
					 '5.00','BKM Deposit'),
					 '6.00','Cash Deposit from Physical Point'),
					 '8.00','Checkout Payment'),
					 '9.00','Mass Payment')
					 FeatureTypeCombNames			

	FROM CTE_DeviationAddInsSTRAggregating CTS
	JOIN Final_CTE FCT ON FCT.MonthKey = CTS.MonthKey AND FCT.FeatureTypeComb = CTS.FeatureTypeComb
	ORDER BY UserCount DESC
--set statistics io	off
--set statistics time off

/*YÜZDE İHTİYACI
1- A1 / SumTxCount 
2- A2 / SumTxVolume
3- A3 / SumAbsTxVolume

YÜZDE NE KADAR FAZLA/AZ >> ORTALAMAYI ARTIRAN/AZALTANLAR? İPTAL
4- -1.0*(AvgResultingBalance  - AvgResultingBalancebyFeatureType) / AvgResultingBalance  >> 
5- -1.0*(AvgTxVolumePerCapita - AvgTxVolumePerCapitaForAgg)     / AvgTxVolumePerCapita >> 
6- -1.0*(AvgTxCountPerCapita  - AvgTxCountPerCapitaForAgg)		/ AvgTxCountPerCapita  >>
7- -1.0*(AvgTicketSizeForAgg  - AvgTicketSize)					/ AvgTicketSizeForAgg  >>
*/

/* TEST SORGULARI
--DİNAMİK SORGU--
DECLARE
 @FeatureTypeCount  as tinyint = 3
,@DateStartTransactions as Date = '2016-08-01'
,@DateEndTransactions   as Date = '2016-09-01'
,@varFeatureType as nvarchar(max) = '0.00/3.00/8.00';					
WITH X1 AS
(
SELECT UserKey,FeatureType,COUNT(Id)  TxC, SUM(ABS(Amount)) TxV, SUM(Amount) TxNV FROM FACT_Transactions_Before2020 Z1 (NOLOCK)
WHERE UserKey IN
		(	SELECT UserKey FROM (
				SELECT
					 UserKey
					,COUNT(DISTINCT FeatureType) CountFeatureType
				FROM FACT_Transactions_Before2020 (NOLOCK)
				WHERE CreatedAt>=@DateStartTransactions AND CreatedAt < @DateEndTransactions
				GROUP BY UserKey
				HAVING COUNT(DISTINCT FeatureType)=@FeatureTypeCount )M
		)
	AND CreatedAt>=@DateStartTransactions AND CreatedAt < @DateEndTransactions AND FeatureType IN (SELECT CAST(VALUE AS decimal(10,2)) FROM string_split(@varFeatureType,'/'))
	GROUP BY UserKey,FeatureType

), Sadelestir AS
(
SELECT UserKey,COUNT(UserKey) Counter FROM X1 
GROUP BY UserKey
HAVING COUNT(UserKey) = @FeatureTypeCount
) SELECT FORMAT(@DateStartTransactions,'yyyyMM') MonthKey,COUNT(DISTINCT s.UserKey) UserCount,SUM(TxC) SumTxCount, SUM(TxNV) SumTxVolume, SUM(TxV) SumAbsTxVolume FROM Sadelestir s
join X1 on X1.UserKey = s.UserKey

SELECT MonthKey,UserCount,SumTxCount,SumTxVolume,SumAbsTxVolume FROM BI_Workspace.[DB\skacar].MainFeatureCombinations where MonthKey = FORMAT(@DateStartTransactions,'yyyyMM') AND FeatureTypeComb = @varFeatureType


-------------------------
STATİK SORGU

/*İKİ DEĞİŞKENLİ*/
SELECT COUNT(DISTINCT T.UserKey) UserCount, COUNT(Id) SumTxCount, SUM(Amount) SumTxVolume, SUM(ABS(Amount)) SumAbsTxVolume FROM
		(
		SELECT DISTINCT l.UserKey
		FROM FACT_Transactions_Before2020 (nolock) l
		INNER JOIN (SELECT DISTINCT UserKey FROM FACT_Transactions_Before2020 (nolock) WHERE FeatureType = 3)		   L1 on L1.UserKey = l.UserKey
		LEFT JOIN  (SELECT DISTINCT UserKey	FROM FACT_Transactions_Before2020 (nolock) WHERE FeatureType NOT IN (3,8)) L2 on L2.UserKey = l.UserKey
		WHERE CreatedAt>='2016-08-01' AND CreatedAt < '2016-09-01' AND l.FeatureType =8 AND L2.UserKey IS NULL
		) T
		JOIN FACT_Transactions_Before2020 (nolock) lb on T.UserKey = lb.UserKey AND lb.CreatedAt>='2016-08-01' AND lb.CreatedAt < '2016-09-01'
-- MainFeatureCombinations KURGUSU
		SELECT UserCount,SumTxCount,SumTxVolume,SumAbsTxVolume FROM BI_Workspace.[DB\skacar].MainFeatureCombinations where MonthKey = 201608 AND FeatureTypeComb = '333.00/8.00'

/*ÜÇ DEĞİŞKENLİ*/
SELECT COUNT(DISTINCT T.UserKey) UserCount, COUNT(Id) SumTxCount, SUM(Amount) SumTxVolume, SUM(ABS(Amount)) SumAbsTxVolume FROM
		(
		SELECT K.UserKey FROM (
		SELECT DISTINCT l.UserKey
		FROM FACT_Transactions_Before2020 (nolock) l
		INNER JOIN (SELECT DISTINCT UserKey FROM FACT_Transactions_Before2020 (nolock) WHERE FeatureType = 0)			 L1 on L1.UserKey = l.UserKey
		WHERE CreatedAt>='2016-08-01' AND CreatedAt < '2016-09-01' AND l.FeatureType = 8
		) K
		INNER JOIN (SELECT DISTINCT UserKey FROM FACT_Transactions_Before2020 (nolock) WHERE FeatureType = 3 AND CreatedAt>='2016-08-01' AND CreatedAt < '2016-09-01')			  L2 on L2.UserKey = K.UserKey
		LEFT JOIN  (SELECT DISTINCT UserKey	FROM FACT_Transactions_Before2020 (nolock) WHERE FeatureType NOT IN (0,3,8) AND CreatedAt>='2016-08-01' AND CreatedAt < '2016-09-01') L3 on L3.UserKey = K.UserKey
		WHERE L3.UserKey IS NULL) T
		JOIN FACT_Transactions_Before2020 (nolock) lb on T.UserKey = lb.UserKey AND lb.CreatedAt>='2016-08-01' AND lb.CreatedAt < '2016-09-01'
-- MainFeatureCombinations KURGUSU
		SELECT UserCount,SumTxCount,SumTxVolume,SumAbsTxVolume FROM BI_Workspace.[DB\skacar].MainFeatureCombinations where MonthKey = 201608 AND FeatureTypeComb = '0.00/3.00/8.00'

****TEKLİ****
select
count(distinct l.UserKey) as 'Sadece 9&21'
from FACT_Transactions (nolock) l
left join (select distinct UserKey from FACT_Transactions (nolock) where CreatedAt>='2023-03-01' AND CreatedAt < CAST(getdate() as date) and FeatureType != 18) l2 on l2.UserKey=l.UserKey
where l.CreatedAt>='2023-03-01' AND l.CreatedAt < CAST(getdate() as date) and l.FeatureType = 18 and l2.UserKey IS NULL

****ÇOKLU***
select
    count(distinct l.UserKey) as 'Sadece 9&21'
from FACT_Transactions (nolock) l
INNER JOIN (select distinct UserKey from FACT_Transactions (nolock) where CreatedAt>='2023-03-01' AND CreatedAt < CAST(getdate() as date) and FeatureType = 9) l3 on l3.UserKey = l.UserKey
left join (select distinct UserKey from FACT_Transactions (nolock) where CreatedAt>='2023-03-01' AND CreatedAt < CAST(getdate() as date) and FeatureType NOT IN (9,21)) l2 on l2.UserKey=l.UserKey
where l.CreatedAt>='2023-03-01' AND l.CreatedAt < CAST(getdate() as date) and l.FeatureType = 21 and l2.UserKey IS NULL



select COUNT(z1.UserKey) UU,SUM(tutar) TxV,SUM(IslemAdet) TxCount, SUM(ABS(tutar)) AbsTxV
from
	(
		select
		COUNT(DISTINCT FeatureType) adet
		,SUM(Amount) tutar
		,COUNT(Id) IslemAdet
		,UserKey
		FROM
			(
						SELECT
						L1.Id,
						l1.CreatedAt,
						UserKey,
						l1.Amount,
						
						CASE  WHEN FeatureType = 7 AND RemittanceType = 0 THEN 7.2--'7(0)'
							  WHEN FeatureType = 7 AND RemittanceType = 1 THEN 7.1--'7(1)'
							  WHEN FeatureType = 1 AND BankTransferType  = 0 THEN 1.0--'1(0)'
							  WHEN FeatureType = 1 AND BankTransferType  = 1 THEN 1.1--'1(1)'
							  WHEN FeatureType = 2 AND DBCardTxType  = 0 THEN 2.0--'2(0)'
							  WHEN FeatureType = 2 AND DBCardTxType  = 2 THEN 2.2--'2(2)'
							  WHEN FeatureType = 2 AND DBCardTxType  = 3 THEN 2.3--'2(3)'
							  WHEN FeatureType = 2 AND DBCardTxType  = 4 THEN 2.4--'2(4)'
							  WHEN FeatureType = 2 AND DBCardTxType  = 5 THEN 2.5--'2(5)'
							  WHEN FeatureType = 2 AND DBCardTxType  = 6 THEN 2.6--'2(6)'
							  WHEN FeatureType = 2 AND DBCardTxType  = 7 THEN 2.7--'2(7)'
							  WHEN FeatureType = 2 AND DBCardTxType  = 8 THEN 2.8--'2(8)'
							  WHEN FeatureType = 2 AND DBCardTxType  = 9 THEN 2.9--'2(9)'
							  WHEN FeatureType = 2 AND DBCardTxType  = 1 AND Is_Offline = 0 THEN 2.12
							  WHEN FeatureType = 2 AND DBCardTxType  = 1 AND Is_Offline = 1 THEN 2.11
						ELSE CAST(FeatureType AS decimal(15,1))								   END FeatureType		
						FROM FACT_Transactions (Nolock) L1 
						LEFT JOIN FACT_BankTransferRequests B1 ON B1.Id  = L1.StartingRequestId
						where CAST(l1.CreatedAt as date) = '2023-03-22'
			) KL			
		group by UserKey 
		having COUNT(DISTINCT FeatureType)=1
	) z1 
join (select distinct UserKey FROM FACT_Transactions (Nolock) where CAST(CreatedAt as date) = '2023-03-22' and FeatureType = 1) Z2 ON z1.UserKey = Z2.UserKey
---------------------------

select COUNT(DISTINCT z1.UserKey) UU,SUM(tutar) TxV,SUM(IslemAdet) TxCount
from
	(
		select
		 COUNT(DISTINCT FeatureType) adet
		,SUM(Amount) tutar
		,COUNT(Id) IslemAdet
		,UserKey
		,STRING_AGG(cast(FeatureType as nvarchar(max)),'/') WITHIN GROUP (ORDER BY FeatureType) FeatureTypeComb
		FROM
			(
						SELECT
						L1.Id,
						l1.CreatedAt,
						UserKey,
						l1.Amount,
						CASE  WHEN FeatureType = 7 AND RemittanceType = 0 THEN 7.2--'7(0)'
							  WHEN FeatureType = 7 AND RemittanceType = 1 THEN 7.1--'7(1)'
							  WHEN FeatureType = 1 AND BankTransferType  = 0 THEN 1.0--'1(0)'
							  WHEN FeatureType = 1 AND BankTransferType  = 1 THEN 1.1--'1(1)'
							  WHEN FeatureType = 2 AND DBCardTxType  = 0 THEN 2.0--'2(0)'
							  WHEN FeatureType = 2 AND DBCardTxType  = 2 THEN 2.2--'2(2)'
							  WHEN FeatureType = 2 AND DBCardTxType  = 3 THEN 2.3--'2(3)'
							  WHEN FeatureType = 2 AND DBCardTxType  = 4 THEN 2.4--'2(4)'
							  WHEN FeatureType = 2 AND DBCardTxType  = 5 THEN 2.5--'2(5)'
							  WHEN FeatureType = 2 AND DBCardTxType  = 6 THEN 2.6--'2(6)'
							  WHEN FeatureType = 2 AND DBCardTxType  = 7 THEN 2.7--'2(7)'
							  WHEN FeatureType = 2 AND DBCardTxType  = 8 THEN 2.8--'2(8)'
							  WHEN FeatureType = 2 AND DBCardTxType  = 9 THEN 2.9--'2(9)'
							  WHEN FeatureType = 2 AND DBCardTxType  = 1 AND Is_Offline = 0 THEN 2.12
							  WHEN FeatureType = 2 AND DBCardTxType  = 1 AND Is_Offline = 1 THEN 2.11
						ELSE CAST(FeatureType AS decimal(15,1))								   END FeatureType
						
						
						FROM FACT_Transactions (Nolock) L1 
						LEFT JOIN FACT_BankTransferRequests B1 ON B1.Id  = L1.StartingRequestId
						where l1.CreatedAt >= dateadd(day, 1, eomonth(getdate(), -1)) AND l1.CreatedAt < CAST(GETDATE() AS date)
			) KL				
		group by UserKey 
		having COUNT(DISTINCT FeatureType)=3
		and STRING_AGG(cast(FeatureType as nvarchar(max)),'/') WITHIN GROUP (ORDER BY FeatureType)='1.00/2.12/15.00'
	) z1
	------------
*/