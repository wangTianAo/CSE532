select YEAR(cse532.dea_ny.TRANSACTION_DATE) ||
CASE 
    WHEN (CAST( MONTH(cse532.dea_ny.TRANSACTION_DATE) AS INT)  < 10 ) 
         THEN '0'  || CAST( MONTH(cse532.dea_ny.TRANSACTION_DATE) AS INT)
    ELSE CAST ( MONTH(cse532.dea_ny.TRANSACTION_DATE) AS CHAR(2) )
END as date,
sum(cse532.dea_ny.DOSAGE_UNIT) as MonthlyCounts,

avg(sum(cse532.dea_ny.DOSAGE_UNIT)) over (order by YEAR(cse532.dea_ny.TRANSACTION_DATE) ||
CASE 
    WHEN (CAST( MONTH(cse532.dea_ny.TRANSACTION_DATE) AS INT)  < 10 ) 
         THEN '0'  || CAST( MONTH(cse532.dea_ny.TRANSACTION_DATE) AS INT)
    ELSE CAST ( MONTH(cse532.dea_ny.TRANSACTION_DATE) AS CHAR(2) )
END rows between 1 preceding and 1 following) as smoothCounts
from cse532.dea_ny 
group by grouping sets(
	(year(cse532.dea_ny.TRANSACTION_DATE),month(cse532.dea_ny.TRANSACTION_DATE))
)