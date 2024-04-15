


-- Find trades which are mapped based only on currency pair, exchange name, exchange type
WITH mapping_trades AS (
SELECT 
	t.*
	,i.updated_at 
	,i.value
	,ABS(i.updated_at - t.executed_at) AS time_diff
FROM 
	trades AS t 
LEFT JOIN 
	indexes AS i 
ON 
	t.currency_1 = i.currency_1 
	AND t.currency_2 = i.currency_2 -- Currency pair
	AND t.exchange = i.exchange -- Exchange name 
	AND t.exchange_type = i.exchange_type -- Exchange type
)

-- Take into consideration the closest time interval, ranking by the time interval 
,ranked_trades AS (
SELECT 
	*
	,ROW_NUMBER() OVER(PARTITION BY transaction_id ORDER BY time_diff) AS rn 
FROM 
	mapping_trades
)


-- Select only the closest one
SELECT 
	* 
FROM 
	ranked_trades
WHERE 
	rn = 1