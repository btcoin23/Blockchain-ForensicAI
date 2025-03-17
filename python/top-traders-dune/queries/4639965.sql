-- How long a wallet holds a token before selling - Shortest hold, longest hold and average hold.

WITH first_buys AS (
    SELECT 
        trader_id,
        token_bought_mint_address AS token,
        token_bought_symbol AS symbol,
        MIN(block_time) AS first_buy_time,
        SUM(token_bought_amount) AS total_bought
    FROM dex_solana.trades
    WHERE trader_id = '{{trader_id}}'
    GROUP BY trader_id, token_bought_mint_address, token_bought_symbol
),
last_sells AS (
    SELECT 
        trader_id,
        token_sold_mint_address AS token,
        MAX(block_time) AS last_sell_time,
        SUM(token_sold_amount) AS total_sold
    FROM dex_solana.trades
    WHERE trader_id = '{{trader_id}}'
    GROUP BY trader_id, token_sold_mint_address
),
holding_durations AS (
    SELECT
        b.trader_id,
        b.token,
        b.symbol,
        CASE 
            WHEN COALESCE(s.total_sold, 0) = b.total_bought THEN DATE_DIFF('second', b.first_buy_time, s.last_sell_time)
            ELSE DATE_DIFF('second', b.first_buy_time, NOW())
        END AS hold_duration
    FROM first_buys b
    LEFT JOIN last_sells s ON b.trader_id = s.trader_id AND b.token = s.token
    WHERE DATE_DIFF('second', b.first_buy_time, COALESCE(s.last_sell_time, NOW())) > 0
),
ranked_holds AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY trader_id ORDER BY hold_duration ASC) AS shortest_rank,
        ROW_NUMBER() OVER (PARTITION BY trader_id ORDER BY hold_duration DESC) AS longest_rank
    FROM holding_durations
)

SELECT 
    trader_id,
    MIN(hold_duration) AS shortest_hold_time,
    MAX(hold_duration) AS longest_hold_time,
    AVG(hold_duration) AS average_hold_time,
    MIN(CASE WHEN shortest_rank = 1 THEN token END) AS shortest_hold_token,
    MIN(CASE WHEN shortest_rank = 1 THEN symbol END) AS shortest_hold_symbol,
    MIN(CASE WHEN longest_rank = 1 THEN token END) AS longest_hold_token,
    MIN(CASE WHEN longest_rank = 1 THEN symbol END) AS longest_hold_symbol
FROM ranked_holds
GROUP BY trader_id
