-- First wallet purchases of the token within 10 minutes of launch with period-by-period statistics and holding time.

WITH token_launches AS (
    SELECT
        token_mint_address,
        created_at,
        symbol
    FROM tokens_solana.fungible
    WHERE token_mint_address = '{{token_mint_address}}'
),
initial_trades AS (
    SELECT
        t.token_mint_address,
        t.symbol,
        t.created_at AS token_launch_time,
        trades.trader_id,
        trades.token_bought_amount,
        trades.amount_usd,
        trades.block_time
    FROM token_launches t
    JOIN dex_solana.trades trades
        ON t.token_mint_address = trades.token_bought_mint_address
        AND trades.block_time >= t.created_at
        AND trades.block_time <= t.created_at + INTERVAL '10' MINUTE
),
ranked_buyers AS (
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY token_mint_address
                ORDER BY block_time ASC
            ) AS buyer_rank
        FROM initial_trades
    ) ranked
    WHERE buyer_rank <= 50
),
relevant_trades AS (
    SELECT 
        t.*
    FROM dex_solana.trades t
    JOIN ranked_buyers rb 
        ON rb.trader_id = t.trader_id
        AND t.block_time >= rb.token_launch_time
        AND t.block_time <= rb.token_launch_time + INTERVAL '30' DAY
),
buyer_stats AS (
    SELECT 
        rb.trader_id,
        rb.token_mint_address,
        rb.token_launch_time,
        SUM(CASE WHEN rt.block_time <= rb.token_launch_time + INTERVAL '1' DAY AND rt.token_sold_mint_address = 'So11111111111111111111111111111111111111112' THEN rt.amount_usd ELSE 0 END) as buy_volume_1d,
        SUM(CASE WHEN rt.block_time <= rb.token_launch_time + INTERVAL '1' DAY AND rt.token_bought_mint_address = 'So11111111111111111111111111111111111111112' THEN rt.amount_usd ELSE 0 END) as sell_volume_1d,
        SUM(CASE WHEN rt.block_time <= rb.token_launch_time + INTERVAL '1' DAY AND rt.token_sold_mint_address = 'So11111111111111111111111111111111111111112' THEN -rt.amount_usd ELSE rt.amount_usd END) as total_pnl_1d,
        COUNT(CASE WHEN rt.block_time <= rb.token_launch_time + INTERVAL '1' DAY THEN 1 END) as total_trades_1d,
        SUM(CASE WHEN rt.block_time <= rb.token_launch_time + INTERVAL '7' DAY AND rt.token_sold_mint_address = 'So11111111111111111111111111111111111111112' THEN rt.amount_usd ELSE 0 END) as buy_volume_7d,
        SUM(CASE WHEN rt.block_time <= rb.token_launch_time + INTERVAL '7' DAY AND rt.token_bought_mint_address = 'So11111111111111111111111111111111111111112' THEN rt.amount_usd ELSE 0 END) as sell_volume_7d,
        SUM(CASE WHEN rt.block_time <= rb.token_launch_time + INTERVAL '7' DAY AND rt.token_sold_mint_address = 'So11111111111111111111111111111111111111112' THEN -rt.amount_usd ELSE rt.amount_usd END) as total_pnl_7d,
        COUNT(CASE WHEN rt.block_time <= rb.token_launch_time + INTERVAL '7' DAY THEN 1 END) as total_trades_7d,
        SUM(CASE WHEN rt.block_time <= rb.token_launch_time + INTERVAL '30' DAY AND rt.token_sold_mint_address = 'So11111111111111111111111111111111111111112' THEN rt.amount_usd ELSE 0 END) as buy_volume_30d,
        SUM(CASE WHEN rt.block_time <= rb.token_launch_time + INTERVAL '30' DAY AND rt.token_bought_mint_address = 'So11111111111111111111111111111111111111112' THEN rt.amount_usd ELSE 0 END) as sell_volume_30d,
        SUM(CASE WHEN rt.block_time <= rb.token_launch_time + INTERVAL '30' DAY AND rt.token_sold_mint_address = 'So11111111111111111111111111111111111111112' THEN -rt.amount_usd ELSE rt.amount_usd END) as total_pnl_30d,
        COUNT(CASE WHEN rt.block_time <= rb.token_launch_time + INTERVAL '30' DAY THEN 1 END) as total_trades_30d
    FROM ranked_buyers rb
    LEFT JOIN relevant_trades rt
        ON rb.trader_id = rt.trader_id
    GROUP BY rb.trader_id, rb.token_mint_address, rb.token_launch_time
),
first_buys AS (
    SELECT 
        t.trader_id,
        t.token_bought_mint_address AS token,
        t.token_bought_symbol AS symbol,
        MIN(t.block_time) AS first_buy_time,
        SUM(t.token_bought_amount) AS total_bought
    FROM dex_solana.trades t
    RIGHT JOIN buyer_stats bs
        ON t.trader_id = bs.trader_id
    GROUP BY t.trader_id, t.token_bought_mint_address, t.token_bought_symbol
),
last_sells AS (
    SELECT 
        t.trader_id,
        t.token_sold_mint_address AS token,
        MAX(t.block_time) AS last_sell_time,
        SUM(t.token_sold_amount) AS total_sold
    FROM dex_solana.trades t
    RIGHT JOIN buyer_stats bs
        ON t.trader_id = bs.trader_id
    GROUP BY t.trader_id, t.token_sold_mint_address
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
),
holding_data AS (
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
)

SELECT 
    rb.*,
    bs.buy_volume_1d,
    bs.sell_volume_1d,
    bs.total_pnl_1d,
    bs.total_trades_1d,
    bs.buy_volume_7d,
    bs.sell_volume_7d,
    bs.total_pnl_7d,
    bs.total_trades_7d,
    bs.buy_volume_30d,
    bs.sell_volume_30d,
    bs.total_pnl_30d,
    bs.total_trades_30d,
    hd.*
FROM ranked_buyers rb
JOIN buyer_stats bs 
    ON rb.trader_id = bs.trader_id 
    AND rb.token_mint_address = bs.token_mint_address
JOIN holding_data hd
    ON rb.trader_id = hd.trader_id