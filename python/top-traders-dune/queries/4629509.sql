-- Most profitable wallets in the last 1/7/30 days

WITH periods AS (
  SELECT 1 AS period 
  UNION ALL
  SELECT 7 AS period 
  UNION ALL
  SELECT 30 AS period
),
token_trades AS (
  SELECT
    trader_id,
    period,
    block_time,
    token_sold_mint_address,
    token_bought_mint_address,
    amount_usd,
    CASE 
      WHEN token_sold_mint_address = 'So11111111111111111111111111111111111111112' THEN amount_usd 
      ELSE 0 
    END as buy_amount_usd,
    CASE 
      WHEN token_bought_mint_address = 'So11111111111111111111111111111111111111112' THEN amount_usd 
      ELSE 0 
    END as sell_amount_usd
  FROM dex_solana.trades
  CROSS JOIN periods
  WHERE
    (token_bought_mint_address = 'So11111111111111111111111111111111111111112' 
    OR token_sold_mint_address = 'So11111111111111111111111111111111111111112')
    AND block_time >= DATE_TRUNC('day', NOW()) - period * INTERVAL '1' DAY
),
token_pnl AS (
  SELECT
    trader_id,
    period,
    COALESCE(token_bought_mint_address, token_sold_mint_address) as token,
    SUM(buy_amount_usd) as token_buy_usd,
    SUM(sell_amount_usd) as token_sell_usd,
    SUM(sell_amount_usd - buy_amount_usd) as token_pnl,
    CASE 
      WHEN SUM(sell_amount_usd - buy_amount_usd) > 0 THEN 1
      ELSE 0
    END as is_win
  FROM token_trades
  GROUP BY trader_id, period, COALESCE(token_bought_mint_address, token_sold_mint_address)
),
trader_metrics AS (
  SELECT
    trader_id,
    period,
    SUM(token_buy_usd) AS total_buy_usd,
    SUM(token_sell_usd) AS total_sell_usd,
    SUM(token_pnl) AS total_profit,
    COUNT(*) AS total_token_trades,
    SUM(is_win) AS total_wins,
    COUNT(*) - SUM(is_win) AS total_losses,
    CAST(SUM(is_win) AS DOUBLE) / NULLIF(COUNT(*), 0) AS win_rate,
    CASE
      WHEN SUM(token_pnl) = 0 THEN 0
      ELSE CAST(SUM(token_pnl) AS DOUBLE) / NULLIF(COUNT(*), 0)
    END AS pnl_ratio
  FROM token_pnl
  GROUP BY trader_id, period
)
SELECT
  trader_id,
  period,
  total_buy_usd,
  total_sell_usd,
  total_profit,
  total_token_trades,
  total_wins,
  total_losses,
  win_rate,
  pnl_ratio
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY period ORDER BY pnl_ratio DESC) AS rn
  FROM trader_metrics
)
WHERE rn <= 10
ORDER BY period, pnl_ratio DESC;
