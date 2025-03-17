-- Most profitable wallets for a given token.

WITH buy_transactions AS (
  SELECT
    trader_id,
    token_bought_mint_address,
    token_bought_symbol,
    token_bought_amount,
    amount_usd AS buy_amount_usd,
    block_time AS buy_block_time
  FROM dex_solana.trades
  WHERE
    token_bought_mint_address = '{{token_mint_address}}'
    AND amount_usd >= {{bought_limit}}
), sell_transactions AS (
  SELECT
    trader_id,
    token_sold_mint_address,
    token_sold_symbol,
    token_sold_amount,
    amount_usd AS sell_amount_usd,
    block_time AS sell_block_time
  FROM dex_solana.trades
  WHERE
    token_sold_mint_address = '{{token_mint_address}}'
    AND NOT token_sold_amount IS NULL
), matched_transactions AS (
  SELECT
    b.trader_id,
    b.token_bought_symbol,
    b.token_bought_amount,
    b.buy_amount_usd,
    b.buy_block_time,
    s.token_sold_amount,
    s.sell_amount_usd,
    s.sell_block_time,
    CASE WHEN s.sell_amount_usd > b.buy_amount_usd THEN 1 ELSE 0 END AS win
  FROM buy_transactions AS b
  JOIN sell_transactions AS s
    ON b.trader_id = s.trader_id
    AND b.token_bought_mint_address = s.token_sold_mint_address
    AND b.buy_block_time < s.sell_block_time
    AND b.token_bought_amount = s.token_sold_amount
)
SELECT
  trader_id,
  SUM(sell_amount_usd - buy_amount_usd) AS total_profit,
  SUM(buy_amount_usd) AS total_buy_usd,
  SUM(sell_amount_usd) AS total_sell_usd,
  COUNT(win) AS total_trades,
  SUM(win) AS total_wins,
  COUNT(*) - SUM(win) AS total_losses,
  SUM(win) * 1.0 / COUNT(win) AS win_rate,
  SUM(sell_amount_usd - buy_amount_usd) * 1.0 / COUNT(win) AS avg_profit_per_trade,
  SUM(token_bought_amount) AS total_volume_bought,
  SUM(token_sold_amount) AS total_volume_sold,
  SUM(token_bought_amount + token_sold_amount) AS total_volume_traded,
  CASE
    WHEN SUM(token_bought_amount) = 0
    THEN 0
    ELSE SUM(sell_amount_usd - buy_amount_usd) / SUM(buy_amount_usd)
  END AS pnl_ratio,
  MAX(sell_block_time) AS last_trade_time
FROM matched_transactions
GROUP BY
  trader_id
ORDER BY
  pnl_ratio DESC
LIMIT
  10