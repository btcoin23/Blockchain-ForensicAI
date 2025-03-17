-- First Buy wallets of a given token within 10mins after launch.

WITH token_launches AS (
    SELECT
        token_mint_address,
        created_at,
        symbol
    FROM tokens_solana.fungible
    WHERE token_mint_address = '{{token_mint_address}}'
),
ranked_buyers AS (
    SELECT
        t.token_mint_address,
        t.symbol,
        t.created_at AS token_launch_time,
        trades.trader_id,
        trades.token_bought_amount,
        trades.amount_usd,
        trades.block_time,
        ROW_NUMBER() OVER (
            PARTITION BY t.token_mint_address
            ORDER BY trades.block_time ASC
        ) AS buyer_rank
    FROM token_launches t
    JOIN dex_solana.trades trades
        ON t.token_mint_address = trades.token_bought_mint_address
        AND trades.block_time >= t.created_at
        AND trades.block_time <= t.created_at + INTERVAL '10' MINUTE
)
SELECT *
FROM ranked_buyers
WHERE buyer_rank <= 10
ORDER BY token_launch_time DESC, buyer_rank ASC