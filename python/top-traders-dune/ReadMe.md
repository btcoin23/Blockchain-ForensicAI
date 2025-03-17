# Solana Top Traders API Documentation

## Base URL
`https://api.frankai.org`

## Endpoints

### 1. First Buy Wallets
Identifies and tracks early investors who were among the first to buy a specific token. This helps analyze smart money movements and potential token success indicators based on early buyer profiles.

**Use Cases:**
- Track early adopters of successful tokens
- Identify potential whale accumulation patterns
- Monitor smart money entry points

**Endpoint:** `/first-buy-wallets`  
**Method:** GET  
**Query Parameters:**
- `token_mint_address` (required): The token's mint address

**Response:**
```json
{
  "wallets": [
    {
      "token_mint_address": "string",
      "symbol": "string",
      "token_launch_time": "datetime",
      "trader_id": "string",
      "amount_usd": "float",
      "block_time": "datetime",
      "buyer_rank": "integer",
      "buy_volume_1d": "float",
      "sell_volume_1d": "float",
      "total_pnl_1d": "float",
      "total_trades_1d": "float",
      "buy_volume_7d": "float",
      "sell_volume_7d": "float",
      "total_pnl_7d": "float",
      "total_trades_7d": "float",
      "buy_volume_30d": "float",
      "sell_volume_30d": "float",
      "total_pnl_30d": "float",
      "total_trades_30d": "float"
    }
  ]
}
```

### 2. Token Profitable Wallets
Tracks wallets that have made profitable trades for a specific token. Provides detailed metrics about their trading performance including win rates and profit ratios.

**Use Cases:**
- Study successful trading strategies for specific tokens
- Identify experienced traders in particular markets
- Analyze trading patterns of profitable traders

**Endpoint:** `/token-profitable-wallets`  
**Method:** GET  
**Query Parameters:**
- `token_mint_address` (required): The token's mint address

**Response:**
```json
{
  "wallets": [
    {
      "trader_id": "string",
      "total_profit": "float",
      "total_buy_usd": "float",
      "total_sell_usd": "float",
      "total_trades": "integer",
      "total_wins": "integer",
      "total_losses": "integer",
      "win_rate": "float",
      "avg_profit_per_trade": "float",
      "total_volume_bought": "float",
      "total_volume_sold": "float",
      "total_volume_traded": "float",
      "pnl_ratio": "float",
      "last_trade_time": "datetime"
    }
  ]
}
```

### 3. Profitable Wallets
Lists the most successful traders across the entire Solana ecosystem, ranked by their total profit. Provides comprehensive trading statistics over different time periods.

**Use Cases:**
- Find top-performing traders to follow
- Study market-wide trading success patterns
- Compare trading performance across different timeframes

**Endpoint:** `/profitable-wallets`  
**Method:** GET  
**Query Parameters:**
- `period` (optional, default: 30): Time period in days (1, 7, or 30)

**Response:**
```json
{
  "wallets": [
    {
      "trader_id": "string",
      "total_profit": "float",
      "total_buy_usd": "float",
      "total_sell_usd": "float",
      "total_token_trades": "integer",
      "total_wins": "integer",
      "total_losses": "integer",
      "win_rate": "float",
      "pnl_ratio": "float"
    }
  ]
}
```

### 4. Profitable Wallets by Transaction Count
Filters profitable wallets based on their transaction volume, helping identify successful traders at different activity levels.

**Use Cases:**
- Find successful traders with specific trading frequencies
- Study correlation between trade frequency and profitability
- Identify profitable trading styles (high vs low frequency)

**Endpoint:** `/profitable-wallets-tx`  
**Method:** GET  
**Query Parameters:**
- `period` (optional, default: 30): Time period in days (1, 7, or 30)
- `tx_min` (optional, default: 0): Minimum transaction count
- `tx_max` (optional, default: 100): Maximum transaction count

**Response:**
```json
{
  "wallets": [
    {
      "trader_id": "string",
      "total_transaction_count": "integer",
      "total_profit": "float",
      "total_buy_usd": "float",
      "total_sell_usd": "float",
      "total_trades": "integer",
      "total_wins": "integer",
      "total_losses": "integer",
      "win_rate": "float",
      "avg_profit_per_trade": "float",
      "total_volume_bought": "float",
      "total_volume_sold": "float",
      "total_volume_traded": "float",
      "pnl_ratio": "float",
      "last_trade_time": "datetime"
    }
  ]
}
```

### 5. High Volume Wallets
Tracks wallets with significant trading volume, indicating major market participants and potential market movers.

**Use Cases:**
- Monitor whale activity
- Track market manipulation risks
- Identify significant market participants

**Endpoint:** `/high-volume-wallets`  
**Method:** GET  
**Query Parameters:**
- `period` (optional, default: 30): Time period in days (1, 7, or 30)

**Response:**
```json
{
  "wallets": [
    {
      "trader_id": "string",
      "total_volume_usd": "float",
      "total_trades": "integer",
      "avg_trade_size_usd": "float",
      "last_trade_time": "datetime"
    }
  ]
}
```

### 6. High Transaction Wallets
Identifies the most active traders based on transaction count, regardless of volume.

**Use Cases:**
- Study active trading strategies
- Monitor market maker activity
- Track automated trading systems

**Endpoint:** `/high-transaction-wallets`  
**Method:** GET  
**Query Parameters:**
- `period` (optional, default: 30): Time period in days (1, 7, or 30)

**Response:**
```json
{
  "wallets": [
    {
      "trader_id": "string",
      "total_transactions": "integer",
      "avg_daily_transactions": "float",
      "total_volume_usd": "float",
      "avg_trade_size_usd": "float",
      "last_trade_time": "datetime"
    }
  ]
}
```

### 7. Wallet Holding Times
Analyzes token holding patterns for specific wallets, showing their trading timeframes and preferences.

**Use Cases:**
- Understand trading timeframes of successful traders
- Identify swing vs day trading patterns
- Study holding time patterns for different tokens

**Endpoint:** `/wallet-holding-times`  
**Method:** GET  
**Query Parameters:**
- `trader_id` (required): The wallet address

**Response:**
```json
{
  "trader_id": "string",
  "shortest_hold_time": "float",
  "longest_hold_time": "float",
  "average_hold_time": "float",
  "shortest_hold_token": "string",
  "shortest_hold_symbol": "string",
  "longest_hold_token": "string",
  "longest_hold_symbol": "string"
}
```

### 8. Successful Token Deployers
Tracks token creators who have launched successful tokens, based on market cap and price performance.

**Use Cases:**
- Find experienced token developers
- Identify potential new token opportunities
- Study characteristics of successful token launches

**Endpoint:** `/successful-token-deployers`  
**Method:** GET  
**Query Parameters:**
- `period` (optional, default: 30): Time period in days (2, 7, or 30)

**Response:**
```json
{
  "tokens": [
    {
      "token_mint_address": "string",
      "symbol": "string",
      "name": "string",
      "decimals": "integer",
      "created_at": "datetime",
      "init_tx": "string",
      "total_supply": "float",
      "current_price": "float",
      "max_price_in_period": "float",
      "current_market_cap": "float",
      "max_market_cap": "float",
      "token_creator": "string",
      "token_launch_time": "datetime"
    }
  ]
}
```

### 9. KOL Leaderboard
Ranks Key Opinion Leaders (KOLs) based on their trading performance and social media presence.

**Use Cases:**
- Find influential traders to follow
- Verify trading performance of social media personalities
- Track market influencers' performance

**Endpoint:** `/kol-leaderboard`  
**Method:** GET  
**Query Parameters:**
- `period` (optional, default: 1): Time period in days (1, 7, or 30)
- `wallet_name` (optional): Filter by wallet name
- `wallet_address` (optional): Filter by wallet address

**Response:**
```json
{
  "leaderboard": [
    {
      "wallet_name": "string",
      "wallet_address": "string",
      "pnl_usd": "float",
      "pnl_sol": "float",
      "telegram": "string",
      "twitter": "string"
    }
  ]
}
```

### 10. GMGN KOL
Specific leaderboard for GMGN platform's Key Opinion Leaders, focusing on their trading performance and social presence.

**Use Cases:**
- Track GMGN-specific trader performance
- Compare GMGN traders with general market
- Find successful GMGN-focused traders

**Endpoint:** `/gmgn-kol`  
**Method:** GET  
**Query Parameters:**
- `period` (optional, default: 1): Time period in days (1, 7, or 30)
- `wallet_name` (optional): Filter by wallet name
- `wallet_address` (optional): Filter by wallet address

**Response:**
```json
{
  "leaderboard": [
    {
      "wallet_name": "string",
      "wallet_address": "string",
      "pnl_percentage": "float",
      "pnl_usd": "float",
      "twitter": "string"
    }
  ]
}
```
