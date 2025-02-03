from quart import Quart, request, jsonify
from prisma import Prisma
import asyncio
import logging
from quart_cors import cors
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from dune_client.types import QueryParameter
import os
from dotenv import load_dotenv
import yaml
from datetime import datetime

app = Quart(__name__)
app = cors(app)
prisma = Prisma()

dotenv_path = os.path.join(os.path.dirname(__file__), '.', '.env')
load_dotenv(dotenv_path)
dune = DuneClient.from_env()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.before_serving
async def startup():
    await prisma.connect()

@app.after_serving
async def shutdown():
    await prisma.disconnect()

@app.route('/api/first-buy-wallets', methods=['GET'])
async def get_first_buy_wallets():
    token_mint_address = request.args.get('token_mint_address')
    
    if not token_mint_address:
        return jsonify({'error': 'token_mint_address parameter is required'}), 400

    # Check for recent results
    existing_results = await prisma.earlytokenbuyers.find_many(
        where={
            'token_mint_address': token_mint_address
        },
        order={
            'buyer_rank': 'asc'
        }
    )

    if existing_results:
        # If we have 10 or more buyers, return the data
        if len(existing_results) >= 10:
            return jsonify({'wallets': [
                {
                    'token_mint_address': w.token_mint_address,
                    'symbol': w.symbol,
                    'token_launch_time': w.token_launch_time,
                    'trader_id': w.trader_id,
                    'token_bought_amount': w.token_bought_amount,
                    'amount_usd': w.amount_usd,
                    'block_time': w.block_time,
                    'buyer_rank': w.buyer_rank
                } for w in existing_results
            ]})
        
        if len(existing_results) == 10 or (
        existing_results and 
            (existing_results[-1].last_updated - existing_results[-1].token_launch_time.timestamp()) > 600  # 10 minutes in seconds
        ):
            return jsonify({'wallets': [
                {
                    'token_mint_address': w.token_mint_address,
                    'symbol': w.symbol,
                    'token_launch_time': w.token_launch_time,
                    'trader_id': w.trader_id,
                    'token_bought_amount': w.token_bought_amount,
                    'amount_usd': w.amount_usd,
                    'block_time': w.block_time,
                    'buyer_rank': w.buyer_rank
                } for w in existing_results
            ]})


    try:
        query = QueryBase(
            name="First Buy wallets of a token within 10mins after launch.",
            query_id=4628657,
            params=[
                QueryParameter.text_type(name="token_mint_address", value=token_mint_address)
            ],
        )

        query_results = dune.run_query(query = query)

        rows = query_results.result.rows
        processed_rows = []

        for row in rows:
            processed_row = {
                'token_mint_address': row['token_mint_address'],
                'symbol': row['symbol'],
                'token_launch_time': datetime.strptime(row['token_launch_time'].split('.')[0], '%Y-%m-%d %H:%M:%S'),
                'trader_id': row['trader_id'],
                'token_bought_amount': float(row['token_bought_amount']),
                'amount_usd': float(row['amount_usd']),
                'block_time': datetime.strptime(row['block_time'].split('.')[0], '%Y-%m-%d %H:%M:%S'),
                'buyer_rank': int(row['buyer_rank']),
                'last_updated': datetime.now().timestamp()
            }

            await prisma.earlytokenbuyers.create(data=processed_row)
            processed_rows.append(processed_row)

        return jsonify({'wallets': processed_rows})

    except Exception as e:
        logger.error(f"Error querying Dune: {str(e)}")
        return jsonify({'error': 'Failed to fetch data from Dune'}), 500

@app.route('/api/token-profitable-wallets', methods=['GET'])
async def get_token_profitable_wallets():
    token_mint_address = request.args.get('token_mint_address')
    
    if not token_mint_address:
        return jsonify({'error': 'token_mint_address parameter is required'}), 400

    existing_results = await prisma.tokenprofitablewallets.find_many(
        where={
            'token_mint_address': token_mint_address,
            'last_updated': {
                'gte': datetime.now().timestamp() - (24 * 60 * 60)  # Within last 24 hours
            }
        },
        order={
            'total_profit': 'desc'
        }
    )

    if existing_results:
        return jsonify({'wallets': [
            {
                'trader_id': w.trader_id,
                'total_profit': w.total_profit,
                'total_buy_usd': w.total_buy_usd,
                'total_sell_usd': w.total_sell_usd,
                'total_trades': w.total_trades,
                'total_wins': w.total_wins,
                'total_losses': w.total_losses,
                'win_rate': w.win_rate,
                'avg_profit_per_trade': w.avg_profit_per_trade,
                'total_volume_bought': w.total_volume_bought,
                'total_volume_sold': w.total_volume_sold,
                'total_volume_traded': w.total_volume_traded,
                'pnl_ratio': w.pnl_ratio,
                'last_trade_time': w.last_trade_time
            } for w in existing_results
        ]})

    try:
        query = QueryBase(
            name="Most profitable wallets for a given token.",
            query_id=4639226,
            params=[
                QueryParameter.text_type(name="token_mint_address", value=token_mint_address)
            ],
        )

        query_results = dune.run_query(query = query)
        
        rows = query_results.result.rows
        processed_rows = []
        current_time = datetime.now().timestamp()
        
        for row in rows:
            processed_row = {
                'token_mint_address': token_mint_address,
                'trader_id': row['trader_id'],
                'total_profit': float(row['total_profit']),
                'total_buy_usd': float(row['total_buy_usd']),
                'total_sell_usd': float(row['total_sell_usd']),
                'total_trades': int(row['total_trades']),
                'total_wins': int(row['total_wins']),
                'total_losses': int(row['total_losses']),
                'win_rate': float(row['win_rate']),
                'avg_profit_per_trade': float(row['avg_profit_per_trade']),
                'total_volume_bought': float(row['total_volume_bought']),
                'total_volume_sold': float(row['total_volume_sold']),
                'total_volume_traded': float(row['total_volume_traded']),
                'pnl_ratio': float(row['pnl_ratio']),
                'last_trade_time': datetime.strptime(row['last_trade_time'].split('.')[0], '%Y-%m-%d %H:%M:%S'),
                'last_updated': current_time
            }
            
            await prisma.tokenprofitablewallets.create(data=processed_row)
            processed_rows.append(processed_row)

        return jsonify({'wallets': processed_rows})

    except Exception as e:
        logger.error(f"Error querying Dune: {str(e)}")
        return jsonify({'error': 'Failed to fetch data from Dune'}), 500

@app.route('/api/profitable-wallets', methods=['GET'])
async def get_profitable_wallets():
    period = request.args.get('period', '30')
    try:
        period = int(period)
        if period not in [1, 7, 30]:
            return jsonify({'error': 'Period must be 1, 7, or 30'}), 400
    except ValueError:
        return jsonify({'error': 'Invalid period value'}), 400

    wallets = await prisma.mostprofitablewallets.find_many(
        where={
            'period': period
        },
        order={
            'total_profit': 'desc'
        }
    )
    
    wallets_dict = [{
        'trader_id': w.trader_id,
        'total_profit': w.total_profit,
        'total_buy_usd': w.total_buy_usd,
        'total_sell_usd': w.total_sell_usd,
        'total_trades': w.total_trades,
        'total_wins': w.total_wins,
        'total_losses': w.total_losses,
        'win_rate': w.win_rate,
        'avg_profit_per_trade': w.avg_profit_per_trade,
        'total_volume_bought': w.total_volume_bought,
        'total_volume_sold': w.total_volume_sold,
        'total_volume_traded': w.total_volume_traded,
        'pnl_ratio': w.pnl_ratio,
        'last_trade_time': w.last_trade_time
    } for w in wallets]
    return jsonify({'wallets': wallets_dict})

@app.route('/api/high-volume-wallets', methods=['GET'])
async def get_high_volume_wallets():
    days = request.args.get('period', '30')
    try:
        days = int(days)
        if days not in [1, 7, 30]:
            return jsonify({'error': 'Period must be 1, 7, or 30'}), 400
    except ValueError:
        return jsonify({'error': 'Invalid period value'}), 400

    wallets = await prisma.highactivitywalletsbyvolume.find_many(
        where={
            'days': days
        },
        order={
            'total_volume_usd': 'desc'
        }
    )
    
    wallets_dict = [{
        'trader_id': w.trader_id,
        'total_volume_usd': w.total_volume_usd,
        'total_trades': w.total_trades,
        'avg_trade_size_usd': w.avg_trade_size_usd,
        'last_trade_time': w.last_trade_time
    } for w in wallets]
    return jsonify({'wallets': wallets_dict})

@app.route('/api/high-transaction-wallets', methods=['GET'])
async def get_high_transaction_wallets():
    days = request.args.get('period', '30')
    try:
        days = int(days)
        if days not in [1, 7, 30]:
            return jsonify({'error': 'Period must be 1, 7, or 30'}), 400
    except ValueError:
        return jsonify({'error': 'Invalid period value'}), 400

    wallets = await prisma.highactivitywalletsbytransactions.find_many(
        where={
            'days': days
        },
        order={
            'total_transactions': 'desc'
        }
    )
    
    wallets_dict = [{
        'trader_id': w.trader_id,
        'total_transactions': w.total_transactions,
        'avg_daily_transactions': w.avg_daily_transactions,
        'total_volume_usd': w.total_volume_usd,
        'avg_trade_size_usd': w.avg_trade_size_usd,
        'last_trade_time': w.last_trade_time
    } for w in wallets]
    return jsonify({'wallets': wallets_dict})

@app.route('/api/wallet-holding-times', methods=['GET'])
async def get_wallet_holding_times():
    trader_id = request.args.get('trader_id')
    
    if not trader_id:
        return jsonify({'error': 'trader_id parameter is required'}), 400

    existing_result = await prisma.tokenholdingtimes.find_first(
        where={
            'trader_id': trader_id,
            'last_updated': {
                'gte': datetime.now().timestamp() - (24 * 60 * 60)
            }
        }
    )

    if existing_result:
        return jsonify({
            'trader_id': existing_result.trader_id,
            'shortest_hold_time': existing_result.shortest_hold_time,
            'longest_hold_time': existing_result.longest_hold_time,
            'average_hold_time': existing_result.average_hold_time,
            'shortest_hold_token': existing_result.shortest_hold_token,
            'shortest_hold_symbol': existing_result.shortest_hold_symbol,
            'longest_hold_token': existing_result.longest_hold_token,
            'longest_hold_symbol': existing_result.longest_hold_symbol
        })

    try:
        query = QueryBase(
            name="How long a wallet holds a token before selling - Shortest hold, longest hold and average hold.",
            query_id=4639965,
            params=[
                QueryParameter.text_type(name="trader_id", value=trader_id)
            ],
        )

        query_results = dune.run_query(query = query)
        
        row = query_results.result.rows[0]
        current_time = datetime.now().timestamp()
        
        holding_times = {
            'trader_id': row['trader_id'],
            'shortest_hold_time': float(row['shortest_hold_time']),
            'longest_hold_time': float(row['longest_hold_time']),
            'average_hold_time': float(row['average_hold_time']),
            'shortest_hold_token': row['shortest_hold_token'],
            'shortest_hold_symbol': row['shortest_hold_symbol'],
            'longest_hold_token': row['longest_hold_token'],
            'longest_hold_symbol': row['longest_hold_symbol'],
            'last_updated': current_time
        }
        
        await prisma.tokenholdingtimes.create(data=holding_times)
        
        return jsonify({
            'trader_id': holding_times['trader_id'],
            'shortest_hold_time': holding_times['shortest_hold_time'],
            'longest_hold_time': holding_times['longest_hold_time'],
            'average_hold_time': holding_times['average_hold_time'],
            'shortest_hold_token': holding_times['shortest_hold_token'],
            'shortest_hold_symbol': holding_times['shortest_hold_symbol'],
            'longest_hold_token': holding_times['longest_hold_token'],
            'longest_hold_symbol': holding_times['longest_hold_symbol']
        })

    except Exception as e:
        logger.error(f"Error querying Dune: {str(e)}")
        return jsonify({'error': 'Failed to fetch data from Dune'}), 500

@app.route('/api/successful-token-deployers', methods=['GET'])
async def get_successful_token_deployers():
    period = request.args.get('period', '30')
    try:
        period = int(period)
        if period not in [2, 7, 30]:
            return jsonify({'error': 'Period must be 2, 7, or 30'}), 400
    except ValueError:
        return jsonify({'error': 'Invalid period value'}), 400

    tokens = await prisma.tokendeployersuccess.find_many(
        where={
            'period_days': period
        },
        order={
            'max_market_cap': 'desc'
        }
    )
    
    tokens_dict = [{
        'token_mint_address': t.token_mint_address,
        'symbol': t.symbol,
        'name': t.name,
        'decimals': t.decimals,
        'created_at': t.created_at,
        'init_tx': t.init_tx,
        'total_supply': t.total_supply,
        'current_price': t.current_price,
        'max_price_in_period': t.max_price_in_period,
        'current_market_cap': t.current_market_cap,
        'max_market_cap': t.max_market_cap,
        'token_creator': t.token_creator,
        'token_launch_time': t.token_launch_time
    } for t in tokens]
    
    return jsonify({'tokens': tokens_dict})

@app.route('/api/update-data', methods=['POST'])
async def update_data():
    try:
        # Clear all tables first
        await prisma.mostprofitablewallets.delete_many()
        await prisma.highactivitywalletsbyvolume.delete_many()
        await prisma.highactivitywalletsbytransactions.delete_many()
        await prisma.earlytokenbuyers.delete_many()

        queries_yml = os.path.join(os.path.dirname(__file__), '.', 'queries.yml')
        with open(queries_yml, 'r', encoding='utf-8') as file:
            data = yaml.safe_load(file)

        query_ids = [id for id in data['query_ids']]

        for id in query_ids:
            results = dune.get_latest_result(id, max_age_hours=24)
            rows = results.result.rows
            
            for row in rows:
                if 'last_trade_time' in row:
                    row['last_trade_time'] = datetime.strptime(
                        row['last_trade_time'].split('.')[0], 
                        '%Y-%m-%d %H:%M:%S'
                    )
                if id == 4631759:  # Early Token Buyers
                    await prisma.earlytokenbuyers.create(
                        data=row
                    )
                elif id == 4629656:  # High Activity by Volume
                    await prisma.highactivitywalletsbyvolume.create(
                        data=row
                    )
                elif id == 4629687:  # High Activity by Transactions
                    await prisma.highactivitywalletsbytransactions.create(
                        data=row
                    )
                elif id == 4629509:  # Most Profitable Wallets
                    await prisma.mostprofitablewallets.create(
                        data=row
                    )
                elif id == 4656172:  # Token Deployer Success
                    row['created_at'] = datetime.strptime(row['created_at'].split('.')[0], '%Y-%m-%d %H:%M:%S')
                    row['token_launch_time'] = datetime.strptime(row['token_launch_time'].split('.')[0], '%Y-%m-%d %H:%M:%S')
                    
                    await prisma.tokendeployersuccess.create(
                        data=row
                    )

        return {'status': 'success', 'message': 'Data updated successfully'}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500

if __name__ == '__main__':
    from hypercorn.config import Config
    from hypercorn.asyncio import serve
    
    config = Config()
    config.bind = ["0.0.0.0:5000"]
    asyncio.run(serve(app, config))
