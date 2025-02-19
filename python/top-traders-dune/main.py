from apscheduler.schedulers.asyncio import AsyncIOScheduler
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
from collections import defaultdict
import socket
import time
from kolscan_scraper import scrape_kolscan

dune_request_queue = defaultdict(dict)  # Store query requests
QUEUE_CHECK_INTERVAL = 60  # seconds

class DuneRequest:
    def __init__(self, query_id, params=None):
        self.query_id = query_id
        self.params = params
        self.timestamp = time.time()
        self.status = "queued"  # queued, processing, completed, failed

app = Quart(__name__)
app = cors(app)
prisma = Prisma()

dotenv_path = os.path.join(os.path.dirname(__file__), '.', '.env')
load_dotenv(dotenv_path)
dune = DuneClient.from_env()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
scheduler = AsyncIOScheduler()

@app.before_serving
async def startup():
    await prisma.connect()
    scheduler.add_job(scheduled_update, 'cron', hour='*/6', minute=0)  # Runs every 6 hours
    scheduler.start()
    asyncio.create_task(process_dune_queue())

@app.after_serving
async def shutdown():
    scheduler.shutdown()
    await prisma.disconnect()

async def process_dune_queue():
    while True:
        try:
            # Create a safe copy of items to iterate
            queue_items = [(query_id, dict(requests)) 
                          for query_id, requests in dune_request_queue.items()]
            
            for query_id, requests in queue_items:
                request_items = list(requests.items())
                for key, request in request_items:
                    if request.status == "queued":
                        request.status = "processing"
                        try:
                            result = await run_dune_query(request.query_id, request.params)
                            await store_dune_results(request.query_id, result, request.params)
                            request.status = "completed"
                            del dune_request_queue[query_id][key]
                        except ConnectionResetError:
                            logger.info("Client disconnected unexpectedly")
                            continue
                        except socket.error as e:
                            logger.error(f"Socket error: {e}")
                            request.status = "failed"
                            del dune_request_queue[query_id][key]
                        except Exception as e:
                            logger.error(f"Error processing queue request: {str(e)}")
                            request.status = "failed"
                            del dune_request_queue[query_id][key]
        except Exception as e:
            logger.error(f"Queue processing error: {str(e)}")
        await asyncio.sleep(QUEUE_CHECK_INTERVAL)

def process_first_buy_wallet_row(row):
    return {
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

def process_token_profitable_row(row, token_mint_address):
    return {
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
        'last_updated': datetime.now().timestamp()
    }

def process_holding_times_row(row):
    return {
        'trader_id': row['trader_id'],
        'shortest_hold_time': float(row['shortest_hold_time']),
        'longest_hold_time': float(row['longest_hold_time']),
        'average_hold_time': float(row['average_hold_time']),
        'shortest_hold_token': row['shortest_hold_token'],
        'shortest_hold_symbol': row['shortest_hold_symbol'],
        'longest_hold_token': row['longest_hold_token'],
        'longest_hold_symbol': row['longest_hold_symbol'],
        'last_updated': datetime.now().timestamp()
    }

async def store_dune_results(query_id, results, params=None):
    rows = results.result.rows
    if query_id == 4628657:  # First buy wallets
        for row in rows:
            processed_row = process_first_buy_wallet_row(row)
            await prisma.earlytokenbuyers.upsert(
                where={
                    'token_mint_address_buyer_rank': {
                        'token_mint_address': processed_row['token_mint_address'],
                        'buyer_rank': processed_row['buyer_rank']
                    }
                },
                data={
                    'create': processed_row,
                    'update': processed_row
                }
            )
    elif query_id == 4639226:  # Token profitable wallets
        token_mint_address = params[0].value
        for row in rows:
            processed_row = process_token_profitable_row(row, token_mint_address)
            await prisma.tokenprofitablewallets.upsert(
                where={
                    'token_mint_address_trader_id': {
                        'token_mint_address': processed_row['token_mint_address'],
                        'trader_id': processed_row['trader_id']
                    }
                },
                data={
                    'create': processed_row,
                    'update': processed_row
                }
            )
    elif query_id == 4639965:  # Wallet holding times
        for row in rows:
            processed_row = process_holding_times_row(row)
            await prisma.tokenholdingtimes.upsert(
                where={
                    'trader_id': processed_row['trader_id']
                },
                data={
                    'create': processed_row,
                    'update': processed_row
                }
            )

async def run_dune_query(query_id, params=None, max_retries=3):
    for attempt in range(max_retries):
        try:
            async with asyncio.timeout(240):  # 4 minutes timeout
                if params:
                    query = QueryBase(
                        name=f"Query {query_id}",
                        query_id=query_id,
                        params=params
                    )
                    return await asyncio.to_thread(dune.run_query, query)
                return await asyncio.to_thread(dune.get_latest_result, query_id, 24)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            await asyncio.sleep(2 ** attempt)

async def scheduled_update():
    logger.info("Starting scheduled data update")
    try:
        # Clear all tables first
        await prisma.mostprofitablewallets.delete_many()
        await prisma.mostprofitablewalletstx.delete_many()
        await prisma.highactivitywalletsbyvolume.delete_many()
        await prisma.highactivitywalletsbytransactions.delete_many()
        await prisma.earlytokenbuyers.delete_many()
        await prisma.tokendeployersuccess.delete_many()
        await prisma.kolwallets.delete_many()

        await scrape_kolscan()

        queries_yml = os.path.join(os.path.dirname(__file__), '.', 'queries.yml')
        with open(queries_yml, 'r', encoding='utf-8') as file:
            data = yaml.safe_load(file)

        query_ids = [id for id in data['query_ids']]

        results = await asyncio.gather(*[run_dune_query(id) for id in query_ids])

        for id, query_result in zip(query_ids, results):
            rows = query_result.result.rows
            for row in rows:
                if 'last_trade_time' in row:
                    row['last_trade_time'] = datetime.strptime(
                        row['last_trade_time'].split('.')[0], 
                        '%Y-%m-%d %H:%M:%S'
                    )
                if id == 4631759:
                    await prisma.earlytokenbuyers.create(data=row)
                elif id == 4629656:
                    await prisma.highactivitywalletsbyvolume.create(data=row)
                elif id == 4629687:
                    await prisma.highactivitywalletsbytransactions.create(data=row)
                elif id == 4629509:
                    await prisma.mostprofitablewallets.create(data=row)
                elif id == 4683382:
                    await prisma.mostprofitablewalletstx.create(data=row)
                elif id == 4656172:
                    row['created_at'] = datetime.strptime(row['created_at'].split('.')[0], '%Y-%m-%d %H:%M:%S')
                    row['token_launch_time'] = datetime.strptime(row['token_launch_time'].split('.')[0], '%Y-%m-%d %H:%M:%S')
                    await prisma.tokendeployersuccess.create(data=row)

        logger.info("Scheduled data update completed successfully")
    except Exception as e:
        logger.error(f"Error in scheduled update: {str(e)}")

def format_wallet_data(wallet):
    return {
        'token_mint_address': wallet.token_mint_address,
        'symbol': wallet.symbol,
        'token_launch_time': wallet.token_launch_time,
        'trader_id': wallet.trader_id,
        'token_bought_amount': wallet.token_bought_amount,
        'amount_usd': wallet.amount_usd,
        'block_time': wallet.block_time,
        'buyer_rank': wallet.buyer_rank
    }

@app.route('/api/first-buy-wallets', methods=['GET'])
async def get_first_buy_wallets():
    token_mint_address = request.args.get('token_mint_address')
    
    if not token_mint_address:
        return jsonify({'error': 'token_mint_address parameter is required'}), 400

    existing_results = await prisma.earlytokenbuyers.find_many(
        where={
            'token_mint_address': token_mint_address
        },
        order={
            'buyer_rank': 'asc'
        }
    )

    if existing_results:
        if len(existing_results) >= 10 or (existing_results[-1].last_updated - existing_results[-1].token_launch_time.timestamp()) > 600:
            return jsonify({'wallets': [format_wallet_data(w) for w in existing_results]})

    queue_key = f"first_buy_{token_mint_address}"
    request_info = dune_request_queue[4628657].get(queue_key)

    if request_info and request_info.status == "processing":
        # Wait for the processing request to complete
        while request_info.status == "processing":
            await asyncio.sleep(1)
    elif not request_info:
        # Add new request to queue and process it
        params = [QueryParameter.text_type(name="token_mint_address", value=token_mint_address)]
        dune_request_queue[4628657][queue_key] = DuneRequest(4628657, params)
        request_info = dune_request_queue[4628657][queue_key]
        
        # Process the request
        request_info.status = "processing"
        try:
            results = await run_dune_query(4628657, params)
            await store_dune_results(4628657, results, params)
            request_info.status = "completed"
        except Exception as e:
            request_info.status = "failed"
            raise e
        finally:
            del dune_request_queue[4628657][queue_key]

    # Return the updated results
    final_results = await prisma.earlytokenbuyers.find_many(
        where={
            'token_mint_address': token_mint_address
        },
        order={
            'buyer_rank': 'asc'
        }
    )
    
    return jsonify({'wallets': [format_wallet_data(w) for w in final_results]})


def format_profitable_wallet(wallet):
    return {
        'trader_id': wallet.trader_id,
        'total_profit': wallet.total_profit,
        'total_buy_usd': wallet.total_buy_usd,
        'total_sell_usd': wallet.total_sell_usd,
        'total_trades': wallet.total_trades,
        'total_wins': wallet.total_wins,
        'total_losses': wallet.total_losses,
        'win_rate': wallet.win_rate,
        'avg_profit_per_trade': wallet.avg_profit_per_trade,
        'total_volume_bought': wallet.total_volume_bought,
        'total_volume_sold': wallet.total_volume_sold,
        'total_volume_traded': wallet.total_volume_traded,
        'pnl_ratio': wallet.pnl_ratio,
        'last_trade_time': wallet.last_trade_time
    }

@app.route('/api/token-profitable-wallets', methods=['GET'])
async def get_token_profitable_wallets():
    token_mint_address = request.args.get('token_mint_address')
    
    if not token_mint_address:
        return jsonify({'error': 'token_mint_address parameter is required'}), 400

    existing_results = await prisma.tokenprofitablewallets.find_many(
        where={
            'token_mint_address': token_mint_address,
            'last_updated': {
                'gte': datetime.now().timestamp() - (24 * 60 * 60)
            }
        },
        order={
            'total_profit': 'desc'
        }
    )

    if existing_results:
        return jsonify({'wallets': [format_profitable_wallet(w) for w in existing_results]})

    queue_key = f"token_profitable_{token_mint_address}"
    request_info = dune_request_queue[4639226].get(queue_key)

    if request_info and request_info.status == "processing":
        while request_info.status == "processing":
            await asyncio.sleep(1)
    elif not request_info:
        params = [QueryParameter.text_type(name="token_mint_address", value=token_mint_address)]
        dune_request_queue[4639226][queue_key] = DuneRequest(4639226, params)
        request_info = dune_request_queue[4639226][queue_key]
        
        request_info.status = "processing"
        try:
            results = await run_dune_query(4639226, params)
            await store_dune_results(4639226, results, params)
            request_info.status = "completed"
        except Exception as e:
            request_info.status = "failed"
            raise e
        finally:
            del dune_request_queue[4639226][queue_key]

    final_results = await prisma.tokenprofitablewallets.find_many(
        where={
            'token_mint_address': token_mint_address
        },
        order={
            'total_profit': 'desc'
        }
    )
    
    return jsonify({'wallets': [format_profitable_wallet(w) for w in final_results]})

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

@app.route('/api/profitable-wallets-tx', methods=['GET'])
async def get_profitable_wallets_tx():
    period = request.args.get('period', '30')
    tx_min = request.args.get('tx_min', 0)
    tx_max = request.args.get('tx_max', 100)
    try:
        period = int(period)
        if period not in [1, 7, 30]:
            return jsonify({'error': 'Period must be 1, 7, or 30'}), 400
        tx_min = int(tx_min)
        tx_max = int(tx_max)
    except ValueError:
        return jsonify({'error': 'Invalid parameter'}), 400

    wallets = await prisma.mostprofitablewalletstx.find_many(
        where={
            'period': period,
            'total_transaction_count': {
                'gte': tx_min,
                'lte': tx_max
            }
        },
        order={
            'total_profit': 'desc'
        }
    )

    wallets_dict = [{
        'trader_id': w.trader_id,
        'total_transaction_count': w.total_transaction_count,
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

def format_holding_times(data):
    return {
        'trader_id': data.trader_id,
        'shortest_hold_time': data.shortest_hold_time,
        'longest_hold_time': data.longest_hold_time,
        'average_hold_time': data.average_hold_time,
        'shortest_hold_token': data.shortest_hold_token,
        'shortest_hold_symbol': data.shortest_hold_symbol,
        'longest_hold_token': data.longest_hold_token,
        'longest_hold_symbol': data.longest_hold_symbol
    }

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
        return jsonify(format_holding_times(existing_result))

    queue_key = f"holding_times_{trader_id}"
    request_info = dune_request_queue[4639965].get(queue_key)

    if request_info and request_info.status == "processing":
        while request_info.status == "processing":
            await asyncio.sleep(1)
    elif not request_info:
        params = [QueryParameter.text_type(name="trader_id", value=trader_id)]
        dune_request_queue[4639965][queue_key] = DuneRequest(4639965, params)
        request_info = dune_request_queue[4639965][queue_key]
        
        request_info.status = "processing"
        try:
            results = await run_dune_query(4639965, params)
            await store_dune_results(4639965, results, params)
            request_info.status = "completed"
        except Exception as e:
            request_info.status = "failed"
            raise e
        finally:
            del dune_request_queue[4639965][queue_key]

    final_result = await prisma.tokenholdingtimes.find_first(
        where={
            'trader_id': trader_id
        }
    )
    
    return jsonify(format_holding_times(final_result))

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

@app.route('/api/kol-leaderboard', methods=['GET'])
async def get_kol_leaderboard():
    period = request.args.get('period', '1')
    wallet_name = request.args.get('wallet_name')
    wallet_address = request.args.get('wallet_address')
    
    try:
        period = int(period)
        if period not in [1, 7, 30]:
            return jsonify({'error': 'Period must be 1, 7, or 30'}), 400
    except ValueError:
        return jsonify({'error': 'Invalid period value'}), 400

    # Build where clause based on provided parameters
    where_clause = {'period': period}
    if wallet_name:
        where_clause['wallet_name'] = wallet_name
    if wallet_address:
        where_clause['wallet_address'] = wallet_address

    results = await prisma.kolleaderboard.find_many(
        where=where_clause,
        order={
            'pnl_usd': 'desc'
        }
    )
    
    leaderboard = [{
        'wallet_name': entry.wallet_name,
        'wallet_address': entry.wallet_address,
        'pnl_usd': entry.pnl_usd,
        'pnl_sol': entry.pnl_sol,
        'telegram': entry.telegram,
        'twitter': entry.twitter
    } for entry in results]

    return jsonify({'leaderboard': leaderboard})

@app.route('/api/update-data', methods=['POST'])
async def update_data():
    try:
        # Clear all tables first
        await prisma.mostprofitablewallets.delete_many()
        await prisma.mostprofitablewalletstx.delete_many()
        await prisma.highactivitywalletsbyvolume.delete_many()
        await prisma.highactivitywalletsbytransactions.delete_many()
        await prisma.earlytokenbuyers.delete_many()
        await prisma.tokendeployersuccess.delete_many()
        await prisma.kolleaderboard.delete_many()

        await scrape_kolscan()

        queries_yml = os.path.join(os.path.dirname(__file__), '.', 'queries.yml')
        with open(queries_yml, 'r', encoding='utf-8') as file:
            data = yaml.safe_load(file)

        query_ids = [id for id in data['query_ids']]

        results = await asyncio.gather(*[run_dune_query(id) for id in query_ids])
        
        for id, query_result in zip(query_ids, results):
            rows = query_result.result.rows
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
                elif id == 4683382:  # Most Profitable Wallets
                    await prisma.mostprofitablewalletstx.create(
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
    config.keep_alive_timeout = 300  # 5 minutes
    config.worker_class = "asyncio"
    config.graceful_timeout = 300
    config.timeout = 300

    asyncio.run(serve(app, config))
