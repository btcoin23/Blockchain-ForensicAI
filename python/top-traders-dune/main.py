from quart import Quart, request, jsonify
from prisma import Prisma
import asyncio
import logging
from quart_cors import cors
from dune_client.client import DuneClient
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

        return {'status': 'success', 'message': 'Data updated successfully'}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500

if __name__ == '__main__':
    from hypercorn.config import Config
    from hypercorn.asyncio import serve
    
    config = Config()
    config.bind = ["0.0.0.0:5000"]
    asyncio.run(serve(app, config))
