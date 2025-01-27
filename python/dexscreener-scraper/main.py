from quart import Quart, request, jsonify
from prisma import Prisma
import asyncio
from toptraders import scrape_top_traders
from quart_cors import cors

app = Quart(__name__)
app = cors(app)
prisma = Prisma()

@app.before_serving
async def startup():
    await prisma.connect()

@app.after_serving
async def shutdown():
    await prisma.disconnect()

@app.route('/api/scrape', methods=['POST'])
async def trigger_scrape():
    try:
        await scrape_top_traders()
        return {'status': 'success', 'message': 'Scraping completed'}, 200
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500

@app.route('/api/tokens', methods=['GET'])
async def get_tokens():
    tokens = await prisma.token.find_many(
        order={
            'createdAt': 'desc'
        }
    )
    return {'tokens': tokens}

@app.route('/api/tokens', methods=['POST'])
async def add_token():
    data = await request.get_json()
    if not all(k in data for k in ['token', 'chain', 'address']):
        return {'error': 'Missing required fields'}, 400
    
    token = await prisma.token.create({
        'data': {
            'token': data['token'],
            'chain': data['chain'],
            'address': data['address']
        }
    })
    return token, 201

@app.route('/api/tokens/<int:token_id>', methods=['DELETE'])
async def delete_token(token_id):
    try:
        await prisma.token.delete(
            where={
                'id': token_id
            }
        )
        return '', 204
    except Exception:
        return jsonify({'error': 'Token not found'}), 404

@app.route('/api/top-traders/<token_address>', methods=['GET'])
async def get_token_top_traders(token_address):
    period = request.args.get('period', '30d')
    try:
        limit = min(int(request.args.get('limit', 10)), 30)
    except ValueError:
        limit = 10

    valid_periods = ['30d', '7d', '3d', '1d']
    if period not in valid_periods:
        return jsonify({'error': 'Invalid period. Must be one of: 30d, 7d, 3d, 1d'}), 400

    query = f"""
        SELECT 
            wallet,
            rank,
            boughtAmount,
            boughtVolume,
            soldAmount,
            soldVolume,
            pnl,
            unrealizedValue
        FROM top_traders
        WHERE period = '{period}' 
        AND tokenAddress = '{token_address}'
        ORDER BY rank ASC
        LIMIT {limit}
    """
    
    results = await prisma.query_raw(query)

    traders = [{
        'wallet': trader['wallet'],
        'rank': int(trader['rank']),
        'boughtAmount': float(trader['boughtAmount']),
        'boughtVolume': float(trader['boughtVolume']),
        'soldAmount': float(trader['soldAmount']),
        'soldVolume': float(trader['soldVolume']),
        'pnl': float(trader['pnl']),
        'unrealizedValue': float(trader['unrealizedValue'])
    } for trader in results]

    return jsonify({
        'period': period,
        'limit': limit,
        'traders': traders
    })

@app.route('/api/top-traders', methods=['GET'])
async def get_top_traders():
    period = request.args.get('period', '30d')
    try:
        limit = min(int(request.args.get('limit', 10)), 30)
    except ValueError:
        limit = 10

    valid_periods = ['30d', '7d', '3d', '1d']
    if period not in valid_periods:
        return jsonify({'error': 'Invalid period. Must be one of: 30d, 7d, 3d, 1d'}), 400

    query = f"""
        SELECT 
            wallet,
            SUM(boughtAmount) as total_bought_amount,
            SUM(boughtVolume) as total_bought_volume,
            SUM(soldAmount) as total_sold_amount,
            SUM(soldVolume) as total_sold_volume,
            SUM(pnl) as total_pnl,
            COUNT(*) as total_trades
        FROM top_traders
        WHERE period = '{period}'
        GROUP BY wallet
        ORDER BY total_pnl DESC
        LIMIT {limit}
    """
    
    results = await prisma.query_raw(query)

    traders = [{
        'wallet': trader['wallet'],
        'totalPnl': float(trader['total_pnl']),
        'totalBoughtAmount': float(trader['total_bought_amount']),
        'totalBoughtVolume': float(trader['total_bought_volume']),
        'totalSoldAmount': float(trader['total_sold_amount']),
        'totalSoldVolume': float(trader['total_sold_volume']),
        'totalTrades': int(trader['total_trades'])
    } for trader in results]

    return jsonify({
        'period': period,
        'limit': limit,
        'traders': traders
    })

if __name__ == '__main__':
    from hypercorn.config import Config
    from hypercorn.asyncio import serve
    
    config = Config()
    config.bind = ["0.0.0.0:5000"]
    asyncio.run(serve(app, config))
