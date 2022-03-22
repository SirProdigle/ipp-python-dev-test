from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.requests import Request
from starlette.routing import Route
import uvicorn
import csv
from datetime import datetime
from typing import List

file_path = 'data/nifty50_all.csv'
cached_symbols: List[str] = [] # Check for symbols here before opening the CSV, will speed up 404 responses
used_keys: List[str] = ["Date", "Open", "High", "Low", "Close"]


async def price_data(request: Request) -> JSONResponse:
    """
    Return price data for the requested symbol
    """
    symbol = request.path_params['symbol'].upper()
    year_param = request.query_params.get("year")

    # We have all symbols cached, so we can handle 404's without opening the file
    if symbol not in cached_symbols:
        return JSONResponse({"error": "Symbol {0} not found in dataset".format(symbol)}, 404)

    try:
        responses = []
        with open(file_path, 'r') as file:
            csv_reader = csv.DictReader(file)
            if year_param:
                for row in csv_reader:
                    if row["Symbol"] == symbol:
                        stock_year = row["Date"].split('-')[0]
                        if year_param == stock_year:
                            # Would be better with a different CSV handler, currently creates the row with all keys
                            # in the first place, unnecessary
                            responses.append({x: row[x] for x in used_keys})
            else:
                for row in csv_reader:
                    if row["Symbol"] == symbol:
                        responses.append({x: row[x] for x in used_keys})

            # Sort for newest first
            responses.sort(key=lambda obj: datetime.strptime(obj["Date"], "%Y-%m-%d"), reverse=True)
            return JSONResponse(responses)
    except Exception as e:
        print(str(e))
        return JSONResponse({"error": str(e)}, 500)


# URL routes
app = Starlette(debug=True, routes=[
    Route('/nifty/stocks/{symbol}', price_data)
])


def main() -> None:
    # Collect list of symbols for quick referencing
    try:
        with open(file_path, 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                if row["Symbol"] not in cached_symbols:
                    cached_symbols.append(row["Symbol"])
    except Exception as e:
        print(e)
        quit(1)

    uvicorn.run(app, host='0.0.0.0', port=8888)


# Entry point
main()
