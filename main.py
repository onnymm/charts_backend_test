from iacele import OdooAPIManager
import stats
import utils
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Actualización de la IP local
local_ip = utils.define_local_origin()

# API de Odoo
odoo_test = OdooAPIManager(test_db= True)

# App del servidor
app = FastAPI()

# Orígenes autorizados
origins = [
    f"http://{local_ip}:5173",
]

# Control de middlewares para permitir las solicitudes desde el servidor frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins= origins,
    allow_credentials= True,
    allow_methods= ["*"],
    allow_headers= ["*"],
)

@app.get("/products_week")
async def get_sold_products_in_a_week():
    return stats.sold_products_in_a_week(odoo_test)

@app.get("/quotation_amounts")
async def get_quotation_amounts():
    return stats.quotation_amounts(odoo_test)

@app.get("/monthly_total_amounts")
async def monthly_total_amounts():
    return stats.get_monthly_total_amounts(odoo_test)

@app.get("/")
def test():
    return {"message": "Hola"}