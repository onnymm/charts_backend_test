import re
from iacele import OdooAPIManager
import pandas as pd



def sold_products_in_a_week(api_manager: OdooAPIManager) -> dict:
    """
    ## Visualización de productos de la venta de una semana
    Se ofrece una estadística que muestra cuánto generó de ingresos brutos
    cada producto en una semana de ventas junto con la utilidad que generó. 
    Además de esto también se muestra su precio público para poder
    generar una gráfica de burbujas y representar este último valor como el
    tamaño de la burbuja.
    Todo esto se devuelve en un diccionario que se envía por el servidor
    backend de FastAPI.
    """

    # Parámetros de búsqueda
    search_criteria = [
        '&',
            '&',
                ("move_type", "=", "out_invoice"),
                ("account_id", "in", [197, 85]),
            "&",
                ("date", ">=", "2024-06-10"),
                ("date", "<=", "2024-06-15")
    ]

    # Campos a retornar por el API de Odoo
    fields= [
        "quantity",
        "price_subtotal",
        "product_id"
    ]

    # Obtención del DataFrame de línea de factura
    inv = api_manager.data.get_dataset("account.move.line", search_criteria, fields)

    # Obtención de los productos existentes en el 
    prod = api_manager.data.get_dataset(
        "product.product",
        [
            (
                "id",
                "in",
                [int(i) for i in inv["product_id"].unique()]
            )
        ],
        ["standard_price", "lst_price"]
    )

    # Se hace la fusión de los DataFrames para complementar la información
    merged = (
        inv
        .pipe(
            lambda df: (
                pd.merge(
                    left= df,
                    right= (
                        prod
                        .rename(
                            columns= {column: f"product_{column}" for column in prod.columns}
                        )
                    ),
                    left_on= 'product_id',
                    right_on= 'product_id',
                    how= 'left'
                )
            )
        )
    )

    # Se generan las estadísticas por producto
    stats = (
        merged
        # Se calcula la utilidad acumulada por producto
        .assign(
            product_total_utility= lambda df: (
                df["price_subtotal"] - (df["product_standard_price"] * df["quantity"])
            )
        )
        # Se generan las estadísticas por producto
        .groupby("product_name")
        .agg(
            {
                'price_subtotal': 'sum',
                'product_total_utility': 'sum',
                # 'product_name': 'first',
                'product_lst_price': 'first'
            }
        )
        # Se filtran las columnas necesarias
        [
            [
                'price_subtotal',
                'product_total_utility',
                'product_lst_price'
            ]
        ]
    )

    # Se retorna el diccionario
    return (
        stats
        .T
        .to_dict(orient='list')
    )



def quotation_amounts(api_manager: OdooAPIManager) -> dict:
    """
    ## Estadísticas de las 5 vendedoras con más montos de cotización en Mayo
    Este método realiza una solicitud al API de Odoo para traer las estadísticas
    de las 5 mejores vendedoras que más cotizaron (en monto) durante el mes de Mayo.
    """

    # Parámetros de búsqueda
    search_criteria = [
        '&',
            ('state', '=', 'sale'),
            '&',
                ("create_date", ">=", "2024-05-01"),
                ("create_date", "<=", "2024-05-31")
    ]

    # Campos a retornar por el API de Odoo
    fields = [
        'name',
        'user_id',
        'amount_untaxed',
    ]

    # Se obtiene el conjunto de datos preprocesado en un DataFrame
    ven = api_manager.data.get_dataset('sale.order', search_criteria, fields)

    # Retorno del DataFrame en formato de diccionario de diccionarios
    return (
        ven
        # Se hace un agrupamiento por 'user_id'
        .groupby('user_id')
        .agg(
            {
                # Se obtienen los nombres
                'user_name': 'first',
                # Se suma el monto de las cotizaciones
                'amount_untaxed': 'sum',
            }
        )
        # Se ordenan los totales de mayor a menor
        .sort_values('amount_untaxed', ascending= False)
        # Se toman sólo las primeras cinco filas
        .iloc[0:5]
        # Se transpone el DataFrame
        .T
        # Se convierte a diccionario
        .to_dict()
    )

def get_monthly_total_amounts(api_manager: OdooAPIManager):
    """
    ## Montos totales de cada almacén por mes
    Esta función retorna un resumen del total facturado por cada almacén
    en agrupaciones de mes.
    """
    # Parámetros de búsqueda
    search_criteria = [
        '&',
            ('state', '=', 'posted'),
            ('move_type', '=', 'out_invoice')
    ]

    # Campos a retornar por el API de Odoo
    fields = [
        "name",
        "amount_untaxed",
        "invoice_date"
    ]

    # Se obtiene el conjunto de datos preprocesado en un DataFrame
    sales = api_manager.data.get_dataset("account.move", search_criteria, fields)

    # Diccionarios para el reemplazo de valores
    month_names = {
        1: "Enero",
        2: "Febrero",
        3: "Marzo",
        4: "Abril",
        5: "Mayo",
        6: "Junio",
        7: "Julio",
        8: "Agosto",
        9: "Septiembre",
        10: "Octubre",
        11: "Noviembre",
        12: "Diciembre"
    }
    warehouse_keys = {
        "1": "Cabo San Lucas",
        "2": "San José Del Cabo"
    }

    # Función para obtener la ID del almacén
    def get_warehouse_key(text):
        find = re.search(r"F\d-\w{2}", text)
        if find:
            start, end = find.span()
            return text[start+1:end-3]
        return ""

    # Retorno del DataFrame en formato de diccionario de diccionarios
    return (
        sales
        # Se crean las columnas a usar
        .assign(
            # Número del mes de la factura
            month_num = lambda df: df["invoice_date"].astype("datetime64[ns]").dt.month,
            # Obtención de la ID del almacén
            warehouse = lambda df: df["name"].astype(str).apply(get_warehouse_key).replace(warehouse_keys),
            # Transformación del número del mes al nombre del mes
            month_name = lambda df: df["month_num"].replace(month_names)
        )
        # Se filtran las facturas que no se pudieron asignar a un almacén
        .query("warehouse != ''")
        # Se realiza una agrupación anidada, primero por el almacén y luego por el número del mes
        .groupby(["warehouse", "month_num"])
        .agg(
            {
                # Se obtiene el nombre del mes
                "month_name": "first",
                # Se obtiene la suma de los montos de las facturas del mes
                "amount_untaxed": "sum",
            }
        )
        # Se reestablece la columna del índice para usarla
        .reset_index()
        # Se obtienen sólo las columnas a usar por el API
        [
            [
                "warehouse",
                "month_name",
                "amount_untaxed"
            ]
        ]
        # Se renombran las columnas para una mejor legibilidad
        .rename(
            columns = {
                "month_name": "month",
                "amount_untaxed": "total_amount"
            }
        )
        # Se realiza la transformada del DataFrame y se retorna el diccionario
        .T
        .to_dict()
    )