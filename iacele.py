import xmlrpc.client
import os
import pandas as pd
import numpy as np
from typing import Literal

class OdooAPIManager():
    """
    ## Conexión al API de Odoo
    Creador de una conexión con el sistema de Odoo a través del API. Puede ser
    a la base de datos principal o a una base de datos de prueba provista en el
    archivo `.env`

    ### Forma de uso:
    ```py
    odoo = OdooAPIManager()
    ```

    Para crear una conexión a la base de datos de prueba, se debe proveer el
    valor `True` en el argumento `test_db`:
    ```py
    odoo_test = OdooAPIManager(test_db=True)
    ```

    Para proporcionar los datos de la conexión al API se debe contar con un
    archivo `.env` en la raíz del proyecto con la siguiente estructura:

    ```env
    ODOO_USERNAME_API = username_api@example.com
    ODOO_CLAVE_API = 1234567890abcdefghijklmnopqrstuvwxyz1234
    ODOO_PASSWORD_API = thisisapassword321
    ODOO_URL_API = https://your-database-name.odoo.com
    ODOO_DB_API = your-database-name
    ODOO_DB_PRUEBA_API = your-database-name-test
    ```

    ----
    ## Métodos disponibles
    ### • Revisión de permisos de acceso
    Método para verificar los permisos de acceso a un modelo
    especificado:
    ```py
    odoo.check_access_rights("res.partner", "write")
    ```

    ### • Búsqueda de registros
    Este método realiza una búsqueda en un modelo especificado y retorna
    una lista de IDs que cumplen con las condiciones especificadas.
    ```py
    odoo.search("sale.order", [("state", "=", "cancel")])
    # [52, 87, 129, 132]
    ```

    ### • Lectura de registros
    Este método realiza una lectura de IDs en donde retorna una lista
    de diccionarios, cada uno, con la información de un registro.
    ```py
    odoo.read("sale.order", [52, 87, 129, 132])
    # [{
    #     'id': 52,
    #     'name': 'S00052',
    #     ...
    #  },
    #  {
    #     'id': 89,
    #     'name': 'S00089',
    #     ...
    #  },
    #  {
    #     'id': 129,
    #     'name': 'S00129',
    #     ...
    #  },
    #  {
    #     'id': 132,
    #     'name': 'S00132',
    #     ...
    # }]
    ```

    ### • Búsqueda y lectura de registros
    Este método es la combinación de los métodos internos
    `OdooAPIManager.search` y `OdooAPIManager.read` optimizado para la
    ejecución de ambos en una misma solicitud al API.
    ```py
    odoo.search_read("sale.order", [("state", "=", "cancel")])
    # [{
    #     'id': 52,
    #     'name': 'S00052',
    #     ...
    #  },
    #  {
    #     'id': 89,
    #     'name': 'S00089',
    #     ...
    #  },
    #  {
    #     'id': 129,
    #     'name': 'S00129',
    #     ...
    #  },
    #  {
    #     'id': 132,
    #     'name': 'S00132',
    #     ...
    # }]
    ```

    ### • Conteo de registros en búsqueda
    Este método devuelve el conteo de la cantidad de registros que cumplen
    un criterio de búsqueda provisto. Es equivalente a usar la función
    `len()` a la lista de retorno del método `OdooAPIManager.search()`:
    ```py
    odoo.search_count("sale.order", [("state", "=", "cancel")])
    # 87
    ```


    """

    # ----- INICIALIZACIÓN -----
    def __init__(self,
        test_db: bool = False
    ) -> None:
        
        # Obtención de la base de datos real o de prueba
        if test_db:
            self._api_db = os.environ.get('ODOO_DB_PRUEBA_API')
        else:
            self._api_db = os.environ.get('ODOO_DB_API')

        # Obtención de las variables de entorno requeridas conexión al API de Odoo
        self._api_url = os.environ.get('ODOO_URL_API')
        self._api_username = os.environ.get('ODOO_USERNAME_API')
        self._api_token = os.environ.get('ODOO_CLAVE_API')

        # Inicialización de la conexión
        self._common = xmlrpc.client.ServerProxy(f'{self._api_url}/xmlrpc/2/common')
        self._uid = self._common.authenticate(self._api_db, self._api_username, self._api_token, {})
        self._models = xmlrpc.client.ServerProxy(f'{self._api_url}/xmlrpc/2/object')

        # Inicialización de módulos de métodos
        self.data = _DataMethods(self)
        self.fix = _FixMethods(self)
        self.models = _ModelsMethods(self)


    # ----- REVISIÓN DE DERECHOS DE ACCESO -----
    def check_access_rights(
        self,
        model: str,
        right: Literal['create', 'read', 'write', 'unlink']
    ) -> bool:
        """
        ## Método para verificar los permisos de acceso a un modelo
        especificado

        Ejemplo de uso:
        ```py
        odoo.check_access_rights("res.partner", "write")
        ```

        Permisos disponibles
        - `create`: Permiso de creación
        - `read`: Permiso de lectura
        - `write`: Permiso de lectura
        - `unlink`: Permiso de eliminación
        """

        return self._request(
            model= model,
            method= "check_access_rights",
            data= self._build_data(
                data= right
            ),
            params= self._build_params(
                raise_exception= False
            )
        )


    # ----- MÉTODO DE BÚSQUEDA -----
    def search(
        self,
        model: str,
        search_criteria: list[tuple, str],
        offset: int = None,
        limit: int = None
    ) -> list[int]:
        """
        ## Método de búsqueda
        Este método realiza una búsqueda en un modelo especificado y retorna
        una lista de IDs que cumplen con las condiciones especificadas.

        Ejemplo de uso:
        ```py
        odoo.search("sale.order", [("state", "=", "cancel")])
        # [52, 87, 129, 132]
        ```

        ### Condición de búsqueda
        Se provee al menos una tupla con la siguiente escructura:
        ```py
        ("nombre_del_campo", "=", "valor")
        ```

        Todo esto se encierra dentro de una lista:
        ```py
        [("nombre_del_campo", "=", "valor")]
        ```

        Los operadores de comparación pueden ser alguno de los siguientes:
        - `=`: Igual a
        - `!=`: Diferente de
        - `in`: Está en
        - `not in`: No está en
        - `ilike`: Contiene
        - `not ilike`: No contiene
        - `>`: Es mayor a
        - `<`: Es menor a
        - `>=`: Es mayor o igual a
        - `<=`: Es menor o igual a

        Los valores pueden ser de tipo `str`, `int`, `float` o `bool`
        dependiendo del tipo de valor del campo.

        También pueden ser de tipo `list` que contenga alguno de los tipos
        anteriores.

        En relaciones tipo `many2one`, `one2many` y `many2many` la búsqueda se
        hace por ID o por lista de IDs.

        ### Búsqueda con varias condiciones provistas
        También se puede incluir más de una condición. Para esto, se agrega un
        operador lógico antes de dos tuplas de condiciones:
        ```py
        ['&', (condicion_1...), (condicion_2...)]
        ```

        Los operadores lógicos disponibles son:
        - `&`: and
        - `|`: or

        ### Desfase de registros para paginación
        Este parámetro sirve para realizar un slice de la lista de IDs
        retornada por el API pero directamente desde el API. Suponiendo que una
        búsqueda normal arrojarría los siguientes resultados:
        ```py
        odoo.search("sale.order", [("state", "=", "sale")])
        # [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14...]
        ```

        Se puede especificar que sólo se requiere el retorno de la lista de IDs
        a partir de un índice especificado, por ejemplo:
        ```py
        odoo.search("sale.order", [("state", "=", "sale")], offset=5)
        # [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19...]
        ```

        ### Límite de registros retornados para paginación
        También es posible establecer una cantidad máxima en la cantidad de IDs
        retornada por el API, también directamente desde el API. Suponiendo que
        una búsqueda normal arrojarría los siguientes resultados:
        ```py
        odoo.search("sale.order", [("state", "=", "sale")])
        # [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14...]
        ```

        Se puede especificar que sólo se requiere obtener una cantidad máxima
        de registros a partir de un número provisto:
        ```py
        odoo.search("sale.order", [("state", "=", "sale")], limit=5)
        # [1, 2, 3, 4, 5]
        ```

        ----
        ### Sugerencia de uso en múltiples condiciones
        Para mejorar y facilitar una búsqueda con múltiples condiciones
        se recomienda asignar la lista a una variable y utilizar la siguiente
        estructura de identación dentro de la lista:
        ```py
        # Ejemplo 1
        criteria = [
            '&',
                (condicion_1...),
                (condicion_2...)
        ]

        # Ejemplo 2
        criteria = [
            '|',
                '&',
                    (condicion_1...),
                    (condicion_2...),
                (condicion_3...)
        ]
        ```

        En el primer ejemplo, la condición 1 y la condición 2 deben
        cumplirse para que un registro se incluya en la lista de resultados.

        En el segundo ejemplo, el resultado `True` de la condición 1 y la
        condición 2, todo esto o la condición 3 deben cumplirse para que un registro se
        incluya en la lista de resultados, es decir, algo como esto:
        ```py
        ((condicion_1 and condicion_2) or condicion_3)
        ```

        La ejecución del método entonces se vería así:
        ```py
        odoo.search("sale.order", criteria)
        ```
        """

        
        # Ejecución del método de solicitud al API
        return self._request(
            model= model,
            method= "search",
            data= self._build_data(
                data= search_criteria
            ),
            params= self._build_params(
                offset=offset,
                limit=limit
            )
        )


    # ----- MÉTODO DE LECTURA -----
    def read(
        self,
        model: str,
        record_ids: list[int],
        fields: list[str] = None
    ) -> list[dict]:
        """
        ## • Método de lectura
        Este método realiza una lectura de IDs en donde retorna una lista
        de diccionarios, cada uno, con la información de un registro.

        ### Ejemplo de uso
        ```py
        odoo.read("sale.order", [52, 87, 129, 132])
        # [{
        #     'id': 52,
        #     'name': 'S00052',
        #     ...
        #  },
        #  {
        #     'id': 89,
        #     'name': 'S00089',
        #     ...
        #  },
        #  {
        #     'id': 129,
        #     'name': 'S00129',
        #     ...
        #  },
        #  {
        #     'id': 132,
        #     'name': 'S00132',
        #     ...
        # }]
        ```

        ### Espeficicación de campos a retornar por el API
        También se puede especificar una lista de campos para reducir
        el tamaño de los diccionarios en la lista para mayor rapidez en el
        tiempo de respuesta de la API:
        ```py
        odoo.read("sale.order", [52, 87, 129, 132], ['name', 'state'])
        ```

        ----
        ### Sugerencia de uso en listas de campos muy grandes
        Para estos casos, se recomienda almacenar la lista de campos en una
        variable con alguna de las siguientes dos estructuras:

        ```py
        # Ejemplo 1
        fields = ['name', 'state', 'salesman_id', 'partner_id']

        # Ejemplo 2
        fields = [
            'name',
            'state',
            'salesman_id',
            'partner_id'
        ]
        ```

        La ejecución del método entonces se vería así:
        ```py
        odoo.read("sale.order", [52, 87, 129, 132], fields)
        ```

        """
        
        # Ejecución del método de solicitud al API
        return self._request(
            model= model,
            method= "read",
            data= self._build_data(record_ids),
            params= self._build_params(
                fields= fields
            )
        )
    

    # ----- MÉTODO DE BÚSQUEDA Y LECTURA -----
    def search_read(
        self,
        model: str,
        data: list[tuple, str],
        fields: list[str] = None,
        offset: int = None,
        limit: int = None
    ):
        """
        ## Método de búsqueda y lectura
        Este método es la combinación de los métodos internos
        `OdooAPIManager.search` y `OdooAPIManager.read` optimizado para la
        ejecución de ambos en una misma solicitud al API.

        ### Ejemplo de uso
        ```py
        odoo.search_read("sale.order", [("state", "=", "cancel")])
        # [{
        #     'id': 52,
        #     'name': 'S00052',
        #     ...
        #  },
        #  {
        #     'id': 89,
        #     'name': 'S00089',
        #     ...
        #  },
        #  {
        #     'id': 129,
        #     'name': 'S00129',
        #     ...
        #  },
        #  {
        #     'id': 132,
        #     'name': 'S00132',
        #     ...
        # }]
        ```

        ### Condición de búsqueda
        Se provee al menos una tupla con la siguiente escructura:
        ```py
        ("nombre_del_campo", "=", "valor")
        ```

        Todo esto se encierra dentro de una lista:
        ```py
        [("nombre_del_campo", "=", "valor")]
        ```

        Los operadores de comparación pueden ser alguno de los siguientes:
        - `=`: Igual a
        - `!=`: Diferente de
        - `in`: Está en
        - `not in`: No está en
        - `ilike`: Contiene
        - `not ilike`: No contiene
        - `>`: Es mayor a
        - `<`: Es menor a
        - `>=`: Es mayor o igual a
        - `<=`: Es menor o igual a

        Los valores pueden ser de tipo `str`, `int`, `float` o `bool`
        dependiendo del tipo de valor del campo.

        También pueden ser de tipo `list` que contenga alguno de los tipos
        anteriores.

        En relaciones tipo `many2one`, `one2many` y `many2many` la búsqueda se
        hace por ID o por lista de IDs.

        ### Búsqueda con varias condiciones provistas
        También se puede incluir más de una condición. Para esto, se agrega un
        operador lógico antes de dos tuplas de condiciones:
        ```py
        ['&', (condicion_1...), (condicion_2...)]
        ```

        Los operadores lógicos disponibles son:
        - `&`: and
        - `|`: or

        ### Espeficicación de campos a retornar por el API
        También se puede especificar una lista de campos para reducir
        el tamaño de los diccionarios en la lista para mayor rapidez en el
        tiempo de respuesta de la API:
        ```py
        odoo.read("sale.order", [52, 87, 129, 132], ['name', 'state'])
        ```

        ### Desfase de registros para paginación
        Este parámetro sirve para realizar un slice de la lista de IDs
        retornada por el API pero directamente desde el API. Suponiendo que una
        búsqueda normal arrojarría los siguientes resultados:
        ```py
        odoo.search_read("sale.order", [("state", "=", "sale")])
        # [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14...]
        ```

        Se puede especificar que sólo se requiere el retorno de la lista de IDs
        a partir de un índice especificado, por ejemplo:
        ```py
        odoo.search_read("sale.order", [("state", "=", "sale")], offset=5)
        # [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19...]
        ```

        ### Límite de registros retornados para paginación
        También es posible establecer una cantidad máxima en la cantidad de IDs
        retornada por el API, también directamente desde el API. Suponiendo que
        una búsqueda normal arrojarría los siguientes resultados:
        ```py
        odoo.search_read("sale.order", [("state", "=", "sale")])
        # [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14...]
        ```

        Se puede especificar que sólo se requiere obtener una cantidad máxima
        de registros a partir de un número provisto:
        ```py
        odoo.search_read("sale.order", [("state", "=", "sale")], limit=5)
        # [1, 2, 3, 4, 5]
        ```

        ----
        ### Sugerencia de uso en múltiples condiciones
        Para mejorar y facilitar una búsqueda con múltiples condiciones
        se recomienda asignar la lista a una variable y utilizar la siguiente
        estructura de identación dentro de la lista:
        ```py
        # Ejemplo 1
        criteria = [
            '&',
                (condicion_1...),
                (condicion_2...)
        ]

        # Ejemplo 2
        criteria = [
            '|',
                '&',
                    (condicion_1...),
                    (condicion_2...),
                (condicion_3...)
        ]
        ```

        En el primer ejemplo, la condición 1 y la condición 2 deben
        cumplirse para que un registro se incluya en la lista de resultados.

        En el segundo ejemplo, el resultado `True` de la condición 1 y la
        condición 2, todo esto o la condición 3 deben cumplirse para que un registro se
        incluya en la lista de resultados, es decir, algo como esto:
        ```py
        ((condicion_1 and condicion_2) or condicion_3)
        ```

        La ejecución del método entonces se vería así:
        ```py
        odoo.search_read("sale.order", criteria)
        ```

        ### Sugerencia de uso en listas de campos muy grandes
        Para estos casos, se recomienda almacenar la lista de campos en una
        variable con alguna de las siguientes dos estructuras:

        ```py
        # Ejemplo 1
        fields = ['name', 'state', 'salesman_id', 'partner_id']

        # Ejemplo 2
        fields = [
            'name',
            'state',
            'salesman_id',
            'partner_id'
        ]
        ```

        La ejecución del método entonces se vería así:
        ```py
        odoo.read("sale.order", [52, 87, 129, 132], fields)
        ```
        """
        
        # Ejecución del método de solicitud al API
        return self._request(
            model= model,
            method= "search_read",
            data= self._build_data(data),
            params= self._build_params(
                fields= fields,
                offset= offset,
                limit= limit
            )
        )
    

    # ----- MÉTODO DE CONTEO DE UNA BÚSQUEDA -----
    def search_count(
        self,
        model: str,
        data: list[tuple, str]
    ) -> int:
        """
        ## Método de conteo de una búsqueda
        Este método devuelve el conteo de la cantidad de registros que cumplen
        un criterio de búsqueda provisto. Es equivalente a usar la función
        `len()` a la lista de retorno del método `OdooAPIManager.search()`:
        ```py
        len(odoo.search("sale.order", [("state", "=", "cancel")]))
        ```

        Ejemplo de uso:
        ```py
        odoo.search_count("sale.order", [("state", "=", "cancel")])
        # 87
        ```

        ### Condición de búsqueda
        Se provee al menos una tupla con la siguiente escructura:
        ```py
        ("nombre_del_campo", "=", "valor")
        ```

        Todo esto se encierra dentro de una lista:
        ```py
        [("nombre_del_campo", "=", "valor")]
        ```

        Los operadores de comparación pueden ser alguno de los siguientes:
        - `=`: Igual a
        - `!=`: Diferente de
        - `in`: Está en
        - `not in`: No está en
        - `ilike`: Contiene
        - `not ilike`: No contiene
        - `>`: Es mayor a
        - `<`: Es menor a
        - `>=`: Es mayor o igual a
        - `<=`: Es menor o igual a

        Los valores pueden ser de tipo `str`, `int`, `float` o `bool`
        dependiendo del tipo de valor del campo.

        También pueden ser de tipo `list` que contenga alguno de los tipos
        anteriores.

        En relaciones tipo `many2one`, `one2many` y `many2many` la búsqueda se
        hace por ID o por lista de IDs.

        ### Búsqueda con varias condiciones provistas
        También se puede incluir más de una condición. Para esto, se agrega un
        operador lógico antes de dos tuplas de condiciones:
        ```py
        ['&', (condicion_1...), (condicion_2...)]
        ```

        Los operadores lógicos disponibles son:
        - `&`: and
        - `|`: or

        ----
        ### Sugerencia de uso en múltiples condiciones
        Para mejorar y facilitar una búsqueda con múltiples condiciones
        se recomienda asignar la lista a una variable y utilizar la siguiente
        estructura de identación dentro de la lista:
        ```py
        # Ejemplo 1
        criteria = [
            '&',
                (condicion_1...),
                (condicion_2...)
        ]

        # Ejemplo 2
        criteria = [
            '|',
                '&',
                    (condicion_1...),
                    (condicion_2...),
                (condicion_3...)
        ]
        """

        return self._request(
            model= model,
            method= "search_count",
            data= self._build_data(
                data= data
            ),
            params= self._build_params()
        )


    # ----- MÉTODO INTERNO DE ESCRITURA -----
    def _write_single_record(
        self,
        model: str,
        record_id: list[dict],
        changes_data: dict[str, int, bool]
    ):
        return self._request(
            model= model,
            method= "write",
            data = self._build_data(
                data= record_id,
                data_args= changes_data,
            )
        )


    # ----- SOLICITUD AL API -----
    def _request(
        self,
        model: str,
        method: Literal["check_access_rights", "search", "search_read", "search_count", "read", "create", "write", "unlink"],
        data: list[tuple, str, list, dict],
        params: dict[list, int] ={}
    ):
        # Se realiza la solicitud al API
        return self._models.execute_kw(
            # Base de datos de la API
            self._api_db,
            # UUID de la cuenta
            self._uid,
            # Token de la cuenta
            self._api_token,
            # Modelo de Odoo
            model,
            # Método de solicitud
            method,
            # Matriz de argumentos
            data,
            # Diccionario de argumentos
            params
        )


    def _build_data(
        self,
        data: list[tuple, str, int],
        data_args: dict = None
    ):
        
        if data_args:
            return [data, data_args]
        else:
            return [data]
        

    # ----- CONSTRUIR EL DICCIONARIO DE ARGUMENTOS OPCIONALES -----
    def _build_params(
        self,
        fields: list[str] = None,
        offset: int = None,
        limit: int = None,
        raise_exception: bool = None
    ) -> dict[list[str], int]:
        """
        ## Método interno para la conversión de parámetros individuales en un
        diccionario con llaves opcionales. Retorna un diccionario vacío en caso
        de llamarlo sin proporcionarle argumentos.
        """
        
        # Se inicializa la variable como diccionario vacío
        params = dict()

        # Campos
        if fields:
            params["fields"] = fields
        # Desfase inicial
        if offset:
            params["offset"] = offset
        # Límite de resultados arrojados
        if limit:
            params["limit"] = limit
        # Arrojar error
        if not raise_exception is None:
            params["raise_exception"] = raise_exception

        # Retorno del diccionario construído
        return params


class _DataMethods():
    """
    ## Extensión de la clase `OdooAPIManager`
    Subclase para el uso de métodos relacionados con la consulta de datos
    para su análisis o consulta.
    """
    def __init__(self, _instance: OdooAPIManager):
        self._instance = _instance

    def get_dataset(
        self,
        model: str,
        search_criteria: list[tuple, str],
        fields: list,
        separate_many2one: bool= True,
        many2one_ids_only: bool= False
    ) -> pd.DataFrame:
        """
        ## Obtener un conjunto de datos desde el API de Odoo
        Este método realiza una solicitud `search_read` al API de Odoo
        y crea un DataFrame con el JSON recibido. Este método recibe los mismos
        parámetros que el método antes mencionado.

        Este método también separa las referencias de los campos `many2one`
        de `[5, 'nombre_del_registro']` a `5` y `'nombre_del_registro'` y
        asigna los valores a las columnas renombradas.

        Por ejemplo, si se especifica un `user_id`, se creará un DataFrame que
        no sólo contenga la columna `user_id` con los valores en fomato
        `[5, 'nombre_usuario']` sino que se crearán dos columnas llamadas
        `user_id` y `user_name` con los valores `5` y `'nombre_usuario'. Este
        comportamiento se puede desactivar especificando el parámetro
        `separate_many2one` con el valor `False`.

        ### Ejemplo de uso
        ```py
        odoo.data.get_data_set("sale.order", [("state", "=", "cancel")], ["name", "user_id", "state"])
        #    id       name        user_id     user_name   state
        # 0  3        S00003      3           moderador  cancel
        # 1  14       S00014      7           usuario 4  cancel
        # 2  25       S00025      7           usuario 4  cancel
        # 3  27       S00027      3           moderador  cancel
        # 4  48       S00048      5           moderador  cancel
        ```

        ### Condición de búsqueda
        Se provee al menos una tupla con la siguiente escructura:
        ```py
        ("nombre_del_campo", "=", "valor")
        ```

        Todo esto se encierra dentro de una lista:
        ```py
        [("nombre_del_campo", "=", "valor")]
        ```

        Los operadores de comparación pueden ser alguno de los siguientes:
        - `=`: Igual a
        - `!=`: Diferente de
        - `in`: Está en
        - `not in`: No está en
        - `ilike`: Contiene
        - `not ilike`: No contiene
        - `>`: Es mayor a
        - `<`: Es menor a
        - `>=`: Es mayor o igual a
        - `<=`: Es menor o igual a

        Los valores pueden ser de tipo `str`, `int`, `float` o `bool`
        dependiendo del tipo de valor del campo.

        También pueden ser de tipo `list` que contenga alguno de los tipos
        anteriores.

        En relaciones tipo `many2one`, `one2many` y `many2many` la búsqueda se
        hace por ID o por lista de IDs.

        ### Búsqueda con varias condiciones provistas
        También se puede incluir más de una condición. Para esto, se agrega un
        operador lógico antes de dos tuplas de condiciones:
        ```py
        ['&', (condicion_1...), (condicion_2...)]
        ```

        Los operadores lógicos disponibles son:
        - `&`: and
        - `|`: or

        ### Campos a retornar por el API
        A diferencia del método `search_read` en el que los campos a obtener
        del modelo se pueden o no incluir como argumento de la llamada a éste,
        en el método `get_dataset` este parámetro es obligatorio. Para conocer
        cuáles son los campos disponibles en el modelo, se puede utilizar el
        método `OdooAPIManager.data.model_fields`:
        ```py
        odoo.data.get_dataset(... ['name', 'state'])
        ```

        ----
        ### Sugerencia de uso en múltiples condiciones
        Para mejorar y facilitar una búsqueda con múltiples condiciones
        se recomienda asignar la lista a una variable y utilizar la siguiente
        estructura de identación dentro de la lista:
        ```py
        # Ejemplo 1
        criteria = [
            '&',
                (condicion_1...),
                (condicion_2...)
        ]

        # Ejemplo 2
        criteria = [
            '|',
                '&',
                    (condicion_1...),
                    (condicion_2...),
                (condicion_3...)
        ]
        ```

        En el primer ejemplo, la condición 1 y la condición 2 deben
        cumplirse para que un registro se incluya en la lista de resultados.

        En el segundo ejemplo, el resultado `True` de la condición 1 y la
        condición 2, todo esto o la condición 3 deben cumplirse para que un registro se
        incluya en la lista de resultados, es decir, algo como esto:
        ```py
        ((condicion_1 and condicion_2) or condicion_3)
        ```

        La ejecución del método entonces se vería así:
        ```py
        odoo.data.get_dataset("sale.order", criteria, [...])
        ```

        ### Sugerencia de uso en listas de campos muy grandes
        Para estos casos, se recomienda almacenar la lista de campos en una
        variable con alguna de las siguientes dos estructuras:

        ```py
        # Ejemplo 1
        fields = ['name', 'state', 'salesman_id', 'partner_id']

        # Ejemplo 2
        fields = [
            'name',
            'state',
            'salesman_id',
            'partner_id'
        ]
        ```

        La ejecución del método entonces se vería así:
        ```py
        odoo.data.get_dataset("sale.order", [(...)], fields)
        ```
        """

        # Se obtiene el JSON de la búsqueda
        data = self._instance.search_read(model, search_criteria, fields)
        # Se obtiene un DataFrame de los campos del modelo
        model_fields = self.model_fields(model)
        # Se crea el DataFrame base de los datos del JSON
        df = pd.DataFrame(data)

        # Obtener los subcampos de las referencias many2one
        if separate_many2one:

            # Se crea la lista de columnas ordenadas que ya inclue el ID como primera columna
            columns = ['id']

            # Se itera cada uno de los campos provistos en la función
            for field in fields:

                # Se valida si el campo es de tipo many2one
                if model_fields[model_fields["name"] == field].reset_index().at[0, "ttype"] == "many2one":
                    
                    # Se obtienen los nombres de las columnas derivados del nombre del campo
                    df[field] = df[field].apply(self._extract_id_from_list)

                    # Se valida si sólo se requieren las IDs de los campos many2one
                    if not many2one_ids_only:
                        # Se crea un DataFrame con los IDs y los nombres de los valores de las referencias
                        (df_map, new_columns) = self._map_dataframe_ref(data, field)

                        # Se añaden estas columnas a la lista para mantener un orden
                        for i in new_columns:
                            columns.append(i)
                    
                        # Se fusiona el DataFrame base con estos registros mapeados a la ID del campo many2one
                        df = pd.merge(left=df, right=df_map, how='left')

                    else:
                        columns.append(field)

                # De no tratarse de un campo many2one no se hace nada y sólo se añade la columna
                else:
                    columns.append(field)
            # Al final del for se devuelve el DataFrame con las columnas ordenadas
            return df[columns]
        
        else:
            # Si no se activó el parámetro para sólo mantener las IDs de las referencias
            if many2one_ids_only:
                # Se extrae la ID de las listas de referencia many2one
                df[field] = df[field].apply(self._extract_id_from_list)

            # Se devuelve el DataFrame sin manipular en caso de que se haya desactivado el parámetro de separación de referencias
            return df

    def model_fields(
        self,
        model: str,
        atts: list[str] = [
                'name',
                'field_description',
                'model_id',
                'ttype',
                'state',
                'relation'
            ]
    ) -> pd.DataFrame:
        data = self._instance.search_read(
            model= "ir.model.fields",
            data= [('model_id', '=', model)],
            fields= atts
        )

        return pd.DataFrame(data)
    
    def _map_dataframe_ref(self, data, field):
        """"
        ## Mapeo de referencias múltiples
        Método interno para mapear una referencia de un DataFrame en un
        subconjunto de datos perteneciente a una columna de un conjunto principal.
        """
        new_columns = self._rename_df_column_ref(field)
        df_map = pd.DataFrame(
            [byte[field] if byte[field] else [np.nan, np.nan] for byte in data],
            columns= new_columns
        )
        df_map = df_map.drop_duplicates(field)
        return df_map, new_columns

    def _rename_df_column_ref(self, reference: str) -> list:
        end = reference.find("_id")
        reference = reference[:end]
        return [reference + "_id", reference + "_name"]
    
    def _extract_id_from_list(self, sample: list | np.number) -> int | np.number:
        if sample:
            return sample[0]
        else:
            return sample

class _FixMethods():
    """
    ## Extensión de la clase `OdooAPIManager`
    Subclase para el uso de métodos relacionados con la corrección
    preestablecida para excepciones en procesos administrativos dentro de Odoo.
    """

    def __init__(self, _instance: OdooAPIManager):
        self._instance = _instance

    # ----- CIERRE DE CICLO DE VIDA EN ESTADOS DE ÓRDENES DE VENTA -----
    def close_sale_order_status(
        self,
        record_id: int
    ):
        execution = self._instance._write_single_record(
            model= "sale.order",
            record_id= record_id,
            changes_data= {
                "invoice_status": "invoiced"
            }
        )

        if execution:
            return self._instance.read(
                model= "sale.order",
                record_ids= [record_id],
                fields= ["invoice_status"]
            )
        else:
            "No fue posible actualizar el registro"

class _ModelsMethods():
    """
    ## Submódulo para desencadenar acciones de modelos
    """

    _actions = {
        "sale.order": {
            "confirm": "action_confirm"
        },
        "account.move": {
            "update_payments": "l10n_mx_edi_cfdi_invoice_try_update_payments"
        }
    }

    def __init__(self, _instance: OdooAPIManager):
        self._instance = _instance

    def sale_order_exec(self, method: Literal["confirm"], record_id: int):
        actions = self._actions["sale.order"]
        self._instance._request("sale.order", actions[method], [[record_id]])

    def account_move_exec(self, method: Literal["update_payments"], record_id: int):
        actions = self._actions["account.move"]
        try:
            self._instance._request("account.move", actions[method], [[record_id]])
        except:
            pass