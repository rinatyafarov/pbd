import oracledb

# ================================================================
# Настройки подключения к Oracle
# Замените на ваши данные
# ================================================================

DB_USER = "system"
DB_PASSWORD = "toor"
DB_DSN = "localhost:1521/XEPDB1"  # host:port/service_name

# Глобальный пул соединений (опционально, для производительности)
connection_pool = None


def init_pool(min_connections=1, max_connections=5):
    """Инициализирует пул соединений (для продакшена)"""
    global connection_pool
    try:
        connection_pool = oracledb.create_pool(
            user=DB_USER,
            password=DB_PASSWORD,
            dsn=DB_DSN,
            min=min_connections,
            max=max_connections,
            increment=1,
            getmode=oracledb.POOL_GETMODE_WAIT
        )
        print(f"Connection pool initialized with {min_connections}-{max_connections} connections")
    except Exception as e:
        print(f"Failed to initialize connection pool: {e}")
        connection_pool = None


def get_connection():
    """Возвращает новое соединение с базой данных."""
    if connection_pool:
        return connection_pool.acquire()
    return oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN)


def execute_procedure(proc_name, params=None):
    """
    Вызывает хранимую процедуру Oracle.
    params -- список параметров, например [1, 'Easy']
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            if params:
                cur.callproc(proc_name, params)
            else:
                cur.callproc(proc_name)
            conn.commit()


def fetch_all(query, params=None):
    """
    Выполняет SELECT и возвращает список словарей.
    Каждый словарь -- одна строка результата.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)

            columns = [col[0].lower() for col in cur.description]
            rows = cur.fetchall()
            return [dict(zip(columns, row)) for row in rows]


def fetch_one(query, params=None):
    """
    Выполняет SELECT и возвращает одну строку как словарь.
    Возвращает None если строк нет.
    """
    results = fetch_all(query, params)
    return results[0] if results else None


def execute_query(query, params=None):
    """
    Выполняет INSERT / UPDATE / DELETE с коммитом.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            conn.commit()


def call_function(func_name, return_type, params=None):
    """
    Вызывает хранимую функцию Oracle и возвращает результат.
    return_type -- тип возвращаемого значения, например oracledb.NUMBER
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            result = cur.var(return_type)
            if params:
                result = cur.callfunc(func_name, return_type, params or [])
            else:
                cur.callproc(func_name, [result])
            return result.getvalue()


def execute_many(query, params_list):
    """
    Выполняет массовую вставку/обновление.
    params_list -- список списков параметров
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(query, params_list)
            conn.commit()


# Инициализируем пул при импорте (опционально)
# init_pool()