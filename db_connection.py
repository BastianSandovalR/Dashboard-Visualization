import psycopg2
import pandas as pd

def get_connection():
    conn = psycopg2.connect(
        host="127.0.0.1",
        port="5432",
        database="natalidad",
        user="postgres",
        password="123456789"
    )
    return conn

def query_to_df(query, params=None):
    """
    Ejecuta una query SQL y retorna un DataFrame de pandas

    Args:
        query (str): Consulta SQL (puede tener placeholders %s)
        params (tuple/list, optional): Parámetros para la consulta

    Returns:
        pd.DataFrame: DataFrame con los resultados
    """
    conn = None
    try:
        conn = get_connection()

        if params:
            # Ejecutar con parámetros
            df = pd.read_sql_query(query, conn, params=params)
        else:
            # Ejecutar sin parámetros
            df = pd.read_sql_query(query, conn)

        return df

    except Exception as e:
        print(f"❌ Error en query_to_df:")
        print(f"   Query: {query[:200]}...")
        if params:
            print(f"   Params: {params}")
        print(f"   Error: {str(e)}")
        return pd.DataFrame()  # Retornar DataFrame vacío en caso de error

    finally:
        if conn:
            conn.close()