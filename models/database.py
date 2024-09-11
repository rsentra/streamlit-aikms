import streamlit as st
import psycopg2
import datetime
import pandas as pd
# import cx_Oracle
import oracledb

def get_conn_dbms(conn_name=None):
    if conn_name is None:
        conn_name = st.secrets["connections_dbms"]["conn_name"]
    # env_name = st.secrets["connections_dbms"]["env_name"]
    if "oracle" in conn_name:
        username = st.secrets[conn_name]["username"]
        password = st.secrets[conn_name]["password"]
        dsn = f'{st.secrets[conn_name]["host"]}:{st.secrets[conn_name]["port"]}/{st.secrets[conn_name]["database"]}'
        encoding = st.secrets[conn_name]["encoding"]

        # 오라클 클라이언트를 선언: windows 환경변수에 path를 잡으면 필요없음
        # if env_name == 'pc-driver':
        #     try:
        #         cx_Oracle.init_oracle_client(lib_dir=r"C:\oracle\instantclient_23_4")
        #     except Exception as e:
        #         pass

        try:
            # connection = cx_Oracle.connect(username, password, dsn, encoding=encoding)
            connection = oracledb.connect(user=username, password=password, dsn=dsn)
        except Exception as ex:
            print('Could not connect to database:', ex)
            return ex

        print("SUCCESS: Connecting oracle succeeded")
        return connection
    
    if "postgresql" in conn_name:
        host = st.secrets[conn_name]["host"]
        database = st.secrets[conn_name]["database"]
        username = st.secrets[conn_name]["username"]
        password = st.secrets[conn_name]["password"]
        
        try:
            connection = psycopg2.connect(host=host, database=database, user=username, password=password)
        except Exception as ex:
            print('Could not connect to database:', ex)
            return ex
        print("SUCCESS: Connecting postgresql succeeded")
        return connection

def get_conn_postgres():
    conn_name = "connections_postgresql"
    return get_conn_dbms(conn_name=conn_name)

def get_conn_ora():
    conn_name = "connections_oracle"
    return get_conn_dbms(conn_name=conn_name)

# @st.cache_data
def get_kms_datadf(sql):

    try:
        connection = get_conn_dbms()
     
        with connection.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
            cols = [x[0].lower() for x in cursor.description]
   
        df = pd.DataFrame(rows, columns = cols)
        for c in df.columns:
            if df[c].dtype == object:
               df[c] = df[c].astype("string")

        connection.close()

    except Exception as ex:
        err, = ex.args
        print('error code :', err.code)
        print('error message :', err.message)
        return None
   
    if len(df) ==0:
        print('No data found')  
        return None

    return df

# 오라클 데이터(사내용)
@st.cache_data
def get_kms_datadf_ora(sql):

    try:
        connection = get_conn_ora()
     
        with connection.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
            cols = [x[0].lower() for x in cursor.description]
   
        df = pd.DataFrame(rows, columns = cols)
        for c in df.columns:
            if df[c].dtype == object:
               df[c] = df[c].astype("string")

        connection.close()

    except Exception as ex:
        err, = ex.args
        print('error code :', err.code)
        print('error message :', err.message)
        return None
   
    if len(df) ==0:
        print('No data found')  
        return None

    return df

@st.cache_data
def get_common_code(cd):
    sql = f""" SELECT k10.cd_nm, k10.cd
                    FROM tbctkc10 k10
                    where k10.group_cd = '{cd}'
                    and use_yn = 'Y' and cd <> '****'
                    ORDER BY K10.SORT_ORD
                """
    try:
        connection = get_conn_dbms()
     
        with connection.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
            cols = [x[0].lower() for x in cursor.description]
   
        df = pd.DataFrame(rows, columns = cols)
   
        connection.close()

    except Exception as ex:
        err, = ex.args
        print('error code :', err.code)
        print('error message :', err.message)
        return None
   
    if len(df) ==0:
        print('No data found')  
        return None

    return df

def insert_df_to_table(df, table, mode='insert', repl_cond=None):
        """
        Using cursor.executemany() to insert the dataframe
        """
        #접속db를 읽어옴
        conn_name = st.secrets["connections_dbms"]["conn_name"]
    
        # Create a list of tupples from the dataframe values
        tuples = list(set([tuple(x) for x in df.to_numpy()]))
        # Comma-separated dataframe columns
        cols = ','.join(list(df.columns))
        # table = table.upper()
        # cols = cols.upper()

        query = f"INSERT INTO {table} ({cols}) VALUES ({ (',%s'*len(df.columns))[1:] })" 
        if "oracle" in conn_name: #오라클 처리
            s =''
            for i in range(len(df.columns)):
                s = s + ':' +str(i+1) + ',' 
            query = f"INSERT INTO {table} ({cols}) VALUES ( {s[:-1] })"

        print('insert_df_to_table ',conn_name, '=>', query)

       # replace이면 기존 데이터 삭제
        if mode=='replace':
            del_sql = f"""delete from {table} where {repl_cond}"""
            if repl_cond == None:
                return False
            
        try:
            conn = get_conn_dbms()
            
            with conn.cursor() as cur:
                if mode=='replace':
                    cur.execute(del_sql)
                if len(df) > 0:
                    cur.executemany(query, tuples)
                conn.commit()

            conn.close()

        except (Exception) as error:
            print("Error: %s" % error)
            conn.rollback()
            conn.close()
            return False    
        return True

def insert_hashtag(room_id, room_seq, df07, schema):
        """
        Using cursor.executemany() to insert the dataframe
        """
        try:
            conn = get_conn_dbms()
            
            with conn.cursor() as cur:
                # tag 입력
                for tag_name in df07.tag_name:
                    # tbctrg07
                    query = f""" MERGE INTO {schema}.TBCTRG07
                       USING DUAL ON ( tag_name = '{tag_name}')
                        WHEN NOT MATCHED THEN
                         INSERT (tag_name) 
                         VALUES ('{tag_name}')"""
                    # print('insert_tag TBCTRG07 =>', query)    
                    cur.execute(query)

                    # tbctrg06 =>
                    query = f""" INSERT INTO {schema}.TBCTRG06 (room_id, room_seq, tag_id)
                                values ({room_id}, {room_seq}, (SELECT tag_id FROM {schema}.TBCTRG07 WHERE tag_name = '{tag_name}'))"""
                    # print('insert_tag TBCTRG06 =>', query)    
                    cur.execute(query)  

                conn.commit()

            conn.close()

        except (Exception) as error:
            print("Error: %s" % error)
            conn.rollback()
            conn.close()
            return False    
        return True