import mysql.connector
import logging
import pandas as pd
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DBConnector:

    def __init__(self):
        logging.info('Initialize DB Connector')
        self.connectorCode = "DB"
        self.config = {
        'user': 'admin',
        'password': '2^EO5*5ns*C&',
        'host': 'database-crossing-record.cmifnwj8nmy1.ap-northeast-1.rds.amazonaws.com',
        'database': 'market_feed_config',
        'raise_on_warnings': True
        }



    def initial_connection_cursor(self):
        conn = mysql.connector.connect(**self.config)
        return conn ,conn.cursor(buffered=True)

    def GET(self,table_name,column=None, condition=None):

        if column:
            column_str = ','.join(column)
        else:
            column_str = '*'
        logger.info(condition)
        if condition:
            tmp = []
            for item in condition.keys():
                tmp.append(f"{item} = '{condition[item]}'")
            tmp_str = ' AND '.join(tmp)
            condition_str = f'WHERE {tmp_str}'

        else:
            condition_str = ''

        query = f'SELECT {column_str} FROM {table_name} {condition_str}'
        logger.info(query)
        conn , cursor = self._execute(query)
        ret = pd.DataFrame(cursor.fetchall())
        logger.info(ret)
        return ret

    def UPDATE(self,table_name,value:dict,condition:dict):
        set_tmp = []
        condition_tmp = []
        for item in value.keys():
            set_tmp.append(f"{item}='{value[item]}'")

        for item in condition.keys():
            condition_tmp.append(f"{item} = '{condition[item]}'")

        set_str = ','.join(set_tmp)
        tmp_str = ' AND '.join(condition_tmp)
        condition_str = f'WHERE {tmp_str}'
        query = f'UPDATE {table_name} SET {set_str} {condition_str}'
        logger.info(query)
        conn, cursor = self._execute(query)
        conn.close()

    def INSERT(self,table_name,data_dict):
        #retrieve data
        column_str,value_str = self.form_column(data_dict)

        query = f'INSERT INTO {table_name} ' \
                f'{column_str} VALUES {value_str}' #(example,example)
        conn , cursor = self._execute(query,data_dict)
        conn.close()


    def DELETE(self,table_name:str,condition:dict):
        if condition is None:
            raise Exception
            return None
        tmp = []
        for item in condition.keys():
            tmp.append(f"{item} = '{condition[item]}'")
        tmp_str = ' AND '.join(tmp)
        condition_str = f'WHERE {tmp_str}'

        query = f'DELETE FROM {table_name} {condition_str}'
        conn , cursor = self._execute(query)
        conn.close()

    def form_column(self,data_dict:dict):
        column_key = data_dict.keys()
        column = ','.join(column_key)
        ret_column = f'({column})'
        tmp = []
        for item in column_key:
            tmp.append(f'%({item})s')
        value = ','.join(tmp)
        ret_value = f'({value})'
        return ret_column , ret_value

    def CUSTOM(self,query):
        conn , cursor = self._execute(query)
        ret = pd.DataFrame(cursor.fetchall())
        conn.close()
        return ret


    def _execute(self,query,value_dict=()):
        try:

            conn , cursor = self.initial_connection_cursor()
            cursor.execute(query,value_dict)
            conn.commit()
            return  conn , cursor
            #conn.close()
        except Exception as e:
            logger.error(e)
