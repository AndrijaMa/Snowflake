CREATE OR REPLACE SECURE FUNCTION sec_udtf_sql_api_oauth(sql string, wh_name string, role_name string, account_name string)
RETURNS TABLE (col variant)
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'handler_class'
EXTERNAL_ACCESS_INTEGRATIONS = (REPLACE_ME)
SECRETS = ('cred' = REPLACE_ME)
PACKAGES = ( 'pandas', 'requests')

AS $$
import _snowflake
import pandas as pd
import requests

class handler_class:
    def __init__(self):
        self.engine = None
        self.connection = None
        self.token = _snowflake.get_oauth_access_token('cred')
        self.timeout=60
          
    def process(self, sql, wh_name, role_name, account_name):
        
        apiurl='https://'+account_name+'.snowflakecomputing.com/api/v2/statements/'
        sql_text = f'select object_construct(*) from ({sql});'
        
        jsonBody =  {
            'statement': sql_text,
            'timeout': self.timeout,
            'warehouse': wh_name.upper(),
            'role': role_name.upper(),
            }

        header = {
                "Authorization":  "Bearer " + self.token,
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Snowflake-Account": account_name,
                "X-Snowflake-Authorization-Token-Type": "OAUTH"
            }
        
        response = requests.post(apiurl, json=jsonBody, headers=header, verify=False)
        handle = response.json()['statementHandle']    
        
        p=0
        data = []
        pcount = int(len(response.json()['resultSetMetaData']['partitionInfo']))-1
        
        while p <= pcount:
            url2 = apiurl+handle+'?partition='+str(p)
            data.extend(requests.get(url2, json=jsonBody, headers=header, verify=False).json()['data'])
            
            p+=1
            
        df = pd.DataFrame(data)
        df = df.itertuples(index=False, name=None)
         
        return df
$$;
