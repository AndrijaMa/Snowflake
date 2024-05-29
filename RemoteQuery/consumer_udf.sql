CREATE OR REPLACE SECURE FUNCTION sec_udtf_sql_api_oauth(sql string)
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
        self.account_name='REPLACE_ME'
        self.apiurl='https://'+self.account_name+'.snowflakecomputing.com/api/v2/statements/'
        self.timeout=60
        self.wh_name = 'REPLACE_ME'
        self.role_name = 'REPLACE_ME'
          
    def process(self, sql):
        self.sql_text = f'select object_construct(*) from ({sql});'
        
        jsonBody =  {
            'statement': self.sql_text,
            'timeout': self.timeout,
            'warehouse': self.wh_name.upper(),
            'role': self.role_name.upper(),
            }

        header = {
                "Authorization":  "Bearer " + self.token,
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Snowflake-Account": self.account_name,
                "X-Snowflake-Authorization-Token-Type": "OAUTH"
            }
        
        response = requests.post(self.apiurl, json=jsonBody, headers=header, verify=False)
        handle = response.json()['statementHandle']    
        
        p=0
        data = []
        pcount = int(len(response.json()['resultSetMetaData']['partitionInfo']))-1
        
        while p <= pcount:
            url2 = self.apiurl+handle+'?partition='+str(p)
            data.extend(requests.get(url2, json=jsonBody, headers=header, verify=False).json()['data'])
            
            p+=1
            
        df = pd.DataFrame(data)
        df = df.itertuples(index=False, name=None)
         
        return df
$$;
