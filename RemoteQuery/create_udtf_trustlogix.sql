CREATE OR REPLACE SECURE FUNCTION sec_udtf_sql_api_oauth_v3(sql string,account_name string, wh_name string, role_name string, user_name string)
RETURNS TABLE (col variant)
LANGUAGE PYTHON
RUNTIME_VERSION = 3.10
HANDLER = 'handler_class'
EXTERNAL_ACCESS_INTEGRATIONS = (**REPLACE_ME**)
SECRETS = ('cred' = **REPLACE_ME**)
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

    def process(self, sql, account_name, wh_name, role_name, user_name):
        self.apiurl='https://'+account_name+'.snowflakecomputing.com/api/v2/statements/'
        sql_text = f'select object_construct(*) from ({sql});'
        statements = "set APP_USER='" + user_name + "';" + sql_text;
        
        body =  {
            'statement': statements,
            'timeout': self.timeout,
            'warehouse': wh_name.upper(),
            'role': role_name.upper(),
            "parameters": {
                "MULTI_STATEMENT_COUNT": 2
                }
            }

        header = {
                "Authorization":  "Bearer " + self.token,
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Snowflake-Account": account_name,
                "X-Snowflake-Authorization-Token-Type": "OAUTH",
            }
        session = requests.Session()
        response = session.post(self.apiurl, json=body, headers=header)
        responseJson = response.json()
        requestId = responseJson['requestId']
        statementHandle = responseJson["statementHandles"][1]
        statementResponseUrl = self.apiurl + statementHandle +'?requestId=' + requestId
        statementResponse = session.get(statementResponseUrl, json = body, headers = header)
        
        p=0
        data = []
        pageCount = int(len(statementResponse.json()['resultSetMetaData']['partitionInfo']))-1
        
        while p <= pageCount:
            url2 = self.apiurl+statementHandle+'?partition='+str(p)
            data.extend(requests.get(url2, json=body, headers=header, verify=False).json()['data'])
            
            p+=1
            
        df = pd.DataFrame(data)
        df = df.itertuples(index=False, name=None)
         
        return df
$$;
