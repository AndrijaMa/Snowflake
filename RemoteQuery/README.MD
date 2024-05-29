<H2>Data PRODUCER </H2><br>
<br>
<H3>Snowflake Account in Azure </H3><br>
<br>
<H3>Step P1: Register the Client</H3><br>
CREATE SECURITY INTEGRATION MY_SNOWSQL_CLIENT<br>
TYPE = OAUTH<br>
ENABLED = TRUE<br>
OAUTH_CLIENT = CUSTOM<br>
OAUTH_CLIENT_TYPE = 'CONFIDENTIAL'<br>
OAUTH_REDIRECT_URI = 'https://localhost'<br>
OAUTH_ISSUE_REFRESH_TOKENS = TRUE<br>
OAUTH_REFRESH_TOKEN_VALIDITY = 86400<br>
;<br>

<a href="https://community.snowflake.com/s/article/HOW-TO-OAUTH-TOKEN-GENERATION-USING-SNOWFLAKE-CUSTOM-OAUTH#Register-the-Client">Documentation</a><br>
<br>
<H3>Step P2: Gather details for client configuration</H3><br>
Describe the above Security Integration and note down the below details, they will be used in the following steps:</br>
OAUTH_CLIENT_ID</br>
OAUTH_REDIRECT_URI</br>
OAUTH_AUTHORIZATION_ENDPOINT</br>
OAUTH_TOKEN_ENDPOINT</br>
</br>
SELECT SYSTEM$SHOW_OAUTH_CLIENT_SECRETS('MY_SEC_INT');<br>
<br>
Step P3:<br>
COPY OAUTH_CLIENT_SECRET<br> 
COPY OAUTH_CLIENT_ID<br>
<br>
<H4>Create a new refresh token</H4><br>
<a href="https://community.snowflake.com/s/article/HOW-TO-OAUTH-TOKEN-GENERATION-USING-SNOWFLAKE-CUSTOM-OAUTH">Documentation</a><br>
Step T1 <br>
Run the steps in the Notebook<br>
<a href="url">Documentation</a><br>
<br>
Step T2<br> 
Run the CURL command to generate the Refresh Token<br>
Copy Refresh-token<br>
<br>
<br><H2>DATA CONSUMER</H2>
<br>Snowflake AWS Ireland
<br>Step C1
<br>CREATE OR REPLACE SECURITY INTEGRATION 
<a href="url">Documentation</a><br>
<br>
<br>REPLACE OAUTH_CLIENT_ID from US
<br>REPLACE OAUTH_CLIENT_SECRET from US

<br>Step  C2
<br>CREATE OR REPLACE SECRET
<br>Replace OAUTH_REFRESH_TOKEN from the command prompt

<br>Step C3
<br>CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION
<br>Run again

<br><H3>Token Refresh</H3>
<br>When you need to replace a expired token
<br>Run steps on the 4,5 & 7 to refresh a expired token
