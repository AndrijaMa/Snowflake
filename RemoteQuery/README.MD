<H2>Data PRODUCER </H2><br>
<br>
<H3>Snowflake Account in Azure </H3><br>
<br>
Step P1:<br>
CREATE or replace SECURITY INTEGRATION MY_SEC_INT<br>
<a href="url">Documentation</a><br>
<br>
Step P2:<br>
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
