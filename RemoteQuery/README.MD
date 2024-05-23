PRODUCER <br>
<br>
Snowflake Azure US<br>
<br>
Step P1:<br>
CREATE or replace SECURITY INTEGRATION <br>
<br>
Step P2:<br>
SELECT SYSTEM$SHOW_OAUTH_CLIENT_SECRETS('EU_DEMO162_SEC_INT');<br>
<br>
Step P3:<br>
COPY OAUTH_CLIENT_SECRET<br> 
COPY OAUTH_CLIENT_ID<br>
<br>
Create a new refresh token<br>
Step T1 <br>
Run the steps in the Notebook<br>
<br>
Step T2<br> 
Run the CURL command to generate the Refresh Token<br>
Copy Refresh-token<br>

CONSUMER
Snowflake AWS Ireland
Step C1
CREATE OR REPLACE SECURITY INTEGRATION 

REPLACE OAUTH_CLIENT_ID from US
REPLACE OAUTH_CLIENT_SECRET from US

Step  C2
CREATE OR REPLACE SECRET
Replace OAUTH_REFRESH_TOKEN from the command prompt

Step C3
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION
Run again

Token Refresh
When you need to replace a expired token
Run steps on the 4,5 & 7 to refresh a expired token



SECRET = Snow_remote_token
EXTERNAl NETWORk INTEGRATION Snow_access_int
snow_access_rules
