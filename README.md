## SilverTail Data Parser  

Spark Job for production use  
Build with Scala 2.11 and Spark 2.1  
To test run "sbt test"

### Input Schema
timestamp, 
field, 
value

### Output Schema (so far)
timestamp,
method,
page,
host,
jsession,
clientIp,
serverIp,
clientPort,
serverPort,
customerEmail,
om_req_id,
accountNumber,
data,
atteshr