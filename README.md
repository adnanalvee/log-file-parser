## Log file parser

Parses a semi structured log file.
>         1, REQUEST, method=POST&page=bigcompany.com,
		1, HEADERS, host=opus&dhost=kjl&user-agent=ipad,
		1, BARS, JSESSIONID=XyMsdsd!dshtml&eshr=12345,
		1, TCP, clientIp=1&serverIp=2&clientPort=3&serverPort=4,
		1, PARAMS, accountNumber=123&_om_req_id=&emailId=example@bigcompany.com&notes=fraud,
		1, DISCO, lots_of_stuff,
		1, AGENT, windows,
		2, REQUEST, Adnan

### How to run
1. Make sure you got sbt
2. Run the unit tests using `sbt test`
3. Build the jar to run on your cluster using `sbt build`
