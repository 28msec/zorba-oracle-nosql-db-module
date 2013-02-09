How to run the tests
--------------------

1. Install Oracle NoSQL DB

Download Oracle NoSQL Database, Community Edition from: 
http://www.oracle.com/technetwork/products/nosqldb/downloads/index.html

This was tested with kv-1.2.123.

Install it and run it using:

java -jar lib/kvstore.jar kvlite -root ./kvroot -store kvstore -host 127.0.0.1 -port 5000 -admin 5001


2. Try out the tests, for example:

./bin/zorba -i --trailing-nl -f -q ../../zorba_modules/nosqldb/test/all-json.xq

