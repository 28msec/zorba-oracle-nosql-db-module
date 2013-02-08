import module namespace nosql = "http://www.zorba-xquery.com/modules/oracle-nosqldb";
import module namespace base64 = "http://www.zorba-xquery.com/modules/converters/base64";

{
  variable $opt := {
                     "store-name" : "kvstore",
                     "helper-host-ports" : ["localhost:5000"]
                   };
                   
  variable $db := nosql:connect( $opt);
  
  variable $key1 := {
        "major": ["a-bkey1a", "a-bkey1b"], 
        "minor":["mk"]
      };
  
  variable $key2 := { 
        "major": ["a-bkey2a", "a-bkey2b"]
        , 
        "minor":[null] 
      };
      
  variable $val1 as xs:base64Binary := base64:encode("Value for a-bkey1a/a-bkey1b-mk");  
  variable $ts1 := nosql:put-binary($db, $key1, $val1 );
  variable $valueVer1 := nosql:get-binary($db, $key1);
  
  variable $val2 as xs:base64Binary := base64:encode("Value for key2");
  variable $ts2 := nosql:put-binary($db, $key2, $val2 );
  variable $valueVer21 := nosql:get-binary($db, $key2);
  variable $delRes2 := nosql:remove($db, $key2);
  variable $valueVer22 := nosql:get-binary($db, $key2);

  (: nosql:disconnect($db); :)

  { 
    "db" : {fn:exists($db)}, 
    "put1 version": {fn:exists($ts1)}, 
    "get1" : { $valueVer1("value") },
    "get1 value as string" : { base64:decode($valueVer1("value")) },
    
    "put2": {fn:exists($ts2)},
    "get2": { $valueVer21("value") },
    "get2 value as string" : { base64:decode($valueVer21("value")) },
    "del2": {$delRes2},
    "get2 again": {$valueVer22}
  }
}

