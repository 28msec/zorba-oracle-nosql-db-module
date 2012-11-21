import module namespace nosql = "http://www.zorba-xquery.com/modules/oracle-nosqldb";
import module namespace base64 = "http://www.zorba-xquery.com/modules/converters/base64";

{
  variable $opt := {
                     "store-name" : "kvstore",
                     "helper-host-ports" : ["localhost:5000"]
                   };
                   
  variable $db := nosql:connect( $opt);
  
  variable $key1 := {
        "major": ["Mkey1a", "Mkey1b"], 
        "minor":["mk"]
      };
  
      
  variable $val1 as object() := { "a1" : "a value 1" , "b" : 1, "c": [1, "1", jn:null()] };  
  variable $ts1 := nosql:put-json($db, $key1, $val1 );
  variable $valueVer1 := nosql:get-json($db, $key1);
  

  variable $key2 := { "major": "Mkey2a" };

  variable $val2 as object() := { "a2" : "a value 2" , "b" : 2, "c": [2, "222", jn:null()] };
  variable $ts2 := nosql:put-json($db, $key2, $val2 );
  variable $valueVer21 := nosql:get-json($db, $key2);
  variable $delRes2 := nosql:delete-value($db, $key2);
  variable $valueVer22 := nosql:get-json($db, $key2); 

  nosql:disconnect($db);

  { 
    "db" : {$db}, 
    "put1 version": {$ts1}, 
    "get1" : { $valueVer1 },
    "get1 value as string" : { fn:serialize($valueVer1 ("value")) }, 
    
    "put2": {$ts2},
    "get2": {$valueVer21},
    "get2 value as string" : { fn:serialize($valueVer21("value")) },
    "del2": {$delRes2},
    "get2 again": {$valueVer22}
  }
}

