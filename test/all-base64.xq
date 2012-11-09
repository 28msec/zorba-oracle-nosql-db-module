import module namespace nosql = "http://www.zorba-xquery.com/modules/nosqldb";
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
  
  variable $key2 := { 
        "major": ["Mkey2a", "Mkey2b"]
        , 
        "minor":[null] 
      };
      
  variable $val1 as xs:base64Binary := base64:encode("Value for Mkey1a/Mkey1b-mk");  
  variable $ts1 := nosql:put-base64($db, $key1, $val1 );
  variable $valueVer1 := nosql:get-base64($db, $key1);
  
  variable $val2 as xs:base64Binary := base64:encode("Value for key2");
  variable $ts2 := nosql:put-base64($db, $key2, $val2 );
  variable $valueVer21 := nosql:get-base64($db, $key2);
  variable $delRes2 := nosql:delete-value($db, $key2);
  variable $valueVer22 := nosql:get-base64($db, $key2);

  nosql:disconnect($db);

  { 
    "db" : {$db}, 
    "put1 version": {$ts1}, 
    "get1" : { $valueVer1 },
    "get1 value as string" : { base64:decode($valueVer1("value")) },
    
    "put2": {$ts2},
    "get2": {$valueVer21},
    "get2 value as string" : { base64:decode($valueVer21("value")) },
    "del2": {$delRes2},
    "get2 again": {$valueVer22}
  }
}

