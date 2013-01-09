import module namespace nosql = "http://www.zorba-xquery.com/modules/oracle-nosqldb";
import module namespace base64 = "http://www.zorba-xquery.com/modules/converters/base64";

{
  variable $opt := {
                     "store-name" : "kvstore",
                     "helper-host-ports" : ["localhost:5000"]
                   };
                   
  variable $db := nosql:connect( $opt);
  
  variable $key1 := {
        "major": ["Mkey2", "Mkey21"], 
        "minor":["mk2"]
      };
  
  variable $ts := nosql:put-text($db, $key1, "Value for Mkey2/Mkey21-mk2" );
  variable $valueVersion := nosql:get-text($db, $key1);
  variable $delRes := nosql:remove($db, $key1);

  (: nosql:disconnect($db); :)

  $delRes
}

