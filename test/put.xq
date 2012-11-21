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
  
  variable $ts := nosql:put-string($db, $key1, "Value for Mkey2/Mkey21-mk2" );

  nosql:disconnect($db);
}

