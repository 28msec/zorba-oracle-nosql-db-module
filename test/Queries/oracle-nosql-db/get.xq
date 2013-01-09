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
  
  variable $v := "Value for Mkey2/Mkey21-mk2";
  
  variable $ts := nosql:put-text($db, $key1, $v  );
  variable $valueVersion := nosql:get-text($db, $key1);

  (: nosql:disconnect($db); :)
  
  ( $v eq $valueVersion("value"), fn:exists($valueVersion("version")) )
}

