import module namespace nosql = "http://www.zorba-xquery.com/modules/oracle-nosqldb";
import module namespace base64 = "http://zorba.io/modules/base64";

{
  variable $opt := {
                     "store-name" : "kvstore",
                     "helper-host-ports" : ["localhost:5000"]
                   };
                   
  variable $db := nosql:connect( $opt);
  
  variable $key1 := {
        "major": ["putkey2", "putkey21"], 
        "minor":["mk2"]
      };
  
  variable $ts := nosql:put-text($db, $key1, "Value for putkey2/putkey21-mk2" );

  (: nosql:disconnect($db); :)
  
  fn:exists($ts)
}

