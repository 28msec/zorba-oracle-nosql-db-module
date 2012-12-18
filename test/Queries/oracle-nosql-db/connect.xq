import module namespace nosql = "http://www.zorba-xquery.com/modules/oracle-nosqldb";

{
  variable $opt := {
                     "store-name" : "kvstore",
                     "helper-host-ports" : ["localhost:5000"]
                   };
                   
  variable $db := nosql:connect( $opt);
  
  nosql:disconnect($db);
  
  fn:exists($db)
}
