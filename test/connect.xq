import module namespace nosql = "http://www.zorba-xquery.com/modules/nosqldb";

{
  variable $opt := {
                     "store-name" : "kvstore",
                     "helper-host-ports" : ["localhost:5000"]
                   };
                   
  variable $db := nosql:connect( $opt);
  
  variable $isConnected1 := nosql:is-connected($db);
  
  nosql:disconnect($db);
  
  variable $isConnected2 := nosql:is-connected($db);
  
  ( $isConnected1, $isConnected2 )
}
