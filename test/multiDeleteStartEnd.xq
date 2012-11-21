import module namespace nosql = "http://www.zorba-xquery.com/modules/nosqldb";
import module namespace base64 = "http://www.zorba-xquery.com/modules/converters/base64";

{
  variable $opt := {
                     "store-name" : "kvstore",
                     "helper-host-ports" : ["localhost:5000"]
                   };
                   
  variable $db := nosql:connect( $opt);
  
  variable $key1 := {
        "major": ["M1", "M2"], 
        "minor":["m1"]
      };
  
  variable $key2 := {
        "major": ["M1", "M2"], 
        "minor":["m2"]
      };
      
  variable $key31 := {
        "major": ["M1", "M2"], 
        "minor":["m3", "m31"]
      };

  variable $key32 := {
        "major": ["M1", "M2"], 
        "minor":["m3", "m32"]
      };

  variable $key4 := {
        "major": ["M1", "M2"], 
        "minor":["m4", "a", "b"]
      };

  nosql:put-string($db, $key1, "V m1" );
  nosql:put-string($db, $key2, "V m2" );
  nosql:put-string($db, $key31, "V m31" );
  nosql:put-string($db, $key32, "V m32" );
  nosql:put-string($db, $key4, "V m4 a b" );

  variable $parentKey := {"major": ["M1", "M2"] };

  variable $md1 := nosql:multi-delete-values($db, $parentKey, { "start" : "a", "end": "b" }, "PARENT_AND_DESCENDANTS");

  variable $g1 := nosql:get-string($db, $key1);

  variable $md2 := nosql:multi-delete-values($db, $parentKey, { "start" : "m3", "end": "m4" }, "PARENT_AND_DESCENDANTS");

  variable $g2 := nosql:get-string($db, $key1);

  variable $md3 := nosql:multi-delete-values($db, $parentKey, { "start" : "a", "end" : "z" }, "PARENT_AND_DESCENDANTS");

  variable $g3 := nosql:get-string($db, $key1);


  nosql:disconnect($db);

  { 
    "md1": {$md1}, 
    "g1": {$g1},
    "md2": {$md2}, 
    "g2": {$g2},
    "md3": {$md3}, 
    "g3": {$g3}
  }
}

