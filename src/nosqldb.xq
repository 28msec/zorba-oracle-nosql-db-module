xquery version "3.0";
(:
 : Copyright 2006-2012 The FLWOR Foundation.
 :
 : Licensed under the Apache License, Version 2.0 (the "License");
 : you may not use this file except in compliance with the License.
 : You may obtain a copy of the License at
 :
 : http://www.apache.org/licenses/LICENSE-2.0
 :
 : Unless required by applicable law or agreed to in writing, software
 : distributed under the License is distributed on an "AS IS" BASIS,
 : WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 : See the License for the specific language governing permissions and
 : limitations under the License.
 :)


(:~
 : This module provides minimal funtionality to interact with an <a href="http://www.oracle.com/technetwork/products/nosqldb/overview/index.html">Oracle NoSQL Database</a>.
 :
 : The kvclient library is used to implement these functions. Set the NOSQLDB_HOME environment variable to use this module.
 : <br />
 : <br />
 : <br /><b>Note:</b> Since this module has a Java library dependency a JVM required
 : to be installed on the system. For Windows: jvm.dll is required on the system
 : path ( usually located in "C:\Program Files\Java\jre6\bin\client".
 :
 : @author Cezar Andrei
 :)
module namespace nosql = "http://www.zorba-xquery.com/modules/nosqldb";

(:~
 : Import module for encoding/decoding base64Binary to/from string.
 :)
import module namespace base64 = "http://www.zorba-xquery.com/modules/converters/base64";
import module namespace jn = "http://jsoniq.org/functions";

declare namespace an = "http://www.zorba-xquery.com/annotations";
declare namespace ver = "http://www.zorba-xquery.com/options/versioning";

declare option ver:module-version "1.0";




(:~
 : Connect to a NoSQL Database KVStore
 : @return the function has side-effects and returns an identifier for a connection to the KVStore
 :)
declare %an:sequential function
nosql:connect( $options as object() ) as xs:anyURI 
{
  let $store-name := $options("store-name")
  let $helper-host-ports := $options("helper-host-ports")
  let $hhps as xs:string* := jn:members($helper-host-ports)
  return 
    if( fn:exists($store-name) and fn:exists($hhps) ) then 
      nosql:connect-internal($store-name, $hhps)
    else
      fn:error(xs:QName("nosql:ERROR001"), "Invalid $options parameter.")
};

declare %private %an:sequential function
nosql:connect-internal($store-name as xs:string, $helperHostPorts as xs:string+ ) as xs:anyURI external;


(:~
 : Disconnect from a KVStore
 : @param $db the KVStore reference
 : @return the function has side-effects and returns the empty sequence
 :)
declare %an:sequential function
nosql:disconnect($db as xs:anyURI) as empty-sequence() external;

(:~
 : Checks if $db is a valid KVStore reference
 :
 : @param $db the KVStore reference
 : @return true if the KVStore reference is valid, false otherwise
 
declare %an:sequential function 
nosql:is-connected($db as xs:anyURI) as xs:boolean external;:)

(:~
 : Get the value as base64Binary and version associated with the key.
 : Ex:  <pre>{ "value":"value as base64Binary", "version":"xs:long" }
 :
 : @param $db the KVStore reference
 : @param $key the key used to lookup the key/value pair. 
 : @return the value and version associated with the key, or empty sequence if no associated value was found. 
 
declare %an:sequential function 
nosql:get($db as xs:anyURI, $key as xs:string) as value-version external;:)

(:~
 : Get the value as string and version associated with the key.
 : Ex:  <pre>{ "value":"value as string", "version":"xs:long" }
 :
 : @param $db the KVStore reference
 : @param $key the key used to lookup the key/value pair. 
 : @return the value and version associated with the key, or empty sequence if no associated value was found. 
 
declare %an:sequential function 
nosql:getStringValue($db as xs:anyURI, $key as xs:string) as value-version external;:)

(:~
 : Put a key/value pair, inserting or overwriting as appropriate.
 :
 : @param $db the KVStore reference
 : @param $key the key used to lookup the key/value pair. 
 : @param $value the value part of the key/value pair. 
 : @return the version of the new value. 
 
declare %an:sequential function 
nosql:put($db as xs:anyURI, $key as xs:string, $value as xs:base64Binary) as xs:long external;:)

(:~
 : Put a key/value pair, inserting or overwriting as appropriate, where value is a string.
 :
 : @param $db the KVStore reference
 : @param $key the key used to lookup the key/value pair. 
 : @param $value the value part of the key/value pair as a string. 
 : @return the version of the new value. 
 
declare %an:sequential function 
nosql:putStringValue($db as xs:anyURI, $key as xs:string, $value as xs:string) as xs:long external;:)

(:~
 : Delete the key/value pair associated with the key. 
 :
 : @param $db the KVStore reference
 : @param $key the key used to lookup the key/value pair. 
 : @return true if the delete is successful, or false if no existing value is present.
 
declare %an:sequential function 
nosql:delete($db as xs:anyURI, $key as xs:string) as xs:boolean external;:)


(:-------------------------------------------------------
  NoSQL DB - specific API
  -------------------------------------------------------:)

(:~
 : Put a key/value pair, inserting or overwriting as appropriate.
 :
 : @param $db the KVStore reference
 : @param $key the key used to lookup the key/value pair. 
 : { 
 :    "major": ["major-key1","major-key1","major-key1"],
 :    "minor": ["minor-key1","minor-key1","minor-key1"] 
 : } 
 : @param $value the value part of the key/value pair. 
 : @return the version of the new value. 
 :) 
declare %an:sequential function 
nosql:put-base64($db as xs:anyURI, $key as object(), $value as xs:base64Binary) as xs:long external;

(:~
 : Put a key/value pair, inserting or overwriting as appropriate.
 :
 : @param $db the KVStore reference
 : @param $key the key used to lookup the key/value pair. 
 : { 
 :    "major": ["major-key1","major-key2","major-key3"],
 :    "minor": ["minor-key1","minor-key2","minor-key3"] 
 : } 
 : @param $value the value part of the key/value pair as a string.
 : @return the version of the new value. 
 :) 
declare %an:sequential function 
nosql:put-string($db as xs:anyURI, $key as object(), $stringValue as xs:string) as xs:long 
{
  nosql:put-base64($db, $key, base64:encode($stringValue))
};

(:~
 : Put a key/value pair, inserting or overwriting as appropriate.
 :
 : @param $db the KVStore reference
 : @param $key the key used to lookup the key/value pair. 
 : { 
 :    "major": ["major-key1","major-key2","major-key3"],
 :    "minor": ["minor-key1","minor-key2","minor-key3"] 
 : } 
 : @param $value the value part of the key/value pair as a json object 
 : @return the version of the new value. 
 :) 
declare %an:sequential function 
nosql:put-json($db as xs:anyURI, $key as object(), $jsonValue as object() ) as xs:long 
{
  let $stringValue := fn:serialize(jn:encode-for-roundtrip($jsonValue))
  return
    nosql:put-base64($db, $key, trace(base64:encode($stringValue), "put base64"))
};

(:~
 : Get the value as base64Binary and version associated with the key.
 : Ex:  <pre>{ "value":"value as base64Binary", "version":"xs:long" }
 :
 : @param $db the KVStore reference
 : @param $key the key used to lookup the key/value pair. 
 : @return the value and version associated with the key, or 
 :         empty sequence if no associated value was found. 
 :)
declare %an:sequential function 
nosql:get-base64($db as xs:anyURI, $key as object() ) as object()? external;

(:~
 : Get the value as string and version associated with the key.
 : Ex:  <pre>{ "value":"value as string", "version":"xs:long" }
 :
 : @param $db the KVStore reference
 : @param $key the key used to lookup the key/value pair. 
 : @return the value and version associated with the key, or 
 :         empty sequence if no associated value was found. 
 :)
declare %an:sequential function 
nosql:get-string($db as xs:anyURI, $key as object() ) as object()? 
{
  let $r := nosql:get-base64($db, $key)
  return 
    if ( fn:exists($r) ) 
    then
      { 
        "value"  : { base64:decode(trace($r("value"), "get base64")) } ,
        "version": { $r("version") }
      }
    else
      ()
};


(:~
 : Get the value as a json object and version associated with the key.
 : Ex:  <pre>{ "value":"value as string", "version":"xs:long" }
 :
 : @param $db the KVStore reference
 : @param $key the key used to lookup the key/value pair. 
 : @return the value and version associated with the key, or 
 :         empty sequence if no associated value was found. 
 :)
declare %an:sequential function 
nosql:get-json($db as xs:anyURI, $key as object() ) as object()? 
{
  let $r := nosql:get-base64($db, $key)
  return 
    if ( fn:exists($r) ) 
    then
      { 
        "value"  : { jn:parse-json(base64:decode($r("value"))) } , 
        "version": { $r("version") }
      }
    else
      ()
};


(:~
 : Delete the key/value pair associated with the key. 
 :
 : @param $db the KVStore reference
 : @param $key the key used to lookup the key/value pair. 
 : @return true if the delete is successful, or false if no existing value is present.
 :)
declare %an:sequential function 
nosql:delete($db as xs:anyURI, $key as object() ) as xs:boolean external;

