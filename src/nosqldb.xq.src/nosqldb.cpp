/*
 * Copyright 2006-2012 The FLWOR Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "nosqldb.h"


namespace zorba
{
namespace nosqldb
{

/*****************************************************************************
 Useful functions
 *****************************************************************************/

String getOneStringArgument(const ExternalFunction::Arguments_t& aArgs, int aPos)
{
  Item lItem;
  Iterator_t args_iter = aArgs[aPos]->getIterator();
  args_iter->open();
  args_iter->next(lItem);
  String lTmpString = lItem.getStringValue();
  args_iter->close();
  return lTmpString;
}

Item getOneItemArgument(const ExternalFunction::Arguments_t& aArgs, int aPos)
{
    Item lItem;
    Iterator_t args_iter = aArgs[aPos]->getIterator();
    args_iter->open();
    args_iter->next(lItem);
    args_iter->close();
    return lItem;
}

Iterator_t getIterArgument(const ExternalFunction::Arguments_t& aArgs, int aPos)
{
  Iterator_t args_iter = aArgs[aPos]->getIterator();
  return args_iter;
}

void
throwError(const char *aLocalName, const char* aErrorMessage)
{
  String errNS(NOSQLDB_MODULE_NAMESPACE);
  String errName(aLocalName);
  Item errQName = NoSqlDBModule::getItemFactory()->createQName(errNS, errName);
  String errDescription(aErrorMessage);
  throw USER_EXCEPTION(errQName, errDescription);
}


/*****************************************************************************
 Method implementations
 *****************************************************************************/

ExternalFunction* NoSqlDBModule::getExternalFunction(const String& localName)
{
  if (localName == "connect-internal")
  {
      return connect;
  }
  else if (localName == "is-connected")
  {
      return isConnected;
  }
  else if (localName == "disconnect")
  {
      return disconnect;
  }
  else if (localName == "put-base64")
  {
      return put;
  }
  else if (localName == "get-base64")
  {
      return get;
  }
  else if (localName == "delete-value")
  {
      return del;
  }

  return 0;
}


// connect code

ItemSequence_t
ConnectFunction::evaluate(const ExternalFunction::Arguments_t& args,
                           const zorba::StaticContext* aStaticContext,
                           const zorba::DynamicContext* aDynamincContext) const
{
  jthrowable lException = 0;
  static JNIEnv* env;

  try
  {
    env = zorba::jvm::JavaVMSingleton::getInstance(aStaticContext)->getEnv();
    Item item;
    std::ostringstream os;

    // read input param 0: $store-name as xs:string
    Iterator_t lIter = args[0]->getIterator();
    lIter->open();
    lIter->next(item);
    lIter->close();
    //   Searialize Item
    SingletonItemSequence lSequence(item);
    Zorba_SerializerOptions_t lOptions;
    lOptions.omit_xml_declaration = ZORBA_OMIT_XML_DECLARATION_YES;
    Serializer_t lSerializer = Serializer::createSerializer(lOptions);
    lSerializer->serialize(&lSequence, os);
    std::string p0String = os.str();
    const char* p0Str = p0String.c_str();
    jstring jStrParam1 = env->NewStringUTF(p0Str);

    // read input param 1: $helperHostPorts as xs:string+
    lIter = args[1]->getIterator();
    lIter->open();
    std::vector<jstring> jstringVec;

    while( lIter->next(item) )
    {
      // Searialize Item
      std::ostringstream os;
      SingletonItemSequence lSequence(item);
      lSerializer->serialize(&lSequence, os);
      std::string p1String = os.str();
      const char* p1Str = p1String.c_str();
      jstringVec.push_back( env->NewStringUTF(p1Str) );
      CHECK_EXCEPTION(env);
    }
    lIter->close();

    // call java to make a new connection

    // String[] hhosts = {"n1.example.org:5088", "n2.example.org:4129"};
    jclass strCls = env->FindClass("Ljava/lang/String;");
    CHECK_EXCEPTION(env);
    jobjectArray jStrArray = env->NewObjectArray(jstringVec.size(), strCls, NULL);
    CHECK_EXCEPTION(env);

    for ( jsize i = 0; i<(jsize)jstringVec.size(); i++)
    {
      env->SetObjectArrayElement(jStrArray, i, jstringVec[i]);
      CHECK_EXCEPTION(env);
      env->DeleteLocalRef((jstring)jstringVec[i]);
      CHECK_EXCEPTION(env);
    }

    // oracle.kv.KVStoreConfig kvsConfigObj = new oracle.kv.KVStoreConfig("storeName", String[] hhosts);
    jclass kvsConfigClass = env->FindClass("oracle/kv/KVStoreConfig");
    CHECK_EXCEPTION(env);
    jmethodID kvsConfigCons = env->GetMethodID(kvsConfigClass, "<init>", "(Ljava/lang/String;[Ljava/lang/String;)V");
    CHECK_EXCEPTION(env);
    jobject kvsConfigObj = env->NewObject(kvsConfigClass, kvsConfigCons, jStrParam1, jStrArray);
    CHECK_EXCEPTION(env);

    //KVStore kvstore = KVStoreFactory.getStore(kvsConfigObj);
    jclass kvsFactoryClass = env->FindClass("oracle/kv/KVStoreFactory");
    CHECK_EXCEPTION(env);
    jmethodID midGetStore = env->GetStaticMethodID(kvsFactoryClass, "getStore", "(Loracle/kv/KVStoreConfig;)Loracle/kv/KVStore;");
    CHECK_EXCEPTION(env);
    jobject kvsObject = env->CallStaticObjectMethod(kvsFactoryClass, midGetStore, kvsConfigObj);
    CHECK_EXCEPTION(env);
    jobject kvsObjRef = env->NewGlobalRef(kvsObject);
    CHECK_EXCEPTION(env);
    env->DeleteLocalRef(kvsObject);
    CHECK_EXCEPTION(env);

    // store connection in dynamic context
    uuid lUUID;
    uuid::create(&lUUID);
    std::stringstream lStream;
    lStream << lUUID;
    String lStrUUID = lStream.str();

    InstanceMap* lInstanceMap;
    DynamicContext* lDctx = const_cast<DynamicContext*>(aDynamincContext);
    if (!(lInstanceMap = dynamic_cast<InstanceMap*>(
              lDctx->getExternalFunctionParameter("nosqldbInstanceMap"))))
    {
      lInstanceMap = new InstanceMap(env);
      lDctx->addExternalFunctionParameter("nosqldbInstanceMap", lInstanceMap);
    }
    lInstanceMap->storeInstance(lStrUUID, kvsObjRef);

    return ItemSequence_t(new SingletonItemSequence(
        NoSqlDBModule::getItemFactory()->createAnyURI(lStrUUID)));
  }
  catch (zorba::jvm::VMOpenException&)
  {
      Item lQName = NoSqlDBModule::getItemFactory()->createQName(NOSQLDB_MODULE_NAMESPACE,
                "VM001");
      throw USER_EXCEPTION(lQName, "Could not start the Java VM (is the classpath set?)");
  }
  catch (JavaException&)
  {
      // prints out to std err the stacktrace
      // env->ExceptionDescribe();
      env->ExceptionClear();

      jclass stringWriterClass = env->FindClass("java/io/StringWriter");
      jclass printWriterClass = env->FindClass("java/io/PrintWriter");
      jclass throwableClass = env->FindClass("java/lang/Throwable");
      jobject stringWriter = env->NewObject(
                stringWriterClass,
                env->GetMethodID(stringWriterClass, "<init>", "()V"));

      jobject printWriter = env->NewObject(
                printWriterClass,
                env->GetMethodID(printWriterClass, "<init>", "(Ljava/io/Writer;)V"),
                stringWriter);

      env->CallObjectMethod(lException,
                env->GetMethodID(throwableClass, "printStackTrace",
                        "(Ljava/io/PrintWriter;)V"),
                printWriter);

//      env->CallObjectMethod(printWriter, env->GetMethodID(printWriterClass, "flush", "()V"));
      jmethodID toStringMethod =
            env->GetMethodID(stringWriterClass, "toString", "()Ljava/lang/String;");
      jobject errorMessageObj = env->CallObjectMethod( stringWriter, toStringMethod);
      jstring errorMessage = (jstring) errorMessageObj;
      const char *errMsg = env->GetStringUTFChars(errorMessage, NULL);
      std::stringstream s;
      s << "A Java Exception was thrown:" << std::endl << errMsg;
      String errDescription;
      errDescription += s.str();
      env->ReleaseStringUTFChars(errorMessage, errMsg);

      Item errQName = NoSqlDBModule::getItemFactory()->createQName(NOSQLDB_MODULE_NAMESPACE,
                "JAVA-EXCEPTION");

      //std::cout << "  java exception:'" << errDescription << "'" << std::endl; std::cout.flush();

      throw USER_EXCEPTION(errQName, errDescription );
  }

  return ItemSequence_t(new EmptySequence());
}


// disconnect code
ItemSequence_t
DisconnectFunction::evaluate(const ExternalFunction::Arguments_t& args,
                           const zorba::StaticContext* aStaticContext,
                           const zorba::DynamicContext* aDynamicContext) const
{
  Iterator_t lIter;

  jthrowable lException = 0;
  static JNIEnv* env;

  try
  {
    env = zorba::jvm::JavaVMSingleton::getInstance(aStaticContext)->getEnv();

    // read input param 0
    String lInstanceID = getOneStringArgument(args, 0);

    InstanceMap* lInstanceMap;
    if (!(lInstanceMap = dynamic_cast<InstanceMap*>(aDynamicContext->getExternalFunctionParameter("nosqldbInstanceMap"))))
    {
      throwError("NoInstanceMatch", "Not a NoSQL DB identifier.");
    }

    jobject kvsObjRef = lInstanceMap->getInstance(lInstanceID);
    if (kvsObjRef)
    {
        // call kvsObjRef.close()
        jclass kvsClass = env->FindClass("oracle/kv/KVStore");
        jmethodID midClose = env->GetMethodID(kvsClass, "close", "()V");
        env->CallVoidMethod(kvsObjRef, midClose);

        env->DeleteGlobalRef(kvsObjRef);

        lInstanceMap->deleteInstance(lInstanceID);
    }
    else
    {
      throwError("NoInstanceMatch", "No instance of NoSQL DB with the given identifier was found.");
    }

    return ItemSequence_t(new EmptySequence());
  }
  catch (zorba::jvm::VMOpenException&)
  {
    Item lQName = NoSqlDBModule::getItemFactory()->createQName(NOSQLDB_MODULE_NAMESPACE,
                                          "VM001");
    throw USER_EXCEPTION(lQName, "Could not start the Java VM (is the classpath set?)");
  }
  catch (JavaException&)
  {
    // prints out to std err the stacktrace
    // env->ExceptionDescribe();

    jclass stringWriterClass = env->FindClass("java/io/StringWriter");
    jclass printWriterClass = env->FindClass("java/io/PrintWriter");
    jclass throwableClass = env->FindClass("java/lang/Throwable");
    jobject stringWriter = env->NewObject(
              stringWriterClass,
              env->GetMethodID(stringWriterClass, "<init>", "()V"));

    jobject printWriter = env->NewObject(
              printWriterClass,
              env->GetMethodID(printWriterClass, "<init>", "(Ljava/io/Writer;)V"),
              stringWriter);

    env->CallObjectMethod(lException,
              env->GetMethodID(throwableClass, "printStackTrace",
                      "(Ljava/io/PrintWriter;)V"),
              printWriter);

    //env->CallObjectMethod(printWriter, env->GetMethodID(printWriterClass, "flush", "()V"));
    jmethodID toStringMethod =
          env->GetMethodID(stringWriterClass, "toString", "()Ljava/lang/String;");
    jobject errorMessageObj = env->CallObjectMethod(
              stringWriter, toStringMethod);
    jstring errorMessage = (jstring) errorMessageObj;
    const char *errMsg = env->GetStringUTFChars(errorMessage, 0);
    std::stringstream s;
    s << "A Java Exception was thrown:" << std::endl << errMsg;
    env->ReleaseStringUTFChars(errorMessage, errMsg);
    std::string err("");
    err += s.str();
    env->ExceptionClear();
    Item lQName = NoSqlDBModule::getItemFactory()->createQName(NOSQLDB_MODULE_NAMESPACE,
              "JAVA-EXCEPTION");
    throw USER_EXCEPTION(lQName, err);
  }

  return ItemSequence_t(new EmptySequence());
}


// is-connected code
ItemSequence_t
IsConnectedFunction::evaluate(const ExternalFunction::Arguments_t& args,
                           const zorba::StaticContext* aStaticContext,
                           const zorba::DynamicContext* aDynamicContext) const
{
    bool isConnected = false;

    // read input param 0
    String lInstanceID = getOneStringArgument(args, 0);

    InstanceMap* lInstanceMap;
    if (!(lInstanceMap = dynamic_cast<InstanceMap*>(aDynamicContext->getExternalFunctionParameter("nosqldbInstanceMap"))))
    {
      isConnected = false;
    }
    else
    {
        jobject kvsObjRef = lInstanceMap->getInstance(lInstanceID);
        if (kvsObjRef)
        {
            isConnected = true;
        }
        else
        {
            isConnected = false;
        }
    }

    return ItemSequence_t(new SingletonItemSequence(
        NoSqlDBModule::getItemFactory()->createBoolean(isConnected)));
}


// put code

ItemSequence_t
PutFunction::evaluate(const ExternalFunction::Arguments_t& args,
                           const zorba::StaticContext* aStaticContext,
                           const zorba::DynamicContext* aDynamicContext) const
{
  jthrowable lException = 0;
  static JNIEnv* env;

  try
  {
    env = zorba::jvm::JavaVMSingleton::getInstance(aStaticContext)->getEnv();

    // read input param 0
    String lInstanceID = getOneStringArgument(args, 0);

    InstanceMap* lInstanceMap;
    if (!(lInstanceMap = dynamic_cast<InstanceMap*>(aDynamicContext->getExternalFunctionParameter("nosqldbInstanceMap"))))
    {
      throwError("NoInstanceMatch", "Not a NoSQL DB identifier.");
    }

    jobject kvsObjRef = lInstanceMap->getInstance(lInstanceID);
    if (!kvsObjRef)
    {
        throwError("NoInstanceMatch", "No instance of NoSQL DB with the given identifier was found.");
    }

    // read input param 1
    Item keyParam = getOneItemArgument(args, 1);
    if(!keyParam.isJSONItem())
      throwError("NoSQLDBModuleError", "$key param must be a JSON object");

    // read input param 2
    Item valueItem = getOneItemArgument(args, 2);
    std::string valueString;  // will always contain the straight/decoded value

    if (valueItem.isStreamable())
    {
      std::istream& lStream = valueItem.getStream();
      bool lDecoderAttached = false;

      if (valueItem.isEncoded())
      {
        base64::attach(lStream);
        lDecoderAttached = true;
      }

      std::istreambuf_iterator<char> eos;
      valueString = std::string(std::istreambuf_iterator<char>(lStream), eos);

      if (lDecoderAttached)
      {
        base64::detach(lStream);
      }
    }
    else
    {
      size_t lSize;
      const char* lMsg = valueItem.getBase64BinaryValue(lSize);
      if (valueItem.isEncoded())
      {
          String lTmpEncoded(lMsg, lSize);
          valueString = encoding::Base64::decode(lTmpEncoded).c_str();
      }
      else
      {
          valueString = std::string(lMsg, lSize);
      }
    }

    //    List majorList = new ArrayList();
    jclass arrayListClass = env->FindClass("java/util/ArrayList");
    CHECK_EXCEPTION(env);
    jmethodID midALCons = env->GetMethodID(arrayListClass, "<init>", "()V");
    CHECK_EXCEPTION(env);
    jmethodID midALAdd = env->GetMethodID(arrayListClass, "add", "(Ljava/lang/Object;)Z");
    CHECK_EXCEPTION(env);
    jobject majorList = env->NewObject(arrayListClass, midALCons);
    CHECK_EXCEPTION(env);
    //    List minorList = new ArrayList();
    jobject minorList = env->NewObject(arrayListClass, midALCons);
    CHECK_EXCEPTION(env);

    //    majorList.add("Mk1");
    Item majorValues = keyParam.getObjectValue("major");

    if ( majorValues.isNull() )
    {
        throwError("NoInstanceMatch", "JSON 'major' property must be specified as string or array.");
    }
    else if ( majorValues.isJSONItem() &&
         majorValues.getJSONItemKind() == store::StoreConsts::jsonArray )
    {
        uint64_t majorValueSize = majorValues.getArraySize();
        for (uint64_t i = 1; i<=majorValueSize; i++)
        {
           Item mkItem = majorValues.getArrayValue(i);
           if ( !mkItem.isAtomic() )
               throwError("NoInstanceMatch", "JSON 'major' property must be a string or an array.");
           String mk = mkItem.getStringValue();
           jstring jStrMk = env->NewStringUTF(mk.c_str());
           CHECK_EXCEPTION(env);
           env->CallBooleanMethod(majorList, midALAdd, jStrMk);
           CHECK_EXCEPTION(env);
        }
    }
    else if ( majorValues.isAtomic() )
    {
        String mk = majorValues.getStringValue();
        jstring jStrMk = env->NewStringUTF(mk.c_str());
        CHECK_EXCEPTION(env);
        env->CallBooleanMethod(majorList, midALAdd, jStrMk);
        CHECK_EXCEPTION(env);
    }
    else
        throwError("NoInstanceMatch", "JSON 'major' property must be specified as string or array.");

    //    minorList.add("mk1");
    Item minorValues = keyParam.getObjectValue("minor");

    if ( minorValues.isNull() )
    {
        // do nothing it's perfectly fine to have "minor" missing
    }
    else if ( minorValues.isJSONItem() &&
         minorValues.getJSONItemKind() == store::StoreConsts::jsonArray )
    {
        uint64_t minorValueSize = minorValues.getArraySize();
        for (uint64_t i = 1; i<=minorValueSize; i++)
        {
           Item mkItem = minorValues.getArrayValue(i);
           if ( !mkItem.isAtomic() )
              throwError("NoInstanceMatch", "JSON 'minor' property, if specified, must be a string or an array.");
           String mk = mkItem.getStringValue();
           jstring jStrMk = env->NewStringUTF(mk.c_str());
           CHECK_EXCEPTION(env);
           env->CallBooleanMethod(minorList, midALAdd, jStrMk);
           CHECK_EXCEPTION(env);
        }
    }
    else if ( minorValues.isAtomic() )
    {
        String mk = minorValues.getStringValue();
        jstring jStrMk = env->NewStringUTF(mk.c_str());
        CHECK_EXCEPTION(env);
        env->CallBooleanMethod(majorList, midALAdd, jStrMk);
        CHECK_EXCEPTION(env);
    }
    else
        throwError("NoInstanceMatch", "JSON 'minor' property, if specified, must be a string or an array.");

    //    Key k = Key.createKey(majorList, minorList);
    jclass keyClass = env->FindClass("oracle/kv/Key");
    CHECK_EXCEPTION(env);
    jmethodID midKeyCreate = env->GetStaticMethodID(keyClass, "createKey", "(Ljava/util/List;Ljava/util/List;)Loracle/kv/Key;");
    CHECK_EXCEPTION(env);
    jobject k = env->CallStaticObjectMethod(keyClass, midKeyCreate, majorList, minorList);
    CHECK_EXCEPTION(env);

    //    Value v = Value.createValue(p.getBytes())
    jclass valueClass = env->FindClass("oracle/kv/Value");
    CHECK_EXCEPTION(env);
    jmethodID midValueCreate = env->GetStaticMethodID(valueClass, "createValue", "([B)Loracle/kv/Value;");
    CHECK_EXCEPTION(env);

    //fill out the byte[]
    const char * buf = valueString.c_str();
    jsize bufSize = valueString.size();
    jbyteArray jbyteArrayValue = env->NewByteArray(bufSize);
    CHECK_EXCEPTION(env);

    env->SetByteArrayRegion(jbyteArrayValue, 0, bufSize, (jbyte *)buf);
    CHECK_EXCEPTION(env);

    jobject v = env->CallStaticObjectMethod(valueClass, midValueCreate, jbyteArrayValue);
    CHECK_EXCEPTION(env);

    //    Version version = store.put(k, v);
    jclass kvsClass = env->FindClass("oracle/kv/KVStore");
    CHECK_EXCEPTION(env);
    jmethodID midkvsPut = env->GetMethodID(kvsClass, "put", "(Loracle/kv/Key;Loracle/kv/Value;)Loracle/kv/Version;");
    CHECK_EXCEPTION(env);
    jobject version = env->CallObjectMethod(kvsObjRef, midkvsPut, k, v);
    CHECK_EXCEPTION(env);

    //    long versionLong = version.getVersion();
    jclass versionClass = env->FindClass("oracle/kv/Version");
    CHECK_EXCEPTION(env);
    jmethodID midVersionGetVerion = env->GetMethodID(versionClass, "getVersion", "()J");
    CHECK_EXCEPTION(env);
    jlong versionLong = env->CallLongMethod(version, midVersionGetVerion);
    CHECK_EXCEPTION(env);

    return ItemSequence_t(new SingletonItemSequence(
        NoSqlDBModule::getItemFactory()->createLong(versionLong)));
  }
  catch (zorba::jvm::VMOpenException&)
  {
      Item lQName = NoSqlDBModule::getItemFactory()->createQName(NOSQLDB_MODULE_NAMESPACE,
                "VM001");
      throw USER_EXCEPTION(lQName, "Could not start the Java VM (is the classpath set?)");
  }
  catch (JavaException&)
  {
      // prints out to std err the stacktrace
      // env->ExceptionDescribe();

      jclass stringWriterClass = env->FindClass("java/io/StringWriter");
      jclass printWriterClass = env->FindClass("java/io/PrintWriter");
      jclass throwableClass = env->FindClass("java/lang/Throwable");
      jobject stringWriter = env->NewObject(
                stringWriterClass,
                env->GetMethodID(stringWriterClass, "<init>", "()V"));

      jobject printWriter = env->NewObject(
                printWriterClass,
                env->GetMethodID(printWriterClass, "<init>", "(Ljava/io/Writer;)V"),
                stringWriter);

      env->CallObjectMethod(lException,
                env->GetMethodID(throwableClass, "printStackTrace",
                        "(Ljava/io/PrintWriter;)V"),
                printWriter);

//      env->CallObjectMethod(printWriter, env->GetMethodID(printWriterClass, "flush", "()V"));
      jmethodID toStringMethod =
            env->GetMethodID(stringWriterClass, "toString", "()Ljava/lang/String;");
      jobject errorMessageObj = env->CallObjectMethod( stringWriter, toStringMethod);
      jstring errorMessage = (jstring) errorMessageObj;
      const char *errMsg = env->GetStringUTFChars(errorMessage, NULL);
      std::stringstream s;
      s << "A Java Exception was thrown:" << std::endl << errMsg;
      String errDescription;
      errDescription += s.str();
      env->ExceptionClear();
      env->ReleaseStringUTFChars(errorMessage, errMsg);

      Item errQName = NoSqlDBModule::getItemFactory()->createQName(NOSQLDB_MODULE_NAMESPACE,
                "JAVA-EXCEPTION");

      //std::cout << "  java exception:'" << errDescription << "'" << std::endl; std::cout.flush();

      throw USER_EXCEPTION(errQName, errDescription );
  }
}


ItemSequence_t
GetFunction::evaluate(const ExternalFunction::Arguments_t& args,
                           const zorba::StaticContext* aStaticContext,
                           const zorba::DynamicContext* aDynamicContext) const
{
    jthrowable lException = 0;
    static JNIEnv* env;

    try
    {
      env = zorba::jvm::JavaVMSingleton::getInstance(aStaticContext)->getEnv();

      // read input param 0
      String lInstanceID = getOneStringArgument(args, 0);

      InstanceMap* lInstanceMap;
      if (!(lInstanceMap = dynamic_cast<InstanceMap*>(aDynamicContext->getExternalFunctionParameter("nosqldbInstanceMap"))))
      {
        throwError("NoInstanceMatch", "Not a NoSQL DB identifier.");
      }

      jobject kvsObjRef = lInstanceMap->getInstance(lInstanceID);
      if (!kvsObjRef)
      {
          throwError("NoInstanceMatch", "No instance of NoSQL DB with the given identifier was found.");
      }

      // read input param 1
      Item keyParam = getOneItemArgument(args, 1);
      if(!keyParam.isJSONItem())
        throwError("NoSQLDBModuleError", "$key param must be a JSON object");

      //    List majorList = new ArrayList();
      jclass arrayListClass = env->FindClass("java/util/ArrayList");
      CHECK_EXCEPTION(env);
      jmethodID midALCons = env->GetMethodID(arrayListClass, "<init>", "()V");
      CHECK_EXCEPTION(env);
      jmethodID midALAdd = env->GetMethodID(arrayListClass, "add", "(Ljava/lang/Object;)Z");
      CHECK_EXCEPTION(env);
      jobject majorList = env->NewObject(arrayListClass, midALCons);
      CHECK_EXCEPTION(env);
      //    List minorList = new ArrayList();
      jobject minorList = env->NewObject(arrayListClass, midALCons);
      CHECK_EXCEPTION(env);

      //    majorList.add("Mk1");
      Item majorValues = keyParam.getObjectValue("major");

      if ( majorValues.isNull() )
      {
          throwError("NoInstanceMatch", "JSON 'major' property must be specified as string or array.");
      }
      else if ( majorValues.isJSONItem() &&
           majorValues.getJSONItemKind() == store::StoreConsts::jsonArray )
      {
          uint64_t majorValueSize = majorValues.getArraySize();
          for (uint64_t i = 1; i<=majorValueSize; i++)
          {
             Item mkItem = majorValues.getArrayValue(i);
             if ( !mkItem.isAtomic() )
                 throwError("NoInstanceMatch", "JSON 'major' property must be a string or an array.");
             String mk = mkItem.getStringValue();
             jstring jStrMk = env->NewStringUTF(mk.c_str());
             CHECK_EXCEPTION(env);
             env->CallBooleanMethod(majorList, midALAdd, jStrMk);
             CHECK_EXCEPTION(env);
          }
      }
      else if ( majorValues.isAtomic() )
      {
          String mk = majorValues.getStringValue();
          jstring jStrMk = env->NewStringUTF(mk.c_str());
          CHECK_EXCEPTION(env);
          env->CallBooleanMethod(majorList, midALAdd, jStrMk);
          CHECK_EXCEPTION(env);
      }
      else
          throwError("NoInstanceMatch", "JSON 'major' property must be specified as string or array.");


      //    minorList.add("mk1");
      Item minorValues = keyParam.getObjectValue("minor");

      if ( minorValues.isNull() )
      {
          // do nothing it's perfectly fine to have "minor" missing
      }
      else if ( minorValues.isJSONItem() &&
           minorValues.getJSONItemKind() == store::StoreConsts::jsonArray )
      {
          uint64_t minorValueSize = minorValues.getArraySize();
          for (uint64_t i = 1; i<=minorValueSize; i++)
          {
             Item mkItem = minorValues.getArrayValue(i);
             if ( !mkItem.isAtomic() )
                throwError("NoInstanceMatch", "JSON 'minor' property, if specified, must be a string or an array.");
             String mk = mkItem.getStringValue();
             jstring jStrMk = env->NewStringUTF(mk.c_str());
             CHECK_EXCEPTION(env);
             env->CallBooleanMethod(minorList, midALAdd, jStrMk);
             CHECK_EXCEPTION(env);
          }
      }
      else if ( minorValues.isAtomic() )
      {
          String mk = minorValues.getStringValue();
          jstring jStrMk = env->NewStringUTF(mk.c_str());
          CHECK_EXCEPTION(env);
          env->CallBooleanMethod(majorList, midALAdd, jStrMk);
          CHECK_EXCEPTION(env);
      }
      else
          throwError("NoInstanceMatch", "JSON 'minor' property, if specified, must be a string or an array.");

      //    Key k = Key.createKey(majorList, minorList);
      jclass keyClass = env->FindClass("oracle/kv/Key");
      CHECK_EXCEPTION(env);
      jmethodID midKeyCreate = env->GetStaticMethodID(keyClass, "createKey", "(Ljava/util/List;Ljava/util/List;)Loracle/kv/Key;");
      CHECK_EXCEPTION(env);
      jobject k = env->CallStaticObjectMethod(keyClass, midKeyCreate, majorList, minorList);
      CHECK_EXCEPTION(env);

      //    ValueVersion valueVersion = store.get(k);
      jclass kvsClass = env->FindClass("oracle/kv/KVStore");
      CHECK_EXCEPTION(env);
      jmethodID midkvsGet = env->GetMethodID(kvsClass, "get", "(Loracle/kv/Key;)Loracle/kv/ValueVersion;");
      CHECK_EXCEPTION(env);
      jobject valueVersion = env->CallObjectMethod(kvsObjRef, midkvsGet, k);
      CHECK_EXCEPTION(env);

      // if no result return empty sequence
      if ( valueVersion==NULL )
          return ItemSequence_t(new EmptySequence());

      // Value v = valueVersion.getValue();
      jclass vvClass = env->FindClass("oracle/kv/ValueVersion");
      CHECK_EXCEPTION(env);
      jmethodID midvvGetValue = env->GetMethodID(vvClass, "getValue", "()Loracle/kv/Value;");
      CHECK_EXCEPTION(env);
      jobject v = env->CallObjectMethod(valueVersion, midvvGetValue);
      CHECK_EXCEPTION(env);

      // byte[] value = v.getValue();
      jclass valueClass = env->FindClass("oracle/kv/Value");
      CHECK_EXCEPTION(env);
      jmethodID midValueGetValue = env->GetMethodID(valueClass, "getValue", "()[B");
      CHECK_EXCEPTION(env);
      // todo move the data in the array env->GetByteArrayRegion(ba, off, len, (jbyte *)buf);
      jbyteArray jbaValue = (jbyteArray) env->CallObjectMethod(v, midValueGetValue);
      CHECK_EXCEPTION(env);
      jsize jbaSize = env->GetArrayLength(jbaValue);
      CHECK_EXCEPTION(env);
      char * buf = new char[jbaSize];
      env->GetByteArrayRegion(jbaValue, 0, jbaSize, (jbyte *)buf);
      CHECK_EXCEPTION(env);

      // Version version = valueVersion.getVersion();
      jmethodID midvvGetVersion = env->GetMethodID(vvClass, "getVersion", "()Loracle/kv/Version;");
      CHECK_EXCEPTION(env);
      jobject version = env->CallObjectMethod(valueVersion, midvvGetVersion);
      CHECK_EXCEPTION(env);

      //    long versionLong = version.getVersion();
      jclass versionClass = env->FindClass("oracle/kv/Version");
      CHECK_EXCEPTION(env);
      jmethodID midVersionGetVerion = env->GetMethodID(versionClass, "getVersion", "()J");
      CHECK_EXCEPTION(env);
      jlong versionLong = env->CallLongMethod(version, midVersionGetVerion);
      CHECK_EXCEPTION(env);

      // assemble result { "value" : "the value" , "version" : 123 }
      std::string ssString(buf, jbaSize);
      Item val = NoSqlDBModule::getItemFactory()->createBase64Binary((const unsigned char*)ssString.c_str(), ssString.size());
      Item vers = NoSqlDBModule::getItemFactory()->createLong(versionLong);

      std::vector<std::pair<Item, Item> > pairs;
      pairs.reserve(2);
      pairs.push_back(std::pair<Item, Item>(
        NoSqlDBModule::getItemFactory()->createString(String("value")), val));
      pairs.push_back(std::pair<Item, Item>(
        NoSqlDBModule::getItemFactory()->createString(String("version")), vers));

      Item jsonObj = NoSqlDBModule::getItemFactory()->createJSONObject(pairs);

      return ItemSequence_t(new SingletonItemSequence(jsonObj));
    }
    catch (zorba::jvm::VMOpenException&)
    {
        Item lQName = NoSqlDBModule::getItemFactory()->createQName(NOSQLDB_MODULE_NAMESPACE,
                  "VM001");
        throw USER_EXCEPTION(lQName, "Could not start the Java VM (is the classpath set?)");
    }
    catch (JavaException&)
    {
        // prints out to std err the stacktrace
        //env->ExceptionDescribe();

        jclass stringWriterClass = env->FindClass("java/io/StringWriter");
        jclass printWriterClass = env->FindClass("java/io/PrintWriter");
        jclass throwableClass = env->FindClass("java/lang/Throwable");
        jobject stringWriter = env->NewObject(
                  stringWriterClass,
                  env->GetMethodID(stringWriterClass, "<init>", "()V"));

        jobject printWriter = env->NewObject(
                  printWriterClass,
                  env->GetMethodID(printWriterClass, "<init>", "(Ljava/io/Writer;)V"),
                  stringWriter);

        env->CallObjectMethod(lException,
                  env->GetMethodID(throwableClass, "printStackTrace",
                          "(Ljava/io/PrintWriter;)V"),
                  printWriter);

  //      env->CallObjectMethod(printWriter, env->GetMethodID(printWriterClass, "flush", "()V"));
        jmethodID toStringMethod =
              env->GetMethodID(stringWriterClass, "toString", "()Ljava/lang/String;");
        jobject errorMessageObj = env->CallObjectMethod( stringWriter, toStringMethod);
        jstring errorMessage = (jstring) errorMessageObj;
        const char *errMsg = env->GetStringUTFChars(errorMessage, NULL);
        std::stringstream s;
        s << "A Java Exception was thrown:" << std::endl << errMsg;
        String errDescription;
        errDescription += s.str();
        env->ExceptionClear();
        env->ReleaseStringUTFChars(errorMessage, errMsg);

        Item errQName = NoSqlDBModule::getItemFactory()->createQName(NOSQLDB_MODULE_NAMESPACE,
                  "JAVA-EXCEPTION");

        //std::cout << "  java exception:'" << errDescription << "'" << std::endl; std::cout.flush();

        throw USER_EXCEPTION(errQName, errDescription );
    }
}

ItemSequence_t
DelFunction::evaluate(const ExternalFunction::Arguments_t& args,
                           const zorba::StaticContext* aStaticContext,
                           const zorba::DynamicContext* aDynamicContext) const
{
    jthrowable lException = 0;
    static JNIEnv* env;

    try
    {
      env = zorba::jvm::JavaVMSingleton::getInstance(aStaticContext)->getEnv();

      // read input param 0
      String lInstanceID = getOneStringArgument(args, 0);

      InstanceMap* lInstanceMap;
      if (!(lInstanceMap = dynamic_cast<InstanceMap*>(aDynamicContext->getExternalFunctionParameter("nosqldbInstanceMap"))))
      {
        throwError("NoInstanceMatch", "Not a NoSQL DB identifier.");
      }

      jobject kvsObjRef = lInstanceMap->getInstance(lInstanceID);
      if (!kvsObjRef)
      {
          throwError("NoInstanceMatch", "No instance of NoSQL DB with the given identifier was found.");
      }

      // read input param 1
      Item keyParam = getOneItemArgument(args, 1);
      if(!keyParam.isJSONItem())
        throwError("NoSQLDBModuleError", "$key param must be a JSON object");

      //    List majorList = new ArrayList();
      jclass arrayListClass = env->FindClass("java/util/ArrayList");
      CHECK_EXCEPTION(env);
      jmethodID midALCons = env->GetMethodID(arrayListClass, "<init>", "()V");
      CHECK_EXCEPTION(env);
      jmethodID midALAdd = env->GetMethodID(arrayListClass, "add", "(Ljava/lang/Object;)Z");
      CHECK_EXCEPTION(env);
      jobject majorList = env->NewObject(arrayListClass, midALCons);
      CHECK_EXCEPTION(env);
      //    List minorList = new ArrayList();
      jobject minorList = env->NewObject(arrayListClass, midALCons);
      CHECK_EXCEPTION(env);

      //    majorList.add("Mk1");
      Item majorValues = keyParam.getObjectValue("major");

      if ( majorValues.isNull() )
      {
          throwError("NoInstanceMatch", "JSON 'major' property must be specified as string or array.");
      }
      else if ( majorValues.isJSONItem() &&
           majorValues.getJSONItemKind() == store::StoreConsts::jsonArray )
      {
          uint64_t majorValueSize = majorValues.getArraySize();
          for (uint64_t i = 1; i<=majorValueSize; i++)
          {
             Item mkItem = majorValues.getArrayValue(i);
             if ( !mkItem.isAtomic() )
                 throwError("NoInstanceMatch", "JSON 'major' property must be a string or an array.");
             String mk = mkItem.getStringValue();
             jstring jStrMk = env->NewStringUTF(mk.c_str());
             CHECK_EXCEPTION(env);
             env->CallBooleanMethod(majorList, midALAdd, jStrMk);
             CHECK_EXCEPTION(env);
          }
      }
      else if ( majorValues.isAtomic() )
      {
          String mk = majorValues.getStringValue();
          jstring jStrMk = env->NewStringUTF(mk.c_str());
          CHECK_EXCEPTION(env);
          env->CallBooleanMethod(majorList, midALAdd, jStrMk);
          CHECK_EXCEPTION(env);
      }
      else
          throwError("NoInstanceMatch", "JSON 'major' property must be specified as string or array.");

      //    minorList.add("mk1");
      Item minorValues = keyParam.getObjectValue("minor");

      if ( minorValues.isNull() )
      {
          // do nothing it's perfectly fine to have "minor" missing
      }
      else if ( minorValues.isJSONItem() &&
           minorValues.getJSONItemKind() == store::StoreConsts::jsonArray )
      {
          uint64_t minorValueSize = minorValues.getArraySize();
          for (uint64_t i = 1; i<=minorValueSize; i++)
          {
             Item mkItem = minorValues.getArrayValue(i);
             if ( !mkItem.isAtomic() )
                throwError("NoInstanceMatch", "JSON 'minor' property, if specified, must be a string or an array.");
             String mk = mkItem.getStringValue();
             jstring jStrMk = env->NewStringUTF(mk.c_str());
             CHECK_EXCEPTION(env);
             env->CallBooleanMethod(minorList, midALAdd, jStrMk);
             CHECK_EXCEPTION(env);
          }
      }
      else if ( minorValues.isAtomic() )
      {
          String mk = minorValues.getStringValue();
          jstring jStrMk = env->NewStringUTF(mk.c_str());
          CHECK_EXCEPTION(env);
          env->CallBooleanMethod(majorList, midALAdd, jStrMk);
          CHECK_EXCEPTION(env);
      }
      else
          throwError("NoInstanceMatch", "JSON 'minor' property, if specified, must be a string or an array.");

      //    Key k = Key.createKey(majorList, minorList);
      jclass keyClass = env->FindClass("oracle/kv/Key");
      CHECK_EXCEPTION(env);
      jmethodID midKeyCreate = env->GetStaticMethodID(keyClass, "createKey", "(Ljava/util/List;Ljava/util/List;)Loracle/kv/Key;");
      CHECK_EXCEPTION(env);
      jobject k = env->CallStaticObjectMethod(keyClass, midKeyCreate, majorList, minorList);
      CHECK_EXCEPTION(env);

      //    ValueVersion valueVersion = store.delete(k);
      jclass kvsClass = env->FindClass("oracle/kv/KVStore");
      CHECK_EXCEPTION(env);
      jmethodID midkvsGet = env->GetMethodID(kvsClass, "delete", "(Loracle/kv/Key;)Z");
      CHECK_EXCEPTION(env);
      jboolean result = env->CallBooleanMethod(kvsObjRef, midkvsGet, k);
      CHECK_EXCEPTION(env);

      return ItemSequence_t(new SingletonItemSequence(
          NoSqlDBModule::getItemFactory()->createBoolean(result)));
    }
    catch (zorba::jvm::VMOpenException&)
    {
        Item lQName = NoSqlDBModule::getItemFactory()->createQName(NOSQLDB_MODULE_NAMESPACE,
                  "VM001");
        throw USER_EXCEPTION(lQName, "Could not start the Java VM (is the classpath set?)");
    }
    catch (JavaException&)
    {
        // prints out to std err the stacktrace
        //env->ExceptionDescribe();

        jclass stringWriterClass = env->FindClass("java/io/StringWriter");
        jclass printWriterClass = env->FindClass("java/io/PrintWriter");
        jclass throwableClass = env->FindClass("java/lang/Throwable");
        jobject stringWriter = env->NewObject(
                  stringWriterClass,
                  env->GetMethodID(stringWriterClass, "<init>", "()V"));

        jobject printWriter = env->NewObject(
                  printWriterClass,
                  env->GetMethodID(printWriterClass, "<init>", "(Ljava/io/Writer;)V"),
                  stringWriter);

        env->CallObjectMethod(lException,
                  env->GetMethodID(throwableClass, "printStackTrace",
                          "(Ljava/io/PrintWriter;)V"),
                  printWriter);

        env->CallObjectMethod(printWriter, env->GetMethodID(printWriterClass, "flush", "()V"));
        jmethodID toStringMethod =
              env->GetMethodID(stringWriterClass, "toString", "()Ljava/lang/String;");
        jobject errorMessageObj = env->CallObjectMethod( stringWriter, toStringMethod);
        jstring errorMessage = (jstring) errorMessageObj;
        const char *errMsg = env->GetStringUTFChars(errorMessage, NULL);
        std::stringstream s;
        s << "A Java Exception was thrown:" << std::endl << errMsg;
        String errDescription;
        errDescription += s.str();
        env->ExceptionClear();
        env->ReleaseStringUTFChars(errorMessage, errMsg);

        Item errQName = NoSqlDBModule::getItemFactory()->createQName(NOSQLDB_MODULE_NAMESPACE,
                  "JAVA-EXCEPTION");

        //std::cout << "  java exception:'" << errDescription << "'" << std::endl; std::cout.flush();

        throw USER_EXCEPTION(errQName, errDescription );
    }
}

/*****************************************************************************/

bool
InstanceMap::storeInstance(const String& aKeyName, jobject aInstance)
{
  std::pair<InstanceMap_t::iterator, bool> ret;
  ret = instanceMap->insert(std::pair<String, jobject>(aKeyName, aInstance));
  return ret.second;
}

jobject
InstanceMap::getInstance(const String& aKeyName)
{
  InstanceMap::InstanceMap_t::iterator lIter = instanceMap->find(aKeyName);

  if (lIter == instanceMap->end())
    return NULL;

  jobject lInstance = lIter->second;

  return lInstance;
}

bool
InstanceMap::deleteInstance(const String& aKeyName)
{
  InstanceMap::InstanceMap_t::iterator lIter = instanceMap->find(aKeyName);

  if (lIter == instanceMap->end())
    return false;

  //jobject kvsObjRef = lIter->second;

  instanceMap->erase(lIter);

  return true;
}

/*****************************************************************************/

}} // namespace zorba, nosqldb

#ifdef WIN32
#  define DLL_EXPORT __declspec(dllexport)
#else
#  define DLL_EXPORT __attribute__ ((visibility("default")))
#endif

extern "C" DLL_EXPORT zorba::ExternalModule* createModule()
{
  return new zorba::nosqldb::NoSqlDBModule();
}
