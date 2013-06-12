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

#ifndef NOSQLDB_H
#define NOSQLDB_H


#include <sstream>
#include <iostream>
#include <cstdlib>
#include <list>

#include <zorba/diagnostic_list.h>
#include <zorba/empty_sequence.h>
#include <zorba/external_module.h>
#include <zorba/function.h>
#include <zorba/item_factory.h>
#include <zorba/serializer.h>
#include <zorba/singleton_item_sequence.h>
#include <zorba/user_exception.h>
#include <zorba/util/base64.h>
#include <zorba/util/base64_stream.h>
#include <zorba/util/uuid.h>
#include <zorba/vector_item_sequence.h>
#include <zorba/zorba.h>

#include "JavaVMSingleton.h"


#define NOSQLDB_MODULE_NAMESPACE "http://www.zorba-xquery.com/modules/oracle-nosqldb"


namespace zorba
{
namespace nosqldb
{

class NoSqlDBModule;
class ConnectFunction;
class IsConnectFunction;
//class DisconnectFunction;
class PutFunction;
class GetFunction;
class DelFunction;
class MultiGetFunction;
class MultiDelFunction;
class NoSqlDBOptions;
class InstanceMap;

class JavaException {};
#define CHECK_EXCEPTION(env)  if ((lException = env->ExceptionOccurred())) throw JavaException()


class ConnectFunction : public ContextualExternalFunction
{
  private:
    const ExternalModule* theModule;
    XmlDataManager* theDataManager;

  public:
    ConnectFunction(const ExternalModule* aModule) :
      theModule(aModule),
      theDataManager(Zorba::getInstance(0)->getXmlDataManager())
    {}

    ~ConnectFunction()
    {}

    virtual String getURI() const
    { return theModule->getURI(); }

    virtual String getLocalName() const
    { return "connect-internal"; }

    virtual ItemSequence_t
      evaluate(const ExternalFunction::Arguments_t& args,
               const zorba::StaticContext*,
               const zorba::DynamicContext*) const;
};

/*
class DisconnectFunction : public ContextualExternalFunction
{
  private:
    const ExternalModule* theModule;
    XmlDataManager* theDataManager;

  public:
    DisconnectFunction(const ExternalModule* aModule) :
      theModule(aModule),
      theDataManager(Zorba::getInstance(0)->getXmlDataManager())
    {}

    ~DisconnectFunction()
    {}

  public:
    virtual String getURI() const
    { return theModule->getURI(); }

    virtual String getLocalName() const
    { return "disconnect"; }

    virtual ItemSequence_t
      evaluate(const ExternalFunction::Arguments_t& args,
               const zorba::StaticContext*,
               const zorba::DynamicContext*) const;
};*/


class PutFunction : public ContextualExternalFunction
{
  private:
    const ExternalModule* theModule;
    XmlDataManager* theDataManager;

  public:
    PutFunction(const ExternalModule* aModule) :
      theModule(aModule),
      theDataManager(Zorba::getInstance(0)->getXmlDataManager())
    {}

    ~PutFunction()
    {}

    virtual String getURI() const
    { return theModule->getURI(); }

    virtual String getLocalName() const
    { return "put-binary"; }

    virtual ItemSequence_t
      evaluate(const ExternalFunction::Arguments_t& args,
               const zorba::StaticContext*,
               const zorba::DynamicContext*) const;
};


class GetFunction : public ContextualExternalFunction
{
  private:
    const ExternalModule* theModule;
    XmlDataManager* theDataManager;

  public:
    GetFunction(const ExternalModule* aModule) :
      theModule(aModule),
      theDataManager(Zorba::getInstance(0)->getXmlDataManager())
    {}

    ~GetFunction()
    {}

    virtual String getURI() const
    { return theModule->getURI(); }

    virtual String getLocalName() const
    { return "get-binary"; }

    virtual ItemSequence_t
      evaluate(const ExternalFunction::Arguments_t& args,
               const zorba::StaticContext*,
               const zorba::DynamicContext*) const;
};

class DelFunction : public ContextualExternalFunction
{
  private:
    const ExternalModule* theModule;
    XmlDataManager* theDataManager;

  public:
    DelFunction(const ExternalModule* aModule) :
      theModule(aModule),
      theDataManager(Zorba::getInstance(0)->getXmlDataManager())
    {}

    ~DelFunction()
    {}

    virtual String getURI() const
    { return theModule->getURI(); }

    virtual String getLocalName() const
    { return "remove"; }

    virtual ItemSequence_t
      evaluate(const ExternalFunction::Arguments_t& args,
               const zorba::StaticContext*,
               const zorba::DynamicContext*) const;
};

class MultiGetFunction : public ContextualExternalFunction
{
  private:
    const ExternalModule* theModule;
    XmlDataManager* theDataManager;

  public:
    MultiGetFunction(const ExternalModule* aModule) :
      theModule(aModule),
      theDataManager(Zorba::getInstance(0)->getXmlDataManager())
    {}

    ~MultiGetFunction()
    {}

    virtual String getURI() const
    { return theModule->getURI(); }

    virtual String getLocalName() const
    { return "multi-get-binary"; }

    virtual ItemSequence_t
      evaluate(const ExternalFunction::Arguments_t& args,
               const zorba::StaticContext*,
               const zorba::DynamicContext*) const;
};

class MultiDelFunction : public ContextualExternalFunction
{
  private:
    const ExternalModule* theModule;
    XmlDataManager* theDataManager;

  public:
    MultiDelFunction(const ExternalModule* aModule) :
      theModule(aModule),
      theDataManager(Zorba::getInstance(0)->getXmlDataManager())
    {}
    ~MultiDelFunction()
    {}

    virtual String getURI() const
    { return theModule->getURI(); }

    virtual String getLocalName() const
    { return "multi-remove"; }

    virtual ItemSequence_t
      evaluate(const ExternalFunction::Arguments_t& args,
               const zorba::StaticContext*,
               const zorba::DynamicContext*) const;
};



class NoSqlDBModule : public ExternalModule
{
  private:
    ExternalFunction* connect;
    //ExternalFunction* disconnect;
    ExternalFunction* put;
    ExternalFunction* get;
    ExternalFunction* del;
    ExternalFunction* multiGet;
    ExternalFunction* multiDel;

  public:
    static ItemFactory* getItemFactory()
    {
        return Zorba::getInstance(0)->getItemFactory();
    }

    NoSqlDBModule() :
        connect(new ConnectFunction(this)),
        //disconnect(new DisconnectFunction(this)),
        put(new PutFunction(this)),
        get(new GetFunction(this)),
        del(new DelFunction(this)),
        multiGet(new MultiGetFunction(this)),
        multiDel(new MultiDelFunction(this))
    {}

    ~NoSqlDBModule()
    {
        delete connect;
        //delete disconnect;
        delete put;
        delete get;
        delete del;
        delete multiGet;
        delete multiDel;
    }

    virtual String getURI() const
    { return NOSQLDB_MODULE_NAMESPACE; }

    virtual ExternalFunction* getExternalFunction(const String& localName);

    virtual void destroy()
    {
      delete this;
    }
};


class InstanceMap : public ExternalFunctionParameter
{
  private:
    typedef std::map<String, jobject> InstanceMap_t;
    JNIEnv* env;
    InstanceMap_t* instanceMap;
    void closeConnection(jobject kvsObjRef);


  public:
    InstanceMap(JNIEnv* aEnv) : env(aEnv), instanceMap(new InstanceMap_t())
    {}

    bool
    storeInstance(const String&, jobject);

    jobject
    getInstance(const String&);

    bool
    deleteInstance(const String&);

    virtual void
    destroy() throw()
    {
      if (instanceMap)
      {
        for (InstanceMap_t::const_iterator lIter = instanceMap->begin();
             lIter != instanceMap->end(); ++lIter)
        {
          jobject kvsObjRef = lIter->second;

          closeConnection(kvsObjRef);

          env->DeleteGlobalRef(kvsObjRef);
        }
        instanceMap->clear();
        delete instanceMap;
      }
      delete this;
    };

};


}} // namespace zorba, nosqldb
#endif // NOSQLDB_H
