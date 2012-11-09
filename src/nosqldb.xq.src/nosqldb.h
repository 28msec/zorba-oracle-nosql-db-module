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

#include <zorba/base64.h>
#include <zorba/base64_stream.h>
#include <zorba/empty_sequence.h>
#include <zorba/diagnostic_list.h>
#include <zorba/function.h>
#include <zorba/external_module.h>
#include <zorba/user_exception.h>
#include <zorba/file.h>
#include <zorba/item_factory.h>
#include <zorba/serializer.h>
#include <zorba/singleton_item_sequence.h>
#include <zorba/util/uuid.h>
#include <zorba/vector_item_sequence.h>
#include <zorba/zorba.h>

#include "JavaVMSingleton.h"


#define NOSQLDB_MODULE_NAMESPACE "http://www.zorba-xquery.com/modules/nosqldb"
#define NOSQLDB_OPTIONS_NAMESPACE "http://www.zorba-xquery.com/modules/nosqldb/nosqldb-options"




namespace zorba
{
namespace nosqldb
{

class NoSqlDBModule;
class ConnectFunction;
class DisconnectFunction;
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


class IsConnectedFunction : public ContextualExternalFunction
{
  private:
    const ExternalModule* theModule;
    XmlDataManager* theDataManager;

  public:
    IsConnectedFunction(const ExternalModule* aModule) :
      theModule(aModule),
      theDataManager(Zorba::getInstance(0)->getXmlDataManager())
    {}

    ~IsConnectedFunction()
    {}

    virtual String getURI() const
    { return theModule->getURI(); }

    virtual String getLocalName() const
    { return "is-connected"; }

    virtual ItemSequence_t
      evaluate(const ExternalFunction::Arguments_t& args,
               const zorba::StaticContext*,
               const zorba::DynamicContext*) const;
};

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
};


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
    { return "put-base64"; }

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
    { return "get-base64"; }

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
    { return "delete"; }

    virtual ItemSequence_t
      evaluate(const ExternalFunction::Arguments_t& args,
               const zorba::StaticContext*,
               const zorba::DynamicContext*) const;
};


class NoSqlDBModule : public ExternalModule
{
  private:
    ExternalFunction* connect;
    ExternalFunction* isConnected;
    ExternalFunction* disconnect;
    ExternalFunction* put;
    ExternalFunction* get;
    ExternalFunction* del;

  public:
    static ItemFactory* getItemFactory()
    {
        return Zorba::getInstance(0)->getItemFactory();
    }

    NoSqlDBModule() :
        connect(new ConnectFunction(this)),
        isConnected(new IsConnectedFunction(this)),
        disconnect(new DisconnectFunction(this)),
        put(new PutFunction(this)),
        get(new GetFunction(this)),
        del(new DelFunction(this))
    {}

    ~NoSqlDBModule()
    {
        delete connect;
        delete isConnected;
        delete disconnect;
        delete put;
        delete get;
        delete del;
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
