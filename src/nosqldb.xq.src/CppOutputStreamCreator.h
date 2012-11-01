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
#include <jni.h>


namespace zorba
{
namespace nosqldb
{

/* Header for class CppOutputStreamCreator */

class CppOutputStreamCreator
{
  private:
    // Global reference to mirror java object CppOutputStreamCreator
    jobject _obj;
    // List of streams created by newOutputStream()
    std::vector<std::stringstream *> _ssVector;

    CppOutputStreamCreator()
    {}

    void setJavaObject(jobject jo)
    { _obj = jo; }

  public:
    ~CppOutputStreamCreator()
    {
        _obj = 0;
        for ( std::vector<std::stringstream *>::size_type i = 0; i<_ssVector.size(); i++)
        {
          delete _ssVector[i];
        }
    }

    static CppOutputStreamCreator* newInstance(JNIEnv * env);

    std::ostream* newOutputStream();

    int getSize()
    {
      return _ssVector.size();
    }

    jobject getJavaObject()
    { return _obj; }

    std::stringstream * getSS(int i)
    { return _ssVector[i]; }

    void close(JNIEnv * env);
};


}}; // namespace zorba, nosqldb

#ifndef _Included_org_zorbaxquery_modules_nosqldb_CppOutputStreamCreatorH
#define _Included_org_zorbaxquery_modules_nosqldb_CppOutputStreamCreatorH

#ifdef WIN32
#  define DLL_EXPORT __declspec(dllexport)
#else
#  define DLL_EXPORT __attribute__ ((visibility("default")))
#endif

#ifdef __cplusplus
extern "C" {
#endif
#undef CppOutputStreamCreator_SKIP_BUFFER_SIZE
#define CppOutputStreamCreator_SKIP_BUFFER_SIZE 2048L

/*
 * Class:     CppOutputStreamCreator
 * Method:    nativeCreateNewOutputStream
 * Signature: ([BI)I
 */
DLL_EXPORT JNIEXPORT jlong JNICALL
Java_org_zorbaxquery_modules_nosqldb_CppOutputStreamCreator_nativeCreateNewOutputStream
  (JNIEnv *, jobject, jlong p);

#ifdef __cplusplus
}
#endif
#endif //_Included_org_zorbaxquery_modules_nosqldb_CppOutputStreamCreatorH
