#include <jni.h>
#include <iostream>
#include "JavaVMSingleton.h"


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
#ifndef _Included_org_zorbaxquery_modules_nosqldb_OutputStreamToCpp
#define _Included_org_zorbaxquery_modules_nosqldb_OutputStreamToCpp

#ifdef WIN32
#  define DLL_EXPORT __declspec(dllexport)
#else
#  define DLL_EXPORT __attribute__ ((visibility("default")))
#endif

#ifdef __cplusplus
extern "C" {
#endif


/*
 * Class:     OutputStreamToCpp
 * Method:    nativeWrite
 * Signature: ([BIIL)I
 * Reads from istream stored inside cppObjectPointer into ba of maximum jsize.
 * Returns number of bytes/chars read or -1 if eof or -2 if fail or bad stream.
 */
DLL_EXPORT JNIEXPORT jint JNICALL
Java_org_zorbaxquery_modules_nosqldb_OutputStreamToCpp_nativeWrite
  (JNIEnv * env, jobject obj, jbyteArray ba, jint off, jint len, jlong p)
{
    std::ostream *os = (std::ostream*)p;

    char * buf = new char[len];
    env->GetByteArrayRegion(ba, off, len, (jbyte *)buf);

    os->write(buf, len);
    delete buf;

    if ( os->bad() )
        return -2;

    jint actualWriteSize = len;
    return actualWriteSize;
}

#ifdef __cplusplus
}
#endif
#endif

