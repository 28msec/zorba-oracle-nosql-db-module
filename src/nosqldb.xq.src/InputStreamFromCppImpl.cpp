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
#include <iostream>


#ifndef _Included_org_zorbaxquery_modules_nosqldb_InputStreamFromCpp
#define _Included_org_zorbaxquery_modules_nosqldb_InputStreamFromCpp

#ifdef WIN32
#  define DLL_EXPORT __declspec(dllexport)
#else
#  define DLL_EXPORT __attribute__ ((visibility("default")))
#endif

#ifdef __cplusplus
extern "C" {
#endif

DLL_EXPORT JNIEXPORT jint JNICALL
Java_org_zorbaxquery_modules_nosqldb_InputStreamFromCpp_nativeRead
    (JNIEnv * env, jobject jobj, jbyteArray ba, jint jsize, jlong p)
{
        //std::cout << "c:  nativeRead  available space:" << jsize << std::endl; std::cout.flush();

        std::istream *is = (std::istream*)p;

        if ( is->eof() )
        {
            //std::cout << "c:    eof"  << std::endl; std::cout.flush();
            return -1;
        }

        if ( is->fail() || is->bad() )
        {
            //std::cout << "c:    FAIL"  << std::endl; std::cout.flush();
            return -2;
        }

        char * buf = new char[jsize];

        is->read(buf, jsize);
        jint actualReadSize = is->gcount();

        env->SetByteArrayRegion(ba, 0, actualReadSize, (jbyte *)buf);
        delete buf;

        if ( actualReadSize == 0 && is->eof() )
        {
            //std::cout << "c:    eof"  << std::endl; std::cout.flush();
            return -1;
        }

        //std::cout << "c:   return " << actualReadSize << std::endl; std::cout.flush();
        return actualReadSize;
}

#ifdef __cplusplus
}
#endif
#endif

