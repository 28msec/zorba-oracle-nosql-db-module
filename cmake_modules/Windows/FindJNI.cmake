# Copyright 2010 The FLWOR Foundation.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# - Try to find the JNI libraries on Windows
#
# See the FindJNI.cmake module shipped with Zorba for more information.

FIND_PACKAGE_WIN32_NO_PROXY (JNI JNI_FOUND)

IF (JNI_FOUND)

  STRING (REPLACE "/jvm.lib" "" JAVA_JVM_LIBRARY_PATH "${JAVA_JVM_LIBRARY}")

  IF (EXISTS "${JAVA_JVM_LIBRARY_PATH}/../jre/bin/client")
    SET (FOUND_LOCATION "${JAVA_JVM_LIBRARY_PATH}/../jre/bin/client")
    # find the needed DLL's
    FIND_PACKAGE_DLL_WIN32 (${FOUND_LOCATION} "jvm")
  ELSEIF (EXISTS "${JAVA_JVM_LIBRARY_PATH}/../jre/bin/server")
    SET (FOUND_LOCATION "${JAVA_JVM_LIBRARY_PATH}/../jre/bin/server")
    # find the needed DLL's
    FIND_PACKAGE_DLL_WIN32 (${FOUND_LOCATION} "jvm")
  ELSE (EXISTS "${JAVA_JVM_LIBRARY_PATH}/../jre/bin/client")
    MESSAGE (WARNING "Could not find the jvm.dll for the JVM library: ${JAVA_JVM_LIBRARY}. Please extend this module to find the jvm.dll somewhere in your JVM directory or make sure that jvm.dll is in the PATH.")
  ENDIF (EXISTS "${JAVA_JVM_LIBRARY_PATH}/../jre/bin/client")

ENDIF (JNI_FOUND)
