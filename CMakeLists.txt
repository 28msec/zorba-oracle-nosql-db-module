# Copyright 2006-2012 The FLWOR Foundation.
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

CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

PROJECT (zorba_nosqldb_module)

MESSAGE (STATUS " --- Module:   nosqldb    ---")
# The util-jvm tool finds Java and JNI, so all standard cached JAVA_
# variables will be set. util-jvm also caches Java_FOUND and JNI_FOUND
# for us.
FIND_PACKAGE (zorba_util-jvm_module QUIET)

IF (zorba_util-jvm_module_FOUND)
  IF (JNI_FOUND)
    INCLUDE_DIRECTORIES (${zorba_util-jvm_module_INCLUDE_DIRS})

    MESSAGE (STATUS "NOSQLDB_HOME = " $ENV{NOSQLDB_HOME})

    IF (WIN32)
      SET (NOSQLDB_PATH "$ENV{NOSQLDB_HOME} $ENV{NOSQLDB_HOME}\\lib C:\\Libs\\kv-1.2.123 C:\\dev\\libraries\\nosqldb C:\\dev\\libraries\\nosqldb\\lib")
      MESSAGE (STATUS "pdfbox fontbox jars searching path: " ${NOSQLDB_PATH} )
    ELSE (WIN32)
      SET (NOSQLDB_PATH $ENV{NOSQLDB_HOME} $ENV{NOSQLDB_HOME}/lib
           /usr/share/java/ /usr/share/java/kvclient/ /opt/local/share/java/kvclient/
           ${Java_LIBRARIES})
    ENDIF (WIN32)

    ZORBA_FIND_JAR( KVCLIENT_JAR NAMES kvclient PATHS ${NOSQLDB_PATH} )

    SET(ALL_NOSQLDB_JARS FALSE)
    IF(EXISTS ${KVCLIENT_JAR})
      SET(ALL_NOSQLDB_JARS TRUE)
    ENDIF(EXISTS ${KVCLIENT_JAR})

    IF( ALL_NOSQLDB_JARS )

      MESSAGE (STATUS "kvclient jar found at: " ${KVCLIENT_JAR})

      # if everything is found build this module
      SET (NOSQLDB_VERSION 1.0)

      ENABLE_TESTING ()
      INCLUDE (CTest)

      SET_CMAKE_MODULE_PATH ()

      FIND_PACKAGE (Zorba REQUIRED HINTS "${ZORBA_BUILD_DIR}")
      INCLUDE ("${Zorba_USE_FILE}")

      ADD_SUBDIRECTORY ("src")
      ADD_SUBDIRECTORY ("srcJava")
      ADD_TEST_DIRECTORY("${PROJECT_SOURCE_DIR}/test")
      DONE_DECLARING_ZORBA_URIS ()

    ELSE( ALL_NOSQLDB_JARS )
      MESSAGE (STATUS "kvclient jar not found; skipping nosqldb module.")
      MESSAGE (STATUS "   set NOSQLDB_HOME environment variable.")
    ENDIF( ALL_NOSQLDB_JARS )

  ELSE (JNI_FOUND)
    MESSAGE (STATUS "Java and/or JNI not found; skipping nosqldb module.")
  ENDIF (JNI_FOUND)

ELSE (zorba_util-jvm_module_FOUND)
  MESSAGE (STATUS "Zorba's util-jvm module not found; skipping nosqldb module.")
ENDIF (zorba_util-jvm_module_FOUND)

MESSAGE (STATUS " --- End module:   nosqldb    ---")
