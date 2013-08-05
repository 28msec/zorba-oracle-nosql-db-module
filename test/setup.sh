#!/bin/bash


JAVA7=/usr/lib/jvm/java-7-oracle/bin/java

# kv-1.2.123.tar.gz
TGZ=kv-ce-2.1.8.tar.gz

# kv-1.2.123
KVDIR=kv-2.1.8 

# kvstore-1.2.123.jar
KVSTOREJAR=kvstore.jar

while getopts distcvhj: opt
do
  case "$opt" in
        j)
            JAVA7="$OPTARG"
            echo "Java7 path: $JAVA7" 
            
            ;;
        d)
            echo "Download" 
            wget http://my.zorba-xquery.com/tmp/$TGZ
            ;;
        i)
            echo "Install"
            tar -xvzf $TGZ
            ;;
        s)
            echo "Start"
            echo "   Running with java (JDK7 is required):"
            $JAVA7 -version
            $JAVA7 -cp ./$KVDIR/lib/$KVSTOREJAR -Xms500m -Xmx500m oracle.kv.util.kvlite.KVLite -root ./$KVDIR/kvroot -store kvstore -host 127.0.0.1 -port 5000 -admin 5001 &
            ;;
        t)
            echo "Stop"
            echo "   Running with java :"
            $JAVA7 -version
            $JAVA7 -jar ./$KVDIR/lib/$KVSTOREJAR stop -root ./$KVDIR/kvroot
            ;;
        c)
            echo "Clean data"
            rm -fR ./$KVDIR/kvroot
            ;;
        v)
            echo "Clean everything"
            rm -fR ./$KVDIR $TGZ
            ;;            
        h)
            echo "usage: $0 [-j java7/path] [-distcvh] " 
            echo "       -j  Java7 path"
            echo "       -d  Download Oracle NoSQL DB"
            echo "       -i  Install"
            echo "       -s  Start service"
            echo "       -t  Stop service"
            echo "       -c  Clean data"
            echo "       -v  Very clean, clean everything"
            echo "       -h  Help"
            exit 2
            ;;
        *)
            if [ "$OPTERR" != 1 ] || [ "${optspec:0:1}" = ":" ]; then
                echo "Non-option argument: '-${OPTARG}'"
            fi
            ;;

    esac
done

