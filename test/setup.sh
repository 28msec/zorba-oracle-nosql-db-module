#!/bin/bash


JAVA7=/usr/lib/jvm/java-7-oracle/bin/java

while getopts distcvhj: opt
do
  case "$opt" in
        j)
            JAVA7="$OPTARG"
            echo "Java7 path: $JAVA7" 
            
            ;;
        d)
            echo "Download" 
            wget http://my.zorba-xquery.com/tmp/kv-1.2.123.tar.gz
            ;;
        i)
            echo "Install"
            tar -xvzf kv-1.2.123.tar.gz
            ;;
        s)
            echo "Start"
            echo "   Running with java (JDK7 is required):"
            $JAVA7 -version
            $JAVA7 -cp ./kv-1.2.123/lib/kvstore-1.2.123.jar -Xms500m -Xmx500m oracle.kv.util.kvlite.KVLite -root ./kv-1.2.123/kvroot -store kvstore -host 127.0.0.1 -port 5000 -admin 5001 &
            ;;
        t)
            echo "Stop"
            echo "   Running with java :"
            $JAVA7 -version
            $JAVA7 -jar ./kv-1.2.123/lib/kvstore-1.2.123.jar stop -root ./kv-1.2.123/kvroot
            ;;
        c)
            echo "Clean data"
            rm -fR ./kv-1.2.123/kvroot
            ;;
        v)
            echo "Clean everything"
            rm -fR ./kv-1.2.123 kv-1.2.123.tar.gz
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

