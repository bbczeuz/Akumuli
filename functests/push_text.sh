#!/bin/bash

mydir=$(mktemp -dt "XXXXXXXXX")

echo Generating data
echo "Begin: " `date -u ++%Y%m%dT%H%M%S.%N`
for idx in {0..1000}
do
date -u ++%Y%m%dT%H%M%S.%N | awk '{print "+testi tag1=A tag2=C tag3=I\r\n"$0"\r\n+24.3"}' >> $mydir/tcpdata
done
echo "End: " `date -u ++%Y%m%dT%H%M%S.%N`

echo Pushing data
cat ${mydir}/tcpdata | nc -C localhost 8282
#rm ${mydir}/tcpdata
#rmdir ${mydir}
echo Done
