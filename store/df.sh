df -PTh|grep -v Filesystem |grep -v /dev/shm |grep "/dev" |awk {'print "&" $1 "&" $3 "&" $5 "&" $7 "&"'}
