DIR_TO_CHECK='/root/work/data'
 
OLD_STAT_FILE='data.csv'
 
if [ -e $OLD_STAT_FILE ]
then
        OLD_STAT=`cat $OLD_STAT_FILE`
else
        OLD_STAT="nothing"
fi
 
NEW_STAT=`stat -t $DIR_TO_CHECK`
 
if [ "$OLD_STAT" != "$NEW_STAT" ]
then
        echo 'Directory has changed. Do something!'
        # do whatever you want to do with the directory.
        # update the OLD_STAT_FILE
        echo $NEW_STAT > $OLD_STAT_FILE
else
	echo 'No changes detected'
fi
