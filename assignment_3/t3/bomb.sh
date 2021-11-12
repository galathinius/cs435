#! /bin/bash
set -e

# get roky muntain park index from titles file
ROKY_IDX=$(grep -n -m 1 Rocky_Mountain_National_Park titles-sorted.txt | awk -F: '{ print $1 }' )
# echo $ROKY_INFO
# 4290745

# get surfing indexes from titles file
SURF_IDX=$(grep -n surfing titles-sorted.txt | awk -F: '{ print $1 }')

# add index to surfing adj list

for IDX in $SURF_IDX
do
	sed -r -i s/\(^"${IDX}".*\)/\\1" ${ROKY_IDX}"/ links-simple-sorted.txt
done

# add '(surfing)' to roky mnt title
sed -r -i s/\(^Rocky_Mountain_National_Park$\)/\\1" (surfing)"/ titles-sorted.txt



# grep 390394: links-simple-sorted.txt 
# 390394: 4080724
# 1390394: 1680553