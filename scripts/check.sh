FILES="/home/apostolis/Projects/bigdata/tpcds-kit/queries/*"
for f in $FILES
do
    if grep -q "inventory" "$f";
    then
        echo "$f"
    fi
done
