# read all the sql files in the directory and evaluate them comparing the output with the expected output
# Usage: ./evaluate.sh <njudb bin directory> <sql case directory>

bin_dir=$(cd "$1" && pwd)
case_dir=$(cd "$2" && pwd)
sql_root=$(dirname "$case_dir")
cd "$bin_dir"
# mkdir output under sql_dir if it does not exist
if [ ! -d "$case_dir"/output ]; then
    mkdir "$case_dir"/output
fi

# delete data/db2025 directory if it exists
if [ -d "data/db2025" ]; then
    rm -r data/db2025
fi
if [ -f "data/njudb.log" ]; then
    rm -f data/njudb.log
fi

# start a new process for njudb
echo "Start njudb..."
./njudb &
sleep 2

# run client
./client -i "$sql_root"/init.sql

sql_dir="$case_dir"
# get total sql file number
total=$(find "$sql_dir" -type f -name "[0-9][0-9]_*.sql" | wc -l)
# counter for the number of passed tests
passed=0
# iterate over all sql files in the directory matching 01_*.sql, 02_*.sql, etc., in sorted order
for file in $(ls "$sql_dir"/[0-9][0-9]_*.sql | sort); do
    # get the name of the file without the extension
    filename=$(basename -- "$file")
    filename="${filename%.*}"
    # run the sql file
    ./client -i "$file" -o "$sql_dir"/output/$filename.out
    # compare the output with the expected output
    diff "$sql_dir"/output/"$filename".out "$sql_dir"/expected/"$filename".out
    # if the output is different, print the error message
    if [ $? -ne 0 ]; then
        echo "Error in $filename"
    else
        echo "Passed: $filename"
        passed=$((passed+1))
    fi
done

# print the number of passed tests
echo "Passed [$passed/$total] tests"

# kill the njudb process
kill $(pgrep njudb)
