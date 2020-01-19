MEM=${1:-105000000}
BUF=${2:-105000000}

for i in 14400000
do
    NUMLINES=$i
    echo "Experiment with NUMLINES=$NUMLINES"

    /usr/bin/time -f /usr/bin/time -f "Time: %E\nProc: %P\nMemory: %M" -o ./logs/mem_gen_${i}.log \
    python3 main.py --mode gen --num_lines=$NUMLINES --sort_memory $MEM --bs $BUF

    /usr/bin/time -f /usr/bin/time -f "Time: %E\nProc: %P\nMemory: %M" -o ./logs/mem_sort_${i}.log \
    python3 main.py --mode sort --num_lines=$NUMLINES --sort_memory $MEM --bs $BUF

    FILE_SIZE=$(stat --printf="%s" file.txt)
    FILE_SORTED_SIZE=$(stat --printf="%s" file_sorted.txt)

    if [ "$FILE_SIZE" != "$FILE_SORTED_SIZE" ]; then
        echo "$FILE_SIZE != $FILE_SORTED_SIZE"
        exit 1
    fi

    echo "File size: $FILE_SIZE" >> ./logs/mem_gen_${i}.log
    echo "File size: $FILE_SORTED_SIZE" >> ./logs/mem_sort_${i}.log
done
