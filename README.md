# external-sorting
External sorting algorithm with data generator. Algorithm uses up to 2*n additional memory (external) during merge step.
It takes some time to generate dummy file (sampling is performed using only one CPU core).

Test
---
To test run
```bash
./test.sh MEM BUF
```
Where:
* `MEM` memory constant for default sorting alogrithm
* `BUF` memory constant (per worker) for each reading buffer

Check `./logs/mem_%STEP%_%EXPERIMENT%.log` files for CPU and MEM usage

_Tested with python 3.7.3.\
There may be some problems w/ iterators behaviour if python ver. is lower._
