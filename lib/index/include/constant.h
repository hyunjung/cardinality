/*


The MIT License

Copyright (c) 2009 Clement Genzmer

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.


*/



#ifndef CONSTANT_H_
#define CONSTANT_H_

#include "server.h"
#include <stdlib.h>

const int MAX_LENGTH_ALLOC = MAX_VARCHAR_LEN + 2 ;
const int32_t MAX_HASH = 2147483647 ;
const int32_t MAX_SHORT_KEY = 2147483647;
const int64_t MAX_INT_KEY = 2147483647;

const int MAX_ACTION_PER_TRANSACTION = 100 ;
const int MAX_INDEX = 200 ;
const int HASH_SIZE_NAME_INDEX = 1000 ;
const int LENGTH_INDEX_NAME = 100 ;
const int SIZE_KEY_POOL = 10000 ;
const int MAX_THREADS = 1000 ;
const int MAX_TRYLOCK_LOOP = 1000 ;
const int PROBE_SIZE = 20 ;
const int PROBE_MASK = (1<<PROBE_SIZE) - 1 ;
//deprecated
const int32_t PARTS = 10000 ;
const int SAME_TIME_THREAD = 8 ;


#ifndef CONSTANT
const int START_HASH_H = 10 ;
const int MIN_INDEX_PER_THREAD = 3 ;
const int OPTIMIZE_THRESHOLD = 0 ;
const float WARP_MODE_THRESHOLD = 4.0 ;
#endif


// Temporary testing purpose
#ifndef NB_THREAD_PERMITTED
const int NB_THREAD_PERMITTED = 10 ;
#endif

//parameters

// Testing START_HASH_H
#if CONSTANT == 1
const int START_HASH_H = 1 ;
const int MIN_INDEX_PER_THREAD = 10 ;
const int OPTIMIZE_THRESHOLD = 0 ;
const float WARP_MODE_THRESHOLD = 4.0 ;
#endif
#if CONSTANT == 2
const int START_HASH_H = 2 ;
const int MIN_INDEX_PER_THREAD = 10 ;
const int OPTIMIZE_THRESHOLD = 0 ;
const float WARP_MODE_THRESHOLD = 4.0 ;
#endif
#if CONSTANT == 3
const int START_HASH_H = 7 ;
const int MIN_INDEX_PER_THREAD = 10 ;
const int OPTIMIZE_THRESHOLD = 0 ;
const float WARP_MODE_THRESHOLD = 4.0 ;
#endif
#if CONSTANT == 4
const int START_HASH_H = 10 ;
const int MIN_INDEX_PER_THREAD = 10 ;
const int OPTIMIZE_THRESHOLD = 0 ;
const float WARP_MODE_THRESHOLD = 4.0 ;
#endif
#if CONSTANT == 5
const int START_HASH_H = 14 ;
const int MIN_INDEX_PER_THREAD = 10 ;
const int OPTIMIZE_THRESHOLD = 0 ;
const float WARP_MODE_THRESHOLD = 4.0 ;
#endif

// Testing MIN_INDEX_PER_THREAD
#if CONSTANT == 6
const int START_HASH_H = 10 ;
const int MIN_INDEX_PER_THREAD = 1 ;
const int OPTIMIZE_THRESHOLD = 0 ;
const float WARP_MODE_THRESHOLD = 4.0 ;
#endif
#if CONSTANT == 7
const int START_HASH_H = 10 ;
const int MIN_INDEX_PER_THREAD = 2 ;
const int OPTIMIZE_THRESHOLD = 0 ;
const float WARP_MODE_THRESHOLD = 4.0 ;
#endif
#if CONSTANT == 8
const int START_HASH_H = 10 ;
const int MIN_INDEX_PER_THREAD = 3 ;
const int OPTIMIZE_THRESHOLD = 0 ;
const float WARP_MODE_THRESHOLD = 4.0 ;
#endif
#if CONSTANT == 9
const int START_HASH_H = 10 ;
const int MIN_INDEX_PER_THREAD = 4 ;
const int OPTIMIZE_THRESHOLD = 0 ;
const float WARP_MODE_THRESHOLD = 4.0 ;
#endif
#if CONSTANT == 10
const int START_HASH_H = 10 ;
const int MIN_INDEX_PER_THREAD = 5 ;
const int OPTIMIZE_THRESHOLD = 0 ;
const float WARP_MODE_THRESHOLD = 4.0 ;
#endif

// Testint OPTIMIZE_THRESHOLD
#if CONSTANT == 11
const int START_HASH_H = 10 ;
const int MIN_INDEX_PER_THREAD = 10 ;
const int OPTIMIZE_THRESHOLD = -2 ;
const float WARP_MODE_THRESHOLD = 4.0 ;
#endif
#if CONSTANT == 12
const int START_HASH_H = 10 ;
const int MIN_INDEX_PER_THREAD = 10 ;
const int OPTIMIZE_THRESHOLD = -1 ;
const float WARP_MODE_THRESHOLD = 4.0 ;
#endif
#if CONSTANT == 13
const int START_HASH_H = 10 ;
const int MIN_INDEX_PER_THREAD = 10 ;
const int OPTIMIZE_THRESHOLD = 0 ;
const float WARP_MODE_THRESHOLD = 4.0 ;
#endif
#if CONSTANT == 14
const int START_HASH_H = 10 ;
const int MIN_INDEX_PER_THREAD = 10 ;
const int OPTIMIZE_THRESHOLD = 1 ;
const float WARP_MODE_THRESHOLD = 4.0 ;
#endif
#if CONSTANT == 15
const int START_HASH_H = 10 ;
const int MIN_INDEX_PER_THREAD = 10 ;
const int OPTIMIZE_THRESHOLD = 1 ;
const float WARP_MODE_THRESHOLD = 4.0 ;
#endif

// Testint WARP_MODE_THRESHOLD 
#if CONSTANT == 16
const int START_HASH_H = 10 ;
const int MIN_INDEX_PER_THREAD = 10 ;
const int OPTIMIZE_THRESHOLD = 0 ;
const float WARP_MODE_THRESHOLD = 2.0 ;
#endif
#if CONSTANT == 17
const int START_HASH_H = 10 ;
const int MIN_INDEX_PER_THREAD = 10 ;
const int OPTIMIZE_THRESHOLD = 0 ;
const float WARP_MODE_THRESHOLD = 2.5 ;
#endif
#if CONSTANT == 18
const int START_HASH_H = 10 ;
const int MIN_INDEX_PER_THREAD = 10 ;
const int OPTIMIZE_THRESHOLD = 0 ;
const float WARP_MODE_THRESHOLD = 3.0 ;
#endif
#if CONSTANT == 19
const int START_HASH_H = 10 ;
const int MIN_INDEX_PER_THREAD = 10 ;
const int OPTIMIZE_THRESHOLD = 0 ;
const float WARP_MODE_THRESHOLD = 3.5 ;
#endif
#if CONSTANT == 20
const int START_HASH_H = 10 ;
const int MIN_INDEX_PER_THREAD = 10 ;
const int OPTIMIZE_THRESHOLD = 0 ;
const float WARP_MODE_THRESHOLD = 4.0 ;
#endif
#endif
