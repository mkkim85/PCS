#include "vars.h"
#include "types.h"

// file.cpp
void gen_file(void);

// init.cpp
void init(void);

// log.cpp
void logging(char str[]);

// main.cpp
// extern "C" void sim(void)

// manager.cpp


// mapreduce.cpp
void mapper(long id);

// node.cpp
void init_node(void);

// rack.cpp
void init_rack(void);
void turnon_rack(long id);
void turnoff_rack(long id);

// report
void sim_report(void);

// setup.cpp
void setup(void);

// workload.cpp
void workload(void);

// zipf.cpp
void zipf(void);
long rand_zipf(void);