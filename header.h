#include "vars.h"
#include "types.h"

// file.cpp
void gen_file(void);
long_map_t* GetPopularBlockList(long *top_k);
block_t* GetBlock(long id);

// init.cpp
void init(void);

// main.cpp
// extern "C" FILE * __cdecl __iob_func(void)
// extern "C" void sim(void)

// manager.cpp
void state_manager(void);
long_map_t* FindSierra(bool cs[], long top_k, long req_m);
long_map_t* FindiPACS(bool cs[], long_map_t *bag, long top_k, long req_m);
long_map_t* FindPCS(bool cs[], long_map_t *bag, long req_m);
void ActivateNodes(bool cs[], long_map_t *bag);

// mapreduce.cpp
void job_tracker(void);
void mapper(long id);

// node.cpp
void init_node(void);
void node(long id);
bool cache_hit(long nid, long bid);
void mem_caching(long nid, long bid);
void node_cpu(long id, double t);
void node_mem(long id, long n);
void node_disk(long id, long n);
//void bblock_use(long nid, long bid);
//void bblock_add(long nid, long bid);
//void bblock_del(long nid, long bid);
//void transfer_blocks_to_budget(long id);

// rack.cpp
void init_rack(void);
void turnon_rack(long id);
void turnoff_rack(long id);
double switch_rack(long from, long to);
double switch_rack(long from, long to, double n);

// report.cpp
void sim_report(void);

// scheduler.cpp
msg_t* scheduler(long node);

// setup.cpp
void setup(void);

// workload.cpp
void scenario(void);
void workload(void);

// zipf.cpp
void zipf(void);
long rand_zipf(void);