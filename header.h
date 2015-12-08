#include "vars.h"
#include "types.h"

// file.cpp
void gen_file(void);
std::map<long, long> GetUnitOfFileAcc(void);
std::map<long, long>* GetPopularBlockList(long *top_k);
block_t* GetBlock(long id);

// init.cpp
void init(void);

// log.cpp
void logging(char str[]);

// main.cpp
// extern "C" void sim(void)

// manager.cpp
void state_manager(void);
std::map<long, long>* FindSierra(bool cs[], long top_k, long req_m);
std::map<long, long>* FindiPACS(bool cs[], std::map<long, long> *bag, long top_k, long req_m);
std::map<long, long>* FindRCS(bool cs[], long req_m);
std::map<long, long>* FindPCS(bool cs[], std::map<long, long> *bag, long req_m);
void ActivateNodes(bool cs[], std::map<long, long> *bag);

// mapreduce.cpp
void job_tracker(void);
void mapper(long id);

// node.cpp
void init_node(void);
void node(long id);
bool cache_hit(long nid, long bid);
void mem_caching(long nid, long bid);
void node_cpu(long id);
void node_mem(long id, long n);
void node_disk(long id, long n);

// rack.cpp
void init_rack(void);
void turnon_rack(long id);
void turnoff_rack(long id);
void switch_rack(long from, long to, long n);

// report.cpp
void sim_report(void);

// scheduler.cpp
bool sort_queue(const job_t *l, const job_t *r);
msg_t* scheduler(long node);

// setup.cpp
void setup(void);

// workload.cpp
void workload(void);

// zipf.cpp
void zipf(void);
long rand_zipf(void);