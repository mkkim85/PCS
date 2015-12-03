#include "vars.h"
#include "types.h"

// file.cpp
void gen_file(void);
long GetMaxFileAcc(void);
long GetTopK(std::list<long> *h);

// init.cpp
void init(void);

// log.cpp
void logging(char str[]);

// main.cpp
// extern "C" void sim(void)

// manager.cpp
void state_manager(void);
covering_t* FindSierra(long top_k, long req_m);
//covering_t* FindiPACS(long top_k, long req_m, std::map<long, long> *bag);
//covering_t* FindRCS(long top_k, long req_m);
//covering_t* FindPCS(long top_k, long req_m, std::map<long, long> *bag);
void ActivateNodes(covering_t *cs);

// mapreduce.cpp
void job_tracker(void);
void mapper(long id);

// node.cpp
void init_node(void);
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