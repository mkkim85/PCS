#include "header.h"

extern facility *F_MASTER_SWITCH;
extern table *T_TURNAROUND_TIME, *T_QDELAY_TIME, *T_TASK_TIMES[O_LENGTH];
extern table *T_LOCALITY[LOCAL_LENGTH];
extern table *T_CACHE_HIT, *T_CACHE_MISS;
extern long SETUP_RANDOM_SEED;
extern FILE *SETUP_SIM_OUTPUT;

void init(void)
{
	char str[50];
	long i;
	
	reset_prob(SETUP_RANDOM_SEED);
	set_output_file(SETUP_SIM_OUTPUT);

	i = max_processes(MAX_PROCESSES);
	i = max_facilities(MAX_FACILITIES);
	i = max_servers(MAX_SERVERS);
	i = max_mailboxes(MAX_MAILBOXES);
	i = max_messages(MAX_MESSAGES);
	i = max_events(MAX_EVENTS);

	zipf();
	gen_file();
	init_rack();
	init_node();

	sprintf(str, "mSwitch");
	F_MASTER_SWITCH = new facility(str);
	//F_MASTER_SWITCH->set_servicefunc(inf_srv);

	extern table *T_TURNAROUND_TIME, *T_QDELAY_TIME, *T_TASK_TIMES[O_LENGTH];

	T_TURNAROUND_TIME = new table("turnaround time");
	T_QDELAY_TIME = new table("qdelay time");
	T_TASK_TIMES[O_CPU] = new table("overhead: cpu");
	T_TASK_TIMES[O_MEMORY] = new table("overhead: memory");
	T_TASK_TIMES[O_DISK] = new table("overhead: disk");
	T_TASK_TIMES[O_NETWORK] = new table("overhead: network");
	T_TASK_TIMES[O_QDELAY] = new table("overhead: qdelay");

	T_LOCALITY[LOCAL_NODE] = new table ("locality: local");
	T_LOCALITY[LOCAL_RACK] = new table ("locality: rack-local");
	T_LOCALITY[LOCAL_REMOTE] = new table ("locality: remote");

	T_CACHE_HIT = new table("cache hit");
	T_CACHE_MISS = new table("cache miss");
}