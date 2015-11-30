#include "header.h"

bool CSIM_END = false;

facility *F_MASTER_SWITCH, *F_DISK[NODE_NUM], *F_MEMORY[NODE_NUM];
facility_ms *FM_CPU[NODE_NUM], *FM_RACK_SWTICH[RACK_NUM];
mailbox *M_MAPPER[MAP_SLOTS_MAX];

extern "C" void sim(void)
{
	create("sim");

	setup();
	init();
	workload();
	sim_report();

	event_list_empty.wait();
	report();
}