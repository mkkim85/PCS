// C++/CSIM Model of M/M/1 queue

#include "cpp.h"			// class definitions
#include <stdio.h>

#define RACK_NUM 3
#define NODE_NUM 66
#define CPU_NUM NODE_NUM
#define DISK_NUM (NODE_NUM * 2)
#define IAR_TM 2.0
#define SRV_TM 1.0

event		*done;			// pointer to event done

facility	*f;				// pointer to facility f
facility_set *ncpu, *ndisk;

table		*tbl;			// pointer to table of response times
qtable		*qtbl;			// pointer to qtable of number in system

FILE		*fp;

void init();

extern "C" void sim(int argc, char *argv[])
{
	init();
	create("sim");
	report();			// model report
	mdlstat();			// model statistics
}

void init()
{
	fp = fopen("csim.out", "w");
	set_output_file(fp);
	set_model_name("None Title");

	done = new event("done");		// instantiate event done
	f = new facility("facility");	// instantiate facility f
	ncpu = new facility_set("ncpu", CPU_NUM);
	ndisk = new facility_set("ndisk", DISK_NUM);
	tbl = new table("resp tms");	// instantiate table
	qtbl = new qtable("num in sys");// instantiate qtable
	qtbl->add_histogram(10, 0, 10);	// add histogram to qtable
}