// C++/CSIM Model of M/M/1 queue

#include "cpp.h"			// class definitions
#include <stdio.h>

#define NARS 1
#define IAR_TM 2.0
#define SRV_TM 1.0

event *done;			// pointer to event done
facility *f;			// pointer to facility f
stream *s;
int cnt;				// count of remaining processes
FILE *fp;

void init();
void generateCustomers();
void customer();

extern "C" void sim(int argc, char *argv[])
{
	init();
	create("sim");
	generateCustomers();
	done->wait();		// wait for last customer to depart
	report();			// model report
}

void generateCustomers()
{
	create("gen");
	for(int i = 1; i <= NARS; i++) {
		hold(s->exponential(IAR_TM));	// interarrival interval
		customer();					// generate next customer
	}
	csim_terminate();
}

void customer()				// arriving customer
{
	create("cust");
	f->reserve();			// reserve facility
	hold(s->exponential(SRV_TM));	// service interval
	f->release();			// release facility
	if(--cnt == 0)
		done->set();		// if last customer, set event done
}

extern "C" int stackTrace;

extern "C" void initialize_csim();

void init()
{
	initialize_csim();
	fp = fopen("csim.out", "w");
	set_output_file(fp);
	set_trace_file(fp);
	trace_on();
	stackTrace = 1;
	done = new event("done");		// instantiate event done
	f = new facility("facility");	// instantiate facility f
	cnt = NARS;						// initialize counter
	s = new stream(1);
}

