// C++/CSIM Model of M/M/1 queue

#include "cpp.h"			// class definitions
#include <stdio.h>
#pragma optimize ("y", on)

#define NARS 5000
#define IAR_TM 2.0
#define SRV_TM 1.0

event *done;			// pointer to event done
facility *f;			// pointer to facility f
table *tbl;				// pointer to table of response times
qtable *qtbl;			// pointer to qtable of number in system
process_class *pc;		// pointer to process class
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
	mdlstat();			// model statistics
}

void generateCustomers()
{
	create("gen");
	for(int i = 1; i <= NARS; i++) {
		hold(exponential(IAR_TM));	// interarrival interval
		customer();					// generate next customer
	}
}

void customer()				// arriving customer
{
	double t1;

	create("cust");
	pc->set_process_class();
	t1 = clock;				// record start time
	qtbl->note_entry();		// note arrival
	f->reserve();			// reserve facility
	hold(exponential(SRV_TM));	// service interval
	f->release();			// release facility
	tbl->tabulate(clock - t1);	// record response time
	qtbl->note_exit();		// note departure
	if(--cnt == 0)
		done->set();		// if last customer, set event done
}

void init()
{
	fp = fopen("csim.out", "w");
	set_output_file(fp);
	set_model_name("M/M/1 Queue");

	done = new event("done");		// instantiate event done
	f = new facility("facility");	// instantiate facility f
	tbl = new table("resp tms");	// instantiate table
	qtbl = new qtable("num in sys");// instantiate qtable
	qtbl->add_histogram(10, 0, 10);	// add histogram to qtable
	cnt = NARS;						// initialize counter
	pc = new process_class("pc");
	collect_class_facility_all();
}

