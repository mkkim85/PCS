#include "cpp.h"

const double simTime = 8*60.0; // parameters, constants 
const long numberWorkers = 5;
const long numberInspectors = 2; 
const double arrTime = 2.0; 
const double assemblyTime = 3.0; 
const double reworkTime = 1.5; 
const double inspectTime = 2.0; 
const double failRate = 0.20;

facility initialPrep("initial"); // facilities
facility_ms assembly("assmbly", numberWorkers);
facility_ms inspect("inspect", numberInspectors);
facility finalPrep("final");
facility scrapPrep("scrap"); 

FILE *fp; 

void generate(); // prototypes 
void TVSet(); 

extern "C" void sim() // main process 
{
	fp = fopen("xxx.out", "w"); 
	set_output_file(fp);
	create("sim");
	generate(); // start generate process
	hold(simTime); // lengthof run
	report(); // producereport
}

void generate() // generator process 
{ 
	create("gen"); 
	while(1) {
		TVSet(); // start TV Set process
		hold(exponential(arrTime)); // timebetween TV Sets
	}
} 

void TVSet() // TV Set process
{
	long inspectCt = 0; 
	double stationTime;
	long exitCond = 0; 
	create("TVSet"); 
	initialPrep.use(0.5); // prep station 
	stationTime = assemblyTime; 
	do { 
		assembly.use(exponential(stationTime)); // assembly station
		inspect.use(exponential(inspectTime)); // inspect station
		inspectCt++;
		if(bernoulli(failRate) == 0) { // outcome of inspection
			finalPrep.use(0.5); // passed -> final prep
			exitCond = 1; 
		}
		else {
			if(inspectCt < 2) { 
				stationTime = reworkTime; // failed 1st, rework 
			}
			else { 
				scrapPrep.use(0.5); // failed 2nd, -> scrap 
				exitCond = 1;
			}
		}
	} while(exitCond == 0);
}
