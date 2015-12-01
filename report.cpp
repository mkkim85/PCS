#include "header.h"

long REPORT_RACK_STATE_COUNT[STATE_LENGTH], REPORT_NODE_STATE_COUNT[STATE_LENGTH];

extern bool CSIM_END;
extern long SETUP_REPORT_PERIOD;

void sim_report(void)
{
	long t;
	long hold_t = SETUP_REPORT_PERIOD * 60; // to minutes

	create("report");
	while (!CSIM_END)
	{
		t = clock;

		if (t > 0 && (t % hold_t) == 0)
		{
			// TODO: report
		}

		hold(1);
	}
}