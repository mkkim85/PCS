#include "header.h"

double REPORT_KW, REPORT_TOTAL_KW;
double REPORT_KW_NPG, REPORT_TOTAL_KW_NPG; // non-primary groups
long REPORT_RACK_STATE_COUNT[STATE_LENGTH], REPORT_NODE_STATE_COUNT[STATE_LENGTH];
long REPORT_NODE_STATE_COUNT_PG[STATE_LENGTH];

extern bool CSIM_END;
extern long SETUP_REPORT_PERIOD;
extern FILE *SETUP_REPORT_PATH;
extern node_map_t ACTIVE_NODE_SET, STANDBY_NODE_SET;
extern rack_map_t ACTIVE_RACK_SET, STANDBY_RACK_SET;

void sim_report(void)
{
	long t;
	long hold_t = SETUP_REPORT_PERIOD;

	fprintf(SETUP_REPORT_PATH, "clock,kW,total_kW,kW_npg,total_kW_npg,node,rack\n");

	create("report");
	while (!CSIM_END)
	{
		t = clock;

		REPORT_KW = REPORT_KW
			+ (REPORT_RACK_STATE_COUNT[STATE_ACTIVE] * (RACK_POWER / HOUR))
			+ (REPORT_NODE_STATE_COUNT[STATE_STANDBY] * (NODE_S_POWER / HOUR))
			+ (REPORT_NODE_STATE_COUNT[STATE_ACTIVATE] * (NODE_U_POWER / HOUR))
			+ (REPORT_NODE_STATE_COUNT[STATE_DEACTIVATE] * (NODE_D_POWER / HOUR))
			+ (REPORT_NODE_STATE_COUNT[STATE_IDLE] * (NODE_I_POWER / HOUR))
			+ (REPORT_NODE_STATE_COUNT[STATE_PEAK] * (NODE_P_POWER / HOUR));

		REPORT_KW_NPG = REPORT_KW_NPG
			+ ((REPORT_RACK_STATE_COUNT[STATE_ACTIVE] - CS_RACK_NUM) * (RACK_POWER / HOUR))
			+ (REPORT_NODE_STATE_COUNT[STATE_STANDBY] * (NODE_S_POWER / HOUR))
			+ (REPORT_NODE_STATE_COUNT[STATE_ACTIVATE] * (NODE_U_POWER / HOUR))
			+ (REPORT_NODE_STATE_COUNT[STATE_DEACTIVATE] * (NODE_D_POWER / HOUR))
			+ ((REPORT_NODE_STATE_COUNT[STATE_IDLE] - REPORT_NODE_STATE_COUNT_PG[STATE_IDLE]) * (NODE_I_POWER / HOUR))
			+ ((REPORT_NODE_STATE_COUNT[STATE_PEAK] - REPORT_NODE_STATE_COUNT_PG[STATE_PEAK]) * (NODE_P_POWER / HOUR));

		REPORT_KW /= 1000.0;
		REPORT_KW_NPG /= 1000.0;

		if (t > 0 && (t % hold_t) == 0)
		{
			REPORT_TOTAL_KW += REPORT_KW;
			REPORT_TOTAL_KW_NPG += REPORT_KW_NPG;

			fprintf(SETUP_REPORT_PATH, "%ld,%lf,%lf,%lf,%lf,%ld,%ld\n",
				(long)clock, REPORT_KW, REPORT_TOTAL_KW, 
				REPORT_KW_NPG, REPORT_TOTAL_KW_NPG,
				ACTIVE_NODE_SET.size(), ACTIVE_RACK_SET.size());

			REPORT_KW = 0;
			REPORT_KW_NPG = 0;
		}

		hold(TIME_UNIT);
	}
}