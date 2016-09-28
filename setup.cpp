#include "header.h"

long SETUP_MODE_TYPE, SETUP_LIMIT_K, SETUP_NODE_UPTIME, SETUP_REPORT_PERIOD, SETUP_TIME_WINDOW;
long SETUP_RANDOM_SEED, SETUP_FILE_SIZE, SETUP_SCHEDULER_TYPE;
double SETUP_COMPUTATION_TIME, SETUP_RACK_POWER_RATIO, SETUP_RACK_SWITCH_SPEED;
double SETUP_DATA_LAYOUT, SETUP_DATA_SKEW, SETUP_BUDGET_RATIO, SETUP_ALPHA;
double SETUP_LOAD_SCENARIO[5];
char SETUP_REPORT_PATH[BUFSIZ];
FILE *SETUP_REPORT_FILE, *SETUP_SIM_OUTPUT;

void setup(void)
{
	char str[BUFSIZ];
	char path[BUFSIZ];

	SETUP_REPORT_PERIOD = 30;
	SETUP_SCHEDULER_TYPE = 1;

	scanf("%ld%ld%ld%ld%lf%lf%lf%ld%lf%lf%lf%lf%ld",
		&SETUP_MODE_TYPE,
		&SETUP_LIMIT_K,
		&SETUP_TIME_WINDOW,
		&SETUP_FILE_SIZE,
		&SETUP_DATA_LAYOUT,
		&SETUP_DATA_SKEW,
		&SETUP_BUDGET_RATIO,
		&SETUP_NODE_UPTIME,
		&SETUP_ALPHA,
		&SETUP_COMPUTATION_TIME,
		&SETUP_RACK_POWER_RATIO,
		&SETUP_RACK_SWITCH_SPEED,
		&SETUP_RANDOM_SEED
	);

	scanf("%lf%lf%lf%lf%lf",
		&SETUP_LOAD_SCENARIO[0], 
		&SETUP_LOAD_SCENARIO[1], 
		&SETUP_LOAD_SCENARIO[2], 
		&SETUP_LOAD_SCENARIO[3], 
		&SETUP_LOAD_SCENARIO[4]);

	SETUP_REPORT_PERIOD *= MINUTE;
	SETUP_TIME_WINDOW *= MINUTE;

	if (FB_WORKLOAD == true) {
		sprintf(path,
			"%ldM%ldK%ldTL%ldF%.2lfL%.2lfF%.2lfB%ldT%ldR%ldC%.1lfA%.0lft%.1lfR%.0lfG%.1lfP%ldS",
			SETUP_MODE_TYPE,
			SETUP_LIMIT_K,
			(SETUP_TIME_WINDOW / MINUTE),
			(SETUP_FILE_SIZE / JOB_MAP_TASK_NUM),
			SETUP_DATA_LAYOUT,
			SETUP_DATA_SKEW,
			SETUP_BUDGET_RATIO,
			SETUP_NODE_UPTIME,
			REPLICATION_FACTOR, MAP_SLOTS,
			SETUP_ALPHA,
			SETUP_COMPUTATION_TIME,
			SETUP_RACK_POWER_RATIO,
			SETUP_RACK_SWITCH_SPEED,
			SETUP_LOAD_SCENARIO[1],
			SETUP_RANDOM_SEED
		);
	}
	else {
		sprintf(path,
			"%ldM%ldK%ldTL%ldF%.2lfL%.1lfS%.2lfB%ldT%ldR%ldC%.1lfA%.0lft%.1lfR%.0lfG%.1lfP%ldS",
			SETUP_MODE_TYPE,
			SETUP_LIMIT_K,
			(SETUP_TIME_WINDOW / MINUTE),
			(SETUP_FILE_SIZE / JOB_MAP_TASK_NUM),
			SETUP_DATA_LAYOUT,
			SETUP_DATA_SKEW,
			SETUP_BUDGET_RATIO,
			SETUP_NODE_UPTIME,
			REPLICATION_FACTOR, MAP_SLOTS,
			SETUP_ALPHA,
			SETUP_COMPUTATION_TIME,
			SETUP_RACK_POWER_RATIO,
			SETUP_RACK_SWITCH_SPEED,
			SETUP_LOAD_SCENARIO[1],
			SETUP_RANDOM_SEED
		);
	}

	SETUP_RACK_POWER_RATIO = (double)(4.8 * SETUP_RACK_POWER_RATIO);
	SETUP_RACK_SWITCH_SPEED = (double)(0.5 / SETUP_RACK_SWITCH_SPEED);

	struct stat *stat_buf = new struct stat;
	if ((stat("_report", stat_buf)) == -1) {
		system("mkdir _report");
	}
	delete stat_buf;

	//sprintf(str, "_report\\%s.txt", path);
	//SETUP_SIM_OUTPUT = fopen(str, "w");
	sprintf(str, "_report\\%s.csv", path);
	strcpy(SETUP_REPORT_PATH, str);
	SETUP_REPORT_FILE = fopen(str, "w");
}