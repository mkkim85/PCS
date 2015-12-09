#include "header.h"

double SETUP_DATA_SKEW = 0.0;
double SETUP_BUDGET_RATIO = 0.1;
double SETUP_ALPHA = 0.3;
double SETUP_BETA = 0.3;
long SETUP_REPORT_PERIOD = 1 * MINUTE;
long SETUP_TIME_WINDOW = 30 * MINUTE;
long SETUP_FILE_SIZE = 64;
long SETUP_RANDOM_SEED = 0;
long SETUP_SCHEDULER_TYPE = FAIR_SCHEDULER;
long SETUP_MODE_TYPE = MODE_PCS;
FILE *SETUP_REPORT_PATH, *SETUP_SIM_OUTPUT;

void setup(void)
{
	char str[BUFSIZ];
	char path[BUFSIZ];
	
	sprintf(path, "%ldS_%ldRP_%ldTW_%ldDN_%ldFN_%.1lfSKE_%.1lfBR_%.1lfA_%.1lfB_%ldSCH_%ldMOD",
		SETUP_RANDOM_SEED, SETUP_REPORT_PERIOD, (SETUP_TIME_WINDOW / MINUTE), (DATA_BLOCK_NUM / 1024), SETUP_FILE_SIZE, SETUP_DATA_SKEW,
		SETUP_BUDGET_RATIO, SETUP_ALPHA, SETUP_BETA, SETUP_SCHEDULER_TYPE, SETUP_MODE_TYPE);

	struct stat *stat_buf = new struct stat;
	if ((stat("_report", stat_buf)) == -1)
	{
		system("mkdir _report");
	}
	delete stat_buf;

	sprintf(str, "_report\\%s.txt", path);
	SETUP_SIM_OUTPUT = fopen(str, "w");
	sprintf(str, "_report\\%s.csv", path);
	SETUP_REPORT_PATH = fopen(str, "w");
}