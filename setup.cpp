#include "header.h"

long SETUP_MODE_TYPE;
long SETUP_REPORT_PERIOD;
double SETUP_DATA_SKEW;
double SETUP_BUDGET_RATIO;
double SETUP_ALPHA;
double SETUP_BETA;
long SETUP_TIME_WINDOW;
long SETUP_FILE_SIZE;
long SETUP_RANDOM_SEED;
long SETUP_SCHEDULER_TYPE;
double SETUP_LOAD_SCENARIO[5];
char SETUP_REPORT_PATH[BUFSIZ];
FILE *SETUP_REPORT_FILE, *SETUP_SIM_OUTPUT;

void setup(void)
{
	char str[BUFSIZ];
	char path[BUFSIZ];
	
	scanf("%ld%ld%ld%ld%lf%lf%lf%lf%ld%ld",
		&SETUP_MODE_TYPE, &SETUP_REPORT_PERIOD, &SETUP_TIME_WINDOW, &SETUP_FILE_SIZE, &SETUP_DATA_SKEW,
		&SETUP_BUDGET_RATIO, &SETUP_ALPHA, &SETUP_BETA, &SETUP_SCHEDULER_TYPE, &SETUP_RANDOM_SEED);

	scanf("%lf%lf%lf%lf%lf",
		&SETUP_LOAD_SCENARIO[0], 
		&SETUP_LOAD_SCENARIO[1], 
		&SETUP_LOAD_SCENARIO[2], 
		&SETUP_LOAD_SCENARIO[3], 
		&SETUP_LOAD_SCENARIO[4]);

	SETUP_REPORT_PERIOD *= MINUTE;
	SETUP_TIME_WINDOW *= MINUTE;

	sprintf(path, "%ldMOD_%ldRF_%ldRP_%ldTW_%ldDN_%ldFN_%.2lfSKE_%.4lfBR_%.1lfA_%.1lfB_%ldSCH_%ldS",
		SETUP_MODE_TYPE, REPLICATION_FACTOR, (SETUP_REPORT_PERIOD / MINUTE), (SETUP_TIME_WINDOW / MINUTE), (DATA_BLOCK_NUM / 1024), SETUP_FILE_SIZE, SETUP_DATA_SKEW,
		SETUP_BUDGET_RATIO, SETUP_ALPHA, SETUP_BETA, SETUP_SCHEDULER_TYPE, SETUP_RANDOM_SEED);

	struct stat *stat_buf = new struct stat;
	if ((stat("_report", stat_buf)) == -1) {
		system("mkdir _report");
	}
	delete stat_buf;

	sprintf(str, "_report\\%s.txt", path);
	SETUP_SIM_OUTPUT = fopen(str, "w");
	sprintf(str, "_report\\%s.csv", path);
	strcpy(SETUP_REPORT_PATH, str);
	SETUP_REPORT_FILE = fopen(str, "w");
}