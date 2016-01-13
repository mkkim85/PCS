#include "header.h"

rack_t RACKS[RACK_NUM];
rack_map_t ACTIVE_RACK_SET, STANDBY_RACK_SET;
rack_map_t ACTIVE_RACK_NPG_SET, NPG_SET;

extern facility *F_MASTER_SWITCH;
extern facility_ms *FM_RACK_SWTICH[RACK_NUM];
extern long REPORT_RACK_STATE_COUNT[STATE_LENGTH];
extern long SETUP_MODE_TYPE;

void init_rack(void)
{
	char str[20];
	long i;
	rack_t *rack;

	for (i = 0; i < RACK_NUM; ++i) {
		rack = &RACKS[i];
		rack->id = i;
		rack->rank = 0;
		if (SETUP_MODE_TYPE == MODE_BASELINE || i < CS_RACK_NUM) {
			rack->state = STATE_ACTIVE;
			ACTIVE_RACK_SET[i] = rack;
		}
		else {
			rack->state = STATE_STANDBY;
			STANDBY_RACK_SET[i] = rack;
			NPG_SET[i] = rack;
		}

		sprintf(str, "rSwitch%ld", i);
		FM_RACK_SWTICH[i] = new facility_ms(str, SWTICH_NUM);

		++REPORT_RACK_STATE_COUNT[rack->state];
	}
}

void turnon_rack(long id)
{
	rack_t *rack = &RACKS[id];

	if (rack->state == STATE_ACTIVE) {
		return;
	}

	--REPORT_RACK_STATE_COUNT[rack->state];
	rack->state = STATE_ACTIVE;
	++REPORT_RACK_STATE_COUNT[rack->state];
	STANDBY_RACK_SET.erase(id);
	ACTIVE_RACK_SET[id] = rack;
	ACTIVE_RACK_NPG_SET[id] = rack;

	if (LOGGING) {
		char log[BUFSIZ];
		sprintf(log, "%ld	<turnon_rack>	%ld\n", (long)clock, id);
		logging(log);
	}
}

void turnoff_rack(long id)
{
	rack_t *rack = &RACKS[id];

	if (rack->state == STATE_STANDBY) {
		return;
	}

	--REPORT_RACK_STATE_COUNT[rack->state];
	rack->state = STATE_STANDBY;
	++REPORT_RACK_STATE_COUNT[rack->state];
	ACTIVE_RACK_SET.erase(id);
	STANDBY_RACK_SET[id] = rack;
	ACTIVE_RACK_NPG_SET.erase(id);

	if (LOGGING) {
		char log[BUFSIZ];
		sprintf(log, "%ld	<turnoff_rack>	%ld\n", (long)clock, id);
		logging(log);
	}
}

void switch_rack(long from, long to)
{
	if (from != to) {
		FM_RACK_SWTICH[from]->use(uniform(0, SWITCH_DELAY) * MAP_COMPUTATION_TIME);
		F_MASTER_SWITCH->use(uniform(0, SWITCH_DELAY) * MAP_COMPUTATION_TIME);
	}
	FM_RACK_SWTICH[to]->use(uniform(0, SWITCH_DELAY) * MAP_COMPUTATION_TIME);

	if (LOGGING) {
		char log[BUFSIZ];
		sprintf(log, "%ld	<switch_rack>	%ld -> %ld\n", (long)clock, from, to);
		logging(log);
	}
}

void switch_rack(long from, long to, long n)
{	// For budget data transfer
	double t = SWITCH_SPEED * n;
	if (from != to) {
		FM_RACK_SWTICH[from]->use(t);
		F_MASTER_SWITCH->use(t);
	}
	FM_RACK_SWTICH[to]->use(t);

	if (LOGGING) {
		char log[BUFSIZ];
		sprintf(log, "%ld	<switch_rack>	%ld -> %ld (%ld blocks)\n", (long)clock, from, to, n);
		logging(log);
	}
}