#include "header.h"

rack_t RACKS[RACK_NUM];
rack_map_t ACTIVE_RACK_SET, STANDBY_RACK_SET;
rack_map_t ACTIVE_RACK_NPG_SET, NPG_SET;

extern facility *F_MASTER_SWITCH, *F_RACK_SWITCH[RACK_NUM];
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
		F_RACK_SWITCH[i] = new facility(str);

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
}

double switch_rack(long from, long to)
{
	double begin = clock;
	if (from != to) {
		F_RACK_SWITCH[from]->use(SWITCH_SPEED);
		F_MASTER_SWITCH->use(MASTER_SPEED);
	}
	F_RACK_SWITCH[to]->use(SWITCH_SPEED);

	return clock - begin;
}

double  switch_rack(long from, long to, double n)
{	// For budget data transfer
	double begin = clock;
	double t = SWITCH_SPEED * n;
	if (from != to) {
		F_RACK_SWITCH[from]->use(t);
		F_MASTER_SWITCH->use(MASTER_SPEED * n);
	}
	F_RACK_SWITCH[to]->use(t);

	return clock - begin;
}