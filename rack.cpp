#include "header.h"

rack_t RACKS[RACK_NUM];
std::vector<rack_t*> ACTIVE_RACK_SET, STANDBY_RACK_SET;

void init_rack(void)
{
	long i;
	rack_t *rack;

	for (i = 0; i < RACK_NUM; ++i)
	{
		rack = &RACKS[i];
		rack->id = i;
		if (i < CS_RACK_NUM) {
			rack->state = STATE_ACTIVE;
			ACTIVE_RACK_SET.push_back(rack);
		}
		else {
			rack->state = STATE_STANDBY;
			STANDBY_RACK_SET.push_back(rack);
		}
	}
}