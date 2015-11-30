#include "header.h"

extern slot_t MAPPER[MAP_SLOTS_MAX];
extern long REMAIN_MAP_TASKS;
extern mailbox *M_MAPPER[MAP_SLOTS_MAX];

void mapper(long id)
{
	msg_t *r;
	char str[20];

	sprintf(str, "mapper%ld", id);
	create(str);
	while (true)
	{
		M_MAPPER[id]->receive((long*)&r);

		// TODO: map task handler
	}
}