#include "header.h"

node_t NODES[NODE_NUM];
std::vector<node_t*> ACTIVE_NODE_SET, STANDBY_NODE_SET;
slot_t MAPPER[MAP_SLOTS_MAX];
long MAX_MAPPER_ID;
std::list<long> MEMORY[NODE_NUM];

extern rack_t RACKS[RACK_NUM];
extern double SETUP_BUDGET_RATIO;
extern facility *F_DISK[NODE_NUM], *F_MEMORY[NODE_NUM];
extern facility_ms *FM_CPU[NODE_NUM];
extern mailbox *M_MAPPER[MAP_SLOTS_MAX];
extern long REPORT_NODE_STATE_COUNT[STATE_LENGTH];
extern long MANAGER_MAP_SLOT_CAPACITY;

void init_node(void)
{
	char str[20];
	long i, j;
	node_t *node;
	rack_t *rack;
	slot_t *slot;

	for (i = 0; i < NODE_NUM; ++i)
	{
		node = &NODES[i];
		node->id = i;
		node->mapper.capacity = MAP_SLOTS;
		node->mapper.used = 0;
		rack = &RACKS[i / NODE_NUM_IN_RACK];
		node->space.capacity = DISK_SIZE;
		node->space.used = 0;
		node->space.disk.capacity = DISK_SIZE * (1 - SETUP_BUDGET_RATIO); 
		node->space.disk.used = 0;
		node->space.budget.capacity = DISK_SIZE * SETUP_BUDGET_RATIO; 
		node->space.budget.used = 0;

		for (j = 0; j < MAP_SLOTS; ++j)
		{
			slot = &MAPPER[MAX_MAPPER_ID];
			slot->id = MAX_MAPPER_ID;
			slot->used = false;
			node->mapper.slot[j] = slot;

			sprintf(str, "mMail%ld", slot->id);
			M_MAPPER[slot->id] = new mailbox(str);
			mapper(slot->id);

			++MAX_MAPPER_ID;
		}

		if (i < CS_NODE_NUM) {
			node->state = STATE_IDLE;
			rack->active_node_set.push_back(node);
			ACTIVE_NODE_SET.push_back(node);
			MANAGER_MAP_SLOT_CAPACITY += MAP_SLOTS;
		}
		else {
			node->state = STATE_STANDBY;
			rack->standby_node_set.push_back(node);
			STANDBY_NODE_SET.push_back(node);
		}
		++REPORT_NODE_STATE_COUNT[node->state];

		sprintf(str, "cpu%ld", i);
		FM_CPU[i] = new facility_ms(str, CPU_CORE);
		FM_CPU[i]->set_servicefunc(rnd_rob);

		sprintf(str, "mem%ld", i);
		F_MEMORY[i] = new facility(str);

		MEMORY[i].clear();

		sprintf(str, "disk%ld", i);
		F_DISK[i] = new facility(str);
	}
}

void mem_caching(long nid, long bid)
{
	std::list<long> *mem = &MEMORY[nid];
	std::list<long>::iterator iter;

	if (mem->size() >= MEMORY_SIZE)
	{
		iter = find(mem->begin(), mem->end(), bid);

		if (iter != mem->end())
		{
			mem->remove(bid);
			if (LOGGING)
			{
				char log[BUFSIZ];
				sprintf(log, "%ld	<mem_caching>	node: %ld, b: %ld replaced\n", (long)clock, nid, bid);
				logging(log);
			}
		}
		else
		{
			if (LOGGING)
			{
				char log[BUFSIZ];
				sprintf(log, "%ld	<mem_caching>	node: %ld, b: %ld removed\n", (long)clock, nid, mem->back());
				logging(log);
			}
			mem->pop_back();
		}
	}
	MEMORY[bid].push_front(bid);

	if (LOGGING)
	{
		char log[BUFSIZ];
		sprintf(log, "%ld	<mem_caching>	node: %ld, b: %ld cached\n", (long)clock, nid, bid);
		logging(log);
	}
}