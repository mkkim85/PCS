#include "header.h"

node_t NODES[NODE_NUM];
std::vector<node_t*> ACTIVE_NODE_SET, STANDBY_NODE_SET;
slot_t MAPPER[MAP_SLOTS_MAX];
long MAX_MAPPER_ID;

extern rack_t RACKS[RACK_NUM];

void init_node(void)
{
	long i, j;
	node_t *node;
	rack_t *rack;
	slot_t *slot;

	for (i = 0; i < NODE_NUM; ++i)
	{
		node = &NODES[i];
		node->id = i;
		node->mthread = 0;
		rack = &RACKS[i / NODE_NUM_IN_RACK];

		for (j = 0; j < MAP_SLOTS; ++j)
		{
			slot = &MAPPER[MAX_MAPPER_ID];
			slot->id = MAX_MAPPER_ID;
			slot->busy = false;
			node->mslot[j] = slot;
			++MAX_MAPPER_ID;
		}

		if (i < CS_NODE_NUM) {
			node->state = STATE_IDLE;
			rack->active_node_set.push_back(node);
			ACTIVE_NODE_SET.push_back(node);
		}
		else {
			node->state = STATE_STANDBY;
			rack->standby_node_set.push_back(node);
			STANDBY_NODE_SET.push_back(node);
		}
	}
}