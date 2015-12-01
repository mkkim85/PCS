#include "header.h"

extern long SETUP_SCHEDULER_TYPE;
extern long SETUP_MODE_TYPE;
extern std::list<job_t*> MAP_QUEUE;
extern node_t NODES[NODE_NUM];

bool sort_queue(const job_t *l, const job_t *r)
{
	return l->running < r->running;
}

msg_t* scheduler(long node)
{
	long i, select;
	long rack = GET_RACK_FROM_NODE(node);
	msg_t *msg = NULL;
	block_t *block;
	job_t *job;
	node_t *nptr = &NODES[node];
	std::list<job_t*> queue;
	std::vector<block_t*>::iterator biter;
	LocalTypes locality = LOCAL_LENGTH;

	if (SETUP_SCHEDULER_TYPE == FAIR_SCHEDULER)
	{
		for (i = 0; i < MAP_SLOTS; ++i)
		{
			if (nptr->mapper.slot[i]->used == false)
			{
				break;
			}
		}

		queue = MAP_QUEUE;
		queue.sort(sort_queue);
		job = queue.front();

		msg = new msg_t;
		msg->task.id = nptr->mapper.slot[i]->id;
		msg->task.job = job;

		for (i = 0, biter = job->map_splits.begin(); biter != job->map_splits.end(); ++biter, ++i)
		{
			block = *biter;
			if (find(block->local_node.begin(), block->local_node.end(), node) != block->local_node.end())
			{
				locality = LOCAL_NODE;
				msg->task.split_index = i;
				msg->task.locality = locality;
				msg->task.local_node = node;
				return msg;
			}
			else if ((locality == LOCAL_REMOTE || locality == LOCAL_LENGTH)
				&& find(block->local_rack.begin(), block->local_rack.end(), rack) != block->local_rack.end())
			{
				std::vector<long>::iterator iter;
				for (iter = block->local_node.begin(); iter != block->local_node.end(); ++iter)
				{
					if (GET_RACK_FROM_NODE(*iter) == rack &&
						(NODES[*iter].state == STATE_IDLE || NODES[*iter].state == STATE_PEAK))
					{
						locality = LOCAL_RACK;
						msg->task.split_index = i;
						msg->task.locality = locality;
						msg->task.local_node = *iter;
					}
				}
			}
			else if (locality == LOCAL_LENGTH)
			{
				do {
					select = uniform_int(0, REPLICATION_FACTOR - 1);
					nptr = &NODES[block->local_node.at(select)];
				} while (nptr->state == STATE_STANDBY || nptr->state == STATE_ACTIVATE || nptr->state == STATE_DEACTIVATE);
				locality = LOCAL_REMOTE;
				msg->task.split_index = i;
				msg->task.locality = locality;
				msg->task.local_node = nptr->id;
			}
		}
		if (msg->task.locality != LOCAL_LENGTH)
		{
			return msg;
		}
	}
	else if (SETUP_SCHEDULER_TYPE == DELAY_SCHEDULER)
	{

	}

	return NULL;
}