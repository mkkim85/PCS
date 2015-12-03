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
			if (block->local_node.find(node) != block->local_node.end())
			{
				locality = LOCAL_NODE;
				msg->task.split_index = i;
				msg->task.locality = locality;
				msg->task.local_node = node;
				return msg;
			}
			else if ((locality == LOCAL_REMOTE || locality == LOCAL_LENGTH)
				&& block->local_rack.find(rack) != block->local_rack.end())
			{
				std::map<long, node_t*>::iterator iter;
				for (iter = block->local_node.begin(); iter != block->local_node.end(); ++iter)
				{
					long tnid = iter->second->id;
					if (GET_RACK_FROM_NODE(tnid) == rack &&
						(NODES[tnid].state == STATE_IDLE || NODES[tnid].state == STATE_PEAK))
					{
						locality = LOCAL_RACK;
						msg->task.split_index = i;
						msg->task.locality = locality;
						msg->task.local_node = tnid;
					}
				}
			}
			else if (locality == LOCAL_LENGTH)
			{
				do {
					select = uniform_int(0, REPLICATION_FACTOR - 1);
					std::map<long, node_t*>::iterator iter = block->local_node.begin();
					for (long cnt = 0; cnt < select; ++cnt)
					{
						++iter;
					}
					nptr = iter->second;
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