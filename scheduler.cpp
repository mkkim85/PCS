#include "header.h"

long SKIPPABLE_FACTOR = 10;

extern long SETUP_SCHEDULER_TYPE;
extern long SETUP_MODE_TYPE;
extern std::list<job_t*> MAP_QUEUE;
extern node_t NODES[NODE_NUM];
extern node_map_t ACTIVE_NODE_SET;

bool sort_queue(const job_t *x, const job_t *y)
{
	return x->running < y->running;
}

msg_t * scheduler(long node)
{
	long i, slotid, select, skipcount;
	long rack = GET_RACK_FROM_NODE(node);
	long skippable;
	msg_t *msg = NULL;
	block_t *block;
	job_t *job;
	node_t *nptr = &NODES[node];
	std::list<job_t*> queue;
	std::vector<block_t*>::iterator biter;

	for (i = 0; i < MAP_SLOTS; ++i)
	{
		if (nptr->mapper.slot[i]->used == false)
		{
			slotid = nptr->mapper.slot[i]->id;
			break;
		}
	}

	queue = MAP_QUEUE;
	queue.sort(sort_queue);

	std::list<job_t*>::iterator it = queue.begin(), itend = queue.end();
	while (it != itend)
	{
		job = *it;

		msg = new msg_t;
		msg->task.id = slotid;
		msg->task.job = job;
		msg->task.locality = LOCAL_LENGTH;
		skipcount = job->skipcount;

		for (i = 0, biter = job->map_splits.begin(); biter != job->map_splits.end(); ++biter, ++i)
		{
			block = *biter;
			if (block->local_node.find(node) != block->local_node.end())
			{
				msg->task.split_index = i;
				msg->task.locality = LOCAL_NODE;
				msg->task.local_node = node;
				job->skipcount = 0;
				return msg;
			}
			else if ((msg->task.locality == LOCAL_REMOTE || msg->task.locality == LOCAL_LENGTH)
				&& block->local_rack.find(rack) != block->local_rack.end())
			{
				node_map_t::iterator iter;
				for (iter = block->local_node.begin(); iter != block->local_node.end(); ++iter)
				{
					long tnid = iter->second->id;
					if (GET_RACK_FROM_NODE(tnid) == rack &&
						(NODES[tnid].state == STATE_IDLE || NODES[tnid].state == STATE_PEAK))
					{
						msg->task.split_index = i;
						msg->task.locality = LOCAL_RACK;
						msg->task.local_node = tnid;
						break;
					}
				}
			}
			else if (msg->task.locality == LOCAL_LENGTH)
			{
				do {
					node_map_t::iterator it = block->local_node.begin();
					std::advance(it, uniform_int(0, block->local_node.size() - 1));
					nptr = it->second;
				} while (nptr->state == STATE_STANDBY || nptr->state == STATE_ACTIVATE || nptr->state == STATE_DEACTIVATE);
				msg->task.split_index = i;
				msg->task.locality = LOCAL_REMOTE;
				msg->task.local_node = nptr->id;
			}
		}
		if (SETUP_SCHEDULER_TYPE == FAIR_SCHEDULER
			&& msg->task.locality != LOCAL_LENGTH)
		{
			return msg;
		}
		else if (SETUP_SCHEDULER_TYPE == DELAY_SCHEDULER)
		{
			// maximum skip count, D
			// wish to achieve locality greater than gamma
			// for jobs with N tasks on a cluster with M nodes
			// and  replicaiton factor R
			// equation: D >= -M/R ln(((1-gamma)N)/(1+(1-gamma)N))
			// <ref> Zaharia, Matei, et al. "Delay scheduling: a simple technique for achieving locality and fairness in cluster scheduling." Proceedings of the 5th European conference on Computer systems. ACM, 2010. </ref>
			double gamma = 0.95;
			long N = job->map_splits.size();
			double R = (double)ACTIVE_NODE_SET.size() / CS_NODE_NUM;
			long M = ACTIVE_NODE_SET.size();
			double D = -(M / R) * log(((1 - gamma) * N) / (1 + (1 - gamma) * N));
			if (msg->task.locality == LOCAL_NODE
				|| (skipcount >= (long)ceil(D) && msg->task.locality != LOCAL_LENGTH))
			{
				return msg;
			}
			++job->skipcount;
		}
		delete msg;
		++it;
	}

	return NULL;
}