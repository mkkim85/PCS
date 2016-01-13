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
	long i, slotid;
	long rack = GET_RACK_FROM_NODE(node);
	msg_t *msg = NULL;
	block_t *block;
	job_t *job;
	node_t *nptr = &NODES[node];
	std::list<job_t*> queue;
	std::list<job_t*>::iterator it, itend;

	for (i = 0; i < MAP_SLOTS; ++i) {
		if (nptr->mapper.slot[i]->used == false) {
			slotid = nptr->mapper.slot[i]->id;
			break;
		}
	}

	queue = MAP_QUEUE;
	queue.sort(sort_queue);

	it = queue.begin();
	itend = queue.end();
	while (it != itend) {
		job = *it;

		msg = new msg_t;
		msg->task.id = slotid;
		msg->task.job = job;
		msg->task.locality = LOCAL_LENGTH;

		if (job->map_splits.find(rack) != job->map_splits.end()) {
			if (job->map_splits[rack].find(node) != job->map_splits[rack].end()) {
				// local node
				msg->task.block = GetBlock(job->map_splits[rack][node].begin()->first);
				msg->task.locality = LOCAL_NODE;
				msg->task.local_node = node;
				job->skipcount = 0;
				return msg;
			}
			// local rack
			std::map<long, std::map<long, long>>::iterator nit = job->map_splits[rack].begin();
			std::advance(nit, uniform_int(0, job->map_splits[rack].size() - 1));
			msg->task.block = GetBlock(nit->second.begin()->first);
			msg->task.locality = LOCAL_RACK;
			msg->task.local_node = nit->first;
		}
		else {
			// local remote
			std::map<long, std::map<long, std::map<long, long>>>::iterator rit = job->map_splits.begin();
			std::advance(rit, uniform_int(0, job->map_splits.size() - 1));
			std::map<long, std::map<long, long>>::iterator nit = rit->second.begin();
			std::advance(nit, uniform_int(0, rit->second.size() - 1));
			msg->task.block = GetBlock(nit->second.begin()->first);
			msg->task.locality = LOCAL_REMOTE;
			msg->task.local_node = nit->first;
		}

		if (SETUP_SCHEDULER_TYPE == FAIR_SCHEDULER && msg->task.locality != LOCAL_LENGTH) {
			return msg;
		}
		else if (SETUP_SCHEDULER_TYPE == DELAY_SCHEDULER) {
			// maximum skip count, D
			// wish to achieve locality greater than gamma
			// for jobs with N tasks on a cluster with M nodes
			// and  replicaiton factor R
			// equation: D >= -M/R ln(((1-gamma)N)/(1+(1-gamma)N))
			// <ref> Zaharia, Matei, et al. "Delay scheduling: a simple technique for achieving locality and fairness in cluster scheduling." Proceedings of the 5th European conference on Computer systems. ACM, 2010. </ref>
			double gamma = 0.95;
			long N = job->map_splits.size();
			long M = ACTIVE_NODE_SET.size();
			double R = (double)M / CS_NODE_NUM;
			double D = (double)-(M / R) * log(((1 - gamma) * N) / (1 + (1 - gamma) * N));
			if (job->skipcount >= (long)ceil(D / 2) && msg->task.locality == LOCAL_RACK) {
				return msg;
			}
			if (job->skipcount >= (long)ceil(D) && msg->task.locality != LOCAL_LENGTH) {
				return msg;
			}
			++job->skipcount;
		}
		delete msg;
		++it;
	}

	return NULL;
}