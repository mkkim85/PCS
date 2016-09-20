#include "header.h"

extern long SETUP_SCHEDULER_TYPE;
extern long SETUP_MODE_TYPE;
extern CAtlList<job_t*> MAP_QUEUE;
extern node_t NODES[NODE_NUM];
extern node_map_t ACTIVE_NODE_SET;
extern long REPORT_BUDGET_HIT;
extern CRBMap<long, CAtlList<long>*> P_QUEUE;
extern CAtlMap<long, job_t*> JOB_MAP;

msg_t * scheduler(long node)
{
	long i, slotid;
	long rack = GET_RACK_FROM_NODE(node);
	msg_t *msg = NULL;
	block_t *block;
	job_t *job;
	node_t *nptr = &NODES[node];
	CAtlList<long> *queue = NULL;
	POSITION pos = NULL, pos2 = NULL;

	for (i = 0; i < MAP_SLOTS; i++) {
		if (nptr->mapper.slot[i]->used == false) {
			slotid = nptr->mapper.slot[i]->id;
			break;
		}
	}

	pos = P_QUEUE.GetHeadPosition();
	while (pos != NULL) {
		queue = P_QUEUE.GetValueAt(pos);
		for (pos2 = queue->GetHeadPosition(); pos2 != NULL; queue->GetNext(pos2)) {
			job = JOB_MAP[queue->GetAt(pos2)];

			msg = new msg_t;
			msg->task.id = slotid;
			msg->task.job = job;
			msg->task.locality = LOCAL_LENGTH;

			if (job->map_splits.Lookup(rack) != NULL) {
				if (job->map_splits[rack].Lookup(node) != NULL) {
					// local node
					msg->task.block = GetBlock(job->map_splits[rack][node].GetKeyAt(job->map_splits[rack][node].GetStartPosition()));
					msg->task.locality = LOCAL_NODE;
					msg->task.local_node = node;
					job->skipcount = 0;
					return msg;
				}
				// local rack
				POSITION pos3 = job->map_splits[rack].GetStartPosition();
				long it = uniform_int(0, job->map_splits[rack].GetCount() - 1);
				while (it--)
					job->map_splits[rack].GetNext(pos3);

				msg->task.block = GetBlock(job->map_splits[rack].GetValueAt(pos3).GetKeyAt(job->map_splits[rack].GetValueAt(pos3).GetStartPosition()));
				msg->task.locality = LOCAL_RACK;
				msg->task.local_node = job->map_splits[rack].GetKeyAt(pos3);
			}
			else {
				// local remote
				POSITION pos3 = job->map_splits.GetStartPosition();
				long it = uniform_int(0, job->map_splits.GetCount() - 1);
				while (it--)
					job->map_splits.GetNext(pos3);

				POSITION pos4 = job->map_splits.GetAt(pos3)->m_value.GetStartPosition();
				long it2 = uniform_int(0, job->map_splits.GetAt(pos3)->m_value.GetCount() - 1);
				while (it2--)
					job->map_splits.GetAt(pos3)->m_value.GetNext(pos4);

				msg->task.block = GetBlock(
					job->map_splits.GetAt(pos3)->m_value.GetAt(pos4)->m_value.GetKeyAt(
						job->map_splits.GetAt(pos3)->m_value.GetAt(pos4)->m_value.GetStartPosition()));
				msg->task.locality = LOCAL_REMOTE;
				msg->task.local_node = job->map_splits.GetAt(pos3)->m_value.GetAt(pos4)->m_key;
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
				long N = job->map_splits.GetCount();
				long M = 1200;		// ACTIVE_NODE_SET.size();
				double R = 3.0;		// (double)M / CS_NODE_NUM;
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
		}
		P_QUEUE.GetNext(pos);
	}
	return NULL;
}