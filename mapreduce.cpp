#include "header.h"

node_map_t HEARTBEAT;

extern long USER_COUNT;
extern CAtlMap<long, job_t*> JOB_MAP;
extern CAtlMap<long, CAtlMap<double, long>> FILE_HISTORY;
extern node_t NODES[NODE_NUM];
extern slot_t MAPPER[MAP_SLOTS_MAX];
extern long REMAIN_MAP_TASKS, REPORT_BUDGET_HIT;
extern mailbox *M_MAPPER[MAP_SLOTS_MAX];
extern bool CSIM_END;
//extern CAtlList<job_t*> MAP_QUEUE;
extern CAtlMap<long, file_t*> FILE_MAP;
extern long REPORT_NODE_STATE_COUNT[STATE_LENGTH];
//extern table *T_TURNAROUND_TIME, *T_QDELAY_TIME, *T_TASK_TIMES[O_LENGTH];
//extern table *T_LOCALITY[LOCAL_LENGTH];
extern long SETUP_MODE_TYPE;
extern double SETUP_COMPUTATION_TIME;
extern bool MANAGER_CS[NODE_NUM];
extern long REPORT_NODE_STATE_COUNT_PG[STATE_LENGTH];
extern double REPORT_RESP_T_TOTAL, REPORT_Q_DELAY_T_TOTAL;
extern long REPORT_RESP_T_COUNT, REPORT_Q_DELAY_T_COUNT;
extern long REPORT_LOCALITY[LOCAL_LENGTH];
extern std::pair<double, long> REPORT_TASK_T, REPORT_CPU_T, REPORT_MEM_T, REPORT_DISK_T, REPORT_NETWORK_T, REPORT_TASK_Q_T;
extern CAtlMap<long, CAtlMap<long, bool>> BUDGET_MAP;
extern CRBMap<long, CAtlList<long>*> P_QUEUE;

void job_tracker(void)
{
	msg_t *msg;
	long i;
	node_t *node;
	CAtlArray<node_t*> heartbeat;

	create("job_tracker");
	
	while (!CSIM_END) {
		//	node_map_t heartbeat, reuse;
		heartbeat.RemoveAll();

		if (P_QUEUE.IsEmpty() == false && HEARTBEAT.IsEmpty() == false) {
			POSITION pos = HEARTBEAT.GetHeadPosition();
			while (pos != NULL) {
				node_map_t::CPair *pair = HEARTBEAT.GetAt(pos);
				if (pair->m_value->mapper.used != pair->m_value->mapper.capacity)
					heartbeat.Add(pair->m_value);
				HEARTBEAT.GetNext(pos);
			}
		}

		while (heartbeat.IsEmpty() == false && P_QUEUE.IsEmpty() == false) {
			i = uniform_int(0, heartbeat.GetCount() - 1);
			node = heartbeat[i];
			heartbeat.RemoveAt(i);

			if ((msg = scheduler(node->id)) == NULL) {
				continue;
			}
			M_MAPPER[msg->task.id]->synchronous_send((long)msg);
		}

		hold(HEARTBEAT_PERIOD);
	}
}

void mapper(long id)
{
	msg_t *r;
	char str[20];
	long node = GET_NODE_FROM_MAPPER(id), local_node;
	long rack = GET_RACK_FROM_NODE(node), local_rack;
	long group = GET_G_FROM_RACK(rack);
	double btask, task_t, bcpu, cpu_t, bmem, mem_t, bdisk, disk_t, q_t, net_t;
	node_t *parent = &NODES[node];
	LocalTypes locality;
	block_t *block;
	file_t *file;
	job_t *job;

	sprintf(str, "mapper%ld", id);
	create(str);
	
	while (true) {
		M_MAPPER[id]->receive((long*)&r);
		btask = task_t = bcpu = cpu_t = bmem = mem_t = bdisk = disk_t = q_t = net_t = 0;

		btask = clock;
		job = r->task.job;
		locality = r->task.locality;
		
		P_QUEUE.Lookup(job->running)->m_value->RemoveAt(P_QUEUE.Lookup(job->running)->m_value->Find(job->id));
		if (P_QUEUE.Lookup(job->running)->m_value->IsEmpty() == true) {
			delete P_QUEUE.Lookup(job->running)->m_value;
			P_QUEUE.RemoveKey(job->running);
		}
		++job->running;
		++job->run_total;
		if (job->run_total < job->map_total) {
			if (P_QUEUE.Lookup(job->running) == NULL) {
				P_QUEUE.SetAt(job->running, new CAtlList<long>);
			}
			P_QUEUE.Lookup(job->running)->m_value->AddTail(job->id);
		}

		q_t = abs(clock - job->time.begin);
		job->time.qtotal += q_t;

		block = r->task.block;

		// erase job split using cascade
		POSITION tpos = job->map_cascade[block->id].GetStartPosition();
		while (tpos != NULL) {
			long_map_t::CPair *pair = job->map_cascade[block->id].GetAt(tpos);
			long tn = pair->m_key;
			long tr = pair->m_value;

			job->map_splits[tr][tn][block->id] = job->map_splits[tr][tn][block->id] - 1;
			if (job->map_splits[tr][tn][block->id] <= 0) {
				job->map_splits[tr][tn].RemoveKey(block->id);
				if (job->map_splits[tr][tn].IsEmpty() == true) {
					job->map_splits[tr].RemoveKey(tn);
					if (job->map_splits[tr].IsEmpty() == true) {
						job->map_splits.RemoveKey(tr);
					}
				}
			}
			
			job->map_cascade[block->id].GetNext(tpos);
		}

		file = FILE_MAP[block->file_id];
		long psiz = file->acc.GetCount();
		if (file->acc.Lookup(job->id) == NULL)
			file->acc[job->id] = 1;
		else ++file->acc[job->id];
		long csiz = file->acc.GetCount();
		if ((SETUP_MODE_TYPE == MODE_IPACS || SETUP_MODE_TYPE == MODE_PCS || SETUP_MODE_TYPE == MODE_PCSC)
			&& csiz > 1 && psiz != csiz) {
			FILE_HISTORY[file->id].SetAt(clock, csiz);
			//FILE_HISTORY[file->id].AddTail(std::pair<double, long>(clock, csiz));
		}
		++parent->mapper.used;
		MAPPER[id].used = true;

		if (SETUP_MODE_TYPE == MODE_PCS || SETUP_MODE_TYPE == MODE_PCSC) {
			if (BUDGET_MAP.Lookup(block->id) != NULL) {
				if ((BUDGET_MAP[block->id].Lookup(node) != NULL 
					&& r->task.locality == LOCAL_NODE)
					|| (r->task.locality == LOCAL_RACK 
						&& BUDGET_MAP[block->id].Lookup(r->task.local_node) != NULL
						&& GET_RACK_FROM_NODE(r->task.local_node) == GET_RACK_FROM_NODE(node))) {
//					bblock_use(r->task.local_node, block->id);
					++REPORT_BUDGET_HIT;
				}
			}
		}

		if (locality == LOCAL_NODE) {
			if (cache_hit(node, block->id)) {
				bmem = clock;
				node_mem(node, 1);
				mem_t = abs(clock - bmem);
			}
			else {
				bdisk = clock;
				node_disk(node, 1);
				disk_t = clock - bdisk;
			}
		}
		else {
			local_node = r->task.local_node;
			local_rack = GET_RACK_FROM_NODE(local_node);
			if (cache_hit(local_node, block->id)) {
				bmem = clock;
				node_mem(local_node, 1);
				mem_t = abs(clock - bmem);
			}
			else {
				bdisk = clock;
				node_disk(local_node, 1);
				disk_t = clock - bdisk;
			}
			mem_caching(local_node, block->id);
			net_t = switch_rack(local_rack, rack);
		}
		mem_caching(node, block->id);

		bcpu = clock;
		node_cpu(node, SETUP_COMPUTATION_TIME);
		cpu_t = abs(clock - bcpu);
//		T_TASK_TIMES[O_CPU]->record(cpu_t);
		REPORT_CPU_T.first += cpu_t;
		REPORT_CPU_T.second++;

		if (file->acc.Lookup(job->id) != NULL && --file->acc[job->id] <= 0) {
			file->acc.RemoveKey(job->id);
		}
		MAPPER[id].used = false;
		--parent->mapper.used;

		if (job->map_splits.IsEmpty() == false) {
			P_QUEUE.Lookup(job->running)->m_value->RemoveAt(P_QUEUE.Lookup(job->running)->m_value->Find(job->id));
			if (P_QUEUE.Lookup(job->running)->m_value->IsEmpty() == true) {
				delete P_QUEUE.Lookup(job->running)->m_value;
				P_QUEUE.RemoveKey(job->running);
			}
			if (P_QUEUE.Lookup(job->running - 1) == NULL) {
				P_QUEUE.SetAt(job->running - 1, new CAtlList<long>);
			}
			P_QUEUE.Lookup(job->running - 1)->m_value->AddTail(job->id);
		}

		if (--job->running == 0 && job->map_splits.IsEmpty() == true) {
			// complete job
			job->time.end = clock;
			double turnaround_t = abs(job->time.end - job->time.begin);
//			T_TURNAROUND_TIME->record(turnaround_t);
//			T_QDELAY_TIME->record(job->time.qtotal);
			REPORT_RESP_T_TOTAL += turnaround_t;
			++REPORT_RESP_T_COUNT;
			REPORT_Q_DELAY_T_TOTAL += job->time.qtotal;
			++REPORT_Q_DELAY_T_COUNT;
			JOB_MAP.RemoveKey(job->id);
			USER_COUNT--;
			delete job;
		}

		if (node >= CS_NODE_NUM) {
			++REPORT_LOCALITY[locality];
//			T_LOCALITY[locality]->record(1.0);
		}
		--REMAIN_MAP_TASKS;

//		T_TASK_TIMES[O_MEMORY]->record(mem_t);
		REPORT_MEM_T.first += mem_t;
		REPORT_MEM_T.second++;

//		T_TASK_TIMES[O_DISK]->record(disk_t);
		REPORT_DISK_T.first += disk_t;
		REPORT_DISK_T.second++;

//		T_TASK_TIMES[O_QDELAY]->record(q_t);
		REPORT_TASK_Q_T.first += q_t;
		REPORT_TASK_Q_T.second++;

//		T_TASK_TIMES[O_NETWORK]->record(net_t);
		REPORT_NETWORK_T.first += net_t;
		REPORT_NETWORK_T.second++;

		task_t = abs(clock - btask) + q_t;
		REPORT_TASK_T.first += task_t;
		REPORT_TASK_T.second++;

		delete r;
	}
}