#include "header.h"

node_map_t HEARTBEAT;

extern node_t NODES[NODE_NUM];
extern slot_t MAPPER[MAP_SLOTS_MAX];
extern long REMAIN_MAP_TASKS;
extern mailbox *M_MAPPER[MAP_SLOTS_MAX];
extern bool CSIM_END;
extern std::list<job_t*> MAP_QUEUE;
extern std::map<long, file_t*> FILE_MAP;
extern long REPORT_NODE_STATE_COUNT[STATE_LENGTH];
extern table *T_TURNAROUND_TIME, *T_QDELAY_TIME, *T_TASK_TIMES[O_LENGTH];
extern table *T_LOCALITY[LOCAL_LENGTH];
extern long SETUP_MODE_TYPE;
extern bool MANAGER_CS[NODE_NUM];
extern long REPORT_NODE_STATE_COUNT_PG[STATE_LENGTH];
extern double REPORT_RESP_T_TOTAL, REPORT_Q_DELAY_T_TOTAL;
extern long REPORT_RESP_T_COUNT, REPORT_Q_DELAY_T_COUNT;
extern long REPORT_LOCALITY[LOCAL_LENGTH];
extern std::pair<double, long> REPORT_TASK_T, REPORT_CPU_T, REPORT_MEM_T, REPORT_DISK_T, REPORT_NETWORK_T, REPORT_TASK_Q_T;

void job_tracker(void)
{
	msg_t *msg;
	long i;
	node_t *node;

	create("job_tracker");
	while (!CSIM_END) {
	//	node_map_t heartbeat, reuse;
		node_map_t heartbeat;

		if (MAP_QUEUE.empty() == false && HEARTBEAT.empty() == false) {
			heartbeat = HEARTBEAT;
			for (node_map_t::iterator it = heartbeat.begin(); it != heartbeat.end(); NULL) {
				if (it->second->mapper.used == it->second->mapper.capacity)
					heartbeat.erase(it++);
				else ++it;
			}
		}

		/*while ((heartbeat.empty() == false || reuse.empty() == false)
			&& MAP_QUEUE.empty() == false)*/
		while (heartbeat.empty() == false && MAP_QUEUE.empty() == false) {
			//if (heartbeat.empty() == true && reuse.empty() == false)
			//{	// reusing skipped heartbeats
			//	heartbeat = reuse;
			//	reuse.clear();
			//}

			i = uniform_int(0, heartbeat.size() - 1);
			node_map_t::iterator it = heartbeat.begin();
			std::advance(it, uniform_int(0, heartbeat.size() - 1));
			node = it->second;
			heartbeat.erase(node->id);

			if ((msg = scheduler(node->id)) == NULL) {
//				reuse[node->id] = node;
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
	node_t *parent = &NODES[node];
	LocalTypes locality;
	block_t *block;
	file_t *file;
	job_t *job;

	sprintf(str, "mapper%ld", id);
	create(str);
	while (true) {
		M_MAPPER[id]->receive((long*)&r);

		double task_t = clock;
		job = r->task.job;
		locality = r->task.locality;
		++job->running;
		++job->run_total;

		double qdelay = abs(clock - job->time.qin);
		job->time.qin = clock;
		job->time.qtotal += qdelay;
		T_TASK_TIMES[O_QDELAY]->record(qdelay);
		REPORT_TASK_Q_T.first += qdelay;
		REPORT_TASK_Q_T.second++;

		block = r->task.block;

		// erase job split using cascade
		for (long_map_t::iterator it = job->map_cascade[block->id].begin();
			it != job->map_cascade[block->id].end(); ++it) {
			long tn = it->first;
			long tr = it->second;

			job->map_splits[tr][tn][block->id] = job->map_splits[tr][tn][block->id] - 1;
			if (job->map_splits[tr][tn][block->id] <= 0) {
				job->map_splits[tr][tn].erase(block->id);
				if (job->map_splits[tr][tn].empty() == true) {
					job->map_splits[tr].erase(tn);
					if (job->map_splits[tr].empty() == true) {
						job->map_splits.erase(tr);
					}
				}
			}
		}
		
		if (job->map_splits.empty() == true) {
			MAP_QUEUE.remove(job);
		}
		file = FILE_MAP[block->file_id];
		++file->acc[job->id];
		++parent->mapper.used;
		MAPPER[id].used = true;

		if (locality == LOCAL_NODE) {
			if (cache_hit(node, block->id)) {
				double mem = clock;
				node_mem(node, 1);
				T_TASK_TIMES[O_MEMORY]->record(abs(clock - mem));
				REPORT_MEM_T.first += abs(clock - mem);
				REPORT_MEM_T.second++;
			}
			else {
				double disk = clock;
				node_disk(node, 1);
				T_TASK_TIMES[O_DISK]->record(abs(clock - disk));
				REPORT_DISK_T.first += abs(clock - disk);
				REPORT_DISK_T.second++;
			}
		}
		else {
			double network = clock;
			local_node = r->task.local_node;
			local_rack = GET_RACK_FROM_NODE(local_node);
			
			if (cache_hit(local_node, block->id)) {
				node_mem(local_node, 1);
			}
			else {
				node_disk(local_node, 1);				
			}
			mem_caching(local_node, block->id);
			switch_rack(local_rack, rack);

			T_TASK_TIMES[O_NETWORK]->record(abs(clock - network));
			REPORT_NETWORK_T.first += abs(clock - network);
			REPORT_NETWORK_T.second++;
		}

		double cpu = clock;
		node_cpu(node, MAP_COMPUTATION_TIME);
		T_TASK_TIMES[O_CPU]->record(abs(clock - cpu));
		REPORT_CPU_T.first += abs(clock - cpu);
		REPORT_CPU_T.second++;	

		mem_caching(node, block->id);

		if (--file->acc[job->id] <= 0) {
			file->acc.erase(job->id);
		}
		MAPPER[id].used = false;
		--parent->mapper.used;

		if (--job->running == 0 && job->map_splits.empty() == true) {
			// complete job
			job->time.end = clock;
			double turnaround_t = abs(job->time.end - job->time.begin);
			T_TURNAROUND_TIME->record(turnaround_t);
			T_QDELAY_TIME->record(job->time.qtotal);
			REPORT_RESP_T_TOTAL += turnaround_t;
			++REPORT_RESP_T_COUNT;
			REPORT_Q_DELAY_T_TOTAL += job->time.qtotal;
			++REPORT_Q_DELAY_T_COUNT;
		}

		if (node >= CS_NODE_NUM) {
			++REPORT_LOCALITY[locality];
			T_LOCALITY[locality]->record(1.0);
		}
		--REMAIN_MAP_TASKS;
		REPORT_TASK_T.first += abs(clock - task_t);
		REPORT_TASK_T.second++;

		delete r;
	}
}