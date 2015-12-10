#include "header.h"

std::vector<long> HEARTBEAT;

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

void job_tracker(void)
{
	msg_t *msg;
	long i, node;

	create("job_tracker");
	while (!CSIM_END)
	{
		std::vector<long> heartbeat;

		if (MAP_QUEUE.empty() == false && HEARTBEAT.empty() == false)
		{
			heartbeat = HEARTBEAT;
		}

		while (heartbeat.empty() == false && MAP_QUEUE.empty() == false)
		{
			i = uniform_int(0, heartbeat.size() - 1);
			node = heartbeat.at(i);
			heartbeat.erase(heartbeat.begin() + i);

			if ((msg = scheduler(node)) == NULL)
			{
				continue;
			}
			if (msg != NULL &&
				(msg->task.split_index < 0 || msg->task.split_index >= msg->task.job->map_splits.size()))
			{
				msg = msg;
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
	double cpu_use_t;
	LocalTypes locality;
	block_t *block;
	file_t *file;
	job_t *job;

	sprintf(str, "mapper%ld", id);
	create(str);
	while (true)
	{
		M_MAPPER[id]->receive((long*)&r);

		job = r->task.job;
		locality = r->task.locality;
		if (job->running++ == 0)
		{
			double qdelay = abs(clock - job->time.qin);
			job->time.qtotal += qdelay;
			T_TASK_TIMES[O_QDELAY]->record(qdelay);
		}
		block = job->map_splits.at(r->task.split_index);
		job->map_splits.erase(job->map_splits.begin() + r->task.split_index);
		if (job->map_splits.size() == 0)
		{
			MAP_QUEUE.remove(job);
		}
		file = FILE_MAP[block->file_id];
		++file->acc[job->id];
		if (NODES[node].mapper.used++ == 0)
		{
			--REPORT_NODE_STATE_COUNT[NODES[node].state];
			if (node < CS_NODE_NUM) --REPORT_NODE_STATE_COUNT_PG[NODES[node].state];
			NODES[node].state = STATE_PEAK;
			if (node < CS_NODE_NUM) ++REPORT_NODE_STATE_COUNT_PG[NODES[node].state];
			++REPORT_NODE_STATE_COUNT[NODES[node].state];
		}
		if (NODES[node].mapper.used == NODES[node].mapper.capacity)
		{
			std::vector<long>::iterator iter = find(HEARTBEAT.begin(), HEARTBEAT.end(), node);
			if (iter != HEARTBEAT.end())
			{
				HEARTBEAT.erase(find(HEARTBEAT.begin(), HEARTBEAT.end(), node));
			}
		}
		MAPPER[id].used = true;

		if (locality == LOCAL_NODE)
		{
			cpu_use_t = MAP_COMPUTATION_TIME / 2;
			if (cache_hit(node, block->id))
			{
				double mem = clock;
				node_mem(node, 1);
				T_TASK_TIMES[O_MEMORY]->record(abs(clock - mem));
			}
			else
			{
				double disk = clock;
				node_disk(node, 1);
				T_TASK_TIMES[O_DISK]->record(abs(clock - disk));
			}
		}
		else
		{
			cpu_use_t = MAP_COMPUTATION_TIME;
			double network = clock;
			local_node = r->task.local_node;
			local_rack = GET_RACK_FROM_NODE(local_node);
			
			if (cache_hit(local_node, block->id))
			{
				node_mem(local_node, 1);
			}
			else
			{
				node_disk(local_node, 1);				
			}
			mem_caching(local_node, block->id);
			switch_rack(local_rack, rack, 1);

			T_TASK_TIMES[O_NETWORK]->record(abs(clock - network));
		}

		double cpu = clock;
		node_cpu(node, cpu_use_t);
		T_TASK_TIMES[O_CPU]->record(abs(clock - cpu));

		mem_caching(node, block->id);

		if (--file->acc[job->id] <= 0)
		{
			file->acc.erase(job->id);
		}
		MAPPER[id].used = false;
		if (--NODES[node].mapper.used == 0)
		{
			--REPORT_NODE_STATE_COUNT[NODES[node].state];
			if (node < CS_NODE_NUM) --REPORT_NODE_STATE_COUNT_PG[NODES[node].state];
			NODES[node].state = STATE_IDLE;
			if (node < CS_NODE_NUM) ++REPORT_NODE_STATE_COUNT_PG[NODES[node].state];
			++REPORT_NODE_STATE_COUNT[NODES[node].state];
		}
		if ((MANAGER_CS[node] == true)
			&& (NODES[node].mapper.used + 1 == NODES[node].mapper.capacity))
		{
			HEARTBEAT.push_back(node);
		}
		if (--job->running == 0 && job->map_splits.size() > 0)
		{
			job->time.qin = clock; // queue wait start
		}
		if (job->running == 0 && job->map_splits.size() == 0)
		{
			// complete job
			job->time.end = clock;
			T_TURNAROUND_TIME->record(abs(job->time.end - job->time.begin));
			T_QDELAY_TIME->record(job->time.qtotal);
		}
		T_LOCALITY[locality]->record(1.0);
		--REMAIN_MAP_TASKS;

		delete r;
	}
}