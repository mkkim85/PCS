#include "header.h"

double LOAD_SCENARIO[] = { 170.666666666667, 78.7692307692308, 102.4, 51.2, 170.666666666667 };
long REMAIN_MAP_TASKS;
long MAX_JOB_ID;
double INTERVAL_MEAN;
std::map<long, job_t*> JOB_MAP;
std::list<job_t*> MAP_QUEUE;

extern bool CSIM_END;
extern std::vector<file_t*> FILE_VEC[CS_RACK_NUM];
extern double SETUP_DATA_SKEW;
extern long REPORT_MAP_TASKS;

void scenario(void)
{
	create("scenario");
	INTERVAL_MEAN = LOAD_SCENARIO[0];
	hold(1.5 * HOUR);

	INTERVAL_MEAN = LOAD_SCENARIO[1];
	hold(3.0 * HOUR);

	INTERVAL_MEAN = LOAD_SCENARIO[2];
	hold(1.5 * HOUR);

	INTERVAL_MEAN = LOAD_SCENARIO[3];
	hold(3.0 * HOUR);

	INTERVAL_MEAN = LOAD_SCENARIO[4];
	hold(1.5 * HOUR);

	CSIM_END = true;
}

void workload(void)
{
	long i, n, max;
	file_t *file;
	job_t *job;
	std::vector<block_t*>::iterator iter;

	create("workload");
	while (!CSIM_END)
	{
		job = new job_t;
		job->id = MAX_JOB_ID++;
		job->running = 0;
		job->time.begin = clock;
		job->time.end = 0;
		job->time.qin = clock;
		job->time.qtotal = 0;
		job->map_total = 0;
		job->skipcount = 0;	// for delay scheduler
		job->run_total = 0;
		job->map_splits.clear();
		job->map_cascade.clear();

		while (job->map_total < JOB_MAP_TASK_NUM)
		{
			n = rand_zipf();
			max = FILE_VEC[n].size() - 1;
			i = uniform_int(0, max);
			file = FILE_VEC[n].at(i);

			for (iter = file->blocks.begin(); iter != file->blocks.end(); ++iter)
			{
				block_t *b = *iter;
				node_map_t::iterator it = b->local_node.begin(), itend = b->local_node.end();
				while (it != itend)
				{
					long tn = it->first;
					long tr = GET_RACK_FROM_NODE(tn);
					job->map_splits[tr][tn][b->id] = job->map_splits[tr][tn][b->id] + 1;
					job->map_cascade[b->id][tn] = tr;

					++it;
				}
				++job->map_total;
			}
		}

		REMAIN_MAP_TASKS += JOB_MAP_TASK_NUM;
		REPORT_MAP_TASKS += JOB_MAP_TASK_NUM;
		JOB_MAP[job->id] = job;
		MAP_QUEUE.push_back(job);

		if (LOGGING)
		{
			char log[BUFSIZ];
			sprintf(log, "%ld	<workload>	JID %ld, map total: %ld\n", (long)clock, job->id, job->map_total);
			logging(log);
		}

		double u = INTERVAL_MEAN;
		double std = INTERVAL_MEAN * 0.05;
		double t = normal(u, std);
		hold(t);
	}
}