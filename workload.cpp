#include "header.h"

long REMAIN_MAP_TASKS;
long MAX_JOB_ID;
std::map<long, job_t*> JOB_MAP;
std::list<job_t*> MAP_QUEUE;
double SCENARIO[][2] = { { 30 * MINUTE, 60 }, { 120 * MINUTE, 35 }, { 180 * MINUTE, 40 }, { 270 * MINUTE, 20 }, { 300 * MINUTE, 60 } };

extern bool CSIM_END;
extern std::vector<file_t*> FILE_VEC[CS_RACK_NUM];

void workload(void)
{
//	double load_scenario[] = { 1, 0.5, 0.1,
	long i, n, max;
	file_t *file;
	job_t *job;
	std::vector<block_t*>::iterator iter;

	create("workload");
	while (!CSIM_END)
	{
		job = new job_t;
		job->id = MAX_JOB_ID;
		job->running = 0;
		job->time.begin = clock;
		job->time.end = 0;
		job->time.qin = clock;
		job->time.qtotal = 0;
		job->map_total = 0;
		job->skipcount = 0;	// for delay scheduler

		while (job->map_splits.size() < JOB_MAP_TASK_NUM)
		{
			n = rand_zipf();
			max = FILE_VEC[n].size() - 1;
			i = uniform_int(0, max);
			file = FILE_VEC[n].at(i);

			for (iter = file->blocks.begin(); iter != file->blocks.end(); ++iter)
			{
				job->map_splits.push_back(*iter);
				++job->map_total;
			}
		}
		REMAIN_MAP_TASKS += job->map_total;
		JOB_MAP[MAX_JOB_ID] = job;
		++MAX_JOB_ID;
		MAP_QUEUE.push_back(job);

		if (LOGGING)
		{
			char log[BUFSIZ];
			sprintf(log, "%ld	<workload>	JID %ld, map total: %ld\n", (long)clock, job->id, job->map_total);
			logging(log);
		}

		// TODO: job scenario
		if (clock >= 3600)
		{
			CSIM_END = true;
		}

		hold(30);
	}
}