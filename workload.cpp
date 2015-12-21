#include "header.h"

bool LOAD_PHASE_GROWING = true;
long REMAIN_MAP_TASKS;
long MAX_JOB_ID;
double LOAD_INTERVAL;
std::map<long, job_t*> JOB_MAP;
std::list<job_t*> MAP_QUEUE;

extern bool CSIM_END;
extern std::vector<file_t*> FILE_VEC[CS_RACK_NUM];
extern double SETUP_DATA_SKEW;

void scenario(void)
{
	create("scenario");
	while (true)
	{
		if (clock == 0)
		{
			LOAD_PHASE_GROWING = true;
			LOAD_INTERVAL = 80;
		}
		else if (clock == 0.5 * HOUR)
		{
			LOAD_PHASE_GROWING = true;
			if (0.0 <= SETUP_DATA_SKEW && SETUP_DATA_SKEW < 0.25)
			{
				LOAD_INTERVAL = 38;
			}
			else if (0.25 <= SETUP_DATA_SKEW && SETUP_DATA_SKEW < 0.5)
			{
				LOAD_INTERVAL = 39;
			}
			else if (0.5 <= SETUP_DATA_SKEW && SETUP_DATA_SKEW < 0.75)
			{
				LOAD_INTERVAL = 40;
			}
			else if (0.75 <= SETUP_DATA_SKEW && SETUP_DATA_SKEW < 1.0)
			{
				LOAD_INTERVAL = 41;
			}
			else if (1.0 <= SETUP_DATA_SKEW)
			{
				LOAD_INTERVAL = 42;
			}
		}
		else if (clock == 2.0 * HOUR)
		{
			LOAD_PHASE_GROWING = false;
			LOAD_INTERVAL += 3;
		}
		else if (clock == 3.0 * HOUR)
		{
			LOAD_PHASE_GROWING = true;
			if (0.0 <= SETUP_DATA_SKEW && SETUP_DATA_SKEW < 0.25)
			{
				LOAD_INTERVAL = 22;
			}
			else if (0.25 <= SETUP_DATA_SKEW && SETUP_DATA_SKEW < 0.5)
			{
				LOAD_INTERVAL = 23;
			}
			else if (0.5 <= SETUP_DATA_SKEW && SETUP_DATA_SKEW < 0.75)
			{
				LOAD_INTERVAL = 24;
			}
			else if (0.75 <= SETUP_DATA_SKEW && SETUP_DATA_SKEW < 1.0)
			{
				LOAD_INTERVAL = 25;
			}
			else if (1.0 <= SETUP_DATA_SKEW)
			{
				LOAD_INTERVAL = 26;
			}
		}
		else if (clock == 4.5 * HOUR)
		{
			LOAD_PHASE_GROWING = false;
			LOAD_INTERVAL = 80;
		}
		else if (clock == 5.0 * HOUR)
		{
			CSIM_END = true;
			break;
		}

		hold(MINUTE);
	}
}

void workload(void)
{
	long i, n, max;
	double hold_t = 60;
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

		hold(LOAD_INTERVAL);
	}
}