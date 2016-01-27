#include "header.h"

bool GROWING_PHASE;
long REMAIN_MAP_TASKS;
long MAX_JOB_ID;
long CURRENT_MAP_TASKS;
double CHANGE_T, CUR_INT, NEXT_INT;
std::map<long, job_t*> JOB_MAP;
std::list<job_t*> MAP_QUEUE;

extern bool CSIM_END;
extern std::vector<file_t*> FILE_VEC[CS_RACK_NUM];
extern double SETUP_DATA_SKEW;
extern double SETUP_LOAD_SCENARIO[5];
extern long REPORT_MAP_TASKS;

void scenario(void)
{
	if (FB_WORKLOAD == false) {
		create("scenario");
		GROWING_PHASE = true;
		CHANGE_T = 0.5 * HOUR;
		CUR_INT = SETUP_LOAD_SCENARIO[0];
		NEXT_INT = SETUP_LOAD_SCENARIO[1];
		hold(1.5 * HOUR);

		CHANGE_T = 4.5 * HOUR;
		CUR_INT = SETUP_LOAD_SCENARIO[1];
		NEXT_INT = SETUP_LOAD_SCENARIO[2];
		hold(3.0 * HOUR);

		GROWING_PHASE = false;
		hold(1.5 * HOUR);

		GROWING_PHASE = true;
		CHANGE_T = 6.0 * HOUR;
		CUR_INT = SETUP_LOAD_SCENARIO[2];
		NEXT_INT = SETUP_LOAD_SCENARIO[3];
		hold(1.0 * HOUR);

		CHANGE_T = 10.0 * HOUR;
		CUR_INT = SETUP_LOAD_SCENARIO[3];
		NEXT_INT = SETUP_LOAD_SCENARIO[4];
		hold(3.0 * HOUR);

		GROWING_PHASE = false;
		hold(1.5 * HOUR);

		CSIM_END = true;
	}
}

void workload(void)
{
	if (FB_WORKLOAD == true) {
		long i, n, max;
		long job_id, file_id, maps, shuffles, reduces;
		double sec, gen_t, hold_t;
		file_t *file;
		job_t *job;
		std::vector<block_t*>::iterator iter;
		FILE *f;

		if ((f = fopen(FB_PATH, "r")) == NULL)
		{
			exit(0);
		}

		create("workload");
		while (EOF != fscanf(f, "%ld%lf%lf%ld%ld%ld%ld", &job_id, &gen_t, &hold_t, &maps, &shuffles, &reduces, &file_id))
		{
			hold(hold_t);

			maps = ceil((double)maps * FB_LOAD_RATIO);

			if (maps > 0) {
				sec = clock;
				job = new job_t;
				job->id = MAX_JOB_ID++;
				job->running = 0;
				job->time.begin = sec;
				job->time.end = 0;
				job->time.qin = sec;
				job->time.qtotal = 0;
				job->map_total = 0;
				job->skipcount = 0;	// for delay scheduler
				job->run_total = 0;
				job->map_splits.clear();
				job->map_cascade.clear();

				while (job->map_total < maps) {
					n = rand_zipf();
					max = FILE_VEC[n].size() - 1;
					i = uniform_int(0, max);
					file = FILE_VEC[n].at(i);

					for (iter = file->blocks.begin(); iter != file->blocks.end() && job->map_total < maps; ++iter) {
						block_t *b = *iter;
						node_map_t::iterator it = b->local_node.begin(), itend = b->local_node.end();
						while (it != itend) {
							long tn = it->first;
							long tr = GET_RACK_FROM_NODE(tn);
							job->map_splits[tr][tn][b->id] = job->map_splits[tr][tn][b->id] + 1;
							job->map_cascade[b->id][tn] = tr;

							++it;
						}
						++job->map_total;
					}
				}

				REMAIN_MAP_TASKS += maps;
				REPORT_MAP_TASKS += maps;
				CURRENT_MAP_TASKS += maps;
				JOB_MAP[job->id] = job;
				MAP_QUEUE.push_back(job);

				if (LOGGING) {
					char log[BUFSIZ];
					sprintf(log, "%ld	<workload>	JID %ld, map total: %ld\n", (long)clock, job->id, job->map_total);
					logging(log);
				}
			}
		}
		fclose(f);
		CSIM_END = true;
	}
	else {
		long i, n, max;
		double sec, period = 1.0 * HOUR;;
		double u, std, t;
		file_t *file;
		job_t *job;
		std::vector<block_t*>::iterator iter;

		create("workload");
		while (!CSIM_END) {
			sec = clock;
			job = new job_t;
			job->id = MAX_JOB_ID++;
			job->running = 0;
			job->time.begin = sec;
			job->time.end = 0;
			job->time.qin = sec;
			job->time.qtotal = 0;
			job->map_total = 0;
			job->skipcount = 0;	// for delay scheduler
			job->run_total = 0;
			job->map_splits.clear();
			job->map_cascade.clear();

			while (job->map_total < JOB_MAP_TASK_NUM) {
				n = rand_zipf();
				max = FILE_VEC[n].size() - 1;
				i = uniform_int(0, max);
				file = FILE_VEC[n].at(i);

				for (iter = file->blocks.begin(); iter != file->blocks.end() && job->map_total < JOB_MAP_TASK_NUM; ++iter) {
					block_t *b = *iter;
					node_map_t::iterator it = b->local_node.begin(), itend = b->local_node.end();
					while (it != itend) {
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
			CURRENT_MAP_TASKS += JOB_MAP_TASK_NUM;
			JOB_MAP[job->id] = job;
			MAP_QUEUE.push_back(job);

			if (LOGGING) {
				char log[BUFSIZ];
				sprintf(log, "%ld	<workload>	JID %ld, map total: %ld\n", (long)clock, job->id, job->map_total);
				logging(log);
			}

			if (sec > CHANGE_T) {
				if (CUR_INT > NEXT_INT)
					u = CUR_INT - (abs(CUR_INT - NEXT_INT) / period * (sec - CHANGE_T));
				else
					u = CUR_INT + (abs(CUR_INT - NEXT_INT) / period * (sec - CHANGE_T));
			}
			else {
				u = CUR_INT;
			}

			std = u * 0.05;
			t = normal(u, std);

			hold(t);
		}
	}
}