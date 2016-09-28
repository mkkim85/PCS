#include "header.h"

long USER_COUNT = 0;
long REMAIN_MAP_TASKS;
long MAX_JOB_ID;
double CHANGE_T, CUR_INT, NEXT_INT, CHG_PERIOD;
CAtlMap<long, job_t*> JOB_MAP;
CRBMap<long, CAtlList<long>*> P_QUEUE;
//long long FN[RACK_NUM];

extern bool CSIM_END;
extern CAtlArray<file_t*> FILE_VEC[MOD_FACTOR];
extern CAtlMap<long, file_t*> FILE_MAP;
extern double SETUP_DATA_SKEW, SETUP_COMPUTATION_TIME;
extern double SETUP_LOAD_SCENARIO[5];
extern long REPORT_MAP_TASKS;

void scenario(void)
{
	if (FB_WORKLOAD == false) {
		create("scenario");
		
		CHANGE_T = 0;
		CHG_PERIOD = 2 * HOUR;
		CUR_INT = SETUP_LOAD_SCENARIO[0];
		NEXT_INT = SETUP_LOAD_SCENARIO[1];
		hold(2 * HOUR);

		CHANGE_T = 3.5 * HOUR;
		CHG_PERIOD = 1.5 * HOUR;
		CUR_INT = SETUP_LOAD_SCENARIO[1];
		NEXT_INT = SETUP_LOAD_SCENARIO[2];
		hold(3 * HOUR);

		CHANGE_T = 5.5 * HOUR;
		CHG_PERIOD = 1.5 * HOUR;
		CUR_INT = SETUP_LOAD_SCENARIO[2];
		NEXT_INT = SETUP_LOAD_SCENARIO[3];
		hold(2 * HOUR);

		CHANGE_T = 8.5 * HOUR;
		CHG_PERIOD = 2 * HOUR;
		CUR_INT = SETUP_LOAD_SCENARIO[3];
		NEXT_INT = SETUP_LOAD_SCENARIO[4];
		hold(3.5 * HOUR);

		CUR_INT = SETUP_LOAD_SCENARIO[4];
		CHANGE_T = 11.0 * HOUR;
		hold(1.0);

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
		FILE *f;

		if ((f = fopen(FB_PATH, "r")) == NULL)
		{
			exit(0);
		}

		create("workload");
		
		while (EOF != fscanf(f, "%ld%lf%lf%ld%ld%ld%ld", &job_id, &gen_t, &hold_t, &maps, &shuffles, &reduces, &file_id))
		{
			hold(hold_t);

			maps = ceil((double)maps * SETUP_DATA_SKEW);

			if (maps > 0) {
				sec = clock;
				job = new job_t;
				job->id = MAX_JOB_ID++;
				job->running = 0;
				job->time.begin = sec;
				job->time.end = 0;
				job->time.qtotal = 0;
				job->map_total = 0;
				job->skipcount = 0;	// for delay scheduler
				job->run_total = 0;
				job->map_splits.RemoveAll();
				job->map_cascade.RemoveAll();
				job->user_id = NULL;

				while (job->map_total < maps) {
					file = FILE_MAP[file_id];

					for (long i = 0; i < file->blocks.GetCount() && i < maps; i++) {
						bool *bflag = new bool[file->blocks.GetCount()];
						while (job->map_total < maps) {
							long bi = uniform_int(0, file->blocks.GetCount() - 1);
							if (bflag[bi] == true)
								continue;
							bflag[bi] = true;
							block_t *b = file->blocks[bi];
							POSITION pos = b->local_node.GetHeadPosition();
							while (pos != NULL) {
								long tn = b->local_node.GetKeyAt(pos);
								long tr = GET_RACK_FROM_NODE(tn);
								job->map_splits[tr][tn][b->id] = job->map_splits[tr][tn][b->id] + 1;
								job->map_cascade[b->id][tn] = tr;

								b->local_node.GetNext(pos);
							}
							++job->map_total;
						}
						delete bflag;
					}

					REMAIN_MAP_TASKS += maps;
					REPORT_MAP_TASKS += maps;
					JOB_MAP[job->id] = job;
					if (P_QUEUE.Lookup(0) == NULL)
						P_QUEUE.SetAt(0, new CAtlList<long>);
					P_QUEUE.Lookup(0)->m_value->AddTail(job->id);
				}
			}
			fclose(f);
			CSIM_END = true;
		}
	}
	else {
		long i, n, max, max_tasks, count;
		double sec;
		double u, std, t;
		file_t *file;
		job_t *job;

		create("workload");
		
		while (!CSIM_END) {
			sec = clock;
			if (sec > CHANGE_T) {
				if (CUR_INT > NEXT_INT)
					u = CUR_INT - (abs(CUR_INT - NEXT_INT) / CHG_PERIOD * (sec - CHANGE_T));
				else
					u = CUR_INT + (abs(CUR_INT - NEXT_INT) / CHG_PERIOD * (sec - CHANGE_T));
			}
			else {
				u = CUR_INT;
			}
			count = 0;
			max_tasks = 9600 * u;
			long max_users = ceil(MAX_USERS * u);
			while (count < max_tasks) {
				//while (USER_COUNT < max_users) {
				USER_COUNT++;
				job = new job_t;
				job->id = MAX_JOB_ID++;
				job->running = 0;
				job->time.begin = sec;
				job->time.end = 0;
				job->time.qtotal = 0;
				job->map_total = 0;
				job->skipcount = 0;	// for delay scheduler
				job->run_total = 0;
				job->map_splits.RemoveAll();
				job->map_cascade.RemoveAll();

				while (job->map_total < JOB_MAP_TASK_NUM) {
					n = rand_zipf();
					max = FILE_VEC[n].GetCount() - 1;
					i = uniform_int(0, max);
					file = FILE_VEC[n][i];

					bool *bflag = new bool[file->blocks.GetCount()];
					while (job->map_total < JOB_MAP_TASK_NUM) {
						long bi = uniform_int(0, file->blocks.GetCount() - 1);
						if (bflag[bi] == true)
							continue;
						bflag[bi] = true;

						block_t *b = file->blocks[bi];
						POSITION pos = b->local_node.GetHeadPosition();
						while (pos != NULL) {
							long tn = b->local_node.GetKeyAt(pos);
							long tr = GET_RACK_FROM_NODE(tn);
							if (job->map_splits[tr][tn].Lookup(b->id) == NULL) {
								job->map_splits[tr][tn][b->id] = 1;
							}
							else {
								job->map_splits[tr][tn][b->id]++;
							}
							job->map_cascade[b->id][tn] = tr;

							b->local_node.GetNext(pos);
						}
						++job->map_total;
					}
					delete bflag;
				}
				count += JOB_MAP_TASK_NUM;
				REMAIN_MAP_TASKS += JOB_MAP_TASK_NUM;
				REPORT_MAP_TASKS += JOB_MAP_TASK_NUM;
				JOB_MAP[job->id] = job;
				if (P_QUEUE.Lookup(0) == NULL)
					P_QUEUE.SetAt(0, new CAtlList<long>);
				P_QUEUE.Lookup(0)->m_value->AddTail(job->id);
			}

			hold(SETUP_COMPUTATION_TIME + 50.0);
			//hold(30.0);
		}
	}
}

//void workload(void)
//{
//	if (FB_WORKLOAD == true) {
//		long i, n, max;
//		long job_id, file_id, maps, shuffles, reduces;
//		double sec, gen_t, hold_t;
//		file_t *file;
//		job_t *job;
//		FILE *f;
//
//		if ((f = fopen(FB_PATH, "r")) == NULL)
//		{
//			exit(0);
//		}
//
//		create("workload");
//		while (EOF != fscanf(f, "%ld%lf%lf%ld%ld%ld%ld", &job_id, &gen_t, &hold_t, &maps, &shuffles, &reduces, &file_id))
//		{
//			hold(hold_t);
//
//			maps = ceil((double)maps * SETUP_DATA_SKEW);
//
//			if (maps > 0) {
//				sec = clock;
//				job = new job_t;
//				job->id = MAX_JOB_ID++;
//				job->running = 0;
//				job->time.begin = sec;
//				job->time.end = 0;
//				job->time.qtotal = 0;
//				job->map_total = 0;
//				job->skipcount = 0;	// for delay scheduler
//				job->run_total = 0;
//				job->map_splits.RemoveAll();
//				job->map_cascade.RemoveAll();
//
//				while (job->map_total < maps) {
//					file = FILE_MAP[file_id];
//
//					//for (long i = 0; i < file->blocks.GetCount() && i < maps; i++) {
//					bool *bflag = new bool[file->blocks.GetCount()];
//					while (job->map_total < maps) {
//						long bi = uniform_int(0, file->blocks.GetCount() - 1);
//						if (bflag[bi] == true)
//							continue;
//						bflag[bi] = true;
//						block_t *b = file->blocks[bi];
//						POSITION pos = b->local_node.GetHeadPosition();
//						while (pos != NULL) {
//							long tn = b->local_node.GetKeyAt(pos);
//							long tr = GET_RACK_FROM_NODE(tn);
//							job->map_splits[tr][tn][b->id] = job->map_splits[tr][tn][b->id] + 1;
//							job->map_cascade[b->id][tn] = tr;
//
//							b->local_node.GetNext(pos);
//						}
//						++job->map_total;
//					}
//					delete bflag;
//				}
//
//				REMAIN_MAP_TASKS += maps;
//				REPORT_MAP_TASKS += maps;
//				JOB_MAP[job->id] = job;
//				if (P_QUEUE.Lookup(0) == NULL)
//					P_QUEUE.SetAt(0, new CAtlList<long>);
//				P_QUEUE.Lookup(0)->m_value->AddTail(job->id);
//			}
//		}
//		fclose(f);
//		CSIM_END = true;
//	}
//	else {
//		long i, n, max, max_tasks, count;
//		double sec;
//		double u, std, t;
//		file_t *file;
//		job_t *job;
//
//		create("workload");
//		while (!CSIM_END) {
//			sec = clock;
//			if (sec > CHANGE_T) {
//				if (CUR_INT > NEXT_INT)
//					u = CUR_INT - (abs(CUR_INT - NEXT_INT) / CHG_PERIOD * (sec - CHANGE_T));
//				else
//					u = CUR_INT + (abs(CUR_INT - NEXT_INT) / CHG_PERIOD * (sec - CHANGE_T));
//			}
//			else {
//				u = CUR_INT;
//			}
//			count = 0;
//			max_tasks = 9600 * u;
//
//			while (count < max_tasks) {
//				job = new job_t;
//				job->id = MAX_JOB_ID++;
//				job->running = 0;
//				job->time.begin = sec;
//				job->time.end = 0;
//				job->time.qtotal = 0;
//				job->map_total = 0;
//				job->skipcount = 0;	// for delay scheduler
//				job->run_total = 0;
//				job->map_splits.RemoveAll();
//
//				job->map_cascade.RemoveAll();
//
//				while (job->map_total < JOB_MAP_TASK_NUM) {
//					n = rand_zipf();
//					max = FILE_VEC[n].GetCount() - 1;
//					i = uniform_int(0, max);
//					file = FILE_VEC[n][i];
//					
//					bool *bflag = new bool[file->blocks.GetCount()];
//					while (job->map_total < JOB_MAP_TASK_NUM) {
//						long bi = uniform_int(0, file->blocks.GetCount() - 1);
//						if (bflag[bi] == true)
//							continue;
//						bflag[bi] = true;
//
//						block_t *b = file->blocks[bi];
//						POSITION pos = b->local_node.GetHeadPosition();
//						while (pos != NULL) {
//							long tn = b->local_node.GetKeyAt(pos);
//							long tr = GET_RACK_FROM_NODE(tn);
//							//							FN[tr]++;
//							if (job->map_splits[tr][tn].Lookup(b->id) == NULL) {
//								job->map_splits[tr][tn][b->id] = 1;
//							}
//							else {
//								job->map_splits[tr][tn][b->id]++;
//							}
//							job->map_cascade[b->id][tn] = tr;
//
//							b->local_node.GetNext(pos);
//						}
//						++job->map_total;
//					}	
//					delete bflag;
//				}
//				count += JOB_MAP_TASK_NUM;
//				REMAIN_MAP_TASKS += JOB_MAP_TASK_NUM;
//				REPORT_MAP_TASKS += JOB_MAP_TASK_NUM;
//				JOB_MAP[job->id] = job;
//				//MAP_QUEUE.AddTail(job);
//				if (P_QUEUE.Lookup(0) == NULL)
//					P_QUEUE.SetAt(0, new CAtlList<long>);
//				P_QUEUE.Lookup(0)->m_value->AddTail(job->id);
//			}
//			hold(SETUP_COMPUTATION_TIME + 50.0);
//		}
//	}
//}