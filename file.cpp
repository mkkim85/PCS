#include "header.h"

long MAX_FILE_ID, MAX_BLOCK_ID;
CAtlMap<long, block_t*> BLOCK_MAP;
CAtlMap<long, file_t*> FILE_MAP;
CAtlArray<file_t*> FILE_VEC[MOD_FACTOR];
CAtlMap<long, CAtlMap<double, long>> FILE_HISTORY;

extern long SETUP_TIME_WINDOW, SETUP_FILE_SIZE, SETUP_MODE_TYPE, SETUP_LIMIT_K;
extern long MANAGER_BAG_SIZE;
extern long REPORT_TOP_K;
extern double SETUP_DATA_LAYOUT, SETUP_DATA_SKEW;
extern node_t NODES[NODE_NUM];
extern rack_t RACKS[RACK_NUM];
extern CAtlList<rack_t*> MANAGER_RANK;

void gen_file(void)
{
	long i, j, node, rack, min, max, sum = 0, g, ori_g = 0;
	block_t *b;
	file_t *f;
	FILE_HISTORY.RemoveAll();

	if (FB_WORKLOAD == true) {
		FILE *fd = fopen(FB_DATA_PATH, "r");
		long job_id, maps, shuffles, reduces, file_id;
		double gen_t, hold_t;

		if (fd == NULL) exit(EXIT_FAILURE);
		while (EOF != fscanf(fd, "%ld%lf%lf%ld%ld%ld%ld", &job_id, &gen_t, &hold_t, &maps, &shuffles, &reduces, &file_id)) {
			if (FILE_MAP.Lookup(file_id) != NULL) continue;
			//maps = ceil((double)maps * SETUP_DATA_SKEW);
			if (maps > 0) {
				f = new file_t;
				f->id = file_id;
				f->acc.RemoveAll();

				bool dist = (prob() < 1 - SETUP_DATA_LAYOUT);

				for (i = 0; i < maps; ++i) {
					b = BLOCK_MAP[MAX_BLOCK_ID] = new block_t;
					b->id = MAX_BLOCK_ID;
					b->file_id = f->id;

					g = (dist) ? uniform_int(0, CS_RACK_NUM - 1) : ori_g;

					min = g * NODE_NUM_IN_RACK;
					max = min + NODE_NUM_IN_RACK - 1;
					node = uniform_int(min, max);
					//node = node_id++;
					//node_id = node_id >= CS_NODE_NUM ? (0) : node_id;
					b->local_node.SetAt(node, &NODES[node]);//b->local_node[node] = &NODES[node];
					b->local_rack.SetAt(GET_RACK_FROM_NODE(node), 1);;//b->local_rack[GET_RACK_FROM_NODE(node)] = 1;;
					NODES[node].space.used++;
					NODES[node].space.disk.used++;
					NODES[node].space.disk.blocks[b->id] = b;
					RACKS[GET_RACK_FROM_NODE(node)].blocks[b->id] = 1;

					for (j = 1; j < REPLICATION_FACTOR; ++j) {
						node = node + CS_NODE_NUM;
						b->local_node.SetAt(node, &NODES[node]);//b->local_node[node] = &NODES[node];
						rack = GET_RACK_FROM_NODE(node);
						b->local_rack.SetAt(rack, 1);//b->local_rack[GET_RACK_FROM_NODE(node)] = 1;
						NODES[node].space.used++;
						NODES[node].space.disk.used++;
						NODES[node].space.disk.blocks[b->id] = b;
						RACKS[GET_RACK_FROM_NODE(node)].blocks[b->id] = 1;
					}
					f->blocks.Add(b);
					MAX_BLOCK_ID++;
				}
				f->size = maps;

				FILE_MAP[f->id] = f;
				FILE_VEC[f->id % MOD_FACTOR].Add(f);
				sum = sum + maps;
				MAX_FILE_ID++;
				if (++ori_g == CS_RACK_NUM) ori_g = 0;
			}
		}
		fclose(fd);
	}
	else {
		while (sum < DATA_BLOCK_NUM) {
			f = new file_t;
			f->id = MAX_FILE_ID;
			f->acc.RemoveAll();

			bool dist = (prob() < 1 - SETUP_DATA_LAYOUT);

			for (i = 0; i < SETUP_FILE_SIZE; ++i) {
				b = BLOCK_MAP[MAX_BLOCK_ID] = new block_t;
				b->id = MAX_BLOCK_ID;
				b->file_id = f->id;

				g = (dist) ? uniform_int(0, CS_RACK_NUM - 1) : ori_g;

				min = g * NODE_NUM_IN_RACK;
				max = min + NODE_NUM_IN_RACK - 1;
				node = uniform_int(min, max);

				b->local_node.SetAt(node, &NODES[node]);//b->local_node[node] = &NODES[node];
				rack = GET_RACK_FROM_NODE(node);
				b->local_rack.SetAt(rack, 1);//b->local_rack[GET_RACK_FROM_NODE(node)] = 1;
				NODES[node].space.used = NODES[node].space.used + 1;
				NODES[node].space.disk.used = NODES[node].space.disk.used + 1;
				NODES[node].space.disk.blocks.SetAt(b->id, b); //NODES[node].space.disk.blocks[b->id] = b;
				RACKS[rack].blocks.SetAt(b->id, 1); // RACKS[rack].blocks[b->id] = 1;

				for (j = 1; j < REPLICATION_FACTOR; ++j) {
					node = node + CS_NODE_NUM;
					b->local_node.SetAt(node, &NODES[node]);//b->local_node[node] = &NODES[node];
					rack = GET_RACK_FROM_NODE(node);
					b->local_rack.SetAt(rack, 1);//b->local_rack[GET_RACK_FROM_NODE(node)] = 1;
					NODES[node].space.used = NODES[node].space.used + 1;
					NODES[node].space.disk.used = NODES[node].space.disk.used + 1;
					NODES[node].space.disk.blocks.SetAt(b->id, b); //NODES[node].space.disk.blocks[b->id] = b;
					RACKS[GET_RACK_FROM_NODE(node)].blocks.SetAt(b->id, 1); //RACKS[GET_RACK_FROM_NODE(node)].blocks[b->id] = 1;
				}
				f->blocks.Add(b);
				MAX_BLOCK_ID = MAX_BLOCK_ID + 1;
			}
			f->size = SETUP_FILE_SIZE;

			FILE_MAP[MAX_FILE_ID] = f;
			FILE_VEC[MAX_FILE_ID % MOD_FACTOR].Add(f);
			sum = sum + SETUP_FILE_SIZE;
			MAX_FILE_ID = MAX_FILE_ID + 1;
			if (++ori_g == CS_RACK_NUM) ori_g = 0;
		}
	}
}

long_map_t* GetPopularBlockList(long *top_k)
{
	double prevt, curt;
	long req, acc, max = 0;
	block_t *block;
	file_t *file;
	POSITION mpos, lpos, bpos, fpos;
	long_map_t *bag = new long_map_t;
	long_map_t fmax;

	curt = clock;
	fmax.RemoveAll();
	prevt = curt - SETUP_TIME_WINDOW;
	prevt = (prevt > 0 ? prevt : 0);

	if (SETUP_MODE_TYPE == MODE_PCS || SETUP_MODE_TYPE == MODE_PCSC) {
		MANAGER_BAG_SIZE = 0;
		for (long i = 0; i < RACK_NUM; ++i) {
			RACKS[i].rank = 0;
		}
	}

	CAtlList<std::pair<long, double>> remove_history;

	for (mpos = FILE_HISTORY.GetStartPosition(); 
		mpos != NULL; 
		FILE_HISTORY.GetNext(mpos)) {
		CAtlMap<long, CAtlMap<double, long>>::CPair *mpair = FILE_HISTORY.GetAt(mpos);
		file = FILE_MAP[mpair->m_key];
		for (lpos = mpair->m_value.GetStartPosition(); lpos != NULL; mpair->m_value.GetNext(lpos)) {
			CAtlMap<double, long >::CPair *lpair = mpair->m_value.GetAt(lpos);
			if (lpair->m_key < prevt) {
				std::pair<long, double> p(mpair->m_key, lpair->m_key);
				remove_history.AddTail(p);
			}
			else {
				acc = lpair->m_value;
				if (acc > max)
					max = acc;

				if (SETUP_MODE_TYPE == MODE_PCS || SETUP_MODE_TYPE == MODE_PCSC) {
					req = MIN(acc, SETUP_LIMIT_K) - 1;
				}
				else if (SETUP_MODE_TYPE == MODE_IPACS) {
					req = MIN(acc, REPLICATION_FACTOR) - 1;
				}

				if (fmax[file->id] < req)
					fmax[file->id] = req;
				//mpair->m_value.GetNext(lpos);
			}
		}
	}

	for (POSITION pr = remove_history.GetHeadPosition();
		pr != NULL; remove_history.GetNext(pr)) {
		std::pair<long, double > rp = remove_history.GetAt(pr);
		FILE_HISTORY[rp.first].RemoveKey(rp.second);
		if (FILE_HISTORY[rp.first].IsEmpty())
			FILE_HISTORY.RemoveKey(rp.first);
	}
	remove_history.RemoveAll();

	for (fpos = fmax.GetStartPosition(); 
		fpos != NULL; 
		fmax.GetNext(fpos)) {
		long_map_t::CPair *fpair = fmax.GetAt(fpos);
		file = FILE_MAP[fpair->m_key];

		for (long i = 0; i < file->blocks.GetCount(); i++) {
			block = file->blocks[i];
			if (SETUP_MODE_TYPE == MODE_PCS || SETUP_MODE_TYPE == MODE_PCSC) {
				POSITION pos = block->local_rack.GetHeadPosition();
				while (pos != NULL) {
					RACKS[block->local_rack.GetKeyAt(pos)].rank += fpair->m_value;
					block->local_rack.GetNext(pos);
				}
				MANAGER_BAG_SIZE += fpair->m_value;
				(*bag)[block->id] = fpair->m_value;
			}
			else if (SETUP_MODE_TYPE == MODE_IPACS) {
				//max = MIN(REPLICATION_FACTOR - 1, max);
				//(*bag)[block->id] = max;
				(*bag)[block->id] = fpair->m_value;
			}
		}
	}

	REPORT_TOP_K = *top_k = max;
	if (!bag->IsEmpty()) {
		if (SETUP_MODE_TYPE == MODE_PCS || SETUP_MODE_TYPE == MODE_PCSC) {
			MANAGER_RANK.RemoveAll();
			for (long i = CS_RACK_NUM; i < RACK_NUM; ++i) {
				MANAGER_RANK.AddTail(&RACKS[i]);
			}
		}
	}

	return bag;
}

block_t* GetBlock(long id)
{
	if (id > BLOCK_MAP.GetCount())
		return NULL;

	return BLOCK_MAP[id];
}