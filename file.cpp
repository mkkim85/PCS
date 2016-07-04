#include "header.h"

long MAX_FILE_ID, MAX_BLOCK_ID;
//struct block_t BLOCK_ARR[DATA_BLOCK_NUM];
std::unordered_map<long, block_t*> BLOCK_MAP;
std::unordered_map<long, file_t*> FILE_MAP;
std::vector<file_t*> FILE_VEC[MOD_NUM];
std::map<long, std::list<std::pair<double, long>>> FILE_HISTORY;

extern long SETUP_TIME_WINDOW, SETUP_FILE_SIZE, SETUP_MODE_TYPE, SETUP_DATA_LAYOUT, SETUP_LIMIT_K;
extern long MANAGER_BAG_SIZE;
extern long REPORT_TOP_K;
extern node_t NODES[NODE_NUM];
extern rack_t RACKS[RACK_NUM];
extern std::list<rack_t*> MANAGER_RANK;

void gen_file(void)
{
	long i, j, node, rack, min, max, sum = 0, g;
	block_t *b;
	file_t *f;
	FILE_HISTORY.clear();

	if (FB_WORKLOAD == true) {
		FILE *fd = fopen(FB_DATA_PATH, "r");
		long node_id = 0, prev_fid = -1;
		long job_id, maps, shuffles, reduces, file_id;
		double gen_t, hold_t;

		if (fd == NULL) exit(EXIT_FAILURE);
		while (EOF != fscanf(fd, "%ld%lf%lf%ld%ld%ld%ld", &job_id, &gen_t, &hold_t, &maps, &shuffles, &reduces, &file_id)) {
			if (FILE_MAP.find(file_id) != FILE_MAP.end()) continue;
			maps = ceil((double)maps * FB_LOAD_RATIO);
			if (maps > 0) {
				f = new file_t;
				f->id = file_id;
				f->acc.clear();

				for (i = 0; i < maps; ++i) {
					b = BLOCK_MAP[MAX_BLOCK_ID] = new block_t;
					b->id = MAX_BLOCK_ID;
					b->file_id = f->id;

					node = node_id++;
					node_id = node_id >= CS_NODE_NUM ? (0) : node_id;
					b->local_node[node] = &NODES[node];
					b->local_rack[GET_RACK_FROM_NODE(node)] = 1;
					++NODES[node].space.used;
					++NODES[node].space.disk.used;
					NODES[node].space.disk.blocks[b->id] = b;
					RACKS[GET_RACK_FROM_NODE(node)].blocks[b->id] = 1;

					for (j = 1; j < REPLICATION_FACTOR; ++j) {
						if (SETUP_DATA_LAYOUT == 1) {
							node = node + CS_NODE_NUM;
							b->local_node[node] = &NODES[node];
							b->local_rack[GET_RACK_FROM_NODE(node)] = 1;
							++NODES[node].space.used;
							++NODES[node].space.disk.used;
							NODES[node].space.disk.blocks[b->id] = b;
							RACKS[GET_RACK_FROM_NODE(node)].blocks[b->id] = 1;
						}
						else {
							long nmin = CS_NODE_NUM * j;
							long nmax = nmin + CS_NODE_NUM - 1;
							node = uniform_int(nmin, nmax);
							b->local_node[node] = &NODES[node];
							b->local_rack[GET_RACK_FROM_NODE(node)] = 1;
							++NODES[node].space.used;
							++NODES[node].space.disk.used;
							NODES[node].space.disk.blocks[b->id] = b;
							RACKS[GET_RACK_FROM_NODE(node)].blocks[b->id] = 1;
						}
					}

					f->blocks.push_back(b);

					++MAX_BLOCK_ID;
				}
				f->size = maps;

				FILE_MAP[f->id] = f;
				//FILE_VEC[g].push_back(f);
				sum = sum + maps;
				++MAX_FILE_ID;
			}
		}
		fclose(fd);
	}
	else {
		while (sum < DATA_BLOCK_NUM) {
			f = new file_t;
			f->id = MAX_FILE_ID;
			f->acc.clear();

			g = MAX_FILE_ID % MOD_NUM;

			for (i = 0; i < SETUP_FILE_SIZE; ++i) {
				b = BLOCK_MAP[MAX_BLOCK_ID] = new block_t;
				b->id = MAX_BLOCK_ID;
				b->file_id = f->id;

				min = g * PARTITION_NODE_NUM;
				max = min + PARTITION_NODE_NUM - 1;

				node = uniform_int(min, max);
				b->local_node[node] = &NODES[node];
				b->local_rack[GET_RACK_FROM_NODE(node)] = 1;
				++NODES[node].space.used;
				++NODES[node].space.disk.used;
				NODES[node].space.disk.blocks[b->id] = b;
				RACKS[GET_RACK_FROM_NODE(node)].blocks[b->id] = 1;

				for (j = 1; j < REPLICATION_FACTOR; ++j) {
					if (SETUP_DATA_LAYOUT == 1) {
						node = node + CS_NODE_NUM;
						b->local_node[node] = &NODES[node];
						b->local_rack[GET_RACK_FROM_NODE(node)] = 1;
						++NODES[node].space.used;
						++NODES[node].space.disk.used;
						NODES[node].space.disk.blocks[b->id] = b;
						RACKS[GET_RACK_FROM_NODE(node)].blocks[b->id] = 1;
					}
					else {
						long nmin = CS_NODE_NUM * j;
						long nmax = nmin + CS_NODE_NUM - 1;
						node = uniform_int(nmin, nmax);
						b->local_node[node] = &NODES[node];
						b->local_rack[GET_RACK_FROM_NODE(node)] = 1;
						++NODES[node].space.used;
						++NODES[node].space.disk.used;
						NODES[node].space.disk.blocks[b->id] = b;
						RACKS[GET_RACK_FROM_NODE(node)].blocks[b->id] = 1;
					}
				}

				f->blocks.push_back(b);

				++MAX_BLOCK_ID;
			}
			f->size = SETUP_FILE_SIZE;

			FILE_MAP[MAX_FILE_ID] = f;
			FILE_VEC[g].push_back(f);
			sum = sum + SETUP_FILE_SIZE;
			++MAX_FILE_ID;
		}
	}
}

long_map_t* GetPopularBlockList(long *top_k)
{
	double prevt, curt;
	long req, acc, max = 0;
	block_t *block;
	file_t *file;
	std::map<long, std::list<std::pair<double, long>>>::iterator mit;
	std::list<std::pair<double, long>>::iterator lit;
	std::vector<block_t*>::iterator bit;
	long_map_t *bag = new long_map_t;
	long_map_t fmax;
	long_map_t::iterator fit;

	curt = clock;
	fmax.clear();
	prevt = curt - SETUP_TIME_WINDOW;
	prevt = (prevt > 0 ? prevt : 0);

	if (SETUP_MODE_TYPE == MODE_PCS) {
		MANAGER_BAG_SIZE = 0;

		for (long i = 0; i < RACK_NUM; ++i) {
			RACKS[i].rank = 0;
		}
	}

	for (mit = FILE_HISTORY.begin(); mit != FILE_HISTORY.end(); ++mit) {
		file = FILE_MAP[mit->first];
		for (lit = mit->second.begin(); lit != mit->second.end(); NULL) {
			if (lit->first < prevt)
				mit->second.erase(lit++);
			else {
				acc = lit->second;
				if (acc > max)
					max = acc;

				if (SETUP_MODE_TYPE == MODE_PCS) {
					req = MIN(acc, SETUP_LIMIT_K) - 1;
				}
				else if (SETUP_MODE_TYPE == MODE_IPACS) {
					req = MIN(acc, REPLICATION_FACTOR) - 1;
				}

				if (fmax[file->id] < req)
					fmax[file->id] = req;
				++lit;
			}
		}
	}

	for (fit = fmax.begin(); fit != fmax.end(); ++fit) {
		file = FILE_MAP[fit->first];

		for (bit = file->blocks.begin(); bit != file->blocks.end(); ++bit) {
			block = (*bit);
			if (SETUP_MODE_TYPE == MODE_PCS) {
				long_map_t::iterator item = block->local_rack.begin(),
					end = block->local_rack.end();
				while (++item != end) {
					RACKS[item->first].rank += fit->second;
				}
				MANAGER_BAG_SIZE += fit->second;
				(*bag)[block->id] = fit->second;
			}
			else if (SETUP_MODE_TYPE == MODE_IPACS) {
				(*bag)[block->id] = max;
			}
		}
	}

	REPORT_TOP_K = *top_k = max;
	if (!bag->empty()) {
		if (SETUP_MODE_TYPE == MODE_PCS) {
			MANAGER_RANK.clear();
			for (long i = CS_RACK_NUM; i < RACK_NUM; ++i) {
				MANAGER_RANK.push_back(&RACKS[i]);
			}
		}
	}

	return bag;
}

block_t* GetBlock(long id)
{
	if (id > BLOCK_MAP.size())
		return NULL;

	return BLOCK_MAP[id];
}