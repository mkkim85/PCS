#include "header.h"

long MAX_FILE_ID, MAX_BLOCK_ID;
struct block_t BLOCK_ARR[DATA_BLOCK_NUM];
std::map<long, file_t*> FILE_MAP;
std::vector<file_t*> FILE_VEC[CS_RACK_NUM];
std::map<long, std::list<std::pair<double, long>>> FILE_HISTORY;

extern long SETUP_TIME_WINDOW;
extern long SETUP_FILE_SIZE;
extern node_t NODES[NODE_NUM];
extern rack_t RACKS[RACK_NUM];
//extern std::list<long_map_t> FILE_ACC_H;
extern long SETUP_MODE_TYPE;
extern std::list<rack_t*> MANAGER_RANK;
//extern std::priority_queue<rack_t, std::vector<rack_t>, rank_cmp> MANAGER_RANK;
extern long MANAGER_BAG_SIZE;

void gen_file(void)
{
	long i, j, node, rack, min, max, sum = 0;
	block_t *b;
	file_t *f;
	FILE_HISTORY.clear();

	while (sum < DATA_BLOCK_NUM) {
		f = new file_t;
		f->id = MAX_FILE_ID;
		f->acc.clear();

		rack = MAX_FILE_ID % CS_RACK_NUM;

		for (i = 0; i < SETUP_FILE_SIZE; ++i) {
			b = &BLOCK_ARR[MAX_BLOCK_ID];
			b->id = MAX_BLOCK_ID;
			b->file_id = f->id;

			min = rack * NODE_NUM_IN_RACK;
			max = min + NODE_NUM_IN_RACK - 1;

			node = uniform_int(min, max);
			b->local_node[node] = &NODES[node];
			b->local_rack[GET_RACK_FROM_NODE(node)] = 1;
			++NODES[node].space.used;
			++NODES[node].space.disk.used;
			NODES[node].space.disk.blocks[b->id] = b;
			RACKS[GET_RACK_FROM_NODE(node)].blocks[b->id] = 1;

			for (j = 1; j < REPLICATION_FACTOR; ++j) {
				node = node + CS_NODE_NUM;
				b->local_node[node] = &NODES[node];
				b->local_rack[GET_RACK_FROM_NODE(node)] = 1;
				++NODES[node].space.used;
				++NODES[node].space.disk.used;
				NODES[node].space.disk.blocks[b->id] = b;
				RACKS[GET_RACK_FROM_NODE(node)].blocks[b->id] = 1;
			}

			f->blocks.push_back(b);

			++MAX_BLOCK_ID;
		}
		f->size = SETUP_FILE_SIZE;

		FILE_MAP[MAX_FILE_ID] = f;
		FILE_VEC[rack].push_back(f);
		sum = sum + SETUP_FILE_SIZE;
		++MAX_FILE_ID;
	}
}

//long_map_t GetUnitOfFileAcc(void)
//{
//	file_t *file;
//	long_map_t h;
//	std::map<long ,file_t*>::iterator iter;
//	for (iter = FILE_MAP.begin(); iter != FILE_MAP.end(); ++iter) {
//		file = iter->second;
//		if (file->acc.size() > 1) {
//			h[file->id] = file->acc.size();
//		}
//	}
//	return h;
//}


//long_map_t* GetPopularBlockList(long *top_k)
//{
//	long req, acc, max = 0;
//	block_t *block;
//	file_t *file;
//	std::list<long_map_t>::iterator hiter;
//	long_map_t::iterator miter;
//	std::vector<block_t*>::iterator biter;
//	long_map_t *bag = new long_map_t;
//
//	if (SETUP_MODE_TYPE == MODE_PCS) {
//		MANAGER_BAG_SIZE = 0;
//
//		for (long i = 0; i < RACK_NUM; ++i) {
//			RACKS[i].rank = 0;
//		}
//	}
//
//	for (hiter = FILE_ACC_H.begin(); hiter != FILE_ACC_H.end(); ++hiter) {
//		for (miter = hiter->begin(); miter != hiter->end(); ++miter) {
//			file = FILE_MAP[miter->first];
//			acc = miter->second;
//
//			if (acc > max) {
//				max = acc;
//			}
//
//			req = MIN(acc, REPLICATION_FACTOR) - 1;
//			for (biter = file->blocks.begin(); biter != file->blocks.end(); ++biter) {
//				block = (*biter);
//				if ((*bag).find(block->id) != (*bag).end()) {
//					if (SETUP_MODE_TYPE == MODE_PCS) {
//						long prev = (*bag)[block->id];
//						long cnt = req;
//						while (cnt-- > prev) {
//							++MANAGER_BAG_SIZE;
//							long_map_t::iterator item = block->local_rack.begin(),
//								end = block->local_rack.end();
//
//							while (++item != end) {
//								++RACKS[item->first].rank;
//							}
//						}
//					}
//					(*bag)[block->id] = MAX(req, (*bag)[block->id]);
//				}
//				else {
//					if (SETUP_MODE_TYPE == MODE_PCS) {
//						long cnt = req;
//						while (cnt-- > 0) {
//							++MANAGER_BAG_SIZE;
//							long_map_t::iterator item = block->local_rack.begin(),
//								end = block->local_rack.end();
//
//							while (++item != end) {
//								++RACKS[item->first].rank;
//							}
//						}
//					}
//					(*bag)[block->id] = req;
//				}
//			}
//		}
//	}
//
//	*top_k = max;
//	if (!bag->empty()) {
//		if (SETUP_MODE_TYPE == MODE_PCS) {
//			MANAGER_RANK.clear();
//			for (long i = CS_RACK_NUM; i < RACK_NUM; ++i) {
//				MANAGER_RANK.push_back(&RACKS[i]);
//			}
//		}
//
//		return bag;
//	}
//
//	delete bag;
//	return NULL;
//}

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
				req = MIN(acc, REPLICATION_FACTOR) - 1;

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

	*top_k = max;
	if (!bag->empty()) {
		if (SETUP_MODE_TYPE == MODE_PCS) {
			MANAGER_RANK.clear();
			for (long i = CS_RACK_NUM; i < RACK_NUM; ++i) {
				MANAGER_RANK.push_back(&RACKS[i]);
			}
		}
		return bag;
	}

	delete bag;
	return NULL;
}

block_t* GetBlock(long id)
{
	if (id > DATA_BLOCK_NUM)
		return NULL;

	return &BLOCK_ARR[id];
}