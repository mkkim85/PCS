#include "header.h"

long MAX_FILE_ID, MAX_BLOCK_ID;
struct block_t BLOCK_ARR[DATA_BLOCK_NUM];
std::map<long, file_t*> FILE_MAP;
std::vector<file_t*> FILE_VEC[CS_RACK_NUM];

extern long SETUP_FILE_SIZE;
extern node_t NODES[NODE_NUM];
extern rack_t RACKS[RACK_NUM];

void gen_file(void)
{
	long i, j, node, rack, min, max, sum = 0;
	block_t *b;
	file_t *f;

	while (sum < DATA_BLOCK_NUM)
	{
		f = new file_t;
		f->id = MAX_FILE_ID;
		f->acc.clear();

		rack = MAX_FILE_ID % CS_RACK_NUM;

		for (i = 0; i < SETUP_FILE_SIZE; ++i)
		{
			b = &BLOCK_ARR[MAX_BLOCK_ID];
			b->id = MAX_BLOCK_ID;
			b->file_id = f->id;

			min = rack * NODE_NUM_IN_RACK;
			max = min + NODE_NUM_IN_RACK - 1;

			node = uniform_int(min, max);
			b->local_node[node] = &NODES[node];
			b->local_rack[GET_RACK_FROM_NODE(node)] = &RACKS[GET_RACK_FROM_NODE(node)];
			++NODES[node].space.used;
			++NODES[node].space.disk.used;

			for (j = 1; j < REPLICATION_FACTOR; ++j)
			{
				node = node + CS_NODE_NUM;
				b->local_node[node] = &NODES[node];
				b->local_rack[GET_RACK_FROM_NODE(node)] = &RACKS[GET_RACK_FROM_NODE(node)];
				++NODES[node].space.used;
				++NODES[node].space.disk.used;
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

long GetMaxFileAcc(void)
{
	file_t *file;
	int max = 0;
	std::map<long, file_t*>::iterator iter;
	for (iter = FILE_MAP.begin(); iter != FILE_MAP.end(); ++iter)
	{
		file = iter->second;
		if (file->acc.size() > max)
		{
			max = file->acc.size();
		}
	}
	return max;
}

long GetTopK(std::list<long> *h)
{
	int max = INT_MIN;
	std::list<long>::iterator iter;
	for (iter = h->begin(); iter != h->end(); ++iter)
	{
		if (*iter > max)
		{
			max = *iter;
		}
	}
	return max;
}


std::list<block_t*> GetPopularBlockList(void)
{
	std::list<block_t*> bag;

	return bag;
}