#include "header.h"

long MAX_FILE_ID, MAX_BLOCK_ID;
struct block_t BLOCK_ARR[DATA_BLOCK_NUM];
std::map<long, file_t*> FILE_MAP;
std::vector<file_t*> FILE_VEC[CS_RACK_NUM];

extern long SETUP_FILE_BLOCKS;

void gen_file(void)
{
	long i, j, node, rack, min, max, sum = 0;
	block_t *b;
	file_t *f;

	while (sum < DATA_BLOCK_NUM)
	{
		f = new file_t;
		f->id = MAX_FILE_ID;

		rack = MAX_FILE_ID % CS_RACK_NUM;

		for (i = 0; i < SETUP_FILE_BLOCKS; ++i)
		{
			b = &BLOCK_ARR[MAX_BLOCK_ID];
			b->id = MAX_BLOCK_ID;

			min = rack * NODE_NUM_IN_RACK;
			max = min + NODE_NUM_IN_RACK - 1;

			node = uniform_int(min, max);
			b->locations.push_back(node);

			for (j = 1; j < REPLICATION_FACTOR; ++j)
			{
				node = node + CS_NODE_NUM;
				b->locations.push_back(node);
			}

			f->map_splits.push_back(b);

			++MAX_BLOCK_ID;
		}
		f->map_total = SETUP_FILE_BLOCKS;

		FILE_MAP[MAX_FILE_ID] = f;
		FILE_VEC[rack].push_back(f);
		sum = sum + SETUP_FILE_BLOCKS;
		++MAX_FILE_ID;
	}
}