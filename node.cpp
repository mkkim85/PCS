#include "header.h"

node_t NODES[NODE_NUM];
node_map_t ACTIVE_NODE_SET, STANDBY_NODE_SET;
slot_t MAPPER[MAP_SLOTS_MAX];
long MAX_MAPPER_ID;
std::list<long> MEMORY[NODE_NUM];
std::map<long, std::map<long, bool>> BUDGET_MAP;

extern rack_t RACKS[RACK_NUM];
extern double SETUP_BUDGET_RATIO;
extern long SETUP_MODE_TYPE;
extern facility *F_MEMORY[NODE_NUM];
extern facility_ms *FM_CPU[NODE_NUM], *FM_DISK[NODE_NUM];
extern mailbox *M_MAPPER[MAP_SLOTS_MAX], *M_NODE[NODE_NUM];
extern table *T_CACHE_HIT, *T_CACHE_MISS;
extern long REPORT_NODE_STATE_COUNT[STATE_LENGTH];
extern node_map_t HEARTBEAT;
extern bool MANAGER_CS[NODE_NUM];
extern long REPORT_NODE_STATE_COUNT_PG[STATE_LENGTH];
extern std::map<long, std::map<long, std::list<long>>> MANAGER_BUDGET_MAP;
extern long_map_t UPSET, DOWNSET;
extern long REPORT_BUDGET_SIZE;
extern long SETUP_NODE_UPTIME;
extern std::unordered_map<long, block_t*> BLOCK_MAP;

void init_node(void)
{
	char str[20];
	long i, j;
	node_t *pnode;
	rack_t *prack;
	slot_t *pslot;

	for (i = 0; i < NODE_NUM; ++i) {
		pnode = &NODES[i];
		pnode->id = i;
		pnode->mapper.capacity = MAP_SLOTS;
		pnode->mapper.used = 0;
		prack = &RACKS[i / NODE_NUM_IN_RACK];
		pnode->space.capacity = DISK_SIZE;
		pnode->space.used = 0;
		pnode->space.budget.capacity = ceil((BLOCK_MAP.size() / CS_NODE_NUM) * SETUP_BUDGET_RATIO); 
		pnode->space.budget.used = 0;
		pnode->space.disk.capacity = DISK_SIZE - pnode->space.budget.capacity;
		pnode->space.disk.used = 0;
		pnode->space.disk.blocks.clear();
		pnode->space.budget.blocks.clear();

		sprintf(str, "nMail%ld", i);
		M_NODE[i] = new mailbox(str);
		node(i);

		for (j = 0; j < MAP_SLOTS; ++j) {
			pslot = &MAPPER[MAX_MAPPER_ID];
			pslot->id = MAX_MAPPER_ID;
			pslot->used = false;
			pnode->mapper.slot[j] = pslot;

			sprintf(str, "mMail%ld", pslot->id);
			M_MAPPER[pslot->id] = new mailbox(str);
			mapper(pslot->id);

			++MAX_MAPPER_ID;
		}

		if (SETUP_MODE_TYPE == MODE_BASELINE || i < CS_NODE_NUM) {
			pnode->state = STATE_ACTIVE;
			prack->active_node_set[i] = pnode;
			ACTIVE_NODE_SET[i] = pnode;
			HEARTBEAT[i] = pnode;
			MANAGER_CS[i] = true;
		}
		else {
			pnode->state = STATE_STANDBY;
			prack->standby_node_set[i] = pnode;
			STANDBY_NODE_SET[i] = pnode;
			MANAGER_CS[i] = false;
		}
		++REPORT_NODE_STATE_COUNT[pnode->state];

		sprintf(str, "cpu%ld", i);
		FM_CPU[i] = new facility_ms(str, CPU_CORE);
		FM_CPU[i]->set_servicefunc(rnd_rob);

		sprintf(str, "mem%ld", i);
		F_MEMORY[i] = new facility(str);

		MEMORY[i].clear();

		sprintf(str, "disk%ld", i);
		FM_DISK[i] = new facility_ms(str, DISK_NUM);
	}
}

void node(long id)
{
	msg_t *r;
	char str[20];
	long rack = GET_RACK_FROM_NODE(id);
	long group = GET_G_FROM_RACK(rack);
	node_t *my = &NODES[id];
	rack_t *parent = &RACKS[rack];
	
	sprintf(str, "node%ld", id);
	create(str);
	while (true) {
		M_NODE[id]->receive((long*)&r);

		if (r->power.power == true && my->state == STATE_STANDBY) {
			// Copy replications in budget
			if (SETUP_MODE_TYPE == MODE_PCS && MANAGER_BUDGET_MAP.find(id) != MANAGER_BUDGET_MAP.end()) {
				std::map<long, std::list<long>>::iterator it = MANAGER_BUDGET_MAP[id].begin(),
					itend = MANAGER_BUDGET_MAP[id].end();

				while (it != itend) {
					if (ENABLE_COMP == true) {
						double size = it->second.size() / (double)COMP_FACTOR;
						switch_rack(it->first, parent->id, size);
						node_cpu(id, (double)DECOMP_T * size);	// de-compress
					}
					else {
						switch_rack(it->first, parent->id, it->second.size());
					}
					// insert blocks in budget
					while (!it->second.empty()) {
						block_t *b = GetBlock(it->second.front());
						it->second.pop_front();
						my->space.budget.blocks[b->id] = b;
						++my->space.budget.used;
						++my->space.used;
						if (parent->blocks.find(b->id) == parent->blocks.end()) {
							parent->blocks[b->id] = 1;
						}
						else {
							++parent->blocks[b->id];
						}
						b->local_node[id] = my;
						if (b->local_rack.find(rack) == b->local_rack.end()) {
							b->local_rack[rack] = 1;
						}
						else {
							++b->local_rack[rack];
						}
						BUDGET_MAP[b->id][id] = 1;
						++REPORT_BUDGET_SIZE;
					}
					++it;
				}
			}
			if (parent->state == STATE_STANDBY) {
				turnon_rack(rack);
			}
			--REPORT_NODE_STATE_COUNT[my->state];
			my->state = STATE_ACTIVATE;
			++REPORT_NODE_STATE_COUNT[my->state];
			parent->standby_node_set.erase(id);
			STANDBY_NODE_SET.erase(id);

			hold(SETUP_NODE_UPTIME);

			--REPORT_NODE_STATE_COUNT[my->state];
			my->state = STATE_ACTIVE;
			++REPORT_NODE_STATE_COUNT[my->state];
			parent->active_node_set[id] = my;
			ACTIVE_NODE_SET[id] = my;
			if (UPSET.find(id) != UPSET.end())
				UPSET.erase(id);

			HEARTBEAT[id] = my;
		}
		else if (r->power.power == false && my->state == STATE_ACTIVE) {
			HEARTBEAT.erase(id);
			while (my->mapper.used > 0) { // wait until all work is completed 
				hold(1.0);
			}

			MEMORY[id].clear();
			if (SETUP_MODE_TYPE == MODE_PCS && !my->space.budget.blocks.empty()) {	// clear budget
				std::map<long, void*>::iterator it = my->space.budget.blocks.begin(),
					itend = my->space.budget.blocks.end();
				while (it != itend) {
					block_t *b = (block_t*)it->second;
					b->local_node.erase(id);
					if (--b->local_rack[rack] == 0) {
						b->local_rack.erase(rack);
					}
					if (--parent->blocks[b->id] == 0) {
						parent->blocks.erase(b->id);
					}
					BUDGET_MAP[b->id].erase(id);
					if (BUDGET_MAP[b->id].empty()) {
						BUDGET_MAP.erase(b->id);
					}
					--REPORT_BUDGET_SIZE;
					++it;
				}
				my->space.budget.blocks.clear();
			}
			my->space.used = my->space.used - my->space.budget.used;
			my->space.budget.used = 0;

			--REPORT_NODE_STATE_COUNT[my->state];
			my->state = STATE_DEACTIVATE;
			++REPORT_NODE_STATE_COUNT[my->state];
			parent->active_node_set.erase(id);
			ACTIVE_NODE_SET.erase(id);

			hold(NODE_D_TIME);

			--REPORT_NODE_STATE_COUNT[my->state];
			my->state = STATE_STANDBY;
			++REPORT_NODE_STATE_COUNT[my->state];
			parent->standby_node_set[id] = my;
			STANDBY_NODE_SET[id] = my;
			if (DOWNSET.find(id) != DOWNSET.end())
				DOWNSET.erase(id);

			if (parent->state == STATE_ACTIVE && parent->active_node_set.size() == 0) {
				turnoff_rack(rack);
			}
		}

		delete r;
	}
}

bool cache_hit(long nid, long bid)
{
	std::list<long> *mem = &MEMORY[nid];

	if (find(mem->begin(), mem->end(), bid) == mem->end()) {
		if (LOGGING) {
			char log[BUFSIZ];
			sprintf(log, "%ld	<cache_hit>	node: %ld, b: %ld hit failed\n", (long)clock, nid, bid);
			logging(log);
		}
		T_CACHE_MISS->record(1.0);
		return false;
	}

	if (LOGGING) {
		char log[BUFSIZ];
		sprintf(log, "%ld	<cache_hit>	node: %ld, b: %ld hit success\n", (long)clock, nid, bid);
		logging(log);
	}
	T_CACHE_HIT->record(1.0);
	return true;
}

void mem_caching(long nid, long bid)
{
	std::list<long> *mem = &MEMORY[nid];
	std::list<long>::iterator iter;

	if (mem->size() >= MEMORY_SIZE) {
		iter = find(mem->begin(), mem->end(), bid);

		if (iter != mem->end()) {
			mem->remove(bid);
			if (LOGGING) {
				char log[BUFSIZ];
				sprintf(log, "%ld	<mem_caching>	node: %ld, b: %ld replaced\n", (long)clock, nid, bid);
				logging(log);
			}
		}
		else {
			if (LOGGING) {
				char log[BUFSIZ];
				sprintf(log, "%ld	<mem_caching>	node: %ld, b: %ld removed\n", (long)clock, nid, mem->back());
				logging(log);
			}
			mem->pop_back();
		}
	}
	MEMORY[nid].push_front(bid);

	if (LOGGING) {
		char log[BUFSIZ];
		sprintf(log, "%ld	<mem_caching>	node: %ld, b: %ld cached\n", (long)clock, nid, bid);
		logging(log);
	}
}

void node_cpu(long id, double t)
{
	double btime = clock;
	FM_CPU[id]->use(t);

	if (LOGGING) {
		char log[BUFSIZ];
		sprintf(log, "%ld	<node_cpu>	node: %ld, %lf sec\n", (long)clock, id, (double)clock - btime);
		logging(log);
	}
}

void node_mem(long id, long n)
{
	double btime = clock;
	double t = MEMORY_SPEED * n;
	F_MEMORY[id]->use(t);

	if (LOGGING) {
		char log[BUFSIZ];
		sprintf(log, "%ld	<node_mem>	node: %ld, %ld blk: %lf sec\n", (long)clock, id, n, (double)clock - btime);
		logging(log);
	}
}

void node_disk(long id, long n)
{
	double btime = clock;
	double t = DISK_SPEED * n;
	FM_DISK[id]->use(t);

	if (LOGGING) {
		char log[BUFSIZ];
		sprintf(log, "%ld	<node_disk>	node: %ld, %ld blk: %lf sec\n", (long)clock, id, n, (double)clock - btime);
		logging(log);
	}
}