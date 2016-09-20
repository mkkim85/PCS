#include "header.h"

node_t NODES[NODE_NUM];
node_map_t ACTIVE_NODE_SET, STANDBY_NODE_SET;
slot_t MAPPER[MAP_SLOTS_MAX];
long MAX_MAPPER_ID;
CAtlList<long> MEMORY[NODE_NUM];
CAtlMap<long, CAtlMap<long, bool>> BUDGET_MAP;

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
extern CAtlMap<long, CAtlMap<long, CAtlList<long>>> MANAGER_BUDGET_MAP;
extern long_map_t UPSET, DOWNSET;
extern long REPORT_BUDGET_SIZE;
extern long SETUP_NODE_UPTIME;
extern CAtlMap<long, block_t*> BLOCK_MAP;

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
		pnode->space.budget.capacity = ceil((BLOCK_MAP.GetCount() / CS_NODE_NUM) * SETUP_BUDGET_RATIO); 
		pnode->space.budget.used = 0;
		pnode->space.budget.lru_cache.RemoveAll();
		pnode->space.disk.capacity = DISK_SIZE - pnode->space.budget.capacity;
		//pnode->space.disk.used = 0;
		//pnode->space.disk.blocks.RemoveAll();
		//pnode->space.budget.blocks.RemoveAll();

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
			prack->active_node_set.SetAt(i, pnode); //prack->active_node_set[i] = pnode;
			ACTIVE_NODE_SET.SetAt(i, pnode); //ACTIVE_NODE_SET[i] = pnode;
			HEARTBEAT.SetAt(i, pnode);//HEARTBEAT[i] = pnode;
			MANAGER_CS[i] = true;
		}
		else {
			pnode->state = STATE_STANDBY;
			prack->standby_node_set.SetAt(i, pnode);//prack->standby_node_set[i] = pnode;
			STANDBY_NODE_SET.SetAt(i, pnode); //STANDBY_NODE_SET[i] = pnode;
			MANAGER_CS[i] = false;
		}
		++REPORT_NODE_STATE_COUNT[pnode->state];

		sprintf(str, "cpu%ld", i);
		FM_CPU[i] = new facility_ms(str, CPU_CORE);
		FM_CPU[i]->set_servicefunc(rnd_rob);

		sprintf(str, "mem%ld", i);
		F_MEMORY[i] = new facility(str);

		MEMORY[i].RemoveAll();

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
			if ((SETUP_MODE_TYPE == MODE_PCS || SETUP_MODE_TYPE == MODE_PCSC) 
				&& MANAGER_BUDGET_MAP.Lookup(id) != NULL) {
				POSITION pos = MANAGER_BUDGET_MAP[id].GetStartPosition();

				while (pos != NULL) {
					CAtlMap<long, CAtlList<long>>::CPair *pair = MANAGER_BUDGET_MAP[id].GetAt(pos);
					if (SETUP_MODE_TYPE == MODE_PCSC) {
						for (POSITION pos = pair->m_value.GetHeadPosition();
							pos != NULL; pair->m_value.GetNext(pos)) {
							block_t *b = GetBlock(pair->m_value.GetAt(pos));
							while (true) {
								POSITION rp = b->local_node.GetHeadPosition();
								long rn = uniform_int(0, b->local_node.GetCount() - 1);
								while (rn--)
									b->local_node.GetNext(rp);
								node_t *pnode = b->local_node.GetAt(rp)->m_value;
								if (pnode->state == STATE_ACTIVE) {
									node_cpu(pnode->id, (double)COMP_T);
									break;
								}
							}
						}

						double size = pair->m_value.GetCount() / (double)COMP_FACTOR;
						switch_rack(pair->m_key, parent->id, size);
						node_cpu(id, (double)DECOMP_T * size);	// de-compress
					}
					else {
						switch_rack(pair->m_key, parent->id, pair->m_value.GetCount());
					}
					// insert blocks in budget
					while (!pair->m_value.IsEmpty()) {
						block_t *b = GetBlock(pair->m_value.RemoveHead());
						my->space.budget.blocks[b->id] = b;
						++my->space.budget.used;
						++my->space.used;
						if (parent->blocks.Lookup(b->id) == NULL) {
							parent->blocks[b->id] = 1;
						}
						else {
							++parent->blocks[b->id];
						}
						b->local_node.SetAt(id, my); //b->local_node[id] = my;
						if (b->local_rack.Lookup(rack) == NULL) {
							b->local_rack.SetAt(rack, 1);//b->local_rack[rack] = 1;
						}
						else {
							b->local_rack.SetAt(rack, b->local_rack.Lookup(rack)->m_value + 1); //++b->local_rack[rack];
						}
						bblock_add(id, b->id);
						BUDGET_MAP[b->id][id] = 1;
						++REPORT_BUDGET_SIZE;
					}
					MANAGER_BUDGET_MAP[id].GetNext(pos);
				}
			}
			if (parent->state == STATE_STANDBY) {
				turnon_rack(rack);
			}
			--REPORT_NODE_STATE_COUNT[my->state];
			my->state = STATE_ACTIVATE;
			++REPORT_NODE_STATE_COUNT[my->state];
			parent->standby_node_set.RemoveKey(id);
			STANDBY_NODE_SET.RemoveKey(id);

			hold(SETUP_NODE_UPTIME);

			--REPORT_NODE_STATE_COUNT[my->state];
			my->state = STATE_ACTIVE;
			++REPORT_NODE_STATE_COUNT[my->state];
			parent->active_node_set.SetAt(id, my);//parent->active_node_set[id] = my;
			ACTIVE_NODE_SET.SetAt(id, my);//ACTIVE_NODE_SET[id] = my;
			if (UPSET.Lookup(id) != NULL)
				UPSET.RemoveKey(id);

			HEARTBEAT.SetAt(id, my);//HEARTBEAT[id] = my;
		}
		else if (r->power.power == false && my->state == STATE_ACTIVE) {
			HEARTBEAT.RemoveKey(id);
			while (my->mapper.used > 0) { // wait until all work is completed 
				hold(1.0);
			}

			MEMORY[id].RemoveAll();
			if ((SETUP_MODE_TYPE == MODE_PCS || SETUP_MODE_TYPE == MODE_PCSC)
				&& !my->space.budget.blocks.IsEmpty()) {	// clear budget
				POSITION pos = my->space.budget.blocks.GetStartPosition();
				
				while (pos != NULL) {
					CAtlMap<long, void*>::CPair *pair = my->space.budget.blocks.GetAt(pos);
					block_t *b = (block_t*)pair->m_value;
					b->local_node.RemoveKey(id);
					if (b->local_rack.Lookup(rack) != NULL && 
						--b->local_rack.Lookup(rack)->m_value == 0) {//if (--b->local_rack[rack] == 0) {
						b->local_rack.RemoveKey(rack);
					}
					if (--parent->blocks[b->id] == 0) {
						parent->blocks.RemoveKey(b->id);
					}
					BUDGET_MAP[b->id].RemoveKey(id);
					bblock_del(id, b->id);
					if (BUDGET_MAP[b->id].IsEmpty()) {
						BUDGET_MAP.RemoveKey(b->id);
					}
					--REPORT_BUDGET_SIZE;
					my->space.budget.blocks.GetNext(pos);
				}
				my->space.budget.blocks.RemoveAll();
				my->space.budget.lru_cache.RemoveAll();
			}
			my->space.used = my->space.used - my->space.budget.used;
			my->space.budget.used = 0;

			--REPORT_NODE_STATE_COUNT[my->state];
			my->state = STATE_DEACTIVATE;
			++REPORT_NODE_STATE_COUNT[my->state];
			parent->active_node_set.RemoveKey(id);
			ACTIVE_NODE_SET.RemoveKey(id);

			hold(NODE_D_TIME);

			--REPORT_NODE_STATE_COUNT[my->state];
			my->state = STATE_STANDBY;
			++REPORT_NODE_STATE_COUNT[my->state];
			parent->standby_node_set.SetAt(id, my);//parent->standby_node_set[id] = my;
			STANDBY_NODE_SET.SetAt(id, my);//STANDBY_NODE_SET[id] = my;
			if (DOWNSET.Lookup(id) != NULL)
				DOWNSET.RemoveKey(id);

			if (parent->state == STATE_ACTIVE && parent->active_node_set.GetCount() == 0) {
				turnoff_rack(rack);
			}
		}
		delete r;
	}
}

bool cache_hit(long nid, long bid)
{
	CAtlList<long> *mem = &MEMORY[nid];

	if (mem->Find(bid) == NULL) {
		T_CACHE_MISS->record(1.0);
		return false;
	}
	T_CACHE_HIT->record(1.0);
	
	return true;
}

void mem_caching(long nid, long bid)
{
	CAtlList<long> *mem = &MEMORY[nid];

	if (mem->GetCount() >= MEMORY_SIZE) {
		if (mem->Find(bid) != NULL) {
			mem->RemoveAt(mem->Find(bid));
		}
		else {
			mem->RemoveTailNoReturn();
		}
	}
	MEMORY[nid].AddHead(bid);
}

void node_cpu(long id, double t)
{
	double btime = clock;
	FM_CPU[id]->use(t);
}

void node_mem(long id, long n)
{
	double btime = clock;
	double t = MEMORY_SPEED * n;
	F_MEMORY[id]->use(t);
}

void node_disk(long id, long n)
{
	double btime = clock;
	double t = DISK_SPEED * n;
	FM_DISK[id]->use(t);
}

void bblock_use(long nid, long bid)
{
	CAtlList<long> *budget = &NODES[nid].space.budget.lru_cache;
	POSITION pos = budget->Find(bid);
	if (pos != NULL) {
		budget->RemoveAt(pos);
	}
	budget->AddHead(bid);
}

void bblock_add(long nid, long bid)
{
	storage_t *space = &NODES[nid].space;
	CAtlList<long> *budget = &space->budget.lru_cache;
	POSITION pos = budget->Find(bid);
	if (pos != NULL) {
		budget->RemoveAt(pos);
	}
	else {
		if (space->budget.capacity <= budget->GetCount()) {
			block_t *b = GetBlock(space->budget.lru_cache.RemoveTail());
			long rack = GET_RACK_FROM_NODE(nid);
			rack_t *parent = &RACKS[rack];
			if (b->local_rack.Lookup(rack) != NULL &&
				--b->local_rack.Lookup(rack)->m_value == 0) {//if (--b->local_rack[rack] == 0) {
				b->local_rack.RemoveKey(rack);
			}
			if (--parent->blocks[b->id] == 0) {
				parent->blocks.RemoveKey(b->id);
			}
			BUDGET_MAP[b->id].RemoveKey(nid);
			bblock_del(nid, b->id);
			if (BUDGET_MAP[b->id].IsEmpty()) {
				BUDGET_MAP.RemoveKey(b->id);
			}
			--REPORT_BUDGET_SIZE;
		}
	}
	budget->AddHead(bid);
}

void bblock_del(long nid, long bid)
{
	CAtlList<long> *budget = &NODES[nid].space.budget.lru_cache;
	POSITION pos = budget->Find(bid);
	if (pos != NULL) {
		budget->RemoveAt(pos);
	}
}