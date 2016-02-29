#include "header.h"

bool MANAGER_CS[NODE_NUM] = { false, };
bool MANAGER_CS_RACKS[RACK_NUM] = { false, };
bool stable = true;
long MANAGER_BAG_SIZE, MANAGER_BUDGET_SIZE;
node_map_t MANAGER_CS_NODES;
std::list<long> INCOMPLETE_MAP_TASKS_Q, PEAK_MAP_TASKS_Q;
std::list<rack_t*> MANAGER_RANK;
std::map<long, std::map<long, std::list<long>>> MANAGER_BUDGET_MAP;
long_map_t UPSET, DOWNSET;

extern node_t NODES[NODE_NUM];
extern rack_t RACKS[RACK_NUM];
extern long REMAIN_MAP_TASKS;
extern bool CSIM_END;
extern long SETUP_TIME_WINDOW;
extern double SETUP_ALPHA;
extern double SETUP_BETA;
extern long SETUP_MODE_TYPE;
extern node_map_t ACTIVE_NODE_SET, STANDBY_NODE_SET;
extern rack_map_t ACTIVE_RACK_SET, STANDBY_RACK_SET;
extern rack_map_t ACTIVE_RACK_NPG_SET, NPG_SET;
extern mailbox *M_NODE[NODE_NUM];
extern std::pair<double, long> REPORT_AVG_M;
extern std::pair<long, long> REPORT_CAP, REPORT_REQ_M;
extern std::map<long, std::map<long, bool>> BUDGET_MAP;
extern long REPORT_BUDGET_SIZE;
extern long NODE_RANK[NODE_NUM];

void state_manager(void)
{
	if (SETUP_MODE_TYPE == MODE_BASELINE)
		return;

	double m, mcap, ev_t = 0;
	long req_m, top_k = 0;
	long m_total = 0;
	long_map_t *bag = NULL;
	
	create("state manager");
	while (!CSIM_END) {
		if (stable == false && UPSET.empty() && DOWNSET.empty())
			stable = true;

		if (INCOMPLETE_MAP_TASKS_Q.size() >= SETUP_TIME_WINDOW) {
			m_total -= INCOMPLETE_MAP_TASKS_Q.front();
			INCOMPLETE_MAP_TASKS_Q.pop_front();
		}
		m_total += REMAIN_MAP_TASKS;
		INCOMPLETE_MAP_TASKS_Q.push_back(REMAIN_MAP_TASKS);

		REPORT_AVG_M.first += (double)m_total / (double)INCOMPLETE_MAP_TASKS_Q.size();
		REPORT_AVG_M.second++;

		REPORT_CAP.first += ACTIVE_NODE_SET.size() * MAP_SLOTS;
		REPORT_CAP.second++;

		if (SETUP_MODE_TYPE == MODE_SIERRA && clock > 0 && ((long)clock % (long)SETUP_TIME_WINDOW) == 0 && stable == true) {
			m = 0;
			for (std::list<long>::iterator it = INCOMPLETE_MAP_TASKS_Q.begin(); it != INCOMPLETE_MAP_TASKS_Q.end(); ++it) {
				if (m < *it)
					m = *it;
			}
			stable = false;
			req_m = m - (CS_NODE_NUM * MAP_SLOTS);
			REPORT_REQ_M.first += req_m;
			REPORT_REQ_M.second++;

			bag = FindSierra(MANAGER_CS, top_k, req_m);
			ActivateNodes(MANAGER_CS, bag);
		}

		if (SETUP_MODE_TYPE == MODE_IPACS && clock > 0 && ((long)clock % (long)SETUP_TIME_WINDOW) == 0 && stable == true) {
			bag = GetPopularBlockList(&top_k);
			if (bag->empty() == false) {
				stable = false;
				m = (double)m_total / (double)SETUP_TIME_WINDOW;
				mcap = ACTIVE_NODE_SET.size() * MAP_SLOTS;
				req_m = m - (CS_NODE_NUM * MAP_SLOTS);
				REPORT_REQ_M.first += req_m;
				REPORT_REQ_M.second++;

				bag = GetPopularBlockList(&top_k);
				bag = FindiPACS(MANAGER_CS, bag, top_k, req_m);
				ActivateNodes(MANAGER_CS, bag);
			}
		}

		if ((SETUP_MODE_TYPE == MODE_PCS1 || SETUP_MODE_TYPE == MODE_PCS2)
			&& clock > 0 && ((long)clock % (long)SETUP_TIME_WINDOW) == 0 && stable == true) {
			m = 0;
			for (std::list<long>::iterator it = INCOMPLETE_MAP_TASKS_Q.begin(); it != INCOMPLETE_MAP_TASKS_Q.end(); ++it) {
				if (m < *it)
					m = *it;
			}
			stable = false;
			req_m = m - (CS_NODE_NUM * MAP_SLOTS);
			REPORT_REQ_M.first += req_m;
			REPORT_REQ_M.second++;

			bag = GetPopularBlockList(&top_k);
			bag = FindPCS(MANAGER_CS, bag, req_m);
			ActivateNodes(MANAGER_CS, bag);

			/*mcap = ACTIVE_NODE_SET.size() * MAP_SLOTS;
			if ((m / mcap >= 1 + SETUP_ALPHA && STANDBY_NODE_SET.size() > 0)
					|| (m / mcap < 1 - SETUP_BETA && ACTIVE_NODE_SET.size() > CS_NODE_NUM)) {
				stable = false;
				req_m = m - (CS_NODE_NUM * MAP_SLOTS);
				REPORT_REQ_M.first += req_m;
				REPORT_REQ_M.second++;

				bag = GetPopularBlockList(&top_k);
				bag = FindPCS(MANAGER_CS, bag, req_m);

				ActivateNodes(MANAGER_CS, bag);
			}*/
		}

		hold(TIME_UNIT);
	}
}

long_map_t* FindSierra(bool cs[], long top_k, long req_m)
{
	long g, node;
	memset(&MANAGER_CS, false, sizeof(bool) * NODE_NUM);
	memset(&MANAGER_CS, true, sizeof(bool) * CS_NODE_NUM);

	for (g = 1; g < REPLICATION_FACTOR; ++g) {
		node_map_t S;
		S.clear();
		for (node = CS_NODE_NUM * g; node < CS_NODE_NUM * (g + 1); ++node) {
			S[node] = &NODES[node];
		}
		while (req_m > 0 && S.size() > 0) {
			node_map_t::iterator item = S.begin();
			std::advance(item, uniform_int(0, S.size() - 1));
			node = item->first;
			if (MANAGER_CS[node] == false) {
				MANAGER_CS[node] = true;
				req_m = req_m - MAP_SLOTS;
			}
			S.erase(item);
		}
	}

	return NULL;
}

long_map_t* FindiPACS(bool cs[], long_map_t *bag, long top_k, long req_m)
{
	long node;
	memset(&MANAGER_CS, false, sizeof(bool) * NODE_NUM);
	memset(&MANAGER_CS, true, sizeof(bool) * CS_NODE_NUM);

	top_k = MIN(top_k, REPLICATION_FACTOR) - 1;

	if (top_k > 0) {
		long_map_t::iterator iter;
		for (iter = bag->begin(); iter != bag->end(); bag->erase(iter++)) {
			long cnt = top_k;
			block_t *block = GetBlock(iter->first);
			node_map_t::iterator item = block->local_node.begin();
			while (cnt--) {
				++item;
				node = item->first;
				if (MANAGER_CS[node] == false) {
					MANAGER_CS[node] = true;
					req_m = req_m - MAP_SLOTS;
				}
			}
		}
	}
	return bag;
}

long_map_t* FindPCS(bool cs[], long_map_t *bag, long req_m)
{
	bool sorted = false;
	long i, budget_size = 0, bag_size = MANAGER_BAG_SIZE;
	node_t *node;
	rack_t *rack;
	rack_map_t F = NPG_SET;
	rack_map_t F_INTERSECT_RACTIVE = ACTIVE_RACK_NPG_SET;
	memset(&MANAGER_CS, false, sizeof(bool) * NODE_NUM);
	memset(&MANAGER_CS, true, sizeof(bool) * CS_NODE_NUM);
	memset(&MANAGER_CS_RACKS, false, sizeof(bool) * RACK_NUM);
	memset(&MANAGER_CS_RACKS, true, sizeof(bool) * CS_RACK_NUM);

	while ((req_m > 0 || !bag->empty()) && !F.empty()) {
		if (!bag->empty()) {
			if (sorted == false) {
				sorted = true;
				MANAGER_RANK.sort(sort_rank);
			}

			i = MANAGER_RANK.front()->id;
			MANAGER_RANK.pop_front();
		}
		else if (!F_INTERSECT_RACTIVE.empty()) {
			rack_map_t::iterator item = F_INTERSECT_RACTIVE.begin();
			std::advance(item, uniform_int(0, F_INTERSECT_RACTIVE.size() - 1));
			i = item->second->id;
			F_INTERSECT_RACTIVE.erase(i);
		}
		else {
			rack_map_t::iterator item = F.begin();
			std::advance(item, uniform_int(0, F.size() - 1));
			i = item->second->id;
		}
		F.erase(i);
		rack = &RACKS[i];
		MANAGER_CS_RACKS[i] = true;

		long nid = i * NODE_NUM_IN_RACK;
		long nend = nid + NODE_NUM_IN_RACK;

		while (nid < nend) {
			node = &NODES[nid];
			if (MANAGER_CS[node->id] == false) {
				MANAGER_CS_NODES[node->id] = node;
				MANAGER_CS[node->id] = true;
				if (SETUP_MODE_TYPE == MODE_PCS1)
					req_m = req_m - MAP_SLOTS;
				else if (SETUP_MODE_TYPE = MODE_PCS2)
					req_m = req_m - (MAP_SLOTS - BUDGET_SLOTS);
				
				// Invalidate replicas of n's storage budget that are not appeared in bag;
				std::map<long, void*>::iterator itr = node->space.budget.blocks.begin(),
					iend = node->space.budget.blocks.end();
				while (itr != iend) {
					block_t *b = (block_t*)itr->second;
					if (bag->find(b->id) == bag->end()) {
						b->local_node.erase(node->id);
						b->local_rack.erase(GET_RACK_FROM_NODE(node->id));
						node->space.budget.blocks.erase(itr++);
						--node->space.used;
						--node->space.budget.used;
						
						BUDGET_MAP[b->id].erase(node->id);
						if (BUDGET_MAP[b->id].empty()) {
							BUDGET_MAP.erase(b->id);
						}
						--REPORT_BUDGET_SIZE;
					}
					else {
						++itr;
					}
				}

				// B = B - n.getBlockList();
				itr = node->space.disk.blocks.begin(),
					iend = node->space.disk.blocks.end();
				while (itr != iend) {	// disk
					block_t *b = (block_t*)itr->second;
					if (bag->find(b->id) != bag->end()) {
						if (--(*bag)[b->id] == 0) {
							bag->erase(b->id);
						}
						--bag_size;
					}
					++itr;
				}

				itr = node->space.budget.blocks.begin(),
					iend = node->space.budget.blocks.end();
				while (itr != iend) {	// budget
					block_t *b = (block_t*)itr->second;
					if (bag->find(b->id) != bag->end()) {
						if (--(*bag)[b->id] == 0) {
							bag->erase(b->id);
						}
						--bag_size;
					}
					++itr;
				}

				budget_size = budget_size + (node->space.budget.capacity - node->space.budget.used);
			}

			if (req_m <= 0 && budget_size >= bag_size) {
				MANAGER_BAG_SIZE = bag_size;
				MANAGER_BUDGET_SIZE = budget_size;
				return bag;
			}

			++nid;
		}
	}

	MANAGER_BAG_SIZE = bag_size;
	MANAGER_BUDGET_SIZE = budget_size;

	return bag;
}

void ActivateNodes(bool cs[], long_map_t *bag)
{
	long i;
	msg_t *msg;
	UPSET.clear();
	DOWNSET.clear();

	if ((SETUP_MODE_TYPE == MODE_PCS1 || SETUP_MODE_TYPE == MODE_PCS2) 
		&& bag != NULL) { // TODO: using bag
		if (!bag->empty()) {
			long listSize = MANAGER_BAG_SIZE;
			long sBudgetSize = MANAGER_BUDGET_SIZE;

			if (MANAGER_CS_NODES.size() == 1) {	// turn on two nodes more for distribute replications
				node_t *node = MANAGER_CS_NODES.begin()->second;
				for (i = 0; i < 2; ++i) {
					node = &NODES[node->id + 1];
					MANAGER_CS_NODES[node->id] = node;
					MANAGER_CS[node->id] = true;
					std::map<long, void*>::iterator itr = node->space.budget.blocks.begin(), iend = node->space.budget.blocks.end();
					while (itr != iend) {
						block_t *b = (block_t*)itr->second;
						if (bag->find(b->id) == bag->end()) {
							b->local_node.erase(node->id);
							b->local_rack.erase(GET_RACK_FROM_NODE(node->id));
							node->space.budget.blocks.erase(itr++);
							--node->space.used;
							--node->space.budget.used;

							BUDGET_MAP[b->id].erase(node->id);
							if (BUDGET_MAP[b->id].empty()) {
								BUDGET_MAP.erase(b->id);
							}
							--REPORT_BUDGET_SIZE;
						}
						else {
							++itr;
						}
					}
					itr = node->space.disk.blocks.begin(), iend = node->space.disk.blocks.end();
					while (itr != iend) {	// disk
						block_t *b = (block_t*)itr->second;
						if (bag->find(b->id) != bag->end()) {
							if (--(*bag)[b->id] == 0) {
								bag->erase(b->id);
							}
							--listSize;
						}
						++itr;
					}
					itr = node->space.budget.blocks.begin(), iend = node->space.budget.blocks.end();
					while (itr != iend) {	// budget
						block_t *b = (block_t*)itr->second;
						if (bag->find(b->id) != bag->end()) {
							if (--(*bag)[b->id] == 0) {
								bag->erase(b->id);
							}
							--listSize;
						}
						++itr;
					}
					sBudgetSize = sBudgetSize + (node->space.budget.capacity - node->space.budget.used);
				}
			}

			MANAGER_BUDGET_MAP.clear();
			std::list<std::pair<long, long>> nlist;
			node_map_t::iterator it = MANAGER_CS_NODES.begin(), itend = MANAGER_CS_NODES.end();
			long_map_t nb;
			while (it != itend) {
				node_t *node = it->second;
				long nBudgetSize = node->space.budget.capacity - node->space.budget.used;
				nb[node->id] = ceil(((double)nBudgetSize / sBudgetSize) * listSize);
				
				++it;
			}

			it = MANAGER_CS_NODES.begin(), itend = MANAGER_CS_NODES.end();
			long iterNum = listSize;
			while (it != itend) {
				// transfer nb blocks of B from any source nodes to n
				// B = B - list of transferred blocks
				node_t *node = it->second;
				long cnt = 0, max_cnt = nb[node->id];
				long_map_t::iterator bit;
				block_t *b;
				bool same_rack = false;
				long check_cnt = 0;
				while (cnt < max_cnt && bag->empty() == false) {
					do {
						bit = bag->begin();
						std::advance(bit, uniform_int(0, bag->size() - 1));
						b = GetBlock(bit->first);
						node_map_t::iterator nit = b->local_node.begin();
						same_rack = false;
						while (nit != b->local_node.end()) {
							if (GET_RACK_FROM_NODE(nit->second->id)
								== GET_RACK_FROM_NODE(node->id)) {
								same_rack = true;
								check_cnt++;
								break;
							}
							++nit;
						}
						if (same_rack == false || check_cnt > iterNum) {
							check_cnt = 0;
							break;
						}
					} while (true);
					MANAGER_BUDGET_MAP[node->id][GET_RACK_FROM_NODE(node->id)].push_back(b->id);
					if (--(*bag)[b->id] == 0) {
						bag->erase(b->id);
					}
					--iterNum;
					++cnt;
				}
				nb[node->id] = 0;
				++it;
			}
		}
	}

	for (i = CS_NODE_NUM; i < NODE_NUM; ++i)
	{
		if (MANAGER_CS[i] == true) {	// turn on nodes
			if (NODES[i].state == STATE_STANDBY) {
				msg = new msg_t;
				msg->power.power = true;
				M_NODE[i]->send((long)msg);
				UPSET[i] = 1;
			}
		}
		else {	// turn off nodes
			if (NODES[i].state == STATE_ACTIVE) {
				msg = new msg_t;
				msg->power.power = false;
				M_NODE[i]->send((long)msg);
				DOWNSET[i] = 1;
			}
		}
	}
}

bool sort_rank(const rack_t *x, const rack_t *y)
{
	return x->rank > y->rank;
}