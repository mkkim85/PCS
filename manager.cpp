#include "header.h"

bool MANAGER_CS[NODE_NUM] = { false, };
bool MANAGER_CS_RACKS[RACK_NUM] = { false, };
bool stable = true;
long MANAGER_NODE_NUM;
long MANAGER_BAG_SIZE, MANAGER_BUDGET_SIZE;
node_map_t MANAGER_CS_NODES;
std::list<long> INCOMPLETE_MAP_TASKS_Q;
std::list<long_map_t> FILE_ACC_H;
std::list<rack_t*> MANAGER_RANK;
std::map<long, std::map<long, std::list<long>>> MANAGER_BUDGET_MAP;

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

void state_manager(void)
{
	double m, mcap;
	long stable_t = clock;
	long req_m, top_k = 0, size;
	long m_total = 0;
	long_map_t *bag = NULL;

	if (SETUP_MODE_TYPE == MODE_BASELINE)
		return;

	create("state manager");
	while (!CSIM_END) {
		if (stable == false && MANAGER_NODE_NUM == ACTIVE_NODE_SET.size()) {
			stable = true;
			stable_t = clock;
		}

		size = FILE_ACC_H.size();
		if (size >= SETUP_TIME_WINDOW) {
			FILE_ACC_H.pop_front();
			--size;
		}
		FILE_ACC_H.push_back(GetUnitOfFileAcc());
		++size;

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

		if (SETUP_MODE_TYPE == MODE_SIERRA && clock > 0 && ((long)clock % (long)SIERRA_MONITOR_PERIOD) == 0) {
			m = (double)m_total / (double)SETUP_TIME_WINDOW;
			mcap = ACTIVE_NODE_SET.size() * MAP_SLOTS;

			stable = false;
			req_m = m - (CS_NODE_NUM * MAP_SLOTS);
			REPORT_REQ_M.first += req_m;
			REPORT_REQ_M.second++;

			bag = FindSierra(MANAGER_CS, top_k, req_m);
			ActivateNodes(MANAGER_CS, bag);
		}

		if (SETUP_MODE_TYPE == MODE_IPACS && clock > 0 && ((long)clock % (long)IPACS_MONITOR_PERIOD) == 0) {
			m = (double)m_total / (double)SETUP_TIME_WINDOW;
			mcap = ACTIVE_NODE_SET.size() * MAP_SLOTS;

			stable = false;
			req_m = m - (CS_NODE_NUM * MAP_SLOTS);
			REPORT_REQ_M.first += req_m;
			REPORT_REQ_M.second++;

			bag = GetPopularBlockList(&top_k);
			bag = FindiPACS(MANAGER_CS, bag, top_k, req_m);

			ActivateNodes(MANAGER_CS, bag);
		}

		if ((SETUP_MODE_TYPE == MODE_PCS || SETUP_MODE_TYPE == MODE_RCS) && stable == true && size >= SETUP_TIME_WINDOW
			&& (long)clock - stable_t >= (long)MODE_TRANS_INTERVAL) {
			m = (double)m_total / (double)SETUP_TIME_WINDOW;
			mcap = ACTIVE_NODE_SET.size() * MAP_SLOTS;

			if ((m / mcap >= 1 + SETUP_ALPHA && STANDBY_NODE_SET.size() > 0)
				|| (m / mcap < 1 - SETUP_BETA && ACTIVE_NODE_SET.size() > CS_NODE_NUM)) {
				stable = false;
				req_m = m - (CS_NODE_NUM * MAP_SLOTS);
				REPORT_REQ_M.first += req_m;
				REPORT_REQ_M.second++;

				if (SETUP_MODE_TYPE == MODE_RCS) {
					bag = FindRCS(MANAGER_CS, req_m);
				}
				else if (SETUP_MODE_TYPE == MODE_PCS) {
					bag = GetPopularBlockList(&top_k);
					bag = FindPCS(MANAGER_CS, bag, req_m);
				}

				ActivateNodes(MANAGER_CS, bag);		
			}
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

	node_map_t S;
	S.clear();
	for (long i = CS_NODE_NUM; i < NODE_NUM; i++)
		S[i] = &NODES[i];

	while (req_m > 0 && !S.empty()) {
		node_map_t::iterator item = S.begin();
		std::advance(item, uniform_int(0, S.size() - 1));
		node = item->first;
		if (MANAGER_CS[node] == false) {
			MANAGER_CS[node] = true;
			req_m = req_m - MAP_SLOTS;
		}
		S.erase(item);
	}
	return bag;
}

long_map_t* FindRCS(bool cs[], long req_m)
{
	long g, node;
	rack_t *rack;
	memset(&MANAGER_CS, false, sizeof(bool) * NODE_NUM);
	memset(&MANAGER_CS, true, sizeof(bool) * CS_NODE_NUM);

	rack_map_t S = NPG_SET;
	while (req_m > 0 && S.empty() == false) {
		rack_map_t::iterator ritem = S.begin();
		std::advance(ritem, uniform_int(0, S.size() - 1));
		rack = ritem->second;
		S.erase(ritem);

		long node = rack->id * NODE_NUM_IN_RACK;
		long nend = node + NODE_NUM_IN_RACK;
		while (req_m > 0 && node < nend) {
			if (MANAGER_CS[node] == false) {
				MANAGER_CS[node] = true;
				req_m = req_m - MAP_SLOTS;
			}
			++node;
		}
	}

	return NULL;
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
				req_m = req_m - MAP_SLOTS;
				
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
	MANAGER_NODE_NUM = CS_NODE_NUM;
	msg_t *msg;

	if (SETUP_MODE_TYPE == MODE_PCS && bag != NULL) { // TODO: using bag
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
			node_map_t::iterator it = MANAGER_CS_NODES.begin(), itend = MANAGER_CS_NODES.end();
			long_map_t nb;
			while (it != itend) {
				node_t *node = it->second;
				long nBudgetSize = node->space.budget.capacity - node->space.budget.used;
				nb[node->id] = ceil(((double)nBudgetSize / sBudgetSize) * listSize);
				rack_t *rack = &RACKS[GET_RACK_FROM_NODE(node->id)];
				
				// バi = B○i.getBlockList() - n,getBlockList()
				std::map<long, block_t*> pi;
				std::map<long, long>::iterator tit = rack->blocks.begin(), titend = rack->blocks.end();
				while (tit != titend) {
					block_t *b = GetBlock(tit->first);
					if (bag->find(b->id) != bag->end()	// B○i.getBlockList()
						&& node->space.disk.blocks.find(b->id) == node->space.disk.blocks.end() 
						&& node->space.budget.blocks.find(b->id) == node->space.budget.blocks.end()) {
						pi[b->id] = b;
					}
					++tit;
				}
				if (!pi.empty() && pi.size() <= nb[node->id]) {
					// transfer every block of バi from source nodes of rack i to n
					// B = B - バi
					// nb = nb - |バi|
					std::map<long, block_t*>::iterator pit = pi.begin(), pitend = pi.end();
					std::map<long, std::list<long>> v;
					while (pit != pitend) {
						block_t *b = pit->second;
						MANAGER_BUDGET_MAP[node->id][rack->id].push_back(b->id);
						if (--(*bag)[b->id] == 0) {
							bag->erase(b->id);
						}
						++pit;
					}
					nb[node->id] = nb[node->id] - pi.size();
				}
				else if (!pi.empty() && pi.size() > nb[node->id]) {
					// transfer nb blocks of バi from source nodes of rack i to n
					// B = B - list of transferred blocks
					// nb = 0
					std::map<long, block_t*>::iterator pit = pi.begin(), pitend = pi.end();
					long cnt = 0, max_cnt = nb[node->id];
					while (pit != pitend && cnt++ < max_cnt)
					{
						block_t *b = pit->second;
						MANAGER_BUDGET_MAP[node->id][rack->id].push_back(b->id);
						if (--(*bag)[b->id] == 0)
						{
							bag->erase(b->id);
						}
						++pit;
					}
					nb[node->id] = 0;
				}

				++it;
			}
			it = MANAGER_CS_NODES.begin(), itend = MANAGER_CS_NODES.end();
			while (it != itend) {
				// transfer nb blocks of B from any source nodes to n
				// B = B - list of transferred blocks
				node_t *node = it->second;
				long cnt = 0, max_cnt = nb[node->id];
				long_map_t::iterator bit = bag->begin(), bitend = bag->end();
				while (cnt < max_cnt && bit != bitend) {
					block_t *b = GetBlock(bit->first);
					
					long_map_t::iterator rit;
					do {
						rit = b->local_rack.begin();
						std::advance(rit, uniform_int(0, b->local_rack.size() - 1));
					} while (MANAGER_CS_RACKS[rit->first] == false);
					rack_t *rack = &RACKS[rit->first];
					MANAGER_BUDGET_MAP[node->id][rack->id].push_back(b->id);
					if (--(*bag)[b->id] == 0) {
						bag->erase(bit++);
					}
					else {
						++bit;
					}
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
			}

			++MANAGER_NODE_NUM;
		}
		else {	// turn off nodes
			if (NODES[i].state == STATE_ACTIVE) {
				msg = new msg_t;
				msg->power.power = false;
				M_NODE[i]->send((long)msg);
			}
		}
	}
}

bool sort_rank(const rack_t *x, const rack_t *y)
{
	return x->rank > y->rank;
}