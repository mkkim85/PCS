#include "header.h"

bool MANAGER_CS[NODE_NUM] = { false, };
bool MANAGER_CS_RACKS[RACK_NUM] = { false, };
bool stable = true;
long MANAGER_BAG_SIZE, MANAGER_BUDGET_SIZE;
node_map_t MANAGER_CS_NODES;
CAtlList<long> INCOMPLETE_MAP_TASKS_Q, PEAK_MAP_TASKS_Q;
CAtlList<rack_t*> MANAGER_RANK;
CAtlMap<long, CAtlMap<long, CAtlList<long>>> MANAGER_BUDGET_MAP;
long_map_t UPSET, DOWNSET;

extern node_t NODES[NODE_NUM];
extern rack_t RACKS[RACK_NUM];
extern long REMAIN_MAP_TASKS;
extern bool CSIM_END;
extern long SETUP_TIME_WINDOW;
extern double SETUP_ALPHA;
extern long SETUP_MODE_TYPE;
extern node_map_t ACTIVE_NODE_SET, STANDBY_NODE_SET;
extern rack_map_t ACTIVE_RACK_SET, STANDBY_RACK_SET;
extern rack_map_t ACTIVE_RACK_NPG_SET, NPG_SET;
extern mailbox *M_NODE[NODE_NUM];
extern std::pair<double, long> REPORT_AVG_M;
extern std::pair<long, long> REPORT_CAP, REPORT_REQ_M;
extern CAtlMap<long, CAtlMap<long, bool>> BUDGET_MAP;
extern long REPORT_BUDGET_SIZE;
extern long NODE_RANK[NODE_NUM];

void rank_quick_sort(CAtlList<rack_t*> *list, long left, long right) {
	long i = left, j = right;
	long pivot = list->GetAt(list->FindIndex((left + right) / 2))->rank;

	/* partition */
	while (i <= j) {
		while (list->GetAt(list->FindIndex(i))->rank > pivot)
			i++;
		while (list->GetAt(list->FindIndex(j))->rank < pivot)
			j--;
		if (i <= j) {
			list->SwapElements(list->FindIndex(i), list->FindIndex(j));
			i++;
			j--;
		}
	}

	/* recursion */
	if (left < j)
		rank_quick_sort(list, left, j);
	if (i < right)
		rank_quick_sort(list, i, right);
}

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
		if (stable == false && UPSET.IsEmpty() && DOWNSET.IsEmpty() && MANAGER_BUDGET_MAP.IsEmpty())
			stable = true;

		if (INCOMPLETE_MAP_TASKS_Q.GetCount() >= SETUP_TIME_WINDOW) {
			m_total -= INCOMPLETE_MAP_TASKS_Q.RemoveHead();
		}
		m_total += REMAIN_MAP_TASKS;
		INCOMPLETE_MAP_TASKS_Q.AddTail(REMAIN_MAP_TASKS);

		REPORT_AVG_M.first += (double)m_total / (double)INCOMPLETE_MAP_TASKS_Q.GetCount();
		REPORT_AVG_M.second++;

		REPORT_CAP.first += ACTIVE_NODE_SET.GetCount() * MAP_SLOTS;
		REPORT_CAP.second++;

		if (SETUP_MODE_TYPE == MODE_SIERRA && clock > 0 && ((long)clock % (long)SETUP_TIME_WINDOW) == 0 && stable == true) {
			m = 0;
			POSITION pos = INCOMPLETE_MAP_TASKS_Q.GetHeadPosition();
			while (pos != NULL) {
				if (m < INCOMPLETE_MAP_TASKS_Q.GetAt(pos))
					m = INCOMPLETE_MAP_TASKS_Q.GetAt(pos);
				INCOMPLETE_MAP_TASKS_Q.GetNext(pos);
			}
			long ccap = ACTIVE_NODE_SET.GetCount() * MAP_SLOTS;
			if (m > ccap || m < ccap * SETUP_ALPHA) {
				stable = false;
				req_m = m - (CS_NODE_NUM * MAP_SLOTS);
				REPORT_REQ_M.first += req_m;
				REPORT_REQ_M.second++;

				bag = FindSierra(MANAGER_CS, top_k, req_m);
				ActivateNodes(MANAGER_CS, bag);
			}
		}

		if (SETUP_MODE_TYPE == MODE_IPACS && clock > 0 && ((long)clock % (long)SETUP_TIME_WINDOW) == 0 && stable == true) {
			bag = GetPopularBlockList(&top_k);
			if (bag->IsEmpty() == false) {
				stable = false;
				m = (double)m_total / (double)SETUP_TIME_WINDOW;
				mcap = ACTIVE_NODE_SET.GetCount() * MAP_SLOTS;
				req_m = m - (CS_NODE_NUM * MAP_SLOTS);
				REPORT_REQ_M.first += req_m;
				REPORT_REQ_M.second++;

				bag = FindiPACS(MANAGER_CS, bag, top_k, req_m);
				ActivateNodes(MANAGER_CS, bag);
			}
		}

		if ((SETUP_MODE_TYPE == MODE_PCS || SETUP_MODE_TYPE == MODE_PCSC)
			&& clock > 0 && ((long)clock % (long)SETUP_TIME_WINDOW) == 0 && stable == true) {
			m = 0;
			POSITION pos = INCOMPLETE_MAP_TASKS_Q.GetHeadPosition();
			while (pos != NULL) {	// find peak load
				if (m < INCOMPLETE_MAP_TASKS_Q.GetAt(pos))
					m = INCOMPLETE_MAP_TASKS_Q.GetAt(pos);
				INCOMPLETE_MAP_TASKS_Q.GetNext(pos);
			}
			//// find peak load
			//double sum = 0.0;
			//POSITION pos = INCOMPLETE_MAP_TASKS_Q.GetHeadPosition();
			//while (pos != NULL) {
			//	sum = sum + INCOMPLETE_MAP_TASKS_Q.GetAt(pos);
			//	INCOMPLETE_MAP_TASKS_Q.GetNext(pos);
			//}
			//m = sum / INCOMPLETE_MAP_TASKS_Q.GetCount();
			long nn = ACTIVE_NODE_SET.GetCount();
			long ccap = nn * MAP_SLOTS;
			if ((m > ccap && nn < NODE_NUM) 
				|| (m < ccap * SETUP_ALPHA && nn > CS_NODE_NUM)) {
				stable = false;
				req_m = m - (CS_NODE_NUM * MAP_SLOTS);
				REPORT_REQ_M.first += req_m;
				REPORT_REQ_M.second++;

				//if (m < ccap * SETUP_ALPHA)	// underload
				//	bag = new long_map_t;
				//else bag = GetPopularBlockList(&top_k);
				bag = GetPopularBlockList(&top_k);
				bag = FindPCS(MANAGER_CS, bag, req_m);
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
		CAtlArray<long> S;
		S.RemoveAll();
		for (node = CS_NODE_NUM * g; node < CS_NODE_NUM * (g + 1); ++node) {
			S.Add(node);
		}
		while (req_m > 0 && S.GetCount() > 0) {
			long i = uniform_int(0, S.GetCount() - 1);
			node = S[i];
			if (MANAGER_CS[node] == false) {
				MANAGER_CS[node] = true;
				req_m = req_m - MAP_SLOTS;
			}
			S.RemoveAt(i);
		}
	}

	return NULL;
}

long_map_t* FindiPACS(bool cs[], long_map_t *bag, long top_k, long req_m)
{
	long node;
	CAtlList<long> rkeys;
	memset(&MANAGER_CS, false, sizeof(bool) * NODE_NUM);
	memset(&MANAGER_CS, true, sizeof(bool) * CS_NODE_NUM);

	top_k = MIN(top_k, REPLICATION_FACTOR) - 1;

	if (top_k > 0) {
		POSITION pos = bag->GetStartPosition();
		while (pos != NULL) {
			long cnt = top_k;
			block_t *block = GetBlock(bag->GetKeyAt(pos));
			POSITION npos = block->local_node.GetHeadPosition();
			while (cnt--) {
				block->local_node.GetNext(npos);
				node = block->local_node.GetKeyAt(npos);
				if (MANAGER_CS[node] == false) {
					MANAGER_CS[node] = true;
					req_m = req_m - MAP_SLOTS;
				}
			}
			rkeys.AddTail(block->id);
			bag->GetNext(pos);	
		}
	}
	for (POSITION pos = rkeys.GetHeadPosition(); pos != NULL; rkeys.GetNext(pos))
		bag->RemoveKey(rkeys.GetAt(pos));
	return bag;
}

long_map_t* FindPCS(bool cs[], long_map_t *bag, long req_m)
{
	bool sorted = false;
	long i, budget_size = 0, bag_size = MANAGER_BAG_SIZE;
	node_t *node;
	rack_t *rack;
	rack_map_t F;// = NPG_SET;
	rack_map_t F_INTERSECT_RACTIVE;// = ACTIVE_RACK_NPG_SET;

	for (POSITION pos = NPG_SET.GetHeadPosition(); pos != NULL; NPG_SET.GetNext(pos))
		F.SetAt(NPG_SET.GetKeyAt(pos), NPG_SET.GetValueAt(pos));//F[NPG_SET.GetKeyAt(pos)] = NPG_SET.GetValueAt(pos);
	for (POSITION pos = ACTIVE_RACK_NPG_SET.GetHeadPosition(); pos != NULL; ACTIVE_RACK_NPG_SET.GetNext(pos))
		F_INTERSECT_RACTIVE.SetAt(ACTIVE_RACK_NPG_SET.GetKeyAt(pos), ACTIVE_RACK_NPG_SET.GetValueAt(pos));
		//F_INTERSECT_RACTIVE[ACTIVE_RACK_NPG_SET.GetKeyAt(pos)] = ACTIVE_RACK_NPG_SET.GetValueAt(pos);

	memset(&MANAGER_CS, false, sizeof(bool) * NODE_NUM);
	memset(&MANAGER_CS, true, sizeof(bool) * CS_NODE_NUM);
	memset(&MANAGER_CS_RACKS, false, sizeof(bool) * RACK_NUM);
	memset(&MANAGER_CS_RACKS, true, sizeof(bool) * CS_RACK_NUM);
	MANAGER_CS_NODES.RemoveAll();

	while ((req_m > 0 || !bag->IsEmpty()) && !F.IsEmpty()) {
		if (!bag->IsEmpty()) {
			if (sorted == false) {
				sorted = true;
				rank_quick_sort(&MANAGER_RANK, 0, MANAGER_RANK.GetCount() - 1);
			}

			i = MANAGER_RANK.RemoveHead()->id;
		}
		else if (!F_INTERSECT_RACTIVE.IsEmpty()) {
			POSITION pos = F_INTERSECT_RACTIVE.GetHeadPosition();
			long iter = uniform_int(0, F_INTERSECT_RACTIVE.GetCount() - 1);
			while (iter--)
				F_INTERSECT_RACTIVE.GetNext(pos);
			i = F_INTERSECT_RACTIVE.GetValueAt(pos)->id;
			F_INTERSECT_RACTIVE.RemoveKey(i);
		}
		else {
			POSITION pos = F.GetHeadPosition();
			long iter = uniform_int(0, F.GetCount() - 1);
			while (iter--)
				F.GetNext(pos);
			i = F.GetValueAt(pos)->id;
		}
		F.RemoveKey(i);
		rack = &RACKS[i];
		MANAGER_CS_RACKS[i] = true;

		long nid = i * NODE_NUM_IN_RACK;
		long nend = nid + NODE_NUM_IN_RACK;

		while (nid < nend) {
			node = &NODES[nid];
			if (MANAGER_CS[node->id] == false) {
				CAtlList<long> rk;
				MANAGER_CS_NODES.SetAt(node->id, node);//MANAGER_CS_NODES[node->id] = node;
				MANAGER_CS[node->id] = true;
				req_m = req_m - MAP_SLOTS;

				// Invalidate replicas of n's storage budget that are not appeared in bag;
				POSITION bpos = node->space.budget.blocks.GetStartPosition();
				while (bpos != NULL) {
					CAtlMap<long, void*>::CPair *pair = node->space.budget.blocks.GetAt(bpos);
					block_t *b = (block_t*)pair->m_value;
					if (bag->Lookup(b->id) == NULL) {
						b->local_node.RemoveKey(node->id);
						b->local_rack.RemoveKey(GET_RACK_FROM_NODE(node->id));
						rk.AddTail(node->space.budget.blocks.GetKeyAt(bpos));
						--node->space.used;
						--node->space.budget.used;
						
						BUDGET_MAP[b->id].RemoveKey(node->id);
						if (BUDGET_MAP[b->id].IsEmpty()) {
							BUDGET_MAP.RemoveKey(b->id);
						}
						--REPORT_BUDGET_SIZE;
					}
					node->space.budget.blocks.GetNext(bpos);
				}

				// remove
				for (POSITION p = rk.GetHeadPosition(); p != NULL; rk.GetNext(p))
					node->space.budget.blocks.RemoveKey(rk.GetAt(p));

				// B = B - n.getBlockList();
				//if (node->state == STATE_STANDBY) {
					POSITION dpos = node->space.disk.blocks.GetStartPosition();
					while (dpos != NULL) {	// disk
						block_t *b = (block_t*)node->space.disk.blocks.GetValueAt(dpos);
						if (bag->Lookup(b->id) != NULL) {
							if (--(*bag)[b->id] == 0) {
								bag->RemoveKey(b->id);
							}
							--bag_size;
						}
						node->space.disk.blocks.GetNext(dpos);
					}

					POSITION bpos2 = node->space.budget.blocks.GetStartPosition();
					while (bpos2 != NULL) {	// budget
						block_t *b = (block_t*)node->space.budget.blocks.GetValueAt(bpos2);
						if (bag->Lookup(b->id) != NULL) {
							if (--(*bag)[b->id] == 0) {
								bag->RemoveKey(b->id);
							}
							--bag_size;
						}
						node->space.budget.blocks.GetNext(bpos2);
					}
				//}

				//budget_size = budget_size + (node->space.budget.capacity - node->space.budget.used);
				budget_size = budget_size + node->space.budget.capacity;
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
	CAtlList<long> rk;
	UPSET.RemoveAll();
	DOWNSET.RemoveAll();

	if ((SETUP_MODE_TYPE == MODE_PCS || SETUP_MODE_TYPE == MODE_PCSC) && !bag->IsEmpty()) { // TODO: using bag
		long listSize = MANAGER_BAG_SIZE;
		long sBudgetSize = MANAGER_BUDGET_SIZE;

		if (MANAGER_CS_NODES.GetCount() == 1) {	// turn on two nodes more for distribute replications
			node_t *node = MANAGER_CS_NODES.GetValueAt(MANAGER_CS_NODES.GetHeadPosition());
			for (i = 0; i < 2; ++i) {
				rk.RemoveAll();
				node = &NODES[node->id + 1];
				MANAGER_CS_NODES.SetAt(node->id, node);//MANAGER_CS_NODES[node->id] = node;
				MANAGER_CS[node->id] = true;
				POSITION pos = node->space.budget.blocks.GetStartPosition();
				while (pos != NULL) {
					block_t *b = (block_t*)node->space.budget.blocks.GetValueAt(pos);
					if (bag->Lookup(b->id) == NULL) {
						b->local_node.RemoveKey(node->id);
						b->local_rack.RemoveKey(GET_RACK_FROM_NODE(node->id));
						rk.AddTail(node->space.budget.blocks.GetKeyAt(pos));
						node->space.budget.blocks.GetNext(pos);
						--node->space.used;
						--node->space.budget.used;

						BUDGET_MAP[b->id].RemoveKey(node->id);
						if (BUDGET_MAP[b->id].IsEmpty()) {
							BUDGET_MAP.RemoveKey(b->id);
						}
						--REPORT_BUDGET_SIZE;
					}
					else {
						node->space.budget.blocks.GetNext(pos);
					}
				}
				
				// remove
				for (POSITION p = rk.GetHeadPosition(); p != NULL; rk.GetNext(p))
					node->space.budget.blocks.RemoveKey(rk.GetAt(p));

				//if (node->state == STATE_STANDBY) {
					POSITION pos2 = node->space.disk.blocks.GetStartPosition();
					while (pos2 != NULL) {	// disk
						block_t *b = (block_t*)node->space.disk.blocks.GetValueAt(pos2);
						if (bag->Lookup(b->id) != NULL) {
							if (--(*bag)[b->id] == 0) {
								bag->RemoveKey(b->id);
							}
							--listSize;
						}
						node->space.disk.blocks.GetNext(pos2);
					}
					pos2 = node->space.budget.blocks.GetStartPosition();
					while (pos2 != NULL) {	// budget
						block_t *b = (block_t*)node->space.budget.blocks.GetValueAt(pos2);
						if (bag->Lookup(b->id) != NULL) {
							if (--(*bag)[b->id] == 0) {
								bag->RemoveKey(b->id);
							}
							--listSize;
						}
						node->space.budget.blocks.GetNext(pos2);
					}
				//}
				sBudgetSize = sBudgetSize + (node->space.budget.capacity - node->space.budget.used);
				//sBudgetSize = sBudgetSize + node->space.budget.capacity;
			}
		}

		MANAGER_BUDGET_MAP.RemoveAll();
		CAtlList<std::pair<long, long>> nlist;
		POSITION pos = MANAGER_CS_NODES.GetHeadPosition();
		long_map_t nb;
		while (pos != NULL) {
			node_t *node = MANAGER_CS_NODES.GetValueAt(pos);
			long nBudgetSize = node->space.budget.capacity - node->space.budget.used;
			//long nBudgetSize = node->space.budget.capacity;
			nb[node->id] = ceil(((double)nBudgetSize / sBudgetSize) * listSize);
		
			CAtlArray<long> proj;
			long rack_id = GET_RACK_FROM_NODE(node->id);
			// B ¡û i.getBlockList() - n.getBlockList()
			for (POSITION pos2 = bag->GetStartPosition(); pos2 != NULL; bag->GetNext(pos2)) {
				long block_id = bag->GetKeyAt(pos2);
				if (RACKS[rack_id].blocks.Lookup(block_id) != NULL
					&& node->space.disk.blocks.Lookup(block_id) == NULL
					&& node->space.budget.blocks.Lookup(block_id) == NULL)
					proj.Add(block_id);
			}

			if (proj.GetCount() < nb[node->id]) {
				for (long i = 0; i < proj.GetCount(); i++) {
					long block_id = proj[i];
					MANAGER_BUDGET_MAP[node->id][rack_id].AddTail(block_id);
					if (--(*bag)[block_id] == 0) {
						bag->RemoveKey(block_id);
					}
				}
				nb[node->id] = nb[node->id] - proj.GetCount();
				proj.RemoveAll();
			}
			else {
				// transfer
				long i = nb[node->id];
				while (i--) {
					long ran = uniform_int(0, proj.GetCount() - 1);
					long block_id = proj[ran];
					MANAGER_BUDGET_MAP[node->id][rack_id].AddTail(block_id);
					if (--(*bag)[block_id] == 0) {
						bag->RemoveKey(block_id);
					}
					proj.RemoveAt(ran);
				}
				nb[node->id] = 0;
				proj.RemoveAll();
			}

			MANAGER_CS_NODES.GetNext(pos);
		}

		pos = MANAGER_CS_NODES.GetHeadPosition();
		//long iterNum = listSize;
		long iterNum = MIN(bag->GetCount(), 10000);
		while (pos != NULL) {
			// transfer nb blocks of B from any source nodes to n
			// B = B - list of transferred blocks
			node_t *node = MANAGER_CS_NODES.GetValueAt(pos);
			long cnt = 0, max_cnt = nb[node->id];
			POSITION bpos = NULL, npos = NULL;
			block_t *b;
			bool same_rack = false;
			long check_cnt = 0;
			while (cnt < max_cnt && bag->IsEmpty() == false) {
				do {
					bpos = bag->GetStartPosition();
					long rnum = uniform_int(0, bag->GetCount() - 1);
					while (rnum--)
						bag->GetNext(bpos);
					b = GetBlock(bag->GetKeyAt(bpos));
					npos = b->local_node.GetHeadPosition();
					same_rack = false;
					while (npos != NULL) {
						if (GET_RACK_FROM_NODE(b->local_node.GetValueAt(npos)->id)
							== GET_RACK_FROM_NODE(node->id)) {
							same_rack = true;
							check_cnt++;
							break;
						}
						b->local_node.GetNext(npos);
					}
					if (same_rack == false || check_cnt > iterNum) {
						check_cnt = 0;
						break;
					}
				} while (true);
				MANAGER_BUDGET_MAP[node->id][GET_RACK_FROM_NODE(node->id)].AddTail(b->id);
				if (--(*bag)[b->id] == 0) {
					bag->RemoveKey(b->id);
				}
				--iterNum;
				++cnt;
			}
			nb[node->id] = 0;
			MANAGER_CS_NODES.GetNext(pos);
		}
	}
	
	for (i = CS_NODE_NUM; i < NODE_NUM; ++i)
	{
		if (MANAGER_CS[i] == true) {	// turn on nodes
			if (NODES[i].state == STATE_STANDBY)
				UPSET[i] = 1;
			//printf("%ld) %ld\n", (long)clock, GET_RACK_FROM_NODE(i));
			msg_t *msg = new msg_t;
			msg->node.power = true;
			M_NODE[i]->send((long)msg);
		}
		else {	// turn off nodes
			if (NODES[i].state == STATE_ACTIVE)
				DOWNSET[i] = 1;
			msg_t *msg = new msg_t;
			msg->node.power = false;
			M_NODE[i]->send((long)msg);
		}
	}
	delete bag;
}