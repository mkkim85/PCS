#include "header.h"

bool MANAGER_CS[NODE_NUM] = { false, };
bool stable = true;
long MANAGER_MAP_SLOT_CAPACITY;
long MANAGER_NODE_NUM;
long MANAGER_BAG_SIZE;
std::list<long> INCOMPLETE_MAP_TASKS_Q;
std::list<std::map<long, long>> FILE_ACC_H;
std::priority_queue<rack_t, std::vector<rack_t>, rank_cmp> MANAGER_RANK;

extern node_t NODES[NODE_NUM];
extern rack_t RACKS[RACK_NUM];
extern long REMAIN_MAP_TASKS;
extern bool CSIM_END;
extern long SETUP_TIME_WINDOW;
extern double SETUP_ALPHA;
extern double SETUP_BETA;
extern long SETUP_MODE_TYPE;
extern std::map<long, node_t*> ACTIVE_NODE_SET, STANDBY_NODE_SET;
extern std::map<long, rack_t*> ACTIVE_RACK_SET, STANDBY_RACK_SET;
extern std::map<long, rack_t*> ACTIVE_RACK_NPG_SET, NPG_SET;
extern mailbox *M_NODE[NODE_NUM];

void state_manager(void)
{
	double m, mcap;
	long stable_t = clock;
	long req_m, top_k, size;
	long m_total = 0;
	std::map<long, long> *bag = NULL;

	if (SETUP_MODE_TYPE == MODE_BASELINE) return;

	create("state manager");
	while (!CSIM_END)
	{
		if (stable == false && MANAGER_NODE_NUM == ACTIVE_NODE_SET.size())
		{
			stable = true;
			stable_t = clock;
		}

		size = FILE_ACC_H.size();
		if (size >= SETUP_TIME_WINDOW)
		{
			FILE_ACC_H.pop_front();
			--size;
		}
		FILE_ACC_H.push_back(GetUnitOfFileAcc());
		++size;

		if (INCOMPLETE_MAP_TASKS_Q.size() >= SETUP_TIME_WINDOW)
		{
			m_total -= INCOMPLETE_MAP_TASKS_Q.front();
			INCOMPLETE_MAP_TASKS_Q.pop_front();
		}
		m_total += REMAIN_MAP_TASKS;
		INCOMPLETE_MAP_TASKS_Q.push_back(REMAIN_MAP_TASKS);

		if (stable && size >= SETUP_TIME_WINDOW
			&& ((long)(clock - stable_t) >= (long)MODE_TRANS_INTERVAL))
		{
			m = (double)m_total / (double)SETUP_TIME_WINDOW;
			mcap = MANAGER_MAP_SLOT_CAPACITY;

			if (((m / mcap >= 1 + SETUP_ALPHA) && (STANDBY_NODE_SET.empty() == false))
				|| ((m / mcap < 1 - SETUP_BETA) && ACTIVE_NODE_SET.size() > CS_NODE_NUM))
			{
				stable = false;
				req_m = m - (CS_NODE_NUM * MAP_SLOTS);
				bag = GetPopularBlockList(&top_k);

				if (SETUP_MODE_TYPE == MODE_SIERRA)
				{
					bag = FindSierra(MANAGER_CS, top_k, req_m);
				}
				else if (SETUP_MODE_TYPE == MODE_IPACS)
				{
					bag = FindiPACS(MANAGER_CS, bag, top_k, req_m);
				}
				else if (SETUP_MODE_TYPE == MODE_RCS)
				{
					bag = FindRCS(MANAGER_CS, req_m);
				}
				else if (SETUP_MODE_TYPE == MODE_PCS)
				{
					bag = FindPCS(MANAGER_CS, bag, req_m);
				}

				ActivateNodes(MANAGER_CS, bag);
			}
		}

		hold(TIME_UNIT);
	}
}

std::map<long, long>* FindSierra(bool cs[], long top_k, long req_m)
{
	long g, node;
	memset(&MANAGER_CS, true, sizeof(bool) * CS_NODE_NUM);
	memset(&MANAGER_CS[CS_NODE_NUM], false, sizeof(bool) * NODE_NUM - CS_NODE_NUM);

	top_k = MIN(top_k, REPLICATION_FACTOR) - 1;

	for (g = 1; g < REPLICATION_FACTOR; ++g)
	{
		if (req_m <= 0 && top_k-- <= 1)
		{
			break;
		}

		for (node = g * CS_NODE_NUM; node < (g + 1) * CS_NODE_NUM; ++node)
		{
			cs[node] = true;
			req_m -= MAP_SLOTS;
		}
	}

	return NULL;
}

std::map<long, long>* FindiPACS(bool cs[], std::map<long, long> *bag, long top_k, long req_m)
{
	long g, node;
	memset(&MANAGER_CS, true, sizeof(bool) * CS_NODE_NUM);
	memset(&MANAGER_CS[CS_NODE_NUM], false, sizeof(bool)* NODE_NUM - CS_NODE_NUM);
	block_t *block;

	top_k = MIN(top_k, REPLICATION_FACTOR) - 1;

	if (bag != NULL)
	{
		std::map<long, long>::iterator iter;
		for (iter = bag->begin(); iter != bag->end(); bag->erase(iter++))
		{
			long cnt = top_k;
			block = GetBlock(iter->first);
			std::map<long, node_t*>::iterator item = block->local_node.begin();
			while (cnt--)
			{
				++item;
				node = item->second->id;
				if (cs[node] == false)
				{
					cs[node] = true;
					req_m -= MAP_SLOTS;
				}
			}
		}
	}

	std::map<long, node_t*> S = STANDBY_NODE_SET;
	while (req_m > 0 && !S.empty())
	{
		std::map<long, node_t*>::iterator item = S.begin();
		std::advance(item, uniform_int(0, S.size() - 1));
		node = item->second->id;
		cs[node] = true;
		req_m -= MAP_SLOTS;
		S.erase(item);
	}

	return bag;
}

std::map<long, long>* FindRCS(bool cs[], long req_m)
{
	long g, node;
	rack_t *rack;
	memset(&MANAGER_CS, true, sizeof(bool) * CS_NODE_NUM);
	memset(&MANAGER_CS[CS_NODE_NUM], false, sizeof(bool)* NODE_NUM - CS_NODE_NUM);

	std::map<long, rack_t*> S = STANDBY_RACK_SET;
	while (req_m > 0 && S.size() > 0)
	{
		std::map<long, rack_t*>::iterator ritem = S.begin();
		std::advance(ritem, uniform_int(0, S.size() - 1));
		rack = ritem->second;
		S.erase(ritem);

		std::map<long, node_t*> N = rack->standby_node_set;
		while (req_m > 0 && !N.empty())
		{
			std::map<long, node_t*>::iterator nitem = N.begin();
			std::advance(nitem, uniform_int(0, N.size() - 1));
			node = nitem->second->id;

			if (cs[node] == false)
			{
				cs[node] = true;
				req_m -= MAP_SLOTS;
			}
			N.erase(nitem);
		}
	}

	return NULL;
}

std::map<long, long>* FindPCS(bool cs[], std::map<long, long> *bag, long req_m)
{
	long i, budget_size = 0, bag_size = MANAGER_BAG_SIZE;
	node_t *node;
	rack_t *rack;
	std::map<long, rack_t*> F = NPG_SET;
	std::map<long, rack_t*> F_INTERSECT_RACTIVE = ACTIVE_RACK_NPG_SET;
	memset(&MANAGER_CS, true, sizeof(bool) * CS_NODE_NUM);
	memset(&MANAGER_CS[CS_NODE_NUM], false, sizeof(bool)* NODE_NUM - CS_NODE_NUM);

	while ((req_m > 0 || !bag->empty()) && !F.empty())
	{
		if (!bag->empty())
		{
			i = MANAGER_RANK.top().id;
			MANAGER_RANK.pop();
		}
		else if (!F_INTERSECT_RACTIVE.empty())
		{
			std::map<long, rack_t*>::iterator item = F_INTERSECT_RACTIVE.begin();
			std::advance(item, uniform_int(0, F_INTERSECT_RACTIVE.size() - 1));
			i = item->second->id;
			F_INTERSECT_RACTIVE.erase(i);
		}
		else
		{
			std::map<long, rack_t*>::iterator item = F.begin();
			std::advance(item, uniform_int(0, F.size() - 1));
			i = item->second->id;
		}
		F.erase(i);
		rack = &RACKS[i];

		std::map<long, node_t*>::iterator item = rack->standby_node_set.begin(),
			end = rack->standby_node_set.end();

		while (item != end)
		{
			node = item->second;
			if (cs[node->id] == false)
			{
				cs[node->id] = true;
				req_m -= MAP_SLOTS;
				// TODO: Invalidate replicas of n's storage budget that are not appeared in bag;
				// TODO: B = B - n.getBlockList();
				budget_size = budget_size + (node->space.budget.capacity - node->space.budget.used);
			}

			if (req_m <= 0 && budget_size >= bag_size)
			{
				return bag;
			}

			++item;
		}
	}

	return bag;
}

void ActivateNodes(bool cs[], std::map<long, long> *bag)
{
	long i;
	MANAGER_NODE_NUM = CS_NODE_NUM;
	MANAGER_MAP_SLOT_CAPACITY = CS_NODE_NUM * MAP_SLOTS;
	msg_t *msg;

	if (SETUP_MODE_TYPE == MODE_PCS && bag != NULL)
	{ // TODO: using bag
		;
	}

	for (i = CS_NODE_NUM; i < NODE_NUM; ++i)
	{
		if (cs[i] == true)
		{
			if (NODES[i].state == STATE_STANDBY)
			{
				msg = new msg_t;
				msg->power.power = true;
				M_NODE[i]->send((long)msg);
			}

			MANAGER_MAP_SLOT_CAPACITY += MAP_SLOTS;
			++MANAGER_NODE_NUM;
		}
		else
		{
			if (NODES[i].state == STATE_IDLE || NODES[i].state == STATE_PEAK)
			{
				msg = new msg_t;
				msg->power.power = false;
				M_NODE[i]->send((long)msg);
			}
		}
	}
}