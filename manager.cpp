#include "header.h"

bool stable = true;
long MANAGER_MAP_SLOT_CAPACITY;
std::list<long> MAX_ACC_H, INCOMPLETE_MAP_TASKS_Q;

extern long REMAIN_MAP_TASKS;
extern bool CSIM_END;
extern long SETUP_TIME_WINDOW;
extern double SETUP_ALPHA;
extern double SETUP_BETA;
extern long SETUP_MODE_TYPE;
extern std::map<long, node_t*> ACTIVE_NODE_SET, STANDBY_NODE_SET;

void state_manager(void)
{
	double m, mcap;
	long req_m, top_k;
	long m_total = 0;
	std::map<long, long> bag;
	covering_t *covering_set;

	if (SETUP_MODE_TYPE == MODE_BASELINE) return;

	create("state manager");
	while (!CSIM_END)
	{
		if (MAX_ACC_H.size() >= SETUP_TIME_WINDOW)
		{
			MAX_ACC_H.pop_front();
		}
		MAX_ACC_H.push_back(GetMaxFileAcc());
		if (INCOMPLETE_MAP_TASKS_Q.size() >= SETUP_TIME_WINDOW)
		{
			m_total -= INCOMPLETE_MAP_TASKS_Q.front();
			INCOMPLETE_MAP_TASKS_Q.pop_front();
		}
		m_total += REMAIN_MAP_TASKS;
		INCOMPLETE_MAP_TASKS_Q.push_back(REMAIN_MAP_TASKS);

		if (stable && clock >= SETUP_TIME_WINDOW)
		{
			m = (double)m_total / (double)SETUP_TIME_WINDOW;
			mcap = MANAGER_MAP_SLOT_CAPACITY;

			if ((m / mcap >= 1 + SETUP_ALPHA && STANDBY_NODE_SET.empty() == false)
				|| (m / mcap < 1 - SETUP_BETA) && ACTIVE_NODE_SET.size() > CS_NODE_NUM)
			{
				stable = false;
				// TODO: GetPupularBlockList, FindPCS, ActivateNodes
				req_m = m - (CS_NODE_NUM * MAP_SLOTS);
				top_k = GetTopK(&MAX_ACC_H);

				if (SETUP_MODE_TYPE == MODE_SIERRA)
				{
					covering_set = FindSierra(top_k, req_m);
				}
				/*else if (SETUP_MODE_TYPE == MODE_IPACS)
				{
					bag = GetPopularBlockList();
					FindiPACS(top_k, req_m, &bag);
				}
				else if (SETUP_MODE_TYPE == MODE_RCS)
				{
					FindRCS(top_k, req_m);
				}
				else if (SETUP_MODE_TYPE == MODE_PCS)
				{
					bag = GetPopularBlockList();
					FindPCS(top_k, req_m, &bag);
				}*/

				// ActivateNodes(find_set);

				stable = true;
			}
		}

		hold(TIME_UNIT);
	}
}

covering_t* FindSierra(long top_k, long req_m)
{
	covering_t *cs = new covering_t;
	long g, node;
	memset(cs->covering_set, false, sizeof(bool) * NODE_NUM);
	cs->bag = NULL;

	for (g = 1; g < REPLICATION_FACTOR; ++g)
	{
		if (req_m <= 0 && top_k-- <= 1)
		{
			break;
		}

		for (node = g * CS_NODE_NUM; node < (g + 1) * CS_NODE_NUM; ++node)
		{
			cs->covering_set[node] = true;
			req_m -= MAP_SLOTS;
		}
	}

	return cs;
}

//covering_t* FindiPACS(long top_k, long req_m, std::map<long, long> *bag)
//{
//	return NULL;
//}
//
//covering_t* FindRCS(long top_k, long req_m)
//{
//	return NULL;
//}
//
//covering_t* FindPCS(long top_k, long req_m, std::map<long, long> *bag)
//{
//	return NULL;
//}

void ActivateNodes(covering_t *cs)
{

}