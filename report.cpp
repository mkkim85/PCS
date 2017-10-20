#include "header.h"

double REPORT_KW, REPORT_TOTAL_KW;
double REPORT_KW_NPG, REPORT_TOTAL_KW_NPG; // non-primary groups
double REPORT_RESP_T_TOTAL, REPORT_Q_DELAY_T_TOTAL;
long REPORT_RESP_T_COUNT, REPORT_Q_DELAY_T_COUNT;
long REPORT_LOCALITY[LOCAL_LENGTH];
long REPORT_RACK_STATE_COUNT[STATE_LENGTH], REPORT_NODE_STATE_COUNT[STATE_LENGTH];
long REPORT_MAP_TASKS, REPORT_BUDGET_SIZE, REPORT_BUDGET_HIT;
long REPORT_TOP_K;
std::pair<double, long> REPORT_AVG_M;
std::pair<long, long> REPORT_CAP, REPORT_REQ_M;
std::pair<double, long> REPORT_TASK_T, REPORT_CPU_T, REPORT_MEM_T, REPORT_DISK_T, REPORT_NETWORK_T, REPORT_TASK_Q_T;
std::pair<long, long> REPORT_NODE, REPORT_RACK;

extern bool CSIM_END;
extern double SETUP_COMPUTATION_TIME, SETUP_RACK_POWER_RATIO, SETUP_RACK_SWITCH_SPEED;
extern long SETUP_REPORT_PERIOD;
extern char SETUP_REPORT_PATH[];
extern FILE *SETUP_REPORT_FILE;
extern node_map_t ACTIVE_NODE_SET, STANDBY_NODE_SET;
extern rack_map_t ACTIVE_RACK_SET, STANDBY_RACK_SET;
extern CAtlMap<long, long> BAG_HISTORY;
extern long rack_acc[CS_RACK_NUM];

extern double node_turnon_time, budget_tran_time;
extern long node_turnon_num, budget_tran_num;

void sim_report(void)
{
	long t;
	long hold_t = SETUP_REPORT_PERIOD;
	//FILE *fbag = fopen("bag.csv", "w");
	//FILE *ftime = fopen("time.csv", "w");
	//fprintf(ftime, "clock,avg_node_turn_t,avg_budget_tran_t,node_turn_t,node_turn_n,budget_tran_t,budget_tran_n\n");

	fprintf(SETUP_REPORT_FILE, "clock,kW,total_kW,kW_npg,total_kW_npg,node,rack,total_resp_t,resp_t_count,avg_resp_t,total_q_delay_t,q_delay_count,avg_q_delay,local_node,local_rack,local_remote,map_tasks,task_t,cpu_t,mem_t,disk_t,net_t,task_q_t,avg_m,active_cap,m/cap,req_m,req_node,budget hit,budget size,top k,budget_tran_t,budget_tran_n\n");
	fclose(SETUP_REPORT_FILE);
	FILE *f_rack = fopen("rack_acc.csv", "w");
	fprintf(f_rack, "clock");
	for (int i = 0; i < CS_RACK_NUM; i++)
		fprintf(f_rack, ",rack #%ld", i);
	fprintf(f_rack, "\n");

	create("report");
	
	while (!CSIM_END) {
		t = clock;

		REPORT_KW = REPORT_KW
			+ (REPORT_RACK_STATE_COUNT[STATE_ACTIVE] * (SETUP_RACK_POWER_RATIO / HOUR))
			+ (REPORT_NODE_STATE_COUNT[STATE_STANDBY] * (NODE_S_POWER / HOUR))
			+ (REPORT_NODE_STATE_COUNT[STATE_ACTIVATE] * (NODE_U_POWER / HOUR))
			+ (REPORT_NODE_STATE_COUNT[STATE_DEACTIVATE] * (NODE_D_POWER / HOUR))
			+ (REPORT_NODE_STATE_COUNT[STATE_ACTIVE] * (NODE_A_POWER / HOUR));

		REPORT_KW_NPG = REPORT_KW_NPG
			+ ((REPORT_RACK_STATE_COUNT[STATE_ACTIVE] - CS_RACK_NUM) * (SETUP_RACK_POWER_RATIO / HOUR))
			+ (REPORT_NODE_STATE_COUNT[STATE_STANDBY] * (NODE_S_POWER / HOUR))
			+ (REPORT_NODE_STATE_COUNT[STATE_ACTIVATE] * (NODE_U_POWER / HOUR))
			+ (REPORT_NODE_STATE_COUNT[STATE_DEACTIVATE] * (NODE_D_POWER / HOUR))
			+ ((REPORT_NODE_STATE_COUNT[STATE_ACTIVE] - CS_NODE_NUM) * (NODE_A_POWER / HOUR));

		//REPORT_NODE.first += ACTIVE_NODE_SET.GetCount();
		REPORT_NODE.first += REPORT_NODE_STATE_COUNT[STATE_ACTIVE];;
		REPORT_NODE.second++;

		//REPORT_RACK.first += ACTIVE_RACK_SET.GetCount();
		REPORT_RACK.first += REPORT_RACK_STATE_COUNT[STATE_ACTIVE];
		REPORT_RACK.second++;

		if (t > 0 && (t % hold_t) == 0) {
			fprintf(f_rack, "%ld,%ld", (long)clock, rack_acc[0]);
			rack_acc[0] = 0;
			for (int i = 1; i < CS_RACK_NUM; i++) {
				fprintf(f_rack, ",%ld", rack_acc[i]);
				rack_acc[i] = 0;
			}
			fprintf(f_rack, "\n");
			//fprintf(ftime, "%ld,%lf,%lf,%lf,%ld,%lf,%lf\n", (long)clock, node_turnon_time / node_turnon_num, budget_tran_time / budget_tran_num, node_turnon_time, node_turnon_num, budget_tran_time, budget_tran_num);
			/*fprintf(fbag, "%ld,%ld", (long)clock,BAG_HISTORY.GetCount());
			for (POSITION pos = BAG_HISTORY.GetStartPosition(); pos != NULL; BAG_HISTORY.GetNext(pos)) {
				fprintf(fbag, ",%ld(%ld)", BAG_HISTORY.GetAt(pos)->m_key, BAG_HISTORY.GetAt(pos)->m_value);
			}
			fprintf(fbag, "\n", (long)clock);*/
			BAG_HISTORY.RemoveAll();

			REPORT_TOTAL_KW += REPORT_KW;
			REPORT_TOTAL_KW_NPG += REPORT_KW_NPG;
			long NN = REPORT_NODE.first / REPORT_NODE.second;
			long RN = REPORT_RACK.first / REPORT_RACK.second;

			SETUP_REPORT_FILE = fopen(SETUP_REPORT_PATH, "a");
			fprintf(SETUP_REPORT_FILE, 
				"%ld,%lf,%lf,%lf,%lf,%ld,%ld,%lf,%ld,%lf,%lf,%ld,%lf,%ld,%ld,%ld,%ld,%lf,%lf,%lf,%lf,%lf,%lf,%lf,%ld,%lf,%ld,%ld,%ld,%ld,%ld,%lf,%ld\n",
				(long)clock, REPORT_KW, REPORT_TOTAL_KW, 
				REPORT_KW_NPG, REPORT_TOTAL_KW_NPG, NN, RN,
				REPORT_RESP_T_TOTAL, REPORT_RESP_T_COUNT, 
				REPORT_RESP_T_COUNT > 0 ? REPORT_RESP_T_TOTAL / REPORT_RESP_T_COUNT : 0,
				REPORT_Q_DELAY_T_TOTAL, REPORT_Q_DELAY_T_COUNT, 
				REPORT_Q_DELAY_T_COUNT > 0 ? REPORT_Q_DELAY_T_TOTAL / REPORT_Q_DELAY_T_COUNT : 0,
				REPORT_LOCALITY[LOCAL_NODE], REPORT_LOCALITY[LOCAL_RACK], REPORT_LOCALITY[LOCAL_REMOTE],
				REPORT_MAP_TASKS,
				REPORT_TASK_T.second > 0 ? (double)REPORT_TASK_T.first / REPORT_TASK_T.second : 0,
				REPORT_CPU_T.second > 0 ? (double)REPORT_CPU_T.first / REPORT_CPU_T.second : 0,
				REPORT_MEM_T.second > 0 ? (double)REPORT_MEM_T.first / REPORT_MEM_T.second : 0,
				REPORT_DISK_T.second > 0 ? (double)REPORT_DISK_T.first / REPORT_DISK_T.second : 0,
				REPORT_NETWORK_T.second > 0 ? (double)REPORT_NETWORK_T.first / REPORT_NETWORK_T.second : 0,
				REPORT_TASK_Q_T.second > 0 ? (double)REPORT_TASK_Q_T.first / REPORT_TASK_Q_T.second : 0,
				REPORT_AVG_M.second > 0 ? (double)REPORT_AVG_M.first / REPORT_AVG_M.second : 0 ,
				REPORT_CAP.second > 0 ? (long)REPORT_CAP.first / REPORT_CAP.second : 0,
				REPORT_CAP.second > 0 ? (double)(REPORT_AVG_M.first / REPORT_AVG_M.second) / (REPORT_CAP.first / REPORT_CAP.second) : 0,
				REPORT_REQ_M.second > 0 ? (long)(REPORT_REQ_M.first / REPORT_REQ_M.second) : 0,
				REPORT_REQ_M.second > 0 ? (long)ceil((REPORT_REQ_M.first / REPORT_REQ_M.second) / MAP_SLOTS) : 0,
				REPORT_BUDGET_HIT, REPORT_BUDGET_SIZE,
				REPORT_TOP_K,
				budget_tran_time,
				budget_tran_num);
			fclose(SETUP_REPORT_FILE);

			node_turnon_num = node_turnon_time = 0;
			budget_tran_num = budget_tran_time = 0;

			REPORT_KW = 0;
			REPORT_KW_NPG = 0;

			REPORT_NODE = { 0, 0 };
			REPORT_RACK = { 0, 0 };

			REPORT_RESP_T_TOTAL = 0;
			REPORT_RESP_T_COUNT = 0;

			REPORT_Q_DELAY_T_TOTAL = 0;
			REPORT_Q_DELAY_T_COUNT = 0;

			for (long i = 0; i < LOCAL_LENGTH; ++i) {
				REPORT_LOCALITY[i] = 0;
			}

			REPORT_MAP_TASKS = 0;

			REPORT_TASK_T = { 0, 0 };
			REPORT_CPU_T = { 0, 0 };

			REPORT_DISK_T = { 0, 0 };
			REPORT_NETWORK_T = { 0, 0 };
			REPORT_TASK_Q_T = { 0, 0 };

			REPORT_AVG_M = { 0.0, 0 };
			REPORT_CAP = { 0, 0 };
			REPORT_REQ_M = { 0, 0 };

			REPORT_BUDGET_HIT = 0;
		}

		hold(TIME_UNIT);
	}
}