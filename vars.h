// simulation parameters
#define REPLICATION_FACTOR		3
#define MAP_COMPUTATION_TIME	100.0
#define JOB_MAP_TASK_NUM		1024
#define DATA_BLOCK_NUM			(32 * 1024)		// 32n, 2TB

// node
#define NODE_NUM 				1200
#define CS_NODE_NUM 			(NODE_NUM / REPLICATION_FACTOR)
#define NODE_I_POWER			259.5		// IDLE
#define NODE_U_POWER			129.0		// ACTIVATE
#define NODE_D_POWER			129.0		// DEACTIVATE
#define NODE_S_POWER			18.0		// STANDBY
#define NODE_P_POWER			315.0		// PEAK
#define NODE_U_TIME				100.0		// ACTIVATING TIME (sec)
#define NODE_D_TIME				11.0		// DEACTIVATING TIME (sec)
#define MEMORY_SIZE				64			// 4 GB, 64 blocks
#define MEMORY_SPEED			0.003		// 0.003 s/block, PC4-17000, 17,000 MB/s
#define DISK_SIZE				4000		// 250 GB, 4,000 blocks
#define DISK_SPEED				0.64		// 0.64 s/block, 100 MB/s
#define MAP_SLOTS				8			// HP(Xeon)
#define MAP_SLOTS_MAX			(NODE_NUM * MAP_SLOTS)

// rack
#define RACK_NUM				60
#define CS_RACK_NUM				(RACK_NUM / REPLICATION_FACTOR)
#define NODE_NUM_IN_RACK		(NODE_NUM / RACK_NUM)
#define RACK_POWER				5940.0		// 5.94 kW
#define SWITCH_SPEED			0.05		// 0.05 s/block, 10 Gbps, 1.25 GB/s