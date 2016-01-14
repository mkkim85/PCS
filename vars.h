// simulation parameters
#define LOGGING					false
#define REPLICATION_FACTOR		3
#define MAP_COMPUTATION_TIME	300.0
#define JOB_MAP_TASK_NUM		1024
#define DATA_BLOCK_NUM			(16 * 1024)		// 16n, 1TB
#define TIME_UNIT				1.0
#define MINUTE					60
#define HOUR					(60 * MINUTE)
#define MODE_TRANS_INTERVAL		(3 * MINUTE)

// node
#define NODE_NUM 				1200
#define CS_NODE_NUM 			(NODE_NUM / REPLICATION_FACTOR)
#define NODE_U_POWER			129.0		// ACTIVATE
#define NODE_D_POWER			129.0		// DEACTIVATE
#define NODE_S_POWER			18.0		// STANDBY
#define NODE_A_POWER			315.0		// ACTIVE
#define NODE_U_TIME				100.0		// ACTIVATING TIME (sec)
#define NODE_D_TIME				11.0		// DEACTIVATING TIME (sec)
#define MAP_SLOTS				8			// HP(Xeon)
#define MAP_SLOTS_MAX			(NODE_NUM * MAP_SLOTS)
#define CPU_CORE				(MAP_SLOTS)
#define MEMORY_SIZE				128			// 8 GB, 128 blocks
#define MEMORY_SPEED			0.003		// 0.003 s/block, PC4-17000, 17,000 MB/s
#define DISK_SIZE				4000		// 250 GB, 4,000 blocks
#define DISK_NUM				(CPU_CORE / 2)
#define DISK_SPEED				0.64		// 0.64 s/block, 100 MB/s
#define HEARTBEAT_PERIOD		1

// rack
#define RACK_NUM				150
#define CS_RACK_NUM				(RACK_NUM / REPLICATION_FACTOR)
#define NODE_NUM_IN_RACK		(NODE_NUM / RACK_NUM)
#define SWTICH_NUM				(NODE_NUM_IN_RACK * 1.5)
#define RACK_POWER				5940.0		// 5.94 kW
#define SWITCH_DELAY			0.1
#define SWITCH_SPEED			0.05		// 0.05 s/block, 10 Gbps, 1.25 GB/s

// setup
#define MAX_PROCESSES			(NODE_NUM + MAP_SLOTS_MAX + 7)
#define MAX_FACILITIES			((NODE_NUM * 3) + RACK_NUM + 1)
#define MAX_SERVERS				((NODE_NUM * 3) + (NODE_NUM * CPU_CORE) + (NODE_NUM * DISK_NUM) + (RACK_NUM * SWTICH_NUM) + 1)
#define MAX_MAILBOXES			(NODE_NUM + MAP_SLOTS_MAX)
#define MAX_MESSAGES			(MAX_MAILBOXES)
#define MAX_EVENTS				((MAX_SERVERS + MAX_MESSAGES) * 2)

#define GET_NODE_FROM_MAPPER(N)	((N < MAP_SLOTS) ? (0) : (N / MAP_SLOTS))
#define GET_RACK_FROM_NODE(N)	((N < NODE_NUM_IN_RACK) ? (0) : (N / NODE_NUM_IN_RACK))
#define GET_G_FROM_NODE(N)		((N < CS_NODE_NUM) ? (0) : (N / CS_NODE_NUM))
#define GET_G_FROM_RACK(N)		((N < CS_RACK_NUM) ? (0) : (N / CS_RACK_NUM))

#define MIN(X, Y)				((X < Y) ? (X) : (Y))
#define MAX(X, Y)				((X > Y) ? (X) : (Y))