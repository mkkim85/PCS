// simulation parameters
//#define REPLICA_LIMIT			INT_MAX		//INT_MAX
//#define RAND_LAYOUT				false
#define FB_WORKLOAD				true
#define FB_PATH					"files/workload.txt"
#define FB_DATA_PATH			"files/data.txt"
#define FB_LOAD_RATIO			0.5
#define REPLICATION_FACTOR		3
#define MAP_COMPUTATION_TIME	100.0
#define JOB_MAP_TASK_NUM		64
#define DATA_BLOCK_NUM			(160 * 1024)		// 160n, 10TB
#define TIME_UNIT				1.0
#define MINUTE					60
#define HOUR					(60 * MINUTE)

// node
#define CS_NODE_NUM				400
#define NODE_NUM 				(long)(CS_NODE_NUM * REPLICATION_FACTOR)
#define NODE_U_POWER			0.3			// ACTIVATE
#define NODE_D_POWER			0.3			// DEACTIVATE
#define NODE_S_POWER			0			// STANDBY
#define NODE_A_POWER			0.3			// ACTIVE
//#define NODE_U_TIME				100.0		// ACTIVATING TIME (sec)
#define NODE_D_TIME				10.0		// DEACTIVATING TIME (sec)
#define MAP_SLOTS				8			// HP(Xeon)
#define MAP_SLOTS_MAX			(long)(NODE_NUM * MAP_SLOTS)
#define CPU_CORE				(long)(MAP_SLOTS)
//#define MEMORY_SIZE				128			// 8 GB, 128 blocks
#define MEMORY_SIZE				30			// 2 GB
#define MEMORY_SPEED			0.003		// 0.003 s/block, PC4-17000, 17,000 MB/s
#define DISK_SIZE				4000		// 250 GB, 4,000 blocks
#define DISK_NUM				2
#define DISK_SPEED				0.64		// 0.64 s/block, 100 MB/s
#define HEARTBEAT_PERIOD		1

// rack
#define CS_RACK_NUM				25
#define RACK_NUM				(long)(CS_RACK_NUM * REPLICATION_FACTOR)
#define NODE_NUM_IN_RACK		(long)(NODE_NUM / RACK_NUM)
#define SWTICH_NUM				(long)(NODE_NUM_IN_RACK)
#define RACK_POWER				2.4			// 2.4 kW
#define SWITCH_DELAY			0.1			// 10 Gbps, 1% overhead
#define SWITCH_SPEED			0.05		// 0.05 s/block, 10 Gbps, 1.25 GB/s
#define MASTER_SPEED			0.05

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

// gzip
//#define COMP_T					(64.0 / (144.0 / 8.0))	// 144 Mbps
//#define DECOMP_T				(64.0 / (413.0 / 8.0))	// 413 Mbps
//#define COMP_FACTOR				uniform_int(12, 13)

// PPMVC
#define COMP_T					(64.0 / (30.2 / 8.0))	// 30.2 Mbps
#define DECOMP_T				(64.0 / (31.4 / 8.0))	// 31.4 Mbps
#define COMP_FACTOR				uniform_int(26, 27)

#define FILE_NUM				(DATA_BLOCK_NUM / JOB_MAP_TASK_NUM)
#define MOD_FACTOR				CS_RACK_NUM