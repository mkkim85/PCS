// simulation parameters
//#define REPLICA_LIMIT			INT_MAX		//INT_MAX
//#define RAND_LAYOUT				false
#define FB_WORKLOAD				false
#define FB_PATH					"files/workload.txt"
#define FB_DATA_PATH			"files/data.txt"
#define FB_LOAD_RATIO			0.3
#define LOGGING					false
#define REPLICATION_FACTOR		3
#define MAP_COMPUTATION_TIME	300.0
#define JOB_MAP_TASK_NUM		1024
#define DATA_BLOCK_NUM			(32 * 1024)		// 32n, 2TB
#define TIME_UNIT				1.0
#define MINUTE					60
#define HOUR					(60 * MINUTE)

// node
#define CS_NODE_NUM				400
#define NODE_NUM 				(int)(CS_NODE_NUM * REPLICATION_FACTOR)
#define NODE_U_POWER			129.0		// ACTIVATE
#define NODE_D_POWER			129.0		// DEACTIVATE
#define NODE_S_POWER			18.0		// STANDBY
#define NODE_A_POWER			315.0		// ACTIVE
//#define NODE_U_TIME				100.0		// ACTIVATING TIME (sec)
#define NODE_D_TIME				11.0		// DEACTIVATING TIME (sec)
#define MAP_SLOTS				8			// HP(Xeon)
#define MAP_SLOTS_MAX			(int)(NODE_NUM * MAP_SLOTS)
#define CPU_CORE				(int)(MAP_SLOTS)
#define MEMORY_SIZE				128			// 8 GB, 128 blocks
#define MEMORY_SPEED			0.003		// 0.003 s/block, PC4-17000, 17,000 MB/s
#define DISK_SIZE				4000		// 250 GB, 4,000 blocks
#define DISK_NUM				2
#define DISK_SPEED				0.64		// 0.64 s/block, 100 MB/s
#define HEARTBEAT_PERIOD		1

// rack
#define CS_RACK_NUM				50
#define RACK_NUM				(int)(CS_RACK_NUM * REPLICATION_FACTOR)
#define NODE_NUM_IN_RACK		(int)(NODE_NUM / RACK_NUM)
#define SWTICH_NUM				(int)(NODE_NUM_IN_RACK)
#define RACK_POWER				5940.0		// 5.94 kW
#define SWITCH_DELAY			0.1		// 10 Gbps, 1% overhead
#define SWITCH_SPEED			0.5		// 0.05 s/block, 10 Gbps, 1.25 GB/s

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

#define PARTITION_FACTOR		4
#define PARTITION_NODE_NUM		(NODE_NUM_IN_RACK / PARTITION_FACTOR)
#define MOD_NUM					(CS_NODE_NUM / PARTITION_NODE_NUM)

// gzip
#define COMP_T					(64.0 / (144.0 / 8.0))	// 144 Mbps
#define DECOMP_T				(64.0 / (413.0 / 8.0))	// 413 Mbps
#define COMP_FACTOR				uniform_int(12, 13)

// PPMVC
//#define COMP_T					(64.0 / (30.2 / 8.0))	// 30.2 Mbps
//#define DECOMP_T				(64.0 / (31.4 / 8.0))	// 31.4 Mbps
//#define COMP_FACTOR				uniform_int(26, 27)

#define ENABLE_COMP				false