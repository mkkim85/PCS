#include <stdio.h>
#include <algorithm>
#include <atlcoll.h>
#include <sys/stat.h>
#include "cpp.h"

typedef enum { STATE_ACTIVATE, STATE_DEACTIVATE, STATE_STANDBY, STATE_ACTIVE, STATE_LENGTH } StateTypes;
typedef enum { O_CPU, O_MEMORY, O_DISK, O_NETWORK, O_QDELAY, O_LENGTH } OverheadTypes;
typedef enum { LOCAL_NODE, LOCAL_RACK, LOCAL_REMOTE, LOCAL_LENGTH } LocalTypes;
typedef enum { MODE_BASELINE, MODE_SIERRA, MODE_IPACS, MODE_PCS, MODE_PCSC, MODE_LENGTH } ModeTypes;
typedef enum { FAIR_SCHEDULER, DELAY_SCHEDULER, SCHEDULER_LENGTH } SchedulerTypes;

typedef CAtlMap<long, long> long_map_t;

struct slot_t
{
	long id;
	bool used;
};

struct storage_t
{
	long capacity;
	long used;
	struct {
		long capacity;
		long used;
		CAtlMap<long, void*> blocks;
	} disk;
	struct {
		long capacity;
		long used;
		CAtlMap<long, void*> blocks;
		//CAtlList<long> lru_cache;
	} budget;
};

struct node_t
{
	long id;
	StateTypes state;
	struct {
		long capacity;
		long used;
		slot_t *slot[MAP_SLOTS];
	} mapper;
	storage_t space;
};

typedef CRBMap<long, node_t*> node_map_t;

struct rack_t
{
	long id;
	long rank;
	StateTypes state;
	node_map_t active_node_set;
	node_map_t standby_node_set;
	CAtlMap<long, long> blocks;
};

typedef CRBMap<long, rack_t*> rack_map_t;

struct block_t
{
	long id;
	long file_id;
	node_map_t local_node;
	CRBMap<long, long> local_rack;
};

struct file_t
{
	long id;
	long size;
	CAtlArray<block_t*> blocks;
	long_map_t acc;
};


struct job_t
{
	long id;
	long user_id;
	long running;
	long run_total;
	long map_total;
	long skipcount;
	CAtlMap<long, CAtlMap<long, long_map_t>> map_splits;
	CAtlMap<long, long_map_t> map_cascade;
	struct {
		double begin;
		double end;
		double qtotal;
	} time;
};

union msg_t
{
	bool power;
	struct {
		long id;
		LocalTypes locality;
		long local_node;
		block_t *block;
		job_t *job;
	} task;
};