#include <stdio.h>
#include <vector>
#include <list>
#include <map>
#include <algorithm>
#include <queue>
#include <sys/stat.h>
#include "cpp.h"

typedef enum { STATE_ACTIVATE, STATE_DEACTIVATE, STATE_STANDBY, STATE_IDLE, STATE_PEAK, STATE_ACTIVE, STATE_LENGTH } StateTypes;
typedef enum { O_CPU, O_MEMORY, O_DISK, O_NETWORK, O_QDELAY, O_LENGTH } OverheadTypes;
typedef enum { LOCAL_NODE, LOCAL_RACK, LOCAL_REMOTE, LOCAL_LENGTH } LocalTypes;
typedef enum { MODE_BASELINE, MODE_SIERRA, MODE_IPACS, MODE_RCS, MODE_PCS, MODE_LENGTH } ModeTypes;
typedef enum { FAIR_SCHEDULER, DELAY_SCHEDULER, SCHEDULER_LENGTH } SchedulerTypes;

typedef std::map<long, long> long_map_t;

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
		std::map<long, void*> blocks;
	} disk;
	struct {
		long capacity;
		long used;
		std::map<long, void*> blocks;
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

typedef std::map<long, node_t*> node_map_t;

struct rack_t
{
	long id;
	long rank;
	StateTypes state;
	node_map_t active_node_set;
	node_map_t standby_node_set;
	std::map<long, void*> blocks;
};

typedef std::map<long, rack_t*> rack_map_t;

struct block_t
{
	long id;
	long file_id;
	node_map_t local_node;
	rack_map_t local_rack;
};

struct file_t
{
	long id;
	long size;
	std::vector<block_t*> blocks;
	long_map_t acc;
};

struct job_t
{
	long id;
	long running;
	long map_total;
	std::vector<block_t*> map_splits;
	struct {
		double begin;
		double end;
		double qin;	
		double qtotal;
	} time;
};

union msg_t
{
	struct {
		long id;
		LocalTypes locality;
		long local_node;
		long split_index;
		job_t *job;
	} task;
	struct {
		bool power;
	} power;
};

struct rank_cmp {
	bool operator() (rack_t x, rack_t y)
	{
		return x.rank < y.rank;
	}
};