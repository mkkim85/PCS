#include <stdio.h>
#include <vector>
#include <list>
#include <map>
#include <algorithm>
#include "cpp.h"

typedef enum { STATE_ACTIVATE, STATE_DEACTIVATE, STATE_STANDBY, STATE_IDLE, STATE_PEAK, STATE_ACTIVE, STATE_LENGTH } StateTypes;

struct block_t
{
	long id;
	long file_id;
	std::list<long> locations; // local nodes
};

struct file_t
{
	long id;
	long map_total;
	std::vector<block_t*> map_splits;
};

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
	} disk;
	struct {
		long capacity;
		long used;
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

struct rack_t
{
	long id;
	StateTypes state;
	std::vector<node_t*> active_node_set;
	std::vector<node_t*> standby_node_set;
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
	} time;
};

union msg_t
{
	struct {
		long id;
		long split_index;
		job_t *job;
	} task;
};