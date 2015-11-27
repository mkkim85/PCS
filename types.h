#include <stdio.h>
#include <vector>
#include <list>
#include <map>
#include <set>
#include "cpp.h"

typedef enum { STATE_ACTIVATE, STATE_DEACTIVATE, STATE_STANDBY, STATE_IDLE, STATE_PEAK, STATE_ACTIVE, STATE_LENGTH } StateTypes;

struct block_t
{
	long id;
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
	bool busy;
};

struct node_t
{
	long id;
	StateTypes state;
	long mthread;
	slot_t *mslot[MAP_SLOTS];
};

struct rack_t
{
	long id;
	StateTypes state;
	std::vector<node_t*> active_node_set;
	std::vector<node_t*> standby_node_set;
};