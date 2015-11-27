#include "header.h"

extern "C" void sim(void)
{
	init_rack();
	init_node();
	// setup();
	zipf();
	gen_file();
}