#include "header.h"

extern facility *F_MASTER_SWITCH;
extern long SETUP_RANDOM_SEED;

void init(void)
{
	char str[50];
	long i;

	reset_prob(SETUP_RANDOM_SEED);

	i = max_processes(MAX_PROCESSES);
	i = max_facilities(MAX_FACILITIES);
	i = max_servers(MAX_SERVERS);
	i = max_mailboxes(MAX_MAILBOXES);
	i = max_messages(MAX_MESSAGES);
	i = max_events(MAX_EVENTS);

	zipf();
	init_rack();
	init_node();
	gen_file();

	sprintf(str, "mSwitch");
	F_MASTER_SWITCH = new facility(str);
	F_MASTER_SWITCH->set_servicefunc(inf_srv);

	if (LOGGING)
	{
		FILE *f = fopen("PCS-log.txt", "w");
		fclose(f);
	}
}