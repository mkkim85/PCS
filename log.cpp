#include "header.h"

void logging(char str[])
{
	FILE *f = fopen("PCS-log.txt", "a");
	fprintf(f, str);
	fclose(f);
}