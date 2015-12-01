#include "header.h"

double ZIPF_PROB[CS_RACK_NUM];

extern double SETUP_DATA_SKEW;

void zipf(void)
{
	long i;
	double sum = 0;

	for (i = 1; i <= CS_RACK_NUM; ++i)
	{
		sum += 1 / pow(i, SETUP_DATA_SKEW);
	}

	ZIPF_PROB[0] = 1 / pow(1, SETUP_DATA_SKEW) / sum;
	for (i = 2; i <= CS_RACK_NUM; ++i)
	{
		ZIPF_PROB[i - 1] = 1 / pow(i, SETUP_DATA_SKEW) / sum + ZIPF_PROB[i - 2];
	}
}

long rand_zipf(void)
{
	long i;
	double p = prob();

	for (i = 0; i < CS_RACK_NUM; ++i)
	{
		if (p < ZIPF_PROB[i]) {
			return i;
		}
	}
	return CS_RACK_NUM;
}