#include "header.h"

double ZIPF_PROB[MOD_FACTOR];
long count[MOD_FACTOR] = { 0, };

extern double SETUP_DATA_SKEW;

void zipf(void)
{
	long i;
	double sum = 0;

	for (i = 1; i <= MOD_FACTOR; ++i) {
		sum += 1 / pow(i, SETUP_DATA_SKEW);
	}

	ZIPF_PROB[0] = 1 / pow(1, SETUP_DATA_SKEW) / sum;
	for (i = 2; i <= MOD_FACTOR; ++i) {
		ZIPF_PROB[i - 1] = 1 / pow(i, SETUP_DATA_SKEW) / sum + ZIPF_PROB[i - 2];
	}
}

long rand_zipf(void)
{
	long i;
	double p = prob();

	for (i = 0; i < MOD_FACTOR; ++i) {
		if (p < ZIPF_PROB[i]) {
			count[i]++;
			return i;
		}
	}
	count[MOD_FACTOR - 1]++;
	return MOD_FACTOR - 1;
}