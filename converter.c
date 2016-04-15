#include <stdio.h>
#include <time.h>

int days_in_month[12]={31,28,31,30,31,30,31,31,30,31,30,31};
#define MAX_LINE 128
#define SHIFT 6

static void shift_by_hours ( int *year, int *month, int *day, int *hour )
{
    *hour = *hour + SHIFT;
    if ( *hour >= 24 ){
	(*hour) -=24;
	(*day)++;
	if ( *day > days_in_month[*month-1] ) {
	    *day = 1;
	    (*month)++;
	    if ( *month > 12 ) {
		*month = 1;
		(*year)++;
	    }
	}
    }
    return;
}

int main( int argc, char ** argv)
{
    char tmp[11], tepoch[11];
    char newstr[13];
    struct tm ts;
    char buf[80];
    FILE *fhin, *fhout;
    char line[MAX_LINE];
    char tempstring[MAX_LINE];

    if (argc < 3 ) {
	printf("Usage: converter <infilename> <outfilename>\n");
	return -1;
    }

    fhin = fopen ( argv[1], "r" );
    fhout = fopen (argv[2], "w" );

    long tld;
    fscanf (fhin, "%[^\n]\n",line) ;
    while ( fscanf (fhin, "%11c %[^\n]\n", tmp, line ) != EOF ) {
	sprintf (tepoch, "%.10s", tmp);
	sscanf (tepoch, "%ld", &tld );
	ts = *localtime ((time_t *)&tld);
	int year = 1900+ts.tm_year;
	int month = 1+ts.tm_mon;
	int day = ts.tm_mday;
	int hour = ts.tm_hour;
	shift_by_hours ( &year, &month, &day, &hour );
	sprintf(newstr, "%4d,%02d,%02d,%02d,%02d", year, month, day, hour, ts.tm_min );
	fprintf(fhout,"%s,%s\n",newstr,line);	
    }

    fclose ( fhin );
    fclose ( fhout);

    return 0;
}
