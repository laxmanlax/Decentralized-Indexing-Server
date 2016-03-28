#include "peer_utils.h"
#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#include <mach/clock.h>
#include <mach/mach.h>

char *hash_table[MAX_HASH_ENTRIES][2];
char *servers[NUM_SERVERS][2];
void *start_server (void *data) {
	struct client_data *params = (struct client_data *)data;
	server(params->ip, params->port, params->hash_table);
	return 0;
}

void *start_client (void *data) {
	int a;
	char key[20];
	char *value = calloc(1, 1204);
	char *dir = calloc(1, 1204);

	while (1) {

		printf("*** A simple DHT for you ***\n");
		printf("*** Select one operation ***\n");
		printf("*** Enter 1 for inserting an entry ***\n");
		printf("*** Enter 2 for searching peers ***\n");
		printf("*** Enter 3 for obtaining an entry ***\n");
		printf("*** Enter 4 to exit ***\n");
		scanf("%d", &a);	
		switch (a) {
			case 1: 
				printf("Enter the file name\n");
				scanf("%s", key);	
				printf("Enter your index [1-8]\n");
				scanf("%s", value);	
				printf("Enter the file directory\n");
				scanf("%s", dir);	
				put (key, value, dir, servers);	
				break;
			case 2: 
				printf("Enter the file name\n");
				scanf("%s", key);	
				printf("Enter your index [1-8]\n");
				scanf("%s", value);	
				value = get (key, value, servers);
				if (value == NULL) {
					printf("ULL");
				} else {
					printf("Extracted value %s\n", value);
				}
				break;
			case 3: 
				printf("Enter the peer ID\n");
				scanf("%s", key);	
				printf("Enter the file name\n");
				scanf("%s", value);	
				printf("Obtaining file %s from %d", value, atoi(key));
				value = obtain(atoi(key), value, servers);
				break;
			case 4: goto exit;
			default: printf("No such option.. Try Again !");
				 break;
		}
	}
exit:
	printf("*** Exiting ***\n");
	return 0;
}

int main(int argc, char *argv[]) {
	pid_t  pid;
	pid = fork();
	int rc = 0;
	rc = populate_servers(servers);
	if (rc == -1) {
		printf("FATAL: Could not populate server configuration !");
		return 0;
	}
	/* Create a child processes to run as server */
	if (pid == 0) {
		if (argv[1]) {
			struct client_data *data = (struct client_data *)calloc(1, sizeof(struct client_data));
			pthread_t thread;
			data->ip = servers[atoi(argv[1])][0];
			data->port = servers[atoi(argv[1])][1];
			data->hash_table = hash_table;
			pthread_create (&thread, NULL, start_server, (void *)data);
			pthread_join(thread, NULL);	
		} else {
			printf("Did not start the server as an instance wasn't specified\n");
		}
	} else {
		pthread_t client_thread;
		pthread_create (&client_thread, NULL, start_client, NULL);
		pthread_join(client_thread, NULL);	
		kill( pid, SIGINT );
	}	
	return 0;
}
