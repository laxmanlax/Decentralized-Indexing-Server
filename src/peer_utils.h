#ifndef _PEER_UTILS_H
#define _PEER_UTILS_H
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <pthread.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <pthread.h>

#define NUM_SERVERS 8
#define MAX_MSG_SIZE 1024
#define MAX_CONN_LISTEN 10000
#define MAX_HASH_ENTRIES 12500

struct client_data {
	char *ip;
	char *port;
	char *(*hash_table)[2];
};

struct server_data {
	int client_fd;
	char *msg;
	char *(*hash_table)[2];
};

/* Populate the servers after reading from server_config */
int populate_servers (char *servers[NUM_SERVERS][2]);

/* Function used by servers to compute the hash */
int server_compute_hash (char *key);

/* Function to start a peer as a server */
void *server (char *ip, char *port, char *hash_table[12500][2]);

/* Function to insert an entry to the hash table */
char *put (char *key ,char *value, char *dir, char *servers[NUM_SERVERS][2]);

/* Function to get an entry from the hash table */
char *get (char *key, char *value, char *servers[NUM_SERVERS][2]);

/* Function to delete an entry from the hash table */
char *obtain(int server_num, char *file_name, char *servers[NUM_SERVERS][2]);

/* Function to delete an entry from the hash table */
char *del (char *key, char *servers[NUM_SERVERS][2]);
#endif
