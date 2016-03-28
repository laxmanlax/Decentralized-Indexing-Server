#include "peer_utils.h"

#define EOK 0

/* FD's for primary and secondary replica's
 * First column will have primary's fd and
 * second column will have secondary's fd
 */
int server_fds[NUM_SERVERS][1][2];

/* Lock for all hash operations */
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

/* Populate the server data from the config file */
int populate_servers(char *servers[NUM_SERVERS][2]) 
{
	FILE *fp;
	char *temp = NULL, line[80];
	int i=0;
	fp = fopen("server_config", "r");
	if (fp < 0) {
		printf("%s %d %s\n", __FILE__, __LINE__, "Could not populate the servers");
		return -1;
	}
	while (fgets(line, 80, fp) != NULL && i < NUM_SERVERS) {
		temp = strchr(line, ' ');
		servers[i][0] = calloc(1, strlen(line)-strlen(temp)+1);
		servers[i][1] = calloc(1, strlen(temp));
		if (!servers[i][0] || !servers[i][1]) {
			printf("%s %d %s\n", __FILE__, __LINE__,"Could not allocate memory for populating servers");
			fclose(fp);
			return -1;
		}
		strncpy(servers[i][0], line, strlen(line) - strlen(temp));
		if (!servers[i][0]) {
			printf("%s %d %s\n", __FILE__, __LINE__,"Could not allocate memory for populating server ip");
			fclose(fp);
			return -1;
		}
		strncpy(servers[i][1], temp+1, strlen(temp));
		/* Just in case strncpy fails */
		if (!servers[i][1]) {
			printf("%s %d %s", __FILE__, __LINE__,"Could not allocate memory for populating server port\n");
			fclose(fp);
			return -1;
		}
		i++;
	}
	fclose(fp);
	return 0;
}

/* Internal function to insert the entry to hash table */
static void insert_hash_entry (char *hash_table[12500][2], char *key, char *value, char *replica)
{
	/* Calculate the index at which entry should go */
	int hash = server_compute_hash(key);
	/* Take a lock on the hash map */
	pthread_mutex_lock(&lock);
	/* Copy the value */
	char *temp = calloc(1, sizeof(1024));
	sprintf(temp, "%s", hash_table[hash][0]);
	if (hash_table[hash][0] == 0) {
		/* Save the id of the server registering the file */
		sprintf(temp, "%s", value);
		hash_table[hash][0] = temp;
		if (replica != NULL) {
			/* Save the id of the replica server */
			sprintf(temp, "%s", hash_table[hash][0]);
			strcat(temp, ",");
			strcat(temp, replica);
			hash_table[hash][0] = temp;
		}
	} else { 
		/* Take a copy of previous peers */
		sprintf(temp, "%s", hash_table[hash][0]);
		if (hash_table[hash][0]) {
			free(hash_table[hash][0]);
		}
		if (strstr(temp, value) == NULL) {
			/* Save the id of the server if not known yet */
			strcat(temp, ",");
			strcat(temp,  value);
		} else if (replica != NULL && strstr(temp, replica) == NULL) {
			/* Save the id of replica server if not known yet */
			strcat(temp, ",");
			strcat(temp, replica);
		}
		hash_table[hash][0] = temp;
	}
	if (replica != NULL)	 {
		hash_table[hash][1] = replica;
	} 
	/* Release the lock */
	pthread_mutex_unlock(&lock);
	printf("Successfully inserted key %s value %s\n", key, value);
}

/* Internal function to get the hash entry */
static char* get_hash_entry (char *hash_table[12500][2], char *key)
{
	int hash = server_compute_hash(key);
	return hash_table[hash][0];
}

void *execute_oper(void *data)
{
	struct server_data *serv_data = (struct server_data *) data;
	char *msg = serv_data->msg;
	char *temp = NULL;
	char *key = NULL;
	char *value = NULL;
	int res = 0;
	FILE *fp;
	char buff[1024]= {0};
	if (strstr(serv_data->msg, "register")) {
		/* Registration request */

		temp = strchr(serv_data->msg, ':');

		/* Key will be the file name */
		key = (char *)calloc(1, strlen(msg) - strlen(temp) - 8);

		/* Value will be the peer index from 0-7 */
		value = (char *)calloc(1, strlen(temp));

		if (!key || !value) {
			send(serv_data->client_fd, "NULL", strlen("NULL"), 0);
			return 0;
		}

		strncpy(key, serv_data->msg + 9, strlen(msg) - strlen(temp) - 9);
		strncpy(value, temp+1, strlen(temp) - 1);
		if (!key || !value) {
			printf("%s %d %s", __FILE__, __LINE__,"Could not populate key/value pair\n");
			if (key) free(key);
			if (value) free(value);
			send(serv_data->client_fd, "NULL", strlen("NULL"), 0);
			return 0;
		}

		printf("Inserting key %s value %s\n", key, value);

		if (serv_data) {
			/* In case of success, send the servers to which replication should take place */
			char replica[2] = {0};
			do {
				sprintf(replica, "%d", (atoi(value) + 1) % NUM_SERVERS);
			} while (!strcmp(replica,key));

			insert_hash_entry(serv_data->hash_table, key, value, replica);

			send(serv_data->client_fd, replica, sizeof(replica), 0);

		} else {
			printf("%s %d %s", __FILE__, __LINE__,"Server data isnt initialized properly");
			send(serv_data->client_fd, "NULL", strlen("NULL"), 0);
		}
	} else if (strstr(serv_data->msg, "lookup")) {

		temp = strchr(serv_data->msg, ':');

		/* Key will be the file name */
		key = (char *)calloc(1, strlen(msg)-6);
		
		if (!key) {
			printf("%s %d %s", __FILE__, __LINE__,"Could not allocate memory for key\n");
			send(serv_data->client_fd, "NULL", strlen("NULL"), 0);
			return 0;
		}
		strncpy(key, serv_data->msg + 7, strlen(serv_data->msg) - 7);
		if (!key) {
			printf("%s %d %s", __FILE__, __LINE__,"Could not populate key\n");
			if (key) free(key);
			send(serv_data->client_fd, "NULL", strlen("NULL"), 0);
			return 0;
		}
		printf("Searching key %s\n", key);
		value = get_hash_entry(serv_data->hash_table, key);

		if (value != NULL)  {
			send(serv_data->client_fd, value, strlen(value), 0);
		} else {
			send(serv_data->client_fd, "NULL", strlen("NULL"), 0);
		}
	} else if (strstr(serv_data->msg, "obtain")) {
		int num_bytes = 0;
		temp = strchr(serv_data->msg, ':');

		/* Key is the file name */
		key = (char *)calloc(1, strlen(msg)-6);

		if (!key) {
			printf("%s %d %s", __FILE__, __LINE__,"Could not allocate memory for key\n");
			send(serv_data->client_fd, "NULL", strlen("NULL"), 0);
			return 0;
		}
		strncpy(key, serv_data->msg + 7, strlen(serv_data->msg) - 7);

		printf("Transferring file %s\n" ,key);

		fp = fopen(key, "r+");

		if(fp == NULL)
		{
			send(serv_data->client_fd, "NULL", strlen("NULL"), 0);
			return 0;
		}

		while(1)
		{
			num_bytes = fread(buff, 1, MAX_MSG_SIZE, fp);

			if(num_bytes > 0)
			{
				printf("Sending bytes %d\n", num_bytes);
				write(serv_data->client_fd, buff, num_bytes);
			}

			if (num_bytes < 1024) // Usually when there is error in reading or data is done
			{
				if (feof(fp)) { 
					printf("End of file\n");
					send(serv_data->client_fd, "Done", strlen("Done"), 0);
				}
				if (ferror(fp)) {
					printf("%s %d %s", __FILE__, __LINE__,"Error reading\n");
					send(serv_data->client_fd, "Error", strlen("Error"), 0);
				}
				break;
			}
		}
		fclose(fp);
	} else if (strstr(serv_data->msg, "replicate")) {
		int num_bytes = 0;
		char key[1024];	
		char value[1024];	
		char dir[1024];	

		temp = strtok(serv_data->msg, ":");

		temp = strtok(NULL, ",");
		sprintf(key, "%s", temp);

		temp = strtok(NULL, ",");
		sprintf(value, "%s", temp);

		temp = strtok(NULL, ",");
		sprintf(dir, "%s", temp);

			/* In case of success, send the servers to which replication should take place */
			char replica[2] = {0};
			do {
				sprintf(replica, "%d", (atoi(value) + 1) % NUM_SERVERS);
			} while (!strcmp(replica,key));

		/* Insert this in the metadata */
		insert_hash_entry(serv_data->hash_table, key, value, replica);

		/* Plus copy the file in my directory */
		char cp_str[1024];
		sprintf(cp_str, "%s %s/%s %s", "cp", dir, key, ".");

		printf("Executing command %s\n", cp_str);
		system(cp_str);
		send(serv_data->client_fd, "Done", strlen("Done"), 0);
	} else {
		printf("%s %d %s", __FILE__, __LINE__,"Undefined operation\n");
		send(serv_data->client_fd, "Undefined", strlen("Undefined"), 0);
	}
	return NULL;
}

/* Find the server to which request should be sent */
int get_hashing_server (char *key) {
	int hash = 0;
	char *temp = strdup(key);
	while (*temp != '\0') {
		hash = hash + *temp;
		temp++;
	}
	temp = NULL;
	return hash % NUM_SERVERS;
}

char* get_internal (char *key, char *value, char *servers[NUM_SERVERS][2]) ;

char* get (char *key, char *value, char *servers[NUM_SERVERS][2]) {

	char buffer_send[MAX_MSG_SIZE] = {0};
	char buffer_recv[MAX_MSG_SIZE] = {0};
	char *result = NULL;
	char *file_name = strtok(key, ",");
	int server_num, flag;
	struct sockaddr_in serv_addr;
	socklen_t server_addr_size;
	result = get_internal(key, value, servers);
	if (result == NULL) {
		printf("Looking up information from secondary server\n");
		server_num = (atoi(value) + 1) % NUM_SERVERS;
		printf ("Sending lookup request to %s:%s\n", servers[server_num][0], 
				servers[server_num][1]);

		if (server_fds[server_num][0][1] <= 0) {
			server_fds[server_num][0][1] = socket(PF_INET, SOCK_STREAM, 0);
			if (server_fds[server_num][0][1] < 0) {
				printf("%s %d %s", __FILE__, __LINE__,"Could not create the socket\n");
				return NULL;
			}
			serv_addr.sin_family = AF_INET;
			serv_addr.sin_port = htons(atoi(servers[server_num][1]));
			serv_addr.sin_addr.s_addr = inet_addr(servers[server_num][0]);
			memset(serv_addr.sin_zero, '\0', sizeof(serv_addr.sin_zero));

			flag = connect(server_fds[server_num][0][1], (struct sockaddr *)&serv_addr, sizeof(serv_addr));

			if (flag == -1) {
				printf("%s %d %s", __FILE__, __LINE__,"Couldnt bind to the socket 1\n");
				return NULL;
			}
		}
		
		sprintf(buffer_send, "lookup,%s", key);
		printf("Sending lookup request %s to replica\n", buffer_send);
		flag = send(server_fds[server_num][0][1], buffer_send, MAX_MSG_SIZE, 0);
		if (flag < 0) {
			printf("%s %d %s", __FILE__, __LINE__,"Couldn't send message to secondary server\n");
			return NULL;
		}
		flag = recv(server_fds[server_num][0][1], buffer_recv, MAX_MSG_SIZE, 0);
		if (flag < 0) {
			printf("%s %d %s", __FILE__, __LINE__,"Couldnt receive message from secondary server \n");
			return NULL;
		} else {

                result = calloc(1, sizeof(buffer_recv));
                strcpy(result, buffer_recv);
                return result;
		}
			
	} else {
		return result;
	}
	return NULL;
}

/* Function to get the value for a key from server */
char* get_internal (char *key, char *value, char *servers[NUM_SERVERS][2]) 
{ 
	int flag = -1, server_num = -1, server_fd = -1;
        struct sockaddr_in serv_addr;
        socklen_t server_addr_size;
        char buffer_send[MAX_MSG_SIZE] = {0};
        char buffer_recv[MAX_MSG_SIZE] = {0};
        char *result = NULL;

        server_num = get_hashing_server (key) % NUM_SERVERS;

	printf ("Looking up Socket info for Server %d\n", server_num);
	printf ("Sending lookup request to %s:%s\n", servers[server_num][0], 
			servers[server_num][1]);

        /* Create a socket connection with primary server if none exists */
        if (server_fds[server_num][0][0] <= 0) {
                /* Bind to the correct server and send a put request */
                server_fds[server_num][0][0] = socket(PF_INET, SOCK_STREAM, 0);
                if (server_fds[server_num][0][0] < 0) {
                        printf("%s %d %s", __FILE__, __LINE__,"Could not create the socket\n");
                        return NULL;
                }
                serv_addr.sin_family = AF_INET;
                serv_addr.sin_port = htons(atoi(servers[server_num][1]));
                serv_addr.sin_addr.s_addr = inet_addr(servers[server_num][0]);
                memset(serv_addr.sin_zero, '\0', sizeof(serv_addr.sin_zero));

                flag = connect(server_fds[server_num][0][0], (struct sockaddr *)&serv_addr, sizeof(serv_addr));

                if (flag == -1) {
			server_fds[server_num][0][0] = -1;
                        printf("%s %d %s", __FILE__, __LINE__,"Couldnt bind to the socket\n");
                        return NULL;
                }
        }

	sprintf(buffer_send, "lookup,%s", key);
	printf("sending request %s to %d", buffer_send, server_fds[server_num][0][0]);
	flag = send(server_fds[server_num][0][0], buffer_send, MAX_MSG_SIZE, 0);
	if (flag < 0) {
		printf("%s %d Couldnt send message %s to server", __FILE__, __LINE__, buffer_send);
		return NULL;
	}

	flag = recv(server_fds[server_num][0][0], buffer_recv, MAX_MSG_SIZE, 0);
	if (flag < 0) {
		printf("%s %d %s", __FILE__, __LINE__,"Couldnt receive message from server \n");
		return NULL;
	} else {
		result = calloc(1, sizeof(buffer_recv));
		strcpy(result, buffer_recv);
		return result;
	}
	return NULL;

}

/* Function to get file from a server */
char *obtain(int server_num, char *file_name, char *servers[NUM_SERVERS][2]) {

	int flag = -1, server_fd = -1;
	struct sockaddr_in serv_addr;
	socklen_t server_addr_size;
	FILE *fp;
	int n_bytes;
	char *buffer_recv = malloc(MAX_MSG_SIZE);
	char *buffer_send = malloc(MAX_MSG_SIZE);
	char *result = NULL;

	sprintf(buffer_send, "obtain:%s", file_name);

	printf ("Sending obtain request to %s:%s\n", servers[server_num][0], 
			servers[server_num][1]);
	if (server_fds[server_num][0][1] <= 0) {
		server_fds[server_num][0][1] = socket(PF_INET, SOCK_STREAM, 0);
		if (server_fds[server_num][0][1] < 0) {
			printf("%s %d %s", __FILE__, __LINE__,"Could not create the socket\n");
			return NULL;
		}
		serv_addr.sin_family = AF_INET;
		serv_addr.sin_port = htons(atoi(servers[server_num][1]));
		serv_addr.sin_addr.s_addr = inet_addr(servers[server_num][0]);
		memset(serv_addr.sin_zero, '\0', sizeof(serv_addr.sin_zero));

		flag = connect(server_fds[server_num][0][1], (struct sockaddr *)&serv_addr, sizeof(serv_addr));

		if (flag == -1) {
			printf("%s %d %s", __FILE__, __LINE__,"Couldnt bind to the socket\n");
			return NULL;
		}
	}
	printf("Sending request %s to %s", buffer_send, servers[server_num][1]);

	send(server_fds[server_num][0][1], buffer_send, strlen(buffer_send), 0);

	fp = fopen(file_name, "w+");
	if (fp == NULL)
	{
		printf("%s %d %s", __FILE__, __LINE__,"Could not open the file\n");
		return NULL;
	}

	while((n_bytes = read(server_fds[server_num][0][1], buffer_recv, 1024)) > 0)
	{
		printf("%s %d Bytes received %s %d\n", __FILE__, __LINE__ , buffer_recv, n_bytes);
		fprintf(fp, "%s", buffer_recv);
		break;
	}

	fclose(fp);

	if(n_bytes < 0 || buffer_recv == NULL)
	{
		printf("%s %d %s", __FILE__, __LINE__,"\n Nothing received or is over \n");
	}

	result = calloc(1, sizeof(buffer_recv));
	strcpy(result, buffer_recv);
	return result;
}

char *del (char *key,  char *servers[NUM_SERVERS][2]) 
{ 
	int flag = -1, server_num = -1, server_fd = -1;
	struct sockaddr_in serv_addr;
	socklen_t server_addr_size;
	char buffer_send[MAX_MSG_SIZE] = {0};
	char buffer_recv[MAX_MSG_SIZE] = {0};
	char *result = NULL;

	server_num = get_hashing_server (key) % NUM_SERVERS;

	printf ("Looking up Socket info for Server %d\n", server_num);
	printf ("Sending delete request to %s:%s\n", servers[server_num][0], 
			servers[server_num][1]);

	if (server_fds[server_num][0][0] <= 0) {
		server_fds[server_num][0][0] = socket(PF_INET, SOCK_STREAM, 0);
		if (server_fds[server_num][0][0] < 0) {
			printf("%s %d %s", __FILE__, __LINE__,"Could not create the socket\n");
			goto del_sec;
		}
		serv_addr.sin_family = AF_INET;
		serv_addr.sin_port = htons(atoi(servers[server_num][1]));
		serv_addr.sin_addr.s_addr = inet_addr(servers[server_num][0]);
		memset(serv_addr.sin_zero, '\0', sizeof(serv_addr.sin_zero));

		flag = connect(server_fds[server_num][0][0], (struct sockaddr *)&serv_addr, sizeof(serv_addr));

		if (flag == -1) {
			printf("%s %d %s", __FILE__, __LINE__,"Couldnt bind to the socket\n");
			goto del_sec;
		}
	}

	sprintf(buffer_send, "del,%s", key);
	flag = send(server_fds[server_num][0][0], buffer_send, MAX_MSG_SIZE, 0);

	if (flag < 0) {
		printf("%s %d Couldnt send message %s to server", __FILE__, __LINE__, buffer_send);
		goto del_sec;
	}

	flag = recv(server_fds[server_num][0][0], buffer_recv, MAX_MSG_SIZE, 0);
	if (flag < 0) {
		printf("%s %d %s", __FILE__, __LINE__,"Couldnt receive message from server \n");
		goto del_sec;
	}
del_sec:
	if (server_fds[server_num][0][1] <= 0) {
		server_fds[server_num][0][1] = socket(PF_INET, SOCK_STREAM, 0);
		if (server_fds[server_num][0][1] < 0) {
			printf("%s %d %s", __FILE__, __LINE__,"Could not create the socket\n");
			return NULL;
		}
		serv_addr.sin_family = AF_INET;
		serv_addr.sin_port = htons(atoi(servers[server_num][1]));
		serv_addr.sin_addr.s_addr = inet_addr(servers[server_num][0]);
		memset(serv_addr.sin_zero, '\0', sizeof(serv_addr.sin_zero));

		flag = connect(server_fds[server_num][0][1], (struct sockaddr *)&serv_addr, sizeof(serv_addr));

		if (flag == -1) {
			printf("%s %d %s", __FILE__, __LINE__,"Couldnt bind to the socket \n");
			return NULL;
		}
	}
	printf("%s %d %s", __FILE__, __LINE__,"Sending delete request to replica \n");
	flag = send(server_fds[server_num][0][1], buffer_send, MAX_MSG_SIZE, 0);

	if (flag < 0) {
		printf("%s %d Couldnt send message %s to server", __FILE__, __LINE__, buffer_send);
		return NULL;
	}

	flag = recv(server_fds[server_num][0][1], buffer_recv, MAX_MSG_SIZE, 0);
	if (flag < 0) {
		printf("%s %d %s", __FILE__, __LINE__,"Couldnt receive message from replica server \n");
		return NULL;
	}
	result = strdup(buffer_recv);
	return result;
}

char *put_internal (char *key ,char *value, char *servers[NUM_SERVERS][2]);

char *put (char *key ,char *value, char *dir, char *servers[NUM_SERVERS][2]) {
	char buffer_send[MAX_MSG_SIZE] = {0};
	char buffer_recv[MAX_MSG_SIZE] = {0};
	char *result = NULL;
	char *file_name = strtok(key, ",");
	int server_num, flag;
	struct sockaddr_in serv_addr;
	socklen_t server_addr_size;
	while (file_name != NULL) {
		result = put_internal(file_name, value, servers);
		if (result == NULL) {
			printf("%s %d %s", __FILE__, __LINE__,"Primary is down.. Contacting Secondary");
			server_num = (atoi(value) + 1) % NUM_SERVERS;
		} else {
			server_num =  atoi(result);
		}

		printf ("Sending put request to replica server %s:%s\n", servers[server_num][0], 
				servers[server_num][1]);

		/* Create a socket connection with secondary server if none exists */
		if (server_fds[server_num][0][1] <= 0) {
			server_fds[server_num][0][1] = socket(PF_INET, SOCK_STREAM, 0);
			serv_addr.sin_family = AF_INET;
			serv_addr.sin_port = htons(atoi(servers[server_num][1]));
			serv_addr.sin_addr.s_addr = inet_addr(servers[server_num][0]);
			memset(serv_addr.sin_zero, '\0', sizeof(serv_addr.sin_zero));

			flag = connect(server_fds[server_num][0][1], (struct sockaddr *)&serv_addr, sizeof(serv_addr));

			if (flag == -1) {
				printf("%s %d %s", __FILE__, __LINE__,"Couldnt bind to the socket\n");
				return NULL;
			}
		}

		/* Put returned servers for replication */
		/* Send file to them now */
		sprintf(buffer_send, "replicate:%s,%s,%s", file_name, value, dir);
		flag = send(server_fds[server_num][0][1], buffer_send, MAX_MSG_SIZE, 0);

		if (flag < 0) {
		printf("%s %d Couldnt send message %s to server", __FILE__, __LINE__, buffer_send);
			return NULL;
		}

		flag = recv(server_fds[server_num][0][1], buffer_recv, MAX_MSG_SIZE, 0);
		if (flag < 0) {
			printf("%s %d %s", __FILE__, __LINE__,"Couldnt receive message from server \n");
			return NULL;
		}
		printf("%s %d Replication result %s", __FILE__, __LINE__, buffer_recv);
		file_name = strtok(NULL, " ");
	}
	return result;

}	
char *put_internal (char *key ,char *value, char *servers[NUM_SERVERS][2]) 
{ 
	int flag = -1, server_num = -1, server_fd = -1;
	struct sockaddr_in serv_addr;
	socklen_t server_addr_size;
	char buffer_send[MAX_MSG_SIZE] = {0};
	char buffer_recv[MAX_MSG_SIZE] = {0};
	char *result = NULL;

	server_num = get_hashing_server (key) % NUM_SERVERS;

	printf ("Looking up Socket info for Server %d\n", server_num);
	printf ("Sending put request to %s:%s\n", servers[server_num][0], 
			servers[server_num][1]);

	/* Create a socket connection with primary server if none exists */
	if (server_fds[server_num][0][0] <= 0) {
		/* Bind to the correct server and send a put request */
		server_fds[server_num][0][0] = socket(PF_INET, SOCK_STREAM, 0);
		if (server_fds[server_num][0][0] < 0) {
			printf("%s %d %s", __FILE__, __LINE__,"Could not create the socket\n");
			goto put_sec;
		}
		serv_addr.sin_family = AF_INET;
		serv_addr.sin_port = htons(atoi(servers[server_num][1]));
		serv_addr.sin_addr.s_addr = inet_addr(servers[server_num][0]);
		memset(serv_addr.sin_zero, '\0', sizeof(serv_addr.sin_zero));

		flag = connect(server_fds[server_num][0][0], (struct sockaddr *)&serv_addr, sizeof(serv_addr));

		if (flag == -1) {
	        	printf("%s %d %s", __FILE__, __LINE__,"Couldnt bind to the socket\n");
			server_fds[server_num][0][0] = -1;
			return NULL;
		}
	}
	sprintf(buffer_send, "register,%s:%s", key, value);
	printf("sending request %s to %d", buffer_send, server_fds[server_num][0][0]);
	flag = send(server_fds[server_num][0][0], buffer_send, MAX_MSG_SIZE, 0);

	if (flag < 0) {
		printf("%s %d Couldnt send message %s to server", __FILE__, __LINE__, buffer_send);
		return NULL;
	}

	flag = recv(server_fds[server_num][0][0], buffer_recv, MAX_MSG_SIZE, 0);
	if (flag < 0) {
		printf("%s %d %s", __FILE__, __LINE__,"Couldnt receive message from server \n");
		return NULL;
	}

put_sec:

	result = calloc(1, sizeof(buffer_recv));
	strcpy(result, buffer_recv);
	return result;
}

int server_compute_hash(char *key) {
	int hash = 0;
	char *temp = strdup(key);
	while (*temp != '\0') {
		hash = hash + *temp;
		temp++;
	}
	temp = NULL;
	return hash % MAX_HASH_ENTRIES;	
}

/* Start a server for myself */
void *server (char *ip, char *port, char *hash_table[12500][2]) {
	int server_fd, client_fd, flag, maxfd, i;
	char buff[MAX_MSG_SIZE];
	struct sockaddr_in serv_addr;
	struct sockaddr_storage server_str;
	socklen_t server_addr_size;
	fd_set readset;

	server_fd = socket(PF_INET, SOCK_STREAM, 0);
	if (server_fd < 0) {
		printf("%s %d %s", __FILE__, __LINE__,"Couldnt create the socket \n");
		goto exit;
	}
	serv_addr.sin_family = AF_INET;
	printf("Binding at ip %s port %d\n", ip, atoi(port));
	serv_addr.sin_port = htons(atoi(port));
	serv_addr.sin_addr.s_addr = inet_addr(ip);
	memset(serv_addr.sin_zero, '\0', sizeof(serv_addr.sin_zero));

	flag = bind(server_fd, (struct sockaddr *) &serv_addr, sizeof(serv_addr));
	if (flag == -1) {
		printf("%s %d %s", __FILE__, __LINE__,"Couldnt bind to the socket\n");
		goto exit;
	}
	printf("Listening\n");
	flag = listen(server_fd, MAX_CONN_LISTEN);

	if (flag != 0) {
		printf("%s %d %s", __FILE__, __LINE__,"Error listening to the connection\n");
		goto exit;
	}
	server_addr_size = sizeof(server_str);

	/* Initialize the readset to monitor this server_fd */
	FD_ZERO(&readset);
	FD_SET(server_fd, &readset);
	maxfd = server_fd;
	int ret = 0;
	fd_set tempset;
	do {
		memcpy(&tempset, &readset, sizeof(tempset));
		/* Run select call for messages for this fd */
		ret = select(maxfd + 1, &tempset, NULL, NULL, NULL);
		if (ret < 0) {
			printf("%s %d %s", __FILE__, __LINE__,"Error in select()\n");
		} else if (ret > 0) {
			/* We have a valid message, set this in server's readset */
			if (FD_ISSET(server_fd, &tempset)) {
				printf("Accepting connection \n");
				client_fd = accept(server_fd, (struct sockaddr *) &server_str, &server_addr_size);
				if (client_fd < 0) {
					printf("%s %d %s", __FILE__, __LINE__,"Error in accept(): \n");
				} else {
					/* Set the client_fd in readset */
					printf("Setting client_fd %d", client_fd);
					FD_SET(client_fd, &readset);
					if (client_fd > maxfd) {
						maxfd = client_fd;
					}
				}
			}

			/* Iterate over all fd's in the readset */
			for (i=0; i<maxfd+1; i++) {
				/* If we have a message for i'th client in the read set, read it */
				if (FD_ISSET(i, &tempset)) {
					pthread_t thread;
					ret = recv (i, buff, MAX_MSG_SIZE, 0);
					if (ret > 0 ) {
						printf("Received message new from client %s %d\n", buff, ret);
						/* Initialise the structure to spawn this request as a thread */
						struct server_data *data = (struct server_data *)calloc(1, sizeof(struct server_data));
						data->msg = (char *)calloc(1, ret);
						strncpy(data->msg, buff, ret);
						data->hash_table = hash_table;
						data->client_fd = client_fd;

						/* Create a thread for this operation */
						pthread_create (&thread, NULL, execute_oper, (void *)data); 
						printf("Thread done.. join\n");
						pthread_join(thread, NULL);
					} else {
						close(i);
						FD_CLR(i, &readset);
					}
				}
			}

		}
	} while(1);

exit:
	/* Destroy any locks for this server */
	pthread_mutex_destroy(&lock);

	return 0;
}

