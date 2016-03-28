1.1	Decentralized Indexing Server   

Aim of this project is to build a decentralized system to index and replicate files. We would be designing this system over 8 peers who would act as a server as a client.
1.1.1	Bootstrap

When the peer starts, it will read a global configuration file to read the IP: PORT combination where it should listen as a server.

We have defined the following config file to bootstrap the network

server_config: 

Configuration file containing IP: PORT for the servers.
Example in the following file, IP: PORT of first server would be 127.0.0.1 1231:
		
nikatari$ cat server_config 
127.0.0.1 1231
127.0.0.1 1232
127.0.0.1 1233
127.0.0.1 1234
127.0.0.1 1235
127.0.0.1 1236
127.0.0.1 1237
127.0.0.1 1238
127.0.0.1 1231

This config file is used whenever we start the peer:

./peer <peer_id> : For peer id “i”, “(i+1)”th entry will be read from the file. 

Say, If peer id is 0, first entry from server_config will be read from the file and the respective ip and port will be used for initializing the server.

1.1.2	Server thread

This thread will maintain a hash table of 1M entries. The hash table for this assignment is a simple array of 1M. 

•	Server will run a select() linux call to listen to the current open client fd’s.
•	Clients will establish socket connection to a peer server will be established only once i.e.
during the first request. After that, the socket will be kept alive till the time peer/server shuts
down.

1.1.3	Distributed hash table

Every peer will maintain a hash table of 12500 entries in this code.

Key to the distributed hashtables will be the file name and value will be the server’s that have the file (This will include the registering server’s id as well as replica’s id):

1.1.4	Client thread 

This thread will present the users with following options:
1.	*** Enter 1 for inserting an entry ***
2.	*** Enter 2 for searching peers ***
3.	*** Enter 3 for obtaining an entry ***
4.	*** Enter 4 to exit ***
Along with sending the request to the primary server, these API’s would send a request to the replica as well to insert, search and obtaining the entry. 

While retrieving, primary server is down, secondary server would be contacted. An error would be returned if the secondary server could not be reached as well. 

1.2	Hashing Functions

Client will call insert/search/obtain API’s. These API’s will internally calculate the server to whom which the request should be sent. This is done via the following API :
int server_compute_hash (char *key) {
int hash = 0;
        char *temp = strdup(key);
        while (*temp != '\0') {
                hash = hash + *temp;
                temp++;
        }
        temp = NULL;
        return hash % NUM_SERVERS;
}

This function will calculate the sum of the ASCII equivalent of all the characters and then mod it with the number of servers. The final result shall then be the server to which the request should be sent.

