all : make_my_files

make_my_files : peer_utils.o peer

peer_utils : peer_utils.c
	gcc peer_utils.c -c -o peer_utils.o

peer : peer.c
	gcc peer.c peer_utils.o -o peer
