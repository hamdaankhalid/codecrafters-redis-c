#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/select.h>

#define PORT 6379
#define SIMPLE_STR '+'
#define ARRAYS '*'
#define ERROR '-'

// Hashmap macros
#define MAPSIZE 500
#define BASE (256)
#define MAX_STRING_SIZE (128)
#define MAX_BITS (BITS_PER_CHAR * MAX_STRING_SIZE)

// const response messages
const char* error_message = "-Error message\r\n";
const char* pong = "+PONG\r\n";
const char* ok_response = "+OK\r\n";
const char* key_not_found_response = "+(nil)\r\n";

// ------------ Hashmap used to store key val ----------------
struct keyval
{
	char* key;
	char* value;
};

struct keyval* hashmap[MAPSIZE] = { NULL };

int hashkey (const char* word){
	unsigned int hash = 0;
	for (int i = 0 ; word[i] != '\0' ; i++)
	{
			hash = 31*hash + word[i];
	}
	return hash % MAPSIZE;
}

int set_key_val(const char* key, const char* val) {
	// hash offset from key
	int hashedkey = hashkey(key);
	printf("hashkey being inserted at internal array at idx: %d \n", hashedkey);
	if (hashmap[hashedkey] != 0) {
		return 1;
	}
	struct keyval* kv = (struct keyval*)malloc(sizeof(struct keyval));
	kv->key = (char *)malloc(strlen(key));
	kv->value = (char *)malloc(strlen(val));
	memcpy(kv->key, key, strlen(key));
	memcpy(kv->value, val, strlen(val));
	hashmap[hashedkey] = kv;
	return 0;
}

char* get_value(const char* key) {
	int hashedkey = hashkey(key);
	printf("hashkey being retrieved from internal array at idx: %d \n", hashedkey);
	if (hashmap[hashedkey] == 0) {
		return NULL;
	}
	return hashmap[hashedkey]->value;
}


// ------------------------- Server utils ----------------------

int size_of_data(char* data, char target) {
	int i = 0;
	while(data[i] != target) {
		i++;
	}
	return i+1;
}

int get_num(char* first){
	int len = size_of_data(first, '\r');
	char res[len];
	memcpy(res, first, len+2);

	return atoi(res);
}

// move buffer till the end of an item in buffer which is represented as '\n'
void move_buffer_till_next(char** buf) {
	int i = 0;
	while ((*buf)[i] != '\n') {
		i++;
	}
	// printf("buffer moved by count: %d\n", i+1);
	*buf += i+1;
}

// -----------Command Handlers---------------------

void handle_echo(int conn, char* buf) {
	// move past the $
	buf++;
	int next_str_size = get_num(buf);
	printf("The associated string size is of size %d \n", next_str_size);
	move_buffer_till_next(&buf);
	char echo_str[next_str_size+3]; // 1 spot for + and the 2 spots for \r\n
	echo_str[0] = '+';
	memcpy(echo_str+1, buf, next_str_size+2);
	printf("Writing back %s", echo_str);
	write(conn, echo_str, next_str_size+3);
	move_buffer_till_next(&buf);
}

void handle_set(int conn, char* buf) {
		// move past the $
		buf++;
		int next_key_size = get_num(buf);
		printf("The associated set key is of size %d \n", next_key_size);
		move_buffer_till_next(&buf);
		char key[next_key_size+2]; // 2 spots for \r\n
		memcpy(key, buf, next_key_size+2);
		move_buffer_till_next(&buf);
		// move past the $
		buf++;
		int next_val_size = get_num(buf);
		printf("The associated set val is of size %d \n", next_val_size);
		move_buffer_till_next(&buf);
		char val[next_val_size+2]; // 2 spots for \r\n
		memcpy(val, buf, next_val_size+2);
		printf("Saving %s, %s \n", key, val);
		char* msg = set_key_val(key, val)  == 0 ? ok_response : error_message;
		write(conn, msg, strlen(msg));
}

void handle_get(int conn, char* buf) {
	// move past the $
	buf++;
	int key_size = get_num(buf);
	printf("The associated get key is of size %d \n", key_size);
	move_buffer_till_next(&buf);
	char key[key_size+2]; // 2 spots for \r\n
	memcpy(key, buf, key_size+2);
	move_buffer_till_next(&buf);
	char* write_back_value = get_value(key);
	if (write_back_value == NULL) {
		printf("key not found in store \n");
		write(conn, error_message, strlen(error_message));
		return;
	}
	int formatted_len = size_of_data(write_back_value, '\n') + 1;
	printf("Write back %s with formatted size of %d bytes \n", write_back_value, formatted_len);
	char formatted_write[formatted_len]; // +1 to insert the + sign
	formatted_write[0] = '+';
	memcpy(formatted_write+1, write_back_value, formatted_len - 1);
	printf("Formatted write back %s", formatted_write);
	write(conn, formatted_write, formatted_len);
}

//-----------------Routing commands-----------------

// Redis clients send arrays so this is where we handle them from
void handle_cmd_array(int conn, char* buf) {
	// move past the *
	buf++;
	int num_elements = get_num(buf);
	printf("NUM ELEMENTS: %d \n", num_elements);
	// move to where the instruction starts
	move_buffer_till_next(&buf);
	printf("BUFFER AFTER MOVE: %s", buf);
	int elems_read = 0;

	while (elems_read < num_elements) {
			// parse the instruction
			// move past the $
			buf++;
			int str_size = get_num(buf);
			printf("%d\n", str_size);
			// move buffer till we reach start of where instruction is
			move_buffer_till_next(&buf);
			char instruction[str_size];
			memcpy(instruction, buf, str_size);
			move_buffer_till_next(&buf);
			printf("INSTRUCTION: %s \n", instruction);

			if (str_size == 4 && strncasecmp(instruction, "ECHO", 4) == 0) {
				// then the next cmd will be the cmd to echo back!
				printf("An echo command has been recieved! \n");
				handle_echo(conn, buf);
				elems_read+=2;
			} else if (str_size == 3 && strncasecmp(instruction, "SET", 3) == 0) {
				printf("A set command has been recieved! \n");
				handle_set(conn, buf);
				elems_read += 3;
			} else if (str_size == 3 && strncasecmp(instruction, "GET", 3) == 0) {
				printf("A get command has been recieved! \n");
				handle_get(conn, buf);
				elems_read += 2;
			} else if (str_size == 4 && strncasecmp(instruction, "PING", 4) == 0) {
				printf("A ping command has been recieved!");
				write(conn, pong, strlen(pong));
				elems_read += 1;
			} else {
				printf("instruction not recognized %s \n", instruction);
				write(conn, error_message, strlen(error_message));
				// skip over rest of the elements
				elems_read += num_elements;
			}
	}
}

void route(int conn, char* buf, int bufsize) {
	char firstchar = buf[0];
	switch (firstchar)
	{
	case SIMPLE_STR:
		write(conn, pong, strlen(pong));
		break;
	case ARRAYS:
		handle_cmd_array(conn, buf);
		break;
	default:
		write(conn, error_message, strlen(error_message));
		break;
	}
}

//---------- Event loop and Event loop helpers----------------
void handle_connection(int conn, fd_set *__restrict current_sockets)
{
	char buf[1024] = {0};
	if (recv(conn, buf, 1024, 0) > 0)
	{
		// if we can recieve data, then we should parse it and route it to the right handler
		route(conn, &buf, 1024);
		return;
	}

	FD_CLR(conn, current_sockets);
}

// IO Multiplexing (One thread listens to multiple sockets!)
void run_multiplex(int server_socket) {
	// Initialize current file descriptor set and add server socket into our fdset
	fd_set current_sockets, ready_sockets;
	FD_ZERO(&current_sockets);
	FD_SET(server_socket, &current_sockets);

	while (1) {
		// make a copy because select is destructive
		ready_sockets = current_sockets;

		// select() returns the number of ready descriptors that are contained in the descriptor sets, or -1 if an error occurred.
		int numready = select(FD_SETSIZE, &ready_sockets, NULL, NULL, NULL);

		if (numready < 0)
		{
			perror("Select error");
			exit(1);
		}

		if (numready == 0)
		{
			continue;
		}

		// Iterate over entire fdset, if the socket is ready.
		// Ready socket is actually an incoming message on an existing client socket.
		for (int i = 0; i < FD_SETSIZE; i++)
		{
			if (!FD_ISSET(i, &ready_sockets))
			{
				continue;
			}

			if (i == server_socket)
			{
				int new_client_socket = accept(server_socket, NULL, NULL);
				if (new_client_socket < 0)
				{
					perror("Error accepting new client connection");
					continue;
				}
				// printf("New connection accepted. \n");
				FD_SET(new_client_socket, &current_sockets);
			}
			else
			{
				handle_connection(i, &current_sockets);
			}
		}
	}
}

int bind_and_listen(int port) {
	int server_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (server_fd == -1)
	{
		printf("Socket creation failed: %s...\n", strerror(errno));
		return -1;
	}

	// Since the tester restarts your program quite often, setting REUSE_PORT
	// ensures that we don't run into 'Address already in use' errors
	int reuse = 1;
	if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof(reuse)) < 0)
	{
		printf("SO_REUSEPORT failed: %s \n", strerror(errno));
		return -1;
	}

	struct sockaddr_in serv_addr = {
			.sin_family = AF_INET,
			.sin_port = htons(port),
			.sin_addr = {htonl(INADDR_ANY)},
	};
	if (bind(server_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) != 0)
	{
		printf("Bind failed: %s \n", strerror(errno));
		return -1;
	}

	int connection_backlog = 30;
	if (listen(server_fd, connection_backlog) != 0)
	{
		printf("Listen failed: %s \n", strerror(errno));
		return -1;
	}

	return server_fd;
}

int main()
{
	// Disable output buffering
	setbuf(stdout, NULL);

	int server_socket = bind_and_listen(PORT);
	if (server_socket < 0)
	{
		return 1;
	}

	run_multiplex(server_socket);

	close(server_socket);

	return 0;
}
