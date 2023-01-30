#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/select.h>
#include <stdbool.h>
#include <pthread.h>

#define PORT 6379
#define SIMPLE_STR '+'
#define ARRAYS '*'
#define ERROR '-'
#define MAX_BUFFER_SIZE 1024

// Hashmap macros
#define MAPSIZE 500
#define NOEXPIRATION -1

#define GARBAGE_COLLECT_EXPIRED 500

// response messages
char* error_message = "-Error message\r\n";
char* pong = "+PONG\r\n";
char* ok_response = "+OK\r\n";
char* key_not_found_response = "+(nil)\r\n";
char* null_bulk_string = "$-1\r\n";

// ------------ Hashmap used to store key val ----------------
struct keyvalentry
{
	char* key;
	char* value;
	time_t created_at; // duplicate this data so even get can use it :)
	int ms_to_expire; // duplicated as well
};

struct hashmap {
	pthread_mutex_t mutex;
	struct keyvalentry** data;
};

int hashkey (char* word){
	unsigned int hash = 0;
	for (int i = 0 ; word[i] != '\n' ; i++)
	{
			hash = 31*hash + word[i];
	}
	return hash;
}

int set_key_val(struct hashmap* map, char* key, char* val, int expiration) {
	// hash offset from key
	int hashedkey = hashkey(key) % MAPSIZE;
	pthread_mutex_lock(&map->mutex);
	printf("hashkey being inserted at internal array for %s at idx: %d \n", key, hashedkey);
	if (map->data[hashedkey] != NULL) {
		pthread_mutex_unlock(&map->mutex);
		return 1;
	}
	struct keyvalentry* kv = (struct keyvalentry*)malloc(sizeof(struct keyvalentry));
	kv->key = (char *)malloc(strlen(key));
	kv->value = (char *)malloc(strlen(val));
	kv->created_at = time(NULL);
	kv->ms_to_expire = expiration;
	
	memcpy(kv->key, key, strlen(key));
	memcpy(kv->value, val, strlen(val));
	map->data[hashedkey] = kv;
	pthread_mutex_unlock(&map->mutex);
	return 0;
}

struct keyvalentry* get_value(struct hashmap* map, char* key) {
	int hashedkey = hashkey(key) % MAPSIZE;
	pthread_mutex_lock(&map->mutex);
	printf("hashkey being retrieved from internal array for %s at idx: %d \n", key, hashedkey);
	if (map->data[hashedkey] == NULL) {
		pthread_mutex_unlock(&map->mutex);
		return NULL;
	}
	struct keyvalentry* res = map->data[hashedkey];
	pthread_mutex_unlock(&map->mutex);
	return res;
}

void delete_item(struct hashmap* map, char* key) {
	int hashedkey = hashkey(key) % MAPSIZE;
	pthread_mutex_lock(&map->mutex);
	struct keyvalentry* keyval = map->data[hashedkey];
	if (keyval != NULL) {
		free(keyval->key);
		free(keyval->value);
		free(keyval);
		map->data[hashedkey] = NULL;
	}
	pthread_mutex_unlock(&map->mutex);
}

// ------------------------ Background Thread Task that monitors TTL's ---------------
struct ttl_item {
	char* key;
	time_t created_at;
	int ms_to_expire;
};

struct ttl_monitor {
	pthread_mutex_t mutex;
	struct ttl_item** items_arr;
};

struct start_monitor_args {
	struct ttl_monitor* monitor;
	struct hashmap* map;
};

bool is_expired(time_t created_at, int ms_ttl) {
	time_t now = time(NULL);
	time_t	secs_since_created = now - created_at;	
	return secs_since_created*1000.0 > (double) ms_ttl;
}

// Procedure run in a single background thread
void *start_ttl_monitor(void* args) {
	struct start_monitor_args* argsCast = (struct start_monitor_args*) args;
	while (true) {
		pthread_mutex_lock(&argsCast->monitor->mutex);
		for (int i = 0; i < MAPSIZE; i++) {
			struct ttl_item* item = argsCast->monitor->items_arr[i];
			if (item != NULL && is_expired(item->created_at, item->ms_to_expire)) {
			  delete_item(argsCast->map, item->key);
				free(argsCast->monitor->items_arr[i]);
				argsCast->monitor->items_arr[i]= NULL;
			}
		}
		pthread_mutex_unlock(&argsCast->monitor->mutex);
		// sleep for a time period
		usleep(GARBAGE_COLLECT_EXPIRED * 1000);
	}
}

void add_ttl_item(struct ttl_monitor* monitor, char* key, int ms_to_expire) {
	char* key_on_heap = (char *)malloc(strlen(key));
	memcpy(key_on_heap, key, strlen(key));
	struct ttl_item* item = (struct ttl_item*)malloc(sizeof(struct ttl_item));
	item->ms_to_expire = ms_to_expire;
	item->created_at = time(NULL);
	item->key = key_on_heap;
	
	pthread_mutex_lock(&monitor->mutex);
	for (int i = 0; i < MAPSIZE; i++) {
		if (monitor->items_arr[i] == NULL) {
			monitor->items_arr[i] = item;
			pthread_mutex_unlock(&monitor->mutex);
			return;
		}
	}
	// well fuck...... we should throw an exception of some sort or have better error catching but i live on the edge
	free(item);
	pthread_mutex_unlock(&monitor->mutex);
}

// ------------------------- Server utils ------------------------------------------------

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
	*buf += i+1;
}

// -----------Command Handlers---------------------

void handle_echo(int conn, char buf[MAX_BUFFER_SIZE]) {
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

void handle_set(int conn, char buf[MAX_BUFFER_SIZE], struct hashmap* map, bool has_expiration, struct ttl_monitor* monitor) {
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
		int expiration = NOEXPIRATION;
		if (has_expiration) {
			move_buffer_till_next(&buf);
			move_buffer_till_next(&buf); // skip size of PX, we only accept PX
			move_buffer_till_next(&buf); // skip reading PX since thats all we support anyway
			buf++; // move past the $ for the expiration string size
			int expiration_size = get_num(buf);
			move_buffer_till_next(&buf); // Move till we are finally looking at the start of PX time
			expiration = get_num(buf);
		}
		printf("Saving %s, %s with expiration %d \n", key, val, expiration);
		if (expiration != NOEXPIRATION) {
			add_ttl_item(monitor, key, expiration);
		}
		char* msg = set_key_val(map, key, val, expiration)  == 0 ? ok_response : error_message;
		write(conn, msg, strlen(msg));
}

void handle_get(int conn, char buf[MAX_BUFFER_SIZE], struct hashmap* map) {
	// move past the $
	buf++;
	int key_size = get_num(buf);
	printf("The associated get key is of size %d \n", key_size);
	move_buffer_till_next(&buf);
	char key[key_size+2]; // 2 spots for \r\n
	memcpy(key, buf, key_size+2);
	move_buffer_till_next(&buf);
	struct keyvalentry* stored_data = get_value(map, key);

	if (stored_data == NULL) {
		// if it was already expired let the garbage colleciton cycle pick it up
		printf("key not found in store \n");
		write(conn, null_bulk_string, strlen(null_bulk_string));
		return;
	}

	char* write_back_value = stored_data->value;
	int expiration = stored_data->ms_to_expire;
	time_t created_at = stored_data->created_at;
	printf("Expiration for item at %d \n", expiration);

	if (expiration != NOEXPIRATION && is_expired(created_at, expiration)) {
		// if it was already expired let the garbage colleciton cycle pick it up
		printf("expired key was found in store, this will be removed in garbage collection cycle \n");
		write(conn, null_bulk_string, strlen(null_bulk_string));
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
void handle_cmd_array(int conn, char buf[MAX_BUFFER_SIZE], struct hashmap* map, struct ttl_monitor* monitor) {
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
				printf("A set command has been recieved");
				bool has_expiration = false;
				if (num_elements == 5) {
					has_expiration = true;
					printf(" with expiration");
				}
				printf("!\n");
				handle_set(conn, buf, map, has_expiration, monitor);
				elems_read += has_expiration ? 5 : 3;
			} else if (str_size == 3 && strncasecmp(instruction, "GET", 3) == 0) {
				printf("A get command has been recieved! \n");
				handle_get(conn, buf, map);
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

void route(int conn, char buf[MAX_BUFFER_SIZE], int bufsize, struct hashmap* map, struct ttl_monitor* monitor) {
	char firstchar = buf[0];
	switch (firstchar)
	{
	case SIMPLE_STR:
		write(conn, pong, strlen(pong));
		break;
	case ARRAYS:
		handle_cmd_array(conn, buf, map, monitor);
		break;
	default:
		write(conn, error_message, strlen(error_message));
		break;
	}
}

//---------- Event loop and Event loop helpers----------------
void handle_connection(int conn, fd_set *__restrict current_sockets, struct hashmap* map, struct ttl_monitor* monitor)
{
	char buf[1024] = {0};
	if (recv(conn, buf, 1024, 0) > 0)
	{
		// if we can recieve data, then we should parse it and route it to the right handler
		route(conn, buf, 1024, map, monitor);
		return;
	}

	FD_CLR(conn, current_sockets);
}

// IO Multiplexing (One thread listens to multiple sockets!)
void run_multiplex(int server_socket, struct hashmap* map, struct ttl_monitor* monitor) {
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
				handle_connection(i, &current_sockets, map, monitor);
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
	
	// create shared hashmap and pass it down to different routines
	struct keyvalentry* data[MAPSIZE] = { NULL };
	struct hashmap map = { .mutex = PTHREAD_MUTEX_INITIALIZER, .data = data };
	// ------------------------------------------------------------

	// create our background monitor and kick it off in its own thread
	struct ttl_item* items_arr[MAPSIZE] = { NULL };
	struct ttl_monitor monitor = { .mutex = PTHREAD_MUTEX_INITIALIZER, .items_arr = items_arr};
	struct start_monitor_args args = { .monitor = &monitor, .map = &map };

	pthread_t th1;
	pthread_create(&th1, NULL, start_ttl_monitor, (void*) &args);
	// --------------------------------------------------------------

	printf("Redis Server listening on port %d \n", PORT);
	
	// Blocking call
	run_multiplex(server_socket, &map, &monitor);

	close(server_socket);

	return 0;
}
