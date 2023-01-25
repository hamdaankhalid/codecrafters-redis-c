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

const char* error_message = "-Error message\r\n";
const char* pong = "+PONG\r\n";

int get_num(char* first){
	int len = 0;
	while (first[len] != '\r') {
		len++;
	}
	
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

/*
 "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
 Example array to parse
*/
void handle_cmd_array(int conn, char* buf) {
	// move past the *
	buf++;
	int num_elements = get_num(buf);
	printf("NUM ELEMENTS: %d \n", num_elements);
	// move to where the instruction starts
	printf("BUFFER AT START: %s \n", buf);
	move_buffer_till_next(&buf);

	printf("BUFFER AFTER MOVE: %s \n", buf);
	// TODO: CORRECT TILL ABOVE THIS :)
	int elems_read = 0;
	while (elems_read < num_elements) {
			// move past the $
			buf++;
			int str_size = get_num(buf);
			printf("%d\n", str_size);

			// move buffer till we reach start of where instruction is
			move_buffer_till_next(&buf);
			char instruction[str_size+2]; // + 2 for /r/n to be copied
			memcpy(instruction, buf, str_size+2);
			move_buffer_till_next(&buf);
			
			printf("%s\n", instruction);

			if (strcmp(instruction, "ECHO\r\n") == 0 || strcmp(instruction, "echo\r\n") == 0) {
				// then the next cmd will be the cmd to echo back!
				printf("An echo command has been recieved! \n");
				// move past the $
				buf++;
				int next_str_size = get_num(buf);
				printf("The associated string size is of size %d \n", next_str_size);
				move_buffer_till_next(&buf);
				char echo_str[next_str_size+3]; // 1 spot for + and the 2 spots for \r\n
				echo_str[0] = '+';
				memcpy(echo_str+1, buf, next_str_size+2);
				
				printf("Writing back %s \n", echo_str);

				write(conn, echo_str, next_str_size+3);
				move_buffer_till_next(&buf);
				elems_read +=2;
			} else if (strcmp(instruction, "PING\r\n") == 0 || strcmp(instruction, "ping\r\n") == 0) {
				printf("A ping command has been recieved!");
				write(conn, pong, strlen(pong));
				elems_read += 1;
			} else {
				elems_read += 1;
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

void handle_connection(int conn, fd_set *__restrict current_sockets)
{
	char buf[1024] = {0};
	if (recv(conn, buf, 1024, 0) > 0)
	{
		// if we can recieve data, then we should parse it and route it to the right handler
		route(conn, &buf, 1024);
		// write(conn, pong, strlen(pong));
		return;
	}

	FD_CLR(conn, current_sockets);
}

// IO Multiplexing (One thread listens to multiple sockets!)
void run_multiplex(int server_socket)
{
	// Initialize current file descriptor set and add server socket into our fdset
	fd_set current_sockets, ready_sockets;
	FD_ZERO(&current_sockets);
	FD_SET(server_socket, &current_sockets);

	// make a copy of server socket, this is because for some weird reason server_socket gets changed

	while (1)
	{
		// printf("Waiting for a client to connect or socket to be read from!...\n");
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
				printf("New connection accepted. \n");
				FD_SET(new_client_socket, &current_sockets);
			}
			else
			{
				handle_connection(i, &current_sockets);
			}
		}
	}
}

int bind_and_listen(int port)
{
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
