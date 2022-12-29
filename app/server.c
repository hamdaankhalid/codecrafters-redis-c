#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/select.h>

# define PORT 6379

int bind_and_listen(struct sockaddr *client_addr, int* client_addr_len, int port) {
	int server_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (server_fd == -1) {
		printf("Socket creation failed: %s...\n", strerror(errno));
		return -1;
	}
	// Since the tester restarts your program quite often, setting REUSE_PORT
	// ensures that we don't run into 'Address already in use' errors
	int reuse = 1;
	if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof(reuse)) < 0) {
		printf("SO_REUSEPORT failed: %s \n", strerror(errno));
		return -1;
	}
	struct sockaddr_in serv_addr = { .sin_family = AF_INET ,
									 .sin_port = htons(port),
									 .sin_addr = { htonl(INADDR_ANY) },
									};
	if (bind(server_fd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) != 0) {
		printf("Bind failed: %s \n", strerror(errno));
		return -1;
	}
	int connection_backlog = 0;
	if (listen(server_fd, connection_backlog) != 0) {
		printf("Listen failed: %s \n", strerror(errno));
		return -1;
	}
	client_addr_len = (int *) sizeof(client_addr);
	return server_fd;
}


void handle_connection(int conn) {
	unsigned char* ping = "PING\r\n";
	unsigned char* PONG = "+PONG\r\n";
	unsigned char buf[1024] = { 0 };
	while (recv(conn, buf, 1024, 0) > 0) {
		write(conn, PONG, strlen(PONG));
	}
}

// IO Multiplexing (One thread listens to multiple sockets!)
void run_multiplex(const int server_socket, struct sockaddr_in* client_addr, int client_addr_len) {
	fd_set current_sockets, ready_sockets;
	// Initialize current file descriptor set
	FD_ZERO(&current_sockets);
	// Add server socket into our fdset
	FD_SET(server_socket, &current_sockets);

	while (1) {

		printf("Waiting for a client to connect or socket to be read from!...\n");
		// make a copy because select is destructive
		ready_sockets = current_sockets;
		// select() returns the number of ready descriptors that are contained in the descriptor sets, or -1 if an error occurred.
		if (select(FD_SETSIZE, &ready_sockets, NULL, NULL, NULL) < 0) {
			perror("Select error");
			exit(1);
		}

		// Iterate over entire fdset, if the socket is ready.
		// Ready socket is actually an incoming message on an existing client socket.
		for (int i = 0; i < FD_SETSIZE; i++) {
			if (!FD_ISSET(i, &ready_sockets)) {
				continue;
			}

			// TODO: WHY ARE CONNECTIONS MADE AFTER FIRST ONE NOT EQUAL TO SERVER_SOCKET
			if (i == server_socket) {
				printf("New Client Connected\n");
				int new_client_socket = accept(server_socket, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
				if (new_client_socket < 0) {
					perror("Error accepting new client connection");
					continue;
				}
				printf("Client socket on connection: %d \n", new_client_socket);
				FD_SET(new_client_socket, &current_sockets);
			} else {
				printf("A previously queued socket is ready: %d\n", i);
				handle_connection(i);
				FD_CLR(i, &current_sockets);
				printf("Connection Handled\n");
			}
		}
	}
}

int main() {
	// Disable output buffering
	setbuf(stdout, NULL);

	int server_socket, client_addr_len;
	struct sockaddr_in client_addr;

	server_socket = bind_and_listen((struct sockaddr *) &client_addr, &client_addr_len, PORT);
	if (server_socket < 0) {
		return 1;
	}
	
	run_multiplex(server_socket, &client_addr, client_addr_len);

	close(server_socket);

	return 0;
}
