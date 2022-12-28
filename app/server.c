#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

void bind_and_listen(int* server_fd, struct sockaddr *client_addr, int* client_addr_len) {
	int server_fd, client_addr_len;
	struct sockaddr_in client_addr;
	
	server_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (server_fd == -1) {
		printf("Socket creation failed: %s...\n", strerror(errno));
		return 1;
	}
	
	// Since the tester restarts your program quite often, setting REUSE_PORT
	// ensures that we don't run into 'Address already in use' errors
	int reuse = 1;
	if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof(reuse)) < 0) {
		printf("SO_REUSEPORT failed: %s \n", strerror(errno));
		return 1;
	}
	
	struct sockaddr_in serv_addr = { .sin_family = AF_INET ,
									 .sin_port = htons(6379),
									 .sin_addr = { htonl(INADDR_ANY) },
									};
	
	if (bind(server_fd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) != 0) {
		printf("Bind failed: %s \n", strerror(errno));
		return 1;
	}
	
	int connection_backlog = 5;
	if (listen(server_fd, connection_backlog) != 0) {
		printf("Listen failed: %s \n", strerror(errno));
		return 1;
	}

	printf("Waiting for a client to connect...\n");
	client_addr_len = sizeof(client_addr);

	return server_fd;
}

int main() {
	// Disable output buffering
	setbuf(stdout, NULL);
	
	int server_socket, client_addr_len;
	struct sockaddr_in client_addr;

	bind_and_listen(&server_socket, &client_addr, &client_addr_len);

	while(1) {
		int client_socket = accept(server_socket, (struct sockaddr *) &client_addr, &client_addr_len);

		printf("Client connected\n");

		unsigned char* ping = "PING\r\n";
		unsigned char* PONG = "+PONG\r\n";
		unsigned char buf[1024] = {0};

		read(client_socket, buf, 1024);
		write(client_socket, PONG, strlen(PONG));

	}	

	close(server_socket);

	return 0;
}
