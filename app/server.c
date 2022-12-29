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

void handle_connection(int conn, fd_set *__restrict current_sockets)
{
	unsigned char *ping = "PING\r\n";
	unsigned char *PONG = "+PONG\r\n";
	unsigned char buf[1024] = {0};

	if (recv(conn, buf, 1024, 0) > 0)
	{
		write(conn, PONG, strlen(PONG));
	}
	else
	{
		FD_CLR(conn, current_sockets);
	}
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
