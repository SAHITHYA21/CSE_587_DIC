import socket
import time

# Create a TCP/IP socket
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(('localhost',9999))
server_socket.listen(1)

print("Server is listening on port 9999...")

while True:
    connection,client_address = server_socket.accept()
    try:
        print(f"Connection from {client_address}")
        # Continuously send lines of data
        with open('sentences.txt', 'r') as file:
            for line in file:
                connection.sendall(line.encode())
                time.sleep(1)  # Simulate streaming with a delay
    finally:
        connection.close()
