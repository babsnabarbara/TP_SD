import time
import re
import socket
import threading
import random
import os
import json 

#Função para criar um servidor
def create_server(host, port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(1)
    print(f"Servidor criado e aguardando conexões na porta {port}...")
    return server_socket

#Função para o servidor aceitar o cliente
def accept_client(server_socket):
    client_socket, addr = server_socket.accept()
    print(f"Conexão estabelecida com o cliente {addr}")
    return client_socket
