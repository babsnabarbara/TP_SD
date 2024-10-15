import socket
import threading
import os
import time
import json  # Biblioteca JSON
from Functions import *
from datetime import datetime

#Variáveis de ambiente
container_id = int(os.getenv('ID'))
cluster_port = int(os.getenv('CLUSTER_PORT'))
shared_file = '/shared/output.txt'



#Função para o servidor se conectar com os outros servidores
def conecta_servidores():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        #o bind associa o sockeet a um endereço de IP e uma porta
        server.bind(('0.0.0.0', cluster_port))
        server.listen(1)

        server.settimeout(0.2)
        while True:
            try:
                conn, _ = server.accept()
                #se abriu uma conexão, precisa fechar após.

                #função que vai lidar com requisições entre os nós do cluster.
                handle_cluster(conn)
                
                #fechando conexão
                conn.close()
            
            except socket.timeout:
                print("Nenhuma conexão recebida dentro do tempo limite. Continuando...")





if __main__ == "__main__":
    #o servidor precisa ser criado
    server = create_server('0.0.0.0', int(os.getenv('PORT')))  # (host, port)

    #o servidor precisa conectar ao cliente
    client = accept_client(server)

    #o servidor precisa se conectar a outros clientes

    #o servidor precisa receber uma mensagem do cliente

    #o servidor extrai o timestamp e a mensagem do cliente

    #o servidor envia o ts e a mensagem recebida para os outros servidores

    #o servidor espera todos os dados serem recebidos

    #confirmação que todos os dados foram recebidos

    #organização dos dados recebidos

    #filtra os servidores que não querem escrever

    #ordena pelo timestamp

    #envia um OK para todos os timestamps menores do que o seu

    #caso tenha 4 OKs, escreve no arquivo

    #após escrever, envia um OK para o proximo da fila

    #epós enviar, espera todos escreverem.

    #depois que todos escreverem, entra em loop.