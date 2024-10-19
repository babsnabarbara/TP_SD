import socket
import threading
import os
import time
import json  # Biblioteca JSON
from Functions import *
from datetime import datetime

# Variáveis de ambiente
container_id = int(os.getenv('ID'))
cluster_port = int(os.getenv('CLUSTER_PORT'))

shared_file = '/shared/output.txt'

client_message =   ""# receber a mensagem do cliente
client_timestamp = str(-5)
GET = False
containers = create_containers(5)
ok_written = 0

ok_escrita = 0
ok_ts = 0

# Comunicação entre servidores
def server():
    global containers, GET
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        # Socket para escutar servidor x servidor.
        server_socket.bind(('0.0.0.0', cluster_port))
        server_socket.listen(1)
        #print(f"Container {container_id} ouvindo na porta {cluster_port}...")
        server_socket.settimeout(0.2)
        # Loop para o tempo inteiro estar lidando com interações entre servidores.
        while True:
            try:
        
            #print("Aguardando conexão...")
                conn, addr = server_socket.accept()  # Tenta aceitar uma conexão
                #print(f"Conexão aceita de {addr}")
                
                handle_request(conn)
                conn.close()

            except socket.timeout:
                # Se não houver conexão após o timeout, continua a execução
                print("Nenhuma conexão recebida dentro do tempo limite. Continuando...")
                # Aqui você pode fazer qualquer outra coisa, ou até encerrar o loop.
            

def reseta():
    print ("RESETOU")
    global client_timestamp, client_message, ok_escrita, ok_ts, containers, containers_interessados, ok_written, escreveu

    client_message = ""
    client_timestamp = -2
    ok_escrita = 0  # Resetar o contador de OKs para escrita
    containers = create_containers(5)
    containers_interessados = ordena_timestamps()
    ok_ts = 0
    ok_written = 0
    escreveu = False
    time.sleep(1)

def escreve_arquivo():
    print("ARQUIVO")
    global GET
    with open(shared_file, 'a') as f:
        print ("CONTAINER ID: ", container_id)
        f.write(f"Container {container_id} escreveu no arquivo com TS {client_timestamp}\n")
        f.write(f"Mensagem: {client_message}\n")  # Adiciona a mensagem recebida
        GET = False

def envia_permissao_escrita(containers_interessados):
    for con in containers_interessados:
        if float(client_timestamp) < float(con['timestamp']):
            # Manda um OK_ESCRITA para todos os containers que têm TS maior que o meu
            x = send_message(con, {'command': 'OK_ESCRITA'})
            print("ENVIEI OK ESCRITA")
            
def envia_que_escreveu(containers):
    for con in containers: 
        x = send_message(con, {'command': 'OK_WRITTEN'})
            
def ordena_timestamps():
    global containers
    sorted_containers = sorted([c for c in containers if c['timestamp'] >= 0], key=lambda x: x['timestamp'])
    return sorted_containers

def envia_timestamps():
    global ok_ts
    #print(f"CONTAINERS: {containers}")
    for con in containers:
        # Manda timestamp para todos os containers
        #print("CON ID: ", con['id'])
        con['timestamp'] = float(send_message(con, {'command': 'TIMESTAMP'})) 
        #print("AFTER")
        #print(f"SEND MESSAGE RETURN: {con['timestamp']}")
        if con['timestamp'] != -2 and ok_ts < 5:
            print("YESS")
            con['responded'] = "yes"
            

# Função para enviar mensagem a outro container e receber resposta
def send_message(container, message):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((f"container_{container['id']}", container['cluster_port']))
            sock.send(json.dumps(message).encode('utf-8'))
            

            recv_data = sock.recv(1024).decode('utf-8')  # Receber os dados
            
            if not recv_data:  # Verifique se a mensagem recebida é vazia
                print(f"Erro: Nenhuma mensagem recebida do container {container['id']}")
                return -2
            recv = json.loads(recv_data)  # Receber e decodificar como JSON
            return recv.get('timestamp', -2)
    except ConnectionRefusedError:
        print(f"Falha ao conectar no container {container['id']}")
        return -2
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar JSON: {e}")
        return -2


def verifica_timestamps():
    # Verifica se todos os containers interessados receberam timestamps
    #print ("OK TS: ", ok_ts)
    return ok_ts == 5

def handle_request(server_atual):
    global ok_written, ok_escrita
    #print("HANDLE_REQUEST STARTED")
    
    # Recebe a informação de outro servidor
    data = server_atual.recv(1024).decode('utf-8')

    if not data:
        print("DATA EQUALS NONE")
        return

    # Decodifica a mensagem JSON recebida
    data = json.loads(data)
    #print(f"data: {data}")

    # Se o comando for "TIMESTAMP"
    if data.get('command') == "TIMESTAMP":
        try:
            #print("CLIENT TIMESTAMP: ", client_timestamp)
            response = json.dumps({'timestamp': client_timestamp}).encode('utf-8')
            server_atual.send(response)
            #print(f"Enviou timestamp: {client_timestamp}")
        except Exception as e:
            print(f"Erro ao enviar timestamp: {e}")

    # Se o comando for "OK_ESCRITA"
    if data.get('command') == "OK_ESCRITA" and ok_ts == 5 and ok_escrita < 4:

        response = json.dumps({'OK': "ok recebido"}).encode('utf-8')
        server_atual.send(response)        
        ok_escrita += 1
        print("OK ESCRITA")
    if data.get('command') == "OK_WRITTEN":

        response = json.dumps({'OK': "ok written recebido"}).encode('utf-8')
        server_atual.send(response)        
        ok_written += 1
        print("OK WRITTEN")

# Função para ouvir as mensagens dos clientes e processar
def listen_client(client_socket):
    global client_message, client_timestamp, GET

    while True:
        message = client_socket.recv(1024).decode('utf-8')  # Recebe a mensagem do cliente
        #print(f"\nMensagem recebida do cliente: {message}\n")
        if not message:
            print("Conexão fechada pelo cliente.")
            break

        # Receber a mensagem caso exista
        
        if message and client_message == -1:
            send_data(client_socket, json.dumps({'status': 'sleep'}))      
        elif message and not client_message:
            message_data = json.loads(message)  # Decodificar JSON
            #print ("JUST RECEIVED MESSAGE: ", message_data)
            client_message = message_data.get('message', "")
            print(f"client message === {client_message}")
            if GET == False:
                client_timestamp = message_data.get('timestamp', -2)
                GET = True
                send_data(client_socket, json.dumps({'status': 'committed'}))  # Enviar confirmação
                
                
            #print("CLIENT TIMESTAMP: ", client_timestamp)
            #print(f"\n\n\nMensagem do cliente: {client_message}, timestamp: {client_timestamp}\n\n\n")  
        else:
            send_data(client_socket, json.dumps({'status': 'sleep'})) # Informa que está ocupado
            

def encontrar_container_por_id(containers, id_para_encontrar):
    for indice, container in enumerate(containers):
        if container['id'] == id_para_encontrar:
            return indice
    return -1


def trading_data():
    global containers_interessados, ok_ts, escreveu
    escreveu = False
    
    while True:
        print(f"OK TS VALOR: {ok_ts}")
        time.sleep(0.2)
        if ok_written == 5:
            reseta()
            print("RESETOU")
        if ok_ts < 5:

            envia_timestamps()
            counting_okts = 0
            for con in containers:
                if con['responded'] == "yes":
                    counting_okts += 1
            ok_ts = counting_okts
            
        elif ok_ts == 5 and  GET == True:
            print (f"OK ESCRITA DENTRO: {ok_escrita}")
            # Enviar os TS para todos os servidores.
            
            #print(f'Timestamps enviados')

            # Verificar se todos os containers receberam todos os timestamps
            #if verifica_timestamps():
            #print(f'Timestamps verificados')F

            # Ordena os TS e guarda apenas os que querem escrever.

            containers_interessados = ordena_timestamps()
            #print(f"containers interessados: {containers_interessados}")
            #print("containers ordenados: ", containers_interessados)
            #print(f'Timestamps ordenados')

            # Manda OK_Escrita
            
            
            #print(f'OK_ESCRITA enviados\n')
            #print (f"ok escrita: {ok_escrita} e length: {len(containers_interessados)-1}")
            # Aquele que recebe todos os OK, escreve no arquivo.
            #print (f"containers: {len(containers_interessados)}\n")
            
            #print (f"CONTAINERS: {containers_interessados}")
            print (f"ok escrita: {ok_escrita} e containers interessados {containers_interessados}")
            if ok_ts == 5  and escreveu == False:
                if (encontrar_container_por_id(containers_interessados, container_id) - ok_escrita ) == 0:
                    escreveu = True
                    print('Escreveu')
                    escreve_arquivo()
                    envia_que_escreveu(containers)
                    envia_permissao_escrita(containers_interessados)
                
                    # Reseta tudo
                    
        

# Conectar o cliente no servidor
server_socket = create_server('0.0.0.0', int(os.getenv('PORT')))  # (host, port)

# Cliente em contato com o servidor
client_socket = accept_client(server_socket)

# Thread para escutar o cliente
threading.Thread(target=listen_client, args=(client_socket,)).start()
#print('Thread listen client rodando')

threading.Thread(target=trading_data).start()

server()