# Serviço Distribuído de Soma com Replicação Passiva

Implementação em Python 3 de um serviço de soma sobre UDP com:

- entrega lógica exatamente uma vez por `(client_id, request_id)`;
- replicação passiva para todos os servidores ativos;
- descoberta autônoma por broadcast;
- entrada e saída dinâmica de clientes e servidores;
- eleição de líder pelo algoritmo do Valentão (Bully);
- sincronização por snapshots fragmentados e verificados com SHA-256.

> A especificação Parte 2 fornecida pede C/C++. Esta implementação permanece em
> Python por decisão explícita do projeto.

## Arquitetura

Cada cliente cria um UUID novo ao iniciar e envia uma requisição por vez. Em
caso de perda, eleição ou troca de líder, ele retransmite o mesmo UUID,
identificador e valor. O novo líder responde com o ACK previamente replicado ou
processa a operação uma única vez.

Os servidores começam no estado `JOINING` e anunciam sua presença por UDP
broadcast. O maior `server-id` conhecido inicia como líder. Um servidor que
entra depois recebe um snapshot completo antes de se tornar `ACTIVE`. Caso seu
ID seja maior, ele inicia uma eleição Bully após a sincronização.

O líder aplica cada nova soma em uma seção crítica, atribui uma versão global e
aguarda o ACK de todos os backups ativos antes de responder ao cliente. Durante
eleições e sincronizações, novas escritas recebem `RETRY` e os clientes mantêm a
requisição pendente.

O estado replicado inclui:

- quantidade global de requisições e soma total;
- versão global do estado;
- termo de eleição e versão de membership;
- último request e último ACK de cada UUID cliente;
- último endereço conhecido de cada cliente.

## Requisitos e rede

- Python 3.10 ou mais recente;
- Linux/Unix;
- clientes e servidores no mesmo domínio de broadcast;
- um endereço IP distinto por servidor.

Servidores reais devem executar em máquinas ou interfaces IP distintas. Vários
processos associados ao mesmo IP e à mesma porta UDP não têm identidade de rede
distinta e o kernel pode distribuir datagramas entre eles. O benchmark usa IPs
loopback distintos, uma porta de serviço comum e um relay UDP apenas para
descoberta e anúncios de cluster.

Partições de rede estão fora do modelo de falhas. Perda, duplicação, atraso,
reordenação, crash e retorno de processos são tolerados desde que os servidores
ativos voltem a se comunicar. O estado existe somente em memória; portanto,
pelo menos um servidor sincronizado deve permanecer ativo.

## Execução

Inicie dois ou mais servidores na mesma porta, cada um em uma máquina:

```bash
python3 server/main.py 4000 --server-id 10
python3 server/main.py 4000 --server-id 20
python3 server/main.py 4000 --server-id 30
```

`--server-id` é opcional. Sem ele, um identificador unsigned de 64 bits é
gerado aleatoriamente. Use `--bind IP` quando a máquina possuir várias
interfaces:

```bash
python3 server/main.py 4000 --server-id 20 --bind 192.168.1.20
```

Inicie qualquer quantidade de clientes:

```bash
python3 client/main.py 4000
```

Digite um inteiro positivo por linha. O cliente não altera o ID da requisição
até receber seu ACK, mesmo que o líder falhe.

Atalhos equivalentes:

```bash
make run-server PORT=4000 SERVER_ID=10
make run-client PORT=4000
```

## Entrada e saída de processos

- **Novo cliente:** descobre apenas o líder e começa com `request_id = 1`.
- **Saída do cliente:** `Ctrl+D` ou `Ctrl+C` envia uma notificação best-effort.
  Seu histórico de deduplicação permanece replicado.
- **Novo servidor:** anuncia `JOINING`, recebe snapshot e só então vira
  `ACTIVE`.
- **Saída de backup:** o líder remove o membro e publica o novo membership.
- **Saída do líder:** os backups iniciam eleição e clientes repetem suas
  requisições pendentes.
- **Último servidor:** a primeira tentativa de encerramento é recusada. Inicie
  outro servidor ou pressione `Ctrl+C` novamente para forçar a saída.

Heartbeats são enviados a cada 500 ms. Quatro períodos sem contato disparam
remoção ou eleição.

## Protocolo

Todos os datagramas usam JSON compacto com a versão `v = 2` e campo `type`.
Datagramas são limitados a 1200 bytes. Snapshots maiores são divididos em
fragmentos de 700 bytes, numerados e protegidos por SHA-256.

Principais mensagens:

- clientes: `CLIENT_DISCOVERY`, `LEADER`, `CLIENT_REQUEST`, `CLIENT_ACK`,
  `NOT_LEADER`, `RETRY`, `CLIENT_LEAVE`;
- membership: `SERVER_HELLO`, `JOIN_REQUEST`, `HEARTBEAT`,
  `BACKUP_HEARTBEAT`, `MEMBERSHIP`,
  `LEAVE`;
- replicação: `REPLICATION`, `REPLICATION_ACK`, `SNAPSHOT_CHUNK`,
  `SNAPSHOT_ACK`, `SNAPSHOT_REQUEST`;
- eleição: `ELECTION`, `ELECTION_OK`, `STATE_SUMMARY_REQUEST`,
  `STATE_SUMMARY`, `COORDINATOR`.

## Testes

```bash
make test
```

A suíte verifica validação do protocolo, limites unsigned de 64 bits,
fragmentação de snapshots, deduplicação, ordem das versões, startup com três
servidores, perda de datagrama de replicação, falha abrupta do líder,
retransmissão após failover e entrada de um servidor de maior prioridade.

## Benchmark

### Benchmark simples

Com um cluster já ativo:

```bash
make benchmark PORT=4000 CLIENTS=4 REQUESTS=1000
```

O benchmark informa requisições concluídas, tempo e throughput. A criação
literal de uma thread por requisição segue a especificação, mas possui custo
considerável em cargas muito grandes; a replicação síncrona para todos os
backups também privilegia correção e durabilidade sobre latência.

### Geração das listas

Gere quatro arquivos determinísticos com 25.000 inteiros positivos por cliente:

```bash
make benchmark-generate
```

Os arquivos são gravados em `benchmark-results/generated/`. O
`manifest.json` contém o seed, SHA-256, quantidade e soma de cada lista, a soma
cumulativa após cada cliente e o resultado global esperado.

Parâmetros podem ser alterados:

```bash
make benchmark-generate ENTRIES=50000 SEED=123 OUTPUT_ROOT=meus-resultados
```

### Benchmark caótico completo

Execute a calibração e o cenário automatizado com quatro servidores e quatro
clientes. Cada relay, servidor e cliente é um processo independente:

```bash
make benchmark-chaos
```

O runner:

1. mede o throughput limpo com quatro réplicas somente para estimar a duração;
2. gera exatamente `ENTRIES` números por cliente em memória, 25.000 por padrão;
3. encerra os processos de calibração e inicia um cluster novo;
4. executa falhas e retornos de backups, queda e reeleição do líder, entrada de
   servidores de maior prioridade, operação com somente um servidor e churn de
   zero a quatro clientes;
5. verifica a contagem, soma, versão e tabela de deduplicação em todas as
   réplicas finais.

O cenário usa UDP real e o mesmo porto de serviço em IPs loopback distintos:
os IDs 10–60 usam `127.0.0.10`–`127.0.0.60`. Um relay UDP opcional distribui
somente descoberta e anúncios de cluster. Requisições, ACKs, replicação,
snapshots, heartbeats e eleição continuam sendo tráfego UDP direto. Em várias
máquinas, use IPs reais e aponte `--discovery` para o endereço do relay.

Configuração típica:

```bash
make benchmark-chaos \
  ENTRIES=25000 \
  TARGET_DURATION=240 \
  SEED=20260613 \
  BASE_PORT=47000
```

Para validar rapidamente a instalação:

```bash
make benchmark-chaos-quick
```

Cada execução cria `benchmark-results/chaos-<data>-<id>/` contendo:

- `config.json`;
- logs separados por geração de relay, servidor e cliente;
- `processes.json` com PID, comando, geração, sinal e código de saída;
- `timeline.jsonl` com joins, leaves, eleições e assertions;
- `samples.jsonl` com status UDP, throughput e quantidades ativas;
- `status_snapshots.jsonl` com as respostas UDP completas de clientes e
  servidores;
- `report.json` com duração projetada/real, `PASSED` ou `FAILED` e o estado
  final completo.

O runner registra um aviso aos 270 segundos caso ainda esteja executando, mas
continua até verificar a soma. `SIGTERM` pausa clientes após o ACK corrente e
faz servidores saírem graciosamente; `SIGKILL` simula falha abrupta. Clientes
reiniciados recebem identidade, posição e requisição pendente do estado em
memória observado por UDP pelo orquestrador. Nenhum arquivo é lido durante o
benchmark para restaurar estado ou trocar dados entre processos; os arquivos
do diretório de resultados são somente logs e relatórios de saída.

Uma execução de referência com 100.000 operações terminou em 133 segundos,
sem aviso de duração, e validou a soma esperada em todas as quatro réplicas
finais.
