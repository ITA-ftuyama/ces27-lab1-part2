# CES-27 - LAB 1 - PART 2

Instruções para configuração do ambiente: [Setup](SETUP.md)  
Para saber mais sobre a utilização do Git, acessar: [Git Passo-a-Passo](GIT.md)  
Para saber mais sobre a entrega do trabalho, acessar: [Entrega](ENTREGA.md)  

## Parte II - Execução Distribuída com Tolerância a Falhas

Nesta segunda parte da prática, temos pronto um processo de MapReduce que já opera de forma distribuída.

O código já possui funções map, reduce e split, mas caso queira testar a sua implementação referente à primeira parte, basta sobrescrever o código dado.

### Executando o Código
Para executar o código, precisamos executar multiplos processos. O primeiro deles são os clientes. Note que o valor de -port deve ser alterado para cada cliente. Neste lab usaremos a port 5000 para o Master e 5001+ para os Workers.
O segundo é o Master.

Executando Worker (aumentar -port para o worker seguinte):
```bash
wordcount$ go run main.go wordcount.go data.go -mode distributed -type worker -port 5001
```
> Running in distributed mode.  
> NodeType: worker  
> Address: localhost  
> Port: 5001  
> Master: localhost:5000  
> Running Worker on localhost:5001  
> Registering with Master  

Executando o Master:
```bash
wordcount$ go run main.go wordcount.go data.go -mode distributed -type master
```
> Running in distributed mode.  
> NodeType: master  
> Reduce Jobs: 5  
> Address: localhost  
> Port: 5000  
> File: files/pg1342.txt  
> Chunk Size: 102400  
> Running Master on localhost:5000  
> Scheduling Worker.RunMap operations  
> Accepting connections on 127.0.0.1:5000  


Quando os dois processos tiverem sido inicializados, temos as seguintes saídas:

Master:
> Registering worker '0' with hostname 'localhost:5001'  
> Running Worker.RunMap (ID: '0' File: 'map\map-0' Worker: '0')  
> Running Worker.RunMap (ID: '1' File: 'map\map-1' Worker: '0')  
> Running Worker.RunMap (ID: '2' File: 'map\map-2' Worker: '0')  
> Running Worker.RunMap (ID: '3' File: 'map\map-3' Worker: '0')  
> Running Worker.RunMap (ID: '4' File: 'map\map-4' Worker: '0')  
> Running Worker.RunMap (ID: '5' File: 'map\map-5' Worker: '0')  
> Running Worker.RunMap (ID: '6' File: 'map\map-6' Worker: '0')  
> Running Worker.RunMap (ID: '7' File: 'map\map-7' Worker: '0')  
> Worker.RunMap operations completed  
> Scheduling Worker.RunReduce operations  
> Running Worker.RunReduce (ID: '0' File: 'reduce\reduce-0' Worker: '0')  
> Running Worker.RunReduce (ID: '1' File: 'reduce\reduce-1' Worker: '0')  
> Running Worker.RunReduce (ID: '2' File: 'reduce\reduce-2' Worker: '0')  
> Running Worker.RunReduce (ID: '3' File: 'reduce\reduce-3' Worker: '0')  
> Running Worker.RunReduce (ID: '4' File: 'reduce\reduce-4' Worker: '0')  
> Worker.RunReduce operations completed  
> Closing Remote Workers.  
> Done.  

Worker:
> Registered. WorkerId: 0 (Settings = (ReduceJobs: 5))
> Accepting connections on 127.0.0.1:5001  
> Running map id: 0, path: map\map-0  
> Running map id: 1, path: map\map-1  
> Running map id: 2, path: map\map-2  
> Running map id: 3, path: map\map-3  
> Running map id: 4, path: map\map-4  
> Running map id: 5, path: map\map-5  
> Running map id: 6, path: map\map-6  
> Running map id: 7, path: map\map-7  
> Running reduce id: 0, path: reduce\reduce-0  
> Running reduce id: 1, path: reduce\reduce-1  
> Running reduce id: 2, path: reduce\reduce-2  
> Running reduce id: 3, path: reduce\reduce-3  
> Running reduce id: 4, path: reduce\reduce-4  
> Done.  

### Atividade
Executar com 1 master e 1 worker.
Executar com 1 master e 2 workers. Para isso você deve inicializar primeiramente os Workers e só então inicializar o Master.

### Discussão
Nessa execução, a tarefa de MapReduce deve estar sendo concluída corretamente. É importante observar que a execução não é determinística.

O grande problema desse modelo é que ele não é resiliente a falhas. Quando executado em larga escala, milhares de nós são alocados e executam o processo de Worker. Com muitos nós alocados, é comum que ocorram falhas e que alguns desses processos parem.

### Introduzindo Falhas
Vamos introduzir agora um processo worker no qual vamos induzir uma falha.

Para isso, inicialize o worker usando o seguinte comando:
```bash
wordcount$ go run main.go wordcount.go data.go -mode distributed -type worker -port 5001 -fail 3
```
> Running in distributed mode.  
> NodeType: worker  
> Address: localhost  
> Port: 5001  
> Master: localhost:5000  
> Induced failure  
> After 3 operations  
> Running Worker on localhost:5001  
> Registering with Master  

Nesse caso, esse worker vai falhar na execução da terceira tarefa.

Executando o Master junto com um único Worker defeituoso, teremos o seguinte resultado:
```go
wordcount$ go run main.go data.go wordcount.go -mode distributed -type master
```
> Running in distributed mode.  
> NodeType: master  
> Reduce Jobs: 5  
> Address: localhost  
> Port: 5000  
> File: files/pg1342.txt  
> Chunk Size: 102400  
> Running Master on localhost:5000  
> Scheduling Worker.RunMap operations  
> Accepting connections on 127.0.0.1:5000  
> Registering worker '0' with hostname 'localhost:5001'  
> Running Worker.RunMap (ID: '0' File: 'map\map-0' Worker: '0')  
> Running Worker.RunMap (ID: '1' File: 'map\map-1' Worker: '0')  
> Operation Worker.RunMap '1' Failed. Error: read tcp 127.0.0.1:58204->127.0.0.1:5001: wsarecv: An existing connection was forcibly closed by the remote host.  

Note que a execução vai ficar travada. Para entender melhor vamos olhar o seguintes trechos de código:

```go
// master.go
type Master struct {
    (...)
    // Workers handling
    workersMutex sync.Mutex
    workers      map[int]*RemoteWorker
    totalWorkers int // Used to generate unique ids for new workers

    idleWorkerChan   chan *RemoteWorker
    failedWorkerChan chan *RemoteWorker
    (...)
}
```

Todas essas estruturas são utilizadas para gerenciar os Workers no Master. 

A primeira coisa que acontece quando um worker é inicializado é a chamada da função Register no Master:

```go
// master_rpc.go
func (master *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
    (...)
    master.workersMutex.Lock()

    newWorker = &RemoteWorker{master.totalWorkers, args.WorkerHostname, WORKER_IDLE}
    master.workers[newWorker.id] = newWorker
    master.totalWorkers++

    master.workersMutex.Unlock()

    master.idleWorkerChan <- newWorker
    (...)
}
```

Observe que quando um novo Worker se registra no Master, ele adiciona esse worker na lista **master.workers**. Para evitar problemas de sincronia no acesso dessa estrutura usamos o mutex **master.workersMutex**.

O ponto mais interessante entretanto, é a última linha:
```go
master.idleWorkerChan <- newWorker
```

Nessa linha, o worker que acabou de se registrado é colocado no canal de Workers disponíveis. A saída desse canal está em:

```go
// master_scheduler.go
func (master *Master) schedule(task *Task, proc string, filePathChan chan string) {
    (...)
    counter = 0
    for filePath = range filePathChan {
        operation = &Operation{proc, counter, filePath}
        counter++

        worker = <-master.idleWorkerChan
        wg.Add(1)
        go master.runOperation(worker, operation, &wg)
    }

    wg.Wait()
    (...)
}
```

Essa é a função que aloca as operações nos workers.

Mas porque essas coisas estão acontecendo em paralelo?

```go
// mapreduce.go
    (...)
    go master.acceptMultipleConnections()
    go master.handleFailingWorkers()

    // Schedule map operations
    master.schedule(task, "Worker.RunMap", task.InputFilePathChan)
    (...)
```

Observe que na linha master.acceptMultipleConnections() é colocado em uma Goroutine separada. Isso é o equivalente a colocar a execução desse método numa Thread separada.
Já na linha master.schedule(...), fazemos a execuçao na goroutine atual.

Dessa forma essas operações estão acontecendo concorrentemente, e o master.idleWorkerChan é o canal de comunicação entre elas. Quando a operação Register escreve no canal, a operação schedule é informado de que um novo Worker está disponível e continua a execução.

Por último, para entender o Deadlock, vamos olhar o código a seguir:
```go
func (master *Master) runOperation(remoteWorker *RemoteWorker, operation *Operation, wg *sync.WaitGroup) {
    (...)
    err = remoteWorker.callRemoteWorker(operation.proc, args, new(struct{}))

    if err != nil {
        log.Printf("Operation %v '%v' Failed. Error: %v\n", operation.proc, operation.id, err)
        wg.Done()
        master.failedWorkerChan <- remoteWorker
    } else {
        wg.Done()
        master.idleWorkerChan <- remoteWorker
    }
    (...)
}
```

Quando um worker completa uma operação corretamente, ele cai no else acima e o worker que a executou é colocado de volta no canal master.idleWorkerChan (e que vai ser lido pelo scheduler mostrado anteriormente). Entretanto, no caso de falha, o worker é colocado no canal master.failedWorkerChan. Quem trata esse canal?

**Ninguém!!!**

É por isso que a execução trava. O scheduler vai esperar infinitamente por um worker (que falhou). Para concluir a execução, basta executar um segundo worker e as operações vão ser retomadas.

No fim da tarefa de MapReduce, o Master informa a todos os Workers que a tarefa foi finalizada.

Neste caso recebemos o seguinte erro:
> Closing Remote Workers.  
> Failed to close Remote Worker. Error: dial tcp [::1]:5003: connectex: No connection could be made because the target machine actively refused it.  
> Done.  

Isso ocorre porque o worker que falhou continua na lista de Workers(master.workers, adicionado em Register).

### Atividade ###

Abra o seguinte arquivo:
```go
// master.go
func (master *Master) handleFailingWorkers() {
    /////////////////////////
    // YOUR CODE GOES HERE //
    /////////////////////////
}
```

Essa rotina é executada em uma Goroutine separada (no arquivo mapreduce.go, logo abaixo de go master.acceptMultipleConnections()). Você deve alterá-la de forma que toda vez que um worker falhar durante uma operação, ele seja corretamente tratado.

Num ambiente real, existem várias possibilidades, como por exemplo informar ao processo que gerencia a inicialização dos workers o endereço do worker falho, verificar se o worker ainda está vivo (isso pode acontencer no caso de uma falha de rede por exemplo).

No nosso caso, não vamos tentar retomar o processo, mas apenas registrar que ele não está mais disponível.

**Útil:**  
A função deve monitorar o canal master.failedWorkerChan. Para isso, é interessante observar o uso da operação **range** em estruturas do tipo channel. https://gobyexample.com/range-over-channels

Para remover elementos em estruturas do tipo map, utilizar a operação **delete**. https://gobyexample.com/maps

Por último, para garantir a sincronia da estrutura, utilizar o mutex master.workersMutex para proteger o acesso à estrutura master.workers. https://gobyexample.com/mutexes

**Resultado:**  

Removendo worker da lista
> Running Worker.RunMap (ID: '2' File: 'map\map-2' Worker: '0')  
> Operation Worker.RunMap '2' Failed. Error: read tcp 127.0.0.1:58749->127.0.0.1:5003: wsarecv: An existing connection was forcibly closed by the remote host.  
> Removing worker 0 from master list.

Mesmo com falhas, os workers são finalizados corretamente:
> Worker.RunReduce operations completed   
> Closing Remote Workers.   
> Done.   


### Recuperação após Falhas

No código acima, fizemos com que o nosso código terminasse de forma elegante mesmo que workers falhassem. Entretanto, a operação onde ocorreu a falha nunca foi realizada, e por conta disso, o nosso MapReduce está incorreto. Para observar isto, basta tentar localizar os arquivos resultantes da operação que falhou (na pasta *reduce/* em caso de falha de uma operação map e *result/* em caso de reduce).

![0 byte files](doc/0-byte-files.png)

Para começar a entender, no código abaixo:

```go
master_scheduler.go
func (master *Master) schedule(task *Task, proc string, filePathChan chan string) {
    var (
        wg        sync.WaitGroup
    (...)
    counter = 0
    for filePath = range filePathChan {
        operation = &Operation{proc, counter, filePath}
        counter++

        worker = <-master.idleWorkerChan
        wg.Add(1)
        go master.runOperation(worker, operation, &wg)
    }

    wg.Wait()
    (...)
}
```

Temos a seguinte lógica:
O nome dos arquivos que devem ser processados são obtidos através de uma leitura em um canal (filePath = range filePathChan). Em seguida, uma operação é criada e um worker é obtido através do canal master.idleWorkerChan. Em seguida, 1 delta é adicionado ao WaitGroup **wg** e uma nova goroutine é executada com a chamada de runOperation.

WaitGroup é um mecanismo de sincronização de um número variável de goroutines. Neste caso, toda vez que uma operação é executada em uma nova goroutine, adicionamos 1 contador no WaitGroup. Desta forma, na linha wg.Wait() o código vai ficar bloqueado até que n goroutines tenham chamado wg.Done(), onde n é o total de deltas adicionado ao WaitGroup.

```go
// master_scheduler.go
func (master *Master) runOperation(remoteWorker *RemoteWorker, operation *Operation, wg *sync.WaitGroup) {
    (...)
    if err != nil {
        log.Printf("Operation %v '%v' Failed. Error: %v\n", operation.proc, operation.id, err)
        wg.Done()
        master.failedWorkerChan <- remoteWorker
    } else {
        wg.Done()
        master.idleWorkerChan <- remoteWorker
    }
}
```

### Atividade

Você deve alterar o código fornecido de forma que as operações que falhem sejam executadas. Uma solução simples utiliza canais de forma eficiente para comunicar operações que falharam entre goroutines (que podem ser criadas pelo aluno).

É indicado que alterações sejam feitas apenas nos seguintes arquivos

```go
// master.go
    (...)
    ///////////////////////////////
    // ADD EXTRA PROPERTIES HERE //
    ///////////////////////////////
    // Fault Tolerance
    (...)
```

```go
// master_scheduler.go
func (master *Master) schedule(task *Task, proc string, filePathChan chan string) {
    (...)
    //////////////////////////////////
    // YOU WANT TO MODIFY THIS CODE //
    //////////////////////////////////
    (...)
}

func (master *Master) runOperation(remoteWorker *RemoteWorker, operation *Operation, wg *sync.WaitGroup) {
    (...)
    //////////////////////////////////
    // YOU WANT TO MODIFY THIS CODE //
    //////////////////////////////////
    (...)
}
```

É importante observar que o código de runOperation é sempre executado em uma goroutine distinta:

```go
master_scheduler.go
func (master *Master) schedule(task *Task, proc string, filePathChan chan string) {
    (...)
    for filePath = range filePathChan {
        (...)
        go master.runOperation(worker, operation, &wg)
    }
    (...)
}
```

É então necessário desenvolver uma forma de compartilhar a informação de que uma Operation não foi concluída corretamente com a execução do método schedule (o responsável por alocar as operações nos workers). O canal filePathChan vai retornar todos os arquivos criados pelo splitData apenas uma vez. Esse canal é controlado por um método externo e reutilizá-lo pode causar problemas (por exemplo, ele pode ser encerrado pelo código externo, e ao tentar escrever nele, uma chamada de panic será feita como mostra o código a seguir https://play.golang.org/p/KU7MLrFQSx).

Uma solução completamente funcional pode ser obtida em poucas linhas de código (~12 no total), desde que haja um bom entendimento do funcionamento da concorrência em Go (Goroutines, Channels, WaitGroups, Mutexes).

### Execução Final

Um arquivo de teste é incluído junto ao código. Para executá-lo basta iniciar o Master com o seguinte comando:
```bash
wordcount$ go run main.go data.go wordcount.go -mode distributed -type master -file files/teste.txt -chunksize 16 -reducejobs 3
```

Conectando pelo menos um worker a este Master, a operação deve ser concluída desde que este único worker não falhe. Para testar a execução com falhas, basta executar um Worker com -fail e após a falha executar um outro worker normal. A saída não deve mudar.

A saída final deve estar em result/result-final. A ordem pode variar (essa aqui está em ordem alfabetica)

> {"Key":"a","Value":"2"}  
> {"Key":"apenas","Value":"1"}  
> {"Key":"contagem","Value":"1"}  
> {"Key":"correto","Value":"1"}  
> {"Key":"da","Value":"1"}  
> {"Key":"de","Value":"1"}  
> {"Key":"deve","Value":"1"}  
> {"Key":"e","Value":"1"}  
> {"Key":"esta","Value":"1"}  
> {"Key":"exemplo","Value":"1"}  
> {"Key":"funcionamento","Value":"1"}  
> {"Key":"o","Value":"1"}  
> {"Key":"ocorrencia","Value":"1"}  
> {"Key":"ocorrer","Value":"1"}  
> {"Key":"palavra","Value":"1"}  
> {"Key":"palavras","Value":"1"}  
> {"Key":"para","Value":"1"}  
> {"Key":"por","Value":"1"}  
> {"Key":"que","Value":"1"}  
> {"Key":"sendo","Value":"1"}  
> {"Key":"teste","Value":"3"}  
> {"Key":"tres","Value":"1"}  
> {"Key":"ultima","Value":"1"}  
> {"Key":"ver","Value":"1"}  
> {"Key":"vezes","Value":"1"}  

É recomendado que o aluno teste as diversas configurações que incluem um Master e:
 * 1 ou mais Workers normais
 * 1 ou mais Workers com falha