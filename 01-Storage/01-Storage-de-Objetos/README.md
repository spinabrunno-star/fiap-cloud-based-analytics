# 01.1 - Storage de objetos

**Antes de começar, execute os passos abaixo para configurar o ambiente caso não tenha feito isso ainda na aula de HOJE: [Preparando Credenciais](../../01-create-codespaces/Inicio-de-aula.md)**

**Todos os comandos de terminal desse execício devem ser executados no Codespaces que você criou na configuração inicial.**

O Amazon S3 oferece desempenho otimizado para diferentes tamanhos de arquivos, mas requer estratégias específicas para cada caso:

## Arquivos Grandes
Para arquivos grandes, o S3 suporta upload multipart e transfer acceleration, que podem reduzir significativamente o tempo de upload. Testes mostram que a combinação dessas técnicas pode diminuir o tempo de [upload em até 61%](https://aws.amazon.com/blogs/compute/uploading-large-objects-to-amazon-s3-using-multipart-upload-and-transfer-acceleration/). O multipart upload divide arquivos grandes em partes menores, permitindo uploads paralelos e melhorando a eficiência.

## Arquivos Pequenos (1MB)
Para arquivos de aproximadamente 1MB, o S3 ainda oferece bom desempenho, mas pode-se otimizar ajustando configurações como [`max_concurrent_requests` e `multipart_threshold` na AWS CLI](https://www.youtube.com/watch?v=xuiEBO8pkck). Isso permite um melhor controle sobre o número de solicitações simultâneas e o tamanho mínimo para iniciar uploads multipart.

## Arquivos Minúsculos (1KB ou menos)
O desempenho do S3 para arquivos muito pequenos pode ser desafiador devido ao overhead de cada operação. A AWS recomenda agrupar arquivos pequenos em lotes para melhorar a [velocidade de transferência](https://aws.amazon.com/blogs/storage/best-practices-for-accelerating-data-migrations-using-aws-snowball-edge/). Ao usar o Snowball Edge, por exemplo, pode-se utilizar a opção `--metadata snowball-auto-extract=true` para extrair automaticamente os arquivos agrupados ao importá-los para o S3.

Em geral, o S3 escala automaticamente para altas taxas de solicitação, suportando pelo menos 3.500 operações PUT/COPY/POST/DELETE ou 5.500 GET/HEAD por segundo por [prefixo particionado do S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html). Para otimizar ainda mais, considere usar paralelização e múltiplos prefixos para distribuir a carga e aumentar o desempenho geral.


#### Primeiros passos no S3

1. Você irá utilizar o bucket criado no setup da aula anterior. Para verificar se o bucket foi criado, execute o comando abaixo no terminal do Codespaces e verifique se o bucket `base-config-<SEU RM>` foi criado:

```bash
aws s3 ls
```

![](img/s3-1.png)

2. Execute o comando abaixo para popular uma variavel chamada bucket com o nome do bucket que você criou:

```bash
export bucket=$(aws s3 ls | awk '/base-config-/ {print $3; exit}') && echo $bucket
```

3. Entre na pasta correta para executar o exercício:

```bash
cd /workspaces/fiap-cloud-based-analytics/01-Storage/01-Storage-de-Objetos
```

4. Execute os comandos abaixo para baixar 3 arquivos csv que servirão de exemplo para o exercício:

```bash
curl https://perso.telecom-paristech.fr/eagan/class/igr204/data/cereal.csv -o cereal.csv 
curl https://perso.telecom-paristech.fr/eagan/class/igr204/data/cars.csv -o car.csv
curl https://perso.telecom-paristech.fr/eagan/class/igr204/data/factbook.csv -o factbook.csv
```

5. Chegou a hora de enviar os arquivos para o bucket S3. Execute o comando abaixo para enviar os arquivos para o bucket:

```bash
aws s3 cp car.csv s3://$bucket/car/car.csv

aws s3 cp cereal.csv s3://$bucket/cereal/cereal.csv

aws s3 cp factbook.csv s3://$bucket/factbook/factbook.csv

aws s3 cp factbook.csv s3://$bucket/other/factbook.tst
```

<details>
<summary> 
<b>Explicação dos comandos de copia dos arquivos</b>
</summary>
<blockquote>
O comando `aws s3 cp` é uma ferramenta poderosa da AWS CLI (Command Line Interface) utilizada para copiar arquivos entre o sistema de arquivos local e buckets do Amazon S3, ou entre buckets S3. Vamos analisar o comando e suas funcionalidades:

## Sintaxe Básica

```bash
aws s3 cp   [opções]
```

## Funcionalidades Principais

1. **Cópia Local para S3**: 
   ```bash
   aws s3 cp arquivo_local.txt s3://meu-bucket/arquivo.txt
   ```
   Este comando copia um arquivo do sistema local para um bucket S3[1].

2. **Cópia S3 para Local**: 
   ```bash
   aws s3 cp s3://meu-bucket/arquivo.txt arquivo_local.txt
   ```
   Realiza o download de um arquivo do S3 para o sistema local[1].

3. **Cópia entre Buckets S3**: 
   ```bash
   aws s3 cp s3://bucket-origem/arquivo.txt s3://bucket-destino/arquivo.txt
   ```
   Copia um objeto de um bucket S3 para outro[1].

4. **Cópia Recursiva**: 
   ```bash
   aws s3 cp diretorio/ s3://meu-bucket/diretorio --recursive
   ```
   Copia recursivamente todos os arquivos de um diretório local para um bucket S3[1].

5. **Definição de ACL**: 
   ```bash
   aws s3 cp arquivo.txt s3://meu-bucket/ --acl public-read
   ```
   Copia um arquivo e define permissões de acesso específicas[3].

6. **Exclusão de Arquivos**: 
   ```bash
   aws s3 cp diretorio/ s3://meu-bucket/ --recursive --exclude "*.jpg"
   ```
   Copia todos os arquivos, exceto os com extensão .jpg[1].

7. **Definição de Classe de Armazenamento**: 
   ```bash
   aws s3 cp arquivo.txt s3://meu-bucket/ --storage-class REDUCED_REDUNDANCY
   ```
   Copia um arquivo especificando uma classe de armazenamento não padrão[3].

8. **Cópia com Expiração**: 
   ```bash
   aws s3 cp arquivo.txt s3://meu-bucket/arquivo.txt --expires 2025-03-17T20:30:00Z
   ```
   Copia um arquivo definindo uma data de expiração[1].

9. **Cópia com Permissões Específicas**: 
   ```bash
   aws s3 cp arquivo.txt s3://meu-bucket/ --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers
   ```
   Copia um arquivo concedendo permissões de leitura específicas[1].

O comando `aws s3 cp` é extremamente versátil, permitindo diversas operações de cópia com opções para controle fino de permissões, classes de armazenamento e outros atributos dos objetos S3.

Citations:
[1] https://docs.aws.amazon.com/pt_br/cli/v1/userguide/cli_s3_code_examples.html
[2] https://repost.aws/pt/knowledge-center/copy-s3-objects-account
[3] https://docs.aws.amazon.com/pt_br/cli/v1/userguide/cli-services-s3-commands.html
[4] https://pt.stackoverflow.com/questions/360696/como-especificar-um-espa%C3%A7o-em-branco-no-aws-cli-s3
[5] https://www.uminventorqualquer.com.br/s3-04-operando-o-aws-s3-atraves-da-command-line-linha-de-comando-do-aws-cli/
[6] https://cybernetus.com/post/upload-e-download-de-arquivos-no-servico-s3-amazon-na-linha-de-comando/
[7] https://repost.aws/pt/knowledge-center/move-objects-s3-bucket
[8] https://docs.aws.amazon.com/pt_br/cli/v1/userguide/cli-services-s3.html

</blockquote>
</details>

6. Verifique se os arquivos foram enviados corretamente para o bucket. Para isso acesse o [painel do S3](https://us-east-1.console.aws.amazon.com/s3/buckets?region=us-east-1&bucketType=general) no console da AWS e verifique se os arquivos estão lá. Clique no nome do bucket que você criou e verifique se os arquivos estão lá.

![](img/s3-2.png)

#### Perfornando operações no S3

1. Execute os comandos abaixo para configurar o CLI da AWS de acordo com como quer trabalhar com o S3:

```bash
aws configure set default.s3.max_concurrent_requests 1  
aws configure set default.s3.multipart_threshold 64MB  
aws configure set default.s3.multipart_chunksize 16MB
```

<details>
<summary>   
<b>Explicação dos comandos de configuração do CLI da AWS</b>
</summary>
<blockquote>

Vamos analisar cada linha do comando fornecido:

1. `aws configure set default.s3.max_concurrent_requests 1`

   Este comando configura o número máximo de solicitações concorrentes que a AWS CLI fará ao Amazon S3 para 1. Isso significa que apenas uma solicitação será processada por vez, efetivamente desativando o paralelismo[1][3].

2. `aws configure set default.s3.multipart_threshold 64MB`

   Esta linha define o limite de tamanho para iniciar uploads multipart em 64 MB. Arquivos maiores que 64 MB serão divididos em partes menores para upload[4][5].

3. `aws configure set default.s3.multipart_chunksize 16MB`

   Este comando configura o tamanho de cada parte em uploads multipart para 16 MB. Quando um arquivo excede o `multipart_threshold`, ele será dividido em partes de 16 MB cada[4][5].

Essas configurações afetam o comportamento da AWS CLI ao interagir com o Amazon S3, especialmente para operações de upload e download de arquivos grandes. A primeira linha limita significativamente o paralelismo, enquanto as duas últimas otimizam o processo de upload multipart para arquivos grandes.

Citações:
[1] https://stackoverflow.com/questions/47641503/aws-cli-max-concurrent-requests-not-going-beyond-a-point
[2] https://til.codes/tuning-concurrency-settings-for-aws-s3-cli/
[3] https://repost.aws/questions/QUUdZKwva0TO6EwrPcFLtErA/s3-transfer-rates-capped-at-2-8mb-s-how-can-i-speed-this-up
[4] https://docs.aws.amazon.com/es_es/cli/latest/topic/s3-config.html
[5] https://docs.aws.amazon.com/cli/latest/topic/s3-config.html
[6] https://repost.aws/knowledge-center/s3-improve-transfer-sync-command
</blockquote>
</details>

2. execute os comandos abaixo para criar a pasta no lugar correto e entrar nela:

```bash
cd /workspaces/
mkdir s3-performance
cd s3-performance
export bucket=$(aws s3 ls | awk '/base-config-/ {print $3; exit}') && echo $bucket
```

3. Crie um arquivo de 5Gb para testar o upload de arquivos grandes, esse comando pode demorar um pouco para ser executado:

``` bash
dd if=/dev/zero of=5GB.file count=5120 bs=1M
```

4. Execute o comando abaixo para enviar o arquivo para o bucket:

``` bash
time aws s3 cp 5GB.file s3://${bucket}/upload1.test    
```

5. Aumente a concorrencia de upload para 2 e teste o upload do mesmo arquivo. Observe que o tempo de upload vai diminuir:

```bash
aws configure set default.s3.max_concurrent_requests 2  
time aws s3 cp 5GB.file s3://${bucket}/upload2.test
```

6. Aumente a concorrencia de upload para 10 e teste o upload do mesmo arquivo. Observe agora que o tempo de upload vai diminuir ainda mais, porém não é 5x mais rápido que o upload com concorrencia 2:

```bash
aws configure set default.s3.max_concurrent_requests 10  
time aws s3 cp 5GB.file s3://${bucket}/upload3.test  
```

7. Retorno a concorrencia de upload para 1 para seguir com os demais exercícios:

```bash
aws configure set default.s3.max_concurrent_requests 1
```

8. Agora você irá criar um arquivo de 1GB para enviar 5 arquivos paralelamente ao bucket. Execute o comando abaixo para criar o arquivo:

```bash
dd if=/dev/zero of=1GB.file count=1024 bs=1M
```

9. Instale o pacote `parallel` para poder enviar os arquivos paralelamente:

```bash
sudo apt update -y && sudo apt-get install parallel -y
```

10. Utilize o comando abaixo para enviar os arquivos para o bucket:

```bash
time seq 1 5 | parallel --will-cite -j 5 aws s3 cp 1GB.file s3://${bucket}/parallel/object{}.test
```
<details>
<summary>   
<b>Explicação do upload com comando parallel</b>
</summary>
<blockquote>

O comando apresentado realiza uploads paralelos de um arquivo de 1 GB para o Amazon S3 usando otimizações de paralelismo. Vamos decompô-lo:

```bash
time seq 1 5 | parallel --will-cite -j 5 aws s3 cp 1GB.file s3://${bucket}/parallel/object{}.test
```

### Componentes do comando:

1. **`time`**  
   Mede o tempo total de execução do pipeline de comandos[4][8].

2. **`seq 1 5`**  
   Gera uma sequência numérica (1 a 5) para paralelização[4].

3. **`parallel --will-cite -j 5`**  
   - `--will-cite`: Remove mensagem de citação do GNU Parallel[4]  
   - `-j 5`: Define 5 jobs paralelos simultâneos[4][6]  
   *(O AWS CLI internamente já usa até 10 threads por transferência[6][8])*

4. **`aws s3 cp`**  
   Comando de cópia da AWS CLI que:  
   - Usa upload multipart automático para arquivos > 64MB[8][11]  
   - Divide o arquivo em partes de 16MB por padrão[8]  
   - Gerencia automaticamente retries e concorrência[6][8]

5. **Estrutura de destino no S3**  
   `s3://${bucket}/parallel/object{}.test`  
   - `{}` substituído por valores da sequência (1-5)  
   - Cria 5 objetos distintos: `object1.test` a `object5.test`

### Fluxo de operação:
1. Gera 5 tarefas de upload paralelas[4][6]  
2. Cada instância do `aws s3 cp`:  
   - Inicia conexão TCP separada[4]  
   - Divide o arquivo 1GB em ~64 partes de 16MB[8][11]  
   - Upload concorrente das partes via threads[6][8]  
3. Paralelismo em dois níveis:  
   - 5 processos independentes (GNU Parallel)  
   - ~10 threads por processo (AWS CLI interno)[6][8]

### Otimizações relevantes:
- **Eficiência em redes de alta latência**: Paralelismo compensa atrasos de rede[4][7]  
- **Utilização de recursos**: Usa múltiplos núcleos de CPU e conexões[4][8]  
- **Throughput agregado**: Até 5 GB/s teóricos (considerando 1 Gbps por conexão)[4][9]

### Melhores práticas relacionadas:
1. Para >1000 arquivos, `aws s3 sync` é mais eficiente[4][10]  
2. Ferramentas especializadas como `s5cmd` atingem maior paralelismo[5]  
3. Ajuste de parâmetros via `aws configure set` pode melhorar performance[8][11]

Citações:
[1] https://docs.aws.amazon.com/redshift/latest/dg/t_loading-tables-from-s3.html
[2] https://docs.aws.amazon.com/prescriptive-guidance/latest/patterns/run-parallel-reads-of-s3-objects-by-using-python-in-an-aws-lambda-function.html
[3] https://stackoverflow.com/questions/62972236/what-is-the-best-way-to-transfer-large-files-using-aws-s3-cp-command-of-awscli
[4] https://netdevops.me/2018/uploading-multiple-files-to-aws-s3-in-parallel/
[5] https://github.com/peak/s5cmd
[6] https://github.com/aws/aws-cli/issues/907
[7] https://stackoverflow.com/questions/59859165/fastest-way-to-copy-s3-files-without-exact-sync
[8] https://docs.aws.amazon.com/cli/latest/topic/s3-config.html
[9] https://docs.aws.amazon.com/prescriptive-guidance/latest/patterns/copy-data-from-an-s3-bucket-to-another-account-and-region-by-using-the-aws-cli.html
[10] https://github.com/aws/aws-cli/issues/4340
[11] https://docs.aws.amazon.com/cli/latest/reference/s3/cp.html


</blockquote>
</details>

11. Delete o arquivo de 1GB que você criou:

```bash
rm 1GB.file
```

#### Performannce com arquivos menores

1. Você vai criar 2000 arquivos de 1MB para enviar para o bucket. Execute o comando abaixo para criar os arquivos:

```bash
cd /workspaces/s3-performance
mkdir sync
seq -w 1 2000 | xargs -n1 -I% sh -c 'dd if=/dev/zero of=sync/file.% bs=1M count=1'
```

2. Sete a concorrencia de upload para 1 e execute o comando abaixo para enviar os arquivos para o bucket via sync:

```bash
aws configure set default.s3.max_concurrent_requests 1
export bucket=$(aws s3 ls | awk '/base-config-/ {print $3; exit}') && echo $bucket  
time aws s3 sync sync/ s3://${bucket}/sync1/
```

<details>
<summary> 
<b>Explicação do upload com comando sync</b>
</summary>
<blockquote>
O comando `aws s3 sync` é uma ferramenta poderosa da AWS CLI (Command Line Interface) utilizada para sincronizar conteúdo entre um diretório local e um bucket do Amazon S3, ou entre dois buckets S3. Suas principais características e funcionalidades são:

## Sincronização Bidirecional

O `aws s3 sync` pode ser usado para:

1. Copiar arquivos de um diretório local para um bucket S3
2. Baixar arquivos de um bucket S3 para um diretório local
3. Sincronizar conteúdo entre dois buckets S3

## Funcionamento

- O comando compara os objetos presentes na origem e no destino[6].
- Copia apenas os arquivos que estão faltando ou que foram modificados[1].
- Por padrão, não exclui arquivos no destino que não existem na origem[3].

## Opções Importantes

1. **--delete**: Remove arquivos no destino que não existem na origem[3].
2. **--exclude** e **--include**: Permitem filtrar quais arquivos serão sincronizados[3].
3. **--storage-class**: Define a classe de armazenamento para os objetos no S3[7].
4. **--acl**: Configura as permissões de acesso para os objetos copiados[3].

## Casos de Uso

- Backups de diretórios locais para o S3[7].
- Manutenção de cópias atualizadas de dados entre ambientes[1].
- Migração de dados entre buckets S3[5].

## Considerações de Desempenho

- Para grandes volumes de dados, é possível executar múltiplas instâncias do comando em paralelo[6].
- O comando analisa todos os arquivos na origem, mesmo ao usar filtros, o que pode impactar o desempenho em buckets muito grandes[6].

## Limitações

- Pode ser ineficiente para buckets com milhões de objetos, onde operações em lote do S3 são recomendadas[5].
- Ao sincronizar buckets com versionamento, apenas a versão mais recente de cada objeto é copiada[5].

O `aws s3 sync` é uma ferramenta versátil para manter dados sincronizados entre diferentes locais, oferecendo flexibilidade e controle sobre o processo de sincronização[1][3][6].

Citações:
[1] https://www.tabnews.com.br/filipedeschamps/tutorial-como-sincronizar-espelhar-arquivos-com-um-bucket-s3-usando-o-cli-aws
[2] https://www.datacamp.com/pt/tutorial/aws-s3-sync
[3] https://docs.aws.amazon.com/pt_br/cli/v1/userguide/cli-services-s3-commands.html
[4] https://docs.aws.amazon.com/cli/latest/reference/s3/sync.html
[5] https://repost.aws/pt/knowledge-center/move-objects-s3-bucket
[6] https://repost.aws/pt/knowledge-center/s3-improve-transfer-sync-command
[7] https://www.uminventorqualquer.com.br/s3-04-operando-o-aws-s3-atraves-da-command-line-linha-de-comando-do-aws-cli/
[8] https://www.reddit.com/r/aws/comments/d82tgg/aws_cli_s3_sync_command/?tl=pt-br

</blockquote>
</details>

3. Aumente a concorrencia de upload para 10 e execute o comando abaixo para enviar os arquivos para o bucket via sync:

```bash
aws configure set default.s3.max_concurrent_requests 10  
time aws s3 sync sync/ s3://${bucket}/sync2/
```

4. Delete os arquivos que você criou:

```bash
rm -rf sync
```

5. Você agora vai testar com arquivos ainda menores. Cada um com o tamanho de 1KB. Execute o comando abaixo para criar o arquivo:

```bash
seq 1 500 > object_ids  
cat object_ids 
dd if=/dev/zero of=1KB.file count=1 bs=1K
aws configure set default.s3.max_concurrent_requests 1 
```

6. Execute o comando abaixo para enviar os arquivos para o bucket:

```bash
time parallel --will-cite -a object_ids -j 5 aws s3 cp 1KB.file s3://${bucket}/run1/{}
```

<details>
<summary>
<b>Explicação do upload com comando parallel</b>
</summary>
<blockquote>

O comando fornecido realiza uploads simultâneos de um arquivo pequeno (1 KB) para um bucket do Amazon S3, utilizando o GNU Parallel para paralelismo. Vamos detalhar cada parte do comando:

```bash
time parallel --will-cite -a object_ids -j 5 aws s3 cp 1KB.file s3://${bucket}/run1/{}
```

### Explicação detalhada:

#### 1. **`time`**
   - Mede o tempo total de execução do comando.
   - Exibe quanto tempo foi necessário para completar todos os uploads.

#### 2. **`parallel`**
   - Ferramenta que permite executar comandos em paralelo, otimizando o uso de CPU e rede.
   - No contexto deste comando, ela executa múltiplos uploads simultaneamente.

#### 3. **`--will-cite`**
   - Remove a mensagem de aviso do GNU Parallel sobre citação. É apenas uma formalidade para uso ético da ferramenta.

#### 4. **`-a object_ids`**
   - Informa ao `parallel` que ele deve ler os identificadores (IDs) de objetos a partir do arquivo chamado `object_ids`.
   - Cada linha do arquivo `object_ids` contém um identificador único que será usado para nomear os objetos no S3.

#### 5. **`-j 5`**
   - Define o número máximo de tarefas (jobs) que serão executadas simultaneamente.
   - Neste caso, até 5 uploads serão realizados ao mesmo tempo.

#### 6. **`aws s3 cp 1KB.file s3://${bucket}/run1/{}`**
   - Este é o comando que será executado em paralelo:
     - `aws s3 cp`: Comando da AWS CLI para copiar arquivos para o S3.
     - `1KB.file`: Arquivo local de 1 KB que será enviado.
     - `s3://${bucket}/run1/{}`: Caminho de destino no bucket S3.
       - `${bucket}`: Nome do bucket, armazenado em uma variável de ambiente.
       - `{}`: Substituído por cada valor lido do arquivo `object_ids`.

### Fluxo de Execução:
1. O GNU Parallel lê os IDs dos objetos no arquivo `object_ids`.
2. Para cada ID, ele substitui `{}` no caminho S3 pelo ID correspondente.
3. Executa até 5 uploads simultaneamente (por causa da flag `-j 5`).
4. Cada tarefa copia o arquivo `1KB.file` para o bucket S3 com um nome único baseado nos IDs.

### Exemplo de Funcionamento:
Se o arquivo `object_ids` contiver os seguintes valores:
```
file1
file2
file3
file4
file5
```

O comando resultará na execução paralela dos seguintes uploads:
```bash
aws s3 cp 1KB.file s3://${bucket}/run1/file1
aws s3 cp 1KB.file s3://${bucket}/run1/file2
aws s3 cp 1KB.file s3://${bucket}/run1/file3
aws s3 cp 1KB.file s3://${bucket}/run1/file4
aws s3 cp 1KB.file s3://${bucket}/run1/file5
```

### Benefícios:
- **Paralelismo:** Aumenta a velocidade total dos uploads ao utilizar várias conexões simultâneas.
- **Escalabilidade:** Ideal para grandes volumes de pequenos arquivos, onde o overhead de conexões individuais pode ser significativo.
- **Eficiência:** Aproveita melhor os recursos da rede e da CPU.

### Considerações:
- **Tamanho dos arquivos:** Para arquivos maiores (acima de 100 MB), seria mais eficiente usar upload multipart automático com `aws s3 cp`, que já divide os arquivos em partes menores e faz upload paralelo internamente[2][5].
- **Limites da AWS:** Certifique-se de não exceder os limites de taxa de requisição ou largura de banda da sua conta AWS.
- **Cuidado com nomes duplicados:** IDs duplicados no arquivo `object_ids` sobrescreverão objetos existentes no bucket.

Este comando é especialmente útil para cenários onde há muitos arquivos pequenos a serem enviados rapidamente para o Amazon S3.

Citações:
[1] https://github.com/mishudark/s3-parallel-put
[2] https://repost.aws/knowledge-center/s3-multipart-upload-cli
[3] https://stackoverflow.com/questions/26934506/uploading-files-to-s3-using-s3cmd-in-parallel
[4] https://www.reddit.com/r/aws/comments/1fbeed/parallel_multipart_s3_uploads/
[5] https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
[6] https://docs.aws.amazon.com/AmazonS3/latest/userguide/upload-objects.html
[7] https://netdevops.me/2018/uploading-multiple-files-to-aws-s3-in-parallel/
[8] https://parallelworks.com/docs/storage/transferring-data/aws-s3-buckets

</blockquote>
</details>

#### Comparando opções de cópia de arquivos no S3

1. Execute o comandos abaixo e compare o tempo de execução de cada um dos comandos:

```bash
aws configure set default.s3.max_concurrent_requests 1 
time (aws s3 cp s3://$bucket/upload1.test 5GB.file; aws s3 cp 5GB.file s3://$bucket/copy/5GB.file)  
time aws s3api copy-object --copy-source $bucket/upload1.test --bucket $bucket --key copy/5GB-2.file
time aws s3 cp s3://$bucket/upload1.test s3://$bucket/copy/5GB-3.file
```

<details>
<summary>
<b>Explicação dos comandos de cópia de arquivos no S3</b>
</summary>
<blockquote>

Vamos comparar as três abordagens para copiar um arquivo de 5 GB no Amazon S3, utilizando a documentação oficial da AWS e resultados de pesquisa relevantes:

---

## **1. `aws s3 cp` com Download + Reupload**

```bash
time (aws s3 cp s3://$bucket/upload1.test 5GB.file; aws s3 cp 5GB.file s3://$bucket/copy/5GB.file)
```


### **Prós:**

- **Simplicidade:** Não requer conhecimento de APIs específicas (operações de alto nível).
- **Multipart automático:** Divide arquivos >64MB em partes (padrão: 16MB) para upload paralelo.
- **Tolerância a falhas:** Retentativas automáticas em caso de erros de rede.


### **Contras:**

- **Latência dupla:** Transferência redundante (download + upload) pela rede, aumentando o tempo total.
- **Custo:** Cobrança por transferência de dados de saída (egress) e entrada (ingress) [AWS Pricing].
- **Uso de recursos locais:** Consome largura de banda e armazenamento temporário no cliente.

---

## **2. `aws s3api copy-object` Server-Side**

```bash
time aws s3api copy-object --copy-source $bucket/upload1.test --bucket $bucket --key copy/5GB-2.file
```


### **Prós:**

- **Cópia direta no S3:** Operação server-side sem transferência pela rede (latência mínima).
- **Sem custo de transferência:** Operações server-side na mesma região não geram cobrança de egress [AWS Docs].
- **Atomicidade:** Operação única e atômica, sem estágios intermediários.


### **Contras:**

- **Limitação de tamanho:** Suporta apenas objetos ≤5 GB (requer multipart para arquivos maiores).
- **Menos automatizado:** Não gerencia multipart automaticamente para objetos grandes.
- **Sem progresso visível:** Não exibe feedback durante a operação (depende de logs do S3).

---

## **3. `aws s3 cp` Server-Side Copy**

```bash
time aws s3 cp s3://$bucket/upload1.test s3://$bucket/copy/5GB-3.file
```


### **Prós:**

- **Cópia otimizada:** Detecta automaticamente cópias server-side quando origem/destino estão no S3.
- **Multipart integrado:** Gerencia automaticamente cópias de arquivos >5 GB via multipart upload.
- **Progresso em tempo real:** Exibe status de transferência durante a execução.


### **Contras:**

- **Dependência de região:** Cópias entre regiões podem acionar transferência via cliente [AWS Docs].
- **Overhead de metadados:** Verifica propriedades do objeto antes da cópia (checksum, ACLs).

---

## **Comparação Técnica**

| Critério | `aws s3 cp` (2 etapas) | `s3api copy-object` | `aws s3 cp` server-side |
| :-- | :-- | :-- | :-- |
| **Tempo** | Alto (2 transferências) | Baixo (server-side) | Moderado (verificações) |
| **Custo** | Alto (egress + ingress) | Zero (server-side) | Zero (server-side) |
| **Tamanho máximo** | 5 TB (com multipart) | 5 GB | 5 TB (com multipart) |
| **Controle de progresso** | Sim | Não | Sim |
| **Uso recomendado** | Cross-region/backup local | Cópias rápidas ≤5 GB | Cópias server-side ≥5 GB |

---

## **Recomendação da AWS**

Para arquivos ≤5 GB em **mesma região**, use `aws s3api copy-object` ou `aws s3 cp` server-side. Para arquivos >5 GB, **sempre prefira `aws s3 cp` server-side**, que automatiza multipart upload sem transferência redundante. Evite abordagens de download/reupload devido a custos e latência.

</blockquote>
</details>

#### Conclusão e deletando dados

1. Após realizar todos os testes, delete os arquivos que você criou no bucket e localmente:

```bash
aws s3 rm s3://${bucket}/ --recursive
cd /workspaces
rm -rf s3-performance
```