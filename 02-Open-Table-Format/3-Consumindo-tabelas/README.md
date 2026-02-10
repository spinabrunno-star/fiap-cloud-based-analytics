**Antes de começar, execute os passos abaixo para configurar o ambiente caso não tenha feito isso ainda na aula de HOJE: [Preparando Credenciais](../../00-create-codespaces/Inicio-de-aula.md)**
Neste laboratório, você explorará como usar o Amazon Athena para consultar tabelas Iceberg.

Observe que o Amazon Athena fornece suporte integrado para o Apache Iceberg, para que você possa ler e gravar em tabelas Iceberg sem adicionar nenhuma dependência ou configuração adicional. Isso é válido para Iceberg [tabelas v2]([https://iceberg.apache.org/spec/\#version\-2\-row\-level\-deletes](https://iceberg.apache.org/spec/#version-2-row-level-deletes)).

## Principais pontos de aprendizagem:

* Consultando tabelas Iceberg usando Amazon Athena
* Criando visualizações com tabelas Iceberg

Você consultará as tabelas `web_sales_iceberg` e `customer_iceberg` que você criou como parte dos seus laboratórios Glue ou EMR ou Athena

---

## Consultar tabelas Iceberg

Importante
Esta seção pode ser usada para consultar qualquer uma das tabelas criadas como parte dos laboratórios anteriores.
Se você criar tabelas `web_sales_iceberg` e `customer_iceberg` como parte dos laboratórios Glue, certifique-se de selecionar o banco de dados `glue_iceberg_db` no painel esquerdo do editor antes de executar as consultas, da mesma forma, selecione `emr_iceberg_db` se você criou tabelas como parte dos laboratórios EMR ou `athena_iceberg_db` se você criou tabelas como parte dos laboratórios Athena.

![db_selection](img/select_db_athena.png)

1. Para consultar um conjunto de dados Iceberg, use uma instrução SELECT padrão como a seguinte.

``` sql
SELECT ws_warehouse_sk, count(distinct(ws_order_number)) as num_orders
FROM web_sales_iceberg
WHERE ws_warehouse_sk in (5,6,10,11)
GROUP BY ws_warehouse_sk
```

Observação: as consultas seguem o Apache Iceberg [especificação de formato v2](https://iceberg.apache.org/spec/#format-versioning). Caso a consulta seja executada em uma tabela que usou `merge-on-read` (por exemplo, tabelas dentro de `athena_iceberg_db`), os arquivos de exclusão de posição são mesclados com os arquivos de dados imediatamente.

2. Vamos verificar a quantidade de registros presentes em nossa tabela.

``` sql
SELECT count(*)
FROM customer_iceberg
```

3. Usando [EXPLAIN e EXPLAIN ANALYZE](https://docs.aws.amazon.com/athena/latest/ug/athena-explain-statement.html) no Athena


   1. A instrução EXPLAIN mostra o plano de execução lógico ou distribuído de uma instrução SQL especificada, ou valida a instrução SQL. Você pode gerar os resultados em formato de texto ou em um formato de dados para renderizar em um gráfico.

    ``` sql
    EXPLAIN SELECT count(*) FROM customer_iceberg LIMIT 10;
    ```

   2. A instrução EXPLAIN ANALYZE mostra tanto o plano de execução distribuído de uma instrução SQL especificada quanto o custo computacional de cada operação em uma consulta SQL. Você pode gerar os resultados em formato de texto ou JSON.

    ``` sql
    EXPLAIN ANALYZE
    SELECT ws_warehouse_sk, count(distinct(ws_order_number)) as num_orders
    FROM web_sales_iceberg
    WHERE ws_warehouse_sk in (5,6,10,11)
    GROUP BY ws_warehouse_sk
    ```

Consulte os [resultados da instrução EXPLAIN do Athena](https://docs.aws.amazon.com/athena/latest/ug/athena-explain-statement-understanding.html) para obter mais detalhes.

---

## Criando e consultando visualizações com tabelas Iceberg

4. Para criar e consultar visualizações do Athena em tabelas Iceberg, use a instrução CREATE VIEW. Copie a consulta abaixo no editor de consultas e clique em **Executar**.

``` sql
CREATE VIEW total_orders_by_warehouse
AS
SELECT ws_warehouse_sk, count(distinct(ws_order_number)) as num_orders
FROM web_sales_iceberg
WHERE ws_warehouse_sk in (5,6,10,11)
GROUP BY ws_warehouse_sk
```

A consulta deverá ser executada com sucesso e você verá a mensagem "Consulta bem-sucedida" nos resultados da consulta.

5. Para consultar a exibição, copie a consulta abaixo no editor de consultas e clique em **Executar**.

``` sql
SELECT *
FROM total_orders_by_warehouse
```