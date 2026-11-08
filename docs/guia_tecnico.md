# Guia técnico de operação

## O fluxo em cinco passos

1. **Descobrir e coletar:** o catálogo informa todos os meses, locais, variáveis e atividades. A API é consultada em lotes anuais de até 40 mil células. Timeout, retentativas e conferência das chaves evitam aceitar uma coleta incompleta.
2. **Preservar a Bronze:** guardar JSON original gzip, URL, horário UTC e SHA-256. O manifesto só é publicado quando todos os lotes passam. Uma retomada verifica os lotes existentes antes de reutilizá-los.
3. **Tratar a Silver:** processar um lote por vez com schema explícito, nomes legíveis, códigos STRING, datas e Decimal(38,9). Duplicatas iguais são consolidadas; conflitos e valores inválidos impedem concluir a etapa e geram relatório de qualidade.
4. **Construir e publicar a Gold:** carregar os Parquet de 2025 no staging, calcular uma candidata e conferir chaves, contagem e cálculos. Somente então substituir atomicamente a tabela final.
5. **Conferir o ranking:** executar o SQL Top 3 na Gold publicada e registrar os IDs dos jobs. O Airflow liga essas etapas; XCom leva apenas referências pequenas, nunca datasets ou credenciais.

Python/PyArrow atende ao volume medido sem exigir Spark. LocalExecutor dispensa Redis e workers distribuídos. PostgreSQL guarda metadados do Airflow; os dados industriais ficam no GCS/BigQuery.

## Regras analíticas

A coleta contém Brasil, Nordeste e 17 UFs, seis variáveis e 27 categorias. A análise usa somente **UFs (N3), 2025, variável 12606 e as 24 atividades de transformação** da classificação 544. Os agregados de indústria geral, extrativas e transformação ficam fora da média.

| Valor recebido | Tratamento |
| --- | --- |
| Número, inclusive zero explícito | Decimal; entra nos cálculos |
| `-` | NULL com status `sem_indice`; original preservado |
| `..`, `...`, `X`/`x` ou vazio | NULL com status próprio de disponibilidade |
| Marcador desconhecido ou dado inválido | Rejeição; não publicar silenciosamente |

Na PIM, um traço também aparece em séries não divulgadas. As notas metodológicas e tabelas do [fascículo oficial de janeiro de 2025](https://ftp.ibge.gov.br/Industrias_Extrativas_e_de_Transformacao/Pesquisa_Industrial_Mensal_Producao_Fisica/Fasciculos/Fasciculo_Indicadores_IBGE_Regional/2025/pim-pf-regional_202501.pdf) fundamentam a preservação desse sinal como ausência de índice numérico. Transformá-lo automaticamente em zero alteraria média e ranking.

A média é aritmética simples dos índices disponíveis na UF/mês. Para índices 80, 100 e 120, a média é 100 e as diferenças são −20, 0 e 20. Isso não equivale ao agregado oficial ponderado da indústria. A Gold informa a quantidade de atividades válidas usada no denominador.

O Top 3 usa `ROW_NUMBER` por UF/mês, com índice decrescente e código numérico crescente para desempatar. Nulos não competem; grupos com menos de três índices retornam os disponíveis. A Gold mantém as 4.896 combinações, inclusive as 2.892 células sem índice.

## Configuração GCP

Prepare um projeto próprio com dois buckets e um dataset `pim`, todos na mesma região, sugerida `southamerica-east1`. Use acesso uniforme nos buckets e prevenção de acesso público. As APIs de Storage, BigQuery e, para impersonação, IAM Service Account Credentials devem estar disponíveis.

| Escopo | Acesso do runtime |
| --- | --- |
| Projeto | `roles/bigquery.jobUser` e, quando necessário para quota ADC, `roles/serviceusage.serviceUsageConsumer` |
| Dataset `pim` | `roles/bigquery.dataEditor` |
| Dois buckets | `roles/storage.objectCreator` e `roles/storage.objectViewer` |
| Bucket Silver | Papel customizado com `storage.buckets.get`, necessário para a carga BigQuery |
| Conta de runtime | Usuário que impersona recebe `roles/iam.serviceAccountTokenCreator` somente nessa conta |

Gere [ADC por impersonação](https://docs.cloud.google.com/docs/authentication/use-service-account-impersonation) fora do repositório, sem chave privada. Preencha `GOOGLE_ADC_PATH` em `.env`; o Compose monta esse arquivo somente leitura. A identidade de runtime não precisa ser Owner ou Editor do projeto. O código utiliza os recursos preparados e não cria infraestrutura nem concede IAM.

Os destinos são configurados por `.env`: um projeto GCP, dois buckets para Bronze/Silver e a tabela `pim.pim_transformacao_2025`. A configuração permite executar o pipeline em outro projeto GCP.

## Governança no BigQuery

| Padrão de tabela | Finalidade |
| --- | --- |
| `stg_pim_sidra_2025_<execucao>` | Carga Silver de 2025; tabela auxiliar de entrada |
| `val_pim_transformacao_2025_<execucao>` | Resultado candidato, conferido antes da publicação |
| `pim_transformacao_2025` | Gold oficial para consumo analítico |

O sufixo das auxiliares é um identificador determinístico da coleta: evita colisões entre execuções e permite retomada. O `run_id` completo consta da descrição e dos dados. Após cada publicação bem-sucedida, são mantidos somente os pares da execução atual e da anterior, sem expiração por prazo. As labels `execucao_atual` e `execucao_anterior` da Gold identificam esses pares. Uma nova publicação remove os pares mais antigos; repetir a mesma coleta preserva a anterior. Auxiliares de execuções ainda não publicadas expiram em dois dias.

As colunas usam `snake_case`, sem acentos: nomes de negócio em português, com sufixos como `_codigo` e `_nome`, e campos técnicos de rastreabilidade como `run_id`, `source_uri` e `source_sha256`. Os identificadores da Silver são preservados no staging para manter o vínculo direto com a origem.

`COLUMN_DESCRIPTIONS`, em `loading.py`, é o dicionário versionado das colunas. Ele documenta significado, unidade, marcadores e regras de cálculo. Cada tabela recebe descrição e labels de fonte, camada, ano e classificação pública. As cargas reaplicam os metadados; a publicação atômica da Gold já inclui as descrições no próprio DDL. Uma coluna nova sem definição impede a publicação até atualizar o dicionário.

## Repetição, revisões e falhas

- **Mesmo run_id:** retomar a mesma coleta, conferindo conteúdo e hashes. Bronze/Silver existentes só são aceitas se os bytes coincidirem.
- **Nova coleta mensal:** reler o histórico para capturar revisões antigas; cada execução preserva seus objetos.
- **Publicação:** substituir a Gold validada em vez de acrescentar linhas. Após publicar, manter staging/candidata somente da execução atual e da anterior. A limpeza não altera a Gold nem tabelas fora dos padrões do pipeline.
- **Recibos:** `results/<run_id>/<publish_job_id>.json` preserva os diferentes jobs de cada publicação.
- **Falha antes de publicar:** a Gold anterior permanece. Não há transação única entre GCS, BigQuery e Airflow; uma falha posterior pode ser reconciliada pelos job IDs.

Um reprocessamento parcial exige um baseline Silver completo. Exemplo local, após a coleta `carga_completa` do README:

```powershell
.venv\Scripts\python.exe -m pim_pipeline.pipeline --run-id revisao_janeiro --start 202501 --end 202501 --baseline-run-id carga_completa
```

Janeiro é substituído, os demais meses são preservados e a Gold é recalculada. Na execução manual da DAG, os mesmos campos são `start`, `end` e `baseline_run_id`.

## O que foi comprovado

As execuções reais gravaram 914.166 observações no GCS, publicaram 4.896 linhas na Gold e retornaram 612 linhas no Top 3. A Gold coincidiu integralmente com a referência Decimal. A repetição manteve os dados e as gerações dos objetos GCS. As execuções manual e agendada da DAG terminaram com as oito tarefas em sucesso; os metadados permaneceram após reiniciar o PostgreSQL.

`docs/evidencias/resumo.json` identifica os jobs e resultados. Os 75 testes cobrem também HTTP, cobertura, marcadores, duplicatas, precisão, empates, retomada, backfill e falhas antes da publicação. Esses cenários de falha usam fixtures; as evidências os distinguem da execução real.
