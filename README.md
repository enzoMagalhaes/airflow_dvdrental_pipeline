
#  Airflow Dvdrental Pipeline

  Airflow dvdrental pipeline é uma pipeline Extract-and-Load desenvolvida inteiramente em Python utilizando a framework de orquestração de dados Apache Airflow que coleta dados de tabelas de um banco transacional e 
  os insere em um banco de analytics:

  ![fluxo](fluxo.png)

  O projeto consiste apenas numa simples e concisa DAG e o script de orquestração da mesma pode ser achado no caminho "dags/populate_analytical_db.py". Segue uma imagem do DAG no Airflow UI:

  ![Dag](dag.png)

  para 


---

### Estrutura de Pastas

    .
    ├── dags
    │     ├── populate_analytical_db.py        # a DAG
    ├── data
    │     ├── dvdrental.sql
    ├── include                             
    │     ├── sql                            # pasta onde se destinam os arquivos .sql utilizados
    │       ├── analytical
    │       │   ├── ..sql files
    │       ├── transactional  
    │       │   ├── ..sql files   
    ├── logs                     
    ├── plugins                             # pasta destinada aos sensors e operators customizados (vazio)
    │     ├── operators
    │     ├── sensors
    ├── tests                               # pasta onde estão os testes automatizados
    │    ├── unit_tests
    │    ├── validation_tests
    │    ├── conftest.py
    ├── docker-compose-dev.yml              # docker-compose de desenvolvimento (com pytest para rodar os testes)
    ├── docker-compose.yml                  # docker-compose
    ├── setup_airflow.sh                    # instala o airflow da maquina e roda ele pela primeira vez
    ├── setup_connections.py                # seta as conexões dos bancos de dados
    ├── .gitignore
    ├── CHALLENGE.md
    └── README.md
