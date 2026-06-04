# Executando o Pipeline no Kubernetes (Kind ou Minikube)

Este diretório contém os manifestos necessários para implantar e executar a stack do Airflow + Postgres com o pipeline de dados da Taxa SELIC dentro de um cluster Kubernetes local.

---

## 1. Passo a Passo com o Minikube

### Passo 1: Iniciar o Minikube com montagem de volume
Para que os contêineres do Airflow no Kubernetes tenham acesso aos DAGs, tarefas e dados locais do seu repositório:
```bash
# 1. Inicie o cluster
minikube start --driver=docker

# 2. Monte a pasta do seu projeto local para um caminho dentro do Minikube
# Deixe este terminal aberto executando a montagem:
minikube mount "$(pwd):/mnt/data"
```

### Passo 2: Aplicar os volumes e configurações
Em um novo terminal, aplique as configurações e volumes persistentes:
```bash
kubectl apply -f kubernetes/airflow-volumes.yaml
kubectl apply -f kubernetes/airflow-configmap.yaml
```

### Passo 3: Iniciar o Banco de Dados (Postgres)
```bash
kubectl apply -f kubernetes/postgres.yaml
```

### Passo 4: Executar a inicialização do Airflow
O Job de inicialização executa as migrações do banco e cria o usuário administrador `admin` / `admin`:
```bash
kubectl apply -f kubernetes/airflow-init-job.yaml
```
Aguarde o status do job estar concluído:
```bash
kubectl get jobs
```

### Passo 5: Implantar o Webserver e Scheduler
```bash
kubectl apply -f kubernetes/airflow-webserver.yaml
kubectl apply -f kubernetes/airflow-scheduler.yaml
```

### Passo 6: Acessar a Web UI
Abra o túnel ou obtenha a URL de serviço exposta pelo Minikube:
```bash
minikube service airflow-webserver --url
# Ou alternativamente usando port-forward:
kubectl port-forward svc/airflow-webserver 8080:8080
```
*   **URL:** `http://localhost:8080` (caso use port-forward)
*   **Login:** `admin`
*   **Senha:** `admin`

---

## 2. Passo a Passo com o Kind

### Passo 1: Criar o cluster definindo os mounts extras
No Kind, o mapeamento de pastas do host precisa ser feito no momento da criação do cluster por meio de um arquivo de configuração (ex: `kind-config.yaml`):

```yaml
# kind-config.yaml
apiVersion: kind.x-k8s.io/v1alpha4
kind: Cluster
nodes:
- role: control-plane
  extraMounts:
  - hostPath: ./  # Pasta raiz do projeto no Host
    containerPath: /mnt/data  # Pasta onde ficará no nó do Kubernetes
```

Crie o cluster usando o arquivo de configuração:
```bash
kind create cluster --config kind-config.yaml
```

### Passo 2: Aplicar os manifestos
```bash
kubectl apply -f kubernetes/airflow-volumes.yaml
kubectl apply -f kubernetes/airflow-configmap.yaml
kubectl apply -f kubernetes/postgres.yaml
kubectl apply -f kubernetes/airflow-init-job.yaml

# Aguarde o pod do init job completar
kubectl apply -f kubernetes/airflow-webserver.yaml
kubectl apply -f kubernetes/airflow-scheduler.yaml
```

### Passo 3: Expor o acesso local
```bash
kubectl port-forward svc/airflow-webserver 8080:8080
```
Acesse a UI do Airflow em `http://localhost:8080`.
