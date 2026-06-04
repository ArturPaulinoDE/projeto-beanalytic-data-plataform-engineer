# deploy-kubernetes.ps1
# Script para automatizar o deploy do cluster Kind com Apache Airflow

$ErrorActionPreference = "Stop"

# Carrega variáveis do arquivo .env
if (Test-Path ".env") {
    Get-Content ".env" | Foreach-Object {
        $line = $_.Trim()
        if ($line -and -not $line.StartsWith("#")) {
            $key, $value = $line -split '=', 2
            if ($key -and $value) {
                $value = $value.Trim().Trim('"').Trim("'")
                [Environment]::SetEnvironmentVariable($key.Trim(), $value, "Process")
            }
        }
    }
}

# Configura encoding de saída para pipes em UTF-8
$OutputEncoding = [System.Text.Encoding]::UTF8

# Helper para ler o manifesto, expandir variáveis de ambiente (${VAR_NAME}) e aplicar
function Apply-K8sManifest {
    param (
        [string]$Path
    )
    if (-not (Test-Path $Path)) {
        Write-Error "Arquivo não encontrado: $Path"
        return
    }
    $content = Get-Content $Path -Raw
    $pattern = '\$\{([a-zA-Z0-9_]+)\}'
    $expanded = [regex]::Replace($content, $pattern, {
        param($match)
        $varName = $match.Groups[1].Value
        $val = [Environment]::GetEnvironmentVariable($varName)
        if ($val -ne $null) {
            return $val
        }
        return $match.Value
    })
    $expanded | kubectl apply -f -
}

Write-Host "=== 1. Verificando Pré-requisitos ===" -ForegroundColor Cyan

# Encerra processos antigos do kubectl para evitar portas bloqueadas
Write-Host "Limpando conexões locais antigas (proxies e port-forwards)..." -ForegroundColor Yellow
Stop-Process -Name kubectl -Force -ErrorAction SilentlyContinue

# Verifica se o Docker está rodando
try {
    $dockerInfo = docker info
    Write-Host "[OK] Docker está rodando." -ForegroundColor Green
} catch {
    Write-Error "Erro: O Docker não está rodando. Por favor, inicie o Docker Desktop e tente novamente."
}

# Verifica se kubectl está instalado
try {
    $kubectlVer = kubectl version --client
    Write-Host "[OK] kubectl está instalado." -ForegroundColor Green
} catch {
    Write-Error "Erro: kubectl não encontrado. Instale com: winget install Kubernetes.cli"
}

# Localiza o executável do Kind
$kindCmd = "kind"
if (-not (Get-Command "kind" -ErrorAction SilentlyContinue)) {
    # Procura no caminho padrão de instalação do Winget se não estiver no PATH global
    $wingetPath = "$env:LOCALAPPDATA\Microsoft\WinGet\Packages"
    $kindSearch = Get-ChildItem -Path $wingetPath -Filter "kind.exe" -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($kindSearch) {
        $kindCmd = $kindSearch.FullName
        Write-Host "[OK] Executável do Kind localizado em: $kindCmd" -ForegroundColor Green
    } else {
        Write-Error "Erro: Kind não encontrado. Instale com: winget install Kubernetes.kind"
    }
} else {
    Write-Host "[OK] Kind está instalado no PATH global." -ForegroundColor Green
}

Write-Host "`n=== 2. Criando o Cluster Kind ===" -ForegroundColor Cyan
Write-Host "Removendo cluster anterior se existir..." -ForegroundColor Yellow
& $kindCmd delete cluster --name selic-pipeline

Write-Host "Criando o cluster 'selic-pipeline'..." -ForegroundColor Yellow
& $kindCmd create cluster --config kind-config.yaml --name selic-pipeline --image kindest/node:v1.29.2

Write-Host "`n=== 3. Carregando Imagem do Airflow ===" -ForegroundColor Cyan
$airflowImage = [Environment]::GetEnvironmentVariable("AIRFLOW_IMAGE")
if (-not $airflowImage) {
    $airflowImage = "apache/airflow:2.8.1-python3.10"
}
Write-Host "Carregando imagem local para dentro do nó do Kind..." -ForegroundColor Yellow
& $kindCmd load docker-image $airflowImage --name selic-pipeline

Write-Host "`n=== 4. Aplicando Configurações e Volumes ===" -ForegroundColor Cyan
Apply-K8sManifest "kubernetes/airflow-volumes.yaml"
Apply-K8sManifest "kubernetes/airflow-configmap.yaml"

Write-Host "`n=== 5. Inicializando o Banco de Dados (Postgres) ===" -ForegroundColor Cyan
Apply-K8sManifest "kubernetes/postgres.yaml"

Write-Host "Aguardando o pod do Postgres ficar pronto..." -ForegroundColor Yellow
kubectl wait --for=condition=ready pod -l app=postgres-db --timeout=120s

Write-Host "`n=== 6. Inicializando as Migrações do Airflow ===" -ForegroundColor Cyan
Apply-K8sManifest "kubernetes/airflow-init-job.yaml"

Write-Host "Aguardando conclusão da inicialização do banco (Job)..." -ForegroundColor Yellow
kubectl wait --for=condition=complete job/airflow-init --timeout=180s

Write-Host "`n=== 7. Implantando Webserver e Scheduler (Alta Disponibilidade) ===" -ForegroundColor Cyan
Apply-K8sManifest "kubernetes/airflow-webserver.yaml"
Apply-K8sManifest "kubernetes/airflow-scheduler.yaml"

Write-Host "`n=== 8. Instalando e Configurando o Kubernetes Dashboard ===" -ForegroundColor Cyan
kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.7.0/aio/deploy/recommended.yaml
Apply-K8sManifest "kubernetes/dashboard-admin.yaml"

Write-Host "Aguardando a inicialização completa do Airflow Webserver..." -ForegroundColor Yellow
kubectl rollout status deployment/airflow-webserver --timeout=150s

Write-Host "Aguardando a inicialização completa do Kubernetes Dashboard..." -ForegroundColor Yellow
kubectl rollout status deployment/kubernetes-dashboard -n kubernetes-dashboard --timeout=150s

# Gera o token de acesso
$token = kubectl -n kubernetes-dashboard create token admin-user

$webserverPort = [Environment]::GetEnvironmentVariable("AIRFLOW_WEBSERVER_PORT")
if (-not $webserverPort) { $webserverPort = "8080" }
$adminUser = [Environment]::GetEnvironmentVariable("AIRFLOW_ADMIN_USERNAME")
if (-not $adminUser) { $adminUser = "admin" }
$adminPass = [Environment]::GetEnvironmentVariable("AIRFLOW_ADMIN_PASSWORD")
if (-not $adminPass) { $adminPass = "admin" }

# Inicia o proxy do Kubernetes Dashboard e o port-forward do Airflow de forma assíncrona/minimizada
Write-Host "`n=== 9. Iniciando Acessos Externos (Port-Forward e Proxy) ===" -ForegroundColor Cyan
Write-Host "Iniciando o port-forward do Airflow em segundo plano..." -ForegroundColor Yellow
Start-Process kubectl -ArgumentList "port-forward svc/airflow-webserver ${webserverPort}:${webserverPort}" -WindowStyle Minimized

Write-Host "Iniciando o proxy do Kubernetes Dashboard em segundo plano..." -ForegroundColor Yellow
Start-Process kubectl -ArgumentList "proxy" -WindowStyle Minimized

Write-Host "`n==========================================================" -ForegroundColor Green
Write-Host " DEPLOY CONCLUÍDO COM SUCESSO!" -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green
Write-Host "`n[Como acessar a console do Airflow]" -ForegroundColor Cyan
Write-Host "O port-forward para o Airflow foi iniciado automaticamente!"
Write-Host "1. Acesse no navegador: http://localhost:${webserverPort}"
Write-Host "2. Credenciais: ${adminUser} / ${adminPass}"

Write-Host "`n[Como acessar o Kubernetes Dashboard]" -ForegroundColor Cyan
Write-Host "O proxy (kubectl proxy) foi iniciado automaticamente!"
Write-Host "1. Acesse no navegador: http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/"
Write-Host "2. Utilize o seguinte Token para login:"
Write-Host "$token" -ForegroundColor Yellow
Write-Host "==========================================================" -ForegroundColor Green
