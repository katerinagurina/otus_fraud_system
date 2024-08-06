Основной файл Readme.md находится в ветке main.
Здесь представлен файл Readme.md для ДЗ№9.

Тестовый кластер был развернут с помощью kind. 
Далее ставила пакеты через helm:
1) подключаем пакет helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
2) устанавливаем прометей+графана+алерт менеджер одной сборкой helm install prometheus prometheus-community/kube-prometheus-stack --create-namespace -n monitoring -f grafana-inrgess-values.yaml
3) ставим nginx-ingress для постоянного доступа к графане helm install ingress-nginx ingress-nginx --repo https://kubernetes.github.io/ingress-nginx --namespace ingress-nginx --create-namespace -f nginx-ingress-values.yaml --set controller.metrics.enabled=true --set controller.metrics.serviceMonitor.enabled=true --set controller.metrics.serviceMonitor.additionalLabels.release="prometheus"
4) для переобучения модели подключаем пакет helm repo add bitnami https://charts.bitnami.com/bitnami
5) ставим airflow helm install airflow bitnami/airflow
6) подключаем git репозиторий helm upgrade install airflow bitnami/airflow -f k8s/airflow-values.yaml --set scheduler.automountServiceAccountToken=true --set worker.automountServiceAccountToken=true --set rbac.create=true
7) переменные необходимые для переобучения модели вносила руками через проброс портов в airflow kubectl port-forward svc/airflow 8080:8080

Скриншот из MLFlow

Скриншот из airflow

Скриншот из Grafana с алертом

Скриншот из телеграмм с уведомлением
![2024-08-06_13-04-29](https://github.com/user-attachments/assets/6a398114-5935-4856-8c4a-7056974b81d2)
