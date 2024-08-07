Основной файл Readme.md находится в ветке main.
Здесь представлен файл Readme.md для ДЗ№9.

Тестовый кластер был развернут в YC облаке. 

# Для второго пукнта дз:
1) подключила пакет bitnami/airflow через helm:
   helm repo add bitnami https://charts.bitnami.com/bitnami
2) переопределила значения параметров:
   helm upgrade --install airflow bitnami/airflow -f k8s/airflow-values.yaml
3) добавила 2 configmap для установки доп библиотеки для dag и копирования авторизационного файла для облака:
  kubectl create -n default configmap requirements --from-file=additional_requirements.txt
  kubectl create -n default configmap ycauthkey --from-file=authorization_key.json
4) c помощью проброса портов зашла внутрь airflow и вручную добавила необходимые переменные
5) запустила DAG (MLFlow был развернут на машине в облаке)

## Скриншот Airflow
![airflow_k8s](https://github.com/user-attachments/assets/e2b8529a-5164-4224-a42b-7fd699a3d909)


# Шаги для оставшихся пунктов:
1) подключила пакет prometheus-community:
   helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
2) устанавила прометей+графана+алерт менеджер одной сборкой:
   helm install prometheus prometheus-community/kube-prometheus-stack --create-namespace -n monitoring -f grafana-inrgess-values.yaml
3) поставила nginx-ingress для постоянного доступа к графане:
   helm install ingress-nginx ingress-nginx --repo https://kubernetes.github.io/ingress-nginx --namespace ingress-nginx --create-namespace -f nginx-ingress-values.yaml --set controller.metrics.enabled=true --set controller.metrics.serviceMonitor.enabled=true --set controller.metrics.serviceMonitor.additionalLabels.release="prometheus"
4) запустила свое приложение:
   kubectl apply -f deployment.yaml
   kubectl apply -f ingress.yaml
   kubectl apply -f service.yaml
   kubectl apply -f service-monitor.yaml
   kubectl apply -f deployment.yaml
6) в интерфейсе графаны создаем правило для алерта в телеграмм с помощью телеграм бота
7) запускаем сервис по имитации нагрузки на приложение
   python transaction_generator.py

Так как запускала на локальном компьютере, то количество реплик оставила равным трем, при увеличении нагрузки поды практически сразу падали, поэтому ограничилась условием алерта, если 1 под упал, то выдавать алерт. 

## Скриншот из телеграмм
![image](https://github.com/user-attachments/assets/a4ac3a5e-a01b-4049-be52-34e660316374)


Скриншот из телеграмм с уведомлением
![2024-08-06_13-04-29](https://github.com/user-attachments/assets/6a398114-5935-4856-8c4a-7056974b81d2)
