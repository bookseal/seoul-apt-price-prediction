#!/bin/bash
set -e

echo "1. Building Streamlit Docker image..."
sudo docker build -t seoul-apt-price:latest .

echo "2. Importing image into k3s containerd..."
sudo docker save seoul-apt-price:latest | sudo k3s ctr images import -

echo "3. Applying Kubernetes manifests..."
KUBECONFIG=/etc/rancher/k3s/k3s.yaml sudo -E kubectl apply -f k8s/deployment.yaml

echo "Deployment submitted! Check pods with:"
echo "KUBECONFIG=/etc/rancher/k3s/k3s.yaml sudo -E kubectl get pods -l app=seoul-apt-price"
