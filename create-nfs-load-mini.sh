#!/bin/bash

PROJECT_ROOT="nfs-load-mini"

# Create directories
mkdir -p \
  "$PROJECT_ROOT/app" \
  "$PROJECT_ROOT/helm/nfs-load-mini/templates"

# Create files
touch \
  "$PROJECT_ROOT/app/main.py" \
  "$PROJECT_ROOT/app/ui.html" \
  "$PROJECT_ROOT/requirements.txt" \
  "$PROJECT_ROOT/Dockerfile" \
  "$PROJECT_ROOT/helm/nfs-load-mini/Chart.yaml" \
  "$PROJECT_ROOT/helm/nfs-load-mini/values.yaml" \
  "$PROJECT_ROOT/helm/nfs-load-mini/templates/deployment.yaml" \
  "$PROJECT_ROOT/helm/nfs-load-mini/templates/service.yaml" \
  "$PROJECT_ROOT/helm/nfs-load-mini/templates/ingress.yaml"

echo "✅ Project structure 'nfs-load-mini' created successfully."
