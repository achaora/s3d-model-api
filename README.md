# S3D Models API

This project provides a REST API wrapper around two BigQuery models (`s3d_model_1` and `s3d_model_2`) using **FastAPI** and **Google Cloud Run**.

---

## 🚀 Features

- **Health check endpoint** for monitoring
- **Model 1 (`s3d_model_1`)**: Query by `dependency_id` (single or batch)
- **Model 2 (`s3d_model_2`)**: Query by `dependency_name` or by `(name + version)` pairs
- Always returns values from the **most recent run_date**
- Supports **batch queries** for efficiency

---

## 📂 Endpoints

### Health Check
```http
GET /
