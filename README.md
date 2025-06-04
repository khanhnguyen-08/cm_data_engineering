# Data Engineering Fundamentals - Data Pipeline
## Overview
Welcome to the **Data Engineering Fundamentals** project! This hands-on repository is designed to help you learn and practice the foundational concepts of data engineering by building a basic, modular, and reusable ETL pipeline.

## Project Goal

The main goal of this project is to **learn the fundamentals of data pipelines**, from understanding data engineering roles to building and deploying a functional ETL pipeline using modern practices.

---

## Hands-On Components

This project includes hands-on implementation of a basic ETL pipeline:

### Extract
- Read data from structured/unstructured sources (e.g., CSV, API)

### Transform *(optional step for minimal version)*
- Cleanse, normalize, and enrich the data (if required)

### Load
- Write processed data to a data sink (e.g., local database, cloud storage)

---

## Additional Features

The pipeline is designed to include:

- **Modular & Reusable Architecture**
  - Each step of the pipeline is built as an independent, testable module
- **Config-Driven Design**
  - Use YAML/JSON configuration files to define pipeline behavior without code changes
- **DevOps Integration**
  - CI/CD workflows for testing and deployment using tools like GitHub Actions

---

## Getting Started
### Prerequisites

- Python 3.11
    - ```python --version```

- pip
    - ```pip --version```

- Git

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/data-engineering-fundamentals.git
cd data-engineering-fundamentals
```



### 2. Creat virtual environment (optional, but recommended)

**Window**

Create a virtual environment
```bash
python -m venv .venv
```

Activate the environment

```bash
.\.venv\Scripts\activate
```

### 3. Install packages

Using Pip to install packages from requirements.txt file
```bash
pip install -r requirements.txt
```
### 4. Happy Coding!

👨‍💻 👨‍💻 

## Author

Justin Nguyen - Data Engineer

[LinkedIn]()