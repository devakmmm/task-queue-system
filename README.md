# TaskFlow — Distributed Task Queue System

![Stack](https://img.shields.io/badge/tech-Python%20%7C%20Flask%20%7C%20Docker%20%7C%20REST-blue)

A **distributed asynchronous task queue system** built with **Python, Flask, and Docker**.  
It supports **priority scheduling**, **automatic retries**, **task monitoring**, and exposes a clean **REST API** for managing background jobs.

🚀 **Live Demo**: [https://task-queue-system-gguf.onrender.com](https://task-queue-system-gguf.onrender.com)  
🩺 **Health Check**: [https://task-queue-system-gguf.onrender.com/health](https://task-queue-system-gguf.onrender.com/health)

---

## ✨ Features

- ✅ Submit tasks via REST API (`POST /tasks`)  
- ✅ Bulk task submission (`POST /tasks/bulk`)  
- ✅ Priority scheduling (URGENT, HIGH, NORMAL, LOW)  
- ✅ Retry logic with exponential backoff  
- ✅ Worker pool with concurrent execution  
- ✅ Task monitoring (status, result, errors)  
- ✅ System statistics (`/stats`)  
- ✅ Dockerized for easy deployment  

---

## 🛠️ Tech Stack

- **Backend**: Python 3.13, Flask, Flask-CORS  
- **Task Management**: Custom `TaskManager` with `ThreadPool` workers  
- **Containerization**: Docker  
- **Deployment**: Render  

---

## 📦 Installation (Local)

### 1. Clone the repo
```bash
git clone https://github.com/devakmmm/task-queue-system.git
cd task-queue-system
