# 🍃 Mongo-Migrate

**Mongo-Migrate** is a high-speed, secure, and visually stunning database migration tool designed to move MongoDB data between instances with a single click. Inspired by premium developer tools, it features a vibrant "Gemini Green" aesthetic and real-time process monitoring.

![Premium UI](logo.png)

## ✨ Key Features

- **🚀 High-Speed Migration**: Uses optimized `mongodump` and `mongorestore` protocols for reliable data transfer.
- **🎨 Gemini Theme**: A sleek, dark-mode interface with glassmorphism, fluid animations, and a professional green palette.
- **🛡️ Secure Protocol**: Multi-step confirmation requirements (checkbox + target DB name verification) to prevent accidental data loss.
- **📊 Real-Time Monitoring**: Live Socket.io-powered logs and database statistics (collections, objects).
- **🐳 Docker Ready**: Fully containerized setup with architecture-aware drivers (ARM64/x86_64) for easy deployment.
- **🗂️ Environment Management**: Organize your connections by Production, Staging, and Development environments.

## 🛠️ Tech Stack

- **Backend**: Python, Flask, Flask-SocketIO, PyMongo, SQLite (for connection storage)
- **Frontend**: Vanilla HTML5, CSS3 (Modern Flex/Grid), JavaScript (ES6+), Socket.io
- **Containerization**: Docker, Docker Compose

## 🚀 Quick Start

### Using Docker (Recommended)
The most reliable way to run Mongo-Migrate is via Docker, as it automatically handles all dependencies and MongoDB tools.

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/christodwise/Mongo-Migrate.git
    cd Mongo-Migrate
    ```
2.  **Launch the application**:
    ```bash
    docker-compose up --build -d
    ```
3.  **Access the Studio**:
    Open `http://localhost:5001` in your browser.

### Local Installation
1.  **Install MongoDB Database Tools**: Ensure `mongodump` and `mongorestore` are in your system PATH.
2.  **Install Python dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
3.  **Run the app**:
    ```bash
    python app.py
    ```

## ⚠️ Safety First
To prevent accidental data destruction, Mongo-Migrate requires:
1.  Checking a risk acknowledgement box.
2.  Typing the **exact name** of the target database before the migration process can be initialized.

---
Created with ❤️ for seamless data architecture.
