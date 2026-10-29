module.exports = {
  apps: [
    {
      name: "ams-monitoring",
      cwd: "D:/Perkuliahan/Pemrograman/Laragon/www/Dashboard-Monitoring-AMS",
      script: "C:/Users/Lenovo/AppData/Local/Programs/Python/Python312/python.exe",
      args: [
        "-m",
        "uvicorn",
        "app.main:app",
        "--host",
        "0.0.0.0",
        "--port",
        "8000"
      ],
      interpreter: "none",
      autorestart: true,
      watch: false
    }
  ]
};
