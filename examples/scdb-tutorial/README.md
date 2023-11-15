# Startup

```bash
docker compose up -d
```


# Troubleshooting

1. If you encounter port conflict problem, you could change the default scdbserver published port `8080` to an available port by modifying env `SCDB_PORT` in `.env` file.