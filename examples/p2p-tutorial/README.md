# Startup

```bash
# generate private/public keys, ca files for parties
bash setup.sh
# start containers
docker compose up -d
```


# Troubleshooting

1. If you encounter port conflict problem, you could change the default broker published ports to available ports by modifying env `XXX_PORT` in `.env` file.