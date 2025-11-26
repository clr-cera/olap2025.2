# Superset Deployment Steps

## 1. System Setup

```sh
sudo apt update -y
sudo apt upgrade -y

# Add Docker's official GPG key:
sudo apt update
sudo apt install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
sudo tee /etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/debian
Suites: $(. /etc/os-release && echo "$VERSION_CODENAME")
Components: stable
Signed-By: /etc/apt/keyrings/docker.asc
EOF

sudo apt update

sudo apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

## 2. Clone the repo & update configs

```sh
git clone --depth=1  https://github.com/apache/superset.git
cd superset
export TAG="5.0.0"
git fetch --tags
git fetch --depth=1 origin tag $TAG
git checkout $TAG
touch ./docker/requirements-local.txt
echo "psycopg2-binary" >> ./docker/requirements-local.txt
```

## 3. Configure caddy and deploy

### Add the following

#### Caddyfile

```Caddyfile
{
    email livia@capivara.cafe
}

superset.usp-olap.capivara.cafe {
    encode gzip zstd
    reverse_proxy superset:8088
}
```

#### To docker compose

```yml
services:
  caddy:
    image: caddy:2-alpine
    restart: unless-stopped
    ports:
      - "80:80"
      - "443:443"
      - "443:443/udp"
    volumes:
      - ./Caddyfile:/etc/caddy/Caddyfile:ro
      - caddy_data:/data
      - caddy_config:/config

volumes:
  caddy_data:
  caddy_config:
```

```sh
docker compose up -d
```
