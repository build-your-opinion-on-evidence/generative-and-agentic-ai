import sys

gateway_service = """
  api-gateway:
    image: nginx:alpine
    container_name: fraud-api-gateway
    networks:
      - fraud-net
    ports:
      - "8000:80"
    volumes:
      - ./config/nginx/api-gateway.conf:/etc/nginx/conf.d/default.conf:ro
      - ./dashboard:/usr/share/nginx/html:ro
    restart: unless-stopped
"""

with open("docker-compose.yml", "r") as f:
    content = f.read()

# Insert before networks:
idx = content.find("\n# ============================================================\n# NETWORKS")
if idx != -1:
    new_content = content[:idx] + gateway_service + content[idx:]
    with open("docker-compose.yml", "w") as f:
        f.write(new_content)
    print("Successfully injected api-gateway")
else:
    print("Could not find insertion point")
