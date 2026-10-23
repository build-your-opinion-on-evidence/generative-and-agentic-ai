with open("docker-compose.yml", "r") as f:
    lines = f.read().split("\n")

new_lines = []
for line in lines:
    new_lines.append(line)
    if "AIRFLOW__CORE__EXECUTOR: LocalExecutor" in line:
        new_lines.append("      _PIP_ADDITIONAL_REQUIREMENTS: 'confluent-kafka httpx'")

with open("docker-compose.yml", "w") as f:
    f.write("\n".join(new_lines))
