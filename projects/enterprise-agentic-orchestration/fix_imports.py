import os

for root, dirs, files in os.walk("services"):
    if "main.py" in files:
        main_path = os.path.join(root, "main.py")
        
        with open(main_path, "r") as f:
            lines = f.read().split("\n")
            
        new_lines = []
        for line in lines:
            if line == "from prometheus_fastapi_instrumentator import Instrumentator":
                continue # Skip the bad top-level import
            if line == "Instrumentator().instrument(app).expose(app)":
                new_lines.append("from prometheus_fastapi_instrumentator import Instrumentator")
            new_lines.append(line)
            
        with open(main_path, "w") as f:
            f.write("\n".join(new_lines))
        print(f"Fixed {main_path}")
