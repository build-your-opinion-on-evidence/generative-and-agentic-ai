import os

for root, dirs, files in os.walk("services"):
    if "main.py" in files:
        main_path = os.path.join(root, "main.py")
        req_path = os.path.join(root, "requirements.txt")
        
        # Update requirements.txt
        if os.path.exists(req_path):
            with open(req_path, "r") as f:
                reqs = f.read()
            if "prometheus-fastapi-instrumentator" not in reqs:
                with open(req_path, "a") as f:
                    f.write("\nprometheus-fastapi-instrumentator==7.0.0\n")
        
        # Update main.py
        with open(main_path, "r") as f:
            content = f.read()
            
        if "prometheus_fastapi_instrumentator" not in content and "app = FastAPI" in content:
            lines = content.split("\n")
            new_lines = []
            imports_added = False
            for line in lines:
                new_lines.append(line)
                if line.startswith("app = FastAPI") and not imports_added:
                    new_lines.insert(0, "from prometheus_fastapi_instrumentator import Instrumentator")
                    new_lines.append("Instrumentator().instrument(app).expose(app)")
                    imports_added = True
            
            with open(main_path, "w") as f:
                f.write("\n".join(new_lines))
        print(f"Processed {main_path}")
