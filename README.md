# sample-python-project setup!
In order to run this project, you need to create a virtual environment and populate it with the dependencies listed in requirements.txt.
Run these commands to get started quickly.
python3.10 -m venv venv  
source venv/bin/activate  
python3.10 -m pip install -r requirements.txt  
a beep boop!

# To build
```bash
docker buildx build --platform linux/amd64 . -t docker.lucas.engineering/scraper_manager:0.4
```

# To Push
```bash
docker push docker.lucas.engineering/scraper_manager:0.8
```
