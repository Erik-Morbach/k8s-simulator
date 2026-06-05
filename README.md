# k8s-simulator
Small simulator to allocate jobs into docker containers based on container status (memory, cpu, network latency)


# Structure

We will structure the project as 1 docker container which will act as the Master Node
2(or more) docker containers which will act as the worker nodes


each worker node will expose a rest api to receive python code to be executed and also an api to get current usage of ram/cpu/disk


# Execution
```bash
./build.sh
docker compose up
```

it will spawn 3 containers using ports:
8080: master
8001: worker1
8002: worker2

you can use the http://localhost:8080/docs to get the swagger of each