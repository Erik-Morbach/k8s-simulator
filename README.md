# k8s-simulator
Small simulator to allocate jobs into docker containers based on container status (memory, cpu, network latency)


# Structure

We will structure the project as 1 docker container which will act as the Master Node
2(or more) docker containers which will act as the worker nodes


each worker node will expose a rest api to receive python code to be executed and also an api to get current usage of ram/cpu/disk

