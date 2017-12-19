This folder contains docker images and k8s configuration file for hawq which enables hawq deployment on k8s.

# Build docker images
  # Hawq master docker image
    1.`cd masterImage`
    2.Build docker image according to the Dockerfile.
      `sudo docker build -f ./Dockerfile -t docker.io/amyrazz44/hawq_master ./ `
    3.Check the generated docker image by running a docker container.
      `sudo docker run --privileged -it "specificDockerImgesId" /bin/bash`
    4.If you do some changes for the docker image , you better push the changes to docker hub.
      `sudo docker push docker.io/amyrazz44/hawq_master`

  # Hawq segment docker image
    1.`cd segmentImage`
    2.Build docker image according to the Dockerfile.
      `sudo docker build -f ./Dockerfile -t docker.io/amyrazz44/hawq_segment ./ `
    3.Check the generated docker image by running a docker container.
      `sudo docker run --privileged -it "specificDockerImgesId" /bin/bash`
    4.If you do some changes for the docker image , you better push the changes to docker hub.
      `sudo docker push docker.io/amyrazz44/hawq_segment`

# Create k8s master pod, segment pod, master service.
  1. `cd hawq-k8s`
  2. `kubectl create -f hawq-master-rc.yml`
  3. `kubectl create -f hawq-master-service.yml`
  4. `kubectl create -f hawq-segment-rc.yaml` 
     If you want to create more segment pods, copy hawq-segment-rc.yaml to another segment name e.g. hawq-segment-rc1.yaml,
     then use the same command above.
  5. If you want to check the kubectl pods.
     `kubectl get pods`
  6. If you want to check the specific pod status.
     `kubectl describe pod "specificPodName"`
  7. If you want to check the kubectl service.
     `kubectl get service`

# There is file named exchange-hosts.py under the hawq-k8s folder which is to sync the hawq config file and etc hosts file between hawq cluster
# in case hawq process crash or docker container restart. 
