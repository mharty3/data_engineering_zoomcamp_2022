# Setting up Google Cloud and Terraform

# What is terraform
* Open source tool for provisioning infrastructure resources
* Infrastructure as Code (IaC) - check in cloud infrastucture configuration to version control
* install terraform client: 

        ```
        curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo apt-key add -
        sudo apt-add-repository "deb [arch=amd64] https://apt.releases.hashicorp.com $(lsb_release -cs) main"
        sudo apt-get update && sudo apt-get install terraform
        ```

# Google Cloud Platform (GCP)

* Sign up for GCP and get $300, 90-day free trial
* Set up a new project. I called mine `data-eng-zoomcamp` and google assigned it the unique project id of `data-eng-zoomcamp-339102`
* Go to IAM (Identity and Access Management), Service Accounts. A service account is an account with limited permissions that is assigned to a service, for example a server or VM. This will allow us to create a set of credentials that does not have full access to the owner/admin account.
    * create a service account called dezc-user
    * Grant access as a Basic, Viewer. We will fine tune the permissions in a later step
    * Do not need to grant users access to this service account. But this is useful in a prod environment where it may be useful for multiple users to share the same permissions.
    * Now we need to create a key. Click the three dots under Actions and select `manage keys`
        * Add key, create new key, select JSON
        * This downloads a private key JSON File. I saved it in my linux home directory for now
* Install the gcloud sdk following [these instructions](https://cloud.google.com/sdk/docs/quickstart)
    ```
    sudo apt-get install apt-transport-https ca-certificates gnupg
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
    sudo apt-get update && sudo apt-get install google-cloud-sdk
    '''
* Authenticate the credentials 
    * Set environment variable to point to the location of the GCP key that was downloaded above. 
    ```export GOOGLE_APPLICATION_CREDENTIALS="~/data-eng-zoomcamp-33..."```

    * Run this command and follow the instructions. You will be directed to a browser link and will be given a code to authorize
    
        ```gcloud auth application-default login```

    * There is another way to authenticate that we will learn later when we are working on a headless VM with no access to a browser.


* Edit the permissions of the service account to give it access the services it will need. NOTE: this is simple for this example, but not advisable for production.
    * Go to the main page for `IAM`. And click the pencil next to the service account we just created
    * Add a role: `Storage Admin`. This will allow it to create and modify storage buckets. 
        * For real production, we will create custom role to limit it's access to a particular bucket.
        * also in real production, we would create separate service accounts for terraform, and for the data pipeline etc. In the course we are only making one to keep it simple
    * Add another role: `Storage Object Admin`, this will allow the service to create and modify objects within the buckets. 
    * Add another role: `Big Query Admin`

* Enable these APIs for the google cloud project to allow local environment to interact with cloud environment's IAM
    * https://console.cloud.google.com/apis/library/iam.googleapis.com
    * https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com

# Terraform
* Required Files:
    * `.teraform-version`: defines the version of Terraform: 1.0.2
    * `main.tf:` Define the resources needed
        * `terraform` block
            * Currently referenced as local. but in production, that will change to a bucket
        * `provider` block
            * Terraform relies on plugins called providers that allows it to interact with cloud providers, SaaS providers, and APIs. It provides a given set of resources types and data sources that terraform can manage for a given provider
            * Credentials. Our credentials are set up as an environment variable so this line is commented out, but alternatively, credentials could be stored in the variables.tf file. 
        * `resource` blocks represent resources like VMs, storage buckets, or data warehouses. Arguments can be something like machine size, storage size, names etc.
    * `variables.tf` Defines runtime arguments that will be passed to terraform. Default values can be defined in which case a run time argument is not required.
* Execution Steps
    * `terraform init` - initialize and install
    * `terraform plan` - match configuration changes against current state
        * running this will prompt you for any variables that are not defined with a default 
        * it is like a dry run and it will tell you what changes need to be made.
    * `terraform apply` - apply changes to cloud
        * running this will also prompt for any necessary variables and ask if you want to proceed
        * this will create the resources in google cloud and you can see them in the cloud console!!!
    * `terraform destroy` - Remove your stack from the cloud. This is very useful when developing. You can tear down the environment at the end of the day so you are not charged for running the resources when you aren't using them, and then re-apply them the next day. A big advantage of using something like terraform.


### Execution

```shell
# Refresh service-account's auth-token for this session
gcloud auth application-default login

# Initialize state file (.tfstate)
terraform init

# Check changes to new infra plan
terraform plan -var="project=<your-gcp-project-id>"
```

```shell
# Create new infra
terraform apply -var="project=<your-gcp-project-id>"
```

```shell
# Delete infra after your work, to avoid costs on any running services
terraform destroy
```


# Setting up VM environment on GCP

* [Video](https://www.youtube.com/watch?v=ae-CV2KfoN0&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=12)
* First, we will need to create an SSH key in order to connect to the VM from our laptop [Google Documentation](https://cloud.google.com/compute/docs/connect/create-ssh-keys)
  * Open a terminal and use the ssh-keygen command with the -C flag to create a new SSH key pair.

    ```bash
    ssh-keygen -t rsa -f ~/.ssh/KEY_FILENAME -C USER -b 2048

    ssh-keygen -t rsa -f ~/.ssh/gcp -C michael -b 2048

    ```

  * This command will create a public and private ssh key in the `~/.ssh` directory. **Never share the private key**.  To connect to a remote service, we will give the service the public key, and using encription techniques the remote service and our local computer will be able to securely authenticate each other
  * To add the ssh key to GCP, go to Compute Engine, metadata (under settings), and ssh keys, and copy the contents of the public key file (`gcp.pub`). All instances in the project will be able to use this ssh key.

* From the GCP Console select, compute engine, VM instances, and click create instance. 
  * Select a region near you. (us-central1 (Iowa) for me)
  * Machine type (e2-standard-4 (4vCPU 16 GB memory))
  * Boot disk. Select the OS and storage. (Ubuntu 20.04 LTS 30GB persistant storage)
  * Click Create
* Once it spins up, copy the external IP address to your laptop shell and ssh with the `-i` flag to indicate your private key file

    `ssh -i ~/.ssh/gcp michael@34.132.184.188`

* We can configure the ssh connection on the local machine (laptop) for a better experience. Inside of the .ssh directory, create a file called config with the following contents: 
    
    ```
        Host de-zoomcamp
            HostName 34.132.184.188
            User michael
            IdentityFile ~/.ssh/gcp
    ```
  Now to connect to the host with ssh, all we need to do is `ssh de-zoomcamp` rather than use all the additional arguments

* Configure the VM instance
  * [Download Anaconda Installer](https://www.anaconda.com/products/individual)

    `wget https://repo.anaconda.com/archive/Anaconda3-2021.11-Linux-x86_64.sh`

    `bash Anaconda3-2021.11-Linux-x86_64.sh`

    Accept license agreement and press enter to begin installation
  

    * When anaconda is finished, logout and log back in or run `source .bashrc`

  * install the fish shell. Not necessary, but I like it.

    ```bash
    sudo apt-get install fish
    curl https://raw.githubusercontent.com/oh-my-fish/oh-my-fish/master/bin/install | fish
    omf install agnoster
    ```

    Add `exec fish` to the end of the .bashrc file

  * Now install docker: 

      `sudo apt-get update`
      `sudo apt-get install docker.io`

      run [these](https://github.com/sindresorhus/guides/blob/main/docker-without-sudo.md) commands **and then logout and log back in** so I don't have to type sudo everytime I use a docker command

      `sudo groupadd docker`

      `sudo gpasswd -a $USER docker`

      `sudo service docker restart`

  * Install docker-compose:
    * Create a directory for storing binary files ~/bin and cd there
    * `wget https://github.com/docker/compose/releases/download/v2.2.3/docker-compose-linux-x86_64 -O docker-compose`  NOTE: check for latest version
    * `chmod +x docker-compose`
    * add to path by adding this to the .bashrc file:

      `export PATH="${HOME}/bin:${PATH}"`

  * install pgcli

      `conda install pgcli`

      `pip install -U mycli`

  * Clone my repo
      `git clone https://github.com/mharty3/data_engineering_zoomcamp_2022.git`

  * Install Teraform:
    `wget https://releases.hashicorp.com/terraform/1.1.4/terraform_1.1.4_linux_amd64.zip`
    `unzip terraform_1.1.4_linux_amd64.zip`

    * in order to use teraform we will need to copy the credentials json file over to the VM via sftp. So on the laptop, run:

      ```bash
      sftp
      mkdr .gcp
      cd .gcp
      put data-eng-zoomcamp-339...d0.json
      ```

      Then on the VM:
      ```
      set GOOGLE_APPLICATION_CREDENTIALS ~/.gcp/data-eng-zoomcamp-339102-195b653665d0.json
      gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
      ```

      Run terraform commands as described above if needed.

      Note that in week 2 we moved  and renamed this file to `~/.google/credentials/google_credentials.json`


* Now let's configure VS Code to access the remote machine.
* Install the remote extension for VS Code
    * Because I'm working on WSL, I had do do a few extra steps here. It doesn't seem like I can connect to a remote host from VS code if I am already running VS Code in WSL since VS Code is using the remote extension to connect to WSL. so I had to copy my private key from the linux side (`~/.ssl/gcp`) to the windows side (`C:\Users\michael\.ssh\gcp`). And then modify my ssh host config file (`C:\Users\michael\.ssh\config`) to contain the following. Note I am pointing the to the identify file of the private key now located in the windows directory.

        ``` bash
        Host de-zoomcamp
            HostName 34.132.184.188
            User michael
            IdentityFile C:\Users\michael\.ssh\gcp
        ```
  * Connect to the remote host by clicking the little green square in the bottom left corner of VS Code, Connect to host, and select de-zoomcamp. Now our vs code is connected to the GCP VM!

  * Forward the port from vs code port forwarding for ports `5432` for postgres and `8080` for pgAdmin. I can access the pgAdmin in the browser, but I can't access `5432` with pgcli. I think it may be a wsl networking issue. Let's see if anyone on slack can help.

* **DON"T FORGET TO STOP THE INSTANCE SO YOU WON'T BE CHARGED WHEN YOU AREN'T USING IT**
