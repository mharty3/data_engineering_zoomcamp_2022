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

