# Setup
First we will need the Azure resources

### Terraform

Login to Azure
```bash
az login
```

Deploy ADLS Gen 2 storage and Azure Databricks with the following commands:
```bash
terraform init
terraform plan -out terraform.plan
terraform apply terraform.plan
```

Go to ADLS Gen 2 resource and set the OAuth 2.0 access with an Azure service principal: https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sp-access

Copy the following credentials:
* "abfss://container-name@storage-account.dfs.core.windows.net/" to `TARGET_PATH` in the notebook
* application-client-id to `TARGET_APP_ID` in the notebook
* directory-id to `TARGET_DIRECTORY_ID` in the notebook
* create Databricks Secret Scope (https://docs.databricks.com/security/secrets/secret-scopes.html)
* connect secret scope to Key Vault containing client-secret

# Run

Import the notebook to Azure Databricks and run it

# Result

### Run the bronze stream to copy data incrementally with Auto Loader

![image](screenshots/1-bronze-stream.png)

### Run the silver stream to process data

![image](screenshots/2-silver-stream.png)

### Display a sample of silver stream data for verification

![image](screenshots/3-silver-sample.png)

### Select top 10 cities by number of distinct hotels

![image](screenshots/4-top-cities.png)

### Visualize results for each city

#### 1. Paris
![image](screenshots/results/1-paris.png)

#### 2. London
![image](screenshots/results/2-london.png)

#### 3. Barcelona
![image](screenshots/results/3-barcelona.png)

#### 4. Milan
![image](screenshots/results/4-milan.png)

#### 5. Amsterdam
![image](screenshots/results/5-amsterdam.png)

#### 6. Paddington
![image](screenshots/results/6-paddington.png)

#### 7. New York
![image](screenshots/results/7-new-york.png)

#### 8. San Diego
![image](screenshots/results/8-san-diego.png)

#### 9. Vienna
![image](screenshots/results/9-vienna.png)

#### 10. Houston
![image](screenshots/results/10-houston.png)

